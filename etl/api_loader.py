import json
import logging
import glob
import os

import duckdb

logger = logging.getLogger(__name__)


def _import_log_file(con, log_file: str, filename: str) -> int:
    """
    Read a JSON-lines log file directly in Python and insert each
    valid line into raw_logs_api.

    Bypasses DuckDB's read_csv, which was previously used as a
    line-splitting trick (delim='\\n', one VARCHAR column per line).
    That approach broke starting with the 2026-07-12 logs: DuckDB's
    CSV sniffer inspects the file before honoring the delim/quote/
    escape overrides, and the nested trace.method/children structures
    introduced on 2026-07-12 contain enough commas that the sniffer
    guessed 5 columns instead of the 1 we declared, raising
    InvalidInputException. Reading the file natively in Python sidesteps
    CSV parsing entirely, and lets us validate + skip bad lines
    individually instead of failing the whole file.
    """
    rows = []
    skipped = 0

    with open(log_file, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                json.loads(line)  # validate it's real JSON before inserting
            except json.JSONDecodeError as ex:
                logger.warning(
                    "%s line %d: skipping invalid JSON (%s)",
                    filename, line_no, ex,
                )
                skipped += 1
                continue
            rows.append((filename, line))

    if rows:
        con.executemany(
            "INSERT INTO raw_logs_api SELECT ?, json(?)",
            rows,
        )

    if skipped:
        logger.warning("%s: skipped %d malformed line(s)", filename, skipped)

    return len(rows)


def refresh():
    """
    Refresh the API log warehouse.

    Returns
    -------
    list[str]
        Paths (as they appeared in the logs/api/*.log glob) of every
        log file that is confirmed present in raw_logs_api after this
        run — i.e. files imported just now, plus files that were
        already imported in a prior run. This is the list that should
        be handed to archive_logs("logs/archive/api") downstream;
        anything not in this list failed to import and should not be
        archived.
    """

    logger.info("=" * 60)
    logger.info("TheFinpedia Log Refresh")
    logger.info("=" * 60)

    con = duckdb.connect("finpedia_logs.db")

    # Everything happens inside try/finally so the connection is always
    # closed — and the file lock released — even if something raises
    # partway through. Without this, an interrupted or crashed run can
    # leave finpedia_logs.db locked for the *next* run (this is exactly
    # what caused the "Conflicting lock is held" error).
    try:
        return _run_refresh(con)
    finally:
        con.close()


def _run_refresh(con):

    logger.info("Connected successfully.")

    # ---------------------------------------------------------
    # Ensure raw_logs exists
    # ---------------------------------------------------------

    con.execute("""
    CREATE TABLE IF NOT EXISTS raw_logs_api (
        source_file VARCHAR,
        json MAP(VARCHAR, JSON)
    )
    """)

    logger.info("raw_logs table ready.")

    # ---------------------------------------------------------
    # Find already imported log files
    # ---------------------------------------------------------

    existing_files = {
        row[0]
        for row in con.execute("""
            SELECT DISTINCT source_file
            FROM raw_logs_api
        """).fetchall()
    }

    logger.info("Already imported : %d file(s)", len(existing_files))

    logger.info("Searching for log files...")

    log_files = sorted(glob.glob("logs/api/*.log"))

    if not log_files:
        logger.info("No log files found.")
        return []

    logger.info("Found %d log files.", len(log_files))

    for log_file in log_files:

        filename = os.path.basename(log_file)

        if filename in existing_files:
            logger.info("%s (already imported)", filename)
        else:
            logger.info("%s (new)", filename)

    logger.info("Importing new log files...")

    new_files = 0
    processed_files = []

    for log_file in log_files:

        filename = os.path.basename(log_file)

        if filename in existing_files:
            # Already in the warehouse from a prior run. Still eligible
            # for archiving if the .log file is still sitting on disk.
            processed_files.append(log_file)
            continue

        logger.info("Importing %s", filename)

        try:
            imported_rows = _import_log_file(con, log_file, filename)
        except Exception as ex:
            logger.error("Failed to import %s: %s", filename, ex)
            continue

        logger.info("%s: inserted %d row(s)", filename, imported_rows)

        new_files += 1
        processed_files.append(log_file)

    if new_files == 0:
        logger.info("No new log files to import.")
    else:
        logger.info("Imported %d new file(s).", new_files)

    logger.info("Current files in warehouse:")

    logger.info(
        "\n%s",
        con.execute("""
            SELECT
                source_file,
                COUNT(*) AS rows
            FROM raw_logs_api
            GROUP BY source_file
            ORDER BY source_file
        """).fetchdf()
    )

    logger.info("Rebuilding event_fact...")

    con.execute("DROP TABLE IF EXISTS event_fact_api")

    con.execute("""
    CREATE TABLE event_fact_api AS

    SELECT

        source_file,

        TRIM(BOTH '"' FROM CAST(json['timestamp'] AS VARCHAR))              AS event_time,

        TRIM(BOTH '"' FROM CAST(json['event.action'] AS VARCHAR))           AS event_action,

        TRIM(BOTH '"' FROM CAST(json['message'] AS VARCHAR))                AS message,

        TRIM(BOTH '"' FROM CAST(json['level'] AS VARCHAR))                  AS level,

        TRIM(BOTH '"' FROM CAST(json['method.name'] AS VARCHAR))            AS method_name,

        TRIM(BOTH '"' FROM CAST(json['route.name'] AS VARCHAR))             AS route_name,

        TRIM(BOTH '"' FROM CAST(json['request.id'] AS VARCHAR))             AS request_id,

        TRIM(BOTH '"' FROM CAST(json['platform.name'] AS VARCHAR))          AS platform_name,

        CAST(json['scheduled_post_planner.id'] AS BIGINT)                  AS planner_id,

        CAST(json['scheduled_post_planner_platform.id'] AS BIGINT)         AS planner_platform_id,

        CAST(json['user.id'] AS BIGINT)                                    AS user_id,

        CAST(json['playlist.id'] AS BIGINT)                                AS playlist_id,
        CAST(json['event.duration_ms'] AS DOUBLE)                          AS duration_ms,

        TRIM(BOTH '"' FROM CAST(json['error.message'] AS VARCHAR))         AS error_message,
        TRIM(BOTH '"' FROM CAST(json['error.stack'] AS VARCHAR))           AS error_stack,
        TRIM(BOTH '"' FROM CAST(json['job.name'] AS VARCHAR))              AS job_name,
        CAST(json['job.attempt'] AS INTEGER)                              AS job_attempt,      
        CAST(json['job.max_attempts'] AS INTEGER)                         AS job_max_attempts,
        TRIM(BOTH '"' FROM CAST(json['http.request.method'] AS VARCHAR))   AS http_method,
        TRIM(BOTH '"' FROM CAST(json['url.path'] AS VARCHAR))             AS url_path,
        TRIM(BOTH '"' FROM CAST(json['service.name'] AS VARCHAR))         AS service_name
        FROM raw_logs_api
    """)

    rows = con.execute("""
    SELECT COUNT(*)
    FROM event_fact_api
    """).fetchone()[0]

    logger.info("event_fact_api rebuilt (%s rows)", f"{rows:,}")

    logger.info("Rebuilding event_catalog_api...")

    con.execute("DROP TABLE IF EXISTS event_catalog_api")

    con.execute("""
    CREATE TABLE event_catalog_api AS

    SELECT DISTINCT

        event_action,

        CASE

            WHEN event_action IS NULL THEN NULL

            WHEN strpos(event_action, '.') = 0 THEN event_action

            WHEN starts_with(event_action, 'content.cobrand.') THEN 'content.cobrand'

            ELSE split_part(event_action, '.', 1)

        END AS event_prefix

    FROM event_fact_api

    WHERE event_action IS NOT NULL

    ORDER BY event_action

    """)

    events = con.execute("""
    SELECT COUNT(*)
    FROM event_catalog_api
    """).fetchone()[0]

    logger.info("event_catalog_api rebuilt (%s unique events)", f"{events:,}")

    logger.info("Synchronizing capability_catalog...")

    # Create table if it doesn't exist
    con.execute("""
    CREATE TABLE IF NOT EXISTS capability_catalog (

        event_prefix VARCHAR PRIMARY KEY,

        domain VARCHAR,

        module VARCHAR,

        layer VARCHAR,

        owner_team VARCHAR,

        criticality VARCHAR,

        remarks VARCHAR,

        discovered_at TIMESTAMP

    )
    """)

    # Insert only new prefixes
    con.execute("""

    INSERT INTO capability_catalog (

        event_prefix,

        discovered_at

    )

    SELECT

        DISTINCT ec.event_prefix,

        CURRENT_TIMESTAMP

    FROM event_catalog_api ec

    LEFT JOIN capability_catalog cc

    ON ec.event_prefix = cc.event_prefix

    WHERE cc.event_prefix IS NULL

    """)

    # Find all prefixes that still need classification

    missing = con.execute("""

    SELECT

        event_prefix,

        discovered_at

    FROM capability_catalog

    WHERE domain IS NULL

    ORDER BY discovered_at DESC,
             event_prefix

    """).fetchall()

    new_prefixes = len(missing)

    total_prefixes = con.execute("""

    SELECT

    COUNT(*)

    FROM capability_catalog

    """).fetchone()[0]

    logger.info("capability_catalog synchronized (%d prefixes)", total_prefixes)
    logger.info("%d prefix(es) require classification", new_prefixes)

    logger.info("=" * 60)
    logger.info("Warehouse Refresh Complete")
    logger.info("=" * 60)

    raw_rows = con.execute("""
    SELECT COUNT(*)
    FROM raw_logs_api
    """).fetchone()[0]

    fact_rows = con.execute("""
    SELECT COUNT(*)
    FROM event_fact_api
    """).fetchone()[0]

    event_count = con.execute("""
    SELECT COUNT(*)
    FROM event_catalog_api
    """).fetchone()[0]

    capability_count = con.execute("""
    SELECT COUNT(*)
    FROM capability_catalog
    """).fetchone()[0]

    logger.info("Raw Logs           : %s", f"{raw_rows:,}")
    logger.info("Event Facts        : %s", f"{fact_rows:,}")
    logger.info("Unique Events      : %s", f"{event_count:,}")
    logger.info("Capabilities       : %s", f"{capability_count:,}")
    logger.info("Unclassified       : %s", f"{new_prefixes:,}")

    logger.info("=" * 60)
    logger.info("Warehouse Ready")
    logger.info("=" * 60)

    # Log any prefixes that still need classification

    if missing:

        logger.info("Prefixes awaiting classification:")

        for prefix, discovered_at in missing:
            logger.info("  - %s   (%s)", prefix, discovered_at)

    else:
        logger.info("All prefixes are classified.")

    return processed_files


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    refresh()
