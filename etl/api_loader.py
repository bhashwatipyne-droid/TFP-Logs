import json
import logging
import glob
import os

import duckdb

from etl.instrumentation_gap import (
    sync_instrumentation_gap_catalog,
    print_instrumentation_backlog,
)

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


# event_time in event_fact_api is a VARCHAR straight from the source
# log's "timestamp" field. Real production data mixes two formats:
#   - genuinely 12-hour values with a correct AM/PM suffix, e.g.
#     "Thu, 02 Jul 2026 10:25:13 AM" -> parses directly with %H %p
#     (DuckDB's strptime correctly combines %H with %p here, including
#     the edge cases of "00 AM" / "12 PM" / "12 AM").
#   - 24-hour values with a spurious/incorrect AM/PM suffix appended,
#     e.g. "Fri, 17 Jul 2026 18:13:04 PM" — 18 is not a valid 12-hour
#     value, so "PM" after it is meaningless. This is a bug in
#     whatever service emits these logs (about 2,100 out of 3,660
#     sampled rows had this shape), not something fixable in the ETL —
#     but it CAN be parsed around safely, because the two cases never
#     overlap: TRY_STRPTIME with CAPABILITY_TIMESTAMP_FORMAT_12H only
#     succeeds when the hour is genuinely in 12-hour range (00-12); it
#     correctly returns NULL for anything outside that range (verified
#     directly — it does not silently produce a wrong result for an
#     out-of-range hour). So whenever the strict parse fails, the hour
#     MUST already be a raw 24-hour value (13-23 has no other valid
#     interpretation), and stripping the trailing AM/PM text before
#     parsing as plain 24-hour is unambiguous in exactly that case.
CAPABILITY_TIMESTAMP_FORMAT_12H = "%a, %d %b %Y %H:%M:%S %p"
CAPABILITY_TIMESTAMP_FORMAT_24H = "%a, %d %b %Y %H:%M:%S"

# Single source of truth for parsing event_time, used everywhere it's
# needed in the aggregation query below rather than repeating this
# COALESCE inline four separate times.
CAPABILITY_TIMESTAMP_SQL = rf"""
    COALESCE(
        TRY_STRPTIME(event_time, '{CAPABILITY_TIMESTAMP_FORMAT_12H}'),
        TRY_STRPTIME(
            regexp_replace(event_time, '\s*(AM|PM)$', '', 'i'),
            '{CAPABILITY_TIMESTAMP_FORMAT_24H}'
        )
    )
"""

# Single source of truth for how an event_prefix is derived from
# event_action. Previously this exact CASE expression was duplicated
# between event_catalog_api and _compute_capability_metadata() — two
# copies of the same logic that could silently drift apart over time
# if one was ever edited without the other. (Concrete evidence this
# already happened once: capability_catalog has an orphaned
# "sso.partner" entry that doesn't match how prefixes are computed
# today, meaning the logic used to derive it worked differently at
# some point in the past.) Both call sites now interpolate this same
# string, so there is exactly one place to ever change it.
EVENT_PREFIX_SQL = """
    CASE
        WHEN event_action IS NULL THEN NULL
        WHEN strpos(event_action, '.') = 0 THEN event_action
        WHEN starts_with(event_action, 'content.cobrand.') THEN 'content.cobrand'
        ELSE split_part(event_action, '.', 1)
    END
"""


def _ensure_capability_catalog_schema(con):
    """
    Step 1: create/migrate capability_catalog to the Event Registry
    schema.

    If the table doesn't exist yet, it's created fresh with the full
    schema. If an older version already exists (the original
    domain/module/layer/owner_team/criticality/remarks/discovered_at
    schema), the new columns are added alongside it via idempotent
    ALTER TABLE ADD COLUMN IF NOT EXISTS calls rather than dropping and
    recreating the table — this preserves any human-entered data that
    might already be sitting in the old columns. The old columns are
    simply left in place, unused, going forward.
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS capability_catalog (
            event_prefix            VARCHAR PRIMARY KEY,
            capability              VARCHAR,
            subsystem               VARCHAR,
            description             VARCHAR,
            classification_status   VARCHAR,
            first_seen              TIMESTAMP,
            last_seen               TIMESTAMP,
            event_count             BIGINT,
            sample_event            VARCHAR,
            sample_service          VARCHAR
        )
    """)

    for column, coltype in [
        ("capability", "VARCHAR"),
        ("subsystem", "VARCHAR"),
        ("description", "VARCHAR"),
        ("classification_status", "VARCHAR"),
        ("first_seen", "TIMESTAMP"),
        ("last_seen", "TIMESTAMP"),
        ("event_count", "BIGINT"),
        ("sample_event", "VARCHAR"),
        ("sample_service", "VARCHAR"),
    ]:
        con.execute(
            f"ALTER TABLE capability_catalog ADD COLUMN IF NOT EXISTS {column} {coltype}"
        )


def _compute_capability_metadata(con):
    """
    Step 2: aggregate event_fact_api into one row per event_prefix.
    Aggregation only — no inserts, no updates.

    Reuses the exact same event_prefix CASE expression as
    event_catalog_api (including the content.cobrand.* special case),
    so the two tables never disagree about what a "prefix" means for
    the same event.

    event_time is a VARCHAR (e.g. "Sun, 12 Jul 2026 00:00:04 AM") that
    does NOT sort correctly as a plain string — weekday-name-first
    ordering has nothing to do with actual chronological order. It's
    parsed via CAPABILITY_TIMESTAMP_SQL, a COALESCE of two formats
    (see that constant's comment for why real data has both) before
    MIN/MAX; a timestamp that fails BOTH formats still degrades to
    NULL gracefully rather than raising, same as before.

    sample_event / sample_service use ARG_MAX keyed on the parsed
    timestamp, so the sample shown is always tied to the MOST RECENT
    event for that prefix — deterministic and meaningful, rather than
    an arbitrary row that could differ from run to run.
    """
    return con.execute(f"""
        SELECT
            {EVENT_PREFIX_SQL} AS event_prefix,

            MIN({CAPABILITY_TIMESTAMP_SQL}) AS first_seen,
            MAX({CAPABILITY_TIMESTAMP_SQL}) AS last_seen,
            COUNT(*) AS event_count,
            ARG_MAX(event_action, {CAPABILITY_TIMESTAMP_SQL}) AS sample_event,
            ARG_MAX(service_name, {CAPABILITY_TIMESTAMP_SQL}) AS sample_service

        FROM event_fact_api
        WHERE event_action IS NOT NULL
        GROUP BY event_prefix
    """).fetchdf()


def sync_capability_catalog(con):
    """
    Step 3: sync capability_catalog from the latest event_fact_api
    aggregation.

    New prefixes are inserted with classification_status = 'Pending',
    capability/subsystem/description left NULL for a human to fill in.

    Existing prefixes only ever get last_seen, event_count,
    sample_event, and sample_service overwritten unconditionally (via
    the ON CONFLICT clause below). capability, subsystem, and
    description never appear in that SET clause at all, so they're
    structurally impossible for this function to overwrite.

    classification_status is a special case: it DOES appear in the SET
    clause, but only as
        classification_status = COALESCE(capability_catalog.classification_status, 'Pending')
    which keeps the existing value untouched whenever one is already
    set (Pending/Classified/Deprecated/Ignored), and only fills in
    'Pending' when it's currently NULL. This matters for one specific
    case: a table migrated from the pre-Event-Registry schema (via
    _ensure_capability_catalog_schema's ALTER TABLE ADD COLUMN) has
    classification_status = NULL on every pre-existing row, since ADD
    COLUMN can only set a single default for all rows, not 'Pending'
    for some and a real status for others. Without this COALESCE,
    every one of those legacy rows would silently and permanently fall
    out of the "needs classification" query below (NULL never equals
    'Pending' in SQL) even though nothing about them had actually been
    classified — which is exactly what happened the first time this
    was deployed against a real database with a pre-existing table.

    first_seen self-corrects toward the true earliest observation via
        first_seen = LEAST(capability_catalog.first_seen, excluded.first_seen)
    This handles a log file backfilled out of order after a prefix is
    already registered — the stored first_seen moves earlier the next
    time a sync sees an earlier timestamp for that prefix, with zero
    manual maintenance. This is NULL-safe: DuckDB's LEAST/GREATEST skip
    NULL arguments the way MIN/MAX do (verified directly), rather than
    propagating NULL the way arithmetic operators do — which matters
    here specifically, since some prefixes have events whose
    timestamps all fail to parse (e.g. content_ad: 58 events, but
    first_seen/last_seen both NULL because TRY_STRPTIME couldn't parse
    any of them). Without that NULL-skipping behavior, a single
    all-unparseable sync run would permanently wipe out a previously
    correct first_seen.

    Returns
    -------
    (int, list[tuple])
        Total prefix count, and the list of (event_prefix, event_count,
        last_seen, sample_event) rows still awaiting classification —
        used both for the summary count and the detailed backlog
        printout.
    """
    _ensure_capability_catalog_schema(con)

    metadata = _compute_capability_metadata(con)

    con.register("capability_metadata", metadata)

    con.execute("""
        INSERT INTO capability_catalog (
            event_prefix, classification_status,
            first_seen, last_seen, event_count,
            sample_event, sample_service
        )
        SELECT
            event_prefix, 'Pending',
            first_seen, last_seen, event_count,
            sample_event, sample_service
        FROM capability_metadata
        WHERE event_prefix IS NOT NULL
        ON CONFLICT (event_prefix) DO UPDATE SET
            last_seen      = excluded.last_seen,
            event_count    = excluded.event_count,
            sample_event   = excluded.sample_event,
            sample_service = excluded.sample_service,
            classification_status = COALESCE(capability_catalog.classification_status, 'Pending'),
            first_seen = LEAST(capability_catalog.first_seen, excluded.first_seen)
    """)

    con.unregister("capability_metadata")

    total_prefixes = con.execute(
        "SELECT COUNT(*) FROM capability_catalog"
    ).fetchone()[0]

    pending = con.execute("""
        SELECT event_prefix, event_count, last_seen, sample_event
        FROM capability_catalog
        WHERE classification_status = 'Pending' OR classification_status IS NULL
        ORDER BY event_count DESC
    """).fetchall()

    return total_prefixes, pending


def _print_capability_coverage(con):
    """
    Print overall Event Registry coverage: how much of the catalog has
    actually been triaged by a human vs still sitting untouched.

    "Classified" here means classification_status is anything other
    than Pending/NULL — i.e. a human has looked at it and made SOME
    decision, whether that decision was 'Classified', 'Deprecated', or
    'Ignored'. That's the meaningful coverage signal: how much of the
    registry a human has actually reviewed, not literally how many are
    labeled with the specific string 'Classified'.
    """
    total, pending = con.execute("""
        SELECT
            COUNT(*),
            SUM(CASE WHEN classification_status = 'Pending' OR classification_status IS NULL THEN 1 ELSE 0 END)
        FROM capability_catalog
    """).fetchone()

    pending = pending or 0
    classified = total - pending
    coverage = (classified / total * 100) if total else 0.0

    logger.info("Total prefixes : %d", total)
    logger.info("Classified     : %d", classified)
    logger.info("Pending        : %d", pending)
    logger.info("Coverage       : %.0f%%", coverage)


def _print_classification_backlog(pending):
    """
    Step 4: richer per-prefix printout of everything still awaiting
    human classification, ordered by event_count descending (already
    sorted that way by sync_capability_catalog's query, NULLS LAST) so
    the highest-volume — i.e. highest-priority — prefixes surface
    first.

    event_count / last_seen / sample_event can be None for a prefix
    that exists in capability_catalog but didn't match any row in this
    run's freshly-computed event aggregation — i.e. a prefix that no
    longer produces any current events (renamed, retired, or a source
    event type that's stopped firing). That's genuinely useful to see
    rather than hide, since it's a signal the prefix might deserve
    classification_status = 'Deprecated' rather than 'Pending'.
    """
    if not pending:
        logger.info("All prefixes are classified.")
        return

    logger.info("Prefixes awaiting classification:")

    for event_prefix, event_count, last_seen, sample_event in pending:
        logger.info("-" * 55)
        logger.info(event_prefix)
        events_display = f"{event_count:,}" if event_count is not None else "0 (no current events — possibly stale/renamed)"
        logger.info("  Events    : %s", events_display)
        logger.info("  Last Seen : %s", last_seen if last_seen is not None else "—")
        logger.info("  Example   : %s", sample_event if sample_event is not None else "—")

    logger.info("-" * 55)


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

    con.execute(f"""
    CREATE TABLE event_catalog_api AS

    SELECT DISTINCT

        event_action,

        {EVENT_PREFIX_SQL} AS event_prefix

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

    total_prefixes, missing = sync_capability_catalog(con)
    new_prefixes = len(missing)

    logger.info("capability_catalog synchronized (%d prefixes)", total_prefixes)
    logger.info("%d prefix(es) require classification", new_prefixes)

    logger.info("Synchronizing instrumentation_gap_catalog (API)...")

    # event_fact_api already has message/service_name/level and the
    # same event_time format capability_catalog already solved for
    # (CAPABILITY_TIMESTAMP_SQL), so it's reused directly here rather
    # than re-deriving a second timestamp parser.
    api_gap_source_sql = f"""
        SELECT
            message,
            service_name,
            level AS log_level,
            {CAPABILITY_TIMESTAMP_SQL} AS parsed_event_time
        FROM event_fact_api
        WHERE event_action IS NULL
          AND message IS NOT NULL
    """

    gap_total, gap_pending = sync_instrumentation_gap_catalog(con, "API", api_gap_source_sql)

    logger.info(
        "instrumentation_gap_catalog synchronized (%d pattern(s), %d pending)",
        gap_total, len(gap_pending),
    )

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

    _print_capability_coverage(con)
    _print_classification_backlog(missing)
    print_instrumentation_backlog(gap_pending, "API")

    return processed_files


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    refresh()
