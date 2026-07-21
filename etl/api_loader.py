import duckdb
import glob
import os


def refresh():

    print("=" * 60)
    print("TheFinpedia Log Refresh")
    print("=" * 60)

    con = duckdb.connect("finpedia_logs.db")

    print("Connected successfully.")
    # ---------------------------------------------------------
    # Ensure raw_logs exists
    # ---------------------------------------------------------

    con.execute("""
    CREATE TABLE IF NOT EXISTS raw_logs_api (
        source_file VARCHAR,
        json MAP(VARCHAR, JSON)
    )
    """)

    print("✓ raw_logs table ready.")

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

    print(f"Already imported : {len(existing_files)} file(s)")

    print("\nSearching for log files...")

    log_files = sorted(glob.glob("logs/api/*.log"))

    if not log_files:
        print("❌ No log files found.")
        return

    print(f"Found {len(log_files)} log files.\n")

    for log_file in log_files:

        filename = os.path.basename(log_file)

        if filename in existing_files:
            print(f"✓ {filename} (already imported)")
        else:
            print(f"○ {filename} (new)")

    print("\nImporting new log files...\n")

    new_files = 0

    for log_file in log_files:

        filename = os.path.basename(log_file)

        if filename in existing_files:
            continue

        print(f"→ Importing {filename}")

        con.execute(f"""
            INSERT INTO raw_logs_api
            SELECT
                '{filename}',
                json(line)
            FROM read_csv(
                '{log_file}',
                columns={{'line':'VARCHAR'}},
                delim='\\n',
                quote='',
                escape=''
            )
        """)

        new_files += 1

    print()

    if new_files == 0:
        print("✓ No new log files to import.")
    else:
        print(f"✓ Imported {new_files} new file(s).")


    print("\nCurrent files in warehouse:\n")

    print(
        con.execute("""
            SELECT
                source_file,
                COUNT(*) AS rows
            FROM raw_logs_api
            GROUP BY source_file
            ORDER BY source_file
        """).fetchdf()
    )


    print("\nRebuilding event_fact...")

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

    print(f"✓ event_fact_api rebuilt ({rows:,} rows)")

    print("\nRebuilding event_catalog_api...")

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

    print(f"✓ event_catalog_apirebuilt ({events:,} unique events)")

    print("\nSynchronizing capability_catalog...")

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

    print(f"✓ capability_catalog synchronized ({total_prefixes} prefixes)")
    print(f"✓ {new_prefixes} prefix(es) require classification")


    print("\n" + "=" * 60)
    print("Warehouse Refresh Complete")
    print("=" * 60)

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

    print(f"Raw Logs           : {raw_rows:,}")
    print(f"Event Facts        : {fact_rows:,}")
    print(f"Unique Events      : {event_count:,}")
    print(f"Capabilities       : {capability_count:,}")
    print(f"Unclassified       : {new_prefixes:,}")

    print("=" * 60)
    print("Warehouse Ready")
    print("=" * 60)

    # Print any prefixes that still need classification

    if missing:

        print("\nPrefixes awaiting classification:\n")

        for prefix, discovered_at in missing:
            print(f"  • {prefix}   ({discovered_at})")

    else:
        print("\n✓ All prefixes are classified.")


if __name__ == "__main__":
    refresh()
