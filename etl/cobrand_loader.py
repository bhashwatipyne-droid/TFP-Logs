import duckdb
import glob
import json
import os
import pandas as pd

from parsers.parser import TraceParser

DB = "finpedia_logs.db"

FIELD_MAP = {

    # ----------------------------
    # Identity
    # ----------------------------

    "request_id": ("details", "request.id"),

    # ----------------------------
    # User
    # ----------------------------

    "user_uuid": ("details", "user.uuid"),

    # ----------------------------
    # Content
    # ----------------------------

    "content_uuid": ("details", "content.uuid"),

    "content_variation_id": ("details", "content_variation.id"),

    "content_variation_uuid": ("details", "content_variation.uuid"),

    "content_type": ("details", "content_type.code"),

    # ----------------------------
    # Cobrand
    # ----------------------------

    "cobrand_job_type": ("details", "cobrand.job.type"),

    "cobrand_status": ("details", "cobrand.status"),

    "cobrand_branch": ("details", "cobrand.branch"),

    # ----------------------------
    # Queue
    # ----------------------------

    "queue_pending_before": (
        "details",
        "queue.pending_count_before_push"
    ),

    "queue_pending_after": (
        "details",
        "queue.pending_count_after_push"
    ),

    "queue_remaining": (
        "details",
        "queue.remaining_count"
    ),

    # ----------------------------
    # Callback
    # ----------------------------

    "callback_completed": (
        "details",
        "callback.completed"
    ),

    "callback_attempt": (
        "details",
        "callback.attempt"
    ),

    "callback_max_attempts": (
        "details",
        "callback.max_attempts"
    ),

    # ----------------------------
    # Output
    # ----------------------------

    "generated_file_exists": (
        "details",
        "cobrand.generated_file_exists"
    ),

    "generated_file_size": (
        "details",
        "cobrand.generated_file_size_bytes"
    ),

    "file_extension": (
        "details",
        "file.extension"
    ),

    # ----------------------------
    # Errors
    # ----------------------------

    "error_stack": (
        "details",
        "error.stack"
    )
}


def refresh():

    con = duckdb.connect(DB)

    parser = TraceParser()

    print("Connected to DuckDB.")

    con.execute("""

    CREATE TABLE IF NOT EXISTS cobrand_raw_logs (

        source_file VARCHAR,

        trace JSON

    )

    """)

    existing_files = {

        row[0]

        for row in con.execute("""

            SELECT DISTINCT source_file

            FROM cobrand_raw_logs

        """).fetchall()

    }

    print(f"Already imported : {len(existing_files)} file(s)")

    print("\nSearching for Cobrand log files...")

    files = sorted(
        glob.glob("logs/cobrand/*.log")
    )

    print(f"Found {len(files)} Cobrand log file(s)")

    new_files = 0

    for file in files:

        filename = os.path.basename(file)

        if filename in existing_files:

            print(f"✓ {filename} (already imported)")

            continue

        print(f"→ Importing {filename}")

        con.execute(f"""

        INSERT INTO cobrand_raw_logs

        SELECT

            '{filename}',

            "trace.method"

        FROM read_json_auto('{file}')

        WHERE "trace.method" IS NOT NULL

        """)

        new_files += 1

    print()

    if new_files == 0:
        print("✓ No new log files to import.")
    else:
        print(f"✓ Imported {new_files} new file(s).")

    print("\nParsing traces from cobrand_raw_logs...")

    df = con.execute("""

    SELECT

        source_file,

        trace

    FROM cobrand_raw_logs

    """).fetchdf()

    print(f"Found {len(df)} traces")

    all_rows = []

    for i, row in df.iterrows():

        trace = row["trace"]

        if isinstance(trace, str):
            trace = json.loads(trace)

        parser.parse(trace)

        for span in parser.rows:

            span.source_file = row["source_file"]

            new_row = {

                "trace_id": span.trace_id,

                "span_id": span.span_id,

                "parent_id": span.parent_id,

                "depth": span.depth,

                "event_time": span.event_time,

                "event_action": span.event_action,

                "method_name": span.method_name,

                "duration_ms": span.duration_ms,

                "result_status": span.result_status,

                "error_message": span.error_message,

                "user_id": span.user_id,

                "user_tracking_id": span.user_tracking_id,

                "content_id": span.content_id,

                "service_name": span.service_name,

                "source": "cobrand",

                "source_file": span.source_file,

                "attributes": span.attributes,

                "details": span.details

            }

            for column, (location, key) in FIELD_MAP.items():

                if location == "details":

                    new_row[column] = span.get_detail(key)

                else:

                    new_row[column] = span.get_attribute(key)

            all_rows.append(new_row)

    final_df = pd.DataFrame(all_rows)

    print()

    print(f"Parsed {len(final_df):,} spans")

    print("Creating span_fact...")

    con.execute("""

    DROP TABLE IF EXISTS span_fact

    """)

    con.execute("""

    CREATE TABLE span_fact (

        trace_id UUID,

        span_id UUID,

        parent_id UUID,

        depth INTEGER,

        source VARCHAR,

        source_file VARCHAR,

        event_time VARCHAR,

        request_id UUID,

        event_action VARCHAR,

        method_name VARCHAR,

        duration_ms DOUBLE,

        result_status VARCHAR,

        error_message VARCHAR,

        error_stack VARCHAR,

        user_id BIGINT,

        user_uuid UUID,

        user_tracking_id BIGINT,

        content_id BIGINT,

        content_uuid UUID,

        content_variation_id BIGINT,

        content_variation_uuid UUID,

        content_type VARCHAR,

        cobrand_job_type VARCHAR,

        cobrand_status VARCHAR,

        cobrand_branch VARCHAR,

        queue_pending_before BIGINT,

        queue_pending_after BIGINT,

        queue_remaining BIGINT,

        callback_completed BOOLEAN,

        callback_attempt INTEGER,

        callback_max_attempts INTEGER,

        generated_file_exists BOOLEAN,

        generated_file_size BIGINT,

        file_extension VARCHAR

    )

    """)

    con.register(
        "final_df",
        final_df
    )

    con.execute("""

    INSERT INTO span_fact (

    trace_id,
    span_id,
    parent_id,
    depth,

    source,
    source_file,

    event_time,
    request_id,

    event_action,
    method_name,

    duration_ms,

    result_status,

    error_message,
    error_stack,

    user_id,
    user_uuid,
    user_tracking_id,

    content_id,
    content_uuid,

    content_variation_id,
    content_variation_uuid,

    content_type,

    cobrand_job_type,
    cobrand_status,
    cobrand_branch,

    queue_pending_before,
    queue_pending_after,
    queue_remaining,

    callback_completed,
    callback_attempt,
    callback_max_attempts,

    generated_file_exists,
    generated_file_size,

    file_extension

    )

    SELECT

    trace_id,
    span_id,
    parent_id,
    depth,

    source,
    source_file,

    event_time,
    request_id,

    event_action,
    method_name,

    duration_ms,

    result_status,

    error_message,
    error_stack,

    user_id,
    user_uuid,
    user_tracking_id,

    content_id,
    content_uuid,

    content_variation_id,
    content_variation_uuid,

    content_type,

    cobrand_job_type,
    cobrand_status,
    cobrand_branch,

    queue_pending_before,
    queue_pending_after,
    queue_remaining,

    callback_completed,
    callback_attempt,
    callback_max_attempts,

    generated_file_exists,
    generated_file_size,

    file_extension

    FROM final_df

    """)

    rows = con.execute("""

    SELECT COUNT(*)

    FROM span_fact

    """).fetchone()[0]

    print(f"✓ span_fact built ({rows:,} spans)")

    context_df = final_df[[
        "span_id",
        "attributes",
        "details"
    ]].copy()

    context_df["attributes"] = context_df["attributes"].apply(
        lambda d: json.dumps(d, default=str)
    )

    context_df["details"] = context_df["details"].apply(
        lambda d: json.dumps(d, default=str)
    )

    con.execute("""

    DROP TABLE IF EXISTS span_context

    """)

    con.execute("""

    CREATE TABLE span_context (

        span_id UUID PRIMARY KEY,

        attributes JSON,

        details JSON

    )

    """)

    con.register(
        "span_context_df",
        context_df
    )

    con.execute("""

    INSERT INTO span_context

    SELECT

        span_id,

        attributes,

        details

    FROM span_context_df

    """)

    context_rows = con.execute("""

    SELECT COUNT(*)

    FROM span_context

    """).fetchone()[0]

    print(f"✓ span_context built ({context_rows:,} rows)")

    print()

    print("Synchronizing dim_span...")

    con.execute("""

    CREATE TABLE IF NOT EXISTS dim_span (

        event_action VARCHAR PRIMARY KEY,

        service_name VARCHAR,

        method_name VARCHAR,

        layer VARCHAR,

        category VARCHAR,

        criticality VARCHAR,

        owner_team VARCHAR,

        remarks VARCHAR,

        discovered_at TIMESTAMP

    )

    """)

    con.execute("""

    INSERT INTO dim_span (

        event_action,

        service_name,

        method_name,

        discovered_at

    )

    SELECT

        event_action,

        source AS service_name,

        method_name,

        CURRENT_TIMESTAMP

    FROM span_fact

    WHERE event_action NOT IN (

        SELECT event_action

        FROM dim_span

    )

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY event_action

        ORDER BY method_name

    ) = 1

    """)

    missing = con.execute("""

    SELECT

    event_action

    FROM dim_span

    WHERE layer IS NULL

    ORDER BY event_action

    """).fetchall()

    print(f"✓ {len(missing)} span(s) require classification")

    if missing:

        print()

        print("Spans awaiting classification:\n")

        for row in missing:

            print(f" • {row[0]}")


if __name__ == "__main__":
    refresh()
