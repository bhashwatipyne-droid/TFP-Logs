import duckdb
import glob
import json
import os
import pandas as pd

from parsers.parser import TraceParser
from etl.instrumentation_gap import (
    sync_instrumentation_gap_catalog,
    print_instrumentation_backlog,
)

DB = "finpedia_logs.db"

# Cobrand's event_time format is subtly different from the API side's:
# "Tue, 21 Jul 2026, 00:00:47 pm" — note the comma before the time
# AND lowercase am/pm (DuckDB's %p is case-insensitive, verified
# directly, so no special handling needed for the case difference).
# Same defensive fallback as api_loader.py's CAPABILITY_TIMESTAMP_SQL:
# try the strict 12-hour+%p parse first (returns NULL for genuinely
# out-of-range hours rather than a silently wrong result — verified),
# then fall back to stripping AM/PM and parsing as raw 24-hour for
# whatever that strict parse rejects.
_COBRAND_TIMESTAMP_FORMAT_12H = "%a, %d %b %Y, %H:%M:%S %p"
_COBRAND_TIMESTAMP_FORMAT_24H = "%a, %d %b %Y, %H:%M:%S"

COBRAND_TIMESTAMP_SQL = rf"""
    COALESCE(
        TRY_STRPTIME(event_time, '{_COBRAND_TIMESTAMP_FORMAT_12H}'),
        TRY_STRPTIME(
            regexp_replace(event_time, '\s*(AM|PM)$', '', 'i'),
            '{_COBRAND_TIMESTAMP_FORMAT_24H}'
        )
    )
"""


def _parse_cobrand_line(line: str):
    """
    Parse one raw line of a Cobrand log file.

    Returns
    -------
    dict | None
        The parsed JSON object, or None if the line is empty or not
        valid JSON (the caller is responsible for logging/counting
        that as a skip — this function just reports the outcome).
    """
    line = line.strip()
    if not line:
        return None
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None


def _import_cobrand_file(con, log_file: str, filename: str):
    """
    Import the trace-bearing lines of one Cobrand log file into
    cobrand_raw_logs, parsing each line directly in Python rather than
    handing the file to DuckDB's read_json_auto().

    DuckDB's JSON reader has its own internal limits/quirks around
    large, deeply-nested single-line JSON that Python's own json
    module doesn't share — application-2026-07-20.log (line 403) and
    application-2026-07-21.log (line 10) both parse cleanly with
    json.loads(), yet DuckDB's read_json_auto() rejected both as
    "Malformed JSON". Parsing in Python sidesteps that class of
    failure entirely, and means one bad line is skipped and logged
    individually instead of aborting the whole file.

    Returns
    -------
    (int, int)
        (rows inserted, lines skipped as invalid JSON)
    """
    rows = []
    skipped = 0

    with open(log_file, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):

            record = _parse_cobrand_line(line)

            if record is None:
                if line.strip():
                    print(f"    ⚠ {filename} line {line_no}: skipping invalid JSON")
                    skipped += 1
                continue

            trace = record.get("trace.method")
            if trace is None:
                continue  # standalone event line, not a trace — handled separately

            # The trace.method sub-object itself never carries a
            # "timestamp" key anywhere in it (verified directly against
            # real data: 0 of 8,836 span_fact rows had one). The only
            # timestamp for this whole trace lives as a SIBLING key on
            # the outer log line — it was being silently discarded here
            # every time trace.method got extracted on its own. Captured
            # now so the parser has something to work with.
            trace_timestamp = record.get("timestamp")

            rows.append((filename, json.dumps(trace), trace_timestamp))

    if rows:
        con.executemany(
            "INSERT INTO cobrand_raw_logs (source_file, trace, trace_timestamp) SELECT ?, json(?), ?",
            rows,
        )

    if skipped:
        print(f"    ⚠ {filename}: skipped {skipped} malformed line(s)")

    return len(rows), skipped


def _read_non_trace_events(files):
    """
    Read all standalone-event ("trace.method" absent) rows across a
    specific list of files, parsing each line directly in Python
    rather than handing the file to DuckDB's read_json_auto() — same
    reasoning as _import_cobrand_file() above. One malformed line, or
    even one entirely unreadable file, no longer aborts extraction for
    every other file.

    Returns
    -------
    (pandas.DataFrame, int)
        The concatenated event rows from every file that could be
        opened (empty DataFrame if none), and how many files were
        successfully opened and scanned (as opposed to files that
        couldn't be opened at all — e.g. deleted mid-run). Individual
        malformed lines are skipped and logged, but don't count
        against a file here, since DuckDB previously failed entire
        FILES over single bad LINES — that distinction no longer
        applies now that we parse line-by-line ourselves.
    """
    all_rows = []
    success_count = 0

    for file in files:

        filename = os.path.basename(file)

        try:
            with open(file, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except OSError as ex:
            print(f"✗ Could not open {filename} for event extraction: {ex}")
            continue

        success_count += 1

        for line_no, line in enumerate(lines, start=1):

            record = _parse_cobrand_line(line)

            if record is None:
                if line.strip():
                    print(f"    ⚠ {filename} line {line_no}: skipping invalid JSON")
                continue

            if record.get("trace.method") is not None:
                continue  # trace-bearing line, handled in cobrand_raw_logs

            record["filename"] = filename
            all_rows.append(record)

    if all_rows:
        return pd.DataFrame(all_rows), success_count

    return pd.DataFrame(), success_count


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

    try:
        return _run_refresh(con)
    finally:
        con.close()


def _run_refresh(con):

    parser = TraceParser()

    print("Connected to DuckDB.")

    con.execute("""

    CREATE TABLE IF NOT EXISTS cobrand_raw_logs (

        source_file VARCHAR,

        trace JSON,

        trace_timestamp VARCHAR

    )

    """)

    # Migration for a table created before trace_timestamp existed —
    # ADD COLUMN IF NOT EXISTS is a no-op if it's already there.
    con.execute(
        "ALTER TABLE cobrand_raw_logs ADD COLUMN IF NOT EXISTS trace_timestamp VARCHAR"
    )

    # raw_id: a stable, unique per-row identifier, assigned once at
    # insert time and never recomputed. This is what makes trace
    # parsing incremental (see the "Parsing traces" section below) —
    # without it, every refresh had to re-parse the ENTIRE historical
    # cobrand_raw_logs table through TraceParser every single run,
    # regardless of whether anything new came in. At current volume
    # that's still sub-second, but it's the one part of this pipeline
    # whose cost scales with TOTAL historical data rather than NEW
    # data since last run — worth fixing before Cobrand volume grows
    # into the tens of thousands of lines/day.
    #
    # DEFAULT nextval(...) means existing INSERT statements don't need
    # to change at all — DuckDB assigns each row (existing or new) its
    # own distinct sequence value automatically, verified directly
    # (ALTER TABLE ADD COLUMN on 3 existing rows assigned 1, 2, 3; a
    # subsequent 2-row INSERT correctly continued at 4, 5 — not the
    # same value repeated for the whole batch).
    con.execute("CREATE SEQUENCE IF NOT EXISTS cobrand_raw_logs_seq START 1")
    con.execute(
        "ALTER TABLE cobrand_raw_logs ADD COLUMN IF NOT EXISTS raw_id "
        "BIGINT DEFAULT nextval('cobrand_raw_logs_seq')"
    )

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
    processed_files = []

    for file in files:

        filename = os.path.basename(file)

        if filename in existing_files:

            print(f"✓ {filename} (already imported)")

            # Already in the warehouse from a prior run. Still eligible
            # for archiving if the .log file is still sitting on disk
            # (mirrors the same handling in api_loader.py).
            processed_files.append(file)

            continue

        print(f"→ Importing {filename}")

        try:
            imported_rows, _skipped = _import_cobrand_file(con, file, filename)
        except OSError as ex:
            print(f"✗ Failed to import {filename}: {ex}")
            continue

        print(f"  inserted {imported_rows} trace row(s)")

        new_files += 1
        processed_files.append(file)

    print()

    if new_files == 0:
        print("✓ No new log files to import.")
    else:
        print(f"✓ Imported {new_files} new file(s).")

    # span_fact's schema is created up front (not after parsing) so the
    # anti-join below has something to check new traces against, even
    # on a completely fresh database. CREATE TABLE IF NOT EXISTS, not
    # DROP+CREATE — this table is now built incrementally: existing
    # spans are never touched, only genuinely new ones get appended.
    # One-time migration: if span_fact already exists from before
    # raw_id existed, its rows have no way to be linked back to
    # cobrand_raw_logs at all — CREATE TABLE IF NOT EXISTS won't
    # retroactively add a column to an existing table. Left alone,
    # every historical trace would look "not yet processed" against
    # an empty/mismatched raw_id, and get re-parsed and duplicated
    # alongside the existing rows. Since cobrand_raw_logs already has
    # every historical trace preserved, the safe fix is a one-time
    # full rebuild of span_fact + span_context here — costs one slower
    # run, then every run after this is genuinely incremental.
    span_fact_exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name = 'span_fact'
    """).fetchone()[0] > 0

    if span_fact_exists:
        existing_span_fact_columns = {
            row[0] for row in con.execute("DESCRIBE span_fact").fetchall()
        }
        if "raw_id" not in existing_span_fact_columns:
            print(
                "\nMigrating span_fact to incremental (raw_id-tracked) "
                "parsing — one-time full rebuild from cobrand_raw_logs..."
            )
            con.execute("DROP TABLE IF EXISTS span_fact")
            con.execute("DROP TABLE IF EXISTS span_context")

    con.execute("""

    CREATE TABLE IF NOT EXISTS span_fact (

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

        file_extension VARCHAR,

        raw_id BIGINT

    )

    """)

    print("\nParsing traces from cobrand_raw_logs...")

    # Only parse raw trace rows that haven't already been turned into
    # spans — this is the whole point of raw_id. Previously this
    # SELECT had no WHERE clause at all, meaning every refresh re-fed
    # the ENTIRE historical cobrand_raw_logs table through TraceParser
    # (pure Python), regardless of whether anything new came in. That
    # cost scales with total historical data, not new data since last
    # run — the anti-join below makes the cost scale with new data
    # only, same as api_loader.py's existing_files pattern.
    df = con.execute("""

    SELECT

        r.source_file,

        r.trace,

        r.trace_timestamp,

        r.raw_id

    FROM cobrand_raw_logs r

    WHERE NOT EXISTS (
        SELECT 1 FROM span_fact sf WHERE sf.raw_id = r.raw_id
    )

    """).fetchdf()

    already_processed = con.execute(
        "SELECT COUNT(DISTINCT raw_id) FROM span_fact"
    ).fetchone()[0]

    print(f"Found {len(df)} new trace(s) to parse ({already_processed:,} already processed)")

    # Read per-file rather than via a wildcard glob, so a malformed
    # file (e.g. application-2026-07-20.log) can't abort the whole
    # preview — it just gets skipped and logged individually.
    event_df, _ = _read_non_trace_events(files)

    if event_df.empty:
        print("No event rows found for preview (no files, or none parsed).")

    print(event_df.head(20))

    all_rows = []

    for i, row in df.iterrows():

        trace = row["trace"]

        if isinstance(trace, str):
            trace = json.loads(trace)

        parser.parse(trace, trace_timestamp=row["trace_timestamp"])

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

                "details": span.details,

                "raw_id": row["raw_id"]

            }

            for column, (location, key) in FIELD_MAP.items():

                if location == "details":

                    new_row[column] = span.get_detail(key)

                else:

                    new_row[column] = span.get_attribute(key)

            all_rows.append(new_row)

    final_df = pd.DataFrame(all_rows)

    print()

    print(f"Parsed {len(final_df):,} new span(s)")

    print("Appending to span_fact...")

    if not final_df.empty:

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

        file_extension,

        raw_id

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

        file_extension,

        raw_id

        FROM final_df

        """)

        con.unregister("final_df")

    rows = con.execute("""

    SELECT COUNT(*)

    FROM span_fact

    """).fetchone()[0]

    print(f"✓ span_fact now has {rows:,} span(s) total ({len(final_df):,} new this run)")

    print("\nBuilding cobrand_event_fact...")

    # Read per-file (see _read_non_trace_events) rather than via a
    # single wildcard glob. cobrand_event_fact is rebuilt from whatever
    # is currently readable on disk (CREATE OR REPLACE), so we only
    # skip the rebuild in the genuinely dangerous case — nothing at
    # all could be read, whether because there are zero files or every
    # file failed to parse. Recreating the table from an empty result
    # in that case would silently wipe out e