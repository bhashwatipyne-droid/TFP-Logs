"""
export_dashboard_tables.py

Run this LOCALLY, after refresh_logs.py, whenever you want the
deployed Render dashboard to see fresh data.

Exports ONLY the tables the dashboard actually queries — confirmed by
grepping every FROM clause in dashboard/queries.py and database.py,
not guessed from memory:

    event_fact_api, span_fact, cobrand_event_fact,
    capability_catalog, instrumentation_gap_catalog, archive_manifest

Deliberately excludes raw_logs_api, cobrand_raw_logs, span_context,
dim_span, and event_catalog_api — none of these are ever queried by
the dashboard. raw_logs_api and cobrand_raw_logs in particular store
the FULL raw JSON text of every single ingested log line (that's
their entire purpose — incremental-import tracking and historical
preservation for the local ETL), which is almost certainly the bulk
of finpedia_logs.db's size. The dashboard only ever needs the
PROCESSED, STRUCTURED output of the pipeline, not the raw input that
produced it.

Output: one compressed Parquet file per table, in dashboard_export/.
Upload that folder (not the .db file) to Google Drive.
"""

import duckdb
from pathlib import Path

DB_PATH = "finpedia_logs.db"
EXPORT_DIR = Path("dashboard_export")

DASHBOARD_TABLES = [
    "event_fact_api",
    "span_fact",
    "cobrand_event_fact",
    "capability_catalog",
    "instrumentation_gap_catalog",
    "archive_manifest",
]


def main():
    EXPORT_DIR.mkdir(exist_ok=True)

    con = duckdb.connect(DB_PATH, read_only=True)

    original_size = Path(DB_PATH).stat().st_size
    print(f"Original finpedia_logs.db: {original_size / 1024 / 1024:,.1f} MB")
    print()

    total_export_size = 0

    for table in DASHBOARD_TABLES:
        exists = con.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name = ?
        """, [table]).fetchone()[0] > 0

        if not exists:
            print(f"  SKIP {table}: table not found in {DB_PATH}")
            continue

        row_count = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]

        out_path = EXPORT_DIR / f"{table}.parquet"
        con.execute(f"""
            COPY "{table}" TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        size = out_path.stat().st_size
        total_export_size += size
        print(f"  {table}: {row_count:,} row(s) -> {size / 1024 / 1024:,.1f} MB")

    con.close()

    print()
    print(f"Total export size: {total_export_size / 1024 / 1024:,.1f} MB")
    print(f"Reduction vs original: {(1 - total_export_size / original_size) * 100:,.1f}%")
    print()
    print(f"Upload the '{EXPORT_DIR}/' folder to Google Drive.")


if __name__ == "__main__":
    main()
