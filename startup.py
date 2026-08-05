"""
startup.py

Run once, before Streamlit starts, on Render:
    python startup.py && streamlit run dashboard/app.py --server.port=$PORT --server.address=0.0.0.0

1. If finpedia_logs.db already exists on disk (a warm restart within
   the same Render instance's uptime — NOT guaranteed across
   redeploys, since Render's filesystem is ephemeral), skip everything
   and exit immediately.
2. Otherwise, download the dashboard_export/ folder (6 small Parquet
   files — see export_dashboard_tables.py) from Google Drive, only if
   it isn't already present locally either.
3. Build finpedia_logs.db as a set of VIEWs directly over those
   Parquet files — NOT a full data copy. DuckDB queries Parquet
   natively and efficiently, so this step is close to instant
   regardless of data size, and dashboard/database.py's read-only
   connection works identically against a view as against a real
   table (verified directly: views appear in SHOW TABLES and
   information_schema.tables exactly like tables do).

This intentionally does NOT run any part of etl/ or parsers/ — those
stay exactly as your local ingestion pipeline, untouched. Render only
ever sees the already-processed output.

Configuration: set the GDRIVE_FOLDER_URL environment variable in
Render's dashboard (Settings -> Environment) to the shareable Google
Drive folder URL containing your dashboard_export/ Parquet files.
Falls back to the hardcoded default below if unset — convenient for
now, but move the real value into Render's environment once you're
ready to stop hardcoding it (the folder link is effectively public to
anyone who has it, given "anyone with the link" sharing).
"""

import os
import sys
import shutil
import zipfile
import tempfile
from pathlib import Path

DB_PATH = "finpedia_logs.db"
PARQUET_DIR = Path("dashboard_export")
ZIP_PATH = Path("dashboard_export.zip")

DASHBOARD_TABLES = [
    "event_fact_api",
    "span_fact",
    "cobrand_event_fact",
    "capability_catalog",
    "instrumentation_gap_catalog",
    "archive_manifest",
]

# The Google Drive file ID for dashboard_export.zip (export_dashboard_
# tables.py's output, zipped). Using the file ID directly with
# gdown.download(id=...) rather than the full share URL — this is the
# call signature confirmed working against the real file (gdown 6.1.0
# has no `fuzzy` parameter, so passing the raw "/view?usp=sharing" URL
# doesn't reliably work; the ID-based call does).
DEFAULT_GDRIVE_FILE_ID = "19C8DpLHweF7fNJezZSJ3RhUSJOdG8kei"


def download_parquet_files():
    import gdown

    file_id = os.environ.get("GDRIVE_FILE_ID", DEFAULT_GDRIVE_FILE_ID)

    print("Downloading Parquet export from Google Drive...")

    gdown.download(id=file_id, output=str(ZIP_PATH), quiet=False)

    if not ZIP_PATH.exists():
        print(f"ERROR: download did not produce {ZIP_PATH}.")
        print("Check the Drive file's sharing is set to 'Anyone with the link'.")
        sys.exit(1)

    print(f"Extracting {ZIP_PATH}...")

    PARQUET_DIR.mkdir(exist_ok=True)

    # Extract to a temp location first, then flatten: find every
    # *.parquet file inside (regardless of whether the zip wrapped
    # them in a subfolder matching PARQUET_DIR's own name or not) and
    # copy them directly into PARQUET_DIR. This works whether the zip
    # was made with `zip -r dashboard_export/` (nested folder) or by
    # zipping the loose files directly (flat).
    with tempfile.TemporaryDirectory() as tmp_dir:
        with zipfile.ZipFile(ZIP_PATH) as z:
            z.extractall(tmp_dir)

        found = list(Path(tmp_dir).rglob("*.parquet"))
        if not found:
            print(f"ERROR: no .parquet files found inside {ZIP_PATH}.")
            sys.exit(1)

        for f in found:
            shutil.copy(f, PARQUET_DIR / f.name)

    ZIP_PATH.unlink(missing_ok=True)

    missing = [
        t for t in DASHBOARD_TABLES
        if not (PARQUET_DIR / f"{t}.parquet").exists()
    ]
    if missing:
        print(f"ERROR: extraction completed but these files are missing: {missing}")
        print("Check the zip actually contains all 6 .parquet files from ")
        print("export_dashboard_tables.py.")
        sys.exit(1)

    print("Download and extraction complete.")


def build_database():
    import duckdb

    print(f"Building {DB_PATH} as views over the Parquet files...")

    con = duckdb.connect(DB_PATH)

    for table in DASHBOARD_TABLES:
        parquet_path = PARQUET_DIR / f"{table}.parquet"
        con.execute(f"""
            CREATE OR REPLACE VIEW "{table}" AS
            SELECT * FROM read_parquet('{parquet_path}')
        """)
        row_count = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        print(f"  {table}: {row_count:,} row(s)")

    con.close()
    print(f"{DB_PATH} ready.")


def main():
    if Path(DB_PATH).exists():
        print(f"{DB_PATH} already exists — skipping download and rebuild.")
        return

    if not PARQUET_DIR.exists() or not any(PARQUET_DIR.glob("*.parquet")):
        download_parquet_files()
    else:
        print(f"{PARQUET_DIR}/ already has Parquet files — skipping download.")

    build_database()


if __name__ == "__main__":
    main()