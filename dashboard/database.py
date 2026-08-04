"""
database.py

DuckDB connection layer for the Logs360 dashboard.

CRITICAL DESIGN CONSTRAINT, verified directly before writing any of
this: DuckDB enforces a single-writer lock at the OS level, across
processes. refresh_logs.py (a separate process, run manually or via
cron) needs read-write access periodically. If this dashboard held a
single long-lived connection open — the typical Streamlit pattern via
st.cache_resource — it would permanently block refresh_logs.py from
ever writing again, for as long as the dashboard process stays up.
Confirmed directly: a reader connection opened and left open causes a
concurrent writer to fail immediately with "Conflicting lock is held",
not just contend briefly.

So this module deliberately does NOT cache a connection object across
reruns. Every query opens a fresh, short-lived, read-only connection
and closes it immediately after use. This means every query pays a
small connection-open cost, but that's milliseconds for a local file,
and st.cache_data (in queries.py) means most dashboard interactions
never touch the database at all within the cache TTL window.

The one unavoidable tradeoff: if a query happens to run during the
few seconds refresh_logs.py is actively writing, it will fail with a
lock error. That's a narrow, expected window (refresh_logs.py runs
take seconds, not minutes) — handled here with a short retry/backoff,
falling back to a clear, non-crashing message if the ETL is still
running after those retries.
"""

import time
import sys
import logging
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

# The dashboard lives in a subfolder (dashboard/) of the main project,
# with etl/ and parsers/ as siblings of that subfolder, one level up.
# Streamlit only adds the directory containing the launched script
# (dashboard/, since app.py lives there) to sys.path automatically —
# it does NOT add the project root, so `from etl.api_loader import
# ...` (in queries.py) fails with ModuleNotFoundError unless the
# project root is added explicitly here, before anything imports from
# etl. Computed from this file's own location rather than relying on
# the current working directory, so this works regardless of which
# directory `streamlit run` was launched from.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

logger = logging.getLogger(__name__)

DB_PATH = "finpedia_logs.db"

# How many times to retry opening a connection if refresh_logs.py
# happens to be writing at that exact moment, and how long to wait
# between attempts. refresh_logs.py runs have consistently taken low
# single-digit seconds throughout this project's development, so this
# retry budget comfortably covers a run in progress without making the
# dashboard feel slow on the (much more common) non-conflicting case.
_MAX_RETRIES = 5
_RETRY_DELAY_SECONDS = 1.0


class WarehouseBusyError(Exception):
    """
    Raised when the database file is locked by refresh_logs.py after
    all retries are exhausted. Callers (pages) should catch this
    specifically and show a friendly "refreshing, try again shortly"
    message rather than letting a raw DuckDB exception surface.
    """
    pass


@contextmanager
def get_connection():
    """
    Open a short-lived, read-only DuckDB connection as a context
    manager. Always closes the connection on exit, even on error —
    this is what keeps the dashboard from ever blocking
    refresh_logs.py for longer than a single query's duration.

    Retries briefly if the file is locked (refresh_logs.py actively
    writing), then raises WarehouseBusyError if still locked.
    """
    db_path = Path(DB_PATH)

    if not db_path.exists():
        raise FileNotFoundError(
            f"{DB_PATH} not found. Run refresh_logs.py at least once "
            f"before starting the dashboard."
        )

    last_error = None

    for attempt in range(_MAX_RETRIES):
        try:
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                yield con
            finally:
                con.close()
            return
        except duckdb.IOException as ex:
            last_error = ex
            if "lock" in str(ex).lower():
                logger.warning(
                    "Database locked (likely refresh_logs.py running), "
                    "retry %d/%d",
                    attempt + 1, _MAX_RETRIES,
                )
                time.sleep(_RETRY_DELAY_SECONDS)
                continue
            raise

    raise WarehouseBusyError(
        "The warehouse is currently being refreshed. Please try again "
        "in a few seconds."
    ) from last_error


def run_query(sql: str, params: list = None) -> pd.DataFrame:
    """
    Execute a SQL query and return the result as a DataFrame.

    Not cached itself — callers in queries.py wrap this with
    @st.cache_data and an explicit TTL, since caching needs to be
    aware of the query's parameters (e.g. the selected date range) to
    invalidate correctly. This function is the uncached primitive
    every cached query function calls.
    """
    with get_connection() as con:
        if params:
            return con.execute(sql, params).fetchdf()
        return con.execute(sql).fetchdf()


def table_exists(table_name: str) -> bool:
    """
    Check whether a table exists before querying it. Used defensively
    on pages that reference tables which may not exist yet on a fresh
    or partially-populated warehouse (e.g. instrumentation_gap_catalog
    before the first Cobrand/API sync has ever run), so a missing
    table shows as an empty/friendly state rather than a crash.
    """
    try:
        with get_connection() as con:
            result = con.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = ?
            """, [table_name]).fetchone()
            return result[0] > 0
    except WarehouseBusyError:
        # If we can't even check, assume it might exist rather than
        # hiding a page that would otherwise work once the ETL finishes.
        return True


@st.cache_data(ttl=30, show_spinner=False)
def get_warehouse_status() -> dict:
    """
    Lightweight status check for the header/sidebar: last refresh
    time (from archive_manifest, the most recently touched table) and
    whether the warehouse is currently reachable at all.
    """
    try:
        df = run_query("""
            SELECT MAX(COALESCE(deleted_at, validated_at, archived_at)) AS last_activity
            FROM archive_manifest
        """)
        last_activity = df["last_activity"].iloc[0] if not df.empty else None
        return {"reachable": True, "last_activity": last_activity}
    except WarehouseBusyError:
        return {"reachable": False, "last_activity": None}
    except Exception as ex:
        logger.error("get_warehouse_status failed: %s", ex)
        return {"reachable": False, "last_activity": None}
