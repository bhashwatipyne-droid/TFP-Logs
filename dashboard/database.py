"""
database.py

DuckDB connection layer for the Logs360 dashboard.

CRITICAL DESIGN CONSTRAINT, verified directly before writing any of
this: DuckDB enforces a single-writer lock at the OS level, across
processes. LOCALLY, refresh_logs.py (a separate process, run manually
or via cron) needs read-write access periodically. If the dashboard
held a single long-lived connection open there, it would permanently
block refresh_logs.py from ever writing again. Confirmed directly: a
reader connection opened and left open causes a concurrent writer to
fail immediately with "Conflicting lock is held", not just contend
briefly. That's why, locally, this module opens a fresh short-lived
connection per query rather than caching one.

ON RENDER, this exact risk doesn't exist: startup.py is the ONLY
writer, it runs once, and it always completes (via the `python
startup.py && streamlit run ...` start command) before Streamlit ever
starts serving requests. Once the dashboard is live, nothing ever
writes to finpedia_logs.db again for that container's lifetime. Paying
the local design's per-query connection-open cost there was pure
overhead with no safety benefit — and it's a REAL cost: Render's free
tier allocates a small fraction of a CPU core (confirmed via Render's
own docs), so repeated connection-open overhead across many small
queries per page compounds into genuinely slow page loads, distinct
from (and in addition to) the free-tier container cold-start delay.

So on Render (detected via the RENDER env var, which Render's own
docs confirm is always set to "true" there — render.com/docs/
environment-variables), this module opens ONE connection lazily and
reuses it for the life of the process. Sharing a single raw DuckDB
Connection object across concurrent threads is NOT safe — verified
directly: 20 threads querying one shared connection concurrently
produced inconsistent results with no error raised, a silent
correctness bug, not just a crash risk. The fix, also verified
directly (50 threads, zero errors, all results correct): give each
query its own lightweight cursor via con.cursor(), which shares the
underlying connection safely.
"""

import os
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

# Render sets this to "true" on every service automatically — see
# render.com/docs/environment-variables. Absent entirely when running
# locally, so IS_RENDER is False by default.
IS_RENDER = os.environ.get("RENDER") == "true"

# How many times to retry opening a connection if refresh_logs.py
# happens to be writing at that exact moment, and how long to wait
# between attempts. refresh_logs.py runs have consistently taken low
# single-digit seconds throughout this project's development, so this
# retry budget comfortably covers a run in progress without making the
# dashboard feel slow on the (much more common) non-conflicting case.
# Only relevant locally — see IS_RENDER above.
_MAX_RETRIES = 5
_RETRY_DELAY_SECONDS = 1.0

# Render-only: the single persistent connection, opened lazily on
# first use and reused for the process's lifetime. Never touched when
# IS_RENDER is False.
_render_connection = None


class WarehouseBusyError(Exception):
    """
    Raised when the database file is locked by refresh_logs.py after
    all retries are exhausted. Callers (pages) should catch this
    specifically and show a friendly "refreshing, try again shortly"
    message rather than letting a raw DuckDB exception surface.

    Cannot happen on Render (no concurrent local writer there — see
    module docstring), only when running locally.
    """
    pass


@contextmanager
def get_connection():
    """
    Yield a DuckDB connection (Render: a shared cursor on the one
    persistent connection; locally: a fresh short-lived connection).
    Callers use this identically either way via `with get_connection()
    as con:` — the branching is fully internal to this function.
    """
    if IS_RENDER:
        global _render_connection

        db_path = Path(DB_PATH)
        if not db_path.exists():
            raise FileNotFoundError(
                f"{DB_PATH} not found. startup.py should have built "
                f"this before Streamlit started."
            )

        if _render_connection is None:
            _render_connection = duckdb.connect(str(db_path), read_only=True)

        # A fresh cursor per call, not the raw connection object
        # itself — verified directly that sharing the raw connection
        # across concurrent threads silently corrupts results, while
        # per-call cursors on a shared connection are safe.
        cursor = _render_connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
        return

    # --- Local path: original fresh-connection-per-call design ---

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


@st.cache_data(ttl=300, show_spinner=False)
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
