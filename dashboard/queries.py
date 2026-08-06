"""
queries.py

SQL query functions for the Logs360 dashboard. Every function here:
  - Returns a pandas DataFrame (or a plain dict of scalars for KPIs).
  - Is wrapped in @st.cache_data with an explicit TTL, since query
    results depend on the selected date range / filters, not just on
    being called with no arguments.
  - Uses database.run_query(), which opens/closes a short-lived
    read-only connection per call — see database.py's docstring for
    why that matters here specifically.

CRITICAL: event_time is stored as a VARCHAR (not a real TIMESTAMP) in
BOTH event_fact_api and span_fact, and in two DIFFERENT broken formats
across the two tables — this was discovered and fixed the hard way
earlier in this project (mixed 12-hour/24-hour-with-spurious-AM/PM
timestamps on the API side; a comma-separated variant on the Cobrand
side). Rather than re-deriving that parsing logic a third time here
(and risking getting it subtly wrong), this module imports the exact
same COALESCE/TRY_STRPTIME expressions the ETL itself uses as its
single source of truth:
    CAPABILITY_TIMESTAMP_SQL  (etl/api_loader.py)   -> for event_fact_api
    COBRAND_TIMESTAMP_SQL     (etl/cobrand_loader.py) -> for span_fact
Both fragments assume the column is literally named "event_time",
which holds for both tables — do not reuse either fragment against a
table with a differently-named timestamp column without checking.
"""

import logging

import pandas as pd
import streamlit as st

from database import run_query, table_exists, get_connection

# Reusing the ETL's own verified timestamp-parsing logic rather than
# redefining it — these modules only define constants/functions at
# import time, no file I/O or side effects happen just by importing.
from etl.api_loader import CAPABILITY_TIMESTAMP_SQL
from etl.cobrand_loader import COBRAND_TIMESTAMP_SQL

# Same reuse principle for pattern normalization: rather than
# re-deriving the regex logic that turns a raw log message into the
# same raw_pattern instrumentation_gap_catalog was built with, import
# the ETL's own functions directly. _first_line_sql has a leading
# underscore (Python convention for "internal"), but it's genuinely
# the single source of truth for this normalization — reusing it here
# is deliberate, not a workaround.
from etl.instrumentation_gap import normalized_message_sql, _first_line_sql

logger = logging.getLogger(__name__)

CACHE_TTL_SECONDS = 60


def _safe_int(value, default: int = 0) -> int:
    """
    Convert a pandas/DuckDB aggregate result to int, safely.

    SUM()/AVG() over zero matching rows returns SQL NULL, which pandas
    represents as NaN (numpy.float64) for numeric columns — NOT None.
    `value or 0` looks like it should handle this but doesn't: NaN is
    truthy in Python, so `NaN or 0` evaluates to NaN, not 0, and
    int(NaN) then raises ValueError. Verified directly before fixing
    every call site that had this pattern. pd.isna() correctly detects
    both None and NaN.
    """
    if pd.isna(value):
        return default
    return int(value)


def _safe_float(value):
    """
    Same NaN-safety as _safe_int, but returns None (not a numeric
    default) when there's no value — appropriate for fields like
    avg_duration_ms where "no data" and "zero" mean different things,
    and where `x is not None` alone is NOT sufficient (NaN is not
    None, so a NaN would silently pass through as a value).
    """
    if pd.isna(value):
        return None
    return float(value)


def is_missing(value) -> bool:
    """
    True for both None and NaN — the check every "is this field
    actually empty" decision in this dashboard should use, since
    plain `value is None` and `value or fallback` both fail specifically
    on NaN (verified directly: NaN is not None, and NaN is truthy).
    Exported for use in components.py's row-label formatting.
    """
    return pd.isna(value)


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def enrich_with_instrumentation_gaps(df: pd.DataFrame, text_column: str, source_column: str = "source") -> pd.DataFrame:
    """
    For rows in df where event_action is missing, look up the matching
    instrumentation_gap_catalog entry by computing the SAME normalized
    raw_pattern the catalog was built with (via the ETL's own
    normalized_message_sql/_first_line_sql), then joining on
    (source_system, raw_pattern).

    text_column: which column in df holds the raw text to match
    against (e.g. "message" for API rows, "error_message" for Cobrand
    span_fact rows — span_fact has no "message" column at all).

    source_column: which column in df holds "API"/"Cobrand" — mapped
    to instrumentation_gap_catalog's "API"/"COBRAND" convention.

    Adds four new columns: signature, probable_component,
    recommended_event_action, raw_pattern. All four are NaN for rows
    that already have a real event_action, or where no catalog entry
    matches (e.g. the text was filtered as noise like "HTTP REQUEST"
    and never made it into the catalog at all, or the underlying
    failure has never been seen before). raw_pattern is included
    specifically so callers can tell a GENUINE short signature apart
    from signature_rules.resolve_signature()'s fallback behavior —
    when no rule matches a pattern, signature defaults to raw_pattern
    itself (the full, potentially very long first line of the raw
    message), not to something short. Without comparing against
    raw_pattern, that fallback looks identical to "genuinely
    classified" and dumps the entire raw message into a UI label —
    confirmed directly in production. See components.format_event_label.
    """
    df = df.copy()
    df["signature"] = None
    df["probable_component"] = None
    df["recommended_event_action"] = None
    df["raw_pattern"] = None

    if not table_exists("instrumentation_gap_catalog"):
        return df

    if "event_action" not in df.columns or text_column not in df.columns:
        return df

    needs_lookup = df["event_action"].isna() & df[text_column].notna()
    if not needs_lookup.any():
        return df

    lookup_rows = df.loc[needs_lookup, [text_column, source_column]].copy()
    lookup_rows.columns = ["raw_text", "source"]
    # instrumentation_gap_catalog uses "API"/"COBRAND"; dashboard rows
    # use "API"/"Cobrand" — normalize before joining.
    lookup_rows["source_system"] = lookup_rows["source"].str.upper()

    pattern_expr = normalized_message_sql(_first_line_sql("i.raw_text"))

    with get_connection() as con:
        con.register("gap_lookup_input", lookup_rows)
        try:
            matches = con.execute(f"""
                SELECT
                    i.raw_text,
                    i.source_system,
                    g.signature,
                    g.probable_component,
                    g.recommended_event_action,
                    g.raw_pattern
                FROM gap_lookup_input i
                LEFT JOIN instrumentation_gap_catalog g
                  ON g.source_system = i.source_system
                 AND g.raw_pattern = {pattern_expr}
            """).fetchdf()
        finally:
            con.unregister("gap_lookup_input")

    # Join the lookup results back onto the original rows needing it.
    # Match on (raw_text, source_system) pair — not a true unique key
    # if two different rows have identical text, but that's fine here:
    # identical text should resolve to the identical catalog entry
    # anyway, so duplicate matches are harmless.
    matches = matches.drop_duplicates(subset=["raw_text", "source_system"])
    matches = matches.set_index(["raw_text", "source_system"])

    for idx in df.index[needs_lookup]:
        key = (df.at[idx, text_column], lookup_rows.loc[idx, "source_system"])
        if key in matches.index:
            row = matches.loc[key]
            df.at[idx, "signature"] = row["signature"]
            df.at[idx, "probable_component"] = row["probable_component"]
            df.at[idx, "recommended_event_action"] = row["recommended_event_action"]
            df.at[idx, "raw_pattern"] = row["raw_pattern"]

    return df


# ---------------------------------------------------------------------
# Date range bounds (for populating the sidebar date picker)
# ---------------------------------------------------------------------

@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_date_bounds() -> dict:
    """
    Earliest and latest parseable event_time across both warehouses,
    used to set sensible default bounds on the sidebar date picker.
    Falls back to None if a table is missing or empty rather than
    raising — the page decides how to handle that.
    """
    bounds = {"min_date": None, "max_date": None}

    if table_exists("event_fact_api"):
        df = run_query(f"""
            SELECT
                MIN({CAPABILITY_TIMESTAMP_SQL}) AS min_ts,
                MAX({CAPABILITY_TIMESTAMP_SQL}) AS max_ts
            FROM event_fact_api
        """)
        if not df.empty and df["min_ts"].iloc[0] is not None:
            bounds["min_date"] = df["min_ts"].iloc[0]
            bounds["max_date"] = df["max_ts"].iloc[0]

    if table_exists("span_fact"):
        df = run_query(f"""
            SELECT
                MIN({COBRAND_TIMESTAMP_SQL}) AS min_ts,
                MAX({COBRAND_TIMESTAMP_SQL}) AS max_ts
            FROM span_fact
        """)
        if not df.empty and df["min_ts"].iloc[0] is not None:
            if bounds["min_date"] is None or df["min_ts"].iloc[0] < bounds["min_date"]:
                bounds["min_date"] = df["min_ts"].iloc[0]
            if bounds["max_date"] is None or df["max_ts"].iloc[0] > bounds["max_date"]:
                bounds["max_date"] = df["max_ts"].iloc[0]

    return bounds


# ---------------------------------------------------------------------
# KPIs
# ---------------------------------------------------------------------

@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_kpis(start_date, end_date) -> dict:
    """
    Core Executive Dashboard KPIs, combined across the API and Cobrand
    warehouses where both exist. error_rate/success_rate are computed
    from the combined totals. avg_duration_ms is API-only, since
    span_fact's duration semantics (per-span, not per-request) aren't
    directly comparable to event_fact_api's duration_ms without a
    design decision this MVP doesn't need to make yet.
    """
    api_totals = {"total": 0, "errors": 0, "warnings": 0, "users": 0, "requests": 0, "avg_duration": None}
    cobrand_totals = {"total": 0, "errors": 0}

    if table_exists("event_fact_api"):
        df = run_query(f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN LOWER(level) = 'error' THEN 1 ELSE 0 END) AS errors,
                SUM(CASE WHEN LOWER(level) = 'warn' THEN 1 ELSE 0 END) AS warnings,
                COUNT(DISTINCT user_id) AS users,
                COUNT(DISTINCT request_id) AS requests,
                AVG(duration_ms) AS avg_duration
            FROM event_fact_api
            WHERE {CAPABILITY_TIMESTAMP_SQL} BETWEEN ? AND ?
        """, [start_date, end_date])
        if not df.empty:
            row = df.iloc[0]
            api_totals = {
                "total": _safe_int(row["total"]),
                "errors": _safe_int(row["errors"]),
                "warnings": _safe_int(row["warnings"]),
                "users": _safe_int(row["users"]),
                "requests": _safe_int(row["requests"]),
                "avg_duration": _safe_float(row["avg_duration"]),
            }

    if table_exists("span_fact"):
        df = run_query(f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN error_message IS NOT NULL THEN 1 ELSE 0 END) AS errors
            FROM span_fact
            WHERE {COBRAND_TIMESTAMP_SQL} BETWEEN ? AND ?
        """, [start_date, end_date])
        if not df.empty:
            row = df.iloc[0]
            cobrand_totals = {
                "total": _safe_int(row["total"]),
                "errors": _safe_int(row["errors"]),
            }

    total_events = api_totals["total"] + cobrand_totals["total"]
    total_errors = api_totals["errors"] + cobrand_totals["errors"]
    error_rate = (total_errors / total_events * 100) if total_events else 0.0

    return {
        "total_events": total_events,
        "errors": total_errors,
        "warnings": api_totals["warnings"],
        "unique_users": api_totals["users"],
        "requests": api_totals["requests"],
        "error_rate": error_rate,
        "success_rate": 100.0 - error_rate,
        "avg_duration_ms": api_totals["avg_duration"],
    }


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_structured_event_rate(start_date, end_date) -> dict:
    """
    % of API events that carry a real event_action, vs. logged with
    none at all (the exact rows instrumentation_gap_catalog exists to
    catch). Deliberately named and computed differently from
    capability_catalog's "coverage" KPI (Capability Coverage page) —
    that one measures how much of the KNOWN, STRUCTURED event set has
    been human-classified. This one measures whether an event was
    structured in the first place. Reusing the word "coverage" for
    both would conflate two different questions.
    """
    if not table_exists("event_fact_api"):
        return {"total": 0, "structured": 0, "unstructured": 0, "structured_rate": 0.0}

    df = run_query(f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN event_action IS NOT NULL THEN 1 ELSE 0 END) AS structured
        FROM event_fact_api
        WHERE {CAPABILITY_TIMESTAMP_SQL} BETWEEN ? AND ?
    """, [start_date, end_date])

    if df.empty:
        return {"total": 0, "structured": 0, "unstructured": 0, "structured_rate": 0.0}

    total = _safe_int(df["total"].iloc[0])
    structured = _safe_int(df["structured"].iloc[0])

    return {
        "total": total,
        "structured": structured,
        "unstructured": total - structured,
        "structured_rate": (structured / total * 100) if total else 0.0,
    }


# ---------------------------------------------------------------------
# Time series
# ---------------------------------------------------------------------

@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_events_over_time(start_date, end_date) -> pd.DataFrame:
    """Daily event counts (API only — the richer, higher-volume side)."""
    if not table_exists("event_fact_api"):
        return pd.DataFrame(columns=["day", "count"])

    return run_query(f"""
        SELECT
            CAST({CAPABILITY_TIMESTAMP_SQL} AS DATE) AS day,
            COUNT(*) AS count
        FROM event_fact_api
        WHERE {CAPABILITY_TIMESTAMP_SQL} BETWEEN ? AND ?
        GROUP BY day
        ORDER BY day
    """, [start_date, end_date])


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_errors_over_time(start_date, end_date) -> pd.DataFrame:
    """Daily error counts (API only)."""
    if not table_exists("event_fact_api"):
        return pd.DataFrame(columns=["day", "count"])

    return run_query(f"""
        SELECT
            CAST({CAPABILITY_TIMESTAMP_SQL} AS DATE) AS day,
            COUNT(*) AS count
        FROM event_fact_api
        WHERE {CAPABILITY_TIMESTAMP_SQL} BETWEEN ? AND ?
          AND LOWER(level) = 'error'
        GROUP BY day
        ORDER BY day
    """, [start_date, end_date])


# ---------------------------------------------------------------------
# Top-N breakdowns
# ---------------------------------------------------------------------

@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_top_services(start_date, end_date, limit: int = 10) -> pd.DataFrame:
    if not table_exists("event_fact_api"):
        return pd.DataFrame(columns=["service_name", "count"])

    return run_query(f"""
        SELECT service_name, COUNT(*) AS count
        FROM event_fact_api
        WHERE {CAPABILITY_TIMESTAMP_SQL} BETWEEN ? AND ?
          AND service_name IS NOT NULL
        GROUP BY service_name
        ORDER BY count DESC
        LIMIT {int(limit)}
    """, [start_date, end_date])


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_top_event_prefixes(limit: int = 10) -> pd.DataFrame:
    """
    From capability_catalog — already aggregated by the ETL, so this
    is NOT date-filtered (event_count there is an all-time total, not
    scoped to the dashboard's selected range). Shown as an all-time
    view alongside the date-filtered charts; a date-scoped version
    would need a fresh GROUP BY event_fact_api, deferred until there's
    a concrete need for it.
    """
    if not table_exists("capability_catalog"):
        return pd.DataFrame(columns=["event_prefix", "event_count"])

    return run_query(f"""
        SELECT event_prefix, event_count
        FROM capability_catalog
        WHERE event_count IS NOT NULL
        ORDER BY event_count DESC
        LIMIT {int(limit)}
    """)


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_top_errors(start_date, end_date, limit: int = 10) -> pd.DataFrame:
    """Top raw error messages from the API warehouse (structured events with level=error)."""
    if not table_exists("event_fact_api"):
        return pd.DataFrame(columns=["error_message", "count"])

    return run_query(f"""
        SELECT error_message, COUNT(*) AS count
        FROM event_fact_api
        WHERE {CAPABILITY_TIMESTAMP_SQL} BETWEEN ? AND ?
          AND LOWER(level) = 'error'
          AND error_message IS NOT NULL
        GROUP BY error_message
        ORDER BY count DESC
        LIMIT {int(limit)}
    """, [start_date, end_date])


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_top_users_with_errors(start_date, end_date, limit: int = 10) -> pd.DataFrame:
    if not table_exists("event_fact_api"):
        return pd.DataFrame(columns=["user_id", "error_count"])

    return run_query(f"""
        SELECT user_id, COUNT(*) AS error_count
        FROM event_fact_api
        WHERE {CAPABILITY_TIMESTAMP_SQL} BETWEEN ? AND ?
          AND LOWER(level) = 'error'
          AND user_id IS NOT NULL
        GROUP BY user_id
        ORDER BY error_count DESC
        LIMIT {int(limit)}
    """, [start_date, end_date])


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_top_platforms(start_date, end_date, limit: int = 10) -> pd.DataFrame:
    if not table_exists("event_fact_api"):
        return pd.DataFrame(columns=["platform_name", "count"])

    return run_query(f"""
        SELECT platform_name, COUNT(*) AS count
        FROM event_fact_api
        WHERE {CAPABILITY_TIMESTAMP_SQL} BETWEEN ? AND ?
          AND platform_name IS NOT NULL
        GROUP BY platform_name
        ORDER BY count DESC
        LIMIT {int(limit)}
    """, [start_date, end_date])


# ---------------------------------------------------------------------
# Failure Explorer
# ---------------------------------------------------------------------

@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_combined_failures_over_time(start_date, end_date) -> pd.DataFrame:
    """Daily failure counts, API + Cobrand combined, one row per (day, source)."""
    frames = []

    if table_exists("event_fact_api"):
        df = run_query(f"""
            SELECT CAST({CAPABILITY_TIMESTAMP_SQL} AS DATE) AS day, 'API' AS source, COUNT(*) AS count
            FROM event_fact_api
            WHERE {CAPABILITY_TIMESTAMP_SQL} BETWEEN ? AND ? AND LOWER(level) = 'error'
            GROUP BY day
        """, [start_date, end_date])
        frames.append(df)

    if table_exists("span_fact"):
        df = run_query(f"""
            SELECT CAST({COBRAND_TIMESTAMP_SQL} AS DATE) AS day, 'Cobrand' AS source, COUNT(*) AS count
            FROM span_fact
            WHERE {COBRAND_TIMESTAMP_SQL} BETWEEN ? AND ? AND error_message IS NOT NULL
            GROUP BY day
        """, [start_date, end_date])
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["day", "source", "count"])

    return pd.concat(frames, ignore_index=True).sort_values("day")


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_failure_distribution_by_source(start_date, end_date) -> pd.DataFrame:
    """Total failures split by source (API vs Cobrand) — feeds a simple pie/bar."""
    df = get_combined_failures_over_time(start_date, end_date)
    if df.empty:
        return pd.DataFrame(columns=["source", "count"])
    return df.groupby("source", as_index=False)["count"].sum()


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_top_failed_events(start_date, end_date, limit: int = 10) -> pd.DataFrame:
    """Which event_action values are failing most (API side — Cobrand's error rows
    often lack event_action, since they're standalone/unstructured, not spans)."""
    if not table_exists("event_fact_api"):
        return pd.DataFrame(columns=["event_action", "count"])

    return run_query(f"""
        SELECT event_action, COUNT(*) AS count
        FROM event_fact_api
        WHERE {CAPABILITY_TIMESTAMP_SQL} BETWEEN ? AND ?
          AND LOWER(level) = 'error'
          AND event_action IS NOT NULL
        GROUP BY event_action
        ORDER BY count DESC
        LIMIT {int(limit)}
    """, [start_date, end_date])


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_top_planners_with_errors(start_date, end_date, limit: int = 10) -> pd.DataFrame:
    if not table_exists("event_fact_api"):
        return pd.DataFrame(columns=["planner_id", "error_count"])

    return run_query(f"""
        SELECT planner_id, COUNT(*) AS error_count
        FROM event_fact_api
        WHERE {CAPABILITY_TIMESTAMP_SQL} BETWEEN ? AND ?
          AND LOWER(level) = 'error'
          AND planner_id IS NOT NULL
        GROUP BY planner_id
        ORDER BY error_count DESC
        LIMIT {int(limit)}
    """, [start_date, end_date])


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_failure_table(start_date, end_date, search: str = "", limit: int = 5000) -> pd.DataFrame:
    """
    Combined, expandable failure table (API + Cobrand). search matches
    against message/error text (case-insensitive substring), applied
    the same way on both sides. `limit` is a SAFETY CAP on how much
    data gets fetched from the database, not the number of rows shown
    on screen at once — the page itself paginates the returned
    DataFrame (see pages/2_Failure_Explorer.py), since rendering
    thousands of expandable Streamlit widgets simultaneously is a
    browser-performance problem independent of how fast this query is.
    """
    frames = []
    like_param = f"%{search}%" if search else "%"

    if table_exists("event_fact_api"):
        df = run_query(f"""
            SELECT
                'API' AS source,
                {CAPABILITY_TIMESTAMP_SQL} AS event_time,
                event_action,
                service_name,
                user_id,
                planner_id,
                request_id,
                message,
                error_message
            FROM event_fact_api
            WHERE {CAPABILITY_TIMESTAMP_SQL} BETWEEN ? AND ?
              AND LOWER(level) = 'error'
              AND (error_message ILIKE ? OR message ILIKE ?)
            ORDER BY {CAPABILITY_TIMESTAMP_SQL} DESC
            LIMIT {int(limit)}
        """, [start_date, end_date, like_param, like_param])
        frames.append(df)

    if table_exists("span_fact"):
        df = run_query(f"""
            SELECT
                'Cobrand' AS source,
                {COBRAND_TIMESTAMP_SQL} AS event_time,
                event_action,
                source_file AS service_name,
                user_id,
                CAST(NULL AS BIGINT) AS planner_id,
                request_id,
                CAST(NULL AS VARCHAR) AS message,
                error_message
            FROM span_fact
            WHERE {COBRAND_TIMESTAMP_SQL} BETWEEN ? AND ?
              AND error_message IS NOT NULL
              AND error_message ILIKE ?
            ORDER BY {COBRAND_TIMESTAMP_SQL} DESC
            LIMIT {int(limit)}
        """, [start_date, end_date, like_param])
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=[
            "source", "event_time", "event_action", "service_name",
            "user_id", "planner_id", "request_id", "message", "error_message",
        ])

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values("event_time", ascending=False).head(limit)

    # message (API) is the same field instrumentation_gap_catalog was
    # built from; error_message is the best available substitute for
    # Cobrand rows (span_fact has no message column at all).
    combined["match_text"] = combined["message"].fillna(combined["error_message"])
    return enrich_with_instrumentation_gaps(combined, text_column="match_text")


# ---------------------------------------------------------------------
# User Investigation
# ---------------------------------------------------------------------

@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_user_summary(user_id) -> dict:
    """High-level counts for one user, across both warehouses."""
    summary = {
        "api_events": 0, "api_errors": 0, "platforms": [],
        "planner_ids": [], "cobrand_events": 0, "cobrand_errors": 0,
        "first_seen": None, "last_seen": None,
    }

    if table_exists("event_fact_api"):
        df = run_query(f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN LOWER(level) = 'error' THEN 1 ELSE 0 END) AS errors,
                MIN({CAPABILITY_TIMESTAMP_SQL}) AS first_seen,
                MAX({CAPABILITY_TIMESTAMP_SQL}) AS last_seen
            FROM event_fact_api
            WHERE user_id = ?
        """, [user_id])
        if not df.empty and df["total"].iloc[0]:
            row = df.iloc[0]
            summary["api_events"] = _safe_int(row["total"])
            summary["api_errors"] = _safe_int(row["errors"])
            summary["first_seen"] = row["first_seen"]
            summary["last_seen"] = row["last_seen"]

        platforms_df = run_query("""
            SELECT DISTINCT platform_name FROM event_fact_api
            WHERE user_id = ? AND platform_name IS NOT NULL
        """, [user_id])
        summary["platforms"] = platforms_df["platform_name"].tolist()

        planners_df = run_query("""
            SELECT DISTINCT planner_id FROM event_fact_api
            WHERE user_id = ? AND planner_id IS NOT NULL
        """, [user_id])
        summary["planner_ids"] = planners_df["planner_id"].tolist()

    if table_exists("span_fact"):
        df = run_query("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN error_message IS NOT NULL THEN 1 ELSE 0 END) AS errors
            FROM span_fact
            WHERE user_id = ?
        """, [user_id])
        if not df.empty:
            summary["cobrand_events"] = _safe_int(df["total"].iloc[0])
            summary["cobrand_errors"] = _safe_int(df["errors"].iloc[0])

    return summary


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_user_timeline(user_id, limit: int = 200) -> pd.DataFrame:
    """
    Chronological activity feed for one user, API + Cobrand combined.
    Not date-range-filtered by design — a user investigation usually
    starts from "show me everything about this person," with the
    analyst narrowing down from there, rather than needing to already
    know which date range matters.
    """
    frames = []

    if table_exists("event_fact_api"):
        df = run_query(f"""
            SELECT
                'API' AS source,
                {CAPABILITY_TIMESTAMP_SQL} AS event_time,
                event_action,
                level,
                service_name,
                request_id,
                message,
                error_message
            FROM event_fact_api
            WHERE user_id = ?
            ORDER BY {CAPABILITY_TIMESTAMP_SQL} DESC
            LIMIT {int(limit)}
        """, [user_id])
        frames.append(df)

    if table_exists("span_fact"):
        df = run_query(f"""
            SELECT
                'Cobrand' AS source,
                {COBRAND_TIMESTAMP_SQL} AS event_time,
                event_action,
                CAST(NULL AS VARCHAR) AS level,
                source_file AS service_name,
                request_id,
                CAST(NULL AS VARCHAR) AS message,
                error_message
            FROM span_fact
            WHERE user_id = ?
            ORDER BY {COBRAND_TIMESTAMP_SQL} DESC
            LIMIT {int(limit)}
        """, [user_id])
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=[
            "source", "event_time", "event_action", "level",
            "service_name", "request_id", "message", "error_message",
        ])

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values("event_time", ascending=False).head(limit)
    combined["match_text"] = combined["message"].fillna(combined["error_message"])
    return enrich_with_instrumentation_gaps(combined, text_column="match_text")


# ---------------------------------------------------------------------
# Workflow Explorer
# ---------------------------------------------------------------------
#
# IMPORTANT SCOPE NOTE: this is deliberately NOT a reconstructed
# "workflow" model (no Gantt chart, no cross-request correlation, no
# computed end-to-end duration). That would require a purpose-built
# workflow_fact table correlating events across request_id/planner_id
# boundaries — a real design question (does one request_id map
# cleanly to one workflow? do retries get new IDs?) that hasn't been
# investigated against real data yet. Building that speculatively here
# risks exactly the kind of wrong-assumption bug this project keeps
# finding the hard way. What IS solid: a plain chronological listing
# of every event matching a given ID, which is still genuinely useful
# for tracing "what happened, in order" — just not a validated
# end-to-end workflow model yet.

@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_workflow_events(id_type: str, id_value, limit: int = 500) -> pd.DataFrame:
    """
    id_type: one of "request_id", "planner_id", "user_id".
    Returns every matching row from event_fact_api (and span_fact, for
    request_id/user_id — span_fact has no planner_id column) in
    chronological order.
    """
    if id_type not in ("request_id", "planner_id", "user_id"):
        raise ValueError(f"Unsupported id_type: {id_type}")

    frames = []

    if table_exists("event_fact_api"):
        df = run_query(f"""
            SELECT
                'API' AS source,
                {CAPABILITY_TIMESTAMP_SQL} AS event_time,
                event_action,
                method_name,
                level,
                CAST(NULL AS VARCHAR) AS result_status,
                duration_ms,
                message,
                error_message
            FROM event_fact_api
            WHERE {id_type} = ?
            ORDER BY {CAPABILITY_TIMESTAMP_SQL}
            LIMIT {int(limit)}
        """, [id_value])
        frames.append(df)

    if id_type in ("request_id", "user_id") and table_exists("span_fact"):
        df = run_query(f"""
            SELECT
                'Cobrand' AS source,
                {COBRAND_TIMESTAMP_SQL} AS event_time,
                event_action,
                method_name,
                CAST(NULL AS VARCHAR) AS level,
                result_status,
                duration_ms,
                CAST(NULL AS VARCHAR) AS message,
                error_message
            FROM span_fact
            WHERE {id_type} = ?
            ORDER BY {COBRAND_TIMESTAMP_SQL}
            LIMIT {int(limit)}
        """, [id_value])
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=[
            "source", "event_time", "event_action", "method_name",
            "level", "result_status", "duration_ms", "message", "error_message",
        ])

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values("event_time")
    combined["match_text"] = combined["message"].fillna(combined["error_message"])
    return enrich_with_instrumentation_gaps(combined, text_column="match_text")


# ---------------------------------------------------------------------
# Capability Coverage
# ---------------------------------------------------------------------

@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_capability_coverage_kpis() -> dict:
    if not table_exists("capability_catalog"):
        return {"total": 0, "classified": 0, "pending": 0, "deprecated": 0, "ignored": 0, "coverage_pct": 0.0}

    df = run_query("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN classification_status = 'Classified' THEN 1 ELSE 0 END) AS classified,
            SUM(CASE WHEN classification_status = 'Pending' OR classification_status IS NULL THEN 1 ELSE 0 END) AS pending,
            SUM(CASE WHEN classification_status = 'Deprecated' THEN 1 ELSE 0 END) AS deprecated,
            SUM(CASE WHEN classification_status = 'Ignored' THEN 1 ELSE 0 END) AS ignored
        FROM capability_catalog
    """)

    if df.empty:
        return {"total": 0, "classified": 0, "pending": 0, "deprecated": 0, "ignored": 0, "coverage_pct": 0.0}

    row = df.iloc[0]
    total = _safe_int(row["total"])
    pending = _safe_int(row["pending"])
    coverage_pct = ((total - pending) / total * 100) if total else 0.0

    return {
        "total": total,
        "classified": _safe_int(row["classified"]),
        "pending": pending,
        "deprecated": _safe_int(row["deprecated"]),
        "ignored": _safe_int(row["ignored"]),
        "coverage_pct": coverage_pct,
    }


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_capability_catalog_table(status_filter: str = "All", search: str = "") -> pd.DataFrame:
    if not table_exists("capability_catalog"):
        return pd.DataFrame()

    where_clauses = ["1=1"]
    params = []

    if status_filter != "All":
        if status_filter == "Pending":
            where_clauses.append("(classification_status = 'Pending' OR classification_status IS NULL)")
        else:
            where_clauses.append("classification_status = ?")
            params.append(status_filter)

    if search:
        where_clauses.append("(event_prefix ILIKE ? OR sample_event ILIKE ? OR capability ILIKE ?)")
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

    where_sql = " AND ".join(where_clauses)

    return run_query(f"""
        SELECT
            event_prefix, capability, subsystem, classification_status,
            event_count, first_seen, last_seen, sample_event, sample_service
        FROM capability_catalog
        WHERE {where_sql}
        ORDER BY event_count DESC NULLS LAST
    """, params)


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_recent_capability_discoveries(limit: int = 10) -> pd.DataFrame:
    if not table_exists("capability_catalog"):
        return pd.DataFrame()

    return run_query(f"""
        SELECT event_prefix, first_seen, event_count, classification_status
        FROM capability_catalog
        ORDER BY first_seen DESC NULLS LAST
        LIMIT {int(limit)}
    """)


# ---------------------------------------------------------------------
# Instrumentation Gaps
# ---------------------------------------------------------------------

@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_instrumentation_gap_kpis() -> dict:
    if not table_exists("instrumentation_gap_catalog"):
        return {"total": 0, "pending": 0, "total_occurrences": 0}

    df = run_query("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN classification_status = 'Pending' OR classification_status IS NULL THEN 1 ELSE 0 END) AS pending,
            SUM(occurrence_count) AS total_occurrences
        FROM instrumentation_gap_catalog
    """)

    if df.empty:
        return {"total": 0, "pending": 0, "total_occurrences": 0}

    row = df.iloc[0]
    return {
        "total": _safe_int(row["total"]),
        "pending": _safe_int(row["pending"]),
        "total_occurrences": _safe_int(row["total_occurrences"]),
    }


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_instrumentation_gap_table(source_system: str = "All", search: str = "") -> pd.DataFrame:
    """
    NOTE: instrumentation_gap_catalog tracks recurring UNSTRUCTURED
    messages (ones with no event.action at all) — it does not track
    which structured events are separately missing user.id/request.id/
    planner.id. That's a different, not-yet-built kind of check (it
    would need its own ETL query over event_fact_api, not this
    catalog); this table only shows what the catalog actually contains.
    """
    if not table_exists("instrumentation_gap_catalog"):
        return pd.DataFrame()

    where_clauses = ["1=1"]
    params = []

    if source_system != "All":
        where_clauses.append("source_system = ?")
        params.append(source_system)

    if search:
        where_clauses.append("(raw_pattern ILIKE ? OR signature ILIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])

    where_sql = " AND ".join(where_clauses)

    return run_query(f"""
        SELECT
            signature, raw_pattern, source_system, occurrence_count,
            last_seen, classification_status, probable_component,
            recommended_event_action
        FROM instrumentation_gap_catalog
        WHERE {where_sql}
        ORDER BY occurrence_count DESC NULLS LAST
    """, params)


# ---------------------------------------------------------------------
# Raw Log Search
# ---------------------------------------------------------------------

def _find_matching_raw_patterns(search: str) -> list:
    """
    If `search` matches a signature (or the raw_pattern itself) in
    instrumentation_gap_catalog, return the distinct raw_pattern
    values so callers can also find raw log lines that NORMALIZE to
    one of those patterns — not just lines that literally contain the
    search text. This is what makes searching a resolved signature
    like "VIDEO_MAX_FPS_TOO_LOW" actually find anything: that string
    never appears in the raw log message itself (the real message says
    something like "The Max FPS found from the video files does not
    meet the minimum value..."), only in the catalog it resolved to.
    """
    if not search or not table_exists("instrumentation_gap_catalog"):
        return []

    df = run_query("""
        SELECT DISTINCT raw_pattern
        FROM instrumentation_gap_catalog
        WHERE signature ILIKE ? OR raw_pattern ILIKE ?
    """, [f"%{search}%", f"%{search}%"])

    return df["raw_pattern"].dropna().tolist()


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def search_raw_logs(
    search: str = "",
    user_id=None,
    request_id: str = "",
    planner_id=None,
    source: str = "Both",
    limit: int = 5000,
) -> pd.DataFrame:
    """
    Search across both warehouses. Any blank/None filter is skipped.

    Searches message, error_message, error_stack, AND event_action —
    error_stack (full stack trace text) was a real gap until now: it
    exists as a real column on every source table but was never
    searched, meaning a term that only appears deep in a stack trace
    (not in the shorter message/error_message fields) was invisible
    to this search regardless of how the term was typed.

    This searches the already-parsed fact tables, not the archived raw
    JSON — searching archived Parquet directly would need its own
    tool, deferred until there's a concrete need to search data that's
    already been deleted from these tables (there isn't one yet —
    nothing is deleted from event_fact_api/span_fact/cobrand_event_fact,
    only the source .log files are).

    `limit` is a safety cap on data fetched, not rows rendered at once
    — see pages/7_Raw_Log_Search.py for pagination.
    """
    frames = []
    like_param = f"%{search}%" if search else None

    # If the search term matches a known signature/pattern, also catch
    # raw log lines that normalize to it, even though the search text
    # itself never appears verbatim in those lines.
    matching_patterns = _find_matching_raw_patterns(search) if search else []

    if source in ("Both", "API") and table_exists("event_fact_api"):
        where_clauses = ["1=1"]
        params = []
        if like_param:
            text_clause = "(message ILIKE ? OR error_message ILIKE ? OR error_stack ILIKE ? OR event_action ILIKE ?)"
            text_params = [like_param, like_param, like_param, like_param]
            if matching_patterns:
                pattern_expr = normalized_message_sql(_first_line_sql("message"))
                placeholders = ", ".join(["?"] * len(matching_patterns))
                text_clause = f"({text_clause} OR {pattern_expr} IN ({placeholders}))"
                text_params = text_params + list(matching_patterns)
            where_clauses.append(text_clause)
            params.extend(text_params)
        if user_id:
            where_clauses.append("user_id = ?")
            params.append(user_id)
        if request_id:
            where_clauses.append("request_id = ?")
            params.append(request_id)
        if planner_id:
            where_clauses.append("planner_id = ?")
            params.append(planner_id)

        where_sql = " AND ".join(where_clauses)
        df = run_query(f"""
            SELECT
                'API' AS source,
                {CAPABILITY_TIMESTAMP_SQL} AS event_time,
                event_action, level, service_name, user_id,
                request_id, planner_id, message, error_message, error_stack
            FROM event_fact_api
            WHERE {where_sql}
            ORDER BY {CAPABILITY_TIMESTAMP_SQL} DESC
            LIMIT {int(limit)}
        """, params)
        frames.append(df)

    if source in ("Both", "Cobrand") and table_exists("span_fact") and not planner_id:
        where_clauses = ["1=1"]
        params = []
        if like_param:
            text_clause = "(error_message ILIKE ? OR error_stack ILIKE ? OR event_action ILIKE ?)"
            text_params = [like_param, like_param, like_param]
            if matching_patterns:
                pattern_expr = normalized_message_sql(_first_line_sql("error_message"))
                placeholders = ", ".join(["?"] * len(matching_patterns))
                text_clause = f"({text_clause} OR {pattern_expr} IN ({placeholders}))"
                text_params = text_params + list(matching_patterns)
            where_clauses.append(text_clause)
            params.extend(text_params)
        if user_id:
            where_clauses.append("user_id = ?")
            params.append(user_id)
        if request_id:
            where_clauses.append("request_id = ?")
            params.append(request_id)

        where_sql = " AND ".join(where_clauses)
        df = run_query(f"""
            SELECT
                'Cobrand' AS source,
                {COBRAND_TIMESTAMP_SQL} AS event_time,
                event_action,
                CAST(NULL AS VARCHAR) AS level,
                source_file AS service_name,
                user_id, request_id,
                CAST(NULL AS BIGINT) AS planner_id,
                CAST(NULL AS VARCHAR) AS message,
                error_message, error_stack
            FROM span_fact
            WHERE {where_sql}
            ORDER BY {COBRAND_TIMESTAMP_SQL} DESC
            LIMIT {int(limit)}
        """, params)
        frames.append(df)

    # cobrand_event_fact holds STANDALONE Cobrand events (no trace) —
    # a genuinely different table from span_fact. This is specifically
    # where instrumentation_gap_catalog's Cobrand-side patterns come
    # from (see etl/cobrand_loader.py's sync wiring), so a signature
    # search that only checked span_fact would silently miss anything
    # that never got wrapped inside a trace — confirmed directly:
    # searching a known signature came back with 0 results because the
    # underlying message lived here, not in span_fact.
    if source in ("Both", "Cobrand") and table_exists("cobrand_event_fact") and not planner_id:
        where_clauses = ["1=1"]
        params = []
        if like_param:
            text_clause = "(message ILIKE ? OR error_message ILIKE ? OR error_stack ILIKE ? OR event_action ILIKE ?)"
            text_params = [like_param, like_param, like_param, like_param]
            if matching_patterns:
                pattern_expr = normalized_message_sql(_first_line_sql("message"))
                placeholders = ", ".join(["?"] * len(matching_patterns))
                text_clause = f"({text_clause} OR {pattern_expr} IN ({placeholders}))"
                text_params = text_params + list(matching_patterns)
            where_clauses.append(text_clause)
            params.extend(text_params)
        if user_id:
            where_clauses.append("TRY_CAST(user_id AS BIGINT) = ?")
            params.append(user_id)
        if request_id:
            where_clauses.append("request_id = ?")
            params.append(request_id)

        where_sql = " AND ".join(where_clauses)
        df = run_query(f"""
            SELECT
                'Cobrand' AS source,
                {COBRAND_TIMESTAMP_SQL} AS event_time,
                event_action,
                level,
                source_file AS service_name,
                TRY_CAST(user_id AS BIGINT) AS user_id,
                request_id,
                CAST(NULL AS BIGINT) AS planner_id,
                message,
                error_message, error_stack
            FROM cobrand_event_fact
            WHERE {where_sql}
            ORDER BY {COBRAND_TIMESTAMP_SQL} DESC
            LIMIT {int(limit)}
        """, params)
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=[
            "source", "event_time", "event_action", "level", "service_name",
            "user_id", "request_id", "planner_id", "message", "error_message", "error_stack",
        ])

    combined = pd.concat(frames, ignore_index=True)
    return combined.sort_values("event_time", ascending=False).head(limit)


# ---------------------------------------------------------------------
# Platform Health
# ---------------------------------------------------------------------
#
# SCOPE NOTE: the original spec asked for one card per named subsystem
# (API, Cobrand, Posting, Scheduler, Authentication, WhatsApp,
# LinkedIn, Facebook, Twitter). There is no "subsystem" column in the
# real schema that maps cleanly to that list. What DOES exist and is
# real: service_name (event_fact_api) and platform_name (event_fact_api,
# covers Twitter/LinkedIn/etc. for posting-related events), plus a
# separate API-vs-Cobrand split from the warehouses themselves. This
# builds health cards from those real columns rather than inventing
# a subsystem taxonomy that isn't backed by data.

@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_service_health(start_date, end_date) -> pd.DataFrame:
    if not table_exists("event_fact_api"):
        return pd.DataFrame(columns=["service_name", "requests", "errors", "error_rate", "avg_duration_ms"])

    return run_query(f"""
        SELECT
            service_name,
            COUNT(*) AS requests,
            CAST(SUM(CASE WHEN LOWER(level) = 'error' THEN 1 ELSE 0 END) AS INTEGER) AS errors,
            ROUND(100.0 * SUM(CASE WHEN LOWER(level) = 'error' THEN 1 ELSE 0 END) / COUNT(*), 2) AS error_rate,
            ROUND(AVG(duration_ms), 1) AS avg_duration_ms
        FROM event_fact_api
        WHERE {CAPABILITY_TIMESTAMP_SQL} BETWEEN ? AND ?
          AND service_name IS NOT NULL
        GROUP BY service_name
        ORDER BY requests DESC
    """, [start_date, end_date])


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_platform_health(start_date, end_date) -> pd.DataFrame:
    if not table_exists("event_fact_api"):
        return pd.DataFrame(columns=["platform_name", "requests", "errors", "error_rate"])

    return run_query(f"""
        SELECT
            platform_name,
            COUNT(*) AS requests,
            CAST(SUM(CASE WHEN LOWER(level) = 'error' THEN 1 ELSE 0 END) AS INTEGER) AS errors,
            ROUND(100.0 * SUM(CASE WHEN LOWER(level) = 'error' THEN 1 ELSE 0 END) / COUNT(*), 2) AS error_rate
        FROM event_fact_api
        WHERE {CAPABILITY_TIMESTAMP_SQL} BETWEEN ? AND ?
          AND platform_name IS NOT NULL
        GROUP BY platform_name
        ORDER BY requests DESC
    """, [start_date, end_date])


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def get_cobrand_health(start_date, end_date) -> dict:
    if not table_exists("span_fact"):
        return {"requests": 0, "errors": 0, "error_rate": 0.0}

    df = run_query(f"""
        SELECT
            COUNT(*) AS requests,
            SUM(CASE WHEN error_message IS NOT NULL THEN 1 ELSE 0 END) AS errors
        FROM span_fact
        WHERE {COBRAND_TIMESTAMP_SQL} BETWEEN ? AND ?
    """, [start_date, end_date])

    if df.empty or not df["requests"].iloc[0]:
        return {"requests": 0, "errors": 0, "error_rate": 0.0}

    requests = int(df["requests"].iloc[0])
    errors = _safe_int(df["errors"].iloc[0])
    return {
        "requests": requests,
        "errors": errors,
        "error_rate": (errors / requests * 100) if requests else 0.0,
    }
