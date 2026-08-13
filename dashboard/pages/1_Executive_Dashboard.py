"""
pages/1_Executive_Dashboard.py

The Executive Dashboard: platform-wide KPIs and trends. First page of
Logs360, built and verified against the real schema before any other
page — see queries.py's docstring for why event_time handling in
particular needed care here.
"""

from datetime import datetime, timedelta

import streamlit as st

from styles import inject_dark_theme
from components import (
    metric_row,
    format_count,
    format_percent,
    format_ms,
    render_warehouse_status_badge,
)
from charts import dual_line_chart, bar_chart
from database import WarehouseBusyError
import queries

st.set_page_config(page_title="Executive Dashboard — Logs360", layout="wide")
inject_dark_theme()
render_warehouse_status_badge()

st.title("Executive Dashboard")

# ---------------------------------------------------------------------
# Date range filter
# ---------------------------------------------------------------------

try:
    bounds = queries.get_date_bounds()
except WarehouseBusyError:
    st.warning("The warehouse is currently being refreshed. Please try again in a few seconds.")
    st.stop()

default_end = bounds["max_date"] or datetime.now()
default_start = bounds["min_date"] or (default_end - timedelta(days=30))

st.sidebar.subheader("Filters")
date_range = st.sidebar.date_input(
    "Date range",
    value=(default_start.date() if hasattr(default_start, "date") else default_start,
           default_end.date() if hasattr(default_end, "date") else default_end),
    min_value=default_start.date() if hasattr(default_start, "date") else default_start,
    max_value=default_end.date() if hasattr(default_end, "date") else default_end,
)

if len(date_range) != 2:
    st.info("Select a start and end date to continue.")
    st.stop()

start_date, end_date = date_range
# Include the full end day (date_input gives a date, not a datetime —
# without this, anything after midnight on the end date is excluded).
end_datetime = datetime.combine(end_date, datetime.max.time())
start_datetime = datetime.combine(start_date, datetime.min.time())

# ---------------------------------------------------------------------
# KPIs
# ---------------------------------------------------------------------

try:
    kpis = queries.get_kpis(start_datetime, end_datetime)
    breakdown = queries.get_failure_breakdown(start_datetime, end_datetime)
except WarehouseBusyError:
    st.warning("The warehouse is currently being refreshed. Please try again in a few seconds.")
    st.stop()

# Day-over-day comparison: the LAST day of the selected range vs the
# day immediately before it — works sensibly whether the selected
# range is a single day or a long historical window, since it always
# compares the most recent day shown against its predecessor.
today_start = datetime.combine(end_date, datetime.min.time())
today_end = datetime.combine(end_date, datetime.max.time())
yesterday = end_date - timedelta(days=1)
yesterday_start = datetime.combine(yesterday, datetime.min.time())
yesterday_end = datetime.combine(yesterday, datetime.max.time())

try:
    today_kpis = queries.get_kpis(today_start, today_end)
    today_breakdown = queries.get_failure_breakdown(today_start, today_end)
    yesterday_kpis = queries.get_kpis(yesterday_start, yesterday_end)
    yesterday_breakdown = queries.get_failure_breakdown(yesterday_start, yesterday_end)

    def _delta(today_val, yesterday_val):
        d = today_val - yesterday_val
        return f"{d:+,}" if d != 0 else None

    errors_delta = _delta(today_kpis["errors"], yesterday_kpis["errors"])
    warnings_delta = _delta(today_kpis["warnings"], yesterday_kpis["warnings"])
    cobrand_delta = _delta(today_breakdown["cobrand_failures"], yesterday_breakdown["cobrand_failures"])
    posting_delta = _delta(today_breakdown["posting_failures"], yesterday_breakdown["posting_failures"])
except WarehouseBusyError:
    errors_delta = warnings_delta = cobrand_delta = posting_delta = None

metric_row([
    {"label": "Errors", "value": format_count(kpis["errors"]), "delta": errors_delta, "delta_color": "inverse"},
    {"label": "Warnings", "value": format_count(kpis["warnings"]), "delta": warnings_delta, "delta_color": "inverse"},
    {"label": "Cobrand Failures", "value": format_count(breakdown["cobrand_failures"]), "delta": cobrand_delta, "delta_color": "inverse"},
    {"label": "Posting Failures", "value": format_count(breakdown["posting_failures"]), "delta": posting_delta, "delta_color": "inverse"},
])

st.caption(
    "Errors combine event_fact_api (level = 'error') and span_fact "
    "(error_message IS NOT NULL). Cobrand Failures is the Cobrand-only "
    "portion of Errors. Posting Failures is event_action LIKE "
    "'posting_job%' AND level = 'error' (API). Arrows compare the last "
    "day of the selected range against the day before it."
)

st.divider()

# ---------------------------------------------------------------------
# Trend chart
# ---------------------------------------------------------------------

events_df = queries.get_events_over_time(start_datetime, end_datetime)
errors_df = queries.get_errors_over_time(start_datetime, end_datetime)

if events_df.empty:
    st.info("No API events found in the selected date range.")
else:
    st.plotly_chart(
        dual_line_chart(events_df, errors_df),
        use_container_width=True,
    )

st.divider()

# ---------------------------------------------------------------------
# Top-N breakdowns
# ---------------------------------------------------------------------

col1, col2 = st.columns(2)

with col1:
    st.subheader("Top Services")
    df = queries.get_top_services(start_datetime, end_datetime)
    if df.empty:
        st.caption("No data for the selected range.")
    else:
        st.plotly_chart(
            bar_chart(df, x="service_name", y="count"),
            use_container_width=True,
        )

with col2:
    st.subheader("Top Platforms")
    df = queries.get_top_platforms(start_datetime, end_datetime)
    if df.empty:
        st.caption("No platform data for the selected range.")
    else:
        st.plotly_chart(
            bar_chart(df, x="platform_name", y="count"),
            use_container_width=True,
        )

col3, col4 = st.columns(2)

with col3:
    st.subheader("Top Event Prefixes")
    st.caption("All-time totals from the Event Registry (capability_catalog) — not date-filtered.")
    df = queries.get_top_event_prefixes()
    if df.empty:
        st.caption("No data available.")
    else:
        st.plotly_chart(
            bar_chart(df, x="event_prefix", y="event_count"),
            use_container_width=True,
        )

with col4:
    st.subheader("Top Users with Errors")
    df = queries.get_top_users_with_errors(start_datetime, end_datetime)
    if df.empty:
        st.caption("No errors in the selected range.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

st.subheader("Top Errors")
df = queries.get_top_errors(start_datetime, end_datetime)
if df.empty:
    st.caption("No structured errors in the selected range.")
else:
    st.dataframe(df, use_container_width=True, hide_index=True)
