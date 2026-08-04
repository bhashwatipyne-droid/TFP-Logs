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
    structured = queries.get_structured_event_rate(start_datetime, end_datetime)
except WarehouseBusyError:
    st.warning("The warehouse is currently being refreshed. Please try again in a few seconds.")
    st.stop()

metric_row([
    {"label": "Total Events", "value": format_count(kpis["total_events"])},
    {"label": "Errors", "value": format_count(kpis["errors"])},
    {"label": "Warnings", "value": format_count(kpis["warnings"])},
    {"label": "Unique Users", "value": format_count(kpis["unique_users"])},
])

metric_row([
    {"label": "Requests", "value": format_count(kpis["requests"])},
    {"label": "Error Rate", "value": format_percent(kpis["error_rate"])},
    {"label": "Success Rate", "value": format_percent(kpis["success_rate"])},
    {"label": "Avg Duration", "value": format_ms(kpis["avg_duration_ms"])},
])

metric_row([
    {"label": "Structured Event Rate", "value": format_percent(structured["structured_rate"])},
    {"label": "Structured Events", "value": format_count(structured["structured"])},
    {"label": "Unstructured Events", "value": format_count(structured["unstructured"])},
])

st.caption(
    "Errors combine event_fact_api (level = 'error') and span_fact "
    "(error_message IS NOT NULL). Avg Duration is API-only. "
    "Structured Event Rate is the share of API events with a real "
    "event_action (API only) — distinct from Capability Coverage's "
    "'coverage', which measures human classification of the KNOWN "
    "structured event set, not whether events are structured at all."
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
