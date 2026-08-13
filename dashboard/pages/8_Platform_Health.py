"""
pages/8_Platform_Health.py

SCOPE NOTE (also shown in the UI): the original spec asked for one
card per named subsystem (API, Cobrand, Posting, Scheduler,
Authentication, WhatsApp, LinkedIn, Facebook, Twitter). There is no
"subsystem" column in the real schema that maps to that list. This
page instead builds health cards from what's actually real:
service_name (event_fact_api), platform_name (covers Twitter/LinkedIn/
etc. for posting-related events), and a separate API-vs-Cobrand split
from the two warehouses themselves.
"""

from datetime import datetime, timedelta

import streamlit as st

from styles import inject_dark_theme
from components import metric_row, format_count, format_percent, render_warehouse_status_badge
from database import WarehouseBusyError
import queries

st.set_page_config(page_title="Social Media Platform Health — Logs360", layout="wide")
inject_dark_theme()
render_warehouse_status_badge()

st.title("Social Media Platform Health")
st.info(
    "Built from service_name and platform_name (real columns) rather than a "
    "named subsystem list (Posting/Scheduler/Auth/etc.), which the current "
    "schema doesn't have a clean mapping for.",
)

try:
    bounds = queries.get_date_bounds()
except WarehouseBusyError:
    st.warning("The warehouse is currently being refreshed. Please try again in a few seconds.")
    st.stop()

default_end = bounds["max_date"] or datetime.now()
default_start = bounds["min_date"] or (default_end - timedelta(days=30))

date_range = st.sidebar.date_input(
    "Date range",
    value=(default_start.date() if hasattr(default_start, "date") else default_start,
           default_end.date() if hasattr(default_end, "date") else default_end),
)

if len(date_range) != 2:
    st.stop()

start_date, end_date = date_range
start_datetime = datetime.combine(start_date, datetime.min.time())
end_datetime = datetime.combine(end_date, datetime.max.time())

try:
    cobrand_health = queries.get_cobrand_health(start_datetime, end_datetime)
    service_health = queries.get_service_health(start_datetime, end_datetime)
    platform_health = queries.get_platform_health(start_datetime, end_datetime)
except WarehouseBusyError:
    st.warning("The warehouse is currently being refreshed. Please try again in a few seconds.")
    st.stop()

st.subheader("Cobrand")
metric_row([
    {"label": "Requests", "value": format_count(cobrand_health["requests"])},
    {"label": "Errors", "value": format_count(cobrand_health["errors"])},
    {"label": "Error Rate", "value": format_percent(cobrand_health["error_rate"])},
])

st.divider()

st.subheader("API Services")
if service_health.empty:
    st.caption("No service data in the selected range.")
else:
    cols = st.columns(min(4, len(service_health)))
    for i, (_, row) in enumerate(service_health.iterrows()):
        with cols[i % len(cols)]:
            st.metric(
                label=row["service_name"],
                value=f"{int(row['requests']):,} req",
                delta=f"{row['error_rate']:.1f}% errors",
                delta_color="inverse",
            )
    st.dataframe(service_health, use_container_width=True, hide_index=True)

st.divider()

st.subheader("Platforms")
if platform_health.empty:
    st.caption("No platform data in the selected range.")
else:
    cols = st.columns(min(4, len(platform_health)))
    for i, (_, row) in enumerate(platform_health.iterrows()):
        with cols[i % len(cols)]:
            st.metric(
                label=row["platform_name"],
                value=f"{int(row['requests']):,} req",
                delta=f"{row['error_rate']:.1f}% errors",
                delta_color="inverse",
            )
    st.dataframe(platform_health, use_container_width=True, hide_index=True)
