"""
pages/2_Failure_Explorer.py

Investigate failures across both warehouses: timeline, distribution,
top failing events/messages/users/planners, and an expandable table.
"""

from datetime import datetime, timedelta, time as time_type

import streamlit as st

from styles import inject_dark_theme
from components import metric_row, format_count, render_warehouse_status_badge, format_event_label
from charts import bar_chart
from database import WarehouseBusyError
import queries
import plotly.express as px

st.set_page_config(page_title="Failure Explorer — Logs360", layout="wide")
inject_dark_theme()
render_warehouse_status_badge()

st.title("Failure Explorer")

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
)

time_col1, time_col2 = st.sidebar.columns(2)
with time_col1:
    start_time = st.time_input("Start time", value=time_type(0, 0))
with time_col2:
    end_time = st.time_input("End time", value=time_type(23, 59, 59))

search = st.sidebar.text_input("Search error text", "")

if len(date_range) != 2:
    st.info("Select a start and end date to continue.")
    st.stop()

start_date, end_date = date_range
start_datetime = datetime.combine(start_date, start_time)
end_datetime = datetime.combine(end_date, end_time)

if start_datetime > end_datetime:
    st.error("Start date/time must be before end date/time.")
    st.stop()

try:
    dist_df = queries.get_failure_distribution_by_source(start_datetime, end_datetime)
    timeline_df = queries.get_combined_failures_over_time(start_datetime, end_datetime)
except WarehouseBusyError:
    st.warning("The warehouse is currently being refreshed. Please try again in a few seconds.")
    st.stop()

metric_row([
    {"label": "Total Failures", "value": format_count(dist_df["count"].sum() if not dist_df.empty else 0)},
    {"label": "API Failures", "value": format_count(dist_df.loc[dist_df["source"] == "API", "count"].sum() if not dist_df.empty else 0)},
    {"label": "Cobrand Failures", "value": format_count(dist_df.loc[dist_df["source"] == "Cobrand", "count"].sum() if not dist_df.empty else 0)},
])

st.divider()

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Failure Timeline")
    if timeline_df.empty:
        st.caption("No failures in the selected range.")
    else:
        fig = px.line(timeline_df, x="day", y="count", color="source", markers=True)
        fig.update_layout(
            plot_bgcolor="#0e1117", paper_bgcolor="#161a23", font_color="#c9d1d9",
            height=300, margin=dict(l=10, r=10, t=30, b=10),
        )
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("By Source")
    if dist_df.empty:
        st.caption("No data.")
    else:
        st.plotly_chart(bar_chart(dist_df, x="source", y="count", horizontal=False), use_container_width=True)

st.divider()

col3, col4 = st.columns(2)

with col3:
    st.subheader("Top Failed Events")
    df = queries.get_top_failed_events(start_datetime, end_datetime)
    if df.empty:
        st.caption("No failed events (API side) in this range.")
    else:
        st.plotly_chart(bar_chart(df, x="event_action", y="count"), use_container_width=True)

with col4:
    st.subheader("Most Affected Planners")
    df = queries.get_top_planners_with_errors(start_datetime, end_datetime)
    if df.empty:
        st.caption("No planner-linked errors in this range.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

col5, col6 = st.columns(2)

with col5:
    st.subheader("Top Error Messages")
    df = queries.get_top_errors(start_datetime, end_datetime)
    if df.empty:
        st.caption("No structured error messages in this range.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

with col6:
    st.subheader("Most Affected Users")
    df = queries.get_top_users_with_errors(start_datetime, end_datetime)
    if df.empty:
        st.caption("No user-linked errors in this range.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

st.divider()

st.subheader("Failure Table")
st.caption("Every row is expandable. Capped at 200 rows for investigation — use Raw Log Search to export more.")

failure_table = queries.get_failure_table(start_datetime, end_datetime, search=search)

if failure_table.empty:
    st.caption("No matching failures.")
else:
    for _, row in failure_table.iterrows():
        header = f"{row['event_time']} · {row['source']} · {format_event_label(row)}"
        with st.expander(header):
            st.json(row.to_dict())
