"""
pages/3_User_Investigation.py

Search by User ID, get a chronological activity feed across both
warehouses, and a summary of platforms/planners/failures for that user.
"""

import streamlit as st

from styles import inject_dark_theme
from components import metric_row, format_count, format_percent, render_warehouse_status_badge, format_event_label
from database import WarehouseBusyError
import queries
from queries import is_missing

st.set_page_config(page_title="User Investigation — Logs360", layout="wide")
inject_dark_theme()
render_warehouse_status_badge()

st.title("User Investigation")
st.caption("Search a User ID to see everything about them across both warehouses.")

user_id_input = st.text_input("User ID", "")

if not user_id_input:
    st.info("Enter a User ID above to begin.")
    st.stop()

try:
    user_id = int(user_id_input)
except ValueError:
    st.error("User ID must be numeric.")
    st.stop()

try:
    summary = queries.get_user_summary(user_id)
    timeline = queries.get_user_timeline(user_id)
except WarehouseBusyError:
    st.warning("The warehouse is currently being refreshed. Please try again in a few seconds.")
    st.stop()

total_events = summary["api_events"] + summary["cobrand_events"]

if total_events == 0:
    st.warning(f"No activity found for user {user_id}.")
    st.stop()

total_errors = summary["api_errors"] + summary["cobrand_errors"]
error_rate = (total_errors / total_events * 100) if total_events else 0.0

metric_row([
    {"label": "Total Events", "value": format_count(total_events)},
    {"label": "API Events", "value": format_count(summary["api_events"])},
    {"label": "Cobrand Events", "value": format_count(summary["cobrand_events"])},
    {"label": "Error Rate", "value": format_percent(error_rate)},
])

col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Platforms")
    if summary["platforms"]:
        for p in summary["platforms"]:
            st.write(f"• {p}")
    else:
        st.caption("No platform activity recorded.")

with col2:
    st.subheader("Planner IDs")
    if summary["planner_ids"]:
        for pid in summary["planner_ids"]:
            st.write(f"• {pid}")
    else:
        st.caption("No planner activity recorded.")

with col3:
    st.subheader("Activity Window")
    if summary["first_seen"] is not None:
        st.write(f"First seen: {summary['first_seen']}")
        st.write(f"Last seen: {summary['last_seen']}")
    else:
        st.caption("No API timestamp data.")

st.divider()

st.subheader("Chronological Activity Feed")
st.caption("Most recent 200 events, API + Cobrand combined.")

if timeline.empty:
    st.caption("No events found.")
else:
    for _, row in timeline.iterrows():
        marker = "[ERROR]" if not is_missing(row.get("error_message")) else "—"
        header = f"{marker} {row['event_time']} · {row['source']} · {format_event_label(row)}"
        with st.expander(header):
            st.json(row.to_dict())