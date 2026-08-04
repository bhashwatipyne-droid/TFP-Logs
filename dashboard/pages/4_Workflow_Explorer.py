"""
pages/4_Workflow_Explorer.py

Filter by Request ID, Planner ID, or User ID and see every matching
event in chronological order.

SCOPE NOTE (shown in the UI too, not just here): this is NOT a
reconstructed workflow model. No Gantt chart, no computed end-to-end
duration, no cross-request correlation. Building that would require a
purpose-built workflow_fact table and a real investigation into
whether request_id/planner_id map cleanly to "one workflow" — a design
question that hasn't been validated against real data yet. What's
here is solid and useful on its own: a plain, correctly-ordered
timeline for a given ID.
"""

from datetime import datetime

import streamlit as st

from styles import inject_dark_theme
from components import metric_row, format_count, render_warehouse_status_badge, format_event_label
from database import WarehouseBusyError
import queries
from queries import is_missing

st.set_page_config(page_title="Workflow Explorer — Logs360", layout="wide")
inject_dark_theme()
render_warehouse_status_badge()

st.title("Workflow Explorer")
st.info(
    "This shows a chronological event sequence for a given ID — it does not "
    "compute end-to-end duration or reconstruct a validated workflow model "
    "across retries. Treat it as a timeline, not a workflow diagram.",
    icon=None,
)

id_type_label = st.radio(
    "Filter by",
    options=["Request ID", "Planner ID", "User ID"],
    horizontal=True,
)

id_type_map = {
    "Request ID": "request_id",
    "Planner ID": "planner_id",
    "User ID": "user_id",
}
id_type = id_type_map[id_type_label]

id_value_input = st.text_input(f"Enter {id_type_label}", "")

if not id_value_input:
    st.stop()

if id_type in ("planner_id", "user_id"):
    try:
        id_value = int(id_value_input)
    except ValueError:
        st.error(f"{id_type_label} must be numeric.")
        st.stop()
else:
    id_value = id_value_input

try:
    events = queries.get_workflow_events(id_type, id_value)
except WarehouseBusyError:
    st.warning("The warehouse is currently being refreshed. Please try again in a few seconds.")
    st.stop()
except ValueError as ex:
    st.error(str(ex))
    st.stop()

if events.empty:
    st.warning(f"No events found for {id_type_label} = {id_value}.")
    st.stop()

failure_count = events["error_message"].notna().sum()

metric_row([
    {"label": "Total Events", "value": format_count(len(events))},
    {"label": "Failures", "value": format_count(failure_count)},
    {"label": "First Event", "value": str(events["event_time"].min())},
    {"label": "Last Event", "value": str(events["event_time"].max())},
])

st.divider()
st.subheader("Chronological Sequence")

for i, (_, row) in enumerate(events.iterrows(), start=1):
    marker = "[ERROR]" if not is_missing(row.get("error_message")) else "—"
    header = f"{i}. {marker} {row['event_time']} · {row['source']} · {format_event_label(row)}"
    with st.expander(header):
        st.json(row.to_dict())