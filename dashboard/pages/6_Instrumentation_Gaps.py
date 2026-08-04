"""
pages/6_Instrumentation_Gaps.py

instrumentation_gap_catalog: recurring unstructured log messages that
don't yet have a proper event.action.

SCOPE NOTE (also shown in the UI): this catalog tracks recurring
messages with no event.action at all. It does NOT separately track
which structured events are missing user.id/request.id/planner.id —
that's a different check that would need its own query over
event_fact_api, not something this catalog currently computes.
"""

import streamlit as st

from styles import inject_dark_theme
from components import metric_row, format_count, render_warehouse_status_badge
from database import WarehouseBusyError
import queries

st.set_page_config(page_title="Instrumentation Gaps — Logs360", layout="wide")
inject_dark_theme()
render_warehouse_status_badge()

st.title("Instrumentation Gaps")
st.caption(
    "Recurring log messages with no structured event.action — candidates for "
    "becoming proper structured events."
)
st.info(
    "This shows unstructured message patterns only. It does not separately "
    "flag structured events missing user.id/request.id/planner.id.",
)

try:
    kpis = queries.get_instrumentation_gap_kpis()
except WarehouseBusyError:
    st.warning("The warehouse is currently being refreshed. Please try again in a few seconds.")
    st.stop()

metric_row([
    {"label": "Total Patterns", "value": format_count(kpis["total"])},
    {"label": "Pending Review", "value": format_count(kpis["pending"])},
    {"label": "Total Occurrences", "value": format_count(kpis["total_occurrences"])},
])

st.divider()

col1, col2 = st.columns([1, 3])
with col1:
    source_filter = st.selectbox("Source", options=["All", "API", "COBRAND"])
with col2:
    search = st.text_input("Search pattern / signature", "")

table = queries.get_instrumentation_gap_table(source_system=source_filter, search=search)

if table.empty:
    st.caption("No matching patterns.")
else:
    st.dataframe(table, use_container_width=True, hide_index=True)
