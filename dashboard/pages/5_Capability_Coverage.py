"""
pages/5_Capability_Coverage.py

The Event Registry (capability_catalog): coverage KPIs, recent
discoveries, and a filterable/sortable table.
"""

import streamlit as st

from styles import inject_dark_theme
from components import metric_row, format_count, format_percent, render_warehouse_status_badge
from database import WarehouseBusyError
import queries

st.set_page_config(page_title="Capability Coverage — Logs360", layout="wide")
inject_dark_theme()
render_warehouse_status_badge()

st.title("Capability Coverage")
st.caption("The Event Registry — every discovered event prefix, and how much of it a human has reviewed.")

try:
    kpis = queries.get_capability_coverage_kpis()
except WarehouseBusyError:
    st.warning("The warehouse is currently being refreshed. Please try again in a few seconds.")
    st.stop()

metric_row([
    {"label": "Total Prefixes", "value": format_count(kpis["total"])},
    {"label": "Coverage", "value": format_percent(kpis["coverage_pct"])},
    {"label": "Pending", "value": format_count(kpis["pending"])},
    {"label": "Classified", "value": format_count(kpis["classified"])},
])

metric_row([
    {"label": "Deprecated", "value": format_count(kpis["deprecated"])},
    {"label": "Ignored", "value": format_count(kpis["ignored"])},
])

st.divider()

st.subheader("Recent Discoveries")
recent = queries.get_recent_capability_discoveries()
if recent.empty:
    st.caption("No prefixes discovered yet.")
else:
    st.dataframe(recent, use_container_width=True, hide_index=True)

st.divider()

st.subheader("Full Catalog")

col1, col2 = st.columns([1, 3])
with col1:
    status_filter = st.selectbox(
        "Classification status",
        options=["All", "Pending", "Classified", "Deprecated", "Ignored"],
    )
with col2:
    search = st.text_input("Search event prefix / capability / sample event", "")

table = queries.get_capability_catalog_table(status_filter=status_filter, search=search)

if table.empty:
    st.caption("No matching prefixes.")
else:
    st.dataframe(table, use_container_width=True, hide_index=True)
