"""
pages/7_Raw_Log_Search.py

Search across both warehouses by message text, User ID, Request ID,
or Planner ID. Download matching rows as CSV.

SCOPE NOTE: this searches the already-parsed fact tables
(event_fact_api / span_fact), not the archived raw JSON in Parquet.
Nothing is deleted from those fact tables (only the source .log files
are, once archived), so this covers everything the warehouse has ever
ingested — a separate archive-search tool would only be needed if
there were a reason to search data that no longer exists in the fact
tables, which isn't currently the case.
"""

import streamlit as st

from styles import inject_dark_theme
from components import render_warehouse_status_badge
from database import WarehouseBusyError
import queries

st.set_page_config(page_title="Raw Log Search — Logs360", layout="wide")
inject_dark_theme()
render_warehouse_status_badge()

st.title("Raw Log Search")

col1, col2 = st.columns([2, 1])
with col1:
    search = st.text_input("Search message / error text", "")
with col2:
    source = st.selectbox("Source", options=["Both", "API", "Cobrand"])

col3, col4, col5 = st.columns(3)
with col3:
    user_id_input = st.text_input("User ID", "")
with col4:
    request_id_input = st.text_input("Request ID", "")
with col5:
    planner_id_input = st.text_input("Planner ID", "")

if not any([search, user_id_input, request_id_input, planner_id_input]):
    st.info("Enter at least one search term or filter above.")
    st.stop()

user_id = None
if user_id_input:
    try:
        user_id = int(user_id_input)
    except ValueError:
        st.error("User ID must be numeric.")
        st.stop()

planner_id = None
if planner_id_input:
    try:
        planner_id = int(planner_id_input)
    except ValueError:
        st.error("Planner ID must be numeric.")
        st.stop()

try:
    results = queries.search_raw_logs(
        search=search,
        user_id=user_id,
        request_id=request_id_input,
        planner_id=planner_id,
        source=source,
    )
except WarehouseBusyError:
    st.warning("The warehouse is currently being refreshed. Please try again in a few seconds.")
    st.stop()

st.caption(f"{len(results)} result(s), capped at 200.")

if results.empty:
    st.caption("No matches.")
else:
    st.dataframe(results, use_container_width=True, hide_index=True)

    st.download_button(
        "Download results as CSV",
        data=results.to_csv(index=False).encode("utf-8"),
        file_name="logs360_search_results.csv",
        mime="text/csv",
    )
