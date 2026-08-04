"""
pages/9_Settings.py

Read-only visibility into warehouse status and ETL configuration.
Deliberately read-only for now — this page shows what's true, it
doesn't let you change pipeline configuration from the dashboard
(AUTO_DELETE_JSON and similar belong in etl/config.py, edited
directly or via its documented environment variable, not through a
UI control that could silently diverge from what refresh_logs.py
actually uses on its own separate schedule).
"""

import streamlit as st

from styles import inject_dark_theme
from components import render_warehouse_status_badge, format_count
from database import run_query, WarehouseBusyError, DB_PATH

st.set_page_config(page_title="Settings — Logs360", layout="wide")
inject_dark_theme()
render_warehouse_status_badge()

st.title("Settings")

st.subheader("Warehouse")
st.write(f"Database file: `{DB_PATH}`")

try:
    tables = run_query("SHOW TABLES")
except WarehouseBusyError:
    st.warning("The warehouse is currently being refreshed. Please try again in a few seconds.")
    st.stop()

st.subheader("Tables")

rows = []
for table_name in tables["name"]:
    try:
        count_df = run_query(f'SELECT COUNT(*) AS n FROM "{table_name}"')
        count = int(count_df["n"].iloc[0])
    except Exception:
        count = None
    rows.append({"table": table_name, "rows": count})

import pandas as pd
st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

st.divider()

st.subheader("ETL Configuration")
try:
    from etl import config as etl_config
    st.write(f"AUTO_DELETE_JSON: `{etl_config.AUTO_DELETE_JSON}`")
    st.caption(
        "Set via etl/config.py or the AUTO_DELETE_JSON environment variable. "
        "This dashboard does not change it — edit refresh_logs.py's environment "
        "directly."
    )
except ImportError:
    st.caption("etl.config not importable from this environment.")
