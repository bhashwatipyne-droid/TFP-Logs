"""
app.py

Logs360 — entry point. This is the Home page in Streamlit's
multi-page app convention; additional pages live in pages/ and appear
automatically in the sidebar navigation.
"""

import streamlit as st

from styles import inject_dark_theme
from components import render_warehouse_status_badge

st.set_page_config(
    page_title="Logs360",
    page_icon="◆",
    layout="wide",
    initial_sidebar_state="expanded",
)

inject_dark_theme()
render_warehouse_status_badge()

st.title("Logs360")
st.caption("TheFinpedia observability platform")

st.markdown("""
Use the sidebar to navigate:

- **Executive Dashboard** — platform-wide KPIs and trends
- **Failure Explorer** — investigate failures across both warehouses
- **User Investigation** — search by User ID, see full activity history
- **Workflow Explorer** — chronological event sequence for a Request/Planner/User ID
- **Capability Coverage** — the Event Registry and its classification progress
- **Instrumentation Gaps** — recurring unstructured messages awaiting review
- **Raw Log Search** — search across both warehouses, export to CSV
- **Platform Health** — service and platform-level health
- **Settings** — warehouse status and ETL configuration (read-only)
""")
