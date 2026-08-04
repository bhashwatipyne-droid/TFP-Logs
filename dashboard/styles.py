"""
styles.py

Minimal dark-mode CSS for the Logs360 dashboard. Kept intentionally
small for this first page — expand as more pages reveal what's
actually needed, rather than pre-building a full design system before
there's more than one page to prove it against.
"""

import streamlit as st

DARK_CSS = """
<style>
    .stApp {
        background-color: #0e1117;
    }

    [data-testid="stMetric"] {
        background-color: #161a23;
        border: 1px solid #262b36;
        border-radius: 10px;
        padding: 16px 20px;
    }

    [data-testid="stMetricLabel"] {
        color: #9aa4b2;
        font-size: 0.85rem;
    }

    [data-testid="stMetricValue"] {
        color: #f0f2f6;
    }

    section[data-testid="stSidebar"] {
        background-color: #12151c;
        border-right: 1px solid #262b36;
    }

    h1, h2, h3 {
        color: #f0f2f6;
        font-weight: 600;
    }

    .logs360-caption {
        color: #6b7280;
        font-size: 0.8rem;
    }

    .logs360-status-ok {
        color: #3fb950;
        font-weight: 600;
    }

    .logs360-status-warn {
        color: #d29922;
        font-weight: 600;
    }

    .logs360-status-error {
        color: #f85149;
        font-weight: 600;
    }
</style>
"""


def inject_dark_theme():
    st.markdown(DARK_CSS, unsafe_allow_html=True)
