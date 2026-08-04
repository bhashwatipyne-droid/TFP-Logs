"""
charts.py

Reusable Plotly chart helpers, styled to match the dashboard's dark
theme. Every chart function takes a DataFrame and column names, not a
specific page's query result shape — so the same helper works for any
page's time-series or top-N breakdown.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

_PLOT_BGCOLOR = "#0e1117"
_PAPER_BGCOLOR = "#161a23"
_GRID_COLOR = "#262b36"
_FONT_COLOR = "#c9d1d9"
_ACCENT = "#58a6ff"
_ERROR_COLOR = "#f85149"


def _apply_dark_layout(fig: go.Figure, height: int = 300) -> go.Figure:
    fig.update_layout(
        plot_bgcolor=_PLOT_BGCOLOR,
        paper_bgcolor=_PAPER_BGCOLOR,
        font_color=_FONT_COLOR,
        height=height,
        margin=dict(l=10, r=10, t=30, b=10),
        xaxis=dict(gridcolor=_GRID_COLOR, showgrid=True),
        yaxis=dict(gridcolor=_GRID_COLOR, showgrid=True),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
    )
    return fig


def line_chart(df: pd.DataFrame, x: str, y: str, title: str = "", color: str = _ACCENT) -> go.Figure:
    """Simple time-series line chart."""
    fig = px.line(df, x=x, y=y, title=title)
    fig.update_traces(line_color=color, line_width=2)
    return _apply_dark_layout(fig)


def bar_chart(df: pd.DataFrame, x: str, y: str, title: str = "", horizontal: bool = True) -> go.Figure:
    """Top-N style bar chart. Horizontal by default (reads better for long labels)."""
    if horizontal:
        fig = px.bar(df, x=y, y=x, orientation="h", title=title)
        fig.update_layout(yaxis=dict(autorange="reversed"))
    else:
        fig = px.bar(df, x=x, y=y, title=title)

    fig.update_traces(marker_color=_ACCENT)
    return _apply_dark_layout(fig)


def dual_line_chart(
    events_df: pd.DataFrame,
    errors_df: pd.DataFrame,
    x: str = "day",
    y: str = "count",
    title: str = "Events & Errors Over Time",
) -> go.Figure:
    """Overlay events and errors on one time axis for quick visual correlation."""
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=events_df[x], y=events_df[y],
        mode="lines", name="Events",
        line=dict(color=_ACCENT, width=2),
    ))

    fig.add_trace(go.Scatter(
        x=errors_df[x], y=errors_df[y],
        mode="lines", name="Errors",
        line=dict(color=_ERROR_COLOR, width=2),
    ))

    fig.update_layout(title=title)
    return _apply_dark_layout(fig, height=340)
