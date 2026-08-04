"""
components.py

Reusable UI building blocks shared across pages.
"""

import pandas as pd
import streamlit as st

from database import get_warehouse_status, WarehouseBusyError


def metric_row(metrics: list[dict]):
    """
    Render a row of st.metric cards.

    metrics: list of dicts like {"label": "Total Events", "value": "1,234", "delta": None}
    """
    cols = st.columns(len(metrics))
    for col, m in zip(cols, metrics):
        with col:
            st.metric(
                label=m["label"],
                value=m["value"],
                delta=m.get("delta"),
            )


def _is_missing(value) -> bool:
    """
    True for both None and NaN. `value is None` and `value or fallback`
    both fail specifically on NaN — verified directly earlier in this
    project (NaN is not None, and NaN is truthy in Python) — which is
    exactly why timeline rows with a missing event_action were
    rendering as the literal string "nan" instead of falling back to
    anything sensible. Every formatter below uses this instead of a
    bare None/truthiness check.
    """
    return pd.isna(value)


def format_count(n) -> str:
    """Format an integer with thousands separators, handling missing values gracefully."""
    if _is_missing(n):
        return "—"
    return f"{int(n):,}"


def format_percent(n) -> str:
    if _is_missing(n):
        return "—"
    return f"{n:.1f}%"


def format_ms(n) -> str:
    if _is_missing(n):
        return "—"
    return f"{n:,.0f} ms"


def format_event_label(row) -> str:
    """
    The header label for one event row in an expandable timeline
    (Failure Explorer, User Investigation, Workflow Explorer).

    row is expected to be a pandas Series with at least: event_action,
    and optionally signature/probable_component (added by
    queries.enrich_with_instrumentation_gaps) and message/error_message.

    Three tiers, in order:
      1. A real event_action -> show it as-is. This is the common case.
      2. No event_action, but the enrichment lookup matched a known
         instrumentation_gap_catalog entry -> show its signature and
         (if known) probable_component. This is the "we've seen this
         exact unstructured failure before and know what it means"
         case — genuinely informative, not a guess.
      3. No event_action and no catalog match -> "Uninstrumented Error"
         if there's error/message text at all (a real failure that's
         simply never been classified yet), or "Unknown Event" if
         there's nothing to go on whatsoever. Both are honest about
         what this is: a gap in instrumentation, not a platform
         failure to alarm on the same way a real error_action would be.

    This directly replaces the old pattern of
        row.get('event_action') or '(no event_action)'
    which rendered as the literal string "nan" for any row with a
    missing event_action, because pandas represents SQL NULL as NaN
    (a float) for these columns, and NaN is truthy — so `or` never
    triggered its fallback. That was a real, confirmed bug, not a
    display preference.
    """
    event_action = row.get("event_action")

    if not _is_missing(event_action):
        return str(event_action)

    signature = row.get("signature")
    probable_component = row.get("probable_component")
    raw_pattern = row.get("raw_pattern")

    # A "signature" that's identical to its own raw_pattern isn't a
    # real classification — it's signature_rules.resolve_signature()'s
    # documented fallback when no rule has matched this pattern yet
    # (it returns the raw pattern text itself, unchanged, rather than
    # None). Treating that as "genuinely classified" was a real,
    # confirmed bug: it dumped an entire multi-line raw log message
    # into a UI header instead of falling back to "Uninstrumented
    # Error" the way a truly unmatched pattern correctly does.
    is_real_signature = (
        not _is_missing(signature)
        and (_is_missing(raw_pattern) or signature != raw_pattern)
    )

    if is_real_signature:
        if not _is_missing(probable_component):
            return f"{signature} ({probable_component})"
        return str(signature)

    has_text = not _is_missing(row.get("message")) or not _is_missing(row.get("error_message"))
    if has_text:
        return "Uninstrumented Error"

    return "Unknown Event"


def render_warehouse_status_badge():
    """
    Small status indicator shown in the sidebar: whether the warehouse
    is currently reachable, and when it was last touched by the ETL.
    Uses get_warehouse_status(), which is cached for 30s so this
    doesn't add a query to every single page interaction.
    """
    try:
        status = get_warehouse_status()
    except WarehouseBusyError:
        st.sidebar.markdown(
            '<span class="logs360-status-warn">● Warehouse refreshing...</span>',
            unsafe_allow_html=True,
        )
        return

    if not status["reachable"]:
        st.sidebar.markdown(
            '<span class="logs360-status-error">● Warehouse unreachable</span>',
            unsafe_allow_html=True,
        )
        return

    last_activity = status["last_activity"]
    label = f"Last refresh: {last_activity}" if last_activity is not None else "No refresh recorded yet"

    st.sidebar.markdown(
        f'<span class="logs360-status-ok">● Warehouse online</span>',
        unsafe_allow_html=True,
    )
    st.sidebar.caption(label)
