"""
Data Quality Dashboard tab — visual pass/warn/fail health summary.
Calls health_check.run_all_checks() and renders results as a
Win Forms-style report card with color-coded status indicators.
"""

import streamlit as st
from admin_dashboard.health_check import run_all_checks


STATUS_ICON = {
    "PASS":  ("✅", "#006600", "#efffef"),
    "WARN":  ("⚠️",  "#886600", "#fffaec"),
    "FAIL":  ("❌", "#990000", "#fff0f0"),
    "ERROR": ("💥", "#660066", "#fff0ff"),
}


def _status_row(result: dict):
    icon, color, bg = STATUS_ICON.get(result["status"], ("?", "#000", "#fff"))
    value_str = (
        f"{result['value']:,}" if isinstance(result["value"], int) else str(result.get("value", ""))
    )
    threshold_str = str(result.get("threshold") or "")
    st.markdown(
        f"""
        <div style="background:{bg};border-left:4px solid {color};
                    padding:4px 8px;margin:2px 0;font-size:11px;
                    font-family:'Courier New',monospace;">
          {icon} &nbsp; <b>{result['name']}</b> &nbsp;
          <span style="color:{color};font-weight:bold;">{result['status']}</span>
          &nbsp;|&nbsp; {value_str}
          {'&nbsp;<span style="color:#666;">(threshold: ' + threshold_str + ')</span>' if threshold_str else ''}
          <br>&nbsp;&nbsp;&nbsp;&nbsp;
          <span style="color:#444;">{result.get('message','')}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _summary_bar(summary: dict):
    pass_n  = summary.get("PASS",  0)
    warn_n  = summary.get("WARN",  0)
    fail_n  = summary.get("FAIL",  0)
    error_n = summary.get("ERROR", 0)
    total   = pass_n + warn_n + fail_n + error_n

    overall = (
        "PASS"  if fail_n == 0 and error_n == 0 and warn_n == 0 else
        "WARN"  if fail_n == 0 and error_n == 0 else
        "FAIL"
    )
    icon, color, bg = STATUS_ICON[overall]

    st.markdown(
        f"""
        <div style="background:{bg};border:2px solid {color};
                    padding:8px 14px;font-size:13px;font-weight:bold;
                    margin-bottom:10px;">
          {icon} Overall: <span style="color:{color};">{overall}</span>
          &nbsp;—&nbsp;
          ✅ {pass_n} pass &nbsp;
          ⚠️ {warn_n} warn &nbsp;
          ❌ {fail_n} fail &nbsp;
          💥 {error_n} error &nbsp;
          <span style="font-weight:normal;font-size:11px;">({total} checks)</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render():
    st.markdown("### Data Quality Dashboard")

    col_run, col_cache, _ = st.columns([2, 2, 8])
    with col_run:
        run_btn = st.button("Run Health Checks", type="primary")
    with col_cache:
        clear_btn = st.button("Clear Cache")

    if clear_btn:
        st.cache_data.clear()
        st.rerun()

    # Store results in session state so they persist across re-runs
    # without re-querying Snowflake every time.
    if run_btn or "health_results" not in st.session_state:
        with st.spinner("Querying Snowflake..."):
            try:
                st.session_state.health_results = run_all_checks()
                st.session_state.health_error = None
            except Exception as e:
                st.session_state.health_error = str(e)
                st.session_state.health_results = None

    if st.session_state.get("health_error"):
        st.error(f"Health check failed: {st.session_state.health_error}")
        return

    results = st.session_state.get("health_results")
    if not results:
        st.info("Click **Run Health Checks** to query Snowflake.")
        return

    _summary_bar(results["summary"])

    # ── Sections ───────────────────────────────────────────────────
    sections = [
        ("Snowflake Connection", [results["connection"]]),
        ("RAW Tables — Row Counts", results["raw_tables"]),
        ("Prices Coverage",          results["prices"]),
        ("Fundamentals Coverage",    results["fundamentals"]),
        ("Mart Tables",              results["mart_tables"]),
        ("FRED Catalog",             results["fred_catalog"]),
    ]

    for section_title, items in sections:
        with st.expander(section_title, expanded=True):
            if not items:
                st.info("No results.")
                continue
            for item in items:
                if isinstance(item, list):
                    for sub in item:
                        _status_row(sub)
                else:
                    _status_row(item)
