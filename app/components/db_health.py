"""
Database Health tab — pipeline row counts, coverage checks, and infrastructure status.

Security model
--------------
This tab surfaces internal pipeline metrics (Snowflake row counts, coverage
thresholds, staleness checks). It is intended for the data engineering team,
not end users. Three env vars control access:

  HEALTH_TAB_ENABLED      Set to "true" to show tab content. Default: "false".
                          Leave unset on Community Cloud unless you also set a
                          password (see below). The tab heading still appears in
                          the UI so the tab count is stable — only the content
                          is gated, not the tab itself.

  HEALTH_CHECK_PASSWORD   Optional password prompt before any data is shown.
                          Works with local .env AND with st.secrets on
                          Community Cloud. Never commit this value to source
                          control.

  IS_LOCAL                Set to "true" when running on a developer machine.
                          Gates the Docker / Airflow infrastructure status
                          checks which make localhost subprocess calls that are
                          meaningless — and a potential shell-execution risk —
                          in a cloud deployment.

Community Cloud deployment checklist
-------------------------------------
1. Add SNOWFLAKE_PRIVATE_KEY (full PEM content) to st.secrets.
2. Leave HEALTH_TAB_ENABLED unset to hide the tab internals, OR set it to
   "true" AND set HEALTH_CHECK_PASSWORD to a strong secret.
3. Do NOT set IS_LOCAL — infrastructure checks will be suppressed automatically.
4. Never set AIRFLOW_ADMIN_PASSWORD or IS_LOCAL in Community Cloud secrets.
"""

import os
import subprocess
import requests
import streamlit as st
from datetime import datetime, timezone

from db.snowflake import execute_sql_cached


# ── Feature flags ─────────────────────────────────────────────────────────────

def _flag(name: str, default: bool = False) -> bool:
    return os.environ.get(name, str(default)).lower() == "true"

HEALTH_ENABLED = _flag("HEALTH_TAB_ENABLED", False)
IS_LOCAL       = _flag("IS_LOCAL", False)

# Password gate — if set, a prompt is shown before any data is rendered.
# Use os.environ so it works with both .env (local) and st.secrets (cloud).
HEALTH_PASSWORD = os.environ.get("HEALTH_CHECK_PASSWORD", "")


# ── Thresholds ────────────────────────────────────────────────────────────────

T = {
    "prices_rows":         5_000_000,
    "company_info_rows":   1_500,
    "macro_rows":          100_000,
    "fin_stmt_rows":       100_000,
    "valuation_rows":      1_000,
    "div_rows":            2_000,
    "earnings_rows":       2_000,
    "analyst_rec_rows":    2_000,
    "analyst_pt_rows":     500,
    "fred_releases":       250,
    "fred_series":         500_000,
    "prices_tickers":      1_500,
    "fin_stmt_tickers":    1_200,
    "valuation_tickers":   1_500,
    "mart_prices_tickers": 1_500,
    "mart_fund_tickers":   1_200,
    "mart_val_tickers":    1_500,
    "macro_mart_rows":     150_000,  # 175 series × full history (208k rows after expansion)
    "catalog_stale_days":  45,
}


# ── Check helpers ─────────────────────────────────────────────────────────────

def _result(name, status, value, threshold, message):
    return {
        "name": name, "status": status,
        "value": value, "threshold": threshold, "message": message,
    }


def _check(name, value, threshold, op=">=", fmt=","):
    ok = (value >= threshold) if op == ">=" else (value <= threshold)
    status = "PASS" if ok else ("WARN" if op == "<=" else "FAIL")
    thr_str = f"{op} {threshold:{fmt}}" if fmt else f"{op} {threshold}"
    msg = f"{value:{fmt}} " + ("✓" if ok else f"(expected {thr_str})")
    return _result(name, status, value, thr_str, msg)


# ── Snowflake checks ──────────────────────────────────────────────────────────

def _run_raw_counts():
    sql = """
        SELECT
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.PRICES)                   AS prices,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.COMPANY_INFO)              AS company_info,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.MACRO_INDICATORS)          AS macro_indicators,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.FINANCIAL_STATEMENTS)      AS financial_statements,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.VALUATION_METRICS)         AS valuation_metrics,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.DIVIDENDS_AND_SPLITS)      AS dividends_and_splits,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.EARNINGS_HISTORY)          AS earnings_history,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.ANALYST_RECOMMENDATIONS)   AS analyst_recommendations,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.ANALYST_PRICE_TARGETS)     AS analyst_price_targets,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.FRED_RELEASES)             AS fred_releases,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.FRED_SERIES_CATALOG)       AS fred_series_catalog
    """
    row = execute_sql_cached(sql).iloc[0]
    return [
        _check("RAW · PRICES",                  int(row["prices"]),                  T["prices_rows"]),
        _check("RAW · COMPANY_INFO",             int(row["company_info"]),            T["company_info_rows"]),
        _check("RAW · MACRO_INDICATORS",         int(row["macro_indicators"]),        T["macro_rows"]),
        _check("RAW · FINANCIAL_STATEMENTS",     int(row["financial_statements"]),    T["fin_stmt_rows"]),
        _check("RAW · VALUATION_METRICS",        int(row["valuation_metrics"]),       T["valuation_rows"]),
        _check("RAW · DIVIDENDS_AND_SPLITS",     int(row["dividends_and_splits"]),    T["div_rows"]),
        _check("RAW · EARNINGS_HISTORY",         int(row["earnings_history"]),        T["earnings_rows"]),
        _check("RAW · ANALYST_RECOMMENDATIONS",  int(row["analyst_recommendations"]), T["analyst_rec_rows"]),
        _check("RAW · ANALYST_PRICE_TARGETS",    int(row["analyst_price_targets"]),   T["analyst_pt_rows"]),
        _check("RAW · FRED_RELEASES",            int(row["fred_releases"]),           T["fred_releases"]),
        _check("RAW · FRED_SERIES_CATALOG",      int(row["fred_series_catalog"]),     T["fred_series"]),
    ]


def _run_coverage():
    sql = """
        SELECT
            COUNT(DISTINCT TICKER) AS prices_tickers
        FROM EQUITY_ANALYTICS.RAW.PRICES
    """
    pt = int(execute_sql_cached(sql).iloc[0]["prices_tickers"])

    sql2 = """
        SELECT
            COUNT(DISTINCT TICKER) AS fin_tickers,
            COUNT(DISTINCT TICKER) AS val_tickers
        FROM EQUITY_ANALYTICS.RAW.FINANCIAL_STATEMENTS
    """
    # Valuation and financial statements separate queries to avoid cross-table confusion
    fin_t = int(execute_sql_cached(
        "SELECT COUNT(DISTINCT TICKER) AS n FROM EQUITY_ANALYTICS.RAW.FINANCIAL_STATEMENTS"
    ).iloc[0]["n"])
    val_t = int(execute_sql_cached(
        "SELECT COUNT(DISTINCT TICKER) AS n FROM EQUITY_ANALYTICS.RAW.VALUATION_METRICS"
    ).iloc[0]["n"])

    latest = execute_sql_cached("""
        SELECT MAX(TO_DATE(DATEADD(second, DATE/1000000000, '1970-01-01'))) AS latest
        FROM EQUITY_ANALYTICS.RAW.PRICES
    """).iloc[0]["latest"]

    days_stale = None
    if latest:
        days_stale = (datetime.now(timezone.utc).date() - latest).days

    return [
        _check("Prices · Distinct Tickers",             pt,    T["prices_tickers"]),
        _check("Financial Statements · Distinct Tickers", fin_t, T["fin_stmt_tickers"]),
        _check("Valuation Metrics · Distinct Tickers",  val_t, T["valuation_tickers"]),
        _result(
            "Prices · Latest Date",
            "PASS" if days_stale is not None and days_stale <= 3 else "WARN",
            str(latest), "<= 3 days ago",
            f"{days_stale} day(s) old ({latest})" if latest else "No data",
        ),
    ]


def _run_marts():
    sql = """
        SELECT
            (SELECT COUNT(DISTINCT TICKER) FROM EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES)      AS p_tickers,
            (SELECT COUNT(DISTINCT TICKER) FROM EQUITY_ANALYTICS.MARTS.FACT_FUNDAMENTALS)       AS f_tickers,
            (SELECT COUNT(DISTINCT TICKER) FROM EQUITY_ANALYTICS.MARTS.FACT_VALUATION_SNAPSHOT) AS v_tickers,
            (SELECT COUNT(*)               FROM EQUITY_ANALYTICS.MARTS.FACT_MACRO_READINGS)     AS macro_rows
    """
    row = execute_sql_cached(sql).iloc[0]
    return [
        _check("MART · FACT_DAILY_PRICES Tickers",       int(row["p_tickers"]),  T["mart_prices_tickers"]),
        _check("MART · FACT_FUNDAMENTALS Tickers",        int(row["f_tickers"]),  T["mart_fund_tickers"]),
        _check("MART · FACT_VALUATION_SNAPSHOT Tickers", int(row["v_tickers"]),  T["mart_val_tickers"]),
        _check("MART · FACT_MACRO_READINGS Rows",         int(row["macro_rows"]), T["macro_mart_rows"]),
    ]


def _run_fred_catalog():
    releases = int(execute_sql_cached(
        "SELECT COUNT(*) AS n FROM EQUITY_ANALYTICS.RAW.FRED_RELEASES"
    ).iloc[0]["n"])
    series = int(execute_sql_cached(
        "SELECT COUNT(*) AS n FROM EQUITY_ANALYTICS.RAW.FRED_SERIES_CATALOG"
    ).iloc[0]["n"])
    stale = execute_sql_cached("""
        SELECT DATEDIFF('day', TO_DATE(LAST_ALTERED), CURRENT_DATE()) AS days_old,
               TO_DATE(LAST_ALTERED) AS last_built
        FROM   EQUITY_ANALYTICS.INFORMATION_SCHEMA.TABLES
        WHERE  TABLE_SCHEMA = 'RAW' AND TABLE_NAME = 'FRED_SERIES_CATALOG'
    """)
    results = [
        _check("FRED · Releases",     releases, T["fred_releases"]),
        _check("FRED · Series Catalog", series, T["fred_series"]),
    ]
    if not stale.empty:
        days = int(stale.iloc[0]["days_old"] or 0)
        results.append(_check(
            "FRED · Catalog Freshness", days, T["catalog_stale_days"], op="<=", fmt=""
        ))
    return results


def _run_all_checks():
    """Return dict of check groups + summary counts. Cached in session state."""
    groups = {}
    errors = []

    for label, fn in [
        ("raw_counts", _run_raw_counts),
        ("coverage",   _run_coverage),
        ("marts",      _run_marts),
        ("fred",       _run_fred_catalog),
    ]:
        try:
            groups[label] = fn()
        except Exception as e:
            groups[label] = [_result(label, "ERROR", None, None, str(e))]
            errors.append(str(e))

    all_results = [r for grp in groups.values() for r in grp]
    summary = {"PASS": 0, "WARN": 0, "FAIL": 0, "ERROR": 0}
    for r in all_results:
        summary[r["status"]] = summary.get(r["status"], 0) + 1

    groups["summary"] = summary
    return groups


# ── Infrastructure status (local only) ───────────────────────────────────────

def _docker_status() -> tuple[bool, str]:
    """Check Airflow Docker services. Only called when IS_LOCAL=true."""
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "--services", "--filter", "status=running"],
            capture_output=True, text=True, timeout=8,
        )
        services = {s.strip() for s in result.stdout.strip().splitlines() if s.strip()}
        expected = {"airflow-webserver", "airflow-scheduler", "airflow-db"}
        missing  = expected - services
        if not missing:
            return True, f"All services running"
        return False, f"Missing: {', '.join(sorted(missing))}"
    except FileNotFoundError:
        return False, "docker not found in PATH"
    except Exception as e:
        return False, str(e)


def _airflow_status() -> tuple[bool, str]:
    """Ping Airflow /health endpoint. Only called when IS_LOCAL=true."""
    try:
        r = requests.get("http://localhost:8080/health", timeout=5)
        if r.status_code == 200:
            return True, "Webserver healthy"
        return False, f"HTTP {r.status_code}"
    except Exception as e:
        return False, str(e)[:60]


def _snowflake_status() -> tuple[bool, str]:
    """Quick Snowflake connectivity check via existing connection pool."""
    try:
        execute_sql_cached("SELECT 1")
        return True, "Connected"
    except Exception as e:
        return False, str(e)[:60]


# ── Rendering helpers ─────────────────────────────────────────────────────────

_STATUS_STYLE = {
    "PASS":  ("✅", "#006600", "#f0fff0"),
    "WARN":  ("⚠️",  "#7a5800", "#fffbec"),
    "FAIL":  ("❌", "#990000", "#fff0f0"),
    "ERROR": ("💥", "#660066", "#fdf0ff"),
}


def _render_row(r: dict):
    icon, color, bg = _STATUS_STYLE.get(r["status"], ("?", "#333", "#fff"))
    val  = f"{r['value']:,}" if isinstance(r["value"], int) else str(r.get("value") or "")
    thr  = f"  <span style='color:#888;font-size:0.85em;'>(threshold: {r['threshold']})</span>" \
           if r.get("threshold") else ""
    st.markdown(
        f"<div style='background:{bg};border-left:4px solid {color};"
        f"padding:5px 10px;margin:2px 0;border-radius:2px;font-size:0.88em;'>"
        f"{icon} <b>{r['name']}</b> — "
        f"<span style='color:{color};font-weight:600;'>{r['status']}</span> "
        f"&nbsp;{val}{thr}"
        f"<br><span style='color:#555;padding-left:1.4em;'>{r.get('message','')}</span>"
        f"</div>",
        unsafe_allow_html=True,
    )


def _render_summary(summary: dict):
    p, w, f, e = (summary.get(k, 0) for k in ("PASS","WARN","FAIL","ERROR"))
    overall = "PASS" if (f + e + w) == 0 else "WARN" if (f + e) == 0 else "FAIL"
    icon, color, bg = _STATUS_STYLE[overall]
    st.markdown(
        f"<div style='background:{bg};border:2px solid {color};"
        f"padding:8px 14px;border-radius:3px;font-weight:700;font-size:1em;margin-bottom:12px;'>"
        f"{icon} Overall: <span style='color:{color};'>{overall}</span>"
        f" &nbsp;—&nbsp; ✅ {p} pass &nbsp; ⚠️ {w} warn &nbsp; ❌ {f} fail &nbsp; 💥 {e} error"
        f"</div>",
        unsafe_allow_html=True,
    )


def _render_infra():
    """Render Docker / Airflow / Snowflake connectivity dots."""
    snow_ok, snow_msg = _snowflake_status()

    if IS_LOCAL:
        docker_ok,  docker_msg  = _docker_status()
        airflow_ok, airflow_msg = _airflow_status()
        cols = st.columns([1, 1, 1, 4])
        items = [
            ("Docker",    docker_ok,  docker_msg),
            ("Airflow",   airflow_ok, airflow_msg),
            ("Snowflake", snow_ok,    snow_msg),
        ]
        for col, (label, ok, msg) in zip(cols[:3], items):
            dot = "🟢" if ok else "🔴"
            col.markdown(f"{dot} **{label}**  \n<small>{msg}</small>", unsafe_allow_html=True)
        overall_ok = docker_ok and airflow_ok and snow_ok
    else:
        # Cloud deployment — only Snowflake is meaningful
        col1, col2, _ = st.columns([1, 1, 5])
        dot = "🟢" if snow_ok else "🔴"
        col1.markdown(f"{dot} **Snowflake**  \n<small>{snow_msg}</small>", unsafe_allow_html=True)
        col2.markdown("⬜ **Docker/Airflow**  \n<small>N/A (cloud)</small>", unsafe_allow_html=True)
        overall_ok = snow_ok

    label = "ALL SYSTEMS NOMINAL" if overall_ok else "DEGRADED"
    color = "#006600" if overall_ok else "#990000"
    cols[-1].markdown(
        f"<div style='background:{color};color:#fff;padding:5px 12px;"
        f"font-weight:700;font-size:0.85em;border-radius:3px;display:inline-block;'>"
        f"{label}</div>",
        unsafe_allow_html=True,
    )


# ── Password gate ─────────────────────────────────────────────────────────────

def _check_password() -> bool:
    """
    If HEALTH_CHECK_PASSWORD is set, show a password prompt.
    Returns True if the user may proceed.
    Never stores the password in session state — only a boolean flag.
    """
    if not HEALTH_PASSWORD:
        return True  # No password configured — open access (rely on HEALTH_ENABLED gate)

    if st.session_state.get("_health_authed"):
        return True

    st.markdown("#### 🔒 Health check access")
    pwd = st.text_input("Password", type="password", key="_health_pwd_input",
                        placeholder="Enter health check password")
    if st.button("Unlock", key="_health_unlock"):
        if pwd == HEALTH_PASSWORD:
            st.session_state["_health_authed"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    return False


# ── Main render ───────────────────────────────────────────────────────────────

def render():
    st.markdown("### 🔍 Database Health")

    # ── Feature flag gate ─────────────────────────────────────────────────────
    if not HEALTH_ENABLED:
        st.info(
            "Database health checks are disabled in this deployment.  \n"
            "Set `HEALTH_TAB_ENABLED=true` in your environment to enable them.",
            icon="ℹ️",
        )
        return

    # ── Password gate ─────────────────────────────────────────────────────────
    if not _check_password():
        return

    # ── Infrastructure status ─────────────────────────────────────────────────
    with st.container():
        _render_infra()

    st.markdown("---")

    # ── Health checks ─────────────────────────────────────────────────────────
    col_run, col_clear, _ = st.columns([2, 2, 8])
    run_btn   = col_run.button("▶  Run Checks", type="primary")
    clear_btn = col_clear.button("↺  Clear Cache")

    if clear_btn:
        st.session_state.pop("_health_results", None)
        st.session_state.pop("_health_ts", None)
        st.rerun()

    if run_btn or "_health_results" not in st.session_state:
        with st.spinner("Querying Snowflake…"):
            st.session_state["_health_results"] = _run_all_checks()
            st.session_state["_health_ts"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    results = st.session_state.get("_health_results")
    if not results:
        st.info("Click **▶ Run Checks** to query Snowflake.")
        return

    ts = st.session_state.get("_health_ts", "")
    if ts:
        st.caption(f"Last run: {ts}")

    _render_summary(results["summary"])

    sections = [
        ("RAW Table Row Counts",  results.get("raw_counts", [])),
        ("Coverage & Freshness",  results.get("coverage",   [])),
        ("Mart Tables",           results.get("marts",       [])),
        ("FRED Catalog",          results.get("fred",        [])),
    ]
    for title, items in sections:
        with st.expander(title, expanded=True):
            if not items:
                st.info("No results.")
            else:
                for item in items:
                    _render_row(item)
