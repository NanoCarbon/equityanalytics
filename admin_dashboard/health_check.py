"""
Structured version of scripts/db_health_check.py.
Returns results as plain dicts so the dashboard can render them — no print statements.

Each check returns:
    {
        "name": str,
        "status": "PASS" | "WARN" | "FAIL" | "ERROR",
        "value": any,          # actual measured value
        "threshold": any,      # what we expected
        "message": str,        # human-readable one-liner
    }
"""

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import snowflake.connector
from typing import Optional


# ── Thresholds (mirrors scripts/db_health_check.py) ──────────────────────────

EXPECTED_TICKERS          = 1500
MIN_TICKERS_PER_DAY       = 1400
MIN_COMPANY_INFO_ROWS     = 1500
MAX_COMPANY_INFO_NULL_PCT = 5.0
MIN_MACRO_RAW_ROWS        = 5000
MACRO_RAW_MART_RATIO      = 0.35
MIN_FUNDAMENTAL_TICKERS   = 1200
MIN_VALUATION_TICKERS     = 1500
MIN_DIV_ROWS              = 2000
MIN_EARNINGS_ROWS         = 2000
MIN_ANALYST_REC_ROWS      = 2000
MIN_ANALYST_PT_ROWS       = 500
MIN_CATALOG_RELEASES      = 250
MIN_CATALOG_SERIES        = 500_000
MAX_CATALOG_STALE_DAYS    = 45


def _conn():
    from ingestion.load import get_connection
    return get_connection()


def _q(sql: str) -> list:
    """Run a SQL query and return list-of-dicts rows."""
    conn = _conn()
    try:
        cur = conn.cursor(snowflake.connector.DictCursor)
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def _scalar(sql: str):
    rows = _q(sql)
    if rows:
        return list(rows[0].values())[0]
    return None


def _result(name, status, value, threshold, message):
    return {"name": name, "status": status, "value": value,
            "threshold": threshold, "message": message}


def _safe(name, fn):
    try:
        return fn()
    except Exception as e:
        return _result(name, "ERROR", None, None, str(e))


# ── Individual checks ─────────────────────────────────────────────────────────

def check_snowflake_connection() -> dict:
    def _run():
        conn = _conn()
        conn.close()
        return _result("Snowflake Connection", "PASS", "connected", None,
                       "Connection opened and closed successfully")
    return _safe("Snowflake Connection", _run)


def check_raw_tables() -> list[dict]:
    """Row counts for all 11 RAW tables."""
    def _run():
        row = _q("""
            SELECT
                (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.PRICES)                  AS prices,
                (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.COMPANY_INFO)             AS company_info,
                (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.MACRO_INDICATORS)         AS macro_indicators,
                (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.FINANCIAL_STATEMENTS)     AS financial_statements,
                (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.VALUATION_METRICS)        AS valuation_metrics,
                (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.DIVIDENDS_AND_SPLITS)     AS dividends_and_splits,
                (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.EARNINGS_HISTORY)         AS earnings_history,
                (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.ANALYST_RECOMMENDATIONS)  AS analyst_recommendations,
                (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.ANALYST_PRICE_TARGETS)    AS analyst_price_targets,
                (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.FRED_RELEASES)            AS fred_releases,
                (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.FRED_SERIES_CATALOG)      AS fred_series_catalog
        """)[0]

        thresholds = {
            "PRICES":                 (">=", 5_000_000),
            "COMPANY_INFO":           (">=", MIN_COMPANY_INFO_ROWS),
            "MACRO_INDICATORS":       (">=", MIN_MACRO_RAW_ROWS),
            "FINANCIAL_STATEMENTS":   (">=", 100_000),
            "VALUATION_METRICS":      (">=", 1_000),
            "DIVIDENDS_AND_SPLITS":   (">=", MIN_DIV_ROWS),
            "EARNINGS_HISTORY":       (">=", MIN_EARNINGS_ROWS),
            "ANALYST_RECOMMENDATIONS":(">=", MIN_ANALYST_REC_ROWS),
            "ANALYST_PRICE_TARGETS":  (">=", MIN_ANALYST_PT_ROWS),
            "FRED_RELEASES":          (">=", MIN_CATALOG_RELEASES),
            "FRED_SERIES_CATALOG":    (">=", MIN_CATALOG_SERIES),
        }
        col_map = {
            "PRICES": "prices", "COMPANY_INFO": "company_info",
            "MACRO_INDICATORS": "macro_indicators",
            "FINANCIAL_STATEMENTS": "financial_statements",
            "VALUATION_METRICS": "valuation_metrics",
            "DIVIDENDS_AND_SPLITS": "dividends_and_splits",
            "EARNINGS_HISTORY": "earnings_history",
            "ANALYST_RECOMMENDATIONS": "analyst_recommendations",
            "ANALYST_PRICE_TARGETS": "analyst_price_targets",
            "FRED_RELEASES": "fred_releases",
            "FRED_SERIES_CATALOG": "fred_series_catalog",
        }
        results = []
        for table, (op, threshold) in thresholds.items():
            count = int(row[col_map[table]])
            ok = count >= threshold
            results.append(_result(
                f"RAW.{table}",
                "PASS" if ok else "FAIL",
                count,
                f"{op} {threshold:,}",
                f"{count:,} rows" + ("" if ok else f" (expected {op} {threshold:,})"),
            ))
        return results

    try:
        return _run()
    except Exception as e:
        return [_result("RAW Tables", "ERROR", None, None, str(e))]


def check_prices_coverage() -> list[dict]:
    def _run():
        results = []

        # Ticker count
        n = int(_scalar("SELECT COUNT(DISTINCT TICKER) FROM EQUITY_ANALYTICS.RAW.PRICES") or 0)
        ok = n >= EXPECTED_TICKERS
        results.append(_result(
            "Prices Ticker Count", "PASS" if ok else "WARN", n,
            f">= {EXPECTED_TICKERS:,}",
            f"{n:,} distinct tickers" + ("" if ok else f" (expected >= {EXPECTED_TICKERS:,})"),
        ))

        # Latest date
        latest = _scalar("""
            SELECT MAX(TO_DATE(DATEADD(second, DATE/1000000000, '1970-01-01')))
            FROM EQUITY_ANALYTICS.RAW.PRICES
        """)
        stale = latest is None
        results.append(_result(
            "Prices Latest Date", "WARN" if stale else "PASS", str(latest),
            "recent trading day", str(latest),
        ))

        return results
    return _safe("Prices Coverage", _run) if False else (lambda: _run())()


def check_fundamentals() -> list[dict]:
    def _run():
        results = []

        stmt_tickers = int(_scalar(
            "SELECT COUNT(DISTINCT TICKER) FROM EQUITY_ANALYTICS.RAW.FINANCIAL_STATEMENTS"
        ) or 0)
        ok = stmt_tickers >= MIN_FUNDAMENTAL_TICKERS
        results.append(_result(
            "Financial Statement Tickers",
            "PASS" if ok else "WARN",
            stmt_tickers,
            f">= {MIN_FUNDAMENTAL_TICKERS:,}",
            f"{stmt_tickers:,} tickers with statement data",
        ))

        val_tickers = int(_scalar(
            "SELECT COUNT(DISTINCT TICKER) FROM EQUITY_ANALYTICS.RAW.VALUATION_METRICS"
        ) or 0)
        ok2 = val_tickers >= MIN_VALUATION_TICKERS
        results.append(_result(
            "Valuation Metric Tickers",
            "PASS" if ok2 else "WARN",
            val_tickers,
            f">= {MIN_VALUATION_TICKERS:,}",
            f"{val_tickers:,} tickers with valuation data",
        ))

        return results
    return _run()


def check_mart_tables() -> list[dict]:
    def _run():
        row = _q("""
            SELECT
                (SELECT COUNT(DISTINCT TICKER) FROM EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES)     AS prices_tickers,
                (SELECT COUNT(DISTINCT TICKER) FROM EQUITY_ANALYTICS.MARTS.FACT_FUNDAMENTALS)     AS fund_tickers,
                (SELECT COUNT(DISTINCT TICKER) FROM EQUITY_ANALYTICS.MARTS.FACT_VALUATION_SNAPSHOT) AS val_tickers,
                (SELECT COUNT(*)               FROM EQUITY_ANALYTICS.MARTS.FACT_MACRO_READINGS)   AS macro_rows
        """)[0]
        results = []

        pt = int(row["PRICES_TICKERS"] or 0)
        ok = pt >= EXPECTED_TICKERS
        results.append(_result(
            "MART.FACT_DAILY_PRICES Tickers",
            "PASS" if ok else "WARN", pt,
            f">= {EXPECTED_TICKERS:,}",
            f"{pt:,} tickers (full-refresh needed if < raw count)",
        ))

        ft = int(row["FUND_TICKERS"] or 0)
        ok2 = ft >= MIN_FUNDAMENTAL_TICKERS
        results.append(_result(
            "MART.FACT_FUNDAMENTALS Tickers",
            "PASS" if ok2 else "WARN", ft,
            f">= {MIN_FUNDAMENTAL_TICKERS:,}",
            f"{ft:,} tickers",
        ))

        vt = int(row["VAL_TICKERS"] or 0)
        ok3 = vt >= MIN_VALUATION_TICKERS
        results.append(_result(
            "MART.FACT_VALUATION_SNAPSHOT Tickers",
            "PASS" if ok3 else "WARN", vt,
            f">= {MIN_VALUATION_TICKERS:,}",
            f"{vt:,} tickers",
        ))

        mr = int(row["MACRO_ROWS"] or 0)
        ok4 = mr >= int(MIN_MACRO_RAW_ROWS * MACRO_RAW_MART_RATIO)
        results.append(_result(
            "MART.FACT_MACRO_READINGS Rows",
            "PASS" if ok4 else "WARN", mr,
            f">= {int(MIN_MACRO_RAW_ROWS * MACRO_RAW_MART_RATIO):,}",
            f"{mr:,} rows",
        ))

        return results
    try:
        return _run()
    except Exception as e:
        return [_result("MART Tables", "ERROR", None, None, str(e))]


def check_fred_catalog() -> list[dict]:
    def _run():
        results = []

        releases = int(_scalar(
            "SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.FRED_RELEASES"
        ) or 0)
        ok = releases >= MIN_CATALOG_RELEASES
        results.append(_result(
            "FRED Releases Count", "PASS" if ok else "FAIL",
            releases, f">= {MIN_CATALOG_RELEASES}", f"{releases:,} releases",
        ))

        series = int(_scalar(
            "SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.FRED_SERIES_CATALOG"
        ) or 0)
        ok2 = series >= MIN_CATALOG_SERIES
        results.append(_result(
            "FRED Series Catalog Count", "PASS" if ok2 else "FAIL",
            series, f">= {MIN_CATALOG_SERIES:,}", f"{series:,} series",
        ))

        # Freshness via INFORMATION_SCHEMA
        cat = _q("""
            SELECT
                TO_DATE(LAST_ALTERED)                                   AS last_built,
                DATEDIFF('day', TO_DATE(LAST_ALTERED), CURRENT_DATE())  AS days_old
            FROM EQUITY_ANALYTICS.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'RAW'
              AND TABLE_NAME   = 'FRED_SERIES_CATALOG'
        """)
        if cat:
            days = int(cat[0]["DAYS_OLD"] or 0)
            ok3 = days <= MAX_CATALOG_STALE_DAYS
            results.append(_result(
                "FRED Catalog Freshness", "PASS" if ok3 else "WARN",
                days, f"<= {MAX_CATALOG_STALE_DAYS} days",
                f"Last updated {days} days ago ({cat[0]['LAST_BUILT']})",
            ))

        return results
    try:
        return _run()
    except Exception as e:
        return [_result("FRED Catalog", "ERROR", None, None, str(e))]


# ── Full suite ────────────────────────────────────────────────────────────────

def run_all_checks() -> dict:
    """
    Run every check and return:
        {
            "connection": dict,
            "raw_tables": [dict, ...],
            "prices": [dict, ...],
            "fundamentals": [dict, ...],
            "mart_tables": [dict, ...],
            "fred_catalog": [dict, ...],
            "summary": {"pass": int, "warn": int, "fail": int, "error": int},
        }
    """
    conn_result   = check_snowflake_connection()
    raw_results   = check_raw_tables()
    price_results = check_prices_coverage()
    fund_results  = check_fundamentals()
    mart_results  = check_mart_tables()
    fred_results  = check_fred_catalog()

    all_results = (
        [conn_result] + raw_results + price_results +
        fund_results + mart_results + fred_results
    )
    summary = {"PASS": 0, "WARN": 0, "FAIL": 0, "ERROR": 0}
    for r in all_results:
        summary[r["status"]] = summary.get(r["status"], 0) + 1

    return {
        "connection":   conn_result,
        "raw_tables":   raw_results,
        "prices":       price_results if isinstance(price_results, list) else [price_results],
        "fundamentals": fund_results,
        "mart_tables":  mart_results,
        "fred_catalog": fred_results,
        "summary":      summary,
    }
