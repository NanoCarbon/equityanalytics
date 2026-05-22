"""
Database Health Check
=====================
Run anytime to get a full picture of data coverage and gaps across all
Snowflake tables. Prints a pass/fail summary with details on any issues.

Usage:
    python scripts/db_health_check.py

    # Verbose -- shows per-series and per-year breakdowns even when passing
    python scripts/db_health_check.py --verbose

    # Check a specific layer only
    python scripts/db_health_check.py --check prices
    python scripts/db_health_check.py --check macro
    python scripts/db_health_check.py --check fundamentals
    python scripts/db_health_check.py --check valuation
    python scripts/db_health_check.py --check raw        # includes all 11 RAW tables + catalog freshness
"""

import sys
import argparse
from datetime import date, timedelta
import os
import sys as _sys
_sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv

load_dotenv()

import warnings
warnings.filterwarnings("ignore")
import logging
logging.disable(logging.WARNING)

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import pandas as pd

def execute_sql(sql: str) -> pd.DataFrame:
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "snowflake_private_key.pem")
    passphrase_str = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    passphrase = passphrase_str.encode() if passphrase_str else None
    with open(key_path, "rb") as f:
        p_key = serialization.load_pem_private_key(f.read(), password=passphrase, backend=default_backend())
    private_key_der = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "TRANSFORM_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "EQUITY_ANALYTICS"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "MARTS"),
        private_key=private_key_der,
        network_timeout=30,
        login_timeout=15,
    )
    cursor = conn.cursor()
    cursor.execute(sql)
    df = cursor.fetch_pandas_all()
    df.columns = [c.lower() for c in df.columns]
    conn.close()
    return df

# ─────────────────────────────────────────────────────────────────
# Thresholds -- adjust these if your universe changes
# ─────────────────────────────────────────────────────────────────
EXPECTED_TICKERS          = 1500  # S&P 1500 + ETFs (~1,600 total, allow some buffer)
MIN_TICKERS_PER_DAY       = 1400  # flag dates below this (post S&P 1500 expansion)
EXPECTED_MACRO_SERIES     = 100   # we have ~108 configured
MACRO_RAW_MART_RATIO      = 0.35  # dbt staging filters/deduplicates -- mart ~35-50% of raw is normal
PRICE_EARLIEST_EXPECTED   = date(2010, 1, 1)
MAX_PRICE_GAP_DAYS        = 5     # more than 5 calendar days without new prices = alert
MAX_MACRO_STALE_DAYS      = 45    # series not updated in 45+ days = alert
MIN_FUNDAMENTAL_TICKERS   = 250   # yfinance returns empty for most S&P 400/600 small-caps under load;
                                  # ~270 tickers is realistic. Raise when batch-pause logic improves coverage.
MIN_VALUATION_DAYS        = 1     # at least 1 valuation snapshot
MIN_CATALOG_RELEASES      = 250   # FRED has ~300 releases
MIN_CATALOG_SERIES        = 500_000  # catalog typically 700K-900K unique series
MAX_CATALOG_STALE_DAYS    = 45    # rebuilt monthly -- flag if >45 days old

PASS = "[PASS]"
FAIL = "[FAIL]"
WARN = "[WARN]"


# ─────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────

def section(title: str):
    print(f"\n{'-' * 60}")
    print(f"  {title}")
    print(f"{'-' * 60}")


def result(status: str, label: str, detail: str = ""):
    detail_str = f"  ->{detail}" if detail else ""
    print(f"  {status}  {label}{detail_str}")


def today():
    return date.today()


# ─────────────────────────────────────────────────────────────────
# Checks
# ─────────────────────────────────────────────────────────────────

def check_prices(verbose: bool = False):
    section("PRICES  --  FACT_DAILY_PRICES")
    issues = 0

    # Overall coverage
    df = execute_sql("""
        SELECT
            MIN(price_date)        AS earliest,
            MAX(price_date)        AS latest,
            COUNT(*)               AS total_rows,
            COUNT(DISTINCT ticker) AS tickers,
            COUNT(DISTINCT price_date) AS trading_days
        FROM EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES
    """)
    row = df.iloc[0]
    earliest    = row["earliest"]
    latest      = row["latest"]
    total_rows  = int(row["total_rows"])
    tickers     = int(row["tickers"])
    trading_days = int(row["trading_days"])

    print(f"\n  Overview: {total_rows:,} rows | {tickers} tickers | {trading_days} trading days")
    print(f"  Range:    {earliest}  ->{latest}\n")

    # Ticker count
    if tickers >= EXPECTED_TICKERS:
        result(PASS, f"Ticker universe: {tickers} tickers")
    else:
        result(FAIL, f"Ticker universe: only {tickers} tickers (expected >={EXPECTED_TICKERS})")
        issues += 1

    # Earliest date
    if earliest <= PRICE_EARLIEST_EXPECTED:
        result(PASS, f"Historical depth: data starts {earliest}")
    else:
        result(WARN, f"Historical depth: starts {earliest} (expected <={PRICE_EARLIEST_EXPECTED})")

    # Recency gap
    days_stale = (today() - latest).days
    biz_days_stale = sum(
        1 for i in range(days_stale)
        if (latest + timedelta(days=i+1)).weekday() < 5
    )
    if biz_days_stale <= MAX_PRICE_GAP_DAYS:
        result(PASS, f"Recency: latest price {latest} ({biz_days_stale} business days ago)")
    else:
        result(FAIL, f"Recency: latest price {latest} -- {biz_days_stale} business days behind today")
        issues += 1

    # Sparse dates (dates with fewer tickers than expected)
    df_sparse = execute_sql(f"""
        SELECT price_date, COUNT(DISTINCT ticker) AS ticker_count
        FROM EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES
        WHERE price_date >= '2024-01-01'
        GROUP BY price_date
        HAVING COUNT(DISTINCT ticker) < {MIN_TICKERS_PER_DAY}
        ORDER BY price_date DESC
    """)
    if df_sparse.empty:
        result(PASS, f"No sparse dates since 2024 (all days have >={MIN_TICKERS_PER_DAY} tickers)")
    else:
        result(FAIL, f"{len(df_sparse)} dates since 2024 with <{MIN_TICKERS_PER_DAY} tickers")
        issues += 1
        if verbose or len(df_sparse) <= 10:  # noqa
            print(f"\n  {'Date':<15} {'Tickers':>8}")
            for _, r in df_sparse.head(20).iterrows():
                print(f"  {str(r['price_date']):<15} {int(r['ticker_count']):>8}")

    # Per-year coverage
    if verbose:
        print("\n  Per-year ticker coverage:")
        df_yr = execute_sql("""
            SELECT
                YEAR(price_date) AS yr,
                MIN(COUNT(DISTINCT ticker)) OVER (PARTITION BY YEAR(price_date)) AS min_per_day,
                ROUND(AVG(COUNT(DISTINCT ticker)) OVER (PARTITION BY YEAR(price_date)), 0) AS avg_per_day,
                MAX(COUNT(DISTINCT ticker)) OVER (PARTITION BY YEAR(price_date)) AS max_per_day
            FROM EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES
            GROUP BY price_date
            QUALIFY ROW_NUMBER() OVER (PARTITION BY YEAR(price_date) ORDER BY price_date) = 1
            ORDER BY yr
        """)
        print(f"\n  {'Year':<6} {'Min/day':>8} {'Avg/day':>8} {'Max/day':>8}")
        for _, r in df_yr.iterrows():
            flag = " [!]" if int(r["min_per_day"]) < MIN_TICKERS_PER_DAY else ""
            print(f"  {int(r['yr']):<6} {int(r['min_per_day']):>8} {int(r['avg_per_day']):>8} {int(r['max_per_day']):>8}{flag}")

    return issues


def check_macro(verbose: bool = False):
    section("MACRO  --  FACT_MACRO_READINGS  +  RAW.MACRO_INDICATORS")
    issues = 0

    # RAW vs MART row counts
    df = execute_sql("""
        SELECT
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.MACRO_INDICATORS)      AS raw_rows,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.MARTS.FACT_MACRO_READINGS)  AS mart_rows,
            (SELECT COUNT(DISTINCT series_id) FROM EQUITY_ANALYTICS.RAW.MACRO_INDICATORS) AS raw_series,
            (SELECT COUNT(DISTINCT series_id) FROM EQUITY_ANALYTICS.MARTS.FACT_MACRO_READINGS) AS mart_series
    """)
    row = df.iloc[0]
    raw_rows   = int(row["raw_rows"])
    mart_rows  = int(row["mart_rows"])
    raw_series = int(row["raw_series"])
    mart_series = int(row["mart_series"])
    ratio = mart_rows / raw_rows if raw_rows > 0 else 0

    print(f"\n  RAW:  {raw_rows:>8,} rows | {raw_series} series")
    print(f"  MART: {mart_rows:>8,} rows | {mart_series} series  (ratio: {ratio:.1%})\n")

    if raw_series >= EXPECTED_MACRO_SERIES:
        result(PASS, f"Series count in RAW: {raw_series} series")
    else:
        result(WARN, f"Series count in RAW: only {raw_series} (expected >={EXPECTED_MACRO_SERIES})")

    if ratio >= MACRO_RAW_MART_RATIO:
        result(PASS, f"RAW->MART propagation: {ratio:.1%} of raw rows in mart")
    else:
        result(FAIL, f"RAW->MART propagation: only {ratio:.1%} -- run: dbt build --select fact_macro_readings --full-refresh")
        issues += 1

    # Recency -- series with stale latest observation
    df_stale = execute_sql(f"""
        SELECT series_id, series_name,
               MAX(observation_date) AS latest,
               DATEDIFF('day', MAX(observation_date), CURRENT_DATE()) AS days_stale
        FROM EQUITY_ANALYTICS.MARTS.FACT_MACRO_READINGS
        GROUP BY series_id, series_name
        HAVING days_stale > {MAX_MACRO_STALE_DAYS}
        ORDER BY days_stale DESC
    """)
    if df_stale.empty:
        result(PASS, f"All series updated within {MAX_MACRO_STALE_DAYS} days")
    else:
        # Many FRED series are low-frequency (quarterly GDP etc.) -- only flag daily series
        result(WARN, f"{len(df_stale)} series last updated >{MAX_MACRO_STALE_DAYS} days ago (may be low-frequency, not errors)")
        if verbose:
            print(f"\n  {'Series ID':<25} {'Latest':<15} {'Days stale':>10}")
            for _, r in df_stale.head(15).iterrows():
                print(f"  {r['series_id']:<25} {str(r['latest']):<15} {int(r['days_stale']):>10}")

    # Coverage range
    df_range = execute_sql("""
        SELECT MIN(observation_date) AS earliest, MAX(observation_date) AS latest
        FROM EQUITY_ANALYTICS.MARTS.FACT_MACRO_READINGS
    """)
    r = df_range.iloc[0]
    print(f"\n  Date range: {r['earliest']}  ->{r['latest']}")

    return issues


def check_fundamentals(verbose: bool = False):
    section("FUNDAMENTALS  --  FACT_FUNDAMENTALS")
    issues = 0

    df = execute_sql("""
        SELECT
            COUNT(DISTINCT ticker)          AS tickers,
            COUNT(DISTINCT period_end_date)  AS periods,
            MIN(period_end_date)             AS earliest,
            MAX(period_end_date)             AS latest,
            COUNT(*)                         AS total_rows
        FROM EQUITY_ANALYTICS.MARTS.FACT_FUNDAMENTALS
    """)
    row = df.iloc[0]
    tickers   = int(row["tickers"])
    total     = int(row["total_rows"])
    earliest  = row["earliest"]
    latest    = row["latest"]

    print(f"\n  {total:,} rows | {tickers} tickers | range: {earliest} -> {latest}\n")

    if tickers >= MIN_FUNDAMENTAL_TICKERS:
        result(PASS, f"Ticker coverage: {tickers} equities with fundamentals")
    else:
        result(FAIL, f"Ticker coverage: only {tickers} (expected >={MIN_FUNDAMENTAL_TICKERS})")
        issues += 1

    # Tickers in DIM_SECURITY but missing from fundamentals (equity only -- ETFs won't have them)
    df_missing = execute_sql("""
        SELECT COUNT(*) AS missing
        FROM EQUITY_ANALYTICS.MARTS.DIM_SECURITY s
        WHERE s.ticker NOT IN (SELECT DISTINCT ticker FROM EQUITY_ANALYTICS.MARTS.FACT_FUNDAMENTALS)
          AND s.sector IS NOT NULL          -- rough proxy for equities vs ETFs
          AND s.sector != 'Unknown'
    """)
    missing = int(df_missing.iloc[0]["missing"])
    if missing < 50:
        result(PASS, f"Equity coverage: {missing} equities with sector data but no fundamentals")
    else:
        result(WARN, f"{missing} equities with sector data but no fundamentals rows")

    return issues


def check_valuation(verbose: bool = False):
    section("VALUATION SNAPSHOTS  --  FACT_VALUATION_SNAPSHOT")
    issues = 0

    df = execute_sql("""
        SELECT
            COUNT(DISTINCT ticker)        AS tickers,
            COUNT(DISTINCT snapshot_date) AS snapshot_days,
            MIN(snapshot_date)            AS earliest,
            MAX(snapshot_date)            AS latest,
            COUNT(*)                      AS total_rows
        FROM EQUITY_ANALYTICS.MARTS.FACT_VALUATION_SNAPSHOT
    """)
    row = df.iloc[0]
    tickers       = int(row["tickers"])
    snapshot_days = int(row["snapshot_days"])
    total         = int(row["total_rows"])
    latest        = row["latest"]

    print(f"\n  {total:,} rows | {tickers} tickers | {snapshot_days} snapshot days")
    print(f"  Latest snapshot: {latest}\n")

    if tickers >= EXPECTED_TICKERS:
        result(PASS, f"Ticker coverage: {tickers} tickers in latest snapshot")
    else:
        result(WARN, f"Ticker coverage: {tickers} tickers (expected >={EXPECTED_TICKERS})")

    if snapshot_days >= MIN_VALUATION_DAYS:
        result(PASS, f"Snapshot history: {snapshot_days} days of valuation data")
    else:
        result(FAIL, f"Snapshot history: only {snapshot_days} days -- run valuation_daily DAG")
        issues += 1

    days_stale = (today() - latest).days if latest else 999
    biz_stale = sum(1 for i in range(days_stale) if (latest + timedelta(days=i+1)).weekday() < 5) if latest else 999
    if biz_stale <= 3:
        result(PASS, f"Recency: latest snapshot {latest} ({biz_stale} business days ago)")
    else:
        result(WARN, f"Recency: latest snapshot {latest} -- {biz_stale} business days behind")

    return issues


def check_raw(verbose: bool = False):
    section("RAW LAYER  --  row counts and freshness")
    issues = 0

    df = execute_sql("""
        SELECT
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.PRICES)                    AS prices,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.COMPANY_INFO)               AS company_info,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.MACRO_INDICATORS)           AS macro_indicators,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.FINANCIAL_STATEMENTS)       AS financial_statements,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.VALUATION_METRICS)          AS valuation_metrics,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.DIVIDENDS_AND_SPLITS)       AS dividends_and_splits,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.EARNINGS_HISTORY)           AS earnings_history,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.ANALYST_RECOMMENDATIONS)    AS analyst_recommendations,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.ANALYST_PRICE_TARGETS)      AS analyst_price_targets,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.FRED_RELEASES)              AS fred_releases,
            (SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.FRED_SERIES_CATALOG)        AS fred_series_catalog
    """)
    row = df.iloc[0]

    tables = {
        "PRICES":                  int(row["prices"]),
        "COMPANY_INFO":            int(row["company_info"]),
        "MACRO_INDICATORS":        int(row["macro_indicators"]),
        "FINANCIAL_STATEMENTS":    int(row["financial_statements"]),
        "VALUATION_METRICS":       int(row["valuation_metrics"]),
        "DIVIDENDS_AND_SPLITS":    int(row["dividends_and_splits"]),
        "EARNINGS_HISTORY":        int(row["earnings_history"]),
        "ANALYST_RECOMMENDATIONS": int(row["analyst_recommendations"]),
        "ANALYST_PRICE_TARGETS":   int(row["analyst_price_targets"]),
        "FRED_RELEASES":           int(row["fred_releases"]),
        "FRED_SERIES_CATALOG":     int(row["fred_series_catalog"]),
    }

    min_rows = {
        "PRICES":                  1_000_000,
        "COMPANY_INFO":            EXPECTED_TICKERS,
        "MACRO_INDICATORS":        100_000,
        "FINANCIAL_STATEMENTS":    100_000,
        "VALUATION_METRICS":       500,
        "DIVIDENDS_AND_SPLITS":    10_000,
        "EARNINGS_HISTORY":        2_000,   # sparse for small-caps; 3K+ expected for 1500 tickers
        "ANALYST_RECOMMENDATIONS": 2_000,   # same -- many S&P 600 tickers have thin coverage
        "ANALYST_PRICE_TARGETS":   500,
        "FRED_RELEASES":           MIN_CATALOG_RELEASES,
        "FRED_SERIES_CATALOG":     MIN_CATALOG_SERIES,
    }

    print()
    for table, count in tables.items():
        threshold = min_rows.get(table, 0)
        if count >= threshold:
            result(PASS, f"RAW.{table:<25} {count:>12,} rows")
        else:
            result(FAIL, f"RAW.{table:<25} {count:>12,} rows  (expected >={threshold:,})")
            issues += 1

    # Company info coverage: every ticker in PRICES should have a metadata row
    cov = execute_sql("""
        SELECT
            COUNT(DISTINCT p.ticker)                                         AS price_tickers,
            COUNT(DISTINCT c.ticker)                                         AS info_tickers,
            COUNT(DISTINCT p.ticker) - COUNT(DISTINCT c.ticker)              AS missing
        FROM (SELECT DISTINCT ticker FROM EQUITY_ANALYTICS.RAW.PRICES) p
        LEFT JOIN EQUITY_ANALYTICS.RAW.COMPANY_INFO c ON p.ticker = c.ticker
    """)
    crow = cov.iloc[0]
    price_t   = int(crow["price_tickers"])
    info_t    = int(crow["info_tickers"])
    missing_t = int(crow["missing"])

    print()
    if missing_t == 0:
        result(PASS, f"COMPANY_INFO coverage: all {info_t:,} price tickers have metadata")
    else:
        result(FAIL, f"COMPANY_INFO coverage: {missing_t:,} tickers in PRICES have no metadata row -- trigger equity_daily")
        issues += 1

    # FRED catalog freshness: use INFORMATION_SCHEMA.TABLES.LAST_ALTERED --
    # more reliable than extracted_at whose storage type varies by connector version
    cat = execute_sql("""
        SELECT
            TO_DATE(LAST_ALTERED)                                   AS last_built,
            DATEDIFF('day', TO_DATE(LAST_ALTERED), CURRENT_DATE())  AS days_old
        FROM EQUITY_ANALYTICS.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'RAW'
          AND TABLE_NAME   = 'FRED_SERIES_CATALOG'
    """)
    if not cat.empty and cat.iloc[0]["last_built"] is not None:
        crow = cat.iloc[0]
        days_old = int(crow["days_old"])
        last_built = str(crow["last_built"])[:10]
        if days_old <= MAX_CATALOG_STALE_DAYS:
            result(PASS, f"FRED catalog freshness: last rebuilt {last_built} ({days_old} days ago)")
        else:
            result(WARN, f"FRED catalog freshness: last rebuilt {last_built} ({days_old} days ago) -- trigger fred_catalog_refresh")
    else:
        result(WARN, "FRED catalog freshness: could not determine last build date")

    return issues


# ─────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Equity Analytics DB health check")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed breakdowns")
    parser.add_argument("--check", choices=["prices", "macro", "fundamentals", "valuation", "raw"],
                        help="Run only one check")
    args = parser.parse_args()

    print(f"\n{'=' * 60}")
    print(f"  EQUITY ANALYTICS -- DATABASE HEALTH CHECK")
    print(f"  {date.today()}  |  Snowflake: EQUITY_ANALYTICS")
    print(f"{'=' * 60}")

    total_issues = 0
    checks = {
        "prices":       check_prices,
        "macro":        check_macro,
        "fundamentals": check_fundamentals,
        "valuation":    check_valuation,
        "raw":          check_raw,
    }

    if args.check:
        total_issues += checks[args.check](verbose=args.verbose)
    else:
        for fn in checks.values():
            total_issues += fn(verbose=args.verbose)

    print(f"\n{'=' * 60}")
    if total_issues == 0:
        print(f"  [PASS]  All checks passed -- database looks healthy")
    else:
        print(f"  [FAIL]  {total_issues} issue(s) found -- see details above")
    print(f"{'=' * 60}\n")

    sys.exit(0 if total_issues == 0 else 1)


if __name__ == "__main__":
    main()
