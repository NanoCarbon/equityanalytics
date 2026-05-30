"""
Extract fundamental financial data from yfinance.

Three data types:
1. Financial statements (income, balance sheet, cash flow) — EAV format
2. Valuation metrics (PE, P/B, margins, etc.) — wide format, one row per ticker per snapshot

Financial statements come from yf.Ticker().income_stmt, .balance_sheet, .cashflow
(plus quarterly_ variants). These return DataFrames with:
  - Index: line item names (e.g. "TotalRevenue", "NetIncome")
  - Columns: period-end Timestamps (e.g. 2024-09-28, 2023-09-30)
  - Values: floats (dollar amounts, ratios, share counts)

We normalize these into EAV rows:
  ticker | statement_type | frequency | period_end_date | line_item | value | extracted_at

Valuation metrics come from yf.Ticker().info — same call as company metadata
but loaded to a separate table with different grain (point-in-time snapshot vs SCD).
"""

import logging
import math
import random
import yfinance as yf
import pandas as pd
import time
from datetime import datetime
from typing import List

logger = logging.getLogger(__name__)

_DELAY_MAX      = 30.0
_BACKOFF_FACTOR = 2.0
_RECOVER_FACTOR = 0.9


# ── Financial statement extraction ────────────────────────────────────────────

STATEMENT_ATTRS = {
    "income_statement": {
        "annual": "income_stmt",
        "quarterly": "quarterly_income_stmt",
    },
    "balance_sheet": {
        "annual": "balance_sheet",
        "quarterly": "quarterly_balance_sheet",
    },
    "cash_flow": {
        "annual": "cashflow",
        "quarterly": "quarterly_cashflow",
    },
}


def _normalize_statement(
    df: pd.DataFrame,
    ticker: str,
    statement_type: str,
    frequency: str,
    extracted_at: datetime,
) -> pd.DataFrame:
    """
    Convert a yfinance financial statement DataFrame (line items × periods)
    into EAV rows: one row per ticker/statement/frequency/period/line_item.

    yfinance returns:
      - Index: line item names as strings
      - Columns: period-end dates as Timestamps
      - Values: floats (NaN where not reported)
    """
    if df is None or df.empty:
        return pd.DataFrame()

    records = []
    for line_item in df.index:
        for period_end in df.columns:
            value = df.loc[line_item, period_end]
            if pd.isna(value):
                continue
            records.append({
                "ticker": ticker,
                "statement_type": statement_type,
                "frequency": frequency,
                "period_end_date": period_end,
                "line_item": line_item,
                "value": float(value),
                "extracted_at": extracted_at,
            })

    return pd.DataFrame(records)


def extract_financial_statements(
    tickers: List[str],
    delay_seconds: float = 2.0,
    batch_size: int = 100,
    batch_pause: float = 30.0,
    long_cooldown_every: int = 500,
    long_cooldown_seconds: float = 300.0,
) -> pd.DataFrame:
    """
    Extract income statement, balance sheet, and cash flow for all tickers.
    Returns EAV DataFrame with columns:
      ticker, statement_type, frequency, period_end_date, line_item, value, extracted_at

    Each ticker requires a separate yfinance Ticker() instantiation.
    Rate-limited with per-ticker delay AND a longer batch pause every `batch_size`
    tickers to avoid Yahoo Finance silently returning all-NaN DataFrames under
    sustained load. Without batch pauses, ~82% of tickers come back empty when
    running 1,500+ tickers with only a 2s per-ticker delay.

    Rate-limiting strategy mirrors extract_valuation_metrics:
      - Jitter on per-ticker sleep to reduce bot-signature patterns.
      - Dynamic backoff already present (doubles on error, recovers on success).
      - Long cooldown every 500 tickers to reset Yahoo's sliding window.
    """
    all_frames = []
    total = len(tickers)
    extracted_at = datetime.utcnow()
    skipped = []
    tickers_with_data = 0
    delay = delay_seconds

    for i, ticker in enumerate(tickers, 1):
        try:
            t = yf.Ticker(ticker)
            ticker_got_data = False

            for stmt_type, attrs in STATEMENT_ATTRS.items():
                for freq, attr_name in attrs.items():
                    try:
                        raw_df = getattr(t, attr_name)
                        normalized = _normalize_statement(
                            raw_df, ticker, stmt_type, freq, extracted_at
                        )
                        if not normalized.empty:
                            all_frames.append(normalized)
                            ticker_got_data = True
                    except Exception as e:
                        logger.warning("%s %s (%s) failed: %s", ticker, stmt_type, freq, e)

            if ticker_got_data:
                tickers_with_data += 1

            if i % 25 == 0:
                logger.info("Fundamentals progress: %d/%d tickers (%d with data so far)", i, total, tickers_with_data)

            delay = max(delay_seconds * 0.5, delay * _RECOVER_FACTOR)

        except Exception as e:
            logger.warning("Could not process %s: %s", ticker, e)
            skipped.append(ticker)
            delay = min(_DELAY_MAX, delay * _BACKOFF_FACTOR)

        if i < total:
            time.sleep(random.uniform(delay * 0.75, delay * 1.5))

        if i % long_cooldown_every == 0 and i < total:
            logger.info(
                "Long cooldown %.0fs after %d tickers (resets Yahoo sliding window)...",
                long_cooldown_seconds, i,
            )
            time.sleep(long_cooldown_seconds)
        elif i % batch_size == 0 and i < total:
            logger.info("Batch pause %ds after %d tickers (%d with data so far)...", batch_pause, i, tickers_with_data)
            time.sleep(batch_pause)

    if skipped:
        logger.warning("Skipped %d tickers: %s%s", len(skipped), skipped[:10], '...' if len(skipped) > 10 else '')

    if not all_frames:
        logger.warning("No financial statement data extracted")
        return pd.DataFrame()

    result = pd.concat(all_frames, ignore_index=True)
    logger.info("Extracted %d financial statement rows for %d tickers", len(result), tickers_with_data)
    return result


# ── Valuation metrics extraction ──────────────────────────────────────────────

VALUATION_FIELDS = [
    # Valuation ratios
    "trailingPE",
    "forwardPE",
    "priceToBook",
    "priceToSalesTrailing12Months",
    "enterpriseToEbitda",
    "enterpriseToRevenue",
    "pegRatio",
    # Profitability
    "grossMargins",
    "operatingMargins",
    "profitMargins",
    "ebitdaMargins",
    "returnOnEquity",
    "returnOnAssets",
    # Leverage & liquidity
    "debtToEquity",
    "currentRatio",
    "quickRatio",
    # Per-share
    "trailingEps",
    "forwardEps",
    "bookValue",
    "revenuePerShare",
    # Growth
    "earningsGrowth",
    "revenueGrowth",
    "earningsQuarterlyGrowth",
    # Dividends
    "dividendYield",
    "payoutRatio",
    "trailingAnnualDividendYield",
    # Absolute values (useful for screening)
    "marketCap",
    "enterpriseValue",
    "totalRevenue",
    "ebitda",
    "freeCashflow",
    "operatingCashflow",
    "totalDebt",
    "totalCash",
    # Risk
    "beta",
]


def extract_valuation_metrics(
    tickers: List[str],
    delay_seconds: float = 2.0,
    batch_size: int = 100,
    batch_pause: float = 30.0,
    long_cooldown_every: int = 500,
    long_cooldown_seconds: float = 300.0,
) -> pd.DataFrame:
    """
    Extract point-in-time valuation and ratio metrics from yfinance .info.

    Returns wide DataFrame with one row per ticker:
      ticker, snapshot_date, trailing_pe, forward_pe, ..., extracted_at

    Note: This calls .info which is the same endpoint as extract_company_info.
    If running both in the same pipeline, consider combining the calls to avoid
    duplicate API hits. The data is loaded to a separate RAW table because
    the grain differs (valuation = daily time series vs company = SCD).

    Rate-limiting strategy (Yahoo Finance .info endpoint):
      - Per-ticker delay with ±50% jitter to avoid bot-signature patterns.
      - Dynamic backoff on exceptions: delay doubles on error, recovers on success.
      - Batch pause (default 30s) every 100 tickers.
      - Long cooldown (default 5 min) every 500 tickers to reset Yahoo's sliding
        rate-limit window. The long cooldown replaces (not adds to) the batch pause
        at the 500-ticker mark to avoid stacking pauses.
      - Explicit 429 / silent-throttle detection: Yahoo often returns a thin dict
        (< 5 keys) instead of raising an exception when throttled. These are logged
        and excluded from results rather than written as all-null rows.
    """
    records = []
    total = len(tickers)
    snapshot_date = datetime.utcnow().date()
    extracted_at = datetime.utcnow()
    skipped = []
    empty_responses = 0
    delay = delay_seconds

    for i, ticker in enumerate(tickers, 1):
        try:
            info = yf.Ticker(ticker).info

            # ── 429 / silent-throttle detection ──────────────────────────────
            # A real yfinance .info response contains 50+ keys. A throttled or
            # delisted response typically returns {} or a dict with only 1-4 keys
            # (e.g. {"trailingPegRatio": null}). Writing all-null rows for these
            # would silently corrupt the valuation time series.
            if len(info) < 5:
                empty_responses += 1
                logger.warning(
                    "Thin .info response for %s (%d keys) — possible 429 or "
                    "delisted ticker. Skipping row. Total thin responses: %d",
                    ticker, len(info), empty_responses,
                )
                skipped.append(ticker)
                delay = min(_DELAY_MAX, delay * _BACKOFF_FACTOR)
                if i < total:
                    time.sleep(random.uniform(delay * 0.75, delay * 1.5))
                continue

            row = {
                "ticker": ticker,
                "snapshot_date": snapshot_date,
            }
            for field in VALUATION_FIELDS:
                value = info.get(field)
                # yfinance sometimes returns the string 'Infinity' (not float inf)
                # or very large sentinel numeric values — both poison PyArrow on load.
                if value is not None:
                    if isinstance(value, str):
                        # Coerce string numerics; treat non-finite strings as null
                        try:
                            value = float(value)
                        except (ValueError, TypeError):
                            value = None
                    if isinstance(value, (int, float)):
                        if not math.isfinite(float(value)) or abs(value) > 1e15:
                            value = None
                row[field] = value

            row["extracted_at"] = extracted_at
            records.append(row)

            if i % 25 == 0:
                logger.info("Valuation progress: %d/%d tickers", i, total)

            delay = max(delay_seconds * 0.5, delay * _RECOVER_FACTOR)

        except Exception as e:
            logger.warning("Could not fetch valuation for %s: %s", ticker, e)
            skipped.append(ticker)
            records.append({
                "ticker": ticker,
                "snapshot_date": snapshot_date,
                **{field: None for field in VALUATION_FIELDS},
                "extracted_at": extracted_at,
            })
            delay = min(_DELAY_MAX, delay * _BACKOFF_FACTOR)

        # ── Per-ticker sleep with jitter ──────────────────────────────────────
        # Jitter (±50% of current delay) makes the request pattern look less
        # automated and reduces Yahoo's bot-detection confidence.
        if i < total:
            time.sleep(random.uniform(delay * 0.75, delay * 1.5))

        # ── Batch / long-cooldown pauses ──────────────────────────────────────
        # Long cooldown every `long_cooldown_every` tickers takes priority over
        # the shorter batch pause — no need to stack both at the same index.
        if i % long_cooldown_every == 0 and i < total:
            logger.info(
                "Long cooldown %.0fs after %d tickers (resets Yahoo sliding window)...",
                long_cooldown_seconds, i,
            )
            time.sleep(long_cooldown_seconds)
        elif i % batch_size == 0 and i < total:
            logger.info("Batch pause %ds after %d tickers...", batch_pause, i)
            time.sleep(batch_pause)

    if skipped:
        logger.warning(
            "Skipped %d tickers: %s%s",
            len(skipped), skipped[:10], '...' if len(skipped) > 10 else '',
        )
    if empty_responses:
        logger.warning(
            "%d thin/empty .info responses detected — if this is high (>1%% of tickers) "
            "consider increasing delay_seconds or long_cooldown_seconds.",
            empty_responses,
        )

    result = pd.DataFrame(records)
    logger.info("Extracted valuation metrics for %d tickers", len(result))
    return result


# ── Test utility ──────────────────────────────────────────────────────────────

def test_extract(tickers: List[str] = None):
    """Quick test with a small set of tickers to validate schema and endpoints."""
    if tickers is None:
        tickers = ["AAPL", "MSFT", "JPM", "SPY", "BND"]  # mix of equity + ETF

    print(f"\n{'='*60}")
    print(f"TEST EXTRACTION — {len(tickers)} tickers: {tickers}")
    print(f"{'='*60}\n")

    print("--- Financial Statements ---")
    stmt_df = extract_financial_statements(tickers, delay_seconds=1.0)
    if not stmt_df.empty:
        print(f"\nShape: {stmt_df.shape}")
        print(f"Columns: {list(stmt_df.columns)}")
        print(f"\nStatement types: {stmt_df['statement_type'].value_counts().to_dict()}")
        print(f"Frequencies: {stmt_df['frequency'].value_counts().to_dict()}")
        print(f"Tickers with data: {stmt_df['ticker'].nunique()}")
        print(f"Unique line items: {stmt_df['line_item'].nunique()}")
        print(f"\nSample rows:")
        print(stmt_df.head(10).to_string(index=False))
    else:
        print("No financial statement data returned")

    print(f"\n--- Valuation Metrics ---")
    val_df = extract_valuation_metrics(tickers, delay_seconds=1.0)
    if not val_df.empty:
        print(f"\nShape: {val_df.shape}")
        print(f"Columns: {list(val_df.columns)}")
        non_null_counts = val_df.drop(columns=["ticker", "snapshot_date", "extracted_at"]).notna().sum()
        print(f"\nFields with data (out of {len(tickers)} tickers):")
        for field, count in non_null_counts.items():
            if count > 0:
                print(f"  {field}: {count}/{len(tickers)}")
        print(f"\nETF check (SPY should have few valuation fields):")
        spy_row = val_df[val_df["ticker"] == "SPY"]
        if not spy_row.empty:
            spy_nulls = spy_row.drop(columns=["ticker", "snapshot_date", "extracted_at"]).isna().sum(axis=1).iloc[0]
            print(f"  SPY null fields: {spy_nulls}/{len(VALUATION_FIELDS)}")
    else:
        print("No valuation data returned")

    return stmt_df, val_df


if __name__ == "__main__":
    test_extract()
