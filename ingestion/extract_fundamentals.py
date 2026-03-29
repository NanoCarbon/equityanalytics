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

import yfinance as yf
import pandas as pd
import time
from datetime import datetime
from typing import List


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
) -> pd.DataFrame:
    """
    Extract income statement, balance sheet, and cash flow for all tickers.
    Returns EAV DataFrame with columns:
      ticker, statement_type, frequency, period_end_date, line_item, value, extracted_at

    Each ticker requires a separate yfinance Ticker() instantiation.
    Rate-limited with configurable delay between tickers.
    """
    all_frames = []
    total = len(tickers)
    extracted_at = datetime.utcnow()
    skipped = []

    for i, ticker in enumerate(tickers, 1):
        try:
            t = yf.Ticker(ticker)

            for stmt_type, attrs in STATEMENT_ATTRS.items():
                for freq, attr_name in attrs.items():
                    try:
                        raw_df = getattr(t, attr_name)
                        normalized = _normalize_statement(
                            raw_df, ticker, stmt_type, freq, extracted_at
                        )
                        if not normalized.empty:
                            all_frames.append(normalized)
                    except Exception as e:
                        print(f"  Warning: {ticker} {stmt_type} ({freq}) failed: {e}")

            if i % 25 == 0:
                print(f"  Fundamentals progress: {i}/{total} tickers")

        except Exception as e:
            print(f"Warning: could not process {ticker}: {e}")
            skipped.append(ticker)

        if i < total:
            time.sleep(delay_seconds)

    if skipped:
        print(f"Skipped {len(skipped)} tickers: {skipped[:10]}{'...' if len(skipped) > 10 else ''}")

    if not all_frames:
        print("No financial statement data extracted")
        return pd.DataFrame()

    result = pd.concat(all_frames, ignore_index=True)
    print(f"Extracted {len(result)} financial statement rows for {total - len(skipped)} tickers")
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
) -> pd.DataFrame:
    """
    Extract point-in-time valuation and ratio metrics from yfinance .info.

    Returns wide DataFrame with one row per ticker:
      ticker, snapshot_date, trailing_pe, forward_pe, ..., extracted_at

    Note: This calls .info which is the same endpoint as extract_company_info.
    If running both in the same pipeline, consider combining the calls to avoid
    duplicate API hits. The data is loaded to a separate RAW table because
    the grain differs (valuation = daily time series vs company = SCD).
    """
    records = []
    total = len(tickers)
    snapshot_date = datetime.utcnow().date()
    extracted_at = datetime.utcnow()
    skipped = []

    for i, ticker in enumerate(tickers, 1):
        try:
            info = yf.Ticker(ticker).info
            row = {
                "ticker": ticker,
                "snapshot_date": snapshot_date,
            }
            for field in VALUATION_FIELDS:
                value = info.get(field)
                # yfinance sometimes returns 'Infinity' or very large sentinel values
                if value is not None and isinstance(value, (int, float)):
                    if abs(value) > 1e18:
                        value = None
                row[field] = value

            row["extracted_at"] = extracted_at
            records.append(row)

            if i % 25 == 0:
                print(f"  Valuation progress: {i}/{total} tickers")

        except Exception as e:
            print(f"Warning: could not fetch valuation for {ticker}: {e}")
            skipped.append(ticker)
            records.append({
                "ticker": ticker,
                "snapshot_date": snapshot_date,
                **{field: None for field in VALUATION_FIELDS},
                "extracted_at": extracted_at,
            })

        if i < total:
            time.sleep(delay_seconds)

    if skipped:
        print(f"Skipped {len(skipped)} tickers: {skipped[:10]}{'...' if len(skipped) > 10 else ''}")

    result = pd.DataFrame(records)
    print(f"Extracted valuation metrics for {len(result)} tickers")
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
