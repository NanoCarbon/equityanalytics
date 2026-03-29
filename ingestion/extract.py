import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, date
from typing import List

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "JPM", "GS", "MS", "BLK", "BX",
    "BRK-B", "V", "MA", "HD", "UNH",
    "SPY", "QQQ", "IWM", "VTI", "AGG"
]

def extract_prices(
    tickers: List[str],
    start_date: date | None = None,
    lookback_days: int = 365
) -> pd.DataFrame:
    """
    Extract OHLCV price data from yfinance.
    If start_date is provided, only extract from that date forward.
    Otherwise extract lookback_days of history.
    """
    end_date = datetime.today()

    if start_date is not None:
        # Add one day to avoid reloading the last loaded date
        effective_start = datetime.combine(start_date, datetime.min.time()) + timedelta(days=1)
    else:
        effective_start = end_date - timedelta(days=lookback_days)

    print(f"Extracting prices from {effective_start.date()} to {end_date.date()}")

    raw = yf.download(
        tickers=tickers,
        start=effective_start.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        auto_adjust=True,
        progress=False
    )

    if raw.empty:
        print("No new data to extract")
        return pd.DataFrame()

    df = raw.stack(level=1, future_stack=True).reset_index()
    df.columns = ["date", "ticker", "close", "high", "low", "open", "volume"]
    df["extracted_at"] = datetime.utcnow()

    return df


def extract_company_info(tickers: List[str]) -> pd.DataFrame:
    """Extract company metadata from yfinance."""
    records = []
    for ticker in tickers:
        try:
            info = yf.Ticker(ticker).info
            records.append({
                "ticker": ticker,
                "company_name": info.get("longName"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "market_cap": info.get("marketCap"),
                "extracted_at": datetime.utcnow()
            })
        except Exception as e:
            print(f"Warning: could not fetch info for {ticker}: {e}")

    return pd.DataFrame(records)