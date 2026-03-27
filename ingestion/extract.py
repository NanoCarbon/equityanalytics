import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import List

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "JPM", "GS", "MS", "BLK", "BX",
    "BRK-B", "V", "MA", "HD", "UNH",
    "SPY", "QQQ", "IWM", "VTI", "AGG"
]

def extract_prices(tickers: List[str], lookback_days: int = 365) -> pd.DataFrame:
    """Extract OHLCV price data from yfinance for a list of tickers."""
    end_date = datetime.today()
    start_date = end_date - timedelta(days=lookback_days)

    raw = yf.download(
        tickers=tickers,
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        auto_adjust=True,
        progress=False
    )

    # Normalize MultiIndex columns → long format
    df = raw.stack(level=1, future_stack=True).reset_index()
    df.columns = ["date", "ticker", "close", "high", "low", "open", "volume"]
    df["extracted_at"] = datetime.utcnow()

    return df


def extract_company_info(tickers: List[str]) -> pd.DataFrame:
    """Extract company metadata (sector, industry, market cap) from yfinance."""
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