import yfinance as yf
import pandas as pd
import time
import requests
from datetime import datetime, timedelta, date
from typing import List
from io import StringIO


def get_sp500_tickers() -> List[str]:
    """Fetch current S&P 500 components from Wikipedia."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        tables = pd.read_html(StringIO(response.text))
        df = tables[0]
        tickers = df['Symbol'].tolist()
        tickers = [t.replace('.', '-') for t in tickers]
        print(f"Fetched {len(tickers)} S&P 500 tickers from Wikipedia")
        return tickers
    except Exception as e:
        print(f"Warning: could not fetch S&P 500 tickers: {e}")
        return FALLBACK_SP500

def get_etf_tickers() -> List[str]:
    """Top 100 ETFs by AUM and liquidity."""
    return [
        # Broad market
        "SPY", "IVV", "VOO", "QQQ", "VTI", "IWM", "IWF", "IWD",
        "IWB", "ITOT", "SCHB", "VV", "MGC", "SPTM", "SCHX",
        # Fixed income
        "BND", "AGG", "TLT", "IEF", "SHY", "LQD", "HYG", "JNK",
        "MUB", "VCIT", "VCSH", "BSV", "BIV", "BLV", "GOVT",
        # International
        "VEA", "VWO", "EFA", "EEM", "IEFA", "IEMG", "VGK", "VPL",
        "EWJ", "EWZ", "EWC", "EWG", "EWU", "EWA", "EWH",
        # Sector
        "XLF", "XLK", "XLV", "XLE", "XLI", "XLY", "XLP", "XLU",
        "XLB", "XLRE", "VNQ", "IYR", "KRE", "XBI", "IBB",
        # Commodities and alternatives
        "GLD", "IAU", "SLV", "USO", "UNG", "DBC", "PDBC", "CORN",
        "WEAT", "SOYB", "CPER", "PALL", "PPLT", "BAR", "SGOL",
        # Factor and smart beta
        "MTUM", "USMV", "VLUE", "QUAL", "SIZE", "VIG", "DVY",
        "SDY", "NOBL", "DGRO", "VYM", "HDV", "SCHD", "SPYD", "FVD",
        # Leveraged / inverse (liquid, widely tracked)
        "TQQQ", "SQQQ", "UPRO", "SPXU", "SSO", "SDS", "TNA", "TZA",
        # Thematic
        "ARKK", "ARKW", "ARKG", "ARKF", "ARKQ", "BOTZ", "ROBO",
        "ICLN", "QCLN", "CNRG", "AIQ", "HACK", "BUG", "CIBR", "WCLD"
    ]


# Fallback list in case Wikipedia scrape fails
FALLBACK_SP500 = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "BRK-B",
    "JPM", "JNJ", "V", "UNH", "XOM", "PG", "MA", "HD", "CVX", "MRK",
    "LLY", "ABBV", "PEP", "KO", "AVGO", "COST", "TMO", "MCD", "ACN",
    "ABT", "DHR", "TXN", "NEE", "PM", "RTX", "HON", "UPS", "AMGN",
    "SBUX", "IBM", "GE", "GS", "BLK", "MS", "AXP", "SPGI", "CAT",
    "BA", "MMM", "DE", "LMT", "NOW"
]


def get_all_tickers() -> List[str]:
    """Get combined list of S&P 500 + ETF tickers, deduplicated."""
    sp500 = get_sp500_tickers()
    etfs = get_etf_tickers()
    all_tickers = list(dict.fromkeys(sp500 + etfs))  # preserves order, dedupes
    print(f"Total unique tickers: {len(all_tickers)}")
    return all_tickers


def extract_prices(
    tickers: List[str],
    start_date: date | None = None,
    lookback_days: int = 365,
    start_date_str: str | None = None,
    end_date_str: str | None = None
) -> pd.DataFrame:
    """
    Bulk extract OHLCV price data from yfinance.
    
    Three modes:
    - start_date_str + end_date_str: explicit range (for backfill)
    - start_date: incremental from this date to today
    - lookback_days: rolling window from today
    """
    if start_date_str and end_date_str:
        effective_start = start_date_str
        effective_end = end_date_str
    elif start_date is not None:
        effective_start = (
            datetime.combine(start_date, datetime.min.time()) + timedelta(days=1)
        ).strftime("%Y-%m-%d")
        effective_end = datetime.today().strftime("%Y-%m-%d")
    else:
        effective_start = (datetime.today() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        effective_end = datetime.today().strftime("%Y-%m-%d")

    print(f"Extracting prices for {len(tickers)} tickers "
          f"from {effective_start} to {effective_end}")

    try:
        raw = yf.download(
            tickers=tickers,
            start=effective_start,
            end=effective_end,
            auto_adjust=True,
            progress=False,
            group_by='ticker'
        )
    except Exception as e:
        print(f"yfinance download failed: {e}")
        return pd.DataFrame()

    if raw.empty:
        print("No price data returned")
        return pd.DataFrame()

    try:
        df = raw.stack(level=0, future_stack=True).reset_index()
        df.columns = ["date", "ticker", "close", "high", "low", "open", "volume"]
        df = df.dropna(subset=["close"])
        df["extracted_at"] = datetime.utcnow()
    except Exception as e:
        print(f"Error normalizing price data: {e}")
        return pd.DataFrame()

    print(f"Extracted {len(df)} price rows")
    return df


def extract_company_info(
    tickers: List[str],
    delay_seconds: float = 2.0
) -> pd.DataFrame:
    """
    Extract company metadata from yfinance with rate limiting.
    Each ticker requires a separate API call so we add a delay.
    """
    records = []
    total = len(tickers)

    for i, ticker in enumerate(tickers, 1):
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
            if i % 50 == 0:
                print(f"  Metadata progress: {i}/{total} tickers")
        except Exception as e:
            print(f"Warning: could not fetch info for {ticker}: {e}")
            records.append({
                "ticker": ticker,
                "company_name": None,
                "sector": None,
                "industry": None,
                "market_cap": None,
                "extracted_at": datetime.utcnow()
            })

        # Rate limiting delay between metadata calls
        if i < total:
            time.sleep(delay_seconds)

    return pd.DataFrame(records)