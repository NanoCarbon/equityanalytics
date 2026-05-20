import yfinance as yf
import pandas as pd
import time
import logging
import requests
from datetime import datetime, timedelta, date
from typing import List
from io import StringIO

logger = logging.getLogger(__name__)

# Timeout for external HTTP calls (seconds)
HTTP_TIMEOUT = 30


def _fetch_wikipedia_sp_tickers(url: str, index_name: str) -> List[str]:
    """
    Shared helper: fetch S&P index components from a Wikipedia table.
    Tries common ticker column names across different index pages.
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    response = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    tables = pd.read_html(StringIO(response.text))
    df = tables[0]
    for col in ('Symbol', 'Ticker symbol', 'Ticker'):
        if col in df.columns:
            tickers = [str(t).replace('.', '-') for t in df[col].tolist() if pd.notna(t)]
            logger.info("Fetched %d %s tickers from Wikipedia", len(tickers), index_name)
            return tickers
    raise ValueError(
        f"Could not find ticker column in {index_name} Wikipedia table. "
        f"Available columns: {df.columns.tolist()}"
    )


def get_sp500_tickers() -> List[str]:
    """Fetch current S&P 500 large-cap components from Wikipedia."""
    try:
        return _fetch_wikipedia_sp_tickers(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            "S&P 500",
        )
    except requests.Timeout:
        logger.warning("Wikipedia S&P 500 request timed out -- using fallback list")
        return FALLBACK_SP500
    except Exception as e:
        logger.warning("Could not fetch S&P 500 tickers: %s -- using fallback list", e)
        return FALLBACK_SP500


def get_sp400_tickers() -> List[str]:
    """Fetch current S&P 400 mid-cap components from Wikipedia."""
    try:
        return _fetch_wikipedia_sp_tickers(
            "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies",
            "S&P 400",
        )
    except requests.Timeout:
        logger.warning("Wikipedia S&P 400 request timed out -- using fallback list")
        return FALLBACK_SP400
    except Exception as e:
        logger.warning("Could not fetch S&P 400 tickers: %s -- using fallback list", e)
        return FALLBACK_SP400


def get_sp600_tickers() -> List[str]:
    """Fetch current S&P 600 small-cap components from Wikipedia."""
    try:
        return _fetch_wikipedia_sp_tickers(
            "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies",
            "S&P 600",
        )
    except requests.Timeout:
        logger.warning("Wikipedia S&P 600 request timed out -- using fallback list")
        return FALLBACK_SP600
    except Exception as e:
        logger.warning("Could not fetch S&P 600 tickers: %s -- using fallback list", e)
        return FALLBACK_SP600


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


# ── Fallback lists (used when Wikipedia is unreachable) ───────────────────────
# These are representative samples only — the live Wikipedia scrape is always
# preferred and returns the full current index membership.

FALLBACK_SP500 = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "BRK-B",
    "JPM", "JNJ", "V", "UNH", "XOM", "PG", "MA", "HD", "CVX", "MRK",
    "LLY", "ABBV", "PEP", "KO", "AVGO", "COST", "TMO", "MCD", "ACN",
    "ABT", "DHR", "TXN", "NEE", "PM", "RTX", "HON", "UPS", "AMGN",
    "SBUX", "IBM", "GE", "GS", "BLK", "MS", "AXP", "SPGI", "CAT",
    "BA", "MMM", "DE", "LMT", "NOW",
]

FALLBACK_SP400 = [
    # Mid-cap representative sample (~400 components)
    "TXRH", "SAIA", "TREX", "RPM", "OLN", "HRB", "GGG", "UFPI",
    "CALM", "CHDN", "MORN", "WTFC", "CFR", "GBCI", "SSB", "RLI",
    "WDFC", "LANC", "SPSC", "SFM", "FRPT", "WEX", "BDC", "ITT",
    "BWXT", "CNO", "COOP", "EEFT", "ENSG", "EPAM", "EXP", "FHI",
    "GATX", "GFF", "HALO", "IBP", "INGR", "INVA", "JEF", "KMPR",
    "LGND", "LIVN", "LPX", "MMS", "MTZ", "NAVI", "ONB", "PB",
    "PIPR", "PNM", "POWI", "RBC", "RGEN", "ROIC", "SLG", "SNV",
]

FALLBACK_SP600 = [
    # Small-cap representative sample (~600 components)
    "BOOT", "CWST", "JBSS", "PLPC", "PLAB", "CLFD", "UFPT", "HAYN",
    "MCRI", "ODC", "WSBC", "LBAI", "CFB", "SEM", "MPX", "CAKE",
    "CACC", "CABO", "CCOI", "CBSH", "CEIX", "CHCO", "CMCO", "CNMD",
    "COHU", "CORE", "CSR", "CTBI", "CTRE", "CVBF", "CVCO", "DAKT",
    "DFIN", "DLX", "DXPE", "EFSC", "EMBC", "EPRT", "ESSA", "ESE",
    "ETD", "EVTC", "EXTR", "FCF", "FCPT", "FISI", "FLGT", "FMNB",
    "FORM", "FRME", "GBLI", "GES", "HLIT", "HMN", "HWKN", "INVA",
    "IPAR", "JBGS", "KFY", "KFRC", "KRT", "LKFN", "LMAT", "LQDT",
]


def get_all_tickers() -> List[str]:
    """
    Get the full S&P Composite 1500 + ETF ticker universe, deduplicated.

    S&P 1500 = S&P 500 (large-cap) + S&P 400 (mid-cap) + S&P 600 (small-cap).
    Covers ~90% of US investable market cap. Together with the ETF list this
    produces roughly 1,600 unique tickers.

    All three index lists are fetched live from Wikipedia on each call so index
    rebalances are picked up automatically. Falls back to static lists if
    Wikipedia is unavailable.
    """
    sp500 = get_sp500_tickers()
    sp400 = get_sp400_tickers()
    sp600 = get_sp600_tickers()
    etfs  = get_etf_tickers()
    all_tickers = list(dict.fromkeys(sp500 + sp400 + sp600 + etfs))
    logger.info(
        "Total unique tickers: %d  (S&P 500: %d | S&P 400: %d | S&P 600: %d | ETFs: %d)",
        len(all_tickers), len(sp500), len(sp400), len(sp600), len(etfs),
    )
    return all_tickers


def get_equity_tickers() -> List[str]:
    """
    Get S&P 1500 equity-only tickers (no ETFs) -- for fundamentals extraction.

    ETFs are excluded because they have no earnings, financial statements,
    or analyst coverage. Returns ~1,500 tickers.
    """
    sp500 = get_sp500_tickers()
    sp400 = get_sp400_tickers()
    sp600 = get_sp600_tickers()
    equity_tickers = list(dict.fromkeys(sp500 + sp400 + sp600))
    logger.info("Total unique equity tickers: %d", len(equity_tickers))
    return equity_tickers


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

    Returns empty DataFrame if no data is available for the requested range.
    Raises on unexpected yfinance or normalization errors so callers can retry.
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

    logger.info(
        "Extracting prices for %d tickers from %s to %s",
        len(tickers), effective_start, effective_end
    )

    try:
        raw = yf.download(
            tickers=tickers,
            start=effective_start,
            end=effective_end,
            auto_adjust=True,
            progress=False,
            group_by='ticker',
            timeout=HTTP_TIMEOUT,
        )
    except Exception as e:
        logger.error("yfinance download failed: %s", e)
        raise

    if raw.empty:
        logger.info("No price data returned for date range %s to %s", effective_start, effective_end)
        return pd.DataFrame()

    try:
        df = raw.stack(level=0, future_stack=True).reset_index()
        df.columns = ["date", "ticker", "close", "high", "low", "open", "volume"]
        df = df.dropna(subset=["close"])
        df["extracted_at"] = datetime.utcnow()
    except Exception as e:
        logger.error("Error normalizing price data: %s", e)
        raise

    logger.info("Extracted %d price rows", len(df))
    return df


def extract_company_info(
    tickers: List[str],
    delay_seconds: float = 2.0
) -> pd.DataFrame:
    """
    Extract company metadata from yfinance with rate limiting.
    Each ticker requires a separate API call so we add a delay.
    Per-ticker failures are caught and filled with nulls so the batch continues.
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
        except Exception as e:
            logger.warning("Could not fetch info for %s: %s", ticker, e)
            records.append({
                "ticker": ticker,
                "company_name": None,
                "sector": None,
                "industry": None,
                "market_cap": None,
                "extracted_at": datetime.utcnow()
            })

        if i % 50 == 0:
            logger.info("Metadata progress: %d/%d tickers", i, total)

        if i < total:
            time.sleep(delay_seconds)

    logger.info("Extracted metadata for %d tickers", len(records))
    return pd.DataFrame(records)


def extract_dividends_and_splits(
    tickers: List[str],
    delay_seconds: float = 0.5,
) -> pd.DataFrame:
    """
    Extract full dividend and stock split history for all tickers.
    yfinance returns history back to IPO — this is a full overwrite source.
    Per-ticker failures are skipped so the batch continues.
    """
    records = []
    total = len(tickers)

    for i, ticker in enumerate(tickers, 1):
        try:
            actions = yf.Ticker(ticker).actions  # Date index, Dividends + Stock Splits cols
            if actions is not None and not actions.empty:
                df = actions.reset_index()
                df.columns = [c.lower().replace(" ", "_") for c in df.columns]
                df["ticker"] = ticker
                df["extracted_at"] = datetime.utcnow()
                records.append(df)
        except Exception as e:
            logger.warning("Could not fetch actions for %s: %s", ticker, e)

        if i % 100 == 0:
            logger.info("Dividends/splits progress: %d/%d", i, total)
        if i < total:
            time.sleep(delay_seconds)

    if not records:
        logger.warning("No dividend/split data returned for any ticker")
        return pd.DataFrame()

    result = pd.concat(records, ignore_index=True)
    logger.info(
        "Extracted %d dividend/split rows for %d tickers",
        len(result), result["ticker"].nunique(),
    )
    return result


def extract_earnings_history(
    tickers: List[str],
    delay_seconds: float = 0.5,
) -> pd.DataFrame:
    """
    Extract EPS actuals vs. analyst estimates history from yfinance.
    Covers ~8–20 quarters per ticker. Full overwrite source.
    """
    records = []
    total = len(tickers)

    for i, ticker in enumerate(tickers, 1):
        try:
            hist = yf.Ticker(ticker).earnings_history
            if hist is not None and not hist.empty:
                df = hist.reset_index()
                df.columns = [c.lower().replace(" ", "_") for c in df.columns]
                df["ticker"] = ticker
                df["extracted_at"] = datetime.utcnow()
                records.append(df)
        except Exception as e:
            logger.warning("Could not fetch earnings history for %s: %s", ticker, e)

        if i % 100 == 0:
            logger.info("Earnings history progress: %d/%d", i, total)
        if i < total:
            time.sleep(delay_seconds)

    if not records:
        logger.warning("No earnings history returned for any ticker")
        return pd.DataFrame()

    result = pd.concat(records, ignore_index=True)
    logger.info(
        "Extracted %d earnings rows for %d tickers",
        len(result), result["ticker"].nunique(),
    )
    return result


def extract_analyst_recommendations(
    tickers: List[str],
    delay_seconds: float = 0.5,
) -> pd.DataFrame:
    """
    Extract analyst firm upgrade/downgrade history (Buy / Hold / Sell ratings).
    Returns several years of rating changes per ticker. Full overwrite source.
    """
    records = []
    total = len(tickers)

    for i, ticker in enumerate(tickers, 1):
        try:
            recs = yf.Ticker(ticker).recommendations
            if recs is not None and not recs.empty:
                df = recs.reset_index()
                df.columns = [c.lower().replace(" ", "_") for c in df.columns]
                df["ticker"] = ticker
                df["extracted_at"] = datetime.utcnow()
                records.append(df)
        except Exception as e:
            logger.warning("Could not fetch recommendations for %s: %s", ticker, e)

        if i % 100 == 0:
            logger.info("Recommendations progress: %d/%d", i, total)
        if i < total:
            time.sleep(delay_seconds)

    if not records:
        logger.warning("No analyst recommendations returned for any ticker")
        return pd.DataFrame()

    result = pd.concat(records, ignore_index=True)
    logger.info(
        "Extracted %d recommendation rows for %d tickers",
        len(result), result["ticker"].nunique(),
    )
    return result


def extract_analyst_price_targets(
    tickers: List[str],
    delay_seconds: float = 0.5,
) -> pd.DataFrame:
    """
    Extract current analyst price target consensus (mean, high, low, count).
    Returns one row per ticker — append daily to build a time series.
    """
    from datetime import date as _date
    records = []
    total = len(tickers)
    snapshot_date = _date.today()

    for i, ticker in enumerate(tickers, 1):
        try:
            pt = yf.Ticker(ticker).analyst_price_targets
            if pt is None:
                pass
            elif isinstance(pt, dict) and pt:
                record = {k.lower(): v for k, v in pt.items()}
                record["ticker"] = ticker
                record["snapshot_date"] = snapshot_date
                record["extracted_at"] = datetime.utcnow()
                records.append(record)
            elif isinstance(pt, pd.DataFrame) and not pt.empty:
                df = pt.reset_index()
                df.columns = [c.lower().replace(" ", "_") for c in df.columns]
                df["ticker"] = ticker
                df["snapshot_date"] = snapshot_date
                df["extracted_at"] = datetime.utcnow()
                records.extend(df.to_dict("records"))
        except Exception as e:
            logger.warning("Could not fetch price targets for %s: %s", ticker, e)

        if i % 100 == 0:
            logger.info("Price targets progress: %d/%d", i, total)
        if i < total:
            time.sleep(delay_seconds)

    if not records:
        logger.warning("No analyst price targets returned for any ticker")
        return pd.DataFrame()

    result = pd.DataFrame(records)
    logger.info(
        "Extracted analyst price targets for %d tickers",
        result["ticker"].nunique() if "ticker" in result.columns else 0,
    )
    return result