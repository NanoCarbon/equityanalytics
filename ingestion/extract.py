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


def get_tickers_from_db(conn) -> tuple[List[str], List[str]]:
    """
    Primary ticker source: read active tickers from RAW.TICKER_UNIVERSE.

    Returns (all_tickers, equity_tickers) where:
      - all_tickers    : every active ticker (S&P 1500 + ETFs)
      - equity_tickers : active tickers with is_equity=TRUE (S&P 1500, no ETFs)

    Falls back to the Wikipedia scrape + hardcoded ETF list on any DB error.
    """
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT ticker, is_equity
            FROM   EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE
            WHERE  is_active = TRUE
            ORDER  BY ticker
        """)
        rows = cur.fetchall()
        cur.close()
        all_tickers    = [r[0] for r in rows]
        equity_tickers = [r[0] for r in rows if r[1]]
        logger.info(
            "Loaded %d active tickers from TICKER_UNIVERSE (%d equity, %d ETF/other)",
            len(all_tickers), len(equity_tickers), len(all_tickers) - len(equity_tickers),
        )
        return all_tickers, equity_tickers
    except Exception as exc:
        logger.warning(
            "Could not load TICKER_UNIVERSE from DB (%s) -- falling back to Wikipedia scrape",
            exc,
        )
        all_t    = get_all_tickers()
        equity_t = get_equity_tickers()
        return all_t, equity_t


def get_all_tickers() -> List[str]:
    """
    Fallback ticker source: scrape S&P 1500 components live from Wikipedia
    and combine with the hardcoded ETF list.

    Prefer get_tickers_from_db() at runtime -- this function is called by
    get_tickers_from_db() on DB failure and by the ticker_universe_sync DAG
    to build the universe that gets written to RAW.TICKER_UNIVERSE.

    Falls back to static sample lists if Wikipedia is unreachable.
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
    Fallback equity-only ticker source (no ETFs) -- for fundamentals extraction.

    Prefer get_tickers_from_db() at runtime. This function is the fallback
    when the DB is unreachable and is used by the ticker_universe_sync DAG.
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
        # Airflow XCom serialises task return values as JSON, so get_max_date()
        # returns a string ('2026-05-19'). Accept both str and date.
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)
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
        # After stack the first two columns are always the date and ticker index
        # levels (their exact names vary by yfinance version).  Newer yfinance
        # releases also emit extra columns (e.g. "Dividends", "Capital Gains")
        # when auto_adjust=True, which breaks a fixed positional rename.
        # Rename by mapping known names, then select only the OHLCV subset.
        col_names = list(df.columns)
        rename_map = {
            col_names[0]: "date",   # first index level  → date
            col_names[1]: "ticker", # second index level → ticker
        }
        for col in col_names[2:]:
            col_lower = str(col).lower()
            if col_lower in ("close", "high", "low", "open", "volume"):
                rename_map[col] = col_lower
        df = df.rename(columns=rename_map)
        df = df[["date", "ticker", "close", "high", "low", "open", "volume"]]
        df = df.dropna(subset=["close"])
        df["extracted_at"] = datetime.utcnow()
    except Exception as e:
        logger.error("Error normalizing price data: %s", e)
        raise

    logger.info("Extracted %d price rows", len(df))
    return df


def extract_company_info(
    tickers: List[str],
    delay_seconds: float = 2.0,
    batch_size: int = 100,
    batch_pause: float = 30.0,
) -> pd.DataFrame:
    """
    Extract company metadata from yfinance with rate limiting.
    Each ticker requires a separate API call so we add a delay.
    Per-ticker failures are caught and filled with nulls so the batch continues.

    Rate-limited with per-ticker delay AND a longer batch pause every `batch_size`
    tickers. equity_daily and valuation_daily both fire at 11pm ET and both hit
    the .info endpoint — without batch pauses Yahoo Finance silently throttles
    after ~270 tickers (same failure mode seen in financial statements).
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

        # Per-ticker delay
        if i < total:
            time.sleep(delay_seconds)

        # Batch pause every batch_size tickers to let Yahoo Finance rate limit reset
        if i % batch_size == 0 and i < total:
            logger.info("Batch pause %ds after %d tickers...", batch_pause, i)
            time.sleep(batch_pause)

    logger.info("Extracted metadata for %d tickers", len(records))
    return pd.DataFrame(records)


def extract_dividends_and_splits(
    tickers: List[str],
    delay_seconds: float = 0.5,
    batch_size: int = 150,
    batch_pause: float = 15.0,
) -> pd.DataFrame:
    """
    Extract full dividend and stock split history for all tickers.
    yfinance returns history back to IPO — this is a full overwrite source.
    Per-ticker failures are skipped so the batch continues.

    Rate-limited with per-ticker delay plus a batch pause every `batch_size`
    tickers. equity_supplemental_weekly runs four tasks in parallel (dividends,
    earnings, recommendations, price targets) — batch pauses prevent sustained
    high-volume load from triggering Yahoo Finance's silent throttle.
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
        if i % batch_size == 0 and i < total:
            logger.info("Batch pause %ds after %d tickers (dividends)...", batch_pause, i)
            time.sleep(batch_pause)

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
    batch_size: int = 150,
    batch_pause: float = 15.0,
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
        if i % batch_size == 0 and i < total:
            logger.info("Batch pause %ds after %d tickers (earnings)...", batch_pause, i)
            time.sleep(batch_pause)

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
    batch_size: int = 150,
    batch_pause: float = 15.0,
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
        if i % batch_size == 0 and i < total:
            logger.info("Batch pause %ds after %d tickers (recommendations)...", batch_pause, i)
            time.sleep(batch_pause)

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
    batch_size: int = 150,
    batch_pause: float = 15.0,
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
        if i % batch_size == 0 and i < total:
            logger.info("Batch pause %ds after %d tickers (price targets)...", batch_pause, i)
            time.sleep(batch_pause)

    if not records:
        logger.warning("No analyst price targets returned for any ticker")
        return pd.DataFrame()

    result = pd.DataFrame(records)
    logger.info(
        "Extracted analyst price targets for %d tickers",
        result["ticker"].nunique() if "ticker" in result.columns else 0,
    )
    return result