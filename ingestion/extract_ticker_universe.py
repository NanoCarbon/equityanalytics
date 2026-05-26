"""
Extract and normalise ticker universe data from multiple external sources.

Sources implemented here:
  - NASDAQ Trader flat files: all US exchange-listed equities + ETFs (Phase 2)
  - International index Wikipedia pages (Phase 3 — not yet implemented)

This module is pure data extraction — no DB writes. The seed script
(ingestion/seed_ticker_universe.py) and sync DAG (dag_ticker_universe_sync.py)
orchestrate the actual MERGE operations against RAW.TICKER_UNIVERSE.

Returned row format (dict) for all functions:
  ticker          str   — full yfinance-ready symbol (suffix included for international)
  name            str   — security name / company name
  exchange        str   — exchange identifier (NASDAQ, NYSE, NYSE_ARCA, NYSE_AMEX, etc.)
  country         str   — ISO-2 country code
  is_equity       bool  — True for common equities; False for ETFs, funds, preferreds
  source          str   — TICKER_UNIVERSE source label (e.g. 'nasdaq_trader')
  yfinance_suffix str   — suffix appended to base symbol for yfinance ('' for US)
"""

import logging
import requests
import pandas as pd
from io import StringIO
from typing import List, Dict

logger = logging.getLogger(__name__)

HTTP_TIMEOUT = 60  # NASDAQ flat files can be >5MB; 60s prevents timeout on slow connections
_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    )
}

# ── NASDAQ Trader URLs ────────────────────────────────────────────────────────

# Official NASDAQ-published daily flat files listing all US exchange-listed securities.
# Free, no API key. Updated every business day by NASDAQ Operations.
NASDAQ_LISTED_URL = 'http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt'
OTHER_LISTED_URL  = 'http://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt'

# Exchange code → human-readable name from otherlisted.txt 'Exchange' column
_EXCHANGE_MAP = {
    'A': 'NYSE_AMEX',   # NYSE American (formerly AMEX)
    'N': 'NYSE',        # New York Stock Exchange
    'P': 'NYSE_ARCA',   # NYSE Arca
    'Z': 'BATS',        # Cboe BZX (formerly BATS)
    'V': 'IEX',         # Investors Exchange
}

# Financial Status codes indicating abnormal trading state — exclude these.
# N = Normal; D = Deficient; E = Delinquent; Q = Bankrupt; S = Suspended;
# G = Deficient+Bankrupt; H = Deficient+Delinquent+Bankrupt;
# J = Delinquent+Bankrupt; K = Deficient+Delinquent
_EXCLUDE_FINANCIAL_STATUS = frozenset({'D', 'E', 'Q', 'S', 'G', 'H', 'J', 'K'})


def _make_row(
    ticker: str,
    name: str,
    exchange: str,
    country: str,
    is_equity: bool,
    source: str,
    yfinance_suffix: str = '',
) -> Dict:
    """Build a normalised ticker row dict."""
    return {
        'ticker':          ticker,
        'name':            name[:200],  # guard against very long names
        'exchange':        exchange,
        'country':         country,
        'is_equity':       is_equity,
        'source':          source,
        'yfinance_suffix': yfinance_suffix,
    }


def _strip_file_creation_line(text: str) -> str:
    """
    NASDAQ Trader files append a 'File Creation Time: ...' trailer line.
    Strip it before parsing so pandas doesn't choke.
    """
    return '\n'.join(
        line for line in text.splitlines()
        if not line.startswith('File Creation Time')
    )


# ── US Universe: NASDAQ Trader files ─────────────────────────────────────────

def fetch_nasdaq_trader_us() -> List[Dict]:
    """
    Download and parse both NASDAQ Trader flat files to produce a complete
    list of all US exchange-listed securities.

    Covers:
      nasdaqlisted.txt  — NASDAQ Global Select, Global Market, Capital Market
      otherlisted.txt   — NYSE, NYSE American (AMEX), NYSE Arca, BATS, IEX

    Filters applied (both files):
      - Test Issue = 'N'                     (exclude test/dummy securities)
      - Financial Status = 'N'               (Normal only; skip bankrupt/suspended)
      - Symbol must not contain '$' or '^'   (warrants, rights, index symbols)
      - Symbol must not be empty

    ETF detection:
      - ETF flag = 'Y' in the file → is_equity = False
      - ETF flag = 'N'             → is_equity = True (common equity assumed)

    Returns:
      List of row dicts suitable for MERGE into RAW.TICKER_UNIVERSE.
      May contain duplicates if a security is cross-listed — caller deduplicates.
    """
    results: List[Dict] = []

    # ── 1. nasdaqlisted.txt ──────────────────────────────────────────────────
    try:
        resp = requests.get(NASDAQ_LISTED_URL, headers=_HEADERS, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        clean_text = _strip_file_creation_line(resp.text)
        df = pd.read_csv(StringIO(clean_text), sep='|', dtype=str)

        # Normalise column names (file has trailing spaces sometimes)
        df.columns = [c.strip() for c in df.columns]

        pre_count = len(results)
        for _, row in df.iterrows():
            sym = str(row.get('Symbol', '')).strip()
            if not sym or sym == 'Symbol':
                continue
            if str(row.get('Test Issue', 'N')).strip().upper() == 'Y':
                continue
            fin_status = str(row.get('Financial Status', 'N')).strip().upper()
            if fin_status in _EXCLUDE_FINANCIAL_STATUS:
                continue
            if '$' in sym or '^' in sym:
                continue

            is_etf = str(row.get('ETF', 'N')).strip().upper() == 'Y'
            name   = str(row.get('Security Name', '')).strip()

            results.append(_make_row(
                ticker=sym,
                name=name,
                exchange='NASDAQ',
                country='US',
                is_equity=not is_etf,
                source='nasdaq_trader',
                yfinance_suffix='',
            ))

        logger.info(
            "nasdaqlisted.txt: parsed %d securities",
            len(results) - pre_count,
        )

    except Exception as exc:
        logger.error("Failed to fetch nasdaqlisted.txt: %s", exc)
        raise

    # ── 2. otherlisted.txt ───────────────────────────────────────────────────
    try:
        resp = requests.get(OTHER_LISTED_URL, headers=_HEADERS, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        clean_text = _strip_file_creation_line(resp.text)
        df = pd.read_csv(StringIO(clean_text), sep='|', dtype=str)
        df.columns = [c.strip() for c in df.columns]

        pre_count = len(results)
        for _, row in df.iterrows():
            # 'ACT Symbol' is the primary tradeable symbol
            sym = str(row.get('ACT Symbol', '')).strip()
            if not sym or sym == 'ACT Symbol':
                continue
            if str(row.get('Test Issue', 'N')).strip().upper() == 'Y':
                continue
            if '$' in sym or '^' in sym:
                continue
            # Financial Status filter: default to 'N' (Normal) if column absent from
            # this file's schema — safe fallback, no securities incorrectly excluded.
            fin_status = str(row.get('Financial Status', 'N')).strip().upper()
            if fin_status in _EXCLUDE_FINANCIAL_STATUS:
                continue

            exchange_code = str(row.get('Exchange', 'N')).strip().upper()
            exchange      = _EXCHANGE_MAP.get(exchange_code, 'NYSE')
            is_etf        = str(row.get('ETF', 'N')).strip().upper() == 'Y'
            name          = str(row.get('Security Name', '')).strip()

            results.append(_make_row(
                ticker=sym,
                name=name,
                exchange=exchange,
                country='US',
                is_equity=not is_etf,
                source='nasdaq_trader',
                yfinance_suffix='',
            ))

        logger.info(
            "otherlisted.txt: parsed %d securities (running total: %d)",
            len(results) - pre_count,
            len(results),
        )

    except Exception as exc:
        logger.error("Failed to fetch otherlisted.txt: %s", exc)
        raise

    # ── 3. Deduplicate on ticker symbol ──────────────────────────────────────
    # If a symbol appears in both files (cross-listed), keep the first occurrence
    # (nasdaqlisted takes priority as it has more metadata fields).
    seen: set = set()
    deduped: List[Dict] = []
    for row in results:
        if row['ticker'] not in seen:
            seen.add(row['ticker'])
            deduped.append(row)

    dupes = len(results) - len(deduped)
    if dupes:
        logger.info("Removed %d cross-listed duplicates; %d unique symbols", dupes, len(deduped))

    return deduped


# ── International indices (Phase 3) ──────────────────────────────────────────
# All five functions scrape Wikipedia constituent tables using the same
# requests + pd.read_html pattern.  Each returns a list of normalised row
# dicts ready for MERGE into RAW.TICKER_UNIVERSE.

_WIKI_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    )
}


def _fetch_wikipedia_table(
    url: str,
    index_name: str,
    ticker_candidates: list[str],
) -> pd.DataFrame:
    """
    Download a page and return the first table that contains one of the
    ticker_candidates column names.

    Wikipedia pages often have multiple tables (info boxes, historical data,
    navigation boxes) before the constituent list.  Scanning by column name
    rather than hardcoding a table index makes the function robust to page
    layout changes.

    Raises RuntimeError on HTTP error, ValueError if no matching table is found.
    """
    response = requests.get(url, headers=_WIKI_HEADERS, timeout=HTTP_TIMEOUT)
    if not response.ok:
        raise RuntimeError(
            f"HTTP {response.status_code} fetching {index_name} page"
        )
    tables = pd.read_html(StringIO(response.text))
    if not tables:
        raise ValueError(f"No tables found on {index_name} page")

    for i, df in enumerate(tables):
        for col in ticker_candidates:
            if col in df.columns:
                logger.debug(
                    "%s: found ticker column '%s' in table %d (of %d)",
                    index_name, col, i, len(tables),
                )
                return df

    raise ValueError(
        f"No table on {index_name} page contained a ticker column. "
        f"Tried: {ticker_candidates}. "
        f"Tables found: {[list(t.columns)[:4] for t in tables]}"
    )


def _fetch_url_tables(url: str, index_name: str) -> list[pd.DataFrame]:
    """
    Download a page and return all parsed tables.  Used for sources (e.g. JPX)
    where the constituent data is spread across multiple tables.
    """
    response = requests.get(url, headers=_WIKI_HEADERS, timeout=HTTP_TIMEOUT)
    if not response.ok:
        raise RuntimeError(
            f"HTTP {response.status_code} fetching {index_name} page"
        )
    tables = pd.read_html(StringIO(response.text))
    if not tables:
        raise ValueError(f"No tables found on {index_name} page")
    logger.debug("%s: found %d table(s)", index_name, len(tables))
    return tables


def _pick_column(df: pd.DataFrame, candidates: list[str], index_name: str) -> str:
    """Return the first candidate column name that exists in df.columns."""
    for col in candidates:
        if col in df.columns:
            return col
    raise ValueError(
        f"Could not find column in {index_name} table. "
        f"Tried: {candidates}. Available: {df.columns.tolist()}"
    )


def fetch_ftse100() -> List[Dict]:
    """
    FTSE 100 constituents from Wikipedia.

    URL: https://en.wikipedia.org/wiki/FTSE_100_Index
    Ticker column: 'Ticker' (current as of 2026) — page layout has changed from
                   'EPIC' in the past; '_fetch_wikipedia_table' scans for either.
    yfinance suffix: .L
    Exchange: LSE
    Country: GB
    """
    _TICKER_CANDIDATES = ['Ticker', 'EPIC', 'Symbol']
    _NAME_CANDIDATES   = ['Company', 'Name', 'Security']

    url = 'https://en.wikipedia.org/wiki/FTSE_100_Index'
    df  = _fetch_wikipedia_table(url, 'FTSE 100', _TICKER_CANDIDATES)

    ticker_col = _pick_column(df, _TICKER_CANDIDATES, 'FTSE 100')
    name_col   = _pick_column(df, _NAME_CANDIDATES,   'FTSE 100')

    results: List[Dict] = []
    for _, row in df.iterrows():
        sym = str(row[ticker_col]).strip()
        if not sym or sym == ticker_col:
            continue
        name = str(row.get(name_col, '')).strip()
        results.append(_make_row(
            ticker=f'{sym}.L',
            name=name,
            exchange='LSE',
            country='GB',
            is_equity=True,
            source='ftse100',
            yfinance_suffix='.L',
        ))

    logger.info("fetch_ftse100: %d constituents", len(results))
    return results


def fetch_tsx60() -> List[Dict]:
    """
    S&P/TSX 60 constituents from Wikipedia.

    URL: https://en.wikipedia.org/wiki/S%26P/TSX_60
    Ticker column: 'Symbol' (current layout; was 'Ticker symbol' historically)
    yfinance suffix: .TO
    Exchange: TSX
    Country: CA
    """
    _TICKER_CANDIDATES = ['Symbol', 'Ticker symbol', 'Ticker']
    _NAME_CANDIDATES   = ['Company', 'Name', 'Security']

    url = 'https://en.wikipedia.org/wiki/S%26P/TSX_60'
    df  = _fetch_wikipedia_table(url, 'TSX 60', _TICKER_CANDIDATES)

    ticker_col = _pick_column(df, _TICKER_CANDIDATES, 'TSX 60')
    name_col   = _pick_column(df, _NAME_CANDIDATES,   'TSX 60')

    results: List[Dict] = []
    for _, row in df.iterrows():
        sym = str(row[ticker_col]).strip()
        if not sym or sym == ticker_col:
            continue
        name = str(row.get(name_col, '')).strip()
        results.append(_make_row(
            ticker=f'{sym}.TO',
            name=name,
            exchange='TSX',
            country='CA',
            is_equity=True,
            source='tsx60',
            yfinance_suffix='.TO',
        ))

    logger.info("fetch_tsx60: %d constituents", len(results))
    return results


def fetch_asx200() -> List[Dict]:
    """
    S&P/ASX 200 constituents from Wikipedia.

    URL: https://en.wikipedia.org/wiki/S%26P/ASX_200
    Ticker column: 'Code'
    yfinance suffix: .AX
    Exchange: ASX
    Country: AU
    """
    _TICKER_CANDIDATES = ['Code', 'Ticker', 'Symbol']
    _NAME_CANDIDATES   = ['Company', 'Name', 'Security']

    url = 'https://en.wikipedia.org/wiki/S%26P/ASX_200'
    df  = _fetch_wikipedia_table(url, 'ASX 200', _TICKER_CANDIDATES)

    ticker_col = _pick_column(df, _TICKER_CANDIDATES, 'ASX 200')
    name_col   = _pick_column(df, _NAME_CANDIDATES,   'ASX 200')

    results: List[Dict] = []
    for _, row in df.iterrows():
        sym = str(row[ticker_col]).strip()
        if not sym or sym == ticker_col:
            continue
        name = str(row.get(name_col, '')).strip()
        results.append(_make_row(
            ticker=f'{sym}.AX',
            name=name,
            exchange='ASX',
            country='AU',
            is_equity=True,
            source='asx200',
            yfinance_suffix='.AX',
        ))

    logger.info("fetch_asx200: %d constituents", len(results))
    return results


# Nikkei 225 official constituent source
# Wikipedia's Nikkei 225 page does not have a structured constituent table with
# TSE codes.  The Nikkei official index page (Nikkei Indexes) does, and it is
# publicly accessible without authentication or API keys.
_JPX_NIKKEI225_URL = 'https://indexes.nikkei.co.jp/en/nkave/index/component?idx=nk225'


def fetch_nikkei225() -> List[Dict]:
    """
    Nikkei 225 constituents from the official Nikkei Indexes page.

    Source: https://indexes.nikkei.co.jp/en/nkave/index/component?idx=nk225
    The page renders multiple tables (one per industry sector) each with
    columns 'Code' (TSE ticker code) and 'Company Name'.

    yfinance format: '{code}.T'  (e.g. '7203.T' for Toyota)
    Codes may be 4-digit numeric (legacy) or alphanumeric (e.g. '285A' for
    KIOXIA) — both are valid TSE codes supported by yfinance.
    Exchange: TSE (Tokyo Stock Exchange)
    Country: JP
    """
    all_tables = _fetch_url_tables(_JPX_NIKKEI225_URL, 'Nikkei 225')

    results: List[Dict] = []
    seen: set = set()
    for df in all_tables:
        if 'Code' not in df.columns or 'Company Name' not in df.columns:
            continue
        for _, row in df.iterrows():
            raw_code = str(row['Code']).strip()
            if not raw_code or raw_code == 'Code':
                continue
            # Strip any trailing '.0' that pandas adds when reading int-like floats
            code = raw_code.split('.')[0].strip() if raw_code.replace('.', '').isdigit() else raw_code
            if not code or code in seen:
                continue
            seen.add(code)
            name = str(row.get('Company Name', '')).strip()
            results.append(_make_row(
                ticker=f'{code}.T',
                name=name,
                exchange='TSE',
                country='JP',
                is_equity=True,
                source='nikkei225',
                yfinance_suffix='.T',
            ))

    logger.info("fetch_nikkei225: %d constituents", len(results))
    return results


def fetch_dax40() -> List[Dict]:
    """
    DAX 40 constituents from Wikipedia.

    URL: https://en.wikipedia.org/wiki/DAX
    Ticker column: 'Ticker' — Wikipedia already provides the full yfinance-ready
    symbol for each constituent.  Most are XETRA-listed (suffix .DE) but a small
    number trade on other exchanges, e.g. Airbus (AIR.PA on Euronext Paris).
    The ticker is stored as-is; yfinance_suffix reflects the actual suffix.

    Exchange: FWB (Frankfurt Stock Exchange / XETRA) for DE-listed constituents.
    Country: DE  (country of DAX membership, not necessarily primary listing).
    """
    _TICKER_CANDIDATES = ['Ticker', 'Symbol']
    _NAME_CANDIDATES   = ['Company', 'Name', 'Security']

    url = 'https://en.wikipedia.org/wiki/DAX'
    df  = _fetch_wikipedia_table(url, 'DAX 40', _TICKER_CANDIDATES)

    ticker_col = _pick_column(df, _TICKER_CANDIDATES, 'DAX 40')
    name_col   = _pick_column(df, _NAME_CANDIDATES,   'DAX 40')

    results: List[Dict] = []
    for _, row in df.iterrows():
        sym = str(row[ticker_col]).strip()
        if not sym or sym == ticker_col:
            continue
        # Wikipedia provides the correct yfinance ticker including exchange suffix.
        # Extract the suffix (e.g. '.DE', '.PA') for the yfinance_suffix field.
        if '.' in sym:
            suffix = '.' + sym.rsplit('.', 1)[1]
            ticker = sym
        else:
            # Rare fallback: symbol without suffix → assume XETRA
            suffix = '.DE'
            ticker = f'{sym}.DE'
        name = str(row.get(name_col, '')).strip()
        results.append(_make_row(
            ticker=ticker,
            name=name,
            exchange='FWB',
            country='DE',
            is_equity=True,
            source='dax40',
            yfinance_suffix=suffix,
        ))

    logger.info("fetch_dax40: %d constituents", len(results))
    return results
