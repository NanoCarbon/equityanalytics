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


# ── International indices (Phase 3 — stubs) ───────────────────────────────────
# These will be implemented in Phase 3. Stubs are here so import works cleanly.

def fetch_ftse100() -> List[Dict]:
    """FTSE 100 constituents from Wikipedia. Suffix: .L  (Phase 3)"""
    raise NotImplementedError("Phase 3: FTSE 100 not yet implemented")


def fetch_tsx60() -> List[Dict]:
    """S&P/TSX 60 constituents from Wikipedia. Suffix: .TO  (Phase 3)"""
    raise NotImplementedError("Phase 3: TSX 60 not yet implemented")


def fetch_asx200() -> List[Dict]:
    """S&P/ASX 200 constituents from Wikipedia. Suffix: .AX  (Phase 3)"""
    raise NotImplementedError("Phase 3: ASX 200 not yet implemented")


def fetch_nikkei225() -> List[Dict]:
    """Nikkei 225 constituents from Wikipedia. Suffix: .T  (Phase 3)"""
    raise NotImplementedError("Phase 3: Nikkei 225 not yet implemented")


def fetch_dax40() -> List[Dict]:
    """DAX 40 constituents from Wikipedia. Suffix: .DE  (Phase 3)"""
    raise NotImplementedError("Phase 3: DAX 40 not yet implemented")
