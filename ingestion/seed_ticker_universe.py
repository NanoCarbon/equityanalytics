"""
Seed script: populates RAW.TICKER_UNIVERSE from all configured sources.

Run order:
  1. S&P 1500 + ETF seed   — Wikipedia scrape, highest source priority
  2. NASDAQ Trader seed     — flat file, adds all other US-listed equities/ETFs
                              without overwriting existing S&P/ETF source labels

Usage (from repo root on Windows, with env vars set):
  python ingestion/seed_ticker_universe.py

Or inside the Airflow container:
  python3 ingestion/seed_ticker_universe.py

Safe to re-run — all writes use MERGE so existing rows are not duplicated.
"""

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

sys.path.insert(0, '/opt/airflow')  # works both locally and in container


# ── Helper ────────────────────────────────────────────────────────────────────

def _merge_rows(cur, rows: list[dict], chunk_size: int = 500) -> None:
    """
    MERGE a list of row dicts into RAW.TICKER_UNIVERSE.

    Source priority rule:
      - Existing tickers with source in (sp500, sp400, sp600, etf) keep their
        source and is_equity values — S&P/ETF membership always wins.
      - Other existing tickers adopt the incoming source (e.g. nasdaq_trader).
      - exchange/country/yfinance_suffix are always refreshed from the incoming
        data (factual metadata — no priority needed).
      - New tickers are inserted with all fields from the incoming row.
    """
    n_chunks = (len(rows) + chunk_size - 1) // chunk_size

    for chunk_idx, i in enumerate(range(0, len(rows), chunk_size)):
        chunk = rows[i : i + chunk_size]

        val_parts = []
        for r in chunk:
            t     = r['ticker'].replace("'", "''")
            src   = r['source'].replace("'", "''")
            exch  = (r.get('exchange') or '').replace("'", "''")
            cntry = (r.get('country') or 'US').replace("'", "''")
            sfx   = (r.get('yfinance_suffix') or '').replace("'", "''")
            is_eq = 'TRUE' if r.get('is_equity', True) else 'FALSE'
            val_parts.append(
                f"('{t}', '{src}', {is_eq}, '{exch}', '{cntry}', '{sfx}')"
            )

        values_sql = ', '.join(val_parts)

        cur.execute(f"""
            MERGE INTO EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE AS tgt
            USING (
                SELECT
                    column1 AS ticker,
                    column2 AS source,
                    column3 AS is_equity,
                    column4 AS exchange,
                    column5 AS country,
                    column6 AS yfinance_suffix
                FROM VALUES {values_sql}
            ) AS src ON tgt.ticker = src.ticker

            WHEN MATCHED THEN UPDATE SET
                -- Preserve S&P/ETF source priority; update metadata for all
                source          = CASE
                                    WHEN tgt.source IN ('sp500','sp400','sp600','etf')
                                    THEN tgt.source
                                    ELSE src.source
                                  END,
                is_equity       = CASE
                                    WHEN tgt.source IN ('sp500','sp400','sp600','etf')
                                    THEN tgt.is_equity
                                    ELSE src.is_equity
                                  END,
                exchange        = COALESCE(NULLIF(src.exchange, ''), tgt.exchange),
                country         = COALESCE(NULLIF(src.country,  ''), tgt.country),
                yfinance_suffix = COALESCE(src.yfinance_suffix, tgt.yfinance_suffix),
                is_active       = TRUE

            WHEN NOT MATCHED THEN INSERT
                (ticker, source, is_equity, is_active, country, yfinance_suffix, exchange)
            VALUES
                (src.ticker, src.source, src.is_equity, TRUE,
                 src.country, src.yfinance_suffix, src.exchange)
        """)

        logger.info(
            "  Chunk %d/%d — %d rows merged",
            chunk_idx + 1, n_chunks, len(chunk),
        )


# ── Step 1: S&P 1500 + ETF seed ───────────────────────────────────────────────

def seed_sp_and_etf(cur) -> None:
    """
    Seed from live Wikipedia scrapes for S&P 500/400/600 + hardcoded ETF list.
    This is the existing seed logic, updated to populate new columns.
    """
    from ingestion.extract import (
        get_sp500_tickers, get_sp400_tickers,
        get_sp600_tickers, get_etf_tickers,
    )

    sp500 = set(get_sp500_tickers())
    sp400 = set(get_sp400_tickers())
    sp600 = set(get_sp600_tickers())
    etfs  = set(get_etf_tickers())

    logger.info(
        "Wikipedia: S&P 500=%d | S&P 400=%d | S&P 600=%d | ETFs=%d",
        len(sp500), len(sp400), len(sp600), len(etfs),
    )

    # Build rows: priority sp500 > sp400 > sp600 > etf
    rows_by_ticker: dict[str, dict] = {}
    for t in sp600:
        rows_by_ticker[t] = {'ticker': t, 'source': 'sp600', 'is_equity': True,
                              'country': 'US', 'yfinance_suffix': '', 'exchange': None}
    for t in sp400:
        rows_by_ticker[t] = {'ticker': t, 'source': 'sp400', 'is_equity': True,
                              'country': 'US', 'yfinance_suffix': '', 'exchange': None}
    for t in sp500:
        rows_by_ticker[t] = {'ticker': t, 'source': 'sp500', 'is_equity': True,
                              'country': 'US', 'yfinance_suffix': '', 'exchange': None}
    for t in etfs:
        if t not in rows_by_ticker:
            rows_by_ticker[t] = {'ticker': t, 'source': 'etf', 'is_equity': False,
                                  'country': 'US', 'yfinance_suffix': '', 'exchange': None}

    rows = list(rows_by_ticker.values())
    logger.info("Merging %d S&P/ETF tickers...", len(rows))
    _merge_rows(cur, rows)
    logger.info("S&P/ETF seed complete (%d tickers)", len(rows))


# ── Step 2: NASDAQ Trader seed ────────────────────────────────────────────────

def seed_nasdaq_trader(cur) -> None:
    """
    Fetch the NASDAQ Trader flat files and MERGE all US-listed securities.
    Source priority is enforced: existing S&P/ETF tickers keep their source label.
    New tickers (not already in the table) are inserted as source='nasdaq_trader'.
    """
    from ingestion.extract_ticker_universe import fetch_nasdaq_trader_us

    logger.info("Fetching NASDAQ Trader files...")
    rows = fetch_nasdaq_trader_us()
    logger.info("Fetched %d unique US-listed securities — starting MERGE...", len(rows))
    _merge_rows(cur, rows)
    logger.info("NASDAQ Trader seed complete (%d rows processed)", len(rows))


# ── Step 3: International indices seed ───────────────────────────────────────

def seed_international_indices(cur) -> None:
    """
    Fetch FTSE 100, TSX 60, ASX 200, Nikkei 225, and DAX 40 constituents from
    Wikipedia and MERGE into RAW.TICKER_UNIVERSE.

    Each index is attempted independently — if one Wikipedia page is down or
    returns unexpected HTML, it is logged and skipped rather than aborting the
    entire seed run.  Existing US tickers sharing a base symbol are never
    touched because international tickers carry a suffix (e.g. .L, .TO).
    """
    from ingestion.extract_ticker_universe import (
        fetch_ftse100, fetch_tsx60, fetch_asx200,
        fetch_nikkei225, fetch_dax40,
    )

    fetchers = [
        ('FTSE 100',   fetch_ftse100),
        ('TSX 60',     fetch_tsx60),
        ('ASX 200',    fetch_asx200),
        ('Nikkei 225', fetch_nikkei225),
        ('DAX 40',     fetch_dax40),
    ]

    total_merged = 0
    for index_name, fetcher in fetchers:
        try:
            rows = fetcher()
            if not rows:
                logger.warning("%s: fetcher returned 0 rows — skipping", index_name)
                continue
            logger.info("%s: merging %d constituents...", index_name, len(rows))
            _merge_rows(cur, rows)
            total_merged += len(rows)
            logger.info("%s: MERGE complete", index_name)
        except Exception as exc:
            logger.error(
                "%s: fetch/merge failed — skipping index (%s: %s)",
                index_name, type(exc).__name__, exc,
            )

    logger.info("International indices seed complete (%d total rows processed)", total_merged)


# ── Step 4: Assign fundamentals_cohort ────────────────────────────────────────

def assign_fundamentals_cohort(cur) -> None:
    """
    Assign fundamentals_cohort (0-3) to all equity tickers that don't have one.
    Cohort = ABS(HASH(ticker)) % 4 — stable and deterministic per ticker symbol.
    ETFs get NULL (they have no financial statements).
    """
    cur.execute("""
        UPDATE EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE
        SET    fundamentals_cohort = ABS(HASH(ticker))::INTEGER % 4
        WHERE  is_equity = TRUE
          AND  fundamentals_cohort IS NULL
    """)
    logger.info("Assigned fundamentals_cohort to newly-added equity tickers")


# ── Main ──────────────────────────────────────────────────────────────────────

def seed_ticker_universe() -> None:
    from ingestion.load import get_connection

    conn = get_connection()
    cur  = conn.cursor()

    try:
        logger.info("=== Step 1: S&P 1500 + ETF seed ===")
        seed_sp_and_etf(cur)

        logger.info("\n=== Step 2: NASDAQ Trader seed ===")
        seed_nasdaq_trader(cur)

        logger.info("\n=== Step 3: International indices seed ===")
        seed_international_indices(cur)

        logger.info("\n=== Step 4: Assign fundamentals cohorts ===")
        assign_fundamentals_cohort(cur)

        # Final summary
        cur.execute("""
            SELECT
                source,
                COUNT(*)                                       AS total,
                SUM(CASE WHEN is_active  THEN 1 ELSE 0 END)   AS active,
                SUM(CASE WHEN is_equity  THEN 1 ELSE 0 END)   AS equities
            FROM EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE
            GROUP BY source
            ORDER BY active DESC
        """)
        rows = cur.fetchall()
        logger.info("\n=== Final TICKER_UNIVERSE state ===")
        logger.info("%-20s  %8s  %8s  %8s", "SOURCE", "TOTAL", "ACTIVE", "EQUITIES")
        total_all = total_active = total_eq = 0
        for r in rows:
            logger.info("%-20s  %8d  %8d  %8d", r[0], r[1], r[2], r[3])
            total_all    += r[1]
            total_active += r[2]
            total_eq     += r[3]
        logger.info("%-20s  %8d  %8d  %8d", "TOTAL", total_all, total_active, total_eq)

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    seed_ticker_universe()
