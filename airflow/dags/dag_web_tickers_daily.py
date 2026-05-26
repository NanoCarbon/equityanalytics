"""
DAG: ticker_universe_sync
Schedule: 3am ET Mon-Fri (08:00 UTC) — runs before equity_daily (4am ET)

Keeps RAW.TICKER_UNIVERSE in sync with all configured ticker sources.

Task chain (runs daily, Mon-Fri ET):
  1. sync_sp_indices      — Scrape S&P 500/400/600 + ETF list from Wikipedia;
                            MERGE into TICKER_UNIVERSE; deactivate tickers that
                            dropped from all S&P indices and the ETF list.
  2. sync_nasdaq_trader   — Download NASDAQ Trader flat files; INSERT new
                            US-listed tickers; reactivate deactivated tickers
                            still present on exchanges; deactivate delisted ones.
  3. sync_international_indices (Monday ET only) — Re-scrape Wikipedia for
                            FTSE 100, TSX 60, ASX 200, Nikkei 225, and DAX 40;
                            MERGE constituents.  Skips on Tue-Fri ET runs since
                            index membership changes only at quarterly rebalances.

Source priority rule:
  S&P/ETF membership always wins (source label never downgraded by NASDAQ or
  international sync).  nasdaq_trader and international sources manage their
  own deactivation independently.

Deactivated tickers are never deleted — they remain with is_active=FALSE so
historical RAW data retains a valid FK reference.
"""

import sys
import logging
from datetime import datetime, timedelta

sys.path.insert(0, '/opt/airflow')

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}


@dag(
    dag_id='ticker_universe_sync',
    description='Sync RAW.TICKER_UNIVERSE from Wikipedia S&P 1500 + ETF list (nightly)',
    schedule='0 8 * * 2-6',     # 3am ET Mon-Fri (8am UTC Tue-Sat)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=['equity', 'universe', 'daily'],
)
def ticker_universe_sync():

    @task(execution_timeout=timedelta(hours=2))
    def sync_sp_indices() -> dict:
        """
        Scrape current S&P 1500 + ETF list and reconcile ONLY those source rows
        against RAW.TICKER_UNIVERSE.

        IMPORTANT: deactivation is scoped to S&P/ETF-sourced tickers only.
        Tickers sourced from 'nasdaq_trader' or international indices have their
        own sync tasks and must NOT be deactivated here when they drop from S&P.
        (A ticker leaving S&P 500 but still listed on NASDAQ remains active under
        source='nasdaq_trader' — it's not delisted, just rebalanced.)

        Returns a summary dict logged to XCom for observability:
          added        — new tickers inserted
          reactivated  — previously inactive tickers back in an index
          deactivated  — tickers removed from ALL S&P indices and ETF list
          unchanged    — tickers already active and still present
        """
        from ingestion.extract import (
            get_sp500_tickers, get_sp400_tickers,
            get_sp600_tickers, get_etf_tickers,
        )
        from ingestion.load import get_connection

        # ── 1. Build current S&P + ETF universe ──────────────────
        sp500 = set(get_sp500_tickers())
        sp400 = set(get_sp400_tickers())
        sp600 = set(get_sp600_tickers())
        etfs  = set(get_etf_tickers())

        logger.info(
            "Scraped: S&P 500=%d | S&P 400=%d | S&P 600=%d | ETFs=%d",
            len(sp500), len(sp400), len(sp600), len(etfs),
        )

        # Assign each ticker its canonical source (highest priority wins)
        current: dict[str, tuple[str, bool]] = {}
        for t in sp600: current[t] = ("sp600", True)
        for t in sp400: current[t] = ("sp400", True)
        for t in sp500: current[t] = ("sp500", True)
        for t in etfs:
            if t not in current:
                current[t] = ("etf", False)

        current_set = set(current)
        logger.info("Total current S&P/ETF universe: %d unique tickers", len(current_set))

        # ── 2. Load existing TICKER_UNIVERSE state ────────────────
        conn = get_connection()
        try:
            cur = conn.cursor()
            # Load source alongside active status so we can scope deactivation
            cur.execute("""
                SELECT ticker, is_active, COALESCE(source, 'manual')
                FROM   EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE
            """)
            rows = cur.fetchall()
            existing        = {r[0]: (r[1], r[2]) for r in rows}  # ticker -> (is_active, source)
            existing_set    = set(existing)
            active_set      = {t for t, (active, _) in existing.items() if active}
            inactive_set    = {t for t, (active, _) in existing.items() if not active}

            # Tickers eligible for S&P-driven deactivation:
            # only those currently sourced from S&P indices or ETF list.
            # nasdaq_trader and international-sourced tickers are managed by their
            # own sync tasks — never touch them here.
            sp_etf_sources = {'sp500', 'sp400', 'sp600', 'etf'}
            sp_etf_active  = {
                t for t, (active, src) in existing.items()
                if active and src in sp_etf_sources
            }

            # ── 3. Compute deltas ─────────────────────────────────
            to_insert      = current_set - existing_set          # brand new tickers
            to_reactivate  = current_set & inactive_set           # came back to an index
            to_update      = current_set & active_set             # refresh source/is_equity
            # Only deactivate tickers that were S&P/ETF-sourced and are no longer present
            to_deactivate  = sp_etf_active - current_set

            # ── 4. Apply changes ──────────────────────────────────

            # INSERT new tickers (country/yfinance_suffix defaults for US)
            if to_insert:
                vals = ", ".join(
                    f"('{t.replace(chr(39), chr(39)*2)}', '{current[t][0]}', "
                    f"{str(current[t][1]).upper()}, 'US', '')"
                    for t in to_insert
                )
                cur.execute(
                    "INSERT INTO EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE "
                    "(ticker, source, is_equity, is_active, country, yfinance_suffix) VALUES " + vals
                )
                logger.info("Inserted %d new tickers: %s", len(to_insert),
                            ", ".join(sorted(to_insert)[:20]))

            # REACTIVATE tickers that returned to an index
            if to_reactivate:
                for t in to_reactivate:
                    src, is_eq = current[t]
                    cur.execute("""
                        UPDATE EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE
                        SET    is_active           = TRUE,
                               source              = %s,
                               is_equity           = %s,
                               deactivated_at      = NULL,
                               deactivation_reason = NULL
                        WHERE  ticker = %s
                    """, (src, is_eq, t))
                logger.info("Reactivated %d tickers: %s", len(to_reactivate),
                            ", ".join(sorted(to_reactivate)[:20]))

            # UPDATE source/is_equity for already-active S&P tickers
            # (a ticker can move between S&P 500/400/600 during rebalances)
            if to_update:
                for t in to_update:
                    src, is_eq = current[t]
                    cur.execute("""
                        UPDATE EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE
                        SET    source = %s, is_equity = %s
                        WHERE  ticker = %s AND (source != %s OR is_equity != %s)
                    """, (src, is_eq, t, src, is_eq))

            # DEACTIVATE S&P/ETF-sourced tickers no longer in any index or ETF list.
            # These remain in the table (soft-delete) for FK integrity.
            # NOTE: if the ticker is still listed (appears in nasdaq_trader file),
            # the nasdaq sync task will reactivate it under source='nasdaq_trader'.
            if to_deactivate:
                ids_str = ", ".join(f"'{t.replace(chr(39), chr(39)*2)}'" for t in to_deactivate)
                cur.execute(f"""
                    UPDATE EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE
                    SET    is_active           = FALSE,
                           deactivated_at      = CURRENT_TIMESTAMP,
                           deactivation_reason = 'Removed from S&P 1500 and ETF list during nightly sync'
                    WHERE  ticker IN ({ids_str})
                      AND  is_active = TRUE
                """)
                logger.warning(
                    "Deactivated %d tickers (removed from all S&P indices): %s",
                    len(to_deactivate), ", ".join(sorted(to_deactivate)),
                )

            # Assign fundamentals_cohort for any newly-inserted equity tickers
            cur.execute("""
                UPDATE EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE
                SET    fundamentals_cohort = ABS(HASH(ticker))::INTEGER % 4
                WHERE  is_equity = TRUE AND fundamentals_cohort IS NULL
            """)

            cur.close()

        finally:
            conn.close()

        result = {
            "added":        len(to_insert),
            "reactivated":  len(to_reactivate),
            "deactivated":  len(to_deactivate),
            "unchanged":    len(to_update),
            "total_sp_etf": len(current_set),
        }
        logger.info(
            "S&P/ETF sync complete — added: %d | reactivated: %d | deactivated: %d | "
            "refreshed: %d | total S&P/ETF active: %d",
            result["added"], result["reactivated"], result["deactivated"],
            result["unchanged"], result["total_sp_etf"],
        )
        return result

    @task(execution_timeout=timedelta(hours=2))
    def sync_nasdaq_trader(sp_result: dict) -> dict:
        """
        Download the NASDAQ Trader flat files and reconcile against
        RAW.TICKER_UNIVERSE for source='nasdaq_trader' tickers.

        Runs after sync_sp_indices so that S&P priority is respected:
          - If a ticker is in S&P AND NASDAQ Trader, its source stays S&P.
          - If a ticker leaves S&P (deactivated above) but is still listed,
            this task reactivates it under source='nasdaq_trader'.
          - Tickers that disappear from the NASDAQ Trader files are soft-deactivated.

        Returns a summary dict.
        """
        from ingestion.extract_ticker_universe import fetch_nasdaq_trader_us
        from ingestion.load import get_connection

        logger.info("Fetching NASDAQ Trader files...")
        nasdaq_rows = fetch_nasdaq_trader_us()
        logger.info("Fetched %d securities from NASDAQ Trader files", len(nasdaq_rows))

        # Build lookup: ticker -> row dict
        nasdaq_by_ticker: dict[str, dict] = {r['ticker']: r for r in nasdaq_rows}
        nasdaq_set = set(nasdaq_by_ticker)

        conn = get_connection()
        try:
            cur = conn.cursor()

            # Load existing state — all sources, not just nasdaq_trader
            cur.execute("""
                SELECT ticker, is_active, COALESCE(source, 'manual')
                FROM   EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE
            """)
            rows        = cur.fetchall()
            existing    = {r[0]: (r[1], r[2]) for r in rows}
            existing_set = set(existing)

            # S&P/ETF sources hold priority — never overwrite them with nasdaq_trader
            priority_sources = {'sp500', 'sp400', 'sp600', 'etf'}
            priority_active  = {
                t for t, (active, src) in existing.items()
                if active and src in priority_sources
            }

            # Tickers currently sourced from nasdaq_trader (active or inactive)
            nasdaq_existing = {
                t for t, (active, src) in existing.items()
                if src == 'nasdaq_trader'
            }
            nasdaq_active = {t for t in nasdaq_existing if existing[t][0]}

            # New tickers: in NASDAQ file but not in TICKER_UNIVERSE at all
            to_insert = nasdaq_set - existing_set

            # Reactivate: in NASDAQ file AND inactive in TICKER_UNIVERSE
            # (includes tickers previously deactivated by S&P sync that are still listed)
            inactive_set = {t for t, (active, _) in existing.items() if not active}
            to_reactivate = (nasdaq_set & inactive_set) - priority_active

            # Deactivate: currently active as nasdaq_trader but no longer in NASDAQ file
            to_deactivate = nasdaq_active - nasdaq_set

            # Batched INSERT for new tickers
            added = 0
            batch = list(to_insert)
            chunk_size = 500
            for i in range(0, len(batch), chunk_size):
                chunk = batch[i:i + chunk_size]
                val_parts = []
                for t in chunk:
                    r = nasdaq_by_ticker[t]
                    t_s   = t.replace("'", "''")
                    nm_s  = r.get('name', '').replace("'", "''")[:200]
                    exch  = r.get('exchange', 'NASDAQ').replace("'", "''")
                    is_eq = str(r.get('is_equity', True)).upper()
                    # Assign cohort for equities
                    val_parts.append(
                        f"('{t_s}', 'nasdaq_trader', {is_eq}, TRUE, "
                        f"'US', '', '{exch}')"
                    )
                vals_sql = ", ".join(val_parts)
                cur.execute(
                    "INSERT INTO EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE "
                    "(ticker, source, is_equity, is_active, country, yfinance_suffix, exchange) "
                    "VALUES " + vals_sql
                )
                added += len(chunk)

            logger.info("Inserted %d new nasdaq_trader tickers", added)

            # Reactivate tickers back in the NASDAQ file
            reactivated = 0
            for t in to_reactivate:
                r = nasdaq_by_ticker[t]
                exch  = r.get('exchange', 'NASDAQ')
                is_eq = r.get('is_equity', True)
                cur.execute("""
                    UPDATE EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE
                    SET    is_active           = TRUE,
                           source              = 'nasdaq_trader',
                           is_equity           = %s,
                           exchange            = %s,
                           country             = 'US',
                           yfinance_suffix     = '',
                           deactivated_at      = NULL,
                           deactivation_reason = NULL
                    WHERE  ticker = %s
                """, (is_eq, exch, t))
                reactivated += 1
            if reactivated:
                logger.info("Reactivated %d nasdaq_trader tickers", reactivated)

            # Update exchange for existing nasdaq_trader tickers (data may refresh)
            cur.execute("""
                UPDATE EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE tu
                SET    exchange = src.exchange
                FROM (
                    SELECT column1 AS ticker, column2 AS exchange
                    FROM VALUES %s
                ) AS src
                WHERE tu.ticker = src.ticker
                  AND tu.source = 'nasdaq_trader'
                  AND tu.exchange IS DISTINCT FROM src.exchange
            """ % (
                ", ".join(
                    f"('{t.replace(chr(39),chr(39)*2)}', "
                    f"'{nasdaq_by_ticker[t].get(\"exchange\",\"NASDAQ\").replace(chr(39),chr(39)*2)}')"
                    for t in nasdaq_set & existing_set
                    if existing[t][1] == 'nasdaq_trader'
                ) or "('__NOOP__', '__NOOP__')"
            ))

            # Soft-deactivate tickers no longer in NASDAQ file
            deactivated = 0
            if to_deactivate:
                ids_str = ", ".join(f"'{t.replace(chr(39), chr(39)*2)}'" for t in to_deactivate)
                cur.execute(f"""
                    UPDATE EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE
                    SET    is_active           = FALSE,
                           deactivated_at      = CURRENT_TIMESTAMP,
                           deactivation_reason = 'No longer listed in NASDAQ Trader files'
                    WHERE  ticker IN ({ids_str})
                      AND  is_active = TRUE
                      AND  source = 'nasdaq_trader'
                """)
                deactivated = len(to_deactivate)
                logger.warning(
                    "Deactivated %d nasdaq_trader tickers (delisted): %s",
                    deactivated, ", ".join(sorted(to_deactivate)[:20]),
                )

            # Assign fundamentals_cohort for any newly-added equity tickers
            cur.execute("""
                UPDATE EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE
                SET    fundamentals_cohort = ABS(HASH(ticker))::INTEGER % 4
                WHERE  is_equity = TRUE AND fundamentals_cohort IS NULL
            """)

            # Also update exchange for S&P tickers that now have exchange data
            # from the NASDAQ file (exchange was NULL before Phase 2 seed)
            cur.execute("""
                UPDATE EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE tu
                SET    exchange = src.exchange
                FROM (
                    SELECT column1 AS ticker, column2 AS exchange
                    FROM VALUES %s
                ) AS src
                WHERE tu.ticker = src.ticker
                  AND tu.source IN ('sp500', 'sp400', 'sp600', 'etf')
                  AND tu.exchange IS NULL
            """ % (
                ", ".join(
                    f"('{t.replace(chr(39),chr(39)*2)}', "
                    f"'{nasdaq_by_ticker[t].get(\"exchange\",\"NASDAQ\").replace(chr(39),chr(39)*2)}')"
                    for t in nasdaq_set & existing_set
                    if existing[t][1] in {'sp500','sp400','sp600','etf'}
                ) or "('__NOOP__', '__NOOP__')"
            ))

            cur.close()

        finally:
            conn.close()

        result = {
            "added":       added,
            "reactivated": reactivated,
            "deactivated": deactivated,
            "total_nasdaq_file": len(nasdaq_set),
        }
        logger.info(
            "NASDAQ Trader sync complete — added: %d | reactivated: %d | deactivated: %d",
            result["added"], result["reactivated"], result["deactivated"],
        )
        return result

    @task(execution_timeout=timedelta(hours=2))
    def sync_international_indices(nasdaq_result: dict, **context) -> dict:
        """
        Re-scrape Wikipedia for all 5 international indices (FTSE 100, TSX 60,
        ASX 200, Nikkei 225, DAX 40) and MERGE into RAW.TICKER_UNIVERSE.

        Runs only on Monday ET (= Tuesday UTC for a DAG scheduled at 08:00 UTC).
        International constituents change only at quarterly rebalances so a
        once-per-week refresh is more than sufficient.

        Each index is attempted independently — a failed Wikipedia scrape for
        one index is logged and skipped without aborting the others.

        Source priority is respected: if a ticker somehow appears in both an
        international index and a US source, the US source is preserved via
        the standard MERGE logic in _merge_rows.
        """
        logical_date = context['logical_date']
        # DAG schedule: 08:00 UTC Tue-Sat = 03:00 ET Mon-Fri.
        # Tuesday UTC (weekday=1) corresponds to Monday ET — run only then.
        if logical_date.weekday() != 1:
            logger.info(
                "International index sync skipped — only runs on Monday ET "
                "(logical_date weekday=%d, need 1=Tuesday UTC)",
                logical_date.weekday(),
            )
            return {"skipped": True}

        from ingestion.extract_ticker_universe import (
            fetch_ftse100, fetch_tsx60, fetch_asx200,
            fetch_nikkei225, fetch_dax40,
        )
        from ingestion.seed_ticker_universe import _merge_rows
        from ingestion.load import get_connection

        fetchers = [
            ('FTSE 100',   fetch_ftse100),
            ('TSX 60',     fetch_tsx60),
            ('ASX 200',    fetch_asx200),
            ('Nikkei 225', fetch_nikkei225),
            ('DAX 40',     fetch_dax40),
        ]

        conn = get_connection()
        total_merged   = 0
        skipped_indices: list[str] = []
        try:
            cur = conn.cursor()

            for index_name, fetcher in fetchers:
                try:
                    rows = fetcher()
                    if not rows:
                        logger.warning("%s: 0 rows returned — skipping", index_name)
                        skipped_indices.append(index_name)
                        continue
                    logger.info("%s: merging %d constituents", index_name, len(rows))
                    _merge_rows(cur, rows)
                    total_merged += len(rows)
                except Exception as exc:
                    logger.error(
                        "%s: fetch/merge failed (%s: %s) — skipping index",
                        index_name, type(exc).__name__, exc,
                    )
                    skipped_indices.append(index_name)

            # Assign fundamentals_cohort for any newly-inserted equity rows
            cur.execute("""
                UPDATE EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE
                SET    fundamentals_cohort = ABS(HASH(ticker))::INTEGER % 4
                WHERE  is_equity = TRUE AND fundamentals_cohort IS NULL
            """)
            cur.close()

        finally:
            conn.close()

        result = {
            "merged":          total_merged,
            "skipped_indices": skipped_indices,
        }
        logger.info(
            "International index sync complete — %d rows merged; skipped: %s",
            total_merged, skipped_indices or "none",
        )
        return result

    sp_result     = sync_sp_indices()
    nasdaq_result = sync_nasdaq_trader(sp_result)
    sync_international_indices(nasdaq_result)   # Monday ET only; skips otherwise


ticker_universe_sync()
