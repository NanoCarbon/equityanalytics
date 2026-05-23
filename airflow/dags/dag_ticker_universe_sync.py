"""
DAG: ticker_universe_sync
Schedule: 3am ET Mon-Fri (08:00 UTC) — runs before equity_daily (4am ET)

Keeps RAW.TICKER_UNIVERSE in sync with the live S&P 1500 index membership
and the hardcoded ETF list.

What it does:
  1. Scrapes the current S&P 500, 400, and 600 components from Wikipedia
  2. MERGEs the result into RAW.TICKER_UNIVERSE:
       - New tickers are INSERT with is_active=TRUE
       - Existing tickers have source/is_equity refreshed, is_active set TRUE
       - Tickers that dropped from ALL indices and the ETF list are marked
         is_active=FALSE with a deactivation_reason
  3. Logs a summary: added, reactivated, deactivated

By running one hour before equity_daily, the downstream DAGs always read a
fresh, reconciled ticker list from TICKER_UNIVERSE rather than calling
Wikipedia themselves.

Deactivated tickers are never deleted — they remain in the table with
is_active=FALSE so historical RAW data retains a valid FK reference.
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

    @task()
    def sync_universe() -> dict:
        """
        Scrape current S&P 1500 + ETF list and reconcile against
        RAW.TICKER_UNIVERSE.

        Returns a summary dict logged to XCom for observability:
          added        — new tickers inserted
          reactivated  — previously inactive tickers back in an index
          deactivated  — tickers no longer in any index or ETF list
          unchanged    — tickers already active and still present
        """
        from ingestion.extract import (
            get_sp500_tickers, get_sp400_tickers,
            get_sp600_tickers, get_etf_tickers,
        )
        from ingestion.load import get_connection

        # ── 1. Build current universe ─────────────────────────────
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
        logger.info("Total current universe: %d unique tickers", len(current_set))

        # ── 2. Load existing TICKER_UNIVERSE state ────────────────
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT ticker, is_active
                FROM   EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE
            """)
            existing = {row[0]: row[1] for row in cur.fetchall()}  # ticker -> is_active

            existing_set  = set(existing)
            active_set    = {t for t, active in existing.items() if active}
            inactive_set  = {t for t, active in existing.items() if not active}

            # ── 3. Compute deltas ─────────────────────────────────
            to_insert      = current_set - existing_set          # brand new
            to_reactivate  = current_set & inactive_set           # came back
            to_update      = current_set & active_set             # refresh source/is_equity
            to_deactivate  = active_set - current_set             # dropped out

            # ── 4. Apply changes ──────────────────────────────────

            # INSERT new tickers
            if to_insert:
                vals = ", ".join(
                    f"('{t.replace(chr(39), chr(39)*2)}', '{current[t][0]}', {str(current[t][1]).upper()})"
                    for t in to_insert
                )
                cur.execute(
                    "INSERT INTO EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE "
                    "(ticker, source, is_equity, is_active) VALUES " + vals
                )
                logger.info("Inserted %d new tickers: %s", len(to_insert),
                            ", ".join(sorted(to_insert)[:20]))

            # REACTIVATE tickers that returned to an index
            if to_reactivate:
                for t in to_reactivate:
                    src, is_eq = current[t]
                    cur.execute("""
                        UPDATE EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE
                        SET    is_active = TRUE,
                               source = %s,
                               is_equity = %s,
                               deactivated_at = NULL,
                               deactivation_reason = NULL
                        WHERE  ticker = %s
                    """, (src, is_eq, t))
                logger.info("Reactivated %d tickers: %s", len(to_reactivate),
                            ", ".join(sorted(to_reactivate)[:20]))

            # UPDATE source/is_equity for already-active tickers (index may change)
            if to_update:
                for t in to_update:
                    src, is_eq = current[t]
                    cur.execute("""
                        UPDATE EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE
                        SET    source = %s, is_equity = %s
                        WHERE  ticker = %s AND (source != %s OR is_equity != %s)
                    """, (src, is_eq, t, src, is_eq))

            # DEACTIVATE tickers no longer in any index or ETF list
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
                    "Deactivated %d tickers (removed from all indices): %s",
                    len(to_deactivate), ", ".join(sorted(to_deactivate)),
                )

            cur.close()

        finally:
            conn.close()

        result = {
            "added":       len(to_insert),
            "reactivated": len(to_reactivate),
            "deactivated": len(to_deactivate),
            "unchanged":   len(to_update),
            "total_active": len(current_set),
        }
        logger.info(
            "Sync complete — added: %d | reactivated: %d | deactivated: %d | "
            "refreshed: %d | total active: %d",
            result["added"], result["reactivated"], result["deactivated"],
            result["unchanged"], result["total_active"],
        )
        return result

    sync_universe()


ticker_universe_sync()
