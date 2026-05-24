"""
DAG: backfill_new_tickers
Schedule: None (manual trigger only)

Purpose:
  Backfills historical OHLCV prices from 2010-01-01 to today for tickers
  that are in the current universe but have NO data in RAW.PRICES yet.

  Designed for ticker universe expansions (e.g. adding S&P 400 + S&P 600).
  Tickers already present in RAW.PRICES are skipped entirely — so re-running
  this DAG after a partial failure is safe and won't create duplicates.

What it does:
  1. Fetches the full ticker universe (S&P 1500 + ETFs, ~1,600 tickers)
  2. Queries RAW.PRICES for the set of tickers already loaded
  3. Computes the diff — only truly new tickers proceed
  4. Downloads 2010-01-01 to today in batches of 50 with 30s delays

How to trigger:
  Airflow UI -> DAGs -> backfill_new_tickers -> Trigger DAG
  Or: airflow dags trigger backfill_new_tickers

After completion:
  Run a full-refresh dbt build so the new tickers flow into the mart layer:
    dbt build --profiles-dir . --select fact_daily_prices --full-refresh
"""

import sys
import logging
import time
from datetime import datetime, timedelta

sys.path.insert(0, '/opt/airflow')

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

BACKFILL_START = "2010-01-01"
BATCH_SIZE     = 50
BATCH_DELAY    = 30   # seconds between batches — avoids yfinance rate limits

DEFAULT_ARGS = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}


@dag(
    dag_id='backfill_new_tickers',
    description='Backfill prices from 2010 for tickers not yet in RAW.PRICES (manual trigger)',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=['backfill', 'manual', 'prices'],
)
def backfill_new_tickers():

    @task()
    def resolve_new_tickers() -> dict:
        """
        Diff the current universe against RAW.PRICES to find tickers with no
        data at all. Returns a summary dict (JSON-serialisable for XCom) with:
          - new_tickers : list of tickers to backfill
          - already_loaded : count of tickers skipped
          - skip : True if nothing to do
        """
        from ingestion.extract import get_tickers_from_db
        from ingestion.load import get_loaded_tickers, get_connection

        conn     = get_connection()
        try:
            universe, _ = get_tickers_from_db(conn)
        finally:
            conn.close()
        already_loaded = get_loaded_tickers("PRICES")
        new_tickers    = [t for t in universe if t not in already_loaded]

        logger.info(
            "Universe: %d | Already in RAW.PRICES: %d | New tickers to backfill: %d",
            len(universe), len(already_loaded), len(new_tickers),
        )

        if not new_tickers:
            logger.info("All tickers already have price data — nothing to backfill")
            return {"new_tickers": [], "already_loaded": len(already_loaded), "skip": True}

        logger.info("First 10 new tickers: %s", new_tickers[:10])
        return {"new_tickers": new_tickers, "already_loaded": len(already_loaded), "skip": False}

    @task()
    def run_backfill(ticker_info: dict) -> dict:
        """
        Download OHLCV prices from BACKFILL_START to today for new tickers only,
        in batches of 50 with 30-second delays between batches.

        Returns a summary with total rows loaded and any failed batches.
        """
        from ingestion.extract import extract_prices
        from ingestion.load import load_dataframe

        if ticker_info.get("skip"):
            logger.info("No new tickers — skipping backfill")
            return {"total_rows": 0, "failed_batches": [], "new_tickers": 0}

        new_tickers = ticker_info["new_tickers"]
        end_date    = datetime.today().strftime("%Y-%m-%d")
        batches     = [new_tickers[i:i + BATCH_SIZE] for i in range(0, len(new_tickers), BATCH_SIZE)]

        logger.info(
            "Backfilling %s -> %s | %d new tickers | %d batches of %d",
            BACKFILL_START, end_date, len(new_tickers), len(batches), BATCH_SIZE,
        )

        total_rows    = 0
        failed_batches = []

        for i, batch in enumerate(batches, 1):
            logger.info("Batch %d/%d: %s -> %s", i, len(batches), batch[0], batch[-1])
            try:
                df = extract_prices(
                    tickers=batch,
                    start_date_str=BACKFILL_START,
                    end_date_str=end_date,
                )
                if df is None or df.empty:
                    logger.info("Batch %d: no data returned — skipping", i)
                else:
                    rows = load_dataframe(df, "PRICES", overwrite=False)
                    total_rows += rows
                    logger.info("Batch %d: loaded %d rows (running total: %d)", i, rows, total_rows)

            except Exception as e:
                logger.error("Batch %d failed: %s", i, e)
                failed_batches.append({"batch": i, "tickers": batch, "error": str(e)})
                logger.info("Continuing to next batch...")

            if i < len(batches):
                logger.info("Sleeping %ds before next batch...", BATCH_DELAY)
                time.sleep(BATCH_DELAY)

        if failed_batches:
            logger.warning(
                "%d of %d batches failed -- re-trigger DAG to retry (no duplicates)",
                len(failed_batches), len(batches),
            )
        else:
            logger.info(
                "Backfill complete -- %d new tickers, %d total rows loaded",
                len(new_tickers), total_rows,
            )

        return {
            "total_rows":    total_rows,
            "new_tickers":   len(new_tickers),
            "failed_batches": failed_batches,
        }

    # ── Wire up ──────────────────────────────────────────────────
    ticker_info = resolve_new_tickers()
    run_backfill(ticker_info)


backfill_new_tickers()
