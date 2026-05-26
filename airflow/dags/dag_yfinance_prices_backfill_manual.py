"""
DAG: backfill_prices
Replaces: backfill_pipeline() in ingestion/pipeline.py

Schedule: None (manual trigger only — run once to load historical data)

What it does:
  Loads historical OHLCV prices from 2010 to the earliest date already
  in RAW.PRICES, in batches of 50 tickers to avoid rate limiting.

  Safe to re-run — incremental boundaries prevent duplicate loads.

How to trigger:
  Airflow UI → DAGs → backfill_prices → Trigger DAG ▶
  Or: airflow dags trigger backfill_prices
"""

import sys
import logging
import time
from datetime import datetime, timedelta

sys.path.insert(0, '/opt/airflow')

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
}


@dag(
    dag_id='backfill_prices',
    description='One-time historical price backfill from 2010 (manual trigger only)',
    schedule=None,              # Never runs automatically
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=['backfill', 'manual'],
)
def backfill_prices():

    @task(execution_timeout=timedelta(hours=2))
    def get_tickers() -> list:
        """Load active tickers from RAW.TICKER_UNIVERSE; falls back to Wikipedia scrape."""
        from ingestion.extract import get_tickers_from_db
        from ingestion.load import get_connection
        conn = get_connection()
        try:
            all_tickers, _ = get_tickers_from_db(conn)
        finally:
            conn.close()
        logger.info("Loaded %d tickers for backfill", len(all_tickers))
        return all_tickers

    @task(execution_timeout=timedelta(hours=2))
    def get_backfill_boundaries() -> dict:
        """
        Determine the date range to backfill.
        - start: 2010-01-01 (hardcoded historical start)
        - end: earliest date already in RAW.PRICES (so we don't overlap)

        Returns a dict (JSON-serialisable for XCom) with start_date and end_date.
        """
        from ingestion.load import get_min_date
        existing_min = get_min_date("PRICES")

        start_date = "2010-01-01"
        if existing_min:
            end_date = str(existing_min)
            logger.info("Existing data starts at %s — backfilling before that", end_date)
        else:
            end_date = datetime.today().strftime("%Y-%m-%d")
            logger.info("No existing data — backfilling everything to today")

        if existing_min and str(existing_min) <= start_date:
            logger.info("Data already starts at or before %s — nothing to do", start_date)
            return {"start_date": start_date, "end_date": end_date, "skip": True}

        return {"start_date": start_date, "end_date": end_date, "skip": False}

    @task(execution_timeout=timedelta(hours=2))
    def run_backfill(tickers: list, boundaries: dict) -> dict:
        """
        Download historical prices in batches of 50 tickers with 30s delays
        between batches (avoids yfinance rate limits).

        Batching strategy: 616 tickers / 50 per batch = ~13 batches.
        Each batch takes ~30s of rate-limit sleep = ~7 minutes total.

        Returns a summary dict with total rows loaded and any failed batches.
        """
        from ingestion.extract import extract_prices
        from ingestion.load import load_dataframe

        if boundaries.get("skip"):
            logger.info("Nothing to backfill — skipping")
            return {"total_rows": 0, "failed_batches": []}

        start_date = boundaries["start_date"]
        end_date   = boundaries["end_date"]
        batch_size = 50
        batch_delay = 30

        batches = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
        logger.info(
            "Backfilling %s → %s | %d tickers | %d batches of %d",
            start_date, end_date, len(tickers), len(batches), batch_size,
        )

        total_rows = 0
        failed_batches = []

        for i, batch in enumerate(batches, 1):
            logger.info("Batch %d/%d: %s → %s", i, len(batches), batch[0], batch[-1])

            try:
                df = extract_prices(
                    tickers=batch,
                    start_date_str=start_date,
                    end_date_str=end_date,
                )

                if df is None or df.empty:
                    logger.info("Batch %d: no data — skipping", i)
                else:
                    rows = load_dataframe(df, "PRICES", overwrite=False)
                    total_rows += rows
                    logger.info("Batch %d: loaded %d rows (running total: %d)", i, rows, total_rows)

            except Exception as e:
                logger.error("Batch %d failed: %s", i, e)
                failed_batches.append({"batch": i, "tickers": batch, "error": str(e)})
                logger.info("Continuing to next batch...")

            if i < len(batches):
                logger.info("Sleeping %ds before next batch...", batch_delay)
                time.sleep(batch_delay)

        if failed_batches:
            logger.warning("%d batches failed — re-run DAG to retry (no duplicates)", len(failed_batches))
        else:
            logger.info("All batches complete — %d total rows loaded", total_rows)

        return {"total_rows": total_rows, "failed_batches": failed_batches}

    # ── Wire up ──────────────────────────────────────────────────
    tickers    = get_tickers()
    boundaries = get_backfill_boundaries()
    run_backfill(tickers, boundaries)


backfill_prices()
