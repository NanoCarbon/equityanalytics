"""
DAG: equity_daily
Replaces: equity_pipeline() in ingestion/pipeline.py

Schedule: Monday–Friday at 11pm ET (04:00 UTC next day)
What it does:
  1. Gets the full ticker list (~616 tickers)
  2. Finds the latest date already in Snowflake (incremental boundary)
  3. Extracts new prices from yfinance + loads to RAW.PRICES
  4. Extracts company metadata + loads to RAW.COMPANY_INFO (full overwrite)

Tasks 3 and 4 are independent so they run in parallel.
"""

import sys
import logging
from datetime import datetime, timedelta

# Make the ingestion package importable inside the container
sys.path.insert(0, '/opt/airflow')

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)


# ── Default args applied to every task in this DAG ───────────────
DEFAULT_ARGS = {
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
}


@dag(
    dag_id='equity_daily',
    description='Daily prices + company metadata → Snowflake RAW',
    schedule='0 4 * * 2-6',    # 11pm ET, Mon–Fri (4am UTC Tue–Sat)
    start_date=datetime(2026, 1, 1),
    catchup=False,              # don't backfill missed runs
    default_args=DEFAULT_ARGS,
    tags=['equity', 'daily'],
)
def equity_daily():

    @task()
    def get_tickers() -> list:
        """Scrape S&P 500 from Wikipedia + hardcoded ETF list → ~616 tickers."""
        from ingestion.extract import get_all_tickers
        tickers = get_all_tickers()
        logger.info("Loaded %d tickers", len(tickers))
        return tickers

    @task()
    def get_max_date() -> str | None:
        """
        Find the most recent date already in RAW.PRICES.
        Returns a date string (e.g. '2026-05-18') or None if table is empty.
        Drives incremental loading — we only pull data after this date.

        Note: we return a string rather than a date object because Airflow
        serialises task return values to JSON for XCom storage.
        """
        from ingestion.load import get_max_date as _get_max_date
        result = _get_max_date("PRICES")
        if result:
            logger.info("Latest date in Snowflake: %s — incremental load", result)
            return str(result)
        logger.info("No existing data — full historical load")
        return None

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def extract_and_load_prices(tickers: list, max_date: str | None) -> int:
        """
        Extract new OHLCV prices from yfinance and append to RAW.PRICES.
        Combines extract + load into one task to avoid passing large
        DataFrames through XCom.

        Returns the number of rows loaded.
        """
        from ingestion.extract import extract_prices
        from ingestion.load import load_dataframe

        df = extract_prices(tickers, start_date=max_date, lookback_days=5)

        if df is None or df.empty:
            logger.info("No new prices to load")
            return 0

        rows = load_dataframe(df, "PRICES", overwrite=False)
        logger.info("Appended %d rows to RAW.PRICES", rows)
        return rows

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def extract_and_load_company_info(tickers: list) -> int:
        """
        Extract company metadata (sector, market cap, etc.) and overwrite
        RAW.COMPANY_INFO. Full overwrite because metadata changes over time
        and we always want the current snapshot.

        Returns the number of rows loaded.
        """
        from ingestion.extract import extract_company_info
        from ingestion.load import load_dataframe

        logger.info("Fetching metadata for %d tickers (2s delay between calls)", len(tickers))
        df = extract_company_info(tickers, delay_seconds=2.0)
        rows = load_dataframe(df, "COMPANY_INFO", overwrite=True)
        logger.info("Overwrote RAW.COMPANY_INFO with %d rows", rows)
        return rows

    # ── Wire up the task dependencies ────────────────────────────
    #
    # tickers ──┬──→ extract_and_load_prices (needs tickers + max_date)
    #           │
    # max_date ─┘
    #
    # tickers ──→ extract_and_load_company_info (independent, runs in parallel)

    tickers  = get_tickers()
    max_date = get_max_date()

    extract_and_load_prices(tickers, max_date)
    extract_and_load_company_info(tickers)


# Instantiate the DAG — Airflow discovers it by executing this module
equity_daily()
