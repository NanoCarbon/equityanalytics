"""
DAG: yfinance_prices_daily
Source: yfinance  |  Frequency: daily (Mon–Fri ET, 4am UTC Tue–Sat)

What it does:
  1. Loads the full ticker list from RAW.TICKER_UNIVERSE
  2. Finds the latest date already in RAW.PRICES (incremental boundary)
  3. Bulk-downloads only the missing OHLCV data via yf.download and appends
     to RAW.PRICES (5-day lookback overlap to catch late-arriving corrections)

Company metadata (sector, market cap, etc.) is refreshed weekly by
yfinance_supplemental_weekly — it doesn't need a daily overwrite.
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
    dag_id='yfinance_prices_daily',
    description='yfinance | Incremental OHLCV prices → Snowflake RAW | daily',
    schedule='0 4 * * 2-6',    # 11pm ET, Mon–Fri (4am UTC Tue–Sat)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=['yfinance', 'prices', 'daily'],
)
def yfinance_prices_daily():

    @task(execution_timeout=timedelta(hours=2))
    def get_tickers() -> list:
        """
        Load active tickers from RAW.TICKER_UNIVERSE (primary source).
        Falls back to live Wikipedia scrape + hardcoded ETF list if DB is unreachable.
        """
        from ingestion.extract import get_tickers_from_db
        from ingestion.load import get_connection
        conn = get_connection()
        try:
            all_tickers, _ = get_tickers_from_db(conn)
        finally:
            conn.close()
        logger.info("Loaded %d tickers", len(all_tickers))
        return all_tickers

    @task(execution_timeout=timedelta(hours=2))
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

    @task(retries=3, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(hours=2))
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

    # ── Wire up the task dependencies ────────────────────────────
    tickers  = get_tickers()
    max_date = get_max_date()

    extract_and_load_prices(tickers, max_date)


# Instantiate the DAG — Airflow discovers it by executing this module
yfinance_prices_daily()
