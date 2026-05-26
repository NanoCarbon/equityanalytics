"""
DAG: fred_macro_daily
Schedule: Monday–Friday at 11pm ET (04:00 UTC next day)

What it does:
  Pulls all 108 FRED macro series incrementally — only observations newer than
  the most recent date already in RAW.MACRO_INDICATORS are fetched and appended.

  FRED does retroactively revise data (GDP estimates, employment numbers), but
  full overwrite every day was destroying the historical backfill. The better
  pattern is incremental append for the daily run + a manual macro_backfill
  trigger any time you want to re-pull full history with latest revisions.

  Falls back to a 30-day lookback if the table is empty.
"""

import sys
import logging
from datetime import datetime, timedelta

sys.path.insert(0, '/opt/airflow')

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
}


@dag(
    dag_id='fred_macro_daily',
    description='FRED | Macro indicator observations → Snowflake RAW | daily',
    schedule='0 4 * * 2-6',    # 11pm ET, Mon–Fri (4am UTC Tue–Sat)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=['fred', 'macro', 'daily'],
)
def fred_macro_daily():

    @task(retries=3, retry_delay=timedelta(minutes=1), execution_timeout=timedelta(hours=2))
    def extract_and_load_macro() -> int:
        """
        Incrementally fetch new FRED observations and append to RAW.MACRO_INDICATORS.

        Finds the most recent date already loaded and only requests data from
        that date forward — preserving the full historical backfill.
        Falls back to a 30-day lookback if the table is empty.

        Returns the number of rows appended.
        """
        import os
        from datetime import date, timedelta
        from ingestion.extract_fred import extract_all_fred_series, get_selected_fred_series
        from ingestion.load import load_dataframe, get_max_date, get_connection

        api_key = os.environ["FRED_API_KEY"]

        # Load series selection from catalog (falls back to FRED_SERIES dict on error)
        conn = get_connection()
        try:
            series_dict = get_selected_fred_series(conn)
        finally:
            conn.close()

        max_date = get_max_date("MACRO_INDICATORS")
        if max_date:
            # Re-fetch from 7 days before max to catch any recent FRED revisions
            start_date = (max_date - timedelta(days=7)).strftime("%Y-%m-%d")
            lookback_days = None
            logger.info("Incremental load: fetching FRED data from %s", start_date)
        else:
            start_date = None
            lookback_days = 30
            logger.info("Table empty — falling back to 30-day lookback")

        df = extract_all_fred_series(
            api_key, lookback_days=lookback_days, start_date=start_date, series_dict=series_dict
        )

        if df is None or df.empty:
            logger.info("No new macro data returned")
            return 0

        logger.info("Extracted %d FRED observations", len(df))
        rows = load_dataframe(df, "MACRO_INDICATORS", overwrite=False)
        logger.info("Appended %d rows to RAW.MACRO_INDICATORS", rows)
        return rows

    extract_and_load_macro()


fred_macro_daily()
