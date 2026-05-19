"""
DAG: macro_daily
Replaces: macro_pipeline() in ingestion/pipeline.py

Schedule: Monday–Friday at 9am ET (14:00 UTC)
What it does:
  1. Pulls 95 FRED macro series (interest rates, inflation, GDP, etc.)
  2. Overwrites RAW.MACRO_INDICATORS in Snowflake

Full overwrite because FRED retroactively revises data (e.g. GDP estimates
get updated months later). Overwriting ensures we always have the latest values.
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
    dag_id='macro_daily',
    description='FRED macro indicators → Snowflake RAW (daily)',
    schedule='0 14 * * 1-5',   # 9am ET, Mon–Fri
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=['macro', 'daily'],
)
def macro_daily():

    @task(retries=3, retry_delay=timedelta(minutes=1))
    def extract_and_load_macro(lookback_days: int = 365) -> int:
        """
        Pull the last N days of all 95 FRED series and overwrite
        RAW.MACRO_INDICATORS. Single task because the DataFrame is
        too large to pass through XCom.

        Returns the number of rows loaded.
        """
        import os
        from ingestion.extract_fred import extract_all_fred_series
        from ingestion.load import load_dataframe

        api_key = os.environ["FRED_API_KEY"]
        df = extract_all_fred_series(api_key, lookback_days=lookback_days)

        if df is None or df.empty:
            logger.info("No macro data returned")
            return 0

        logger.info("Extracted %d FRED observations", len(df))
        rows = load_dataframe(df, "MACRO_INDICATORS", overwrite=True)
        logger.info("Overwrote RAW.MACRO_INDICATORS with %d rows", rows)
        return rows

    extract_and_load_macro()


macro_daily()
