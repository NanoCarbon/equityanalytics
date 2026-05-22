"""
DAG: fred_new_series_backfill
Schedule: None (manual trigger only)

Fetches full available history ONLY for FRED series that are defined in
FRED_SERIES (extract_fred.py) but not yet present in RAW.MACRO_INDICATORS.

This is the right tool after adding new series to FRED_SERIES — it avoids
re-pulling history for the ~93 series already loaded, which would waste
~90 API calls and overwrite well-established historical data.

Use macro_backfill (full overwrite) only when you want to refresh ALL
series with FRED's latest revised values (e.g. after GDP restatements).

Trigger:
  docker compose exec airflow-webserver airflow dags trigger fred_new_series_backfill

Or via Airflow UI → DAGs → fred_new_series_backfill → Trigger DAG.

Expected runtime: ~0.5s per new series + 1s per series for FRED API.
At 80 new series: roughly 2–3 minutes total.
"""

import sys
import logging
from datetime import datetime, timedelta

sys.path.insert(0, '/opt/airflow')

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

# Pull full history from this date (FRED clips to series start automatically)
FULL_HISTORY_START = '1900-01-01'

DEFAULT_ARGS = {
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
}


@dag(
    dag_id='fred_new_series_backfill',
    description='FRED full history → Snowflake RAW.MACRO_INDICATORS (new series only, manual trigger)',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=['macro', 'backfill', 'manual', 'fred'],
)
def fred_new_series_backfill():

    @task()
    def identify_new_series() -> list[str]:
        """
        Returns the list of series IDs that are in FRED_SERIES but have
        no rows yet in RAW.MACRO_INDICATORS.
        """
        from ingestion.extract_fred import FRED_SERIES
        from ingestion.load import get_connection

        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT DISTINCT SERIES_ID FROM EQUITY_ANALYTICS.RAW.MACRO_INDICATORS"
            )
            already_loaded = {row[0] for row in cur.fetchall()}
            cur.close()
        finally:
            conn.close()

        all_defined = set(FRED_SERIES.keys())
        new_series = sorted(all_defined - already_loaded)

        logger.info(
            "FRED_SERIES defined: %d | Already in RAW: %d | New to backfill: %d",
            len(all_defined), len(already_loaded), len(new_series),
        )
        if new_series:
            logger.info("New series: %s", ", ".join(new_series))
        else:
            logger.info("No new series — nothing to backfill.")

        return new_series

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def backfill_new_series(new_series: list[str]) -> int:
        """
        Fetches full history for each new series and appends to
        RAW.MACRO_INDICATORS. Uses overwrite=False to protect existing data.

        Returns the total number of rows appended.
        """
        import os
        import time
        import requests
        import pandas as pd
        from datetime import datetime as dt

        from ingestion.extract_fred import FRED_SERIES, extract_fred_series
        from ingestion.load import load_dataframe

        if not new_series:
            logger.info("No new series to backfill — skipping.")
            return 0

        api_key = os.environ["FRED_API_KEY"]
        frames = []
        skipped = []
        total = len(new_series)

        for i, series_id in enumerate(new_series, 1):
            logger.info("Fetching %s (%d/%d)...", series_id, i, total)
            try:
                df = extract_fred_series(
                    api_key=api_key,
                    series_id=series_id,
                    start_date=FULL_HISTORY_START,
                )
                if df is not None and not df.empty:
                    frames.append(df)
                    logger.info("  %s: %d observations", series_id, len(df))
                else:
                    skipped.append(series_id)
                    logger.warning("  %s: no data returned (premium or invalid?)", series_id)
            except Exception as e:
                skipped.append(series_id)
                logger.error("  %s: fetch failed — %s", series_id, e)

            if i < total:
                time.sleep(0.5)   # respect FRED rate limit

        if not frames:
            logger.warning("No data fetched for any new series.")
            return 0

        combined = pd.concat(frames, ignore_index=True)
        logger.info(
            "Fetched %d observations across %d series (%d skipped/empty)",
            len(combined), combined["series_id"].nunique(), len(skipped),
        )
        if skipped:
            logger.warning("Skipped series (premium/invalid): %s", ", ".join(skipped))

        rows = load_dataframe(combined, "MACRO_INDICATORS", overwrite=False)
        logger.info("Appended %d rows to RAW.MACRO_INDICATORS", rows)
        return rows

    new = identify_new_series()
    backfill_new_series(new)


fred_new_series_backfill()
