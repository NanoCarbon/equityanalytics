"""
DAG: macro_backfill
Schedule: None (manual trigger only)

Fetches FULL available history for every FRED series (back to 1990 or the
series start date, whichever is later) and OVERWRITES RAW.MACRO_INDICATORS.

Run this once to populate historical macro data. It is safe to re-run —
FRED returns the latest revised values so a re-run also refreshes any
retroactive revisions (e.g., GDP estimates get restated months later).

Trigger from the Airflow UI or CLI:
  docker compose exec airflow-webserver airflow dags trigger macro_backfill

At ~0.5s per series, ~108 series takes roughly 1 minute to fetch.
FRED history varies by series: GDP back to 1947, DFF to 1954,
VIX to 1990, SOFR to 2018. Expect ~500K rows total.
"""

import sys
import logging
from datetime import datetime, timedelta

sys.path.insert(0, '/opt/airflow')

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

# How far back to request. FRED silently clips to the series start date,
# so '1900-01-01' is safe — it just gets you everything available.
FULL_HISTORY_START = '1900-01-01'

DEFAULT_ARGS = {
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
}


@dag(
    dag_id='fred_macro_backfill_manual',
    description='FRED | Full macro history re-pull → Snowflake RAW | manual',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=['fred', 'macro', 'backfill', 'manual'],
)
def fred_macro_backfill_manual():

    @task(retries=2, retry_delay=timedelta(minutes=5), execution_timeout=timedelta(hours=2))
    def extract_and_load_full_history() -> int:
        """
        Fetch complete history for all ~108 FRED series and overwrite
        RAW.MACRO_INDICATORS. Returns the number of rows loaded.

        Uses 0.5s rate-limit delay between series calls (~1 min total fetch time).
        Full overwrite ensures retroactively revised series (e.g., GDP, payrolls)
        always reflect the latest published values.
        """
        import os
        from ingestion.extract_fred import extract_all_fred_series, get_selected_fred_series
        from ingestion.load import load_dataframe, get_connection

        api_key = os.environ["FRED_API_KEY"]

        # Load series selection from catalog (falls back to FRED_SERIES dict on error)
        conn = get_connection()
        try:
            series_dict = get_selected_fred_series(conn)
        finally:
            conn.close()

        logger.info("Starting full FRED history backfill from %s for %d series", FULL_HISTORY_START, len(series_dict))
        df = extract_all_fred_series(api_key, start_date=FULL_HISTORY_START, series_dict=series_dict)

        if df is None or df.empty:
            logger.warning("No data returned from FRED — check API key and network")
            return 0

        logger.info(
            "Fetched %d observations across %d series",
            len(df), df["series_id"].nunique(),
        )
        rows = load_dataframe(df, "MACRO_INDICATORS", overwrite=True)
        logger.info("Overwrote RAW.MACRO_INDICATORS with %d rows", rows)
        return rows

    extract_and_load_full_history()


fred_macro_backfill_manual()
