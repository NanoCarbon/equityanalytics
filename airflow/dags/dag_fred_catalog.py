"""
DAG: fred_catalog_refresh
Schedule: 11pm ET on the 1st of each month (04:00 UTC on 2nd)

Crawls all FRED statistical releases (~300) and builds a metadata catalog
of every series available — with popularity scores, frequency, units, and
history bounds. Stores two tables in Snowflake RAW:

  RAW.FRED_RELEASES      — one row per release (~300 rows)
  RAW.FRED_SERIES_CATALOG — one row per unique series (~50-150K rows)

These tables act as an information schema for FRED. Use them to:

  1. Find popular series you're not ingesting yet:
       SELECT release_name, series_id, title, popularity, frequency, units,
              observation_start
       FROM   RAW.FRED_SERIES_CATALOG
       WHERE  series_id NOT IN (SELECT DISTINCT series_id FROM RAW.MACRO_INDICATORS)
         AND  popularity >= 50
       ORDER  BY popularity DESC;

  2. Browse what's available in a specific domain:
       SELECT series_id, title, popularity, frequency, observation_start
       FROM   RAW.FRED_SERIES_CATALOG
       WHERE  release_name ILIKE '%employment%'
       ORDER  BY popularity DESC;

  3. See your current coverage rate:
       SELECT
           COUNT(*)                                               AS total_in_catalog,
           COUNT(DISTINCT mi.series_id)                          AS currently_ingesting,
           ROUND(COUNT(DISTINCT mi.series_id) / COUNT(*) * 100, 1) AS coverage_pct
       FROM   RAW.FRED_SERIES_CATALOG c
       LEFT JOIN (SELECT DISTINCT series_id FROM RAW.MACRO_INDICATORS) mi
           ON c.series_id = mi.series_id;

Run time: ~10–20 minutes (0.5s rate limit * ~300 releases + pagination).
Safe to re-run — both tables are full overwrites.
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
    dag_id='fred_catalog_refresh',
    description='Build FRED series metadata catalog → Snowflake RAW (monthly)',
    schedule='0 4 2 * *',          # 11pm ET on 1st of each month (4am UTC on 2nd)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=['macro', 'catalog', 'monthly'],
)
def fred_catalog_refresh():

    @task(retries=1, retry_delay=timedelta(minutes=10), execution_timeout=timedelta(hours=1))
    def build_and_load_catalog() -> dict:
        """
        Crawl all FRED releases, collect series metadata, and overwrite
        RAW.FRED_RELEASES and RAW.FRED_SERIES_CATALOG.

        Returns a summary dict with row counts (logged to XCom for observability).
        Single task — DataFrames can't be passed through XCom, and the two loads
        are tightly coupled (releases must exist before catalog).
        """
        import os
        from ingestion.extract_fred_catalog import extract_fred_catalog
        from ingestion.load import load_dataframe

        api_key = os.environ.get("FRED_API_KEY")
        if not api_key:
            raise ValueError("FRED_API_KEY environment variable is required but not set")

        logger.info("Starting FRED catalog crawl...")
        releases_df, catalog_df = extract_fred_catalog(api_key)

        results = {"releases_loaded": 0, "series_loaded": 0}

        if releases_df is not None and not releases_df.empty:
            rows = load_dataframe(releases_df, "FRED_RELEASES", overwrite=True)
            logger.info("Loaded %d rows to RAW.FRED_RELEASES", rows)
            results["releases_loaded"] = rows
        else:
            logger.warning("No release data to load")

        if catalog_df is not None and not catalog_df.empty:
            rows = load_dataframe(catalog_df, "FRED_SERIES_CATALOG", overwrite=True)
            logger.info("Loaded %d rows to RAW.FRED_SERIES_CATALOG", rows)
            results["series_loaded"] = rows
        else:
            logger.warning("No catalog data to load")

        logger.info(
            "Catalog refresh complete — %d releases, %d series",
            results["releases_loaded"], results["series_loaded"],
        )
        return results

    build_and_load_catalog()


fred_catalog_refresh()
