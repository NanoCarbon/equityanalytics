"""
DAG: fred_new_series_backfill
Schedule: None (manual trigger only)

Fetches full available history ONLY for series that are active in
RAW.FRED_SELECTION but not yet present in RAW.MACRO_INDICATORS.

RAW.FRED_SELECTION is the canonical source of which series to extract.
Add a series there (is_active = TRUE) instead of editing extract_fred.py,
then trigger this DAG to backfill its history.

Trigger:
  docker compose exec airflow-webserver airflow dags trigger fred_new_series_backfill

Or via Airflow UI → DAGs → fred_new_series_backfill → Trigger DAG.

Expected runtime: ~1s per new series for the FRED API call.
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
    dag_id='fred_new_series_backfill_manual',
    description='FRED | Full history for newly added series → Snowflake RAW | manual',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=['fred', 'macro', 'backfill', 'manual'],
)
def fred_new_series_backfill_manual():

    @task(execution_timeout=timedelta(hours=2))
    def identify_new_series() -> list[dict]:
        """
        Returns the list of series that are active in FRED_SELECTION but have
        no rows yet in RAW.MACRO_INDICATORS.

        Returns a list of dicts: [{"series_id": "...", "local_name": "..."}]
        """
        from ingestion.extract_fred import get_selected_fred_series
        from ingestion.load import get_connection

        conn = get_connection()
        try:
            # All active selections with their names
            selected = get_selected_fred_series(conn)

            # What's already loaded
            cur = conn.cursor()
            cur.execute(
                "SELECT DISTINCT SERIES_ID FROM EQUITY_ANALYTICS.RAW.MACRO_INDICATORS"
            )
            already_loaded = {row[0] for row in cur.fetchall()}
            cur.close()
        finally:
            conn.close()

        new_series = [
            {"series_id": sid, "local_name": name}
            for sid, name in sorted(selected.items())
            if sid not in already_loaded
        ]

        logger.info(
            "FRED_SELECTION active: %d | Already in RAW: %d | New to backfill: %d",
            len(selected), len(already_loaded), len(new_series),
        )
        if new_series:
            logger.info("New series: %s", ", ".join(s["series_id"] for s in new_series))
        else:
            logger.info("No new series — nothing to backfill.")

        return new_series

    @task(retries=2, retry_delay=timedelta(minutes=5), execution_timeout=timedelta(hours=2))
    def backfill_new_series(new_series: list[dict]) -> int:
        """
        Fetches full history for each new series and appends to
        RAW.MACRO_INDICATORS. Uses overwrite=False to protect existing data.

        Marks series as inactive in FRED_SELECTION if they consistently
        return 400 (invalid ID) — keeps the selection table clean.

        Returns the total number of rows appended.
        """
        import os
        import time

        from ingestion.extract_fred import extract_fred_series, _RateLimitError
        from ingestion.load import load_dataframe, get_connection
        import pandas as pd

        if not new_series:
            logger.info("No new series to backfill — skipping.")
            return 0

        api_key = os.environ["FRED_API_KEY"]
        frames = []
        skipped = []
        invalid = []   # series returning 400 — will be deactivated
        total = len(new_series)
        delay = 0.5

        for i, entry in enumerate(new_series, 1):
            series_id  = entry["series_id"]
            local_name = entry["local_name"]
            logger.info("Fetching %s (%d/%d) — %s", series_id, i, total, local_name)

            while True:
                try:
                    df = extract_fred_series(
                        api_key=api_key,
                        series_id=series_id,
                        start_date=FULL_HISTORY_START,
                        series_name=local_name,
                    )
                    break
                except _RateLimitError:
                    delay = min(delay * 2.0, 30.0)
                    logger.warning("Rate limited — sleeping %.1fs before retry", delay)
                    time.sleep(delay)

            if df is None:
                skipped.append(series_id)
                logger.warning("  %s: network failure", series_id)
            elif df.empty:
                # Could be invalid ID (400) or genuinely no data
                skipped.append(series_id)
                invalid.append(series_id)
                logger.warning("  %s: no data returned (invalid ID or no observations)", series_id)
            else:
                frames.append(df)
                delay = max(0.5, delay * 0.9)
                logger.info("  %s: %d observations", series_id, len(df))

            if i < total:
                time.sleep(delay)

        # Deactivate confirmed-invalid series so they don't appear in future backfills
        if invalid:
            conn = get_connection()
            try:
                cur = conn.cursor()
                ids_str = ", ".join(f"'{s}'" for s in invalid)
                cur.execute(f"""
                    UPDATE EQUITY_ANALYTICS.RAW.FRED_SELECTION
                    SET is_active = FALSE,
                        deactivated_at = CURRENT_TIMESTAMP,
                        deactivation_reason = 'No data returned during backfill — likely invalid series ID'
                    WHERE series_id IN ({ids_str})
                """)
                cur.close()
                logger.warning(
                    "Deactivated %d series in FRED_SELECTION (no data): %s",
                    len(invalid), ", ".join(invalid),
                )
            finally:
                conn.close()

        if not frames:
            logger.warning("No data fetched for any new series.")
            return 0

        combined = pd.concat(frames, ignore_index=True)
        logger.info(
            "Fetched %d observations across %d series (%d skipped)",
            len(combined), combined["series_id"].nunique(), len(skipped),
        )

        rows = load_dataframe(combined, "MACRO_INDICATORS", overwrite=False)
        logger.info("Appended %d rows to RAW.MACRO_INDICATORS", rows)
        return rows

    new = identify_new_series()
    backfill_new_series(new)


fred_new_series_backfill_manual()
