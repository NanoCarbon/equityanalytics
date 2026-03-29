from prefect import flow, task, get_run_logger
from datetime import datetime
import pandas as pd


@task(name="get-ticker-list", retries=2)
def task_get_tickers() -> list:
    logger = get_run_logger()
    from ingestion.extract import get_all_tickers
    tickers = get_all_tickers()
    logger.info(f"Loaded {len(tickers)} tickers")
    return tickers


@task(name="get-max-date", retries=2)
def task_get_max_date() -> object:
    logger = get_run_logger()
    from ingestion.load import get_max_date
    max_date = get_max_date("PRICES")
    if max_date:
        logger.info(f"Latest date in Snowflake: {max_date} — incremental load")
    else:
        logger.info("No existing data — full historical load")
    return max_date


@task(name="extract-prices", retries=3, retry_delay_seconds=60)
def task_extract_prices(
    tickers: list,
    max_date: object,
    lookback_days: int = 365
) -> pd.DataFrame:
    logger = get_run_logger()
    from ingestion.extract import extract_prices
    df = extract_prices(tickers, start_date=max_date, lookback_days=lookback_days)
    if df.empty:
        logger.info("No new prices to load")
    else:
        logger.info(f"Extracted {len(df)} new rows")
    return df


@task(
    name="extract-company-info",
    retries=2,
    retry_delay_seconds=60,
    timeout_seconds=3600
)
def task_extract_company_info(tickers: list) -> pd.DataFrame:
    logger = get_run_logger()
    from ingestion.extract import extract_company_info
    logger.info(f"Fetching metadata for {len(tickers)} tickers with 2s delay")
    df = extract_company_info(tickers, delay_seconds=2.0)
    logger.info(f"Fetched metadata for {len(df)} tickers")
    return df


@task(name="load-prices", retries=2)
def task_load_prices(df: pd.DataFrame) -> int:
    logger = get_run_logger()
    if df.empty:
        logger.info("No new prices to load — skipping")
        return 0
    from ingestion.load import load_dataframe
    rows = load_dataframe(df, "PRICES", overwrite=False)
    logger.info(f"Appended {rows} rows to RAW.PRICES")
    return rows


@task(name="load-company-info", retries=2)
def task_load_company_info(df: pd.DataFrame) -> int:
    from ingestion.load import load_dataframe
    return load_dataframe(df, "COMPANY_INFO", overwrite=True)


@task(
    name="extract-fred-data",
    retries=3,
    retry_delay_seconds=30,
    description="Pull macro indicator data from FRED API"
)
def task_extract_fred(lookback_days: int = 365) -> pd.DataFrame:
    logger = get_run_logger()
    import os
    from ingestion.extract_fred import extract_all_fred_series
    api_key = os.environ["FRED_API_KEY"]
    df = extract_all_fred_series(api_key, lookback_days=lookback_days)
    logger.info(f"Extracted {len(df)} FRED observations")
    return df


@task(name="load-macro-indicators", retries=2)
def task_load_macro(df: pd.DataFrame) -> int:
    logger = get_run_logger()
    if df.empty:
        logger.info("No macro data to load")
        return 0
    from ingestion.load import load_dataframe
    rows = load_dataframe(df, "MACRO_INDICATORS", overwrite=True)
    logger.info(f"Loaded {rows} rows to RAW.MACRO_INDICATORS")
    return rows


@flow(name="equity-ingestion-pipeline", log_prints=True)
def equity_pipeline(lookback_days: int = 365):
    tickers = task_get_tickers()
    max_date = task_get_max_date()
    prices_df = task_extract_prices(tickers, max_date, lookback_days)
    company_df = task_extract_company_info(tickers)
    task_load_prices(prices_df)
    task_load_company_info(company_df)


@flow(
    name="macro-ingestion-pipeline",
    description="Daily ELT: FRED API → Snowflake RAW macro indicators",
    log_prints=True
)
def macro_pipeline(lookback_days: int = 365):
    macro_df = task_extract_fred(lookback_days)
    task_load_macro(macro_df)


@flow(name="backfill-pipeline", log_prints=True)
def backfill_pipeline(
    start_date: str = "2010-01-01",
    batch_size: int = 50,
    batch_delay_seconds: int = 30
):
    """
    One-time historical backfill flow.
    Safe to re-run — only appends rows not already present.
    """
    import time
    from ingestion.extract import get_all_tickers, extract_prices
    from ingestion.load import load_dataframe, get_min_date

    logger = get_run_logger()

    tickers = get_all_tickers()
    logger.info(f"Backfilling {len(tickers)} tickers from {start_date}")

    existing_min = get_min_date("PRICES")
    if existing_min:
        end_date = str(existing_min)
        logger.info(f"Existing data starts at {end_date} — backfilling before that")
    else:
        end_date = datetime.today().strftime("%Y-%m-%d")
        logger.info("No existing data — full load")

    if existing_min and str(existing_min) <= start_date:
        logger.info(f"Data already starts at or before {start_date} — nothing to backfill")
        return

    batches = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
    logger.info(f"Processing {len(batches)} batches of up to {batch_size} tickers")

    total_rows = 0
    failed_batches = []

    for i, batch in enumerate(batches, 1):
        logger.info(f"Batch {i}/{len(batches)}: {batch[0]} → {batch[-1]}")

        try:
            df = extract_prices(
                tickers=batch,
                start_date_str=start_date,
                end_date_str=end_date
            )

            if df.empty:
                logger.info(f"Batch {i}: no data returned — skipping")
            else:
                rows = load_dataframe(df, "PRICES", overwrite=False)
                total_rows += rows
                logger.info(f"Batch {i}: loaded {rows} rows — running total: {total_rows}")

        except Exception as e:
            logger.error(f"Batch {i} failed: {e}")
            failed_batches.append({
                "batch_num": i,
                "tickers": batch,
                "error": str(e)
            })
            logger.info(f"Continuing to batch {i + 1}...")

        if i < len(batches):
            logger.info(f"Sleeping {batch_delay_seconds}s...")
            time.sleep(batch_delay_seconds)

    logger.info(f"Backfill complete — {total_rows} total rows loaded")

    if failed_batches:
        logger.warning(f"{len(failed_batches)} batches failed:")
        for fb in failed_batches:
            logger.warning(f"  Batch {fb['batch_num']}: {fb['error']}")
        logger.warning("Re-run backfill_pipeline to retry — successful batches won't duplicate")
    else:
        logger.info("All batches completed successfully")
        logger.info("Next: dbt build --select fact_daily_prices --full-refresh")


if __name__ == "__main__":
    equity_pipeline()