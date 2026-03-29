from prefect import flow, task, get_run_logger
import pandas as pd


@task(name="get-max-date", retries=2)
def task_get_max_date() -> object:
    logger = get_run_logger()
    from ingestion.load import get_max_date
    max_date = get_max_date("PRICES")
    if max_date:
        logger.info(f"Latest date in Snowflake: {max_date} — running incremental load")
    else:
        logger.info("No existing data found — running full historical load")
    return max_date


@task(name="extract-prices", retries=3, retry_delay_seconds=30)
def task_extract_prices(max_date: object, lookback_days: int = 365) -> pd.DataFrame:
    logger = get_run_logger()
    from ingestion.extract import extract_prices, TICKERS
    df = extract_prices(TICKERS, start_date=max_date, lookback_days=lookback_days)
    if df.empty:
        logger.info("No new prices to load")
    else:
        logger.info(f"Extracted {len(df)} new rows")
    return df


@task(name="extract-company-info", retries=3, retry_delay_seconds=60)
def task_extract_company_info() -> pd.DataFrame:
    from ingestion.extract import extract_company_info, TICKERS
    return extract_company_info(TICKERS)


@task(name="load-prices", retries=2)
def task_load_prices(df: pd.DataFrame) -> int:
    logger = get_run_logger()
    if df.empty:
        logger.info("No new prices to load — skipping")
        return 0
    from ingestion.load import load_dataframe
    # overwrite=False means APPEND not replace
    rows = load_dataframe(df, "PRICES", overwrite=False)
    logger.info(f"Appended {rows} rows to RAW.PRICES")
    return rows


@task(name="load-company-info", retries=2)
def task_load_company_info(df: pd.DataFrame) -> int:
    from ingestion.load import load_dataframe
    # Company info always overwrites — metadata changes infrequently
    return load_dataframe(df, "COMPANY_INFO", overwrite=True)


@flow(name="equity-ingestion-pipeline", log_prints=True)
def equity_pipeline(lookback_days: int = 365):
    # Check what's already loaded
    max_date = task_get_max_date()

    # Extract only new data
    prices_df = task_extract_prices(max_date, lookback_days)
    company_df = task_extract_company_info()

    # Load
    task_load_prices(prices_df)
    task_load_company_info(company_df)


if __name__ == "__main__":
    equity_pipeline()