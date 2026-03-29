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


@task(
    name="load-macro-indicators",
    retries=2,
    description="Load FRED macro data to Snowflake RAW.MACRO_INDICATORS"
)
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
    max_date = task_get_max_date()
    prices_df = task_extract_prices(max_date, lookback_days)
    company_df = task_extract_company_info()
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


if __name__ == "__main__":
    equity_pipeline()