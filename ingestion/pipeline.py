from prefect import flow, task, get_run_logger
import pandas as pd


@task(name="extract-prices", retries=3, retry_delay_seconds=30)
def task_extract_prices(tickers: list, lookback_days: int = 365) -> pd.DataFrame:
    logger = get_run_logger()
    from ingestion.extract import extract_prices
    logger.info(f"Extracting prices for {len(tickers)} tickers")
    df = extract_prices(tickers, lookback_days)
    logger.info(f"Extracted {len(df)} rows")
    return df


@task(name="extract-company-info", retries=3, retry_delay_seconds=60)
def task_extract_company_info(tickers: list) -> pd.DataFrame:
    from ingestion.extract import extract_company_info
    return extract_company_info(tickers)


@task(name="load-prices", retries=2)
def task_load_prices(df: pd.DataFrame) -> int:
    logger = get_run_logger()
    from ingestion.load import load_dataframe
    rows = load_dataframe(df, "PRICES", overwrite=True)
    logger.info(f"Loaded {rows} rows to RAW.PRICES")
    return rows


@task(name="load-company-info", retries=2)
def task_load_company_info(df: pd.DataFrame) -> int:
    from ingestion.load import load_dataframe
    return load_dataframe(df, "COMPANY_INFO", overwrite=True)


@flow(name="equity-ingestion-pipeline", log_prints=True)
def equity_pipeline(lookback_days: int = 365):
    from ingestion.extract import TICKERS

    prices_df = task_extract_prices(TICKERS, lookback_days)
    company_df = task_extract_company_info(TICKERS)

    task_load_prices(prices_df)
    task_load_company_info(company_df)


if __name__ == "__main__":
    equity_pipeline()