"""
Prefect flows for fundamental financial data ingestion.

Three flows:
1. fundamentals_test_pipeline  — small ticker set, validates schema + endpoints
2. fundamentals_backfill       — all tickers, batched, one-time historical load
3. fundamentals_pipeline       — scheduled weekly, full overwrite (data is small)
4. valuation_pipeline          — scheduled daily, append snapshot
"""

from prefect import flow, task, get_run_logger
from datetime import datetime
import pandas as pd


# ── Tasks ─────────────────────────────────────────────────────────────────────

@task(name="get-ticker-list-fundamentals", retries=2)
def task_get_tickers() -> list:
    logger = get_run_logger()
    from ingestion.extract import get_all_tickers
    tickers = get_all_tickers()
    logger.info(f"Loaded {len(tickers)} tickers")
    return tickers


@task(name="filter-equity-tickers", retries=1)
def task_filter_equities(tickers: list) -> list:
    """
    Filter out ETFs and other non-equity tickers that won't have
    financial statements. Uses the ETF list from extract.py.
    """
    logger = get_run_logger()
    from ingestion.extract import get_etf_tickers
    etf_set = set(get_etf_tickers())
    equities = [t for t in tickers if t not in etf_set]
    logger.info(f"Filtered to {len(equities)} equity tickers ({len(tickers) - len(equities)} ETFs excluded)")
    return equities


@task(
    name="extract-financial-statements",
    retries=2,
    retry_delay_seconds=60,
    timeout_seconds=7200,
)
def task_extract_statements(
    tickers: list,
    delay_seconds: float = 2.0,
) -> pd.DataFrame:
    logger = get_run_logger()
    from ingestion.extract_fundamentals import extract_financial_statements
    logger.info(f"Extracting financial statements for {len(tickers)} tickers")
    df = extract_financial_statements(tickers, delay_seconds=delay_seconds)
    if df.empty:
        logger.warning("No financial statement data extracted")
    else:
        logger.info(f"Extracted {len(df)} statement rows")
    return df


@task(
    name="extract-valuation-metrics",
    retries=2,
    retry_delay_seconds=60,
    timeout_seconds=7200,
)
def task_extract_valuations(
    tickers: list,
    delay_seconds: float = 2.0,
) -> pd.DataFrame:
    logger = get_run_logger()
    from ingestion.extract_fundamentals import extract_valuation_metrics
    logger.info(f"Extracting valuation metrics for {len(tickers)} tickers")
    df = extract_valuation_metrics(tickers, delay_seconds=delay_seconds)
    if df.empty:
        logger.warning("No valuation data extracted")
    else:
        logger.info(f"Extracted valuation metrics for {len(df)} tickers")
    return df


@task(name="load-financial-statements", retries=2)
def task_load_statements(df: pd.DataFrame, overwrite: bool = True) -> int:
    logger = get_run_logger()
    if df.empty:
        logger.info("No statement data to load — skipping")
        return 0
    from ingestion.load import load_dataframe
    rows = load_dataframe(df, "FINANCIAL_STATEMENTS", overwrite=overwrite)
    logger.info(f"Loaded {rows} rows to RAW.FINANCIAL_STATEMENTS (overwrite={overwrite})")
    return rows


@task(name="load-valuation-metrics", retries=2)
def task_load_valuations(df: pd.DataFrame, overwrite: bool = False) -> int:
    logger = get_run_logger()
    if df.empty:
        logger.info("No valuation data to load — skipping")
        return 0
    from ingestion.load import load_dataframe
    rows = load_dataframe(df, "VALUATION_METRICS", overwrite=overwrite)
    logger.info(f"Loaded {rows} rows to RAW.VALUATION_METRICS (overwrite={overwrite})")
    return rows


# ── Flows ─────────────────────────────────────────────────────────────────────

@flow(name="fundamentals-test-pipeline", log_prints=True)
def fundamentals_test_pipeline(
    tickers: list = None,
    delay_seconds: float = 1.0,
):
    """
    Phase 1 test flow: small ticker set to validate endpoints and schema.
    Run this first before backfill or scheduled loads.

    Usage:
        prefect deployment run 'fundamentals-test-pipeline/test-fundamentals'
    """
    logger = get_run_logger()

    if tickers is None:
        # Mix of large-cap, mid-cap, different sectors, and ETFs
        tickers = [
            "AAPL", "MSFT", "JPM", "JNJ", "XOM",  # mega-cap, diverse sectors
            "NVDA", "COST", "NEE", "PLD", "SHW",    # growth, REIT, industrials
            "SPY", "BND", "GLD",                      # ETFs (should have no statements)
        ]

    logger.info(f"Test extraction for {len(tickers)} tickers: {tickers}")

    # Split equities and ETFs
    from ingestion.extract import get_etf_tickers
    etf_set = set(get_etf_tickers())
    equity_tickers = [t for t in tickers if t not in etf_set]
    etf_tickers = [t for t in tickers if t in etf_set]

    logger.info(f"Equities: {equity_tickers}")
    logger.info(f"ETFs (expect empty statements): {etf_tickers}")

    # Extract financial statements (equities only)
    stmt_df = task_extract_statements(equity_tickers, delay_seconds=delay_seconds)

    if not stmt_df.empty:
        logger.info(f"\n--- Financial Statements Summary ---")
        logger.info(f"Total rows: {len(stmt_df)}")
        logger.info(f"Tickers with data: {stmt_df['ticker'].nunique()}")
        logger.info(f"Statement types: {stmt_df['statement_type'].value_counts().to_dict()}")
        logger.info(f"Frequencies: {stmt_df['frequency'].value_counts().to_dict()}")
        logger.info(f"Unique line items: {stmt_df['line_item'].nunique()}")
        logger.info(f"Period range: {stmt_df['period_end_date'].min()} to {stmt_df['period_end_date'].max()}")

        # Load to Snowflake
        task_load_statements(stmt_df, overwrite=True)

    # Extract valuation metrics (all tickers including ETFs)
    val_df = task_extract_valuations(tickers, delay_seconds=delay_seconds)

    if not val_df.empty:
        logger.info(f"\n--- Valuation Metrics Summary ---")
        logger.info(f"Total rows: {len(val_df)}")
        non_null = val_df.drop(columns=["ticker", "snapshot_date", "extracted_at"]).notna().sum()
        logger.info(f"Fields with data: {(non_null > 0).sum()}/{len(non_null)}")

        # Load to Snowflake
        task_load_valuations(val_df, overwrite=True)

    logger.info("\n--- Test Complete ---")
    logger.info("Next steps:")
    logger.info("  1. Check RAW.FINANCIAL_STATEMENTS and RAW.VALUATION_METRICS in Snowflake")
    logger.info("  2. Run: dbt build --select +fact_fundamentals +fact_valuation_snapshot")
    logger.info("  3. If tests pass, proceed to backfill")


@flow(name="fundamentals-backfill-pipeline", log_prints=True)
def fundamentals_backfill_pipeline(
    batch_size: int = 50,
    batch_delay_seconds: int = 30,
    delay_seconds: float = 2.0,
):
    """
    One-time historical backfill of all equity tickers' financial statements.

    yfinance returns up to ~4 years annual + ~8 quarters by default,
    so this is a bounded dataset. Full overwrite to RAW since we're
    replacing whatever the test load put there.

    Usage:
        prefect deployment run 'fundamentals-backfill-pipeline/backfill-fundamentals'
    """
    import time
    logger = get_run_logger()

    all_tickers = task_get_tickers()
    equity_tickers = task_filter_equities(all_tickers)

    logger.info(f"Backfilling {len(equity_tickers)} equity tickers in batches of {batch_size}")

    batches = [
        equity_tickers[i:i + batch_size]
        for i in range(0, len(equity_tickers), batch_size)
    ]

    all_stmt_frames = []
    failed_batches = []

    for i, batch in enumerate(batches, 1):
        logger.info(f"Batch {i}/{len(batches)}: {batch[0]} → {batch[-1]}")

        try:
            from ingestion.extract_fundamentals import extract_financial_statements
            df = extract_financial_statements(batch, delay_seconds=delay_seconds)

            if not df.empty:
                all_stmt_frames.append(df)
                logger.info(f"Batch {i}: {len(df)} rows")
            else:
                logger.info(f"Batch {i}: no data returned")

        except Exception as e:
            logger.error(f"Batch {i} failed: {e}")
            failed_batches.append({"batch_num": i, "tickers": batch, "error": str(e)})

        if i < len(batches):
            logger.info(f"Sleeping {batch_delay_seconds}s...")
            time.sleep(batch_delay_seconds)

    # Combine and load all at once (overwrite)
    if all_stmt_frames:
        combined = pd.concat(all_stmt_frames, ignore_index=True)
        logger.info(f"Total: {len(combined)} statement rows for {combined['ticker'].nunique()} tickers")
        task_load_statements(combined, overwrite=True)

    # Now do valuation metrics for ALL tickers (including ETFs)
    logger.info("Extracting valuation metrics for all tickers...")
    val_df = task_extract_valuations(all_tickers, delay_seconds=delay_seconds)
    task_load_valuations(val_df, overwrite=True)

    if failed_batches:
        logger.warning(f"{len(failed_batches)} batches failed:")
        for fb in failed_batches:
            logger.warning(f"  Batch {fb['batch_num']}: {fb['error']}")

    logger.info("Backfill complete. Next: dbt build --select +fact_fundamentals +fact_valuation_snapshot --full-refresh")


@flow(
    name="fundamentals-ingestion-pipeline",
    description="Weekly ELT: yfinance financial statements → Snowflake RAW",
    log_prints=True,
)
def fundamentals_pipeline():
    """
    Scheduled weekly (Saturday). Full overwrite of financial statements.
    yfinance always returns the same ~4yr/~8Q window, so overwrite is
    simpler and catches any retroactive restatements.
    """
    all_tickers = task_get_tickers()
    equity_tickers = task_filter_equities(all_tickers)

    stmt_df = task_extract_statements(equity_tickers, delay_seconds=2.0)
    task_load_statements(stmt_df, overwrite=True)


@flow(
    name="valuation-snapshot-pipeline",
    description="Daily ELT: yfinance valuation metrics → Snowflake RAW",
    log_prints=True,
)
def valuation_pipeline():
    """
    Scheduled daily (weekdays). Appends a new snapshot row per ticker.
    Builds a time series of PE, margins, etc.
    """
    tickers = task_get_tickers()
    val_df = task_extract_valuations(tickers, delay_seconds=2.0)
    task_load_valuations(val_df, overwrite=False)
