"""
DAGs: fundamentals_weekly + valuation_daily
Replaces: fundamentals_pipeline() + valuation_pipeline() in pipeline_fundamentals.py

Two separate DAGs defined in one file:

  fundamentals_weekly  — runs every Saturday at 11pm ET
    - Extracts income statement, balance sheet, cash flow for ~500 S&P 500 equities
    - Full overwrite (catches retroactive restatements)

  valuation_daily — runs Monday–Friday at 11pm ET
    - Extracts PE, margins, EV/EBITDA, etc. for all ~616 tickers
    - Appends a new snapshot row per ticker (builds a time series)
"""

import sys
import logging
from datetime import datetime, timedelta

sys.path.insert(0, '/opt/airflow')

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
}


# ── DAG 1: Weekly financial statements ───────────────────────────

@dag(
    dag_id='fundamentals_weekly',
    description='Financial statements for S&P 500 equities → Snowflake RAW (weekly)',
    schedule='0 4 * * 0',      # 11pm ET Saturday (4am UTC Sunday)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=['fundamentals', 'weekly'],
)
def fundamentals_weekly():

    @task()
    def get_equity_tickers() -> list:
        """
        Load equity-only tickers from RAW.TICKER_UNIVERSE (is_equity=TRUE).
        ETFs have no 10-K filings and are excluded from financial statements.
        Falls back to Wikipedia scrape if DB is unreachable.
        """
        from ingestion.extract import get_tickers_from_db
        from ingestion.load import get_connection
        conn = get_connection()
        try:
            _, equity_tickers = get_tickers_from_db(conn)
        finally:
            conn.close()
        logger.info("Loaded %d equity tickers", len(equity_tickers))
        return equity_tickers

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def extract_and_load_statements(equity_tickers: list) -> int:
        """
        Extract income statement, balance sheet, and cash flow data
        from yfinance for all equity tickers and overwrite
        RAW.FINANCIAL_STATEMENTS.

        Data is stored in EAV (Entity-Attribute-Value) format so the schema
        is resilient to new line items being added by yfinance.

        Returns the number of rows loaded.
        """
        from ingestion.extract_fundamentals import extract_financial_statements
        from ingestion.load import load_dataframe

        logger.info("Extracting statements for %d tickers (2s delay between calls)", len(equity_tickers))
        df = extract_financial_statements(equity_tickers, delay_seconds=2.0)

        if df is None or df.empty:
            logger.warning("No financial statement data extracted")
            return 0

        logger.info("Extracted %d statement rows for %d tickers",
                    len(df), df['ticker'].nunique() if 'ticker' in df.columns else 0)
        rows = load_dataframe(df, "FINANCIAL_STATEMENTS", overwrite=True)
        logger.info("Overwrote RAW.FINANCIAL_STATEMENTS with %d rows", rows)
        return rows

    tickers = get_equity_tickers()
    extract_and_load_statements(tickers)


# ── DAG 2: Daily valuation snapshots ─────────────────────────────

@dag(
    dag_id='valuation_daily',
    description='Valuation metrics snapshot for all tickers → Snowflake RAW (daily)',
    schedule='0 4 * * 2-6',    # 11pm ET, Mon–Fri (4am UTC Tue–Sat)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=['fundamentals', 'valuation', 'daily'],
)
def valuation_daily():

    @task()
    def get_all_tickers() -> list:
        """
        Load all active tickers from RAW.TICKER_UNIVERSE (equities + ETFs).
        Valuation metrics (PE, P/B, etc.) are available for ETFs too.
        Falls back to Wikipedia scrape if DB is unreachable.
        """
        from ingestion.extract import get_tickers_from_db
        from ingestion.load import get_connection
        conn = get_connection()
        try:
            all_tickers, _ = get_tickers_from_db(conn)
        finally:
            conn.close()
        logger.info("Loaded %d tickers", len(all_tickers))
        return all_tickers

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def extract_and_load_valuations(tickers: list) -> int:
        """
        Extract point-in-time valuation metrics (PE ratio, P/B, EV/EBITDA,
        profit margins, beta, dividend yield, etc.) and APPEND to
        RAW.VALUATION_METRICS.

        Unlike financial statements, we append rather than overwrite because
        each daily run creates a new snapshot — this builds a time series of
        how valuation multiples change over time.

        Returns the number of rows loaded.
        """
        from ingestion.extract_fundamentals import extract_valuation_metrics
        from ingestion.load import load_dataframe

        logger.info("Extracting valuation metrics for %d tickers", len(tickers))
        df = extract_valuation_metrics(tickers, delay_seconds=2.0)

        if df is None or df.empty:
            logger.warning("No valuation data extracted")
            return 0

        logger.info("Extracted valuation metrics for %d tickers", len(df))
        rows = load_dataframe(df, "VALUATION_METRICS", overwrite=False)
        logger.info("Appended %d rows to RAW.VALUATION_METRICS", rows)
        return rows

    tickers = get_all_tickers()
    extract_and_load_valuations(tickers)


# Instantiate both DAGs
fundamentals_weekly()
valuation_daily()
