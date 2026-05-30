"""
DAG: yfinance_supplemental_weekly
Source: yfinance  |  Frequency: weekly (Saturday 11pm ET / 4am UTC Sunday)

Loads five supplemental yfinance data types that don't need daily refreshes:

  1. dividends_and_splits  — Full corporate action history (full overwrite)
  2. earnings_history      — EPS actuals vs. estimates, ~8-20Q per ticker (full overwrite)
  3. analyst_recs          — Upgrade/downgrade history from analyst firms (full overwrite)
  4. analyst_targets       — Price target consensus snapshot (append — builds time series)
  5. company_info          — Sector, industry, market cap, etc. (full overwrite)
                            Runs last — hits the .info endpoint (heaviest, 2s/ticker).
                            Moved here from prices_daily — metadata changes slowly
                            and daily execution caused simultaneous .info load with
                            valuation_daily (429 risk at 12,500+ tickers).

SEQUENTIAL EXECUTION
Tasks run one after another (not in parallel). Yahoo Finance rate-limits at the
IP level — running all five tasks simultaneously from one host produced sustained
429 storms that corrupted most of the fetched data. Sequential execution keeps the
aggregate request rate safe at the cost of total runtime (~18-22 hrs at 12K tickers).
This DAG starts Sunday at 4am UTC and finishes well before Tuesday's daily runs.

First run acts as the backfill — yfinance returns full history for dividends,
earnings, and recommendations, so no separate backfill DAG is needed.
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

# Delay between per-ticker yfinance calls — keeps Yahoo Finance happy
RATE_DELAY = 0.5


@dag(
    dag_id='yfinance_supplemental_weekly',
    description='yfinance | Company info, dividends, earnings, analyst data → Snowflake RAW | weekly',
    schedule='0 4 * * 0',      # 11pm ET Saturday (4am UTC Sunday)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=['yfinance', 'supplemental', 'weekly'],
)
def yfinance_supplemental_weekly():

    @task(execution_timeout=timedelta(hours=2))
    def get_tickers() -> list:
        """
        Load all active tickers from RAW.TICKER_UNIVERSE (primary source).
        Falls back to Wikipedia scrape + ETF list if DB is unreachable.
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

    @task(execution_timeout=timedelta(hours=2))
    def get_equity_tickers() -> list:
        """
        Load equity-only tickers from RAW.TICKER_UNIVERSE (is_equity=TRUE).
        ETFs have no earnings, financial statements, or analyst coverage.
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
    def extract_and_load_company_info(tickers: list) -> int:
        """
        Extract company metadata (sector, industry, market cap, etc.) from
        yfinance and overwrite RAW.COMPANY_INFO. Full overwrite — always
        reflects the current snapshot.

        Runs weekly rather than daily: metadata changes slowly and daily
        execution was causing simultaneous .info calls alongside valuation_daily
        (429 risk at 12,500+ tickers). Weekly cadence is sufficient.

        Returns the number of rows loaded.
        """
        from ingestion.extract import extract_company_info
        from ingestion.load import load_dataframe

        logger.info("Fetching company metadata for %d tickers (2s delay between calls)", len(tickers))
        df = extract_company_info(tickers, delay_seconds=2.0)
        rows = load_dataframe(df, "COMPANY_INFO", overwrite=True)
        logger.info("Overwrote RAW.COMPANY_INFO with %d rows", rows)
        return rows

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def extract_and_load_dividends(tickers: list) -> int:
        """
        Extract full dividend and stock split history for all tickers
        (equities + ETFs — ETFs pay dividends too) and overwrite
        RAW.DIVIDENDS_AND_SPLITS.

        yfinance returns history back to IPO, so the first run is the backfill.
        Returns the number of rows loaded.
        """
        from ingestion.extract import extract_dividends_and_splits
        from ingestion.load import load_dataframe

        logger.info("Extracting dividends/splits for %d tickers", len(tickers))
        df = extract_dividends_and_splits(tickers, delay_seconds=RATE_DELAY)

        if df is None or df.empty:
            logger.warning("No dividend/split data returned")
            return 0

        rows = load_dataframe(df, "DIVIDENDS_AND_SPLITS", overwrite=True)
        logger.info("Overwrote RAW.DIVIDENDS_AND_SPLITS with %d rows", rows)
        return rows

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def extract_and_load_earnings(equity_tickers: list) -> int:
        """
        Extract EPS actuals vs. analyst estimates for equity tickers and
        overwrite RAW.EARNINGS_HISTORY.

        ETFs excluded — they have no earnings. Returns number of rows loaded.
        """
        from ingestion.extract import extract_earnings_history
        from ingestion.load import load_dataframe

        logger.info("Extracting earnings history for %d equity tickers", len(equity_tickers))
        df = extract_earnings_history(equity_tickers, delay_seconds=RATE_DELAY)

        if df is None or df.empty:
            logger.warning("No earnings history returned")
            return 0

        rows = load_dataframe(df, "EARNINGS_HISTORY", overwrite=True)
        logger.info("Overwrote RAW.EARNINGS_HISTORY with %d rows", rows)
        return rows

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def extract_and_load_recommendations(equity_tickers: list) -> int:
        """
        Extract analyst upgrade/downgrade history and overwrite
        RAW.ANALYST_RECOMMENDATIONS.

        yfinance returns several years of firm-level rating changes per ticker.
        Returns number of rows loaded.
        """
        from ingestion.extract import extract_analyst_recommendations
        from ingestion.load import load_dataframe

        logger.info("Extracting recommendations for %d tickers", len(equity_tickers))
        df = extract_analyst_recommendations(equity_tickers, delay_seconds=RATE_DELAY)

        if df is None or df.empty:
            logger.warning("No analyst recommendations returned")
            return 0

        rows = load_dataframe(df, "ANALYST_RECOMMENDATIONS", overwrite=True)
        logger.info("Overwrote RAW.ANALYST_RECOMMENDATIONS with %d rows", rows)
        return rows

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def extract_and_load_price_targets(equity_tickers: list) -> int:
        """
        Extract current analyst price target consensus (mean/high/low/count)
        and APPEND to RAW.ANALYST_PRICE_TARGETS.

        One row per ticker per run — builds a time series of how the consensus
        evolves. Returns number of rows loaded.
        """
        from ingestion.extract import extract_analyst_price_targets
        from ingestion.load import load_dataframe

        logger.info("Extracting price targets for %d tickers", len(equity_tickers))
        df = extract_analyst_price_targets(equity_tickers, delay_seconds=RATE_DELAY)

        if df is None or df.empty:
            logger.warning("No analyst price targets returned")
            return 0

        rows = load_dataframe(df, "ANALYST_PRICE_TARGETS", overwrite=False)
        logger.info("Appended %d rows to RAW.ANALYST_PRICE_TARGETS", rows)
        return rows

    # ── Wire up tasks ─────────────────────────────────────────────
    #
    # get_tickers and get_equity_tickers run in parallel (fast DB reads).
    # All five extraction tasks then run SEQUENTIALLY to stay within Yahoo
    # Finance's IP-level rate limit. Parallel execution from a single host
    # causes sustained 429 storms that corrupt most of the fetched data.
    #
    # Execution order (fastest → slowest, .info endpoint saved for last):
    #
    # get_tickers ──────────────────────────────────────────────────────→ t_div ──→ t_info
    #                                                                         ↑
    # get_equity_tickers ──→ t_earn ──→ t_recs ──→ t_pt ──────────────────────┘
    #
    # Simplified linear chain enforced by >> :
    # t_div >> t_earn >> t_recs >> t_pt >> t_info

    all_tickers    = get_tickers()
    equity_tickers = get_equity_tickers()

    t_div  = extract_and_load_dividends(all_tickers)
    t_earn = extract_and_load_earnings(equity_tickers)
    t_recs = extract_and_load_recommendations(equity_tickers)
    t_pt   = extract_and_load_price_targets(equity_tickers)
    t_info = extract_and_load_company_info(all_tickers)

    # Force sequential execution — >> sets downstream dependency regardless
    # of whether the tasks share input data.
    t_div >> t_earn >> t_recs >> t_pt >> t_info


yfinance_supplemental_weekly()
