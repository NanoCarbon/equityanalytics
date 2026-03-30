import os
import time
import logging
import streamlit as st
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


@st.cache_resource
def get_snowflake_connection():
    logger.info("Opening Snowflake connection")
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse="TRANSFORM_WH",
        database="EQUITY_ANALYTICS",
        schema="MARTS",
        authenticator="programmatic_access_token",
        token=os.environ["SNOWFLAKE_TOKEN"],
        network_timeout=30,
        login_timeout=15,
    )


def _run_query(conn, sql: str) -> pd.DataFrame:
    """Execute SQL against an open connection and return a DataFrame."""
    start = time.monotonic()
    cursor = conn.cursor()
    cursor.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 30")
    cursor.execute(sql)
    df = cursor.fetch_pandas_all()
    df.columns = [c.lower() for c in df.columns]
    logger.info("Query %.2fs -> %d rows", time.monotonic() - start, len(df))
    return df


# Snowflake error codes that indicate a stale/dropped session rather than a
# genuine token expiry. The connector surfaces errno 390114 for both cases,
# so we attempt a reconnect first before surfacing the error to the user.
_RECONNECT_CODES = {
    390114,  # "Authentication token has expired" — also fired on idle session drop
    390113,  # Session no longer exists
    250001,  # Unable to connect to Snowflake
}


def execute_sql(sql: str) -> pd.DataFrame:
    conn = get_snowflake_connection()
    try:
        return _run_query(conn, sql)
    except snowflake.connector.errors.ProgrammingError as e:
        if e.errno in _RECONNECT_CODES:
            logger.warning(
                "Snowflake session stale (errno %d) — clearing cache and reconnecting", e.errno
            )
            get_snowflake_connection.clear()
            fresh_conn = get_snowflake_connection()
            try:
                return _run_query(fresh_conn, sql)
            except Exception as retry_err:
                logger.error("Retry after reconnect failed: %s", retry_err)
                raise
        logger.error("Snowflake query error: %s", e)
        raise
    except Exception as e:
        logger.error("Unexpected query error: %s", e)
        get_snowflake_connection.clear()
        raise


@st.cache_data(ttl=300, show_spinner=False)
def execute_sql_cached(sql: str) -> pd.DataFrame:
    return execute_sql(sql)


@st.cache_data(ttl=3600, show_spinner=False)
def load_securities() -> pd.DataFrame:
    return execute_sql("""
        SELECT ticker, company_name, sector, industry,
               market_cap_usd, first_trading_date, last_trading_date
        FROM EQUITY_ANALYTICS.MARTS.DIM_SECURITY
        ORDER BY market_cap_usd DESC NULLS LAST
    """)


@st.cache_data(ttl=3600, show_spinner=False)
def load_macro_series() -> pd.DataFrame:
    return execute_sql("""
        SELECT series_id, series_name,
               MIN(observation_date) AS first_observation,
               MAX(observation_date) AS last_observation,
               COUNT(*)              AS observation_count
        FROM EQUITY_ANALYTICS.MARTS.FACT_MACRO_READINGS
        GROUP BY series_id, series_name
        ORDER BY series_id
    """)


@st.cache_data(ttl=3600, show_spinner=False)
def load_summary_stats() -> dict:
    df = execute_sql("""
        SELECT
            (SELECT COUNT(DISTINCT ticker)    FROM EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES)    AS equity_count,
            (SELECT COUNT(DISTINCT series_id) FROM EQUITY_ANALYTICS.MARTS.FACT_MACRO_READINGS)  AS macro_series_count,
            (SELECT COUNT(*)                  FROM EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES)    AS price_rows,
            (SELECT COUNT(*)                  FROM EQUITY_ANALYTICS.MARTS.FACT_FUNDAMENTALS)    AS fundamental_rows,
            (SELECT MIN(price_date)           FROM EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES)    AS price_start,
            (SELECT MAX(price_date)           FROM EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES)    AS price_end
    """)
    return df.iloc[0].to_dict() if not df.empty else {}