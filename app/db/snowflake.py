import os
import time
import logging
import streamlit as st
import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def _load_private_key() -> bytes:
    """
    Load the RSA private key and return DER bytes for the Snowflake connector.
    Key-pair auth never expires — no more manual token rotation.

    Two auth modes, tried in order:

    1. SNOWFLAKE_PRIVATE_KEY  — PEM content as a string (Streamlit Community Cloud
       pattern: store the full key in st.secrets / the Cloud secrets manager).
       Use this when deploying to Community Cloud so no .pem file is needed.

    2. SNOWFLAKE_PRIVATE_KEY_PATH — path to a .pem file on disk (default:
       snowflake_private_key.pem). Use this for local development.

    Optional:
        SNOWFLAKE_PRIVATE_KEY_PASSPHRASE — passphrase if the key is encrypted.
    """
    passphrase_str = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    passphrase = passphrase_str.encode() if passphrase_str else None

    # ── Mode 1: key content from env / st.secrets (Community Cloud) ──────────
    key_content = os.environ.get("SNOWFLAKE_PRIVATE_KEY")
    if key_content:
        try:
            p_key = serialization.load_pem_private_key(
                key_content.strip().encode(),
                password=passphrase,
                backend=default_backend(),
            )
        except (ValueError, TypeError) as e:
            raise RuntimeError(
                "Failed to parse SNOWFLAKE_PRIVATE_KEY env var as a PEM key. "
                "Ensure the full key including headers is stored in the secret, "
                "and that SNOWFLAKE_PRIVATE_KEY_PASSPHRASE is set if encrypted. "
                f"Original error: {e}"
            ) from e
        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    # ── Mode 2: key file on disk (local development) ──────────────────────────
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "snowflake_private_key.pem")
    if not os.path.exists(key_path):
        raise FileNotFoundError(
            f"Snowflake private key not found. "
            "For local dev: set SNOWFLAKE_PRIVATE_KEY_PATH in .env pointing to a .pem file. "
            "For Community Cloud: set SNOWFLAKE_PRIVATE_KEY in st.secrets with the full PEM content."
        )
    try:
        with open(key_path, "rb") as f:
            p_key = serialization.load_pem_private_key(
                f.read(),
                password=passphrase,
                backend=default_backend(),
            )
    except (ValueError, TypeError) as e:
        raise RuntimeError(
            f"Failed to parse Snowflake private key at '{key_path}'. "
            "Check that SNOWFLAKE_PRIVATE_KEY_PASSPHRASE is set if the key is encrypted. "
            f"Original error: {e}"
        ) from e

    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


@st.cache_resource
def get_snowflake_connection():
    logger.info("Opening Snowflake connection (key-pair auth)")
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "TRANSFORM_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "EQUITY_ANALYTICS"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "MARTS"),
        private_key=_load_private_key(),
        network_timeout=30,
        login_timeout=15,
    )


def _clean_sql(sql: str) -> str:
    """
    Strip trailing semicolons and whitespace.
    If the LLM produced multiple semicolon-separated statements,
    keep only the first — Snowflake connector rejects multiple statements.
    """
    sql = sql.strip()
    # Strip trailing semicolon first
    if sql.endswith(";"):
        sql = sql[:-1].strip()
    # If there are still semicolons, the LLM produced multiple statements.
    # Take everything before the first one.
    if ";" in sql:
        first = sql[:sql.index(";")].strip()
        logger.warning("LLM generated multiple statements — using only the first")
        sql = first
    return sql


def _run_query(conn, sql: str) -> pd.DataFrame:
    """Execute SQL against an open connection and return a DataFrame."""
    sql = _clean_sql(sql)
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


@st.cache_data(ttl=60, show_spinner=False)
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