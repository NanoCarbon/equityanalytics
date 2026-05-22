import os
import logging
from datetime import datetime, date

import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────
# Table name whitelist — prevents SQL injection via f-string queries
# ─────────────────────────────────────────────────────────────────

_ALLOWED_TABLES = frozenset({
    "PRICES",
    "COMPANY_INFO",
    "MACRO_INDICATORS",
    "FINANCIAL_STATEMENTS",
    "VALUATION_METRICS",
    "DIVIDENDS_AND_SPLITS",
    "EARNINGS_HISTORY",
    "ANALYST_RECOMMENDATIONS",
    "ANALYST_PRICE_TARGETS",
    "FRED_RELEASES",
    "FRED_SERIES_CATALOG",
})


def _validate_table_name(table_name: str) -> str:
    """
    Return the upper-cased table name if it is in the known whitelist,
    otherwise raise ValueError.  Prevents arbitrary SQL execution via
    f-string interpolation in get_max_date / get_min_date.
    """
    upper = table_name.upper()
    if upper not in _ALLOWED_TABLES:
        raise ValueError(
            f"Table '{table_name}' is not in the allowed list: {sorted(_ALLOWED_TABLES)}"
        )
    return upper


# ─────────────────────────────────────────────────────────────────
# Auth helper
# ─────────────────────────────────────────────────────────────────

def _load_private_key() -> bytes:
    """
    Load the RSA private key from disk and return it as DER bytes,
    ready to pass directly to the Snowflake connector.

    Reads:
        SNOWFLAKE_PRIVATE_KEY_PATH       path to .pem file (default: snowflake_private_key.pem)
        SNOWFLAKE_PRIVATE_KEY_PASSPHRASE passphrase if the key is encrypted (optional)
    """
    key_path = os.environ.get(
        "SNOWFLAKE_PRIVATE_KEY_PATH", "snowflake_private_key.pem"
    )
    passphrase_str = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    passphrase = passphrase_str.encode() if passphrase_str else None

    if not os.path.exists(key_path):
        raise FileNotFoundError(
            f"Snowflake private key not found at '{key_path}'. "
            "Set SNOWFLAKE_PRIVATE_KEY_PATH in your .env or place the key file there."
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


# ─────────────────────────────────────────────────────────────────
# Connection
# ─────────────────────────────────────────────────────────────────

def get_connection():
    """
    Open a Snowflake connection using RSA key-pair authentication.
    Key-pair auth never expires — no more token rotation.
    """
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse="TRANSFORM_WH",
        database="EQUITY_ANALYTICS",
        schema="RAW",
        private_key=_load_private_key(),
        network_timeout=30,
        login_timeout=15,
    )


# ─────────────────────────────────────────────────────────────────
# Incremental boundary helpers
# ─────────────────────────────────────────────────────────────────

def get_max_date(table_name: str) -> date | None:
    """
    Returns the most recent date already loaded into a table.
    Returns None if the table is empty or doesn't exist.
    Raises on auth or network failures so callers aren't silently misled.
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT MAX(TO_DATE(DATEADD(second, DATE / 1000000000, '1970-01-01')))
            FROM EQUITY_ANALYTICS.RAW.{_validate_table_name(table_name)}
        """)
        result = cursor.fetchone()[0]
        logger.info("get_max_date(%s) = %s", table_name, result)
        return result
    except snowflake.connector.errors.ProgrammingError as e:
        if "does not exist" in str(e).lower():
            logger.info("Table %s does not exist yet — treating as empty", table_name)
            return None
        logger.error("Snowflake error in get_max_date(%s): %s", table_name, e)
        raise
    finally:
        conn.close()


def get_min_date(table_name: str) -> date | None:
    """
    Returns the earliest date already loaded — used to know where backfill should stop.
    Raises on auth or network failures so callers aren't silently misled.
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT MIN(TO_DATE(DATEADD(second, DATE / 1000000000, '1970-01-01')))
            FROM EQUITY_ANALYTICS.RAW.{_validate_table_name(table_name)}
        """)
        result = cursor.fetchone()[0]
        logger.info("get_min_date(%s) = %s", table_name, result)
        return result
    except snowflake.connector.errors.ProgrammingError as e:
        if "does not exist" in str(e).lower():
            logger.info("Table %s does not exist yet — treating as empty", table_name)
            return None
        logger.error("Snowflake error in get_min_date(%s): %s", table_name, e)
        raise
    finally:
        conn.close()


def get_loaded_tickers(table_name: str) -> set:
    """
    Returns the set of distinct ticker symbols already present in a RAW table.
    Returns an empty set if the table is empty or does not exist yet.

    Used to diff the current ticker universe against what's already loaded,
    so backfills can skip tickers that already have data.
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT DISTINCT TICKER FROM EQUITY_ANALYTICS.RAW.{_validate_table_name(table_name)}"
        )
        tickers = {row[0] for row in cursor.fetchall()}
        logger.info("get_loaded_tickers(%s) = %d distinct tickers", table_name, len(tickers))
        return tickers
    except snowflake.connector.errors.ProgrammingError as e:
        if "does not exist" in str(e).lower():
            logger.info("Table %s does not exist yet — returning empty set", table_name)
            return set()
        logger.error("Snowflake error in get_loaded_tickers(%s): %s", table_name, e)
        raise
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────
# Bulk loader
# ─────────────────────────────────────────────────────────────────

def load_dataframe(df: pd.DataFrame, table_name: str, overwrite: bool = False) -> int:
    """
    Bulk load a DataFrame into the Snowflake RAW schema.
    Returns the number of rows loaded.
    Raises ValueError if df is empty to prevent silent no-op loads.
    """
    if df is None or df.empty:
        raise ValueError(
            f"load_dataframe called with empty DataFrame for table {table_name}. "
            "Check upstream extraction before calling load."
        )

    conn = get_connection()
    try:
        df.columns = [c.upper() for c in df.columns]

        # Explicitly DROP before loading when overwrite=True.
        # write_pandas(overwrite=True) is unreliable for large tables in some
        # connector versions — it may emit CREATE TABLE (not CREATE OR REPLACE),
        # which fails when the table already exists.  Explicit DROP is simpler
        # and version-agnostic.  write_pandas then sees a clean slate and
        # auto_create_table=True creates it fresh.
        if overwrite:
            cursor = conn.cursor()
            cursor.execute(
                f"DROP TABLE IF EXISTS EQUITY_ANALYTICS.RAW.{table_name.upper()}"
            )
            cursor.close()

        success, num_chunks, num_rows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name.upper(),
            database="EQUITY_ANALYTICS",
            schema="RAW",
            overwrite=False,        # table is already gone; let auto_create_table handle it
            auto_create_table=True,
        )

        if not success:
            raise RuntimeError(f"write_pandas reported failure for table {table_name}")

        logger.info(
            "Loaded %d rows to RAW.%s in %d chunk(s) (overwrite=%s)",
            num_rows, table_name.upper(), num_chunks, overwrite,
        )
        return num_rows

    except Exception as e:
        logger.error("Failed to load data to RAW.%s: %s", table_name.upper(), e)
        raise
    finally:
        conn.close()
