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

    with open(key_path, "rb") as f:
        p_key = serialization.load_pem_private_key(
            f.read(),
            password=passphrase,
            backend=default_backend(),
        )

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
            FROM EQUITY_ANALYTICS.RAW.{table_name.upper()}
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
            FROM EQUITY_ANALYTICS.RAW.{table_name.upper()}
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

        success, num_chunks, num_rows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name.upper(),
            database="EQUITY_ANALYTICS",
            schema="RAW",
            overwrite=overwrite,
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
