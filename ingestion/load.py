import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import os
from datetime import datetime, date


def get_connection():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse="TRANSFORM_WH",
        database="EQUITY_ANALYTICS",
        schema="RAW",
        authenticator="programmatic_access_token",
        token=os.environ["SNOWFLAKE_TOKEN"]
    )


def get_max_date(table_name: str) -> date | None:
    """
    Returns the most recent date already loaded into a table.
    Returns None if the table is empty or doesn't exist.
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT MAX(TO_DATE(DATEADD(second, DATE / 1000000000, '1970-01-01')))
            FROM EQUITY_ANALYTICS.RAW.{table_name.upper()}
        """)
        result = cursor.fetchone()[0]
        return result
    except Exception:
        return None
    finally:
        conn.close()

def get_min_date(table_name: str):
    """Returns the earliest date already loaded — used to know where backfill should stop."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT MIN(TO_DATE(DATEADD(second, DATE / 1000000000, '1970-01-01')))
            FROM EQUITY_ANALYTICS.RAW.{table_name.upper()}
        """)
        result = cursor.fetchone()[0]
        return result
    except Exception:
        return None
    finally:
        conn.close()
        
def load_dataframe(df: pd.DataFrame, table_name: str, overwrite: bool = False) -> int:
    """
    Bulk load a DataFrame into Snowflake RAW schema.
    Returns the number of rows loaded.
    """
    conn = get_connection()

    df.columns = [c.upper() for c in df.columns]

    success, num_chunks, num_rows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=table_name.upper(),
        database="EQUITY_ANALYTICS",
        schema="RAW",
        overwrite=overwrite,
        auto_create_table=True
    )

    print(f"Loaded {num_rows} rows to RAW.{table_name.upper()}")
    conn.close()
    return num_rows