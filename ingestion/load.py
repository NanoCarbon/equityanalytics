import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import os


def get_connection():
    """Create a Snowflake connection using environment variables."""
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse="TRANSFORM_WH",
        database="EQUITY_ANALYTICS",
        schema="RAW"
    )


def load_dataframe(df: pd.DataFrame, table_name: str, overwrite: bool = False) -> int:
    """
    Bulk load a DataFrame into Snowflake RAW schema.
    Returns the number of rows loaded.
    """
    conn = get_connection()

    # Snowflake convention: uppercase column names
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