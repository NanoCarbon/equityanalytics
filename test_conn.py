import os
from dotenv import load_dotenv
load_dotenv()

token = os.environ.get('SNOWFLAKE_TOKEN', '')
print(f'Token length: {len(token)}')
print(f'Token starts with: {token[:20]}')
print(f'Token ends with: {token[-20:]}')
print(f'User: {os.environ.get("SNOWFLAKE_USER")}')
print(f'Account: {os.environ.get("SNOWFLAKE_ACCOUNT")}')

import snowflake.connector
try:
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse="TRANSFORM_WH",
        database="EQUITY_ANALYTICS",
        schema="RAW",
        authenticator="programmatic_access_token",
        token=token
    )
    print("SUCCESS - connected to Snowflake")
    conn.close()
except Exception as e:
    print(f"FAILED: {e}")