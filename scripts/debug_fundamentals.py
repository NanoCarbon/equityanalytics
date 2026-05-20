"""
Quick diagnostic: find out why tickers are being dropped in int_fundamentals_pivoted.
Checks which RAW tickers are missing the anchor line items used in the pivot filter.
"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv()
from ingestion.load import get_connection

conn = get_connection()
cur = conn.cursor()

cur.execute("SELECT COUNT(DISTINCT ticker) FROM EQUITY_ANALYTICS.RAW.FINANCIAL_STATEMENTS")
raw_tickers = cur.fetchone()[0]
print(f"RAW tickers in FINANCIAL_STATEMENTS: {raw_tickers}")

cur.execute("""
    SELECT COUNT(DISTINCT ticker)
    FROM EQUITY_ANALYTICS.RAW.FINANCIAL_STATEMENTS
    WHERE line_item IN ('Total Revenue', 'Total Assets', 'Operating Cash Flow')
""")
anchor_tickers = cur.fetchone()[0]
print(f"Tickers with at least one anchor line item: {anchor_tickers}")
print(f"Tickers that will be filtered OUT by the pivot WHERE clause: {raw_tickers - anchor_tickers}")

print("\nTop line items for tickers WITHOUT any anchor line item:")
cur.execute("""
    WITH no_anchors AS (
        SELECT DISTINCT ticker
        FROM EQUITY_ANALYTICS.RAW.FINANCIAL_STATEMENTS
        WHERE ticker NOT IN (
            SELECT DISTINCT ticker
            FROM EQUITY_ANALYTICS.RAW.FINANCIAL_STATEMENTS
            WHERE line_item IN ('Total Revenue', 'Total Assets', 'Operating Cash Flow')
        )
    )
    SELECT line_item, COUNT(DISTINCT f.ticker) AS ticker_count
    FROM EQUITY_ANALYTICS.RAW.FINANCIAL_STATEMENTS f
    JOIN no_anchors n ON f.ticker = n.ticker
    GROUP BY line_item
    ORDER BY ticker_count DESC
    LIMIT 25
""")
print(f"\n  {'Line Item':<52} {'Tickers':>8}")
print("  " + "-" * 62)
for row in cur.fetchall():
    print(f"  {row[0]:<52} {int(row[1]):>8}")

conn.close()
