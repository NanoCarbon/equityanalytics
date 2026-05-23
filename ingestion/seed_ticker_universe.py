"""
One-time seed script: populates RAW.TICKER_UNIVERSE from the live Wikipedia
scrape + hardcoded ETF list.

Usage (from inside the Airflow container):
  python3 ingestion/seed_ticker_universe.py

Safe to re-run — uses MERGE so existing rows are not duplicated.
"""
import sys

sys.path.insert(0, '/opt/airflow')

from ingestion.extract import (
    get_sp500_tickers, get_sp400_tickers,
    get_sp600_tickers, get_etf_tickers,
)
from ingestion.load import get_connection


def seed_ticker_universe():
    sp500 = set(get_sp500_tickers())
    sp400 = set(get_sp400_tickers())
    sp600 = set(get_sp600_tickers())
    etfs  = set(get_etf_tickers())

    print(f"S&P 500: {len(sp500)} | S&P 400: {len(sp400)} | S&P 600: {len(sp600)} | ETFs: {len(etfs)}")

    # Build deduped rows: priority sp500 > sp400 > sp600 > etf
    rows = {}
    for t in sp600:
        rows[t] = ("sp600", True)
    for t in sp400:
        rows[t] = ("sp400", True)
    for t in sp500:
        rows[t] = ("sp500", True)
    for t in etfs:
        if t not in rows:
            rows[t] = ("etf", False)

    print(f"Total unique tickers to seed: {len(rows)}")

    conn = get_connection()
    cur  = conn.cursor()

    chunk_size = 500
    items      = list(rows.items())
    upserted   = 0

    for i in range(0, len(items), chunk_size):
        chunk = items[i : i + chunk_size]

        # Stage values into a temp VALUES clause then MERGE
        val_rows = []
        for ticker, (source, is_equity) in chunk:
            t_safe = ticker.replace("'", "''")
            val_rows.append(
                f"('{t_safe}', '{source}', {str(is_equity).upper()})"
            )

        values_sql = ", ".join(val_rows)

        cur.execute(f"""
            MERGE INTO EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE AS tgt
            USING (
                SELECT column1 AS ticker, column2 AS source, column3 AS is_equity
                FROM VALUES {values_sql}
            ) AS src ON tgt.ticker = src.ticker
            WHEN MATCHED THEN UPDATE SET
                source    = src.source,
                is_equity = src.is_equity,
                is_active = TRUE
            WHEN NOT MATCHED THEN INSERT (ticker, source, is_equity, is_active)
                VALUES (src.ticker, src.source, src.is_equity, TRUE)
        """)

        upserted += len(chunk)
        print(f"  Upserted {upserted}/{len(rows)}")

    cur.close()
    conn.close()
    print(f"\nDone — {upserted} tickers in RAW.TICKER_UNIVERSE")


if __name__ == "__main__":
    seed_ticker_universe()
