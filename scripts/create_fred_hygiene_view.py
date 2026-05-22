"""
One-shot script: creates (or replaces) VW_FRED_HYGIENE in Snowflake RAW schema.

Run from repo root:
    python scripts/create_fred_hygiene_view.py
"""

import os
import sys
from pathlib import Path

# Ensure repo root is on path and .env is loaded
repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root))

from dotenv import load_dotenv
load_dotenv(repo_root / ".env")

import snowflake.connector
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key, Encoding, PrivateFormat, NoEncryption,
)
from cryptography.hazmat.backends import default_backend

# ── Auth ──────────────────────────────────────────────────────────────────────

key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "snowflake_private_key.pem")
with open(repo_root / key_path, "rb") as fh:
    p_key = load_pem_private_key(fh.read(), password=None, backend=default_backend())

pk_bytes = p_key.private_bytes(
    encoding=Encoding.DER,
    format=PrivateFormat.PKCS8,
    encryption_algorithm=NoEncryption(),
)

conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    private_key=pk_bytes,
    database="EQUITY_ANALYTICS",
    schema="RAW",
    warehouse="COMPUTE_WH",
)

# ── DDL ───────────────────────────────────────────────────────────────────────

DDL = """
CREATE OR REPLACE VIEW EQUITY_ANALYTICS.RAW.VW_FRED_HYGIENE AS
/*
  FRED Series Hygiene View
  ========================
  One row per FRED series. Shows the two most-recent observation dates and
  cumulative distinct-observation counts, enabling quick answers to:

    - Is this series being updated?   → LATEST_DATE should be recent
    - How deep is the history?        → LATEST_ROWS (distinct obs dates)
    - Was the last run incremental?   → LATEST_ROWS - PREV_ROWS = 1 for a
                                         normal daily run; > 1 = backfill /
                                         gap catch-up

  Column semantics
  ----------------
  FRED_SERIES_ID   : FRED series identifier (e.g. 'UNRATE', 'DGS10')
  FRED_SERIES_NAME : Human-readable name as stored at extraction time
  LATEST_DATE      : Most recent observation date in RAW.MACRO_INDICATORS
  LATEST_ROWS      : Count of DISTINCT observation dates for this series
                     (not raw table rows — deduped to avoid inflation from
                     the 7-day rolling re-extract in macro_daily)
  PREV_DATE        : Second-most-recent observation date
  PREV_ROWS        : LATEST_ROWS - 1 (count before the latest obs was added)
*/
WITH deduped AS (
    -- One row per series + observation date; collapse re-extraction duplicates
    SELECT
        series_id,
        MAX(series_name)                                             AS series_name,
        TO_DATE(DATEADD(second, date / 1000000000, '1970-01-01'))   AS obs_date
    FROM EQUITY_ANALYTICS.RAW.MACRO_INDICATORS
    GROUP BY series_id, date
),
ranked AS (
    SELECT
        series_id,
        series_name,
        obs_date,
        ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY obs_date DESC)  AS rn,
        COUNT(*)     OVER (PARTITION BY series_id)                          AS total_obs
    FROM deduped
)
SELECT
    r1.series_id                  AS FRED_SERIES_ID,
    r1.series_name                AS FRED_SERIES_NAME,
    r1.obs_date                   AS LATEST_DATE,
    r1.total_obs                  AS LATEST_ROWS,
    r2.obs_date                   AS PREV_DATE,
    GREATEST(r1.total_obs - 1, 0) AS PREV_ROWS
FROM ranked r1
LEFT JOIN ranked r2
    ON  r1.series_id = r2.series_id
    AND r2.rn = 2
WHERE r1.rn = 1
ORDER BY r1.series_id
"""

# ── Execute ───────────────────────────────────────────────────────────────────

cur = conn.cursor()
print("Creating VW_FRED_HYGIENE...")
cur.execute(DDL)
print("  Created:", cur.fetchone())

print("Sanity check — row count:")
cur.execute("SELECT COUNT(*) FROM EQUITY_ANALYTICS.RAW.VW_FRED_HYGIENE")
print(f"  {cur.fetchone()[0]} series in view")

print("\nSample rows:")
cur.execute("""
    SELECT FRED_SERIES_ID, FRED_SERIES_NAME, LATEST_DATE, LATEST_ROWS, PREV_DATE, PREV_ROWS
    FROM EQUITY_ANALYTICS.RAW.VW_FRED_HYGIENE
    ORDER BY LATEST_DATE DESC
    LIMIT 8
""")
rows = cur.fetchall()
print(f"  {'SERIES_ID':<25} {'LATEST_DATE':<12} {'LATEST_ROWS':>12} {'PREV_DATE':<12} {'PREV_ROWS':>10}")
print(f"  {'-'*25} {'-'*12} {'-'*12} {'-'*12} {'-'*10}")
for r in rows:
    print(f"  {str(r[0]):<25} {str(r[2]):<12} {r[3]:>12,} {str(r[4] or ''):<12} {r[5]:>10,}")

cur.close()
conn.close()
print("\nDone.")
