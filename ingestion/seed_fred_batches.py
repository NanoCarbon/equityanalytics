"""
Seeds FRED_SELECTION from catalog by popularity tier.

Usage (from inside the Airflow container):
  python3 ingestion/seed_fred_batches.py --batch 1   # pop >= 70  (~93 series)
  python3 ingestion/seed_fred_batches.py --batch 2   # pop 50-69  (~498 series)
  python3 ingestion/seed_fred_batches.py --batch 3   # pop 30-49  (~1,548 series)
  python3 ingestion/seed_fred_batches.py --batch 4   # pop 15-29  (~3,765 series)

After running, trigger the backfill DAG:
  airflow dags trigger fred_new_series_backfill
"""
import sys
import argparse

sys.path.insert(0, '/opt/airflow')
from ingestion.load import get_connection

BATCH_FILTERS = {
    1: ("c.popularity >= 70",               "pop ≥ 70"),
    2: ("c.popularity >= 50 AND c.popularity < 70", "pop 50–69"),
    3: ("c.popularity >= 30 AND c.popularity < 50", "pop 30–49"),
    4: ("c.popularity >= 15 AND c.popularity < 30", "pop 15–29"),
}


def seed_batch(batch: int):
    where, label = BATCH_FILTERS[batch]
    print(f"Seeding Batch {batch} ({label}) into FRED_SELECTION...")

    conn = get_connection()
    cur = conn.cursor()

    # Fetch catalog series for this tier not already selected
    cur.execute(f"""
        SELECT c.series_id, c.title AS local_name, c.release_name AS category
        FROM   EQUITY_ANALYTICS.RAW.FRED_SERIES_CATALOG c
        LEFT   JOIN EQUITY_ANALYTICS.RAW.FRED_SELECTION s ON s.series_id = c.series_id
        WHERE  s.series_id IS NULL
          AND  UPPER(c.title) NOT LIKE '%DISCONTINUED%'
          AND  {where}
        ORDER  BY c.popularity DESC
    """)
    rows = cur.fetchall()
    print(f"Found {len(rows)} series to insert")

    if not rows:
        print("Nothing to insert — already up to date.")
        cur.close()
        conn.close()
        return

    # Insert in chunks
    chunk_size = 500
    inserted = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        values = ", ".join(
            "('{sid}', '{nm}', '{cat}', TRUE)".format(
                sid=r[0].replace("'", "''"),
                nm=(r[1] or r[0]).replace("'", "''")[:490],
                cat=(r[2] or 'Uncategorized').replace("'", "''")[:99],
            )
            for r in chunk
        )
        cur.execute(
            "INSERT INTO EQUITY_ANALYTICS.RAW.FRED_SELECTION "
            "(series_id, local_name, category, is_active) VALUES " + values
        )
        inserted += len(chunk)
        print(f"  Inserted {inserted}/{len(rows)}")

    print(f"\nBatch {batch} seeding complete — {inserted} series added to FRED_SELECTION")
    print("Now trigger the backfill DAG:")
    print("  docker compose exec airflow-webserver airflow dags trigger fred_new_series_backfill")

    cur.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, choices=[1, 2, 3, 4], required=True,
                        help="Which batch tier to seed (1=pop≥70, 2=50-69, 3=30-49, 4=15-29)")
    args = parser.parse_args()
    seed_batch(args.batch)
