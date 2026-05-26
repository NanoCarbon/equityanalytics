#!/usr/bin/env bash
# run_remaining_waves.sh
# Automatically runs backfill waves until no tickers remain.
# Each wave: 1000 tickers, all 4 data types.
# Safe to re-run — idempotent skip logic in backfill.py.

set -a
source "$(dirname "$0")/../.env"
set +a

cd "$(dirname "$0")/.."

WAVE=11

while true; do
    echo ""
    echo "============================================================"
    echo "Generating wave ${WAVE} ticker file..."
    echo "============================================================"

    python - <<PYEOF
import sys; sys.path.insert(0, '.')
from ingestion.load import get_connection, get_loaded_tickers
conn = get_connection()
cur = conn.cursor()
cur.execute('SELECT ticker FROM EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE WHERE is_active = TRUE ORDER BY source, ticker')
universe = [r[0] for r in cur.fetchall()]
cur.close(); conn.close()
loaded = get_loaded_tickers('PRICES')
missing = [t for t in universe if t not in loaded]
wave = missing[:1000]
if not wave:
    print("DONE")
else:
    open('scripts/backfill_wave${WAVE}.txt', 'w').write('\n'.join(wave))
    print(f"REMAINING={len(missing)}")
    print(f"First 5: {wave[:5]}")
    print(f"Last 5:  {wave[-5:]}")
PYEOF

    # Check if Python reported DONE
    CHECK=$(python - <<PYEOF
import sys; sys.path.insert(0, '.')
from ingestion.load import get_connection, get_loaded_tickers
conn = get_connection()
cur = conn.cursor()
cur.execute('SELECT ticker FROM EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE WHERE is_active = TRUE ORDER BY source, ticker')
universe = [r[0] for r in cur.fetchall()]
cur.close(); conn.close()
loaded = get_loaded_tickers('PRICES')
missing = [t for t in universe if t not in loaded]
print(len(missing))
PYEOF
)

    echo "Tickers remaining: ${CHECK}"

    if [ "${CHECK}" -eq 0 ]; then
        echo ""
        echo "============================================================"
        echo "All tickers backfilled! Run dbt full-refresh to propagate:"
        echo "  dbt build --select fact_daily_prices fact_fundamentals fact_valuation_snapshot --full-refresh"
        echo "============================================================"
        break
    fi

    LOGFILE="logs/backfill_wave${WAVE}_$(date +%Y%m%d_%H%M%S).log"
    echo "Launching wave ${WAVE} → ${LOGFILE}"

    python scripts/backfill.py \
        --tickers-file "scripts/backfill_wave${WAVE}.txt" \
        --start 2010-01-01 \
        --types prices company_info financials valuation \
        > "${LOGFILE}" 2>&1

    echo "Wave ${WAVE} complete. Log: ${LOGFILE}"
    WAVE=$((WAVE + 1))
done
