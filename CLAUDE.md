# CLAUDE.md — Equity Analytics Pipeline

This file is read by Claude Code at the start of every session. Keep it current.

---

## What this project is

A production-style ELT pipeline and AI-powered analytics application:
- **Ingestion:** Python + yfinance + FRED API, orchestrated by Apache Airflow on local Docker
- **Warehouse:** Snowflake (`EQUITY_ANALYTICS` DB) — three schemas: RAW → STAGING → MARTS
- **Transformation:** dbt Core (Kimball dimensional model)
- **Application:** Streamlit + Claude API (natural language → SQL → Plotly chart)
- **CI/CD:** GitHub Actions (dbt tests + AI code review on every PR)

---

## Current state (as of May 2026)

- **12,524 tickers** in `RAW.TICKER_UNIVERSE` — S&P Composite 1500, all US exchange-listed securities (NASDAQ Trader), and 5 international indices (FTSE 100, TSX 60, ASX 200, Nikkei 225, DAX 40)
- **All backfill waves complete (1–11)** — prices, company_info, financials, and valuation loaded from 2010-01-01 for all tickers
- **dbt full-refresh complete** — `fact_daily_prices`, `fact_fundamentals`, `fact_valuation_snapshot` rebuilt; PASS=23, WARN=2, ERROR=0
- **6,100 active FRED macro series** — all 4 catalog-expansion batches complete; 4.1M RAW rows; 1.84M mart rows
- **Branch:** `yfinance-phase3-international`

### Ticker universe breakdown

| Source | Count | Country | yfinance suffix |
|---|---|---|---|
| nasdaq_trader | 10,280 | US | (none) |
| sp600 | 603 | US | (none) |
| sp500 | 503 | US | (none) |
| sp400 | 400 | US | (none) |
| nikkei225 | 225 | JP | `.T` |
| asx200 | 200 | AU | `.AX` |
| etf | 113 | US | (none) |
| ftse100 | 100 | GB | `.L` |
| tsx60 | 60 | CA | `.TO` |
| dax40 | 40 | DE | `.DE` (most); `.PA` for Airbus |
| **TOTAL** | **12,524** | | |

7,293 equities (fundamentals eligible) / 5,231 ETFs.

---

## Running things

### dbt (run from repo root — `profiles.yml` lives here)

dbt does NOT auto-read `.env`. Load env vars first:

```powershell
# Windows PowerShell
Get-Content .env | Where-Object { $_ -notmatch '^\s*#' -and $_ -match '=' } | ForEach-Object {
    $k, $v = $_ -split '=', 2; Set-Item "env:$($k.Trim())" $v.Trim()
}
```

```bash
# Git Bash
set -a && source .env && set +a
export SNOWFLAKE_PRIVATE_KEY_PATH="$(pwd)/snowflake_private_key.pem"
```

Then run from `dbt_project/` or pass `--profiles-dir ..`:
```bash
dbt debug --profiles-dir ..
dbt build --profiles-dir ..
dbt build --profiles-dir .. --select fact_daily_prices --full-refresh
dbt build --profiles-dir .. --select fact_daily_prices fact_fundamentals fact_valuation_snapshot --full-refresh
dbt test  --profiles-dir ..
```

**dbt is NOT installed inside Airflow Docker containers.** Always run locally.

### Airflow (Docker Compose, local)

```powershell
docker compose up -d airflow-webserver airflow-scheduler   # start
docker compose down                                         # stop (keeps data)
docker compose ps                                           # check status
```

UI: `http://localhost:8080` (admin / admin for local dev)

Trigger a DAG:
```powershell
docker compose exec airflow-webserver airflow dags trigger <dag_id>
```

**`docker compose restart` does NOT reload env vars.** Always use `up -d` after env var changes.

### Streamlit app

```powershell
streamlit run app/streamlit_app.py --server.port 8501
```

Or double-click `equity_analytics.bat` — does: docker up → Airflow health wait → db_health_check → Streamlit.

### Backfill script (for future ticker additions)

```bash
# Generate missing-ticker file
python -c "
import sys; sys.path.insert(0, '.')
from ingestion.load import get_connection, get_loaded_tickers
conn = get_connection()
cur = conn.cursor()
cur.execute('SELECT ticker FROM EQUITY_ANALYTICS.RAW.TICKER_UNIVERSE WHERE is_active = TRUE ORDER BY source, ticker')
universe = [r[0] for r in cur.fetchall()]
cur.close(); conn.close()
loaded = get_loaded_tickers('PRICES')
missing = [t for t in universe if t not in loaded]
open('scripts/backfill_missing.txt', 'w').write('\n'.join(missing))
print(f'{len(missing)} tickers to backfill')
"

# Launch
nohup python scripts/backfill.py \
  --tickers-file scripts/backfill_missing.txt \
  --start 2010-01-01 \
  --types prices company_info financials valuation \
  > logs/backfill_$(date +%Y%m%d_%H%M%S).log 2>&1 &
echo "PID: $!"
```

`scripts/backfill.py` is fully idempotent — each data type independently skips already-loaded tickers. Safe to re-run after interruption.

---

## Architecture

### Data flow — never skip layers
```
yfinance / FRED API  →  RAW (append/overwrite)  →  dbt STAGING  →  dbt MARTS
```
RAW is append-only source of truth. Never write directly to STAGING or MARTS from Python.

### Warehouse schema

```
EQUITY_ANALYTICS
├── RAW
│   ├── TICKER_UNIVERSE         -- canonical ticker list (12,524 tickers); FK source for all RAW tables
│   │     columns: ticker (PK), source, is_active, is_equity, exchange, country,
│   │              yfinance_suffix, fundamentals_cohort (0–3 or NULL for ETFs), added_at, deactivated_at
│   │     Source priority MERGE rule: sp500 > sp400 > sp600 > etf > nasdaq_trader (S&P/ETF never overwritten)
│   ├── PRICES                  -- daily OHLCV, ~25M+ rows, 2010–present, incremental append
│   ├── COMPANY_INFO            -- metadata snapshot, overwrite each run
│   ├── MACRO_INDICATORS        -- 6,100 FRED series, 4.1M rows, incremental append
│   ├── FINANCIAL_STATEMENTS    -- EAV format (income/balance/cashflow), weekly overwrite
│   ├── VALUATION_METRICS       -- point-in-time ratios, daily append
│   ├── DIVIDENDS_AND_SPLITS    -- full corporate action history, weekly overwrite
│   ├── EARNINGS_HISTORY        -- EPS actuals vs. estimates, weekly overwrite
│   ├── ANALYST_RECOMMENDATIONS -- upgrade/downgrade history, weekly overwrite
│   ├── ANALYST_PRICE_TARGETS   -- consensus price target snapshot, weekly append
│   ├── FRED_RELEASES           -- FRED publication metadata, monthly overwrite
│   ├── FRED_SERIES_CATALOG     -- all FRED series metadata (~800K rows), monthly overwrite
│   ├── FRED_SELECTION          -- canonical selection: which series to extract (persists across refreshes)
│   └── VW_FRED_HYGIENE         -- view: latest/prev obs date + row count per series
├── STAGING (views)             -- 13 stg_* models; one per RAW source
├── INTERMEDIATE (views)        -- int_daily_returns, int_fundamentals_pivoted
└── MARTS (tables)              -- dim_date, dim_security, fact_daily_prices, fact_macro_readings,
                                   fact_fundamentals, fact_valuation_snapshot
```

Key Snowflake quirk: DATE columns in RAW are stored as nanosecond Unix epoch (NUMBER(38,0)).
Convert with: `TO_DATE(DATEADD(second, date / 1000000000, '1970-01-01'))`

### Incremental vs. full-refresh rules

| Table | Strategy | Why |
|---|---|---|
| RAW.PRICES | Incremental append (by date) | Too large to reload daily |
| RAW.MACRO_INDICATORS | Incremental append (7-day overlap) | Preserves FRED backfill; catches revisions |
| RAW.COMPANY_INFO | Full overwrite | Small, always-current snapshot |
| RAW.FINANCIAL_STATEMENTS | Full overwrite | Catches retroactive restatements |
| RAW.VALUATION_METRICS | Daily append | Builds point-in-time time series |
| All supplemental RAW tables | Weekly overwrite | Small, corrections needed |

Adding new tickers always requires `dbt build --select fact_daily_prices --full-refresh` — incremental filter misses brand-new tickers.

---

## DAGs

Naming convention: `{source}_{content}_{frequency}`. LocalExecutor — concurrent subprocesses.

| DAG | Schedule (UTC) | File | Description |
|---|---|---|---|
| `web_tickers_daily` | 8am Mon–Fri (`0 8 * * 2-6`) | dag_web_tickers_daily.py | sync_sp_indices → sync_nasdaq_trader → sync_international_indices (Mon ET only) |
| `yfinance_prices_daily` | 4am Mon–Fri (`0 4 * * 2-6`) | dag_yfinance_prices_daily.py | Prices (incremental) |
| `fred_macro_daily` | 4am Mon–Fri (`0 4 * * 2-6`) | dag_fred_macro_daily.py | FRED macro series (reads FRED_SELECTION at runtime, incremental) |
| `yfinance_valuation_daily` | 6pm Mon–Fri (`0 18 * * 2-6`) | dag_yfinance_fundamentals.py | Valuation snapshot (daily append) — noon EDT |
| `yfinance_fundamentals_weekly` | 4am Sunday (`0 4 * * 0`) | dag_yfinance_fundamentals.py | Financial statements for ~7,293 equity tickers (full overwrite) |
| `yfinance_supplemental_weekly` | 4am Sunday (`0 4 * * 0`) | dag_yfinance_supplemental_weekly.py | Dividends → earnings → recommendations → price targets → company info (sequential, ~18h) |
| `fred_catalog_monthly` | 4am 2nd of month (`0 4 2 * *`) | dag_fred_catalog_monthly.py | FRED metadata catalog (monthly overwrite) |
| `yfinance_prices_backfill_manual` | None (manual) | dag_yfinance_prices_backfill_manual.py | Historical OHLCV back to 2010 |
| `yfinance_new_tickers_backfill_manual` | None (manual) | dag_yfinance_new_tickers_backfill_manual.py | Backfill only tickers not yet in RAW.PRICES |
| `fred_macro_backfill_manual` | None (manual, **paused**) | dag_fred_macro_backfill_manual.py | Full FRED history, all series — full overwrite (dangerous) |
| `fred_new_series_backfill_manual` | None (manual) | dag_fred_new_series_backfill_manual.py | Full history for NEW series only — append-safe, idempotent |

`web_tickers_daily` runs before `yfinance_prices_daily` so DAGs always read a fresh ticker list.

`yfinance_valuation_daily` runs at noon EDT (not overnight) to avoid overlap with weekly runs and to keep it during waking hours.

`yfinance_supplemental_weekly` tasks run **sequentially** (not in parallel) — Yahoo Finance rate-limits at the IP level and 5 parallel workers cause sustained 429 storms at 12K+ tickers.

`sync_international_indices` checks `logical_date.weekday() == 1` (Tuesday UTC = Monday ET) — skips Tue–Fri since index membership only changes at quarterly rebalances.

---

## Key files

| File | Purpose |
|---|---|
| `ingestion/extract.py` | yfinance: prices, company info, dividends, earnings, analyst data; `get_tickers_from_db(conn)` primary ticker source |
| `ingestion/extract_ticker_universe.py` | Ticker universe: `fetch_nasdaq_trader_us()`, `fetch_ftse100/tsx60/asx200/nikkei225/dax40()` |
| `ingestion/seed_ticker_universe.py` | One-shot seeder: S&P/ETF + NASDAQ Trader + international MERGE, cohort assignment |
| `ingestion/extract_fred.py` | FRED API: `FRED_SERIES` dict is fallback-only; `get_selected_fred_series(conn)` reads from RAW.FRED_SELECTION |
| `ingestion/extract_fred_catalog.py` | FRED releases + series catalog crawler |
| `ingestion/extract_fundamentals.py` | Financial statements (EAV) + valuation metrics |
| `ingestion/load.py` | `load_dataframe()`, `get_connection()`, `get_max_date()`, `get_loaded_tickers()` |
| `scripts/backfill.py` | Parameterized backfill CLI: `--tickers-file`, `--source`, `--missing`, `--types`, rate-limit flags; idempotent |
| `scripts/db_health_check.py` | Standalone health check — run anytime, exits 0/1 |
| `app/streamlit_app.py` | Main app entry point (4 tabs: Overview, AI Analytics, Event Study, DB Health) |
| `app/components/chat.py` | AI Analytics tab — Claude SQL generation + Plotly |
| `app/db/snowflake.py` | `execute_sql_cached()`, `_load_private_key()` (supports file path and PEM string for Community Cloud) |
| `agents/prompts.py` | `SYSTEM_PROMPT` (schema context for SQL generation), `FRED_CATEGORIES` |
| `dbt_project/models/` | staging (13) / intermediate (2) / marts (6) dbt models |
| `profiles.yml` | dbt connection profile (gitignored; reads env vars) |
| `docker-compose.yml` | Airflow Docker Compose (4 services: db, webserver, scheduler, init) |
| `equity_analytics.bat` | App launcher for Windows |

---

## Adding new FRED series

`RAW.FRED_SELECTION` is the canonical source — no code changes needed:
```sql
-- 1. Find it in the catalog
SELECT series_id, title, popularity FROM RAW.FRED_SERIES_CATALOG
WHERE title ILIKE '%your term%' AND UPPER(title) NOT LIKE '%DISCONTINUED%'
ORDER BY popularity DESC;

-- 2. Add to selection
INSERT INTO RAW.FRED_SELECTION (series_id, local_name, category, is_active)
VALUES ('SERIES_ID', 'Descriptive name', 'Category', TRUE);
```
```bash
# 3. Backfill full history for new series only
docker compose exec airflow-webserver airflow dags trigger fred_new_series_backfill

# 4. Full-refresh the mart
dbt build --profiles-dir .. --select fact_macro_readings --full-refresh
```

---

## Common pitfalls

- **dbt without env vars** — will error `Env var required but not provided: 'SNOWFLAKE_ACCOUNT'`. Always load `.env` first.
- **`SNOWFLAKE_PRIVATE_KEY_PATH` for dbt** — must be absolute path when running from `dbt_project/`. Use `export SNOWFLAKE_PRIVATE_KEY_PATH="$(pwd)/snowflake_private_key.pem"` from repo root before cd-ing in.
- **`macro_backfill` vs `fred_new_series_backfill`** — `macro_backfill` overwrites ALL series history. Always prefer `fred_new_series_backfill` for new series. `macro_backfill` is paused intentionally.
- **`get_connection()` not `get_snowflake_connection()`** — the function in `ingestion/load.py` is `get_connection()`.
- **dbt in Docker** — dbt is not installed in Airflow containers. Run locally.
- **`docker compose restart` vs `docker compose up -d`** — `restart` does NOT reload env vars. Always use `up -d` after env var changes.
- **FRED catalog + backfill simultaneously** — both hit FRED API causing 429 bursts. Run separately.
- **`order_by` for FRED `/releases`** — `popularity` is invalid for `/releases`; use `release_id`. Only `/release/series` accepts `popularity`.
- **ETFs in financial statements** — `extract_fundamentals.py` filters out ETFs. Don't remove this filter.
- **yfinance `'Infinity'` string** — PE ratios for negative-earnings tickers return the string `'Infinity'`. Load code coerces via `float()` + `math.isfinite()`. Keep this guard.
- **Wikipedia table scanning** — Wikipedia pages have many tables before the constituent list. `_fetch_wikipedia_table()` scans all tables for a matching ticker column name; never hardcode `tables[0]`.
- **Nikkei 225 source** — Wikipedia has no constituent table with TSE codes. Uses JPX official page (`indexes.nikkei.co.jp`) with ~34 sector tables. `_fetch_url_tables()` handles multi-table format.
- **DAX 40 exchange suffixes** — Wikipedia provides correct per-ticker suffixes (Airbus = `AIR.PA`, not `AIR.DE`). Use tickers as-is; only append `.DE` if no dot present.
- **Adding new tickers always needs `--full-refresh`** — incremental fact tables miss historical rows for brand-new tickers.

---

## Security model

- **Snowflake auth:** RSA key-pair (`snowflake_private_key.pem`, gitignored). Never expires. Both `app/db/snowflake.py` and `ingestion/load.py` use `_load_private_key()`:
  - Local: `SNOWFLAKE_PRIVATE_KEY_PATH` → reads `.pem` file
  - Community Cloud: `SNOWFLAKE_PRIVATE_KEY` → PEM string in `st.secrets`
- **SQL injection:** `get_max_date()` / `get_min_date()` / `get_loaded_tickers()` use `_validate_table_name()` whitelist. Never f-string table names.
- **DB Health tab:** gated by `HEALTH_TAB_ENABLED` env var. Docker/Airflow subprocess calls only run when `IS_LOCAL=true`. Password-protectable via `HEALTH_CHECK_PASSWORD`.
- **App:** currently local only. Designed for Streamlit Community Cloud — no hardcoded credentials, all secrets via env vars / `st.secrets`.
- **gitignore:** `*.pem`, `*.p8`, `profiles.yml`, `.env`, `airflow/logs/**` — never commit these.

---

## Airflow REST API auth reference

Base URL: `http://localhost:8080/api/v1`. Credentials: `admin:admin`.

Three things must ALL be true:
1. `AIRFLOW__API__AUTH_BACKENDS` includes `basic_auth` — set in `docker-compose.yml`. Only takes effect after `docker compose up -d`, NOT `docker compose restart`.
2. Admin user exists in Airflow metadata DB.
3. Admin role has correct FAB permissions.

Quick test:
```powershell
$h = @{ Authorization = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("admin:admin")) }
Invoke-RestMethod "http://localhost:8080/api/v1/dags" -Headers $h | Select-Object total_entries
```

| Symptom | Fix |
|---|---|
| 403 on all endpoints | `docker compose up -d airflow-webserver` (recreate, not restart) |
| 403 after basic_auth confirmed | `docker compose exec airflow-webserver airflow sync-perm` |
| "No data found" from users list | Re-create admin user manually |

---

## Known issues / next steps

### Near-term

1. **db_health_check.py thresholds** — `EXPECTED_TICKERS=1500` is stale; update to ~11,000 now that backfill is complete
2. **Phase 5 — Fundamentals cohort rotation** — update `fundamentals_weekly` to use `WEEKOFYEAR(CURRENT_DATE()) % 4` filter; change load to per-cohort scoped DELETE + INSERT; run `fact_fundamentals --full-refresh` after first 4-week cycle. Reduces weekly runtime from ~5h to ~1.5h per run.
3. **GitHub branch protection** — add rule on `main`: require PRs, status checks, include administrators
4. **chart_agent.py system prompt** — add supplemental table schemas (DIVIDENDS_AND_SPLITS, EARNINGS_HISTORY, ANALYST_RECOMMENDATIONS, ANALYST_PRICE_TARGETS) to Claude system prompt for SQL generation
5. **dbt models for supplemental tables** — staging + mart models for all four supplemental RAW tables
6. **Historical valuation ratios dbt model** — compute trailing PE/P/B/P/S/yield/beta from `fact_daily_prices` × `fact_fundamentals` join using point-in-time statement dates (avoids look-ahead bias)
7. **FRED catalog retry hardening** — exponential backoff needed; current 15s single retry insufficient for sustained 429 bursts
8. **`assert_price_history_unchanged.sql` baselines** — all 15 expected_close values are `null::float`; populate from actual warehouse values to make the regression test functional

### Phase 6 — Crypto

Add cryptocurrency market data as a first-class asset class alongside equities.

- **Data source:** CoinGecko API (free tier: 30 req/min, 10K req/month) or CoinMarketCap (paid). CoinGecko preferred — no API key required for basic OHLCV.
- **Scope:** Top 200–500 coins by market cap. BTC, ETH, SOL, etc. Daily OHLCV + market cap + volume.
- **RAW tables:** `CRYPTO_PRICES` (daily OHLCV, append), `CRYPTO_METADATA` (name, symbol, category, overwrite)
- **DAG:** `coingecko_prices_daily` — `0 5 * * *` (daily, 5am UTC after equity prices). No rate-limit concerns at 500 coins vs 12K equity tickers.
- **dbt:** `stg_crypto_prices` → `fact_crypto_prices`. Grain: coin × date. Reuse `dim_date`. New `dim_crypto` dimension.
- **Ticker universe:** Crypto lives in a separate dimension table (`dim_crypto`) — do not mix into `RAW.TICKER_UNIVERSE` which is equity/ETF only.
- **App integration:** New "Crypto" tab in Streamlit. Cross-asset correlation analysis (BTC vs SPY, risk-off signals). Claude SQL agent needs `dim_crypto` + `fact_crypto_prices` added to system prompt.
- **Consideration:** 24/7 trading — no concept of "market closed". Daily close = UTC midnight snapshot. Some coins have extreme volatility; update `assert_return_bounds` thresholds or add a separate crypto-specific bounds test.

### Phase 7 — Prediction Markets

Ingest prediction market contract prices as a macro signal layer — market-implied probabilities for macro events (Fed rate decisions, election outcomes, recession probability, etc.).

- **Data sources:**
  - **Polymarket** — decentralised, REST API, no auth required. Real-money markets. Best for macro/political events.
  - **Kalshi** — regulated US exchange, REST API (requires account). Cleaner data, official categories.
  - **Metaculus** — free API, aggregated crowd forecasts (not real-money but high-volume and well-calibrated).
- **Scope (Phase 7a):** Fed funds rate outcome markets, US recession probability, CPI/inflation surprise contracts, S&P 500 direction markets. ~20–50 active contracts at any time.
- **RAW tables:** `PREDICTION_MARKET_PRICES` (contract × date × yes_price, append), `PREDICTION_MARKET_CONTRACTS` (metadata: question, category, resolution_date, overwrite)
- **DAG:** `polymarket_signals_daily` — `0 6 * * *` (after equity prices and crypto). Lightweight — only 20–50 active contracts.
- **dbt:** `stg_prediction_market_prices` → `fact_prediction_market_prices`. Grain: contract × date. `dim_prediction_contract` dimension.
- **App integration:** "Macro Signals" section in Streamlit Overview tab. Show market-implied Fed rate path alongside FRED macro series. Cross-signal: does recession probability predict equity drawdowns?
- **Consideration:** Contracts resolve and expire — need `is_resolved` flag and resolution value in `dim_prediction_contract`. Prices are 0–1 (probability); no need for the equity return bounds tests. Polymarket data can be sparse on weekends for some contracts.

---

## Git workflow

Branch off `main`, open PR, CI runs dbt tests + AI code review.

```bash
git checkout -b feature/my-feature main
git push origin feature/my-feature
gh pr create --title "..." --body "..."
```

dbt CI builds into isolated `CI_{pr_number}` Snowflake schema. Merging to main triggers prod deploy to `MARTS`.
