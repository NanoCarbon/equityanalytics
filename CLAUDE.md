# CLAUDE.md — Equity Analytics Pipeline

This file is read by Claude Code at the start of every session. Keep it current.

---

## What this project is

A production-style ELT pipeline and analytics application:
- **Ingestion:** Python + yfinance + FRED API, orchestrated by Apache Airflow on local Docker
- **Warehouse:** Snowflake (`EQUITY_ANALYTICS` DB) with three schemas: RAW → STAGING → MARTS
- **Transformation:** dbt Core (Kimball dimensional model)
- **Application:** Streamlit + Claude API (natural language → SQL → Plotly chart)
- **CI/CD:** GitHub Actions (dbt tests + AI code review on every PR)

---

## Running things

### dbt (run from repo root — `profiles.yml` is here)

dbt does NOT auto-read `.env`. Load env vars first every time:

```powershell
# Windows PowerShell
Get-Content .env | ForEach-Object {
    if ($_ -match '^([^#][^=]*)=(.*)$') {
        [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), 'Process')
    }
}
```

Then:
```powershell
dbt build --profiles-dir .                                              # all models + tests
dbt build --profiles-dir . --select fact_macro_readings --full-refresh  # single model full-refresh
dbt build --profiles-dir . --select +fact_fundamentals --full-refresh   # model + upstream
dbt test --profiles-dir .                                               # tests only
dbt debug --profiles-dir .                                              # verify connection
```

dbt binary path (Windows Store Python): `C:\Users\edwar\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\Scripts\dbt.exe`

**dbt is NOT installed inside the Airflow Docker container.** Always run dbt locally.

### Airflow (Docker Compose, local)

```powershell
docker compose up -d airflow-webserver airflow-scheduler   # start
docker compose down                                         # stop (keeps data)
docker compose ps                                           # check status
```

Airflow UI: `http://localhost:8080` (admin / admin for local dev)

Trigger a DAG manually:
```powershell
docker compose exec airflow-webserver airflow dags trigger <dag_id>
```

REST API (alternative):
```powershell
curl -s -X POST "http://localhost:8080/api/v1/dags/<dag_id>/dagRuns" `
  -H "Content-Type: application/json" -u "admin:admin" -d '{}'
```

### App

```powershell
streamlit run app/streamlit_app.py --server.port 8501
```

Or double-click `equity_analytics.bat` — it does: docker up → wait for Airflow → db_health_check → Streamlit.

---

## Architecture constraints

### Data flow — never skip layers
```
yfinance / FRED API  →  RAW (append/overwrite)  →  dbt STAGING  →  dbt MARTS
```
RAW is append-only source of truth. Never write directly to STAGING or MARTS from Python.

### Incremental vs. full-refresh rules
| Table | Strategy | Why |
|---|---|---|
| RAW.PRICES | Incremental append (by date) | Too large to reload daily |
| RAW.MACRO_INDICATORS | Incremental append (7-day overlap) | Preserves FRED backfill |
| RAW.COMPANY_INFO | Full overwrite | Small, always-current |
| RAW.FINANCIAL_STATEMENTS | Full overwrite | Catches restatements |
| RAW.VALUATION_METRICS | Daily append | Builds point-in-time time series |
| All other RAW supplemental | Weekly overwrite | Small, corrections needed |

### Adding new FRED series
1. Add to `FRED_SERIES` dict in `ingestion/extract_fred.py`
2. Add to `FRED_CATEGORIES` dict in `agents/prompts.py`
3. Trigger `fred_new_series_backfill` DAG (only fetches NEW series — safe to run anytime)
4. `dbt build --profiles-dir . --select fact_macro_readings --full-refresh`

Do NOT trigger `macro_backfill` (full overwrite of ALL series) unless intentionally refreshing all FRED history.

### Adding new equity tickers
1. Tickers from S&P 1500 are auto-detected via Wikipedia scraping on every run
2. For manual additions: trigger `equity_daily`, then `dbt build --select fact_daily_prices --full-refresh`
3. Same for fundamentals: trigger `fundamentals_weekly`, then `dbt build --select +fact_fundamentals --full-refresh`

---

## Key files

| File | Purpose |
|---|---|
| `ingestion/extract_fred.py` | `FRED_SERIES` dict (175 series), `extract_all_fred_series()`, `extract_fred_series()` |
| `ingestion/extract.py` | yfinance: prices, company info, dividends, earnings, analyst data |
| `ingestion/extract_fundamentals.py` | Financial statements (EAV) + valuation metrics |
| `ingestion/load.py` | `load_dataframe()`, `get_connection()`, `get_max_date()`, `get_loaded_tickers()` |
| `ingestion/extract_fred_catalog.py` | FRED release crawler (monthly) |
| `airflow/dags/dag_macro_daily.py` | `macro_daily` DAG — daily incremental FRED append |
| `airflow/dags/dag_fred_new_series_backfill.py` | `fred_new_series_backfill` — new series only, safe to re-run |
| `airflow/dags/dag_macro_backfill.py` | `macro_backfill` — full overwrite of all series (paused; manual only) |
| `app/streamlit_app.py` | Main app entry point (4 tabs: Overview, AI Analytics, Event Study, DB Health) |
| `app/components/db_health.py` | DB Health tab — Snowflake checks, thresholds, security gate |
| `app/components/overview.py` | Overview tab — stack, data coverage, example prompts |
| `app/components/chat.py` | AI Analytics tab — Claude SQL generation + Plotly |
| `app/db/snowflake.py` | `execute_sql_cached()`, `_load_private_key()` (supports both file path and PEM string) |
| `agents/prompts.py` | `SYSTEM_PROMPT` (schema context for SQL generation), `FRED_CATEGORIES` |
| `dbt_project/models/` | staging / intermediate / marts dbt models |
| `scripts/db_health_check.py` | Standalone health check — run anytime, exits 0/1 |
| `scripts/create_fred_hygiene_view.py` | One-shot: creates `VW_FRED_HYGIENE` in Snowflake RAW |
| `profiles.yml` | dbt connection profile (gitignored; reads from env vars) |
| `docker-compose.yml` | Airflow Docker Compose (4 services: db, webserver, scheduler, init) |
| `equity_analytics.bat` | App launcher for Windows |

---

## Snowflake schema reference

```
EQUITY_ANALYTICS
├── RAW
│   ├── PRICES                  -- ~1,600 tickers, daily OHLCV, incremental append
│   ├── COMPANY_INFO            -- metadata, overwrite each run
│   ├── MACRO_INDICATORS        -- 175 FRED series, incremental append
│   ├── FINANCIAL_STATEMENTS    -- EAV format, weekly overwrite
│   ├── VALUATION_METRICS       -- 37 ratio fields, daily append
│   ├── DIVIDENDS_AND_SPLITS    -- full history, weekly overwrite
│   ├── EARNINGS_HISTORY        -- EPS actuals vs. estimates, weekly overwrite
│   ├── ANALYST_RECOMMENDATIONS -- upgrade/downgrade history, weekly overwrite
│   ├── ANALYST_PRICE_TARGETS   -- consensus snapshot, weekly append
│   ├── FRED_RELEASES           -- FRED publication metadata, monthly overwrite
│   ├── FRED_SERIES_CATALOG     -- all FRED series metadata (~50-150K rows), monthly overwrite
│   └── VW_FRED_HYGIENE         -- view: latest/prev obs date + row count per series (duplicate detector)
├── STAGING (views)             -- stg_prices, stg_companies, stg_macro_indicators, stg_financial_statements, stg_valuation_metrics
├── INTERMEDIATE (views)        -- int_daily_returns, int_fundamentals_pivoted
└── MARTS (tables)              -- dim_date, dim_security, fact_daily_prices, fact_macro_readings, fact_fundamentals, fact_valuation_snapshot
```

Key Snowflake quirk: DATE columns in RAW are stored as nanosecond Unix epoch (NUMBER(38,0)).
Convert with: `TO_DATE(DATEADD(second, date / 1000000000, '1970-01-01'))`

---

## DAGs

| DAG | Schedule | Description |
|---|---|---|
| `equity_daily` | `0 4 * * 2-6` (11pm ET Mon-Fri) | Prices + company info (incremental) |
| `macro_daily` | `0 4 * * 2-6` (11pm ET Mon-Fri) | 175 FRED series (incremental append, 7-day overlap) |
| `valuation_daily` | `0 4 * * 2-6` (11pm ET Mon-Fri) | Valuation snapshot (daily append) |
| `fundamentals_weekly` | `0 4 * * 0` (11pm ET Saturday) | Financial statements (full overwrite) |
| `equity_supplemental_weekly` | `0 4 * * 0` (11pm ET Saturday) | Dividends, earnings, analyst data |
| `fred_catalog_refresh` | `0 4 2 * *` (11pm ET, 1st of month) | FRED metadata catalog (monthly overwrite) |
| `backfill_prices` | None (manual) | Historical OHLCV back to 2010 |
| `backfill_new_tickers` | None (manual) | Backfill ONLY tickers not yet in RAW.PRICES |
| `macro_backfill` | None (manual, **paused**) | Full FRED history, all series — full overwrite |
| `fred_new_series_backfill` | None (manual) | Full FRED history, NEW series only — append |

`macro_daily` automatically picks up any series added to `FRED_SERIES` — no DAG changes needed.

---

## Common pitfalls

- **dbt without env vars** — will error `Env var required but not provided: 'SNOWFLAKE_ACCOUNT'`. Always load `.env` first.
- **`macro_backfill` vs `fred_new_series_backfill`** — `macro_backfill` overwrites everything. Always prefer `fred_new_series_backfill` when adding new series. `macro_backfill` is paused intentionally.
- **`get_connection()` not `get_snowflake_connection()`** — the function in `ingestion/load.py` is `get_connection()`.
- **dbt in Docker** — dbt is not installed in Airflow containers. Run locally.
- **`docker compose restart` vs `docker compose up -d`** — `restart` does NOT reload env vars from `docker-compose.yml`. Always use `up -d` after env var changes.
- **FRED catalog + backfill simultaneously** — both hit FRED API and cause 429 bursts. Run them separately.
- **`order_by` for FRED `/releases`** — `popularity` is invalid; use `release_id`. Only `/release/series` accepts `popularity`.
- **ETFs in financial statements** — `extract_fundamentals.py` filters out ETFs using `get_etf_tickers()`. Don't remove this filter.
- **yfinance `'Infinity'` string** — PE ratios for negative-earnings tickers return the string `'Infinity'`. The load code coerces through `float()` + `math.isfinite()`. Keep this guard.
- **Adding new series: update both files** — `extract_fred.py` (FRED_SERIES dict) AND `agents/prompts.py` (FRED_CATEGORIES). The prompts file provides category context to Claude for SQL generation.

---

## Security model

- **Snowflake auth:** RSA key-pair (`snowflake_private_key.pem`, gitignored). Never expires. Both `app/db/snowflake.py` and `ingestion/load.py` use `_load_private_key()` which supports:
  - Local: `SNOWFLAKE_PRIVATE_KEY_PATH` env var → reads `.pem` file
  - Community Cloud: `SNOWFLAKE_PRIVATE_KEY` env var → PEM string in `st.secrets`
- **SQL injection:** `get_max_date()` / `get_min_date()` use `_validate_table_name()` whitelist. Never f-string table names directly.
- **DB Health tab:** gated by `HEALTH_TAB_ENABLED` env var. Docker/Airflow subprocess calls only run when `IS_LOCAL=true`. Password-protectable via `HEALTH_CHECK_PASSWORD`.
- **App:** currently local only. Designed for Streamlit Community Cloud — no hardcoded credentials, all secrets via env vars / `st.secrets`.
- **gitignore:** `*.pem`, `*.p8`, `profiles.yml`, `.env`, `airflow/logs/**` — never commit these.

---

## Git workflow

Branch off `main`, open PR, CI runs dbt tests + AI code review.
Current active branch: `fred-series-expansion` (pushed, PR not yet open).

```powershell
git checkout -b feature/my-feature main
git push origin feature/my-feature
gh pr create --title "..." --body "..."
```

dbt CI builds into isolated `CI_{pr_number}` Snowflake schema. Merging to main triggers prod deploy to `MARTS`.
