# Equity Analytics Pipeline — Project Context

## Stack
- Python + yfinance + Apache Airflow 2.9.3 on **local Docker** (ingestion + orchestration)
- Snowflake (warehouse) — account: QYCMQJK-HTC96121, user: DBT_USER
- dbt Core (transformation) — `profiles.yml` at repo root reads from env vars
- Streamlit + Claude API (analytics chat app on port 8501) — deployed to Streamlit Community Cloud
- Streamlit (admin dashboard on port 8502) — local only, launched via `launch_admin.bat`
- GitHub Actions (CI/CD + AI code review)

## Infrastructure
- Docker Compose runs **locally on Windows** — not on EC2
- Airflow UI: `http://localhost:8080`
- Airflow admin credentials: `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` in `.env` (currently admin/admin for local dev)
- `AIRFLOW_SECRET_KEY` in `.env` — signs Airflow sessions; keep stable or active sessions invalidate
- Docker Compose services: `airflow-db` (Postgres metadata), `airflow-webserver`, `airflow-scheduler`, `airflow-init`
- `./ingestion` is volume-mounted into all Airflow containers at `/opt/airflow/ingestion`
- Code fixes to `ingestion/` deploy instantly — no container restart needed
- Snowflake private key must also be mounted — see Known Issues

## Warehouse structure
```
EQUITY_ANALYTICS
├── RAW
│   ├── PRICES (table) — ~1,600 tickers, daily OHLCV, incremental append
│   ├── COMPANY_INFO (table) — ~1,600 tickers, metadata, overwrite on each run
│   ├── MACRO_INDICATORS (table) — 108 FRED series, incremental append
│   ├── FINANCIAL_STATEMENTS (table) — EAV format, income/balance/cashflow, overwrite on each run
│   ├── VALUATION_METRICS (table) — point-in-time ratios (PE, margins, etc.), daily append
│   ├── DIVIDENDS_AND_SPLITS (table) — full corporate action history, weekly overwrite
│   ├── EARNINGS_HISTORY (table) — EPS actuals vs. estimates, weekly overwrite
│   ├── ANALYST_RECOMMENDATIONS (table) — upgrade/downgrade history, weekly overwrite
│   ├── ANALYST_PRICE_TARGETS (table) — consensus price target snapshot, weekly append
│   ├── FRED_RELEASES (table) — FRED publication metadata, monthly overwrite
│   └── FRED_SERIES_CATALOG (table) — all FRED series metadata, monthly overwrite
├── STAGING (views)
│   ├── STG_PRICES
│   ├── STG_COMPANIES
│   ├── STG_MACRO_INDICATORS
│   ├── STG_FINANCIAL_STATEMENTS
│   └── STG_VALUATION_METRICS
├── INTERMEDIATE (views)
│   ├── INT_DAILY_RETURNS
│   └── INT_FUNDAMENTALS_PIVOTED
└── MARTS (tables)
    ├── DIM_DATE
    ├── DIM_SECURITY
    ├── FACT_DAILY_PRICES
    ├── FACT_MACRO_READINGS
    ├── FACT_FUNDAMENTALS
    └── FACT_VALUATION_SNAPSHOT
```

## Data coverage
- ~1,600 tickers — full S&P Composite 1500 (S&P 500 + S&P 400 mid-cap + S&P 600 small-cap) + top ETFs
  - All three index lists scraped live from Wikipedia on every DAG run (auto-picks up rebalances)
  - Static fallback lists built into extract.py for all three indices
  - Universe confirmed: S&P 500 ~503, S&P 400 ~400, S&P 600 ~603, ETFs ~116, total ~1,619 unique
- ~108 FRED macro series across 11 categories (5 premium series removed — require paid FRED subscription)
- Financial statements: income statement, balance sheet, cash flow (annual + quarterly)
  - EAV format in RAW/staging, pivoted to ~35 named columns in marts
  - ETFs excluded from statement extraction (no 10-K filings)
- Valuation metrics: 37 fields (PE, P/B, margins, growth, dividends, beta, etc.)
  - All ~1,600 tickers including ETFs
  - Daily snapshots build a time series
- Dividends & splits: full corporate action history back to IPO (weekly overwrite)
- Earnings history: EPS actuals vs. analyst estimates, ~8-20 quarters per equity (weekly overwrite)
- Analyst recommendations: upgrade/downgrade history from analyst firms (weekly overwrite)
- Analyst price targets: mean/high/low/count consensus snapshot (weekly append)
- FRED catalog: ~300 releases, 50K-150K series in FRED_SERIES_CATALOG (monthly overwrite)

## Airflow DAGs
All DAGs use the TaskFlow API. LocalExecutor — multiple DAGs run as concurrent subprocesses.
32 global task slots, 16 per DAG max.

All scheduled DAGs run at **11pm ET** (04:00 UTC next day). This keeps runs in the evening
when the operator can monitor them locally.

| DAG | Schedule | Cron | File | Description |
|---|---|---|---|---|
| `equity_daily` | 11pm ET Mon-Fri | `0 4 * * 2-6` | dag_equity_daily.py | Prices + company info (incremental) |
| `macro_daily` | 11pm ET Mon-Fri | `0 4 * * 2-6` | dag_macro_daily.py | 108 FRED macro series (incremental append) |
| `fundamentals_weekly` | 11pm ET Saturday | `0 4 * * 0` | dag_fundamentals.py | Financial statements (full overwrite) |
| `valuation_daily` | 11pm ET Mon-Fri | `0 4 * * 2-6` | dag_fundamentals.py | Valuation snapshot (daily append) |
| `equity_supplemental_weekly` | 11pm ET Saturday | `0 4 * * 0` | dag_equity_supplemental.py | Dividends, earnings, analyst data |
| `fred_catalog_refresh` | 11pm ET 1st of month | `0 4 2 * *` | dag_fred_catalog.py | FRED metadata catalog (monthly overwrite) |
| `backfill_prices` | None (manual) | — | dag_backfill.py | Historical OHLCV prices back to 2010 |
| `backfill_new_tickers` | None (manual) | — | dag_backfill_new_tickers.py | Backfill ONLY tickers not yet in RAW.PRICES |
| `macro_backfill` | None (manual) | — | dag_macro_backfill.py | Full FRED history (back to series start) |

Trigger a DAG manually (run from the repo root on Windows):
```powershell
docker compose exec airflow-webserver airflow dags trigger <dag_id>
```

## Admin dashboard

A separate local-only Streamlit app (`admin_dashboard/app.py`) runs on port 8502. It is NOT the public analytics app.

**Launch:** double-click `launch_admin.bat` or pin it to Start via `pin_to_start_menu.ps1`.

`launch_admin.bat` does four things in sequence:
1. `docker compose up -d` — ensures containers are running
2. Polls `http://localhost:8080/health` (24 × 5s) — waits for Airflow webserver
3. Runs `scripts/db_health_check.py` — prints structured health summary
4. `streamlit run admin_dashboard/app.py --server.port 8502` — opens the dashboard

**Admin dashboard components:**
- `admin_dashboard/airflow_client.py` — thin Airflow REST API v1 wrapper; reads `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` from env
- `admin_dashboard/health_check.py` — structured health checks (same logic as `scripts/db_health_check.py` but returns dicts)
- `admin_dashboard/styles/winforms.css` — Windows 98 Win Forms aesthetic (gray `#d4d0c8` background, inset borders, raised buttons)
- `admin_dashboard/components/status_bar.py` — Docker / Airflow / Snowflake traffic-light status dots
- `admin_dashboard/components/dag_monitor.py` — DAG list, last run state, pause/unpause toggle, trigger with confirmation dialog
- `admin_dashboard/components/log_viewer.py` — reads Airflow logs from filesystem (handles Windows Unicode colon U+F03A in path names)
- `admin_dashboard/components/data_quality.py` — color-coded pass/warn/fail health check table

**Airflow REST API — auth reference (read this before debugging 403s):**

The REST API base URL is `http://localhost:8080/api/v1`. Credentials are `admin:admin` (set in `.env`).

Three things must ALL be true for the API to work:
1. `AIRFLOW__API__AUTH_BACKENDS` includes `basic_auth` — set in `docker-compose.yml` under `x-airflow-common` environment block. **This env var only takes effect after `docker compose up -d` (full recreate), NOT after `docker compose restart`.**
2. The admin user exists in the Airflow metadata DB.
3. The admin role has the correct FAB permissions.

**Quick auth test (PowerShell):**
```powershell
$h = @{ Authorization = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("admin:admin")) }
Invoke-RestMethod "http://localhost:8080/api/v1/dags" -Headers $h | Select-Object total_entries
```
Expected: `total_entries: 9`. Any other result → see troubleshooting below.

**Troubleshooting by symptom:**

| Symptom | Cause | Fix |
|---|---|---|
| Connection refused / closed unexpectedly | Webserver not up yet or stale PID file | Wait 30s; if still failing: `docker compose exec airflow-webserver rm -f /opt/airflow/airflow-webserver.pid` then `docker compose up -d airflow-webserver` |
| 403 on ALL endpoints incl. `/dags` | `basic_auth` backend not active | `docker compose up -d airflow-webserver` (must recreate, not just restart) |
| 403 after confirming basic_auth is set | Admin user missing or FAB perms stale | Run both: `docker compose exec airflow-webserver airflow users list` to confirm user exists, then `docker compose exec airflow-webserver airflow sync-perm` |
| 200 on `/version` but 403 on `/dags` | Same as above — `/version` is public, `/dags` requires auth | Same fix as above |
| "No data found" from `airflow users list` | DB wiped (volume reset) — user was never created | `docker compose exec airflow-webserver airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin` |
| Auth worked before, now broken after webserver restart | `docker compose restart` doesn't reload env vars | Always use `docker compose up -d <service>` to pick up env var changes |

**Trigger a DAG (no REST API needed — use CLI directly):**
```powershell
docker compose exec airflow-webserver airflow dags trigger <dag_id>
```

**Check task states for a run:**
```powershell
docker compose exec airflow-webserver airflow tasks states-for-dag-run <dag_id> <run_id>
```

## Key ingestion behaviors

**macro_daily** — incremental append, NOT overwrite:
- Queries `MAX(date)` already in `RAW.MACRO_INDICATORS`
- Fetches FRED data from `(max_date - 7 days)` forward to catch recent FRED revisions
- Falls back to 30-day lookback if table is empty
- Use `macro_backfill` (manual trigger) to restore full history or pull latest revisions across all time

**equity_daily** — incremental prices, overwrite metadata:
- `RAW.PRICES`: checks `MAX(price_date)` and only downloads newer trading days
- `RAW.COMPANY_INFO`: full overwrite every run (metadata always current)

**fred_catalog_refresh** — full crawl, all-or-nothing:
- Crawls all ~300 FRED releases at 0.5s/request rate limit (~10-20 min total)
- Do NOT run simultaneously with macro_backfill — both hit FRED API and cause 429 rate limits
- No checkpointing: if it fails mid-crawl it restarts from release 1
- Stores result in memory; overwrites both tables atomically at the end

## Fundamentals data
- `RAW.FINANCIAL_STATEMENTS` — EAV format: ticker, statement_type, frequency, period_end_date, line_item, value
  - statement_type: income_statement, balance_sheet, cash_flow  |  frequency: annual, quarterly
  - Source: yfinance `.income_stmt`, `.balance_sheet`, `.cashflow` (+ quarterly variants)
  - Load strategy: full overwrite (catches retroactive restatements; ~580K rows for 500 equities)
  - ETFs excluded — no 10-K filings

- `RAW.VALUATION_METRICS` — wide format: ticker, snapshot_date + 37 ratio fields
  - Source: yfinance `.info`  |  Load strategy: daily append (builds PE/margin time series)
  - All tickers including ETFs
  - NOTE: yfinance .info is current-only — cannot be used to backfill historical ratios
  - Historical ratio backfill requires computing from prices x financial statements (on roadmap)

- dbt models added: `stg_financial_statements`, `stg_valuation_metrics`, `int_fundamentals_pivoted`,
  `fact_fundamentals` (grain: ticker + period_end_date + frequency),
  `fact_valuation_snapshot` (grain: ticker + snapshot_date)
- 3 new singular tests: no future fundamentals, no negative revenue, no negative assets

## dbt models
- 13 models total across staging, intermediate, and marts layers
- Schema routing centralized in `dbt_project.yml` via `+schema:` — do NOT add
  `{{ config(schema='...') }}` to individual model files
- Incremental materialization on all four fact tables
- unique_key=['ticker', 'price_date'] on fact_daily_prices
- unique_key=['ticker', 'period_end_date', 'frequency'] on fact_fundamentals
- unique_key=['ticker', 'snapshot_date'] on fact_valuation_snapshot

## Running dbt locally
dbt Core does not auto-load `.env` — export env vars first:

```powershell
# Windows PowerShell
Get-Content .env | Where-Object { $_ -notmatch '^\s*#' -and $_ -match '=' } | ForEach-Object {
    $k, $v = $_ -split '=', 2; Set-Item "env:$($k.Trim())" $v.Trim()
}
dbt debug --profiles-dir .
dbt build --profiles-dir .
```

```bash
# Linux/Mac
export $(grep -v '^#' .env | xargs)
dbt build --profiles-dir .
```

## Key operational patterns
- Adding new tickers requires `dbt build --select fact_daily_prices --full-refresh`
  because the incremental filter (price_date > max existing) excludes historical rows for new tickers
- Same pattern applies to fact_fundamentals and fact_valuation_snapshot
- Snowflake DATE column stored as nanosecond Unix timestamps (NUMBER(38,0))
  Conversion: to_date(dateadd(second, date / 1000000000, '1970-01-01'))
- Financial statements use full overwrite (yfinance returns fixed ~4yr/8Q window, catches restatements)
- Valuation metrics append daily to build time series
- ETFs filtered out for financial statement extraction using get_etf_tickers() exclusion list
- yfinance line item names use spaces (e.g. "Total Revenue", "Net Income") — not CamelCase
- Derived margins computed in a separate CTE in int_fundamentals_pivoted to avoid
  Snowflake GROUP BY + CASE nesting issues

## FRED API notes
- Valid `order_by` values for `/fred/releases` endpoint: release_id, name, press_release, realtime_start, realtime_end
  - `popularity` is NOT valid for /releases — only valid on /release/series
  - Bug was silently returning 0 results (HTTP 400 logged at DEBUG level, returned {})
- Rate limit: 429 errors get one 15s retry; if catalog + backfill run simultaneously they both stall
- Do not run fred_catalog_refresh and macro_backfill at the same time

## Auth
- Snowflake: RSA key-pair auth — private key in `snowflake_private_key.pem` (gitignored)
  - `SNOWFLAKE_PRIVATE_KEY_PATH` env var points to the `.pem` file
  - `app/db/snowflake.py` and `ingestion/load.py` both use `_load_private_key()` helper
  - Key-pair auth never expires — no token rotation needed
- dbt: same RSA key via `profiles.yml` (`private_key_path` field) — profiles.yml is gitignored
- Airflow: credentials and secret key in `.env` — see "Airflow REST API — auth reference" in the Admin dashboard section above for full troubleshooting
  - `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` (currently admin/admin) — used at init AND by the admin dashboard REST API client
  - `AIRFLOW_SECRET_KEY` — signs sessions; generated with `python -c "import secrets; print(secrets.token_hex(32))"`
  - `AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth,...'` — set in `docker-compose.yml` (not `.env`); required for REST API basic auth to work; only takes effect after `docker compose up -d`, NOT `docker compose restart`
- GitHub Actions secrets: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY, ANTHROPIC_API_KEY

## Key files
- launch_admin.bat — local admin launcher: docker up → Airflow health check → db_health_check → Streamlit on port 8502
- pin_to_start_menu.ps1 — creates a Start Menu shortcut for launch_admin.bat (run once elevated)
- admin_dashboard/app.py — admin Streamlit app entry point (port 8502, Win Forms aesthetic)
- admin_dashboard/airflow_client.py — Airflow REST API v1 wrapper
- admin_dashboard/health_check.py — structured health checks returning dicts
- admin_dashboard/styles/winforms.css — Windows 98 Win Forms CSS
- admin_dashboard/components/status_bar.py — system status dots (Docker/Airflow/Snowflake)
- admin_dashboard/components/dag_monitor.py — DAG list, trigger with confirmation, pause/unpause
- admin_dashboard/components/log_viewer.py — Airflow log filesystem reader (handles Unicode colon U+F03A)
- admin_dashboard/components/data_quality.py — health check results table
- ingestion/extract.py — yfinance: prices, company info, dividends, earnings, analyst data
  - get_sp500_tickers() / get_sp400_tickers() / get_sp600_tickers() — live Wikipedia scrapers
  - get_all_tickers() — full universe (S&P 1500 + ETFs, ~1,619)
  - get_equity_tickers() — S&P 1500 equities only, no ETFs (~1,500)
- ingestion/extract_fred.py — FRED API: ~108 series dict, rate-limited extraction
- ingestion/extract_fred_catalog.py — FRED releases + series catalog crawler
  - get_all_releases() uses order_by="release_id" (not "popularity" — invalid for /releases)
- ingestion/extract_fundamentals.py — financial statements (EAV) + valuation metrics extraction
- ingestion/load.py — Snowflake bulk loading, get_max_date, get_min_date, get_loaded_tickers, SQL injection whitelist
  - get_loaded_tickers(table_name) — returns set of distinct tickers already in a table
- airflow/dags/dag_equity_daily.py — equity_daily DAG (prices + company info)
- airflow/dags/dag_macro_daily.py — macro_daily DAG (incremental FRED append)
- airflow/dags/dag_fundamentals.py — fundamentals_weekly + valuation_daily DAGs
- airflow/dags/dag_equity_supplemental.py — equity_supplemental_weekly DAG (dividends, earnings, analyst)
- airflow/dags/dag_fred_catalog.py — fred_catalog_refresh DAG (monthly FRED metadata crawl)
- airflow/dags/dag_backfill.py — backfill_prices DAG (manual, OHLCV history to 2010)
- airflow/dags/dag_backfill_new_tickers.py — backfill new-only tickers (diffs against RAW.PRICES)
- airflow/dags/dag_macro_backfill.py — macro_backfill DAG (manual, full FRED history)
- scripts/db_health_check.py — reusable DB health check (run anytime, exits 0=pass/1=fail)
  - Thresholds: EXPECTED_TICKERS=1500, MIN_TICKERS_PER_DAY=1400, MIN_FUNDAMENTAL_TICKERS=1200
  - MACRO_RAW_MART_RATIO=0.35 (dbt staging deduplicates/filters, ~41% propagation is correct)
  - All print statements use pure ASCII (Windows cp1252 compatibility)
- app/db/snowflake.py — Snowflake connection + query helpers for Streamlit app
- agents/chart_agent.py — Streamlit + Claude chat app, two-step LLM pipeline
- dbt_project.yml — dbt project config, model-paths: dbt_project/models
- profiles.yml — dbt Core connection profile (gitignored)
- docker-compose.yml — Airflow Docker Compose services

## GitHub Actions behavior
- PR opened -> dbt build runs in CI_{pr_number} schema, AI code review posted as comment
- Merge to main -> dbt build runs with target: prod writing to MARTS directly
- PRs blocked from merging if any dbt model or test fails

## Bugs fixed (May 2026)

### equity_daily — `TypeError: combine() argument 1 must be datetime.date, not str`
- **Root cause:** Airflow XCom serializes task return values as JSON. `get_max_date()` returns a Python `date`, which XCom round-trips as the string `'2026-05-19'`. `extract_prices()` passed it directly to `datetime.combine()` which requires a `date` object.
- **Fix:** `ingestion/extract.py` — added `if isinstance(start_date, str): start_date = date.fromisoformat(start_date)` before the `datetime.combine()` call.
- **Impact:** This bug silently broke every nightly `equity_daily` run after the S&P 1500 expansion. No new prices were loading.

### valuation_daily — `ArrowInvalid: Could not convert 'Infinity' with type str to double`
- **Root cause:** yfinance returns the string literal `'Infinity'` (not `float('inf')`) for PE ratios on tickers with negative earnings. The sentinel filter only checked `isinstance(value, (int, float))`, so the string slipped through and poisoned PyArrow schema on Snowflake load.
- **Fix:** `ingestion/extract_fundamentals.py` — added `import math`; now coerces strings through `float()` first, then applies `math.isfinite()` + `abs(value) > 1e18` guard.
- **Impact:** `valuation_daily` ran 7 hours on ~1,400 tickers then failed on attempt 3. Fix prevents future recurrence.

### equity_daily — `ValueError: Length mismatch: Expected axis has 8 elements, new values have 7 elements`
- **Root cause:** Newer yfinance releases with `auto_adjust=True` emit extra columns (`Dividends`, `Capital Gains`) in the bulk download. The previous `df.columns = [7 fixed names]` positional rename crashed when the DataFrame had 8 columns.
- **Fix:** `ingestion/extract.py` — replaced positional rename with a name-mapped rename (`rename_map` dict keyed on actual column names), then `df = df[["date", "ticker", "close", "high", "low", "open", "volume"]]` to select only OHLCV. Future extra columns are silently ignored.
- **Impact:** Blocked the manual re-run on 2026-05-21 after the XCom fix. Confirmed fixed — prices loaded successfully in the next run.

### Rate limiting — silent all-NaN returns from Yahoo Finance
- **Root cause:** `equity_daily` and `valuation_daily` both fire at 11pm ET and both call `yf.Ticker().info` for ~1,619 tickers at 2s/ticker = 54 min of concurrent load. Yahoo Finance silently returns all-NaN DataFrames under sustained load (no error raised).
- **Fix:** Added batch pauses to all six affected extraction functions:
  - `extract_company_info`, `extract_valuation_metrics`: pause 30s every 100 tickers (`.info` endpoint)
  - `extract_dividends_and_splits`, `extract_earnings_history`, `extract_analyst_recommendations`, `extract_analyst_price_targets`: pause 15s every 150 tickers
  - `extract_financial_statements`: already had batch pauses

## Security fixes applied (May 2026)
- SQL injection: `get_max_date`/`get_min_date` use `_validate_table_name()` whitelist
- Snowflake auth: replaced programmatic access token (SNOWFLAKE_TOKEN) with RSA key-pair
- `.gitignore`: `*.pem`, `*.p8` wildcards; added `profiles.yml`, `airflow/logs/**`
- Airflow admin credentials and secret key now configured via `.env` env vars

## Known issues / pending work (priority order)
1. **GitHub branch protection** — Add rule on `main`: require PRs, require status checks, include administrators
2. **dbt full-refresh pending** — `fact_daily_prices`, `fact_fundamentals`, `fact_valuation_snapshot` need full-refresh after the price and valuation bug fixes to backfill the days that were missed:
   ```powershell
   dbt build --profiles-dir . --select fact_daily_prices fact_macro_readings +fact_fundamentals fact_valuation_snapshot --full-refresh
   ```
3. **valuation_daily re-trigger** — scheduled run for 2026-05-20 picked up automatically and is running (task: `extract_and_load_valuations`). Confirm it completes, then trigger for today's date if missed.
4. **FRED catalog retry hardening** — single 15s sleep + one retry is insufficient for sustained 429 bursts; needs exponential backoff. Workaround: run catalog alone, never simultaneously with macro_backfill.
5. **macro_backfill pending** — MACRO_INDICATORS was overwritten by a macro_daily bug (now fixed). Need to re-trigger macro_backfill to restore full history, then: `dbt build --profiles-dir . --select fact_macro_readings --full-refresh`
6. **Update chart_agent.py system prompt** with supplemental table schemas (DIVIDENDS_AND_SPLITS, EARNINGS_HISTORY, ANALYST_RECOMMENDATIONS, ANALYST_PRICE_TARGETS)

## Backfill status (as of May 2026)
- Price backfill: COMPLETE — RAW.PRICES has ~9.2M rows, 2010 to present, ~1,619 tickers
  - Used dag_backfill_new_tickers.py to backfill only the 1,003 new S&P 400/600 tickers
- FRED macro backfill: PENDING — macro_daily overwrote history with a bug (now fixed).
  Re-trigger macro_backfill DAG, then full-refresh fact_macro_readings.
- Fundamentals: equity_daily completed for all ~1,619 tickers (COMPANY_INFO confirmed ~1,600 rows)
- FRED catalog: confirm row counts — RAW.FRED_RELEASES should be ~300, RAW.FRED_SERIES_CATALOG ~50K+

## Next steps
1. Confirm equity_daily (manual__2026-05-21T04:06:42) fully succeeds (extract_and_load_company_info running)
2. Confirm valuation_daily (scheduled__2026-05-20) fully succeeds
3. dbt full-refresh (see item 2 in Known Issues above)
4. Trigger macro_backfill to restore FRED history (do NOT run simultaneously with fred_catalog_refresh)
5. GitHub branch protection rules on `main`
6. Update chart_agent.py system prompt with supplemental table schemas
7. Historical valuation ratios dbt model (prices x fundamentals, avoids look-ahead bias)
