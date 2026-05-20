# Equity Analytics Pipeline — Project Context

## Stack
- Python + yfinance + Apache Airflow 2.9.3 on AWS EC2 (ingestion + orchestration)
- Snowflake (warehouse) — account: QYCMQJK-HTC96121, user: DBT_USER
- dbt Core (transformation) — `profiles.yml` at repo root reads from env vars
- Streamlit + Claude API (chat app) — deployed to Streamlit Community Cloud
- GitHub Actions (CI/CD + AI code review)

## Infrastructure
- EC2: t3.small, us-east-2, Amazon Linux 2023 — **stop when not actively debugging to save cost**
- Airflow UI: `http://<ec2-public-ip>:8080`
- Security group: port 22 (SSH) + port 8080 (Airflow UI) restricted to known IP
- Docker Compose: `airflow-db` (Postgres metadata), `airflow-webserver`, `airflow-scheduler`
- `./ingestion` is volume-mounted into all Airflow containers at `/opt/airflow/ingestion`
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

Trigger a DAG manually from the EC2 server:
```bash
docker compose exec airflow-webserver airflow dags trigger <dag_id>
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
- GitHub Actions secrets: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY, ANTHROPIC_API_KEY

## Key files
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

## Security fixes applied (May 2026)
- SQL injection: `get_max_date`/`get_min_date` use `_validate_table_name()` whitelist
- Snowflake auth: replaced programmatic access token (SNOWFLAKE_TOKEN) with RSA key-pair
- `.gitignore`: `*.pem`, `*.p8` wildcards; added `profiles.yml`, `airflow/logs/**`
- Airflow admin password set via env var, not hardcoded

## Known issues / pending work (priority order)
1. **GitHub branch protection** — Add rule on `main`: require PRs, require status checks, include administrators
2. **FRED catalog retry hardening** — single 15s sleep + one retry is insufficient for sustained 429 bursts;
   needs exponential backoff. Workaround: run catalog alone, never simultaneously with macro_backfill.
3. **macro_backfill pending** — MACRO_INDICATORS was overwritten by macro_daily bug (now fixed).
   Need to re-trigger macro_backfill to restore full history, then:
   `dbt build --profiles-dir . --select fact_macro_readings --full-refresh`
4. **FRED catalog pending confirmation** — fred_catalog_refresh was running as of session end (release ~190/324).
   Verify it completed: check RAW.FRED_RELEASES row count should be ~300, RAW.FRED_SERIES_CATALOG ~50K+.

## Backfill status (as of May 2026)
- Price backfill: COMPLETE — RAW.PRICES has ~9.2M rows, 2010 to present, ~1,619 tickers
  - Used dag_backfill_new_tickers.py to backfill only the 1,003 new S&P 400/600 tickers
- FRED macro backfill: PENDING — macro_daily overwrote history with a bug (now fixed).
  Re-trigger macro_backfill DAG, then full-refresh fact_macro_readings.
- Fundamentals: equity_daily completed for all ~1,619 tickers (COMPANY_INFO confirmed ~1,600 rows)
- FRED catalog: ran but may still be in progress — verify row counts before marking complete.

## Next steps
1. Confirm FRED catalog completed (check Airflow UI + row counts)
2. Trigger macro_backfill to restore FRED history (do NOT run simultaneously with catalog)
3. After macro_backfill: `dbt build --profiles-dir . --select fact_macro_readings --full-refresh`
4. GitHub branch protection rules
5. Update chart_agent.py system prompt with supplemental table schemas
6. Historical valuation ratios dbt model (prices x fundamentals, avoids look-ahead bias)
