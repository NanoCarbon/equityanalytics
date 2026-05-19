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
│   ├── PRICES (table) — 616 tickers, daily OHLCV, incremental append
│   ├── COMPANY_INFO (table) — 616 tickers, metadata, overwrite on each run
│   ├── MACRO_INDICATORS (table) — 90 FRED series, overwrite on each run
│   ├── FINANCIAL_STATEMENTS (table) — EAV format, income/balance/cashflow, overwrite on each run
│   └── VALUATION_METRICS (table) — point-in-time ratios (PE, margins, etc.), daily append
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
- 616 tickers — full S&P 500 (scraped live from Wikipedia) + top ETFs
- 90 FRED macro series across 11 categories (5 premium series removed — see Known Issues)
- Financial statements: income statement, balance sheet, cash flow (annual + quarterly)
  - EAV format in RAW/staging, pivoted to ~35 named columns in marts
  - ~276 unique line items from yfinance (spaced names, e.g. "Total Revenue")
  - ETFs excluded from statement extraction (no 10-K filings)
- Valuation metrics: 37 fields (PE, P/B, margins, growth, dividends, beta, etc.)
  - All tickers including ETFs (beta, dividend yield still useful)
  - Daily snapshots build a time series

## Airflow DAGs
All DAGs in `airflow/dags/`. Extract + load are combined into single tasks because Airflow XCom
serializes return values to a database — DataFrames are too large to pass between tasks.

| DAG | Schedule | File | Description |
|---|---|---|---|
| `equity_daily` | `0 14 * * 1-5` | dag_equity_daily.py | Prices + company info |
| `macro_daily` | `0 14 * * 1-5` | dag_macro_daily.py | 90 FRED macro series |
| `fundamentals_weekly` | `0 15 * * 6` | dag_fundamentals.py | Financial statements (full overwrite) |
| `valuation_daily` | `0 14 * * 1-5` | dag_fundamentals.py | Valuation snapshot (daily append) |
| `backfill_prices` | `None` (manual) | dag_backfill.py | Historical prices back to 2010 |

Trigger a DAG manually from the EC2 server:
```bash
docker compose exec airflow-webserver airflow dags trigger <dag_id>
```

## Fundamentals data
- `RAW.FINANCIAL_STATEMENTS` — EAV format: ticker, statement_type, frequency, period_end_date, line_item, value
  - statement_type: income_statement, balance_sheet, cash_flow  |  frequency: annual, quarterly
  - Source: yfinance `.income_stmt`, `.balance_sheet`, `.cashflow` (+ quarterly variants)
  - Load strategy: full overwrite (catches retroactive restatements; ~580K rows for 500 equities)
  - ETFs excluded — no 10-K filings

- `RAW.VALUATION_METRICS` — wide format: ticker, snapshot_date + 37 ratio fields
  - Source: yfinance `.info`  |  Load strategy: daily append (builds PE/margin time series)
  - All tickers including ETFs

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

## Auth
- Snowflake: RSA key-pair auth — private key in `snowflake_private_key.pem` (gitignored)
  - `SNOWFLAKE_PRIVATE_KEY_PATH` env var points to the `.pem` file
  - `app/db/snowflake.py` and `ingestion/load.py` both use `_load_private_key()` helper
  - Key-pair auth never expires — no token rotation needed
- dbt: same RSA key via `profiles.yml` (`private_key_path` field) — profiles.yml is gitignored
- GitHub Actions secrets: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY, ANTHROPIC_API_KEY

## Key files
- ingestion/extract.py — yfinance extraction, S&P 500 Wikipedia scraper, bulk download
- ingestion/extract_fred.py — FRED API extraction, 90 series dict
- ingestion/extract_fundamentals.py — financial statements (EAV) + valuation metrics extraction
- ingestion/load.py — Snowflake bulk loading, get_max_date, get_min_date, SQL injection whitelist
- airflow/dags/dag_equity_daily.py — equity_daily DAG
- airflow/dags/dag_macro_daily.py — macro_daily DAG
- airflow/dags/dag_fundamentals.py — fundamentals_weekly + valuation_daily DAGs
- airflow/dags/dag_backfill.py — backfill_prices DAG
- app/db/snowflake.py — Snowflake connection + query helpers for Streamlit app
- agents/chart_agent.py — Streamlit + Claude chat app, two-step LLM pipeline
- dbt_project.yml — dbt project config, model-paths: dbt_project/models
- profiles.yml — dbt Core connection profile (gitignored)
- docker-compose.yml — Airflow Docker Compose services

## GitHub Actions behavior
- PR opened → dbt build runs in CI_{pr_number} schema, AI code review posted as comment
- Merge to main → dbt build runs with target: prod writing to MARTS directly
- PRs blocked from merging if any dbt model or test fails

## Security fixes applied (May 2026)
- SQL injection: `get_max_date`/`get_min_date` use `_validate_table_name()` whitelist
- Snowflake auth: replaced programmatic access token (SNOWFLAKE_TOKEN) with RSA key-pair
- `.gitignore`: `*.pem`, `*.p8` wildcards; added `profiles.yml`, `airflow/logs/**`
- Airflow admin password set via env var, not hardcoded

## Known issues / pending fixes (priority order)
1. **Snowflake private key not mounted in Airflow containers** — blocks ALL DAGs
   - Key lives at `~/equityanalytics/snowflake_private_key.pem` on EC2 host
   - Fix: add to `docker-compose.yml` volumes block:
     `- ./snowflake_private_key.pem:/opt/airflow/snowflake_private_key.pem:ro`
   - And set `SNOWFLAKE_PRIVATE_KEY_PATH=/opt/airflow/snowflake_private_key.pem` in `.env` on EC2

2. **FRED rate limiting (429 errors)**
   - No delay between 90 sequential API calls
   - Fix: add `time.sleep(0.5)` between calls in `extract_fred.py`

3. **FRED premium series (403 errors)**
   - SP500, NASDAQCOM, DJIA, WILL5000PR, NIKKEI225 require paid FRED subscription
   - Fix: remove these 5 series from the series dict in `extract_fred.py`

4. **GitHub branch protection**
   - Add rule on `main`: require PRs, require status checks, include administrators

## Backfill status
- Price backfill: `backfill_prices` DAG ready (manual trigger). After run:
  `dbt build --profiles-dir . --select fact_daily_prices --full-refresh`
- Fundamentals: `fundamentals_weekly` DAG handles the load. After first run:
  `dbt build --profiles-dir . --select +fact_fundamentals +fact_valuation_snapshot --full-refresh`

## Next steps
- Fix the 3 DAG blockers above (key mount → FRED rate limit → FRED premium series)
- GitHub branch protection
- Update chart_agent.py system prompt with fundamentals schema
- Market scanner + recommendation engine (new Streamlit tab, AI scoring agent)
