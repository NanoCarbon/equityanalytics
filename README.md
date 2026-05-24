# Equity Analytics Pipeline

A production-style ELT pipeline and AI-powered analytics application built as a portfolio project for data engineering roles in financial services. Ingests the full S&P Composite 1500 universe plus top ETFs, 203 Federal Reserve macro indicators, complete fundamental financial data (income statements, balance sheets, cash flow, and valuation metrics), supplemental equity data (dividends, earnings history, analyst ratings, price targets), and a full FRED series catalog — models them into a Kimball dimensional warehouse, and exposes the data through a natural language chat interface that generates SQL and interactive charts on demand.

## Live Demo

[equity-analytics.streamlit.app](https://equity-analytics.streamlit.app)

---

## Architecture

```
S&P 1500 + ETF prices         FRED macro indicators       Financial statements
     (yfinance)                    (FRED API)               + valuation metrics
          |                             |                       (yfinance)
  Python ingestion            Python ingestion             Python ingestion
  Airflow orchestrated        Airflow orchestrated         Airflow orchestrated
          |                             |                            |
       Snowflake RAW schema (append-only / overwrite landing zone)
                        |
            dbt transformations
       staging -> intermediate -> marts
                        |
          Snowflake MARTS schema
       Kimball dimensional model
                        |
       Streamlit + Claude API
   natural language -> SQL -> chart
```

---

## Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python + yfinance | S&P 1500 + ETF prices, company metadata, financial statements, valuation metrics, dividends, earnings, analyst data |
| Ingestion | Python + FRED API | 203 FRED macro series (waves 1–6 complete) + full FRED series catalog |
| Orchestration | Apache Airflow 2.9.3 (Docker Compose, local) | Scheduling, retries, observability |
| Warehouse | Snowflake | Three-schema ELT architecture |
| Transformation | dbt Core | Kimball dimensional modeling |
| Quality | dbt tests + GitHub Actions | 25+ automated tests on every PR |
| AI Code Review | Claude API + GitHub Actions | Automated PR code review comments |
| Application | Streamlit + Claude API | Natural language analytics interface |

---

## Data Coverage

### Equities
- **~1,600 tickers** — full S&P Composite 1500 (S&P 500 + S&P 400 mid-cap + S&P 600 small-cap) + top 100 ETFs by AUM and liquidity
- All three index lists are fetched live from Wikipedia on each DAG run — index rebalances are picked up automatically
- Daily OHLCV prices with incremental loads — only new trading days extracted each run
- Company metadata: sector, industry, market cap, exchange

### Fundamentals
- **Financial statements** — income statement, balance sheet, cash flow for ~1,500 S&P 1500 equities (ETFs excluded)
  - ~4 years annual + ~8 quarters per ticker from yfinance
  - EAV format in RAW/staging, pivoted to ~35 named columns in marts
  - Full overwrite weekly to catch retroactive restatements
- **Valuation metrics** — 37 point-in-time fields per ticker per day (PE, P/B, EV/EBITDA, margins, growth rates, dividends, beta, etc.)
  - All ~1,600 tickers including ETFs
  - Daily append builds a time series of how ratios evolve
- **Dividends and splits** — full corporate action history back to IPO for all tickers (weekly overwrite)
- **Earnings history** — EPS actuals vs. analyst estimates, ~8–20 quarters per equity ticker (weekly overwrite)
- **Analyst recommendations** — upgrade/downgrade history from all major firms (weekly overwrite)
- **Analyst price targets** — mean/high/low/count consensus snapshot appended weekly to build a time series

### Macro Indicators (FRED)
203 series across 15 categories. Premium index series (SP500, NASDAQCOM, DJIA, WILL5000PR, NIKKEI225) excluded — require a paid FRED subscription:
- **Interest rates** (14) — DFF, FEDFUNDS, SOFR, DFEDTARU, DFEDTARL, full Treasury curve DGS1MO → DGS30
- **Yield curve & spreads** (18) — T10Y2Y, T10Y3M, DFII2–DFII30 (TIPS), Treasury–Fed Funds spreads, Aaa/Baa–FF spreads, TEDRATE, HQM corporate bond spot rates
- **Inflation** (20) — CPI headline/core, PCE headline/core, PPI, UMich inflation expectations, CPI sub-components (housing, energy, medical, transport, recreation, durables), Atlanta Fed sticky/flexible CPI, GDP deflator
- **Labor market** (26) — UNRATE, U6RATE, PAYEMS, CIVPART, EMRATIO, CNP16OV, JOLTS (openings, hires, quits, layoffs), jobless claims, manufacturing/services/construction/financial/mining/trade payrolls, average hours, average earnings, ECI wages, productivity (OPHNFB), unit labor costs
- **GDP & growth** (15) — GDP, real GDP, potential GDP, GDP deflator, industrial production (total + manufacturing + capacity utilization), durable/capital goods orders, inventory-to-sales ratio, CFNAI and 3-month MA
- **Consumer** (10) — retail sales (total + ex-auto), PCE (total + durables + nondurables + services), disposable income, saving rate, consumer credit, credit card interest rate
- **Credit & financial conditions** (18) — HY/IG/commercial paper spreads, corporate bond yields (Aaa/Baa), mortgage/residential/business loan delinquency rates, bank loans outstanding, deposits, commercial paper outstanding, SLOOS lending standards and demand (C&I + consumer), NFCI and adjusted NFCI
- **Housing** (13) — mortgage rates (30yr/15yr), housing starts (total + single-family), building permits, new/existing home sales, median sale price, Case-Shiller, homeowner/rental vacancy rates, total construction spending
- **Money supply** (11) — M1, M2, M2 velocity, monetary base, retail/institutional money funds, Fed balance sheet (WALCL), reserve balances, RRPONTSYD
- **Trade & FX** (15) — exports, imports, export prices, broad USD index, 8 major currency pairs (EUR, JPY, GBP, CNY, CAD, BRL, KRW, INR, MXN), wholesale inventories and sales
- **Energy & commodities** (12) — WTI, Brent, gasoline, natural gas, retail electricity, gold, copper, nickel, iron ore, wheat, corn, cotton
- **Market indicators** (4) — VIX, NBER recession indicators (two variants), Empire State manufacturing survey
- **Regional Fed manufacturing** (12) — Philly Fed (general activity, new orders, prices paid, employment, shipments), Richmond Fed (business conditions, employment), Dallas Fed (activity, production, employment), Kansas City Fed (activity, production)
- **Government finance & fiscal** (8) — gross federal debt, debt/GDP, monthly Treasury surplus/deficit, federal receipts, expenditures, government net saving, outlays/GDP, revenue/GDP
- **Banking profitability & deposits** (8) — net interest margin, ROE, ROA, consumer loan delinquency, total deposits, prime rate, large time deposits, bank equity/assets ratio

### FRED Series Catalog
- **RAW.FRED_RELEASES** — one row per FRED statistical release (~300 rows), rebuilt monthly
- **RAW.FRED_SERIES_CATALOG** — one row per unique FRED series (~50–150K rows), rebuilt monthly
- Use as an information schema to discover new series, check coverage gaps, and prioritize what to ingest next

---

## Warehouse Structure

```
EQUITY_ANALYTICS
├── RAW
│   ├── PRICES                  -- daily OHLCV, ~1,600 tickers, incremental append
│   ├── COMPANY_INFO            -- company metadata, overwrite on each run
│   ├── MACRO_INDICATORS        -- 175 FRED series, incremental append
│   ├── FINANCIAL_STATEMENTS    -- EAV format (income/balance/cashflow), weekly overwrite
│   ├── VALUATION_METRICS       -- point-in-time ratios, daily append
│   ├── DIVIDENDS_AND_SPLITS    -- full corporate action history, weekly overwrite
│   ├── EARNINGS_HISTORY        -- EPS actuals vs. estimates, weekly overwrite
│   ├── ANALYST_RECOMMENDATIONS -- upgrade/downgrade history, weekly overwrite
│   ├── ANALYST_PRICE_TARGETS   -- consensus price target snapshot, weekly append
│   ├── FRED_RELEASES           -- FRED publication metadata, monthly overwrite
│   ├── FRED_SERIES_CATALOG     -- all FRED series metadata, monthly overwrite
│   └── VW_FRED_HYGIENE         -- view: latest/prev obs date + row-count per series (duplicate detector)
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

---

## Pipeline Architecture

### Orchestration

Pipelines run on **Apache Airflow 2.9.3** deployed via Docker Compose locally on Windows. The Airflow UI is accessible at `http://localhost:8080`. All DAGs use the TaskFlow API (`@dag` / `@task` decorators) with LocalExecutor.

### Airflow DAGs

**`equity_daily`** — schedule `0 4 * * 2-6` (11pm ET Mon–Fri)
- Fetches current S&P 1500 components dynamically from Wikipedia (3 index pages)
- Checks max loaded date and extracts only new trading days
- Bulk price download for all ~1,600 tickers in batches
- Per-ticker metadata extraction with rate limiting
- Appends to `RAW.PRICES`, overwrites `RAW.COMPANY_INFO`

**`macro_daily`** — schedule `0 4 * * 2-6` (11pm ET Mon–Fri)
- All 203 FRED series fetched automatically — adding series to `FRED_SERIES` in `extract_fred.py` is sufficient, no DAG changes needed
- Incremental append: queries `MAX(date)` already loaded and fetches only newer observations (with a 7-day overlap to catch FRED revisions)
- Falls back to 30-day lookback if the table is empty

**`fundamentals_weekly`** — schedule `0 4 * * 0` (11pm ET Saturday)
- Financial statements: full overwrite of `RAW.FINANCIAL_STATEMENTS` (catches restatements)
- ETFs filtered out — no 10-K filings

**`valuation_daily`** — schedule `0 4 * * 2-6` (11pm ET Mon–Fri)
- Valuation metrics: daily append to `RAW.VALUATION_METRICS` (builds time series)

**`equity_supplemental_weekly`** — schedule `0 4 * * 0` (11pm ET Saturday)
- Four tasks run in parallel after ticker lists are resolved:
  - Dividends + splits: full history overwrite → `RAW.DIVIDENDS_AND_SPLITS`
  - Earnings history: EPS actuals vs. estimates overwrite → `RAW.EARNINGS_HISTORY`
  - Analyst recommendations: upgrade/downgrade history overwrite → `RAW.ANALYST_RECOMMENDATIONS`
  - Analyst price targets: weekly consensus snapshot append → `RAW.ANALYST_PRICE_TARGETS`

**`fred_catalog_refresh`** — schedule `0 4 2 * *` (11pm ET on the 1st of each month)
- Crawls all ~300 FRED statistical releases
- Collects metadata for every series in each release
- Overwrites `RAW.FRED_RELEASES` and `RAW.FRED_SERIES_CATALOG`

**`backfill_prices`** — `schedule=None` (manual trigger only)
- Loads historical OHLCV back to 2010-01-01 for all tickers
- Batches of 50 tickers with 30-second delays between batches

**`macro_backfill`** — `schedule=None` (manual trigger only, **paused by default**)
- Full FRED history from 1900-01-01 for **all** configured series — full overwrite of `RAW.MACRO_INDICATORS`
- Use only when intentionally refreshing all series (e.g. after GDP restatements). Dangerous: overwrites history.

**`fred_new_series_backfill`** — `schedule=None` (manual trigger only)
- Safe alternative: computes the set difference between `FRED_SERIES` and series already in `RAW.MACRO_INDICATORS`
- Fetches full history only for series not yet loaded — appends with `overwrite=False`, never touches existing data
- Idempotent: safe to re-run. Use this after adding new series to `FRED_SERIES`.

### Transformation Layer

dbt models follow a strict three-tier architecture:

**Staging** — one model per source. Cleans types, renames columns, handles nulls. No business logic.
- `stg_prices` — converts Unix nanosecond timestamps to dates, casts price fields
- `stg_companies` — standardizes sector/industry, coalesces nulls to 'Unknown'
- `stg_macro_indicators` — converts timestamps, casts values
- `stg_financial_statements` — converts timestamps, casts values, filters nulls
- `stg_valuation_metrics` — renames camelCase yfinance fields to snake_case columns

**Intermediate** — reusable business logic building blocks.
- `int_daily_returns` — daily return via LAG window function, 30-day annualized rolling volatility, 52-week high/low range
- `int_fundamentals_pivoted` — pivots EAV financial statements to ~35 named columns, computes derived margins in a separate CTE to avoid Snowflake GROUP BY + CASE nesting issues

**Marts** — Kimball dimensional model, consumption-ready tables.
- `dim_security` — one row per ticker with company metadata, first/last trading date
- `dim_date` — calendar dimension spanning 2009 to present with fiscal labels
- `fact_daily_prices` — grain: ticker + trading day. Incremental with `unique_key=['ticker', 'price_date']`
- `fact_macro_readings` — grain: series_id + observation_date. Incremental
- `fact_fundamentals` — grain: ticker + period_end_date + frequency. Incremental with `unique_key=['ticker', 'period_end_date', 'frequency']`
- `fact_valuation_snapshot` — grain: ticker + snapshot_date. Incremental with `unique_key=['ticker', 'snapshot_date']`

### Data Quality

25+ automated dbt tests across three layers:

**Staging tests** — not_null on critical columns, unique ticker in company model, accepted_values on statement_type and frequency

**Mart tests** — not_null on all fact columns, unique + not_null on dimension keys, referential integrity

**Singular business rule tests**
- `assert_no_future_prices` — no price dates beyond today
- `assert_return_bounds` — no daily return exceeding +-50%
- `assert_no_negative_prices` — no close price <= 0
- `assert_no_negative_volume` — no volume < 0
- `assert_no_future_fundamentals` — no period_end_date beyond today
- `assert_no_negative_revenue` — no total_revenue < 0
- `assert_no_negative_assets` — no total_assets < 0

### Database Health Check

`scripts/db_health_check.py` — standalone reusable test suite. Run anytime to confirm data quality without Airflow or Streamlit:

```bash
python scripts/db_health_check.py             # full check, all layers
python scripts/db_health_check.py --verbose   # includes per-year and per-series breakdowns
python scripts/db_health_check.py --check prices|macro|fundamentals|valuation|raw
```

Checks: ticker universe size, price history depth and recency, macro series count and RAW->MART propagation, fundamental coverage, valuation snapshot freshness, and RAW row count minimums for all tables. Returns exit code 0 (pass) or 1 (fail).

### CI/CD

Two GitHub Actions workflows:

**`dbt_ci.yml`** — triggers on every PR touching dbt files
- Builds into an isolated `CI_{pr_number}` Snowflake schema — concurrent runs never interfere
- On merge to main, runs with `target: prod` writing directly to MARTS
- PRs cannot merge if any model or test fails
- CI schema cleanup runs with `continue-on-error: true` — a cleanup failure won't block the PR

**`code_review.yml`** — triggers on every PR
- Walks the repo, chunks all Python, SQL, and YAML files
- Sends to Claude API with a senior financial data engineer system prompt
- Posts full code review as a PR comment with critical issues, warnings, and suggestions

---

## Analytics Application

### Analytics App (port 8501)

A Streamlit chat interface powered by Claude accepts natural language prompts, generates Snowflake SQL against the mart layer, executes it, and renders interactive Plotly charts.

**Two-step LLM pipeline:**
1. Claude generates Snowflake SQL from the natural language prompt using full schema context
2. Claude determines optimal chart type and axis mappings from the returned DataFrame structure

**Query result caching:** identical prompts return a cached DataFrame for 5 minutes — no redundant Snowflake round-trips.

**Example prompts — prices and macro:**
- "Compare cumulative returns for SPY, QQQ and IWM over the last year"
- "Show me 30-day rolling volatility for AAPL, MSFT and GOOGL"
- "Which sector had the highest average daily return last month?"
- "Show me tickers trading closest to their 52-week high"
- "How did SPY perform during periods when the yield curve was inverted?"
- "Compare SPY daily returns against the Fed funds rate over the last year"

**Example prompts — fundamentals:**
- "Show me AAPL's revenue and net income trend over the last 4 years"
- "Which S&P 500 stocks have the lowest trailing PE ratio?"
- "Compare operating margins for AAPL, MSFT, GOOGL and META"
- "Show me the top 10 stocks by free cash flow yield"
- "How has JPM's return on equity changed over time?"
- "Compare debt-to-equity ratios across bank stocks"
- "Which stocks have the highest revenue growth?"

### DB Health Tab

The main app includes a **DB Health** tab (tab 04) for pipeline operations and data quality checks. It is gated by environment variable (`HEALTH_TAB_ENABLED=true`) so it can be safely hidden when deployed to Streamlit Community Cloud.

**Checks run across three sections:**
- **RAW layer** — row counts for all 11 RAW tables, verifying minimum expected rows
- **Coverage** — distinct ticker counts across prices, fundamentals, and valuations; FRED catalog row count; staleness of FRED series catalog
- **MARTS layer** — row counts and ticker coverage across all 6 mart tables
- **Infrastructure** (local only, `IS_LOCAL=true`) — Docker daemon status, Airflow API reachability, Snowflake connection

**Security model:**

| Env var | Local `.env` | Community Cloud |
|---|---|---|
| `HEALTH_TAB_ENABLED` | `true` | leave unset (hides content) or `true` + password |
| `HEALTH_CHECK_PASSWORD` | optional | recommended if tab is enabled |
| `IS_LOCAL` | `true` | leave unset (disables Docker/Airflow subprocess calls) |

**Launch:** double-click `equity_analytics.bat` or pin to Start via `pin_to_start_menu.ps1`. The launcher runs `scripts/db_health_check.py` in the terminal before opening the app.

---

## Key Design Decisions

**Three-schema warehouse architecture (RAW / STAGING / MARTS)**
RAW is append-only and immutable — a broken transformation never corrupts source data. Transformations can always be replayed from RAW. Staging cleans without business logic. Marts are the only layer analysts and applications query.

**Kimball dimensional modeling over one big table**
Fact and dimension tables with declared grain make the mart layer intuitive for analysts and optimized for aggregation queries. The `unique_key` constraint on incremental fact models prevents duplicate rows even if ingestion overlaps.

**Incremental loads over full refresh**
Daily runs check `MAX(price_date)` already in Snowflake and only extract newer trading days. A full year of history for ~1,600 tickers is ~400K rows — no reason to reload it daily. The dbt incremental model propagates only new rows through the transformation layer.

**Financial statements as full overwrite, valuation metrics as append**
yfinance returns a fixed ~4yr/8Q window for financial statements, and values are subject to retroactive restatement. Full overwrite is the correct strategy — the data is small and this guarantees the warehouse reflects current reported numbers. Valuation metrics (PE, margins, etc.) are point-in-time and never retroactively corrected, so daily append is correct and builds a queryable time series.

**EAV -> pivot pattern for financial statements**
yfinance returns ~276 unique line items with spaced names (e.g. "Total Revenue", "Net Income"). Storing as EAV in RAW is resilient to schema drift — new line items from yfinance don't break the load. The intermediate pivot model selects the ~35 most analytically useful fields by name and computes derived margins in a separate CTE to work around Snowflake's GROUP BY + CASE expression nesting restrictions.

**Live Wikipedia scraping for index membership**
Rather than maintaining a static ticker list, all three S&P index component lists (500, 400, 600) are fetched live from Wikipedia on each DAG run. When stocks are added or removed from an index, the next daily run picks up the change automatically. Static fallback lists cover the rare case where Wikipedia is unreachable.

**FRED catalog as an information schema**
Rather than manually tracking which FRED series exist, a monthly DAG crawls all ~300 FRED statistical releases and catalogs every series with its popularity score (0-100). This makes it easy to find high-value series not yet being ingested by querying `RAW.FRED_SERIES_CATALOG` against `RAW.MACRO_INDICATORS`.

**Rate limiting for all per-ticker and per-series calls**
Price data uses yfinance bulk download. Everything else (metadata, fundamentals, FRED series) uses per-item calls with explicit `time.sleep()` delays — 0.5s for most calls, 2s for valuation metrics. FRED gets a 15s backoff on 429 with one retry before skipping.

**RSA key-pair authentication (never expires)**
All Snowflake connections use RSA key-pair auth — the connector receives DER-encoded private key bytes loaded from a `.pem` file via the `cryptography` library. Unlike programmatic access tokens, RSA keys never expire. The public key is registered in Snowflake once; the private key is stored locally (gitignored), mounted into Docker containers via volume, and stored as a GitHub Secret for CI.

**Isolated CI schemas**
Each GitHub Actions run builds into `CI_{pr_number}` — a fresh, isolated Snowflake schema. Concurrent PR runs never interfere. Production MARTS schema is only written to on merge to main.

**AI-powered code review on every PR**
A GitHub Actions workflow calls the Claude API with all modified files and posts a structured code review as a PR comment — critical issues, warnings, suggestions, and strengths. Files exceeding the review limit are truncated with a visible marker rather than silently dropped.

---

## Repository Structure

```
equityanalytics/
├── ingestion/
│   ├── extract.py                    # yfinance extraction -- S&P 1500 scrapers, bulk price download
│   ├── extract_fred.py               # FRED API extraction -- 108 series with rate limiting
│   ├── extract_fred_catalog.py       # FRED releases + series catalog crawler
│   ├── extract_fundamentals.py       # Financial statements (EAV) + valuation metrics
│   └── load.py                       # Snowflake bulk loading, get_max_date, get_min_date
├── airflow/
│   ├── dags/
│   │   ├── dag_equity_daily.py              # equity_daily DAG -- prices + company info
│   │   ├── dag_macro_daily.py               # macro_daily DAG -- 175 FRED series, incremental
│   │   ├── dag_fundamentals.py              # fundamentals_weekly + valuation_daily DAGs
│   │   ├── dag_equity_supplemental.py       # equity_supplemental_weekly -- dividends, earnings, analyst data
│   │   ├── dag_fred_catalog.py              # fred_catalog_refresh -- monthly FRED metadata crawl
│   │   ├── dag_backfill.py                  # backfill_prices DAG -- manual trigger only
│   │   ├── dag_backfill_new_tickers.py      # backfill_new_tickers -- new tickers only, safe re-run
│   │   ├── dag_macro_backfill.py            # macro_backfill DAG -- full FRED history, all series (paused)
│   │   └── dag_fred_new_series_backfill.py  # fred_new_series_backfill -- new series only, append-safe
│   ├── logs/                             # Airflow task logs (gitignored)
│   └── plugins/                          # Custom operators (future)
├── dbt_project/
│   ├── models/
│   │   ├── staging/              # stg_prices, stg_companies, stg_macro_indicators,
│   │   │                         #   stg_financial_statements, stg_valuation_metrics
│   │   ├── intermediate/         # int_daily_returns, int_fundamentals_pivoted
│   │   └── marts/                # dim_date, dim_security, fact_daily_prices,
│   │                             #   fact_macro_readings, fact_fundamentals, fact_valuation_snapshot
│   ├── tests/                    # Singular business rule tests
│   └── macros/                   # generate_schema_name
├── admin_dashboard/
│   ├── app.py                        # Admin Streamlit app entry point (port 8502)
│   ├── airflow_client.py             # Airflow REST API v1 wrapper
│   ├── health_check.py               # Structured health checks (returns dicts)
│   ├── styles/
│   │   └── winforms.css              # Windows 98 Win Forms aesthetic CSS
│   └── components/
│       ├── status_bar.py             # Docker / Airflow / Snowflake status dots
│       ├── dag_monitor.py            # DAG list, trigger with confirmation, pause/unpause
│       ├── log_viewer.py             # Airflow log filesystem reader
│       └── data_quality.py           # Health check results table
├── scripts/
│   ├── db_health_check.py            # Reusable DB health check -- run anytime to confirm data quality
│   └── create_fred_hygiene_view.py   # One-shot: creates VW_FRED_HYGIENE in Snowflake RAW schema
├── app/
│   └── db/
│       └── snowflake.py              # Snowflake connection + query helpers for the Streamlit app
├── agents/
│   ├── chart_agent.py            # Streamlit + Claude chat application
│   └── code_reviewer.py          # AI code review agent
├── .github/
│   └── workflows/
│       ├── dbt_ci.yml            # dbt build + test on every PR, prod deploy on merge
│       └── code_review.yml       # AI code review comment on every PR
├── equity_analytics.bat          # App launcher (docker up + Airflow health wait + db_health_check + Streamlit)
├── pin_to_start_menu.ps1         # Creates a Start Menu shortcut for equity_analytics.bat
├── CLAUDE.md                     # Claude Code project guide (architecture, patterns, pitfalls)
├── docker-compose.yml            # Airflow services (webserver, scheduler, init, postgres)
├── dbt_project.yml               # dbt project config
├── profiles.yml                  # dbt Core connection profile (gitignored)
├── requirements.txt
├── .env.example
└── CONTEXT.md                    # Project state reference
```

---

## Setup

### Prerequisites
- Python 3.11+
- Snowflake account with an RSA key pair registered for your user
- Docker + Docker Compose (for Airflow)
- Anthropic API key
- FRED API key (free at fred.stlouisfed.org)

### Environment Variables
Copy `.env.example` to `.env`:

```
SNOWFLAKE_USER=DBT_USER
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_PRIVATE_KEY_PATH=snowflake_private_key.pem
# SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=   # only if key was generated with a passphrase
SNOWFLAKE_WAREHOUSE=TRANSFORM_WH
SNOWFLAKE_DATABASE=EQUITY_ANALYTICS
SNOWFLAKE_SCHEMA=MARTS

# Airflow — used at init (to create the admin user) AND at runtime by the admin dashboard REST API client
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_EMAIL=you@example.com
AIRFLOW_ADMIN_PASSWORD=replace-with-strong-password
AIRFLOW_SECRET_KEY=replace-with-output-of-python-secrets-token-hex-32

ANTHROPIC_API_KEY=your_anthropic_key
FRED_API_KEY=your_fred_api_key
```

Generate `AIRFLOW_SECRET_KEY` with:
```bash
# Python (cross-platform)
python -c "import secrets; print(secrets.token_hex(32))"

# Linux/Mac alternative
openssl rand -hex 32
```

### Snowflake Setup
Run in a Snowflake worksheet:

```sql
CREATE WAREHOUSE TRANSFORM_WH WAREHOUSE_SIZE='X-SMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE;
CREATE DATABASE EQUITY_ANALYTICS;
CREATE SCHEMA EQUITY_ANALYTICS.RAW;
CREATE SCHEMA EQUITY_ANALYTICS.STAGING;
CREATE SCHEMA EQUITY_ANALYTICS.INTERMEDIATE;
CREATE SCHEMA EQUITY_ANALYTICS.MARTS;

-- Register your RSA public key for the service user
ALTER USER DBT_USER SET RSA_PUBLIC_KEY='<paste your public key here>';
```

### RSA Key Pair Setup

```bash
# Generate key pair (no passphrase for simplicity)
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out snowflake_private_key.pem
openssl rsa -in snowflake_private_key.pem -pubout -out snowflake_public_key.pem
```

Copy the public key content (without header/footer lines) and register it in Snowflake using the `ALTER USER` command above.

### Starting Airflow

```bash
# First time only -- initialize the database and create admin user
docker compose run --rm airflow-init

# Start webserver + scheduler (runs in background)
docker compose up -d airflow-webserver airflow-scheduler

# Airflow UI -> http://localhost:8080
```

### Running dbt

First, load environment variables (dbt Core does not auto-read `.env`):
```bash
# Linux/Mac
export $(grep -v '^#' .env | xargs)

# Windows PowerShell
Get-Content .env | Where-Object { $_ -notmatch '^\s*#' -and $_ -match '=' } | ForEach-Object {
    $k, $v = $_ -split '=', 2; Set-Item "env:$($k.Trim())" $v.Trim()
}
```

Then run dbt from the project root (where `profiles.yml` lives):
```bash
dbt debug --profiles-dir .                               # verify connection
dbt build --profiles-dir .                               # all models and tests
dbt test --profiles-dir .                                # tests only
dbt build --profiles-dir . --select staging              # staging layer only
dbt build --profiles-dir . --select fact_daily_prices --full-refresh
```

### Running the Analytics App

```bash
streamlit run agents/chart_agent.py
```

### Running the AI Code Reviewer

```bash
python agents/code_reviewer.py
# Output saved to code_review.md
```

---

## Backfill Runbook

One-time operations to populate historical data. All backfills are idempotent — safe to re-run if interrupted.

### Price Backfill (2010 to present)

Loads ~4M rows of daily OHLCV data across ~1,600 tickers. Batched in groups of 50 with 30-second delays.

**Trigger via Airflow UI:**
1. Go to `http://localhost:8080`
2. Enable the `backfill_prices` DAG
3. Click "Trigger DAG"

After the backfill completes, run a full-refresh dbt build to propagate all historical rows:
```bash
dbt build --profiles-dir . --select fact_daily_prices --full-refresh
```

### Adding New FRED Series (targeted backfill)

After adding series to `FRED_SERIES` in `ingestion/extract_fred.py`:

```bash
# 1. Trigger the targeted backfill — appends full history for NEW series only, never touches existing data
docker compose exec airflow-webserver airflow dags trigger fred_new_series_backfill

# 2. Full-refresh the mart to incorporate new series history
dbt build --profiles-dir . --select fact_macro_readings --full-refresh
```

The DAG computes `FRED_SERIES.keys() - series already in RAW.MACRO_INDICATORS` at runtime — idempotent and safe to re-run.

### Macro Backfill (full FRED history — use sparingly)

Overwrites `RAW.MACRO_INDICATORS` with complete FRED history for **all** configured series. Use only when intentionally refreshing revised data across all series (e.g. after GDP restatements). `macro_backfill` is paused by default to prevent accidental triggers.

**Trigger via Airflow UI:**
1. Un-pause and trigger the `macro_backfill` DAG
2. Re-pause it immediately after

After completion, rebuild the mart:
```bash
dbt build --profiles-dir . --select fact_macro_readings --full-refresh
```

### Fundamentals Backfill (all ~1,500 equity tickers)

Loads financial statements and valuation metrics for the full S&P 1500 equity universe.

**Trigger via Airflow UI:**
1. Enable and trigger the `fundamentals_weekly` DAG manually

Validate the load, then run dbt:
```sql
SELECT COUNT(*), COUNT(DISTINCT ticker) FROM EQUITY_ANALYTICS.RAW.FINANCIAL_STATEMENTS;
SELECT COUNT(*), COUNT(DISTINCT ticker) FROM EQUITY_ANALYTICS.RAW.VALUATION_METRICS;
```
```bash
dbt build --profiles-dir . --select +fact_fundamentals +fact_valuation_snapshot --full-refresh
```

### Adding New Tickers

When the ticker universe changes (index rebalance, manual addition):

```bash
# 1. Trigger equity_daily DAG to load new prices and metadata
#    (new tickers are auto-detected on next scheduled run too)

# 2. Full-refresh fact_daily_prices -- incremental filter misses brand-new tickers
dbt build --profiles-dir . --select fact_daily_prices --full-refresh

# 3. Trigger fundamentals_weekly DAG for financial statements
# 4. Full-refresh fact_fundamentals for the same reason
dbt build --profiles-dir . --select +fact_fundamentals --full-refresh
```

---

## Tickers Covered

| Category | Examples | Count |
|---|---|---|
| S&P 500 (large-cap) | AAPL, MSFT, GOOGL, NVDA, JPM, JNJ... | ~503 |
| S&P 400 (mid-cap) | TXRH, SAIA, RPM, MORN, WTFC, SPSC... | ~400 |
| S&P 600 (small-cap) | BOOT, CWST, HAYN, MCRI, JBSS... | ~600 |
| Broad market ETFs | SPY, IVV, VOO, QQQ, VTI, IWM | 15 |
| Fixed income ETFs | BND, AGG, TLT, IEF, LQD, HYG | 15 |
| International ETFs | VEA, VWO, EFA, EEM, EWJ, EWZ | 15 |
| Sector ETFs | XLF, XLK, XLV, XLE, XBI, VNQ | 15 |
| Commodity ETFs | GLD, IAU, SLV, USO, DBC | 15 |
| Factor / smart beta | MTUM, USMV, VLUE, QUAL, SCHD | 15 |
| Thematic ETFs | ARKK, BOTZ, ICLN, HACK, WCLD | 15 |
| **Total unique** | | **~1,600** |

Index membership is fetched live from Wikipedia on each DAG run — rebalances are picked up automatically.

---

## Macro Indicators Covered

| Category | Series | Key examples |
|---|---|---|
| Interest rates | 14 | DFF, FEDFUNDS, SOFR, DFEDTARU, DFEDTARL, DGS1MO → DGS30 (full curve) |
| Yield curve & spreads | 18 | T10Y2Y, T10Y3M, DFII2–DFII30 (TIPS), T10YFFM, AAAFF, BAAFF, TEDRATE, HQMCB |
| Inflation | 20 | CPI/PCE/PPI, CPI sub-components (housing/energy/medical/transport), Atlanta Fed sticky/flexible CPI |
| Labor market | 26 | UNRATE, U6RATE, PAYEMS, JOLTS (openings/hires/quits/layoffs), sector payrolls, productivity, ULC |
| GDP & growth | 15 | GDP, GDPC1, GDPPOT, INDPRO, TCU, DGORDER, ISRATIO, CFNAI, CFNAIMA3 |
| Consumer | 10 | RETAILSMNSA, PCE (total + durables + nondurables + services), DSPIC96, PSAVERT, TOTALSL |
| Credit & financial | 18 | HY/IG/CP spreads, Aaa/Baa yields, mortgage/business/consumer delinquency, SLOOS, NFCI, ANFCI |
| Housing | 13 | MORTGAGE30US/15US, HOUST, PERMIT, HSN1F, CSUSHPISA, MSPUS, TTLCON, vacancy rates |
| Money supply | 11 | M1SL, M2SL, M2V, BOGMBASE, WALCL, TOTRESNS, RRPONTSYD, money market funds |
| Trade & FX | 15 | Exports/imports, export prices, DTWEXBGS, USD/EUR/JPY/GBP/CNY/CAD/BRL/KRW/INR/MXN |
| Energy & commodities | 12 | WTI, Brent, natural gas, gold, copper, nickel, iron ore, wheat, corn, cotton |
| Market indicators | 4 | VIXCLS, USREC, USRECM, Empire State manufacturing (GACDISA) |
| Regional Fed manufacturing *(Wave 4)* | 12 | Philly (PHFRBIND/NDI/P/E/SIP), Richmond (RMBSIICS/E), Dallas (DALLASMI/PE/EO), KC (KANSASMI/PE) |
| Government finance & fiscal *(Wave 5)* | 8 | GFDEBTN, GFDEGDQ188S, MTSDS133FMS, MTSO133FMS, FGEXPND, GGSAVE, FYONGDA188S, HBFRGDP |
| Banking profitability & deposits *(Wave 6)* | 8 | USNIM, USROE, USROA, DRCLACBS, WDTGAL, DPRIME, DTCTMFNM, EQTATOA |
| **Total** | **203** | |

---

## Roadmap

### Near-term (pipeline)
- **Historical valuation ratios** — dbt model computing trailing PE, P/B, P/S, dividend yield, and beta from existing `fact_daily_prices` x `fact_fundamentals` join, using point-in-time financial statement dates to avoid look-ahead bias. Extends ratio history back to 2010 without any new data sources.
- **dbt models for supplemental tables** — staging and mart models for `DIVIDENDS_AND_SPLITS`, `EARNINGS_HISTORY`, `ANALYST_RECOMMENDATIONS`, and `ANALYST_PRICE_TARGETS`.
- **NASDAQ Trader file integration** — replace Wikipedia scraping with the official NASDAQ trader file to cover all ~8,000–9,000 US-listed securities including OTC, micro-cap, and newly-listed stocks.

### FRED Series Expansion Roadmap

The goal is exhaustive coverage of the FRED catalog — systematic breadth over selective curation. Use `RAW.FRED_SERIES_CATALOG` to verify series IDs before adding each wave. Query against `RAW.MACRO_INDICATORS` to confirm gaps.

**Wave 4 — Regional Federal Reserve Economic Surveys** ✅ COMPLETE (12 series)

Five regional Fed banks publish monthly manufacturing and services surveys. We have Chicago (CFNAI, NFCI) and New York (Empire State). Missing:

| Series | Description | Bank |
|---|---|---|
| PHFRBIND | General Activity Index | Philadelphia Fed |
| PHFRBNDI | New Orders Index | Philadelphia Fed |
| PHFRBP | Prices Paid Index | Philadelphia Fed |
| PHFRBE | Employment Index | Philadelphia Fed |
| PHFRBSIP | Shipments Index | Philadelphia Fed |
| RMBSIICS | Business Conditions Index | Richmond Fed |
| RMBSIE | Employment Index | Richmond Fed |
| DALLASMI | General Business Activity | Dallas Fed |
| DALLASPE | Production Volume | Dallas Fed |
| DALLASEO | Employment | Dallas Fed |
| KANSASMI | Manufacturing Activity | Kansas City Fed |
| KANSASPE | Production | Kansas City Fed |

Rationale: Regional surveys provide leading signals on manufacturing activity and inflation pressure at a sub-national level — correlated with but leading national ISM data.

---

**Wave 5 — Government Finance & Fiscal Policy** ✅ COMPLETE (8 series)

Currently zero federal fiscal coverage despite fiscal policy being a primary macro driver.

| Series | Description | Frequency |
|---|---|---|
| GFDEBTN | Gross Federal Debt (millions USD) | Quarterly |
| GFDEGDQ188S | Federal Debt as % of GDP | Quarterly |
| MTSDS133FMS | Federal Surplus or Deficit (monthly Treasury statement) | Monthly |
| MTSO133FMS | Federal Government Total Receipts | Monthly |
| FGEXPND | Federal Government Current Expenditures | Quarterly |
| GGSAVE | Government Net Saving (national accounts) | Quarterly |
| FYONGDA188S | Federal Net Outlays as % of Nominal GDP | Annual |
| HBFRGDP | Federal Revenue as % of GDP | Annual |

Rationale: Debt sustainability and fiscal impulse affect real rates, inflation, and growth. Completely absent from current coverage.

---

**Wave 6 — Banking System Profitability & Deposit Data** ✅ COMPLETE (8 series)

We have lending standards (SLOOS) and loan quality (delinquency rates) but not bank profitability or deposit flows.

| Series | Description | Frequency |
|---|---|---|
| USNIM | Net Interest Margin (all commercial banks) | Quarterly |
| USROE | Return on Equity (all commercial banks) | Quarterly |
| USROA | Return on Assets (all commercial banks) | Quarterly |
| DRCCLACBS | Credit Card Loan Delinquency Rate | Quarterly |
| DRCLACBS | All Consumer Loan Delinquency Rate | Quarterly |
| WDTGAL | Total Deposits at All Commercial Banks | Weekly |
| DPSACBW027SBOG | Deposits (already loaded — confirm) | Weekly |
| RESBALNS | Reserve Balances with Federal Reserve Banks | Weekly |

Rationale: NIM compression/expansion is a primary driver of bank earnings and credit availability. Deposit flows (bank run risk) became critical post-SVB.

---

**Wave 7 — FX Completion (Remaining G10 + EM)** (~8 series)

We have 9 currency pairs. Missing G10: CHF, AUD, NZD, NOK, SEK. Missing liquid EM: TRY, ZAR, TWD.

| Series | Pair | Notes |
|---|---|---|
| DEXSZUS | CHF/USD | Swiss safe-haven flow indicator |
| DEXUSAL | AUD/USD | Commodity-linked currency |
| DEXNZUS | NZD/USD | Commodity + carry trade |
| DEXNOUS | NOK/USD | Oil-correlated |
| DEXSDUS | SEK/USD | European proxy |
| DEXSFUS | ZAR/USD | EM risk appetite proxy |
| DEXTHUS | THB/USD | SE Asia EM |

Rationale: Systematic completion of all major traded pairs rather than selective coverage.

---

**Wave 8 — Granular Industry Employment (BLS CES)** (~12 series)

We have 6 broad employment sectors. The BLS Current Employment Statistics has ~100 industry categories. Next level of granularity:

| Series | Description |
|---|---|
| USINFO | All Employees: Information |
| USGOVT | All Employees: Government (Federal + State + Local) |
| CES0800000001 | All Employees: Leisure and Hospitality |
| CES0600000001 | All Employees: Professional and Business Services |
| CES0700000001 | All Employees: Education and Health Services |
| AHETPI | Average Hourly Earnings: Total Private |
| AHEMAN | Average Hourly Earnings: Manufacturing |
| AWHI | Average Weekly Hours: All Private Industries |
| JTSOPELY | JOLTS Job Openings: Professional and Business Services |
| JTSOPHY | JOLTS Job Openings: Healthcare and Social Assistance |
| JTSOGOSY | JOLTS Job Openings: Government |
| JTSOCONSY | JOLTS Job Openings: Construction |

Rationale: Sector-level employment is a leading indicator of sector-specific activity. Industry wage data drives PCE services inflation.

---

**Wave 9 — Price Indices Beyond CPI/PCE** (~10 series)

We have consumer prices and PPI. Missing: import/export price indices, construction costs, producer price sub-components.

| Series | Description | Frequency |
|---|---|---|
| IR | Import Price Index (all commodities) | Monthly |
| IQ | Export Price Index (all commodities) | Monthly |
| PPIFIS | PPI: Final Demand for Services | Monthly |
| PPIFGS | PPI: Final Demand Goods | Monthly |
| PPIIDC | PPI: Industrial Chemicals | Monthly |
| PCUOMFGOMFG | PPI: Manufacturing | Monthly |
| WPUFD49104 | PPI: Finished Consumer Foods | Monthly |
| USALOLITONOSTSAM | OECD Composite Leading Indicator: US | Monthly |
| BOGZ1FA896902605Q | Household Net Worth | Quarterly |

Rationale: Import/export prices are leading indicators of CPI pass-through. Construction costs affect residential investment modeling.

---

**Wave 10 — Business Surveys & Composite Leading Indicators** (~8 series)

Forward-looking survey data that markets price ahead of hard economic data.

| Series | Description | Source |
|---|---|---|
| NAPMPI | ISM Manufacturing PMI | ISM (verify FRED availability) |
| NAPMII | ISM Non-Manufacturing PMI | ISM (verify FRED availability) |
| USALOLITONOSTSAM | OECD CLI for United States | OECD / FRED |
| BSCICP02USM460S | Business Confidence Index | OECD / FRED |
| CSCICP02USM460S | Consumer Confidence Index | OECD / FRED |
| NFBUSOPSM | NFIB Business Optimism (if available) | NFIB |
| GACDISA066MSFRBNY | Empire State (already loaded) | NY Fed |

Note: Verify all series IDs against `RAW.FRED_SERIES_CATALOG` before adding — some survey series have restricted access or inconsistent FRED coverage.

---

**Wave 11 — Transportation & Supply Chain Proxies** (~6 series)

Currently zero logistics coverage. FRED has limited but useful transportation series.

| Series | Description | Frequency |
|---|---|---|
| TRUCKD11 | Truck Tonnage Index | Monthly |
| RAILFRTCARLOADSAMSA | Rail Carloads (SA) | Monthly |
| DTSGFHFNM | Freight Transportation Services Index | Monthly |
| AIRRPAX | Air Revenue Passenger Miles (if available) | Monthly |

Rationale: Freight volume is a coincident indicator of goods-producing economic activity. Trucking tonnage often leads GDP revisions.

---

**Wave 12 — Population & Demographic Flows** (~6 series)

| Series | Description | Frequency |
|---|---|---|
| SPPOPtotusm | Total Population: US | Monthly |
| NETMIGUSDOILBRUSA | Net International Migration | Annual |
| NQPOP65USLTOT | Population 65+ | Quarterly |
| USAPOPL | Total Population (BLS) | Monthly |
| LFPART | Labor Force Participation components (further breakdown) | Monthly |

Rationale: Demographic flows determine long-run labor supply, housing demand, and social security sustainability — missing from current coverage.

---

**Wave 13 — State-Level Economic Data** (~variable)

Currently 100% national/aggregate data. State-level adds a geographic cross-section dimension.

Priority states (by GDP): CA, TX, NY, FL, IL, PA, OH, GA, NJ, WA

Key series types available per state:
- Unemployment rates: `LASST{FIPS}UR` pattern
- Nonfarm payrolls: `SMU{FIPS}000000001SA` pattern  
- Coincident economic activity index: `{STATE}PHCI` (Philadelphia Fed state indices)
- Home price indices: `{STATE}STHPI` (FHFA)

Note: Adding state-level data requires deciding on either a sample of key states or a complete 50-state sweep. Recommend a dedicated DAG for state-level series rather than adding to `FRED_SERIES`.

---

### Future (non-FRED)
- **Options data** — open interest, implied volatility surface from yfinance or a dedicated provider
- **Insider transactions** — SEC Form 4 filings via EDGAR API
- **Short interest** — FINRA-reported short volume and short interest ratio
- **International equities** — major index components from LSE, TSX, ASX via yfinance (`.L`, `.TO`, `.AX` suffix routing)
- **SEC EDGAR filings** — 8-K, 10-K, 10-Q structured data via EDGAR API for event-driven analysis
