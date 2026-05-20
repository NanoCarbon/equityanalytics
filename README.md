# Equity Analytics Pipeline

A production-style ELT pipeline and AI-powered analytics application built as a portfolio project for data engineering roles in financial services. Ingests the full S&P Composite 1500 universe plus top ETFs, 108 Federal Reserve macro indicators, complete fundamental financial data (income statements, balance sheets, cash flow, and valuation metrics), supplemental equity data (dividends, earnings history, analyst ratings, price targets), and a full FRED series catalog — models them into a Kimball dimensional warehouse, and exposes the data through a natural language chat interface that generates SQL and interactive charts on demand.

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
| Ingestion | Python + FRED API | 108 macro economic indicators + full FRED series catalog |
| Orchestration | Apache Airflow 2.9.3 (Docker, local) | Scheduling, retries, observability |
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
108 series across 11 categories (5 premium series removed — SP500, NASDAQCOM, DJIA, WILL5000PR, NIKKEI225 require a paid FRED subscription):
- Interest rates and yield curve (DFF, SOFR, DGS2, DGS10, T10Y2Y, T10Y3M, DFEDTARU, DFEDTARL...)
- Inflation (CPI, Core CPI, PCE, Core PCE, PPI...)
- Labor market (UNRATE, U6RATE, PAYEMS, JOLTS, jobless claims...)
- GDP and growth (GDP, Real GDP, GDPPOT, industrial production, retail sales...)
- Credit and financial conditions (HY spread, IG spread, TED spread, mortgage rates, LOANS...)
- Housing (housing starts, building permits, Case-Shiller, existing home sales...)
- Money supply (M1, M2, M2V, monetary base, WALCL, TOTRESNS, WTREGEN...)
- Energy and commodities (WTI, Brent, natural gas, gasoline, gold...)
- FX rates (USD/EUR, USD/JPY, USD/GBP, USD/CNY, DTWEXBGS...)
- Consumer and sentiment (UMich sentiment, durable goods, consumer credit...)

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
│   ├── MACRO_INDICATORS        -- 108 FRED series, incremental append
│   ├── FINANCIAL_STATEMENTS    -- EAV format (income/balance/cashflow), weekly overwrite
│   ├── VALUATION_METRICS       -- point-in-time ratios, daily append
│   ├── DIVIDENDS_AND_SPLITS    -- full corporate action history, weekly overwrite
│   ├── EARNINGS_HISTORY        -- EPS actuals vs. estimates, weekly overwrite
│   ├── ANALYST_RECOMMENDATIONS -- upgrade/downgrade history, weekly overwrite
│   ├── ANALYST_PRICE_TARGETS   -- consensus price target snapshot, weekly append
│   ├── FRED_RELEASES           -- FRED publication metadata, monthly overwrite
│   └── FRED_SERIES_CATALOG     -- all FRED series metadata, monthly overwrite
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

Pipelines run on **Apache Airflow 2.9.3** deployed via Docker Compose (locally or on EC2). The Airflow UI is accessible at `http://localhost:8080`. All DAGs use the TaskFlow API (`@dag` / `@task` decorators) with LocalExecutor.

### Airflow DAGs

**`equity_daily`** — schedule `0 4 * * 2-6` (11pm ET Mon–Fri)
- Fetches current S&P 1500 components dynamically from Wikipedia (3 index pages)
- Checks max loaded date and extracts only new trading days
- Bulk price download for all ~1,600 tickers in batches
- Per-ticker metadata extraction with rate limiting
- Appends to `RAW.PRICES`, overwrites `RAW.COMPANY_INFO`

**`macro_daily`** — schedule `0 4 * * 2-6` (11pm ET Mon–Fri)
- 108 FRED series fetched with graceful 403/429 handling and rate limiting
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

**`macro_backfill`** — `schedule=None` (manual trigger only)
- Full FRED history from 1900-01-01 for all 108 configured series
- Single overwrite of `RAW.MACRO_INDICATORS`

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
│   │   ├── dag_equity_daily.py           # equity_daily DAG -- prices + company info
│   │   ├── dag_macro_daily.py            # macro_daily DAG -- FRED series
│   │   ├── dag_fundamentals.py           # fundamentals_weekly + valuation_daily DAGs
│   │   ├── dag_equity_supplemental.py    # equity_supplemental_weekly -- dividends, earnings, analyst data
│   │   ├── dag_fred_catalog.py           # fred_catalog_refresh -- monthly FRED metadata crawl
│   │   ├── dag_backfill.py               # backfill_prices DAG -- manual trigger only
│   │   └── dag_macro_backfill.py         # macro_backfill DAG -- full FRED history, manual trigger
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
├── scripts/
│   └── db_health_check.py            # Reusable DB health check -- run anytime to confirm data quality
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

AIRFLOW_SECRET_KEY=replace-with-output-of-openssl-rand-hex-32
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_EMAIL=you@example.com
AIRFLOW_ADMIN_PASSWORD=replace-with-strong-password

ANTHROPIC_API_KEY=your_anthropic_key
FRED_API_KEY=your_fred_api_key
```

Generate `AIRFLOW_SECRET_KEY` with:
```bash
openssl rand -hex 32
# Windows PowerShell alternative:
# -join ((1..32) | ForEach-Object { '{0:x2}' -f (Get-Random -Max 256) })
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

### Macro Backfill (full FRED history)

Loads the complete FRED history (back to 1900-01-01 where available) for all 108 configured series.

**Trigger via Airflow UI:**
1. Enable and trigger the `macro_backfill` DAG

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

| Category | Series | Examples |
|---|---|---|
| Interest rates | 12 | DFF, SOFR, DGS2, DGS5, DGS10, DGS30, DFEDTARU, DFEDTARL |
| Yield curve and real rates | 7 | T10Y2Y, T10Y3M, DFII2, DFII5, DFII10, DFII30 |
| Inflation | 7 | CPIAUCSL, CPILFESL, PCEPI, PCEPILFE, PPIACO |
| Labor market | 10 | UNRATE, U6RATE, PAYEMS, JTSJOL, ICSA |
| GDP and growth | 10 | GDP, GDPC1, GDPPOT, INDPRO, TCU, DGORDER |
| Consumer | 6 | RETAILSMNSA, PCE, DSPIC96, PSAVERT |
| Credit and financial | 10 | BAMLH0A0HYM2, BAMLH0A3HYM2, MORTGAGE30US, LOANS, DRSFRMACBS |
| Housing | 12 | HOUST, PERMIT, HSN1F, CSUSHPISA |
| Money supply | 8 | M1SL, M2SL, M2V, BOGMBASE, WALCL, TOTRESNS, WTREGEN |
| Trade and FX | 13 | DEXUSEU, DEXJPUS, DEXCHUS, DTWEXBGS, BOPTEXP |
| Energy and commodities | 6 | DCOILWTICO, DCOILBRENTEU, DHHNGSP, GOLDAMGBD228NLBM |
| **Total** | | **~108** |

---

## Roadmap

### Near-term
- **NASDAQ Trader file integration** — replace Wikipedia scraping with the official NASDAQ trader file (`nasdaqtrader.com/dynamic/SymbolDirectory/nasdaqtraded.txt`) to cover all ~8,000–9,000 US-listed securities. Adds OTC, micro-cap, and newly-listed stocks that don't appear in the S&P 1500. Filterable by security type (common stock vs ETF vs preferred), exchange, and test-issue flag. Expected DAG runtime ~80 minutes with current batch/delay settings.
- **Historical valuation ratios** — dbt model computing trailing PE, P/B, P/S, dividend yield, and beta from existing `fact_daily_prices` x `fact_fundamentals` join, using point-in-time financial statement dates to avoid look-ahead bias. Extends ratio history back to 2010 without any new data sources.
- **dbt models for supplemental tables** — staging and mart models for `DIVIDENDS_AND_SPLITS`, `EARNINGS_HISTORY`, `ANALYST_RECOMMENDATIONS`, and `ANALYST_PRICE_TARGETS`.

### Future
- **Options data** — open interest, implied volatility surface from yfinance or a dedicated provider
- **Insider transactions** — SEC Form 4 filings via EDGAR API
- **Short interest** — FINRA-reported short volume and short interest ratio
- **International equities** — major index components from LSE, TSX, ASX via yfinance (ticker suffix routing: `.L`, `.TO`, `.AX`)
