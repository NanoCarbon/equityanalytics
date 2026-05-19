# Equity Analytics Pipeline

A production-style ELT pipeline and AI-powered analytics application built as a portfolio project for data engineering roles in financial services. Ingests the full S&P 500 universe plus top ETFs, 95 Federal Reserve macro indicators, and complete fundamental financial data (income statements, balance sheets, cash flow, and valuation metrics), models them into a Kimball dimensional warehouse, and exposes the data through a natural language chat interface that generates SQL and interactive charts on demand.

## Live Demo

[equity-analytics.streamlit.app](https://equity-analytics.streamlit.app)

---

## Architecture

```
S&P 500 + ETF prices          FRED macro indicators       Financial statements
     (yfinance)                    (FRED API)               + valuation metrics
          ↓                             ↓                       (yfinance)
  Python ingestion            Python ingestion             Python ingestion
  Airflow orchestrated        Airflow orchestrated         Airflow orchestrated
          ↓                             ↓                            ↓
       Snowflake RAW schema (append-only / overwrite landing zone)
                        ↓
            dbt transformations
       staging → intermediate → marts
                        ↓
          Snowflake MARTS schema
       Kimball dimensional model
                        ↓
       Streamlit + Claude API
   natural language → SQL → chart
```

---

## Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python + yfinance | S&P 500 + ETF OHLCV prices, company metadata, financial statements, valuation metrics |
| Ingestion | Python + FRED API | 95 macro economic indicators |
| Orchestration | Apache Airflow 2.9.3 on AWS EC2 | Scheduling, retries, observability — always-on cloud server |
| Warehouse | Snowflake | Three-schema ELT architecture |
| Transformation | dbt Core | Kimball dimensional modeling |
| Quality | dbt tests + GitHub Actions | 25+ automated tests on every PR |
| AI Code Review | Claude API + GitHub Actions | Automated PR code review comments |
| Application | Streamlit + Claude API | Natural language analytics interface |

---

## Data Coverage

### Equities
- **616 tickers** — full S&P 500 components + top 100 ETFs by AUM and liquidity
- Daily OHLCV prices with incremental loads — only new trading days are extracted on each run
- Company metadata: sector, industry, market cap

### Fundamentals
- **Financial statements** — income statement, balance sheet, cash flow for ~500 S&P 500 equities (ETFs excluded)
  - ~4 years annual + ~8 quarters per ticker from yfinance
  - EAV format in RAW/staging, pivoted to ~35 named columns in marts
  - Full overwrite weekly to catch retroactive restatements
- **Valuation metrics** — 37 point-in-time fields per ticker per day (PE, P/B, margins, growth rates, dividends, beta, etc.)
  - All 616 tickers including ETFs
  - Daily append builds a time series of how ratios evolve

### Macro Indicators (FRED)
90 series across 11 categories (5 premium series removed — SP500, NASDAQCOM, DJIA, WILL5000PR, NIKKEI225 require a paid FRED subscription):
- Interest rates and yield curve (DFF, DGS2, DGS10, T10Y2Y, T10Y3M...)
- Inflation (CPI, Core CPI, PCE, Core PCE, PPI...)
- Labor market (UNRATE, U6RATE, PAYEMS, JOLTS, jobless claims...)
- GDP and growth (GDP, Real GDP, industrial production, retail sales...)
- Credit and financial conditions (HY spread, IG spread, TED spread, mortgage rates...)
- Housing (housing starts, building permits, Case-Shiller, existing home sales...)
- Money supply (M1, M2, monetary base...)
- Energy and commodities (WTI, Brent, natural gas, gasoline...)
- FX rates (USD/EUR, USD/JPY, USD/GBP, USD/CNY, USD/CAD...)
- Consumer and sentiment (UMich sentiment, durable goods, consumer credit...)

---

## Warehouse Structure

```
EQUITY_ANALYTICS
├── RAW
│   ├── PRICES               — daily OHLCV, 616 tickers, incremental append
│   ├── COMPANY_INFO         — company metadata, overwrite on each run
│   ├── MACRO_INDICATORS     — 90 FRED series, overwrite on each run
│   ├── FINANCIAL_STATEMENTS — EAV format (income/balance/cashflow), weekly overwrite
│   └── VALUATION_METRICS    — point-in-time ratios, daily append
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

Pipelines run on **Apache Airflow 2.9.3** deployed via Docker Compose on an AWS EC2 instance (t3.small, us-east-2). The Airflow UI is accessible at `http://<ec2-ip>:8080`. All DAGs use the TaskFlow API (`@dag` / `@task` decorators) with LocalExecutor.

### Airflow DAGs

**`equity_daily`** — schedule `0 14 * * 1-5` (9am ET weekdays)
- Fetches current S&P 500 components dynamically from Wikipedia
- Checks max loaded date and extracts only new trading days
- Bulk price download for all 616 tickers in a single yfinance call
- Per-ticker metadata extraction with rate limiting
- Appends to `RAW.PRICES`, overwrites `RAW.COMPANY_INFO`

**`macro_daily`** — schedule `0 14 * * 1-5` (9am ET weekdays)
- 90 FRED series fetched with graceful error handling
- Overwrites `RAW.MACRO_INDICATORS` on each run

**`fundamentals_weekly`** — schedule `0 15 * * 6` (10am ET Saturdays)
- Financial statements: full overwrite of `RAW.FINANCIAL_STATEMENTS` (catches restatements)
- ETFs filtered out — no 10-K filings

**`valuation_daily`** — schedule `0 14 * * 1-5` (9am ET weekdays)
- Valuation metrics: daily append to `RAW.VALUATION_METRICS` (builds time series)

**`backfill_prices`** — `schedule=None` (manual trigger only)
- Loads historical OHLCV back to 2010-01-01 for all 616 tickers
- Batches of 50 tickers with 30-second delays between batches

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
- `assert_return_bounds` — no daily return exceeding ±50%
- `assert_no_negative_prices` — no close price ≤ 0
- `assert_no_negative_volume` — no volume < 0
- `assert_no_future_fundamentals` — no period_end_date beyond today
- `assert_no_negative_revenue` — no total_revenue < 0
- `assert_no_negative_assets` — no total_assets < 0

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
Daily runs check `MAX(price_date)` already in Snowflake and only extract newer trading days. A full year of history for 616 tickers is ~155,000 rows — no reason to reload it daily. The dbt incremental model propagates only new rows through the transformation layer.

**Financial statements as full overwrite, valuation metrics as append**
yfinance returns a fixed ~4yr/8Q window for financial statements, and values are subject to retroactive restatement. Full overwrite is the correct strategy — the data is small (~580K rows) and this guarantees the warehouse reflects the current reported numbers. Valuation metrics (PE, margins, etc.) are point-in-time and never retroactively corrected, so daily append is correct and builds a queryable time series.

**EAV → pivot pattern for financial statements**
yfinance returns ~276 unique line items with spaced names (e.g. "Total Revenue", "Net Income"). Storing as EAV in RAW is resilient to schema drift — new line items from yfinance don't break the load. The intermediate pivot model selects the ~35 most analytically useful fields by name and computes derived margins in a separate CTE to work around Snowflake's GROUP BY + CASE expression nesting restrictions.

**Rate limiting for metadata and fundamentals extraction**
Price data uses yfinance bulk download — all 616 tickers in one request. Company metadata, financial statements, and valuation metrics require per-ticker API calls. A 2-second delay between tickers prevents Yahoo Finance rate limiting.

**RSA key-pair authentication (never expires)**
All Snowflake connections use RSA key-pair auth — the connector receives DER-encoded private key bytes loaded from a `.pem` file via the `cryptography` library. Unlike programmatic access tokens, RSA keys never expire. The public key is registered in Snowflake once; the private key is stored locally (gitignored) and as a GitHub Secret for CI.

**Airflow on EC2 (always-on orchestration)**
Moving from a laptop-dependent Prefect setup to Airflow running in Docker on an EC2 instance means pipelines execute on schedule whether or not a local machine is running. The EC2 security group restricts port 22 (SSH) and port 8080 (Airflow UI) to a known IP.

**Isolated CI schemas**
Each GitHub Actions run builds into `CI_{pr_number}` — a fresh, isolated Snowflake schema. Concurrent PR runs never interfere. Production MARTS schema is only written to on merge to main.

**AI-powered code review on every PR**
A GitHub Actions workflow calls the Claude API with all modified files and posts a structured code review as a PR comment — critical issues, warnings, suggestions, and strengths. Files exceeding the review limit are truncated with a visible marker rather than silently dropped.

---

## Repository Structure

```
equityanalytics/
├── ingestion/
│   ├── extract.py                # yfinance extraction — S&P 500 scraper, bulk price download
│   ├── extract_fred.py           # FRED API extraction — 90 series
│   ├── extract_fundamentals.py   # Financial statements (EAV) + valuation metrics
│   └── load.py                   # Snowflake bulk loading, get_max_date, get_min_date
├── airflow/
│   ├── dags/
│   │   ├── dag_equity_daily.py       # equity_daily DAG — prices + company info
│   │   ├── dag_macro_daily.py        # macro_daily DAG — FRED series
│   │   ├── dag_fundamentals.py       # fundamentals_weekly + valuation_daily DAGs
│   │   └── dag_backfill.py           # backfill_prices DAG — manual trigger only
│   ├── logs/                         # Airflow task logs (gitignored)
│   └── plugins/                      # Custom operators (future)
├── dbt_project/
│   ├── models/
│   │   ├── staging/              # stg_prices, stg_companies, stg_macro_indicators,
│   │   │                         #   stg_financial_statements, stg_valuation_metrics
│   │   ├── intermediate/         # int_daily_returns, int_fundamentals_pivoted
│   │   └── marts/                # dim_date, dim_security, fact_daily_prices,
│   │                             #   fact_macro_readings, fact_fundamentals, fact_valuation_snapshot
│   ├── tests/                    # Singular business rule tests
│   └── macros/                   # generate_schema_name
├── app/
│   └── db/
│       └── snowflake.py          # Snowflake connection + query helpers for the Streamlit app
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
- AWS EC2 instance (t3.small or larger) — or run Airflow locally
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
# First time only — initialize the database and create admin user
docker compose run --rm airflow-init

# Start webserver + scheduler (runs in background)
docker compose up -d airflow-webserver airflow-scheduler

# Airflow UI → http://localhost:8080
```

On EC2, replace `localhost` with your instance's public IP. Make sure port 8080 is open in your security group (restrict to your IP only).

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

One-time operations to populate historical data. Both backfills are idempotent — safe to re-run if interrupted.

### Price Backfill (2010 to present)

Loads ~2.4M rows of daily OHLCV data across 616 tickers. Batched in groups of 50 with 30-second delays between batches.

**Trigger via Airflow UI:**
1. Go to `http://<ec2-ip>:8080`
2. Enable the `backfill_prices` DAG
3. Click "Trigger DAG"

**Or trigger via CLI on the EC2 server:**
```bash
docker compose exec airflow-webserver airflow dags trigger backfill_prices
```

After the backfill completes, run a full-refresh dbt build to propagate all historical rows through the transformation layer — the incremental filter would otherwise skip them:
```bash
dbt build --profiles-dir . --select fact_daily_prices --full-refresh
```

### Fundamentals Backfill (all ~500 equity tickers)

Loads financial statements and valuation metrics for the full equity universe. yfinance returns ~4 years annual + ~8 quarters per ticker, so the total dataset is bounded (~580K statement rows).

**Trigger via Airflow UI:**
1. Enable and trigger the `fundamentals_weekly` DAG manually

**Or run directly:**
```bash
python -c "
from dotenv import load_dotenv; load_dotenv()
from ingestion.extract_fundamentals import extract_all_financial_statements
from ingestion.load import load_dataframe
# ... see dag_fundamentals.py for full task logic
"
```

Validate the load before running dbt:
```sql
SELECT COUNT(*), COUNT(DISTINCT ticker) FROM EQUITY_ANALYTICS.RAW.FINANCIAL_STATEMENTS;
SELECT COUNT(*), COUNT(DISTINCT ticker) FROM EQUITY_ANALYTICS.RAW.VALUATION_METRICS;

-- Spot check a single ticker
SELECT statement_type, frequency, COUNT(*)
FROM EQUITY_ANALYTICS.RAW.FINANCIAL_STATEMENTS
WHERE ticker = 'AAPL'
GROUP BY 1, 2 ORDER BY 1, 2;
```

After validation, run dbt with full-refresh to build all historical rows into the mart layer:
```bash
dbt build --profiles-dir . --select +fact_fundamentals +fact_valuation_snapshot --full-refresh
```

### Adding New Tickers

When expanding the ticker universe beyond the current 616:

```bash
# 1. Trigger equity_daily DAG to load new prices and metadata
docker compose exec airflow-webserver airflow dags trigger equity_daily

# 2. Full-refresh fact_daily_prices — the incremental filter
#    (price_date > max existing) misses historical rows for brand new tickers
dbt build --profiles-dir . --select fact_daily_prices --full-refresh

# 3. Trigger fundamentals_weekly DAG for statements
docker compose exec airflow-webserver airflow dags trigger fundamentals_weekly

# 4. Full-refresh fact_fundamentals for the same reason
dbt build --profiles-dir . --select +fact_fundamentals --full-refresh
```

---

## Tickers Covered

| Category | Examples | Count |
|---|---|---|
| S&P 500 components | AAPL, MSFT, GOOGL, NVDA, JPM, JNJ... | 503 |
| Broad market ETFs | SPY, IVV, VOO, QQQ, VTI, IWM | 15 |
| Fixed income ETFs | BND, AGG, TLT, IEF, LQD, HYG | 15 |
| International ETFs | VEA, VWO, EFA, EEM, EWJ, EWZ | 15 |
| Sector ETFs | XLF, XLK, XLV, XLE, XBI, VNQ | 15 |
| Commodity ETFs | GLD, IAU, SLV, USO, DBC | 15 |
| Factor / smart beta | MTUM, USMV, VLUE, QUAL, SCHD | 15 |
| Thematic ETFs | ARKK, BOTZ, ICLN, HACK, WCLD | 15 |
| **Total unique** | | **~616** |

---

## Macro Indicators Covered

| Category | Series | Examples |
|---|---|---|
| Interest rates | 10 | DFF, DGS2, DGS5, DGS10, DGS30 |
| Yield curve and real rates | 5 | T10Y2Y, T10Y3M, DFII5, DFII10 |
| Inflation | 7 | CPIAUCSL, CPILFESL, PCEPI, PCEPILFE, PPIACO |
| Labor market | 10 | UNRATE, U6RATE, PAYEMS, JTSJOL, ICSA |
| GDP and growth | 10 | GDP, GDPC1, INDPRO, TCU, DGORDER |
| Consumer | 6 | RETAILSMNSA, PCE, DSPIC96, PSAVERT |
| Credit and financial | 8 | BAMLH0A0HYM2, TEDRATE, MORTGAGE30US |
| Housing | 12 | HOUST, PERMIT, HSN1F, CSUSHPISA |
| Money supply | 5 | M1SL, M2SL, BOGMBASE |
| Trade and FX | 13 | DEXUSEU, DEXJPUS, DEXCHUS, BOPTEXP |
| Energy and commodities | 5 | DCOILWTICO, DCOILBRENTEU, DHHNGSP |
| **Total** | | **~90** |
