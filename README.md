# Equity Analytics Pipeline

A production-style ELT pipeline and AI-powered analytics application built as a portfolio project for data engineering roles in financial services. Ingests the full S&P 500 universe plus top ETFs and 95 Federal Reserve macro indicators, models them into a Kimball dimensional warehouse, and exposes the data through a natural language chat interface that generates SQL and interactive charts on demand.

## Live Demo

[equity-analytics.streamlit.app](https://equity-analytics.streamlit.app)

---

## Architecture

```
S&P 500 + ETF prices          FRED macro indicators
     (yfinance)                    (FRED API)
          ↓                             ↓
  Python ingestion            Python ingestion
  Prefect orchestrated        Prefect orchestrated
          ↓                             ↓
       Snowflake RAW schema (append-only landing zone)
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
| Ingestion | Python + yfinance | S&P 500 + ETF OHLCV prices and company metadata |
| Ingestion | Python + FRED API | 95 macro economic indicators |
| Orchestration | Prefect Cloud | Scheduling, retries, observability |
| Warehouse | Snowflake | Three-schema ELT architecture |
| Transformation | dbt Cloud | Kimball dimensional modeling |
| Quality | dbt tests + GitHub Actions | 19 automated tests on every PR |
| AI Code Review | Claude API + GitHub Actions | Automated PR code review comments |
| Application | Streamlit + Claude API | Natural language analytics interface |

---

## Data Coverage

### Equities
- **616 tickers** — full S&P 500 components + top 100 ETFs by AUM and liquidity
- Daily OHLCV prices with incremental loads — only new trading days are extracted on each run
- Company metadata: sector, industry, market cap

### Macro Indicators (FRED)
95 series across 11 categories:
- Interest rates and yield curve (DFF, DGS2, DGS10, T10Y2Y, T10Y3M...)
- Inflation (CPI, Core CPI, PCE, Core PCE, PPI...)
- Labor market (UNRATE, U6RATE, PAYEMS, JOLTS, jobless claims...)
- GDP and growth (GDP, Real GDP, industrial production, retail sales...)
- Credit and financial conditions (HY spread, IG spread, TED spread, mortgage rates...)
- Housing (housing starts, building permits, Case-Shiller, existing home sales...)
- Money supply (M1, M2, monetary base...)
- Energy and commodities (WTI, Brent, natural gas, gasoline...)
- FX rates (USD/EUR, USD/JPY, USD/GBP, USD/CNY, USD/CAD...)
- Market indicators (VIX, NASDAQ, S&P 500 index, Wilshire 5000...)
- Consumer and sentiment (UMich sentiment, durable goods, consumer credit...)

---

## Pipeline Architecture

### Ingestion Layer

Two independent Prefect pipelines run on a daily schedule:

**`equity_pipeline`** — pulls price and metadata from yfinance
- Fetches current S&P 500 components dynamically from Wikipedia
- Bulk price download for all 616 tickers in a single yfinance call
- Per-ticker metadata extraction with 2-second rate limiting delay
- Incremental loads — checks max loaded date and only extracts new rows
- Appends to `RAW.PRICES` and overwrites `RAW.COMPANY_INFO`

**`macro_pipeline`** — pulls economic indicators from FRED API
- 95 series fetched with graceful error handling for invalid series IDs
- Overwrites `RAW.MACRO_INDICATORS` on each run

### Transformation Layer

dbt models follow a strict three-tier architecture:

**Staging** — one model per source. Cleans types, renames columns, handles nulls. No business logic.
- `stg_prices` — converts Unix nanosecond timestamps to dates, casts price fields
- `stg_companies` — standardizes sector/industry, coalesces nulls to 'Unknown'
- `stg_macro_indicators` — converts timestamps, casts values

**Intermediate** — reusable business logic building blocks, not directly queried by consumers.
- `int_daily_returns` — daily return via LAG window function, 30-day annualized rolling volatility, 52-week high/low range

**Marts** — Kimball dimensional model, consumption-ready tables.
- `dim_security` — one row per ticker with company metadata, first/last trading date
- `dim_date` — calendar dimension spanning 2020 to present with fiscal labels
- `fact_daily_prices` — grain: ticker + trading day. Incremental materialization with `unique_key=['ticker', 'price_date']`
- `fact_macro_readings` — grain: series_id + observation_date. Incremental materialization

### Data Quality

19 automated dbt tests across three layers:

**Staging tests (7)** — not_null on critical columns, unique ticker in company model

**Mart tests (8)** — not_null on all fact columns, unique + not_null on dimension keys, referential integrity

**Singular business rule tests (4)**
- `assert_no_future_prices` — no price dates beyond today
- `assert_return_bounds` — no daily return exceeding ±50%
- `assert_no_negative_prices` — no close price ≤ 0
- `assert_no_negative_volume` — no volume < 0

### CI/CD

Two GitHub Actions workflows:

**`dbt_ci.yml`** — triggers on every PR touching dbt files
- Installs dbt-snowflake and authenticates via RSA key pair stored as GitHub Secret
- Builds into an isolated `CI_{pr_number}` Snowflake schema — concurrent runs never interfere
- On merge to main, runs with `target: prod` writing directly to MARTS
- PRs cannot merge if any model or test fails

**`code_review.yml`** — triggers on every PR
- Walks the repo, collects all Python, SQL, and YAML files
- Sends to Claude API with a senior financial data engineer system prompt
- Posts full code review as a PR comment with critical issues, warnings, and suggestions

---

## Analytics Application

A Streamlit chat interface powered by Claude accepts natural language prompts, generates Snowflake SQL against the mart layer, executes it, and renders interactive Plotly charts.

**Two-step LLM pipeline:**
1. Claude generates Snowflake SQL from the natural language prompt using full schema context
2. Claude determines optimal chart type and axis mappings from the returned DataFrame structure

**Example prompts:**
- "Compare cumulative returns for SPY, QQQ and IWM over the last year"
- "Show me 30-day rolling volatility for AAPL, MSFT and GOOGL"
- "Which sector had the highest average daily return last month?"
- "Show me tickers trading closest to their 52-week high"
- "How did SPY perform during periods when the yield curve was inverted?"
- "Compare SPY daily returns against the Fed funds rate over the last year"
- "Show me the Fed funds rate trend alongside the S&P 500 index"

Cross-dataset questions combining equity prices and macro indicators are fully supported since both datasets share the same Snowflake database.

---

## Key Design Decisions

**Three-schema warehouse architecture (RAW / STAGING / MARTS)**
RAW is append-only and immutable — a broken transformation never corrupts source data. Transformations can always be replayed from RAW. Staging cleans without business logic. Marts are the only layer analysts and applications query.

**Kimball dimensional modeling over one big table**
Fact and dimension tables with declared grain make the mart layer intuitive for analysts and optimized for aggregation queries. The `unique_key` constraint on incremental fact models prevents duplicate rows even if ingestion overlaps.

**Incremental loads over full refresh**
Daily runs check `MAX(price_date)` already in Snowflake and only extract newer trading days. A full year of history for 616 tickers is ~155,000 rows — no reason to reload it daily. The dbt incremental model propagates only new rows through the transformation layer.

**Rate limiting for metadata extraction**
Price data uses yfinance bulk download — all 616 tickers in one request. Company metadata requires a per-ticker API call. A 2-second delay between metadata requests prevents Yahoo Finance rate limiting. Total metadata run time: ~20 minutes.

**Key pair authentication throughout**
Snowflake's MFA policy blocks password-based programmatic access. RSA key pair auth is used for dbt Cloud and GitHub Actions — the private key never travels over a network, it signs a challenge locally. Keys are stored as encrypted GitHub Secrets and injected at runtime.

**Isolated CI schemas**
Each GitHub Actions run builds into `CI_{pr_number}` — a fresh, isolated Snowflake schema. Concurrent PR runs never interfere. Production MARTS schema is only written to on merge to main.

**Nanosecond timestamp conversion**
yfinance returns pandas timestamps as Unix nanoseconds. `write_pandas` stores these as `NUMBER(38,0)` in Snowflake. The staging layer converts via `to_date(dateadd(second, date / 1000000000, '1970-01-01'))`. In production this would be handled at ingestion time with explicit DDL before load.

**AI-powered code review on every PR**
A GitHub Actions workflow calls the Claude API with all modified files and posts a structured code review as a PR comment — critical issues, warnings, suggestions, and strengths. This makes code quality feedback automatic rather than dependent on reviewer availability.

---

## Repository Structure

```
equityanalytics/
├── ingestion/
│   ├── extract.py           # yfinance extraction — S&P 500 scraper, bulk download
│   ├── extract_fred.py      # FRED API extraction — 95 series
│   ├── load.py              # Snowflake bulk loading, get_max_date, get_min_date
│   └── pipeline.py          # Prefect flows: equity, macro, backfill
├── dbt_project/
│   ├── models/
│   │   ├── staging/         # stg_prices, stg_companies, stg_macro_indicators
│   │   ├── intermediate/    # int_daily_returns
│   │   └── marts/           # dim_date, dim_security, fact_daily_prices, fact_macro_readings
│   ├── tests/               # Singular business rule tests
│   └── macros/              # generate_schema_name
├── agents/
│   ├── chart_agent.py       # Streamlit + Claude chat application
│   └── code_reviewer.py     # AI code review agent
├── .github/
│   └── workflows/
│       ├── dbt_ci.yml       # dbt build + test on every PR, prod deploy on merge
│       └── code_review.yml  # AI code review comment on every PR
├── dbt_project.yml          # dbt project config, profile: equity_analytics
├── requirements.txt
├── .env.example
└── CONTEXT.md               # Project state reference for onboarding
```

---

## Setup

### Prerequisites
- Python 3.11+
- Snowflake account (free 30-day trial sufficient)
- Prefect Cloud account (free tier)
- dbt Cloud account (free Developer tier)
- Anthropic API key
- FRED API key (free at fred.stlouisfed.org)

### Environment Variables
Copy `.env.example` to `.env`:

```
SNOWFLAKE_USER=your_snowflake_username
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_TOKEN=your_programmatic_access_token
ANTHROPIC_API_KEY=your_anthropic_key
FRED_API_KEY=your_fred_api_key
```

### Snowflake Setup
Run in a Snowflake worksheet:

```sql
CREATE WAREHOUSE TRANSFORM_WH WAREHOUSE_SIZE='X-SMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE;
CREATE DATABASE EQUITY_ANALYTICS;
CREATE SCHEMA EQUITY_ANALYTICS.RAW;
CREATE SCHEMA EQUITY_ANALYTICS.STAGING;
CREATE SCHEMA EQUITY_ANALYTICS.MARTS;
```

### Running the Pipeline

```bash
pip install -r requirements.txt

# Start Prefect — serves both equity and macro pipelines
python deploy.py

# Trigger manual runs (in a second terminal)
prefect deployment run 'equity-ingestion-pipeline/daily-equity-ingestion'
prefect deployment run 'macro-ingestion-pipeline/daily-macro-ingestion'
```

### Running dbt

In dbt Cloud IDE or locally with dbt Core:

```bash
dbt build          # run all models and tests
dbt test           # tests only
dbt build --select staging    # staging layer only
dbt build --select fact_daily_prices --full-refresh  # force full rebuild
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
| Market indicators | 9 | VIXCLS, SP500, NASDAQCOM, WILL5000PR |
| **Total** | | **~95** |