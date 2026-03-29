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
  Prefect orchestrated        Prefect orchestrated         Prefect orchestrated
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
| Orchestration | Prefect Cloud | Scheduling, retries, observability |
| Warehouse | Snowflake | Three-schema ELT architecture |
| Transformation | dbt Cloud | Kimball dimensional modeling |
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

## Warehouse Structure

```
EQUITY_ANALYTICS
├── RAW
│   ├── PRICES               — daily OHLCV, 616 tickers, incremental append
│   ├── COMPANY_INFO         — company metadata, overwrite on each run
│   ├── MACRO_INDICATORS     — 95 FRED series, overwrite on each run
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

### Ingestion Layer

Three independent Prefect pipelines:

**`equity_pipeline`** — daily weekdays 9am
- Fetches current S&P 500 components dynamically from Wikipedia
- Bulk price download for all 616 tickers in a single yfinance call
- Per-ticker metadata extraction with 2-second rate limiting delay
- Incremental loads — checks max loaded date and only extracts new rows
- Appends to `RAW.PRICES`, overwrites `RAW.COMPANY_INFO`

**`macro_pipeline`** — daily weekdays 9am
- 95 series fetched with graceful error handling for invalid series IDs
- Overwrites `RAW.MACRO_INDICATORS` on each run

**`fundamentals_pipeline`** — weekly Saturday 10am + daily valuation snapshot weekdays 9am
- Financial statements: full overwrite of `RAW.FINANCIAL_STATEMENTS` (catches restatements)
- Valuation metrics: daily append to `RAW.VALUATION_METRICS` (builds time series)
- ETFs filtered out for statement extraction — no 10-K filings

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

**Key pair authentication throughout**
Snowflake's MFA policy blocks password-based programmatic access. RSA key pair auth is used for dbt Cloud and GitHub Actions — the private key never travels over a network. Keys are stored as encrypted GitHub Secrets and injected at runtime.

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
│   ├── extract_fred.py           # FRED API extraction — 95 series
│   ├── extract_fundamentals.py   # Financial statements (EAV) + valuation metrics
│   ├── load.py                   # Snowflake bulk loading, get_max_date, get_min_date
│   ├── pipeline.py               # Prefect flows: equity, macro, backfill
│   └── pipeline_fundamentals.py  # Prefect flows: fundamentals test, backfill, weekly, daily valuation
├── dbt_project/
│   ├── models/
│   │   ├── staging/              # stg_prices, stg_companies, stg_macro_indicators,
│   │   │                         #   stg_financial_statements, stg_valuation_metrics
│   │   ├── intermediate/         # int_daily_returns, int_fundamentals_pivoted
│   │   └── marts/                # dim_date, dim_security, fact_daily_prices,
│   │                             #   fact_macro_readings, fact_fundamentals, fact_valuation_snapshot
│   ├── tests/                    # Singular business rule tests
│   └── macros/                   # generate_schema_name
├── agents/
│   ├── chart_agent.py            # Streamlit + Claude chat application
│   └── code_reviewer.py          # AI code review agent
├── .github/
│   └── workflows/
│       ├── dbt_ci.yml            # dbt build + test on every PR, prod deploy on merge
│       └── code_review.yml       # AI code review comment on every PR
├── deploy.py                     # Prefect deployment registration (gitignored in practice)
├── dbt_project.yml               # dbt project config
├── requirements.txt
├── .env.example
└── CONTEXT.md                    # Project state reference
```

---

## Setup

### Prerequisites
- Python 3.11+
- Snowflake account (free 30-day trial sufficient)
- Prefect Cloud account (free tier — note: 5 deployment limit)
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

### Running the Daily Pipelines

```bash
pip install -r requirements.txt

# Start Prefect — registers and serves all scheduled pipelines
python deploy.py

# Trigger manual runs (in a second terminal)
prefect deployment run 'equity-ingestion-pipeline/daily-equity-ingestion'
prefect deployment run 'macro-ingestion-pipeline/daily-macro-ingestion'
prefect deployment run 'fundamentals-ingestion-pipeline/weekly-fundamentals-ingestion'
```

**Note:** Prefect free tier has a 5-deployment limit. If you hit it, run flows directly (see Backfill section below) or delete unused deployments in the Prefect Cloud UI before re-registering.

### Running dbt

In dbt Cloud IDE or locally with dbt Core:

```bash
dbt build                                                    # all models and tests
dbt test                                                     # tests only
dbt build --select staging                                   # staging layer only
dbt build --select fact_daily_prices --full-refresh          # force full rebuild of prices
dbt build --select +fact_fundamentals +fact_valuation_snapshot --full-refresh  # fundamentals full rebuild
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

**Option A — via Prefect deployment (if under the 5-deployment limit):**
```bash
python deploy.py
prefect deployment run 'backfill-pipeline/historical-backfill'
```

**Option B — direct invocation (bypasses deployment registry):**
```bash
python -c "
from dotenv import load_dotenv; load_dotenv()
from ingestion.pipeline import backfill_pipeline
backfill_pipeline(start_date='2010-01-01', batch_size=50, batch_delay_seconds=30)
"
```

After the backfill completes, run a full-refresh dbt build to propagate all historical rows through the transformation layer — the incremental filter would otherwise skip them:
```bash
dbt build --select fact_daily_prices --full-refresh
```

### Fundamentals Backfill (all ~500 equity tickers)

Loads financial statements and valuation metrics for the full equity universe. yfinance returns ~4 years annual + ~8 quarters per ticker, so the total dataset is bounded (~580K statement rows).

**Direct invocation (recommended — avoids deployment limit):**
```bash
python -c "
from dotenv import load_dotenv; load_dotenv()
from ingestion.pipeline_fundamentals import fundamentals_backfill_pipeline
fundamentals_backfill_pipeline(batch_size=50, batch_delay_seconds=30, delay_seconds=2.0)
"
```

Progress logs directly to your terminal. Expect roughly 30-45 minutes total. Failed batches are logged by name — re-running the flow is safe and won't create duplicates.

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
dbt build --select +fact_fundamentals +fact_valuation_snapshot --full-refresh
```

### Adding New Tickers

When expanding the ticker universe beyond the current 616:

```bash
# 1. Run the equity pipeline to load new prices and metadata
prefect deployment run 'equity-ingestion-pipeline/daily-equity-ingestion'

# 2. Full-refresh fact_daily_prices — the incremental filter
#    (price_date > max existing) misses historical rows for brand new tickers
dbt build --select fact_daily_prices --full-refresh

# 3. Run the fundamentals pipeline for statements
prefect deployment run 'fundamentals-ingestion-pipeline/weekly-fundamentals-ingestion'

# 4. Full-refresh fact_fundamentals for the same reason
dbt build --select +fact_fundamentals --full-refresh
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