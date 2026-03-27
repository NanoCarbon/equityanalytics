# Equity Analytics Pipeline

A production-style ELT pipeline and AI-powered analytics application built as a portfolio project for data engineering roles in financial services.

## Overview

This project ingests public equity and ETF data, models it into a Kimball dimensional warehouse, and exposes it through a natural language chat interface that generates SQL queries and interactive charts on demand.

## Architectureyfinance API
↓
Python ingestion (Prefect orchestrated)
↓
Snowflake RAW schema (landing zone)
↓
dbt transformations (staging → intermediate → marts)
↓
Streamlit + Claude API (natural language → SQL → chart)

# Equity Analytics Pipeline

A production-style ELT pipeline and AI-powered analytics application built as a portfolio project for data engineering roles in financial services.

## Overview

This project ingests public equity and ETF data, models it into a Kimball dimensional warehouse, and exposes it through a natural language chat interface that generates SQL queries and interactive charts on demand.

## Architecture
```
yfinance API
     ↓
Python ingestion (Prefect orchestrated)
     ↓
Snowflake RAW schema (landing zone)
     ↓
dbt transformations (staging → intermediate → marts)
     ↓
Streamlit + Claude API (natural language → SQL → chart)
```

## Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python + yfinance | Extract OHLCV prices and company metadata |
| Orchestration | Prefect Cloud | Scheduling, retries, observability |
| Warehouse | Snowflake | Three-schema ELT architecture |
| Transformation | dbt Cloud | Kimball dimensional modeling |
| Quality | dbt tests + GitHub Actions | Automated data quality on every PR |
| Application | Streamlit + Claude API | Natural language analytics interface |

## Data Pipeline

### Ingestion
Two Prefect tasks extract data from yfinance for 20 tickers — equities, financials, and ETFs — covering one year of daily OHLCV prices and company metadata. Data lands in `EQUITY_ANALYTICS.RAW` with full metadata lineage via `extracted_at` timestamps.

### Transformation
dbt models follow a strict three-tier architecture:

**Staging** — one model per source table. Cleans types, renames columns, handles nulls. No business logic.
- `stg_prices` — converts Unix nanosecond timestamps to dates, casts price fields
- `stg_companies` — standardizes sector/industry, coalesces nulls

**Intermediate** — reusable business logic building blocks.
- `int_daily_returns` — calculates daily returns, 30-day annualized rolling volatility, and 52-week high/low using window functions

**Marts** — Kimball dimensional model, consumption-ready.
- `dim_security` — one row per ticker with company metadata
- `dim_date` — calendar dimension spanning 2020 to present
- `fact_daily_prices` — grain: ticker + trading day, joins all dimensions

### Data Quality
Eight automated dbt tests assert:
- `fact_daily_prices.ticker` and `price_date` are never null
- `fact_daily_prices.close_price` and `daily_return` are never null
- `dim_security.ticker` is unique and not null
- `dim_date.date_key` is unique and not null

### CI/CD
GitHub Actions runs `dbt build` on every pull request that touches dbt files. Each PR gets an isolated Snowflake CI schema (`CI_{pr_number}`) so runs never interfere with production. PRs cannot merge if any model or test fails.

## Analytics Application

A Streamlit chat interface powered by Claude accepts natural language prompts, generates Snowflake SQL, executes it, and renders interactive Plotly charts.

Example prompts:
- "Compare cumulative returns for SPY, QQQ and IWM over the last year"
- "Show me the 30-day rolling volatility for AAPL, MSFT and GOOGL"
- "Which sector had the highest average daily return last month?"
- "Show me tickers trading closest to their 52-week high"
- "Compare JPM and GS closing prices over the last 6 months"

The application uses a two-step LLM call — first to generate SQL from the natural language prompt with full schema context, second to determine the optimal chart type and axis mappings from the returned data structure.

## Key Design Decisions

**Three-schema architecture (RAW / STAGING / MARTS)**
Separates concerns so a broken transformation never corrupts source data. RAW is append-only and immutable. Transformations can always be replayed from RAW.

**Kimball dimensional modeling**
Fact and dimension tables follow Kimball conventions — surrogate keys, conformed dimensions, declared grain. This makes the mart layer intuitive for analysts and optimized for aggregation queries.

**Prefect orchestration over cron**
Wrapping ingestion in Prefect tasks adds retries, observability, and scheduling without changing the core Python logic. Each task reports state independently — a failure in `load-prices` doesn't mask a success in `extract-prices`.

**Key pair authentication for dbt Cloud**
Snowflake's MFA policy blocks password-based programmatic access. Key pair auth is the production-grade alternative — the private key never travels over a network, it signs a challenge locally. GitHub Actions stores the private key as an encrypted secret and injects it at runtime.

**Isolated CI schemas**
Each GitHub Actions run builds into `CI_{pr_number}` rather than a shared schema. This means concurrent CI runs never interfere and production schemas are never touched by automated tests.

**Nanosecond timestamp handling**
yfinance returns pandas timestamps as Unix nanoseconds. Snowflake's `write_pandas` stores these as `NUMBER(38,0)`. The staging layer converts via `to_date(dateadd(second, date / 1000000000, '1970-01-01'))`. In production this would be handled at ingestion time with explicit type casting before load.

## Repository Structure
```
equityanalytics/
├── ingestion/
│   ├── extract.py          # yfinance extraction logic
│   ├── load.py             # Snowflake bulk loading
│   └── pipeline.py         # Prefect flow definition
├── dbt_project/
│   └── models/
│       ├── staging/        # Source cleaning layer
│       ├── intermediate/   # Business logic layer
│       └── marts/          # Consumption layer
├── agents/
│   └── chart_agent.py      # Streamlit + Claude chat app
├── .github/
│   └── workflows/
│       └── dbt_ci.yml      # GitHub Actions CI pipeline
├── deploy.py               # Prefect deployment (gitignored)
├── dbt_project.yml         # dbt project configuration
└── requirements.txt
```

## Setup

### Prerequisites
- Python 3.11+
- Snowflake account
- Prefect Cloud account (free tier)
- dbt Cloud account (free tier)
- Anthropic API key

### Environment Variables
Copy `.env.example` to `.env` and fill in your values:
```
SNOWFLAKE_USER=your_snowflake_username
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_TOKEN=your_programmatic_access_token
ANTHROPIC_API_KEY=your_anthropic_key
```

### Running the Pipeline
```bash
pip install -r requirements.txt

# Start the Prefect serving process
python deploy.py

# Trigger a manual run
prefect deployment run equity-ingestion-pipeline/daily-equity-ingestion
```

### Running the Analytics App
```bash
streamlit run agents/chart_agent.py
```

## Tickers Covered

| Category | Tickers |
|---|---|
| Technology | AAPL, MSFT, GOOGL, AMZN, META |
| Financials | JPM, GS, MS, BLK, BX |
| Diversified | BRK-B, V, MA, HD, UNH |
| ETFs | SPY, QQQ, IWM, VTI, AGG |