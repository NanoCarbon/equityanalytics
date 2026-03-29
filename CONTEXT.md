# Equity Analytics Pipeline — Project Context

## Stack
- Python + yfinance + Prefect Cloud (ingestion)
- Snowflake (warehouse) — account: QYCMQJK-HTC96121, user: EDWARDWLIN
- dbt Cloud (transformation) — connected to GitHub NanoCarbon/equityanalytics
- Streamlit + Claude API (chat app)
- GitHub Actions (CI/CD)

## Warehouse structure
EQUITY_ANALYTICS
├── RAW — PRICES, COMPANY_INFO, MACRO_INDICATORS
├── STAGING — STG_PRICES, STG_COMPANIES, STG_MACRO_INDICATORS, INT_DAILY_RETURNS (views)
└── MARTS — DIM_DATE, DIM_SECURITY, FACT_DAILY_PRICES, FACT_MACRO_READINGS (tables)

## Data coverage
- 616 tickers (S&P 500 + top ETFs)
- 95 FRED macro series
- Daily incremental loads via Prefect
- Backfill planned to 2010-01-01

## Key files
- ingestion/extract.py — yfinance extraction, S&P 500 scraper
- ingestion/extract_fred.py — FRED API extraction
- ingestion/load.py — Snowflake bulk loading, get_max_date/get_min_date
- ingestion/pipeline.py — Prefect flows (equity, macro, backfill)
- agents/chart_agent.py — Streamlit + Claude chat app
- deploy.py — Prefect serve (gitignored)
- dbt_project.yml — dbt project config, profile: equity_analytics

## Auth
- Snowflake: programmatic access token in .env as SNOWFLAKE_TOKEN
- dbt Cloud: RSA key pair auth (keys gitignored)
- GitHub Actions secrets: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY, ANTHROPIC_API_KEY

## Known issues / decisions
- dbt-fusion 2.0 preview ignores generate_schema_name macro
- Workaround: {{ config(schema='marts') }} in each mart model directly
- Snowflake DATE column stored as nanosecond timestamps (NUMBER)
- Conversion: to_date(dateadd(second, date / 1000000000, '1970-01-01'))
- flow.serve() used instead of work pool (free Prefect tier limitation)

## Current status
- Daily incremental pipeline working
- 19 dbt tests passing
- GitHub Actions CI/CD on PRs + merge to main
- AI code review agent posting PR comments
- Streamlit app deployed (or deploying) to Streamlit Community Cloud
- Backfill to 2010 planned — code built, not yet run