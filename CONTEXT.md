# Equity Analytics Pipeline — Project Context

## Stack
- Python + yfinance + Prefect Cloud (ingestion)
- Snowflake (warehouse) — account: QYCMQJK-HTC96121, user: EDWARDWLIN
- dbt Cloud (transformation) — connected to GitHub NanoCarbon/equityanalytics
- Streamlit + Claude API (chat app) — deployed to Streamlit Community Cloud
- GitHub Actions (CI/CD + AI code review)

## Warehouse structure
```
EQUITY_ANALYTICS
├── RAW
│   ├── PRICES (table) — 616 tickers, daily OHLCV, incremental append
│   ├── COMPANY_INFO (table) — 616 tickers, metadata, overwrite on each run
│   └── MACRO_INDICATORS (table) — 95 FRED series, overwrite on each run
├── STAGING (views)
│   ├── STG_PRICES
│   ├── STG_COMPANIES
│   ├── STG_MACRO_INDICATORS
│   └── INT_DAILY_RETURNS
└── MARTS (tables)
    ├── DIM_DATE
    ├── DIM_SECURITY
    ├── FACT_DAILY_PRICES
    └── FACT_MACRO_READINGS
```

## Data coverage
- 616 tickers — full S&P 500 (scraped live from Wikipedia) + top ETFs
- 95 FRED macro series across 11 categories
- Daily incremental loads via Prefect (equity + macro pipelines)
- Backfill to 2010-01-01 planned — code built in pipeline.py, not yet run

## Prefect deployments
Both flows served via `deploy.py` (gitignored):
- `equity-ingestion-pipeline/daily-equity-ingestion` — 9am weekdays
- `macro-ingestion-pipeline/daily-macro-ingestion` — 9am weekdays

Trigger manually:
```bash
prefect deployment run 'equity-ingestion-pipeline/daily-equity-ingestion'
prefect deployment run 'macro-ingestion-pipeline/daily-macro-ingestion'
```

## dbt models
- 6 models total: stg_prices, stg_companies, stg_macro_indicators, 
  int_daily_returns, fact_daily_prices, fact_macro_readings, 
  dim_date, dim_security
- 19 tests passing (7 staging, 8 marts, 4 singular business rules)
- Incremental materialization on fact_daily_prices and fact_macro_readings
- unique_key=['ticker', 'price_date'] on fact_daily_prices

## Key operational patterns
- Adding new tickers to the universe requires `dbt build --select fact_daily_prices --full-refresh`
  because the incremental filter (price_date > max existing) excludes historical rows for new tickers
- Daily incremental runs work correctly for existing tickers
- dbt-fusion 2.0 preview ignores generate_schema_name macro
  Workaround: {{ config(schema='marts') }} in each mart model file directly
- Snowflake DATE column stored as nanosecond Unix timestamps (NUMBER(38,0))
  Conversion: to_date(dateadd(second, date / 1000000000, '1970-01-01'))
- flow.serve() used instead of work pool (Prefect free tier blocks custom work pools)

## Auth
- Snowflake: programmatic access token in .env as SNOWFLAKE_TOKEN (expires — check date)
- dbt Cloud: RSA key pair auth (snowflake_private_key.pem and snowflake_public_key.pem — gitignored)
- GitHub Actions secrets: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY, ANTHROPIC_API_KEY
- FRED API key in .env as FRED_API_KEY

## Key files
- ingestion/extract.py — yfinance extraction, S&P 500 Wikipedia scraper, bulk download
- ingestion/extract_fred.py — FRED API extraction, 95 series dict
- ingestion/load.py — Snowflake bulk loading, get_max_date, get_min_date
- ingestion/pipeline.py — Prefect flows: equity_pipeline, macro_pipeline, backfill_pipeline
- agents/chart_agent.py — Streamlit + Claude chat app, two-step LLM pipeline
- agents/code_reviewer.py — AI code review agent, chunks files, posts PR comments
- dbt_project.yml — dbt project config, profile: equity_analytics, model-paths: dbt_project/models
- .github/workflows/dbt_ci.yml — CI on PR, prod deploy on merge to main
- .github/workflows/code_review.yml — AI code review on every PR

## GitHub Actions behavior
- PR opened → dbt build runs in CI_{pr_number} schema, AI code review posted as comment
- Merge to main → dbt build runs with target: prod writing to MARTS directly
- PRs blocked from merging if any dbt model or test fails

## Known issues / decisions made
- dbt-fusion 2.0 ignores generate_schema_name macro — use config(schema=) per model
- Snowflake free trial: $20/month after 30 days — set calendar reminder before expiry
- Prefect free tier: no custom work pools — use flow.serve() locally
- Programmatic access token for Snowflake expires — regenerate in Snowsight when auth fails
- yfinance Wikipedia scrape needs User-Agent header to avoid 403
- FRED series validation: some series IDs are invalid — extract_fred_series handles 400s gracefully
- S&P 500 tickers use dashes not dots (BRK-B not BRK.B) — handled in get_sp500_tickers()

## Backfill plan (not yet run)
- Target: 2010-01-01 to earliest date in RAW.PRICES
- backfill_pipeline flow in pipeline.py — batch size 50, 30s delay between batches
- After backfill: run dbt build --select fact_daily_prices --full-refresh
- Expected ~2.4M rows in fact_daily_prices after backfill

## Interview context (BDT & MSD Partners)
- Hiring manager meeting and technical screen coming up
- Role: Data Engineer, Associate — Snowflake, dbt, Prefect, Fivetran, Azure
- This project was built specifically to demonstrate production-grade DE skills
- Key differentiators: incremental loads, Kimball modeling, CI/CD, LLM agent layer
- Be ready to explain: nanosecond timestamp issue, incremental vs full-refresh tradeoff,
  why flow.serve() vs worker, why schema separation matters, Kimball vs OBT