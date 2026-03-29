# Equity Analytics Pipeline — Project Context

## Stack
- Python + yfinance + Prefect Cloud (ingestion)
- Snowflake (warehouse) — account: QYCMQJK-HTC96121, user: EDWARDWLIN
- dbt Core (transformation) — connected to GitHub NanoCarbon/equityanalytics
- Streamlit + Claude API (chat app) — deployed to Streamlit Community Cloud
- GitHub Actions (CI/CD + AI code review)

## Warehouse structure
```
EQUITY_ANALYTICS
├── RAW
│   ├── PRICES (table) — 616 tickers, daily OHLCV, incremental append
│   ├── COMPANY_INFO (table) — 616 tickers, metadata, overwrite on each run
│   ├── MACRO_INDICATORS (table) — 95 FRED series, overwrite on each run
│   ├── FINANCIAL_STATEMENTS (table) — EAV format, income/balance/cashflow, overwrite on each run
│   └── VALUATION_METRICS (table) — point-in-time ratios (PE, margins, etc.), daily append
├── STAGING (views)
│   ├── STG_PRICES
│   ├── STG_COMPANIES
│   ├── STG_MACRO_INDICATORS
│   ├── STG_FINANCIAL_STATEMENTS
│   ├── STG_VALUATION_METRICS
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
- 95 FRED macro series across 11 categories
- Financial statements: income statement, balance sheet, cash flow (annual + quarterly)
  - EAV format in RAW/staging, pivoted to ~35 named columns in marts
  - ~276 unique line items from yfinance (spaced names, e.g. "Total Revenue")
  - ETFs excluded from statement extraction (no 10-K filings)
- Valuation metrics: 37 fields (PE, P/B, margins, growth, dividends, beta, etc.)
  - All tickers including ETFs (beta, dividend yield still useful)
  - Daily snapshots build a time series
- Daily incremental loads via Prefect (equity + macro pipelines)
- Weekly full overwrite for financial statements (Saturday 10am)
- Daily append for valuation snapshots (weekdays 9am)

## Prefect deployments
All flows served via `deploy.py` (gitignored):
- `equity-ingestion-pipeline/daily-equity-ingestion` — 9am weekdays
- `macro-ingestion-pipeline/daily-macro-ingestion` — 9am weekdays
- `fundamentals-test-pipeline/test-fundamentals` — manual trigger only
- `fundamentals-backfill-pipeline/backfill-fundamentals` — manual trigger only
- `fundamentals-ingestion-pipeline/weekly-fundamentals-ingestion` — Saturday 10am
- `valuation-snapshot-pipeline/daily-valuation-snapshot` — 9am weekdays

Trigger manually:
```bash
prefect deployment run 'equity-ingestion-pipeline/daily-equity-ingestion'
prefect deployment run 'macro-ingestion-pipeline/daily-macro-ingestion'
prefect deployment run 'fundamentals-test-pipeline/test-fundamentals'
prefect deployment run 'fundamentals-backfill-pipeline/backfill-fundamentals'
```

Or run flows directly (bypassing deployment registry):
```bash
python -c "from dotenv import load_dotenv; load_dotenv(); from ingestion.pipeline_fundamentals import fundamentals_test_pipeline; fundamentals_test_pipeline()"
```

## dbt models
- 10 models total: stg_prices, stg_companies, stg_macro_indicators,
  stg_financial_statements, stg_valuation_metrics,
  int_daily_returns, int_fundamentals_pivoted,
  fact_daily_prices, fact_macro_readings, fact_fundamentals,
  fact_valuation_snapshot, dim_date, dim_security
- Tests passing across staging, marts, and singular business rules
- Incremental materialization on fact_daily_prices, fact_macro_readings,
  fact_fundamentals, and fact_valuation_snapshot
- unique_key=['ticker', 'price_date'] on fact_daily_prices
- unique_key=['ticker', 'period_end_date', 'frequency'] on fact_fundamentals
- unique_key=['ticker', 'snapshot_date'] on fact_valuation_snapshot

## Key operational patterns
- Adding new tickers to the universe requires `dbt build --select fact_daily_prices --full-refresh`
  because the incremental filter (price_date > max existing) excludes historical rows for new tickers
- Same pattern for fact_fundamentals — new tickers need `--full-refresh`
- Daily incremental runs work correctly for existing tickers
- Snowflake DATE column stored as nanosecond Unix timestamps (NUMBER(38,0))
  Conversion: to_date(dateadd(second, date / 1000000000, '1970-01-01'))
- flow.serve() used instead of work pool (Prefect free tier blocks custom work pools)
- Financial statements use full overwrite (yfinance returns fixed ~4yr/8Q window, catches restatements)
- Valuation metrics append daily to build time series
- ETFs filtered out for financial statement extraction using get_etf_tickers() exclusion list
- yfinance line item names use spaces (e.g. "Total Revenue", "Net Income") — not CamelCase
- Derived margins (gross, operating, net) computed in a separate CTE in int_fundamentals_pivoted
  to avoid Snowflake GROUP BY + CASE nesting issues

## Auth
- Snowflake: programmatic access token in .env as SNOWFLAKE_TOKEN (expires — check date)
- dbt: RSA key pair auth (snowflake_private_key.pem and snowflake_public_key.pem — gitignored)
- GitHub Actions secrets: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY, ANTHROPIC_API_KEY
- FRED API key in .env as FRED_API_KEY

## Key files
- ingestion/extract.py — yfinance extraction, S&P 500 Wikipedia scraper, bulk download
- ingestion/extract_fred.py — FRED API extraction, 95 series dict
- ingestion/extract_fundamentals.py — financial statements (EAV) + valuation metrics extraction
- ingestion/load.py — Snowflake bulk loading, get_max_date, get_min_date
- ingestion/pipeline.py — Prefect flows: equity_pipeline, macro_pipeline, backfill_pipeline
- ingestion/pipeline_fundamentals.py — Prefect flows: test, backfill, weekly statements, daily valuations
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
- Snowflake free trial: $20/month after 30 days — set calendar reminder before expiry
- Prefect free tier: no custom work pools — use flow.serve() locally
- Programmatic access token for Snowflake expires — regenerate in Snowsight when auth fails
- yfinance Wikipedia scrape needs User-Agent header to avoid 403
- FRED series validation: some series IDs are invalid — extract_fred_series handles 400s gracefully
- S&P 500 tickers use dashes not dots (BRK-B not BRK.B) — handled in get_sp500_tickers()
- yfinance financial statement line items use spaced names (e.g. "Total Revenue") not CamelCase
- Prefect Pydantic validation rejects None defaults for list params — use empty list default instead

## Backfill status
### Price backfill (not yet run)
- Target: 2010-01-01 to earliest date in RAW.PRICES
- backfill_pipeline flow in pipeline.py — batch size 50, 30s delay between batches
- After backfill: run dbt build --select fact_daily_prices --full-refresh
- Expected ~2.4M rows in fact_daily_prices after backfill

### Fundamentals backfill (not yet run)
- Phase 1 test load complete: 10 equities + 3 ETFs validated, 14,344 statement rows loaded
- Phase 2: run fundamentals_backfill_pipeline for all ~500 equities
- yfinance returns ~4 years annual + ~8 quarters per ticker
- Estimated ~580K total statement rows for full universe
- After backfill: dbt build --select +fact_fundamentals +fact_valuation_snapshot --full-refresh

## Next steps
- Run fundamentals backfill (Phase 2)
- Update chart_agent.py system prompt with fact_fundamentals and fact_valuation_snapshot schemas
- Add fundamentals example prompts to Streamlit sidebar
- Expand FRED series catalog via FRED categories API sorted by popularity
- Consider yfinance additional data: analyst estimates, insider transactions, institutional holdings