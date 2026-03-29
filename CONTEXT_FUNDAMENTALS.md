## Fundamentals Data (Phase 1 — added March 2026)

### New RAW tables
- `RAW.FINANCIAL_STATEMENTS` — EAV format: ticker, statement_type, frequency, period_end_date, line_item, value
  - statement_type: income_statement, balance_sheet, cash_flow
  - frequency: annual, quarterly
  - Source: yfinance `.income_stmt`, `.balance_sheet`, `.cashflow` (+ quarterly variants)
  - Load strategy: full overwrite (data is small, ~580K rows for 500 equities)
  - ETFs excluded — they don't have financial statements

- `RAW.VALUATION_METRICS` — wide format: ticker, snapshot_date, trailing_pe, forward_pe, ...
  - Source: yfinance `.info` (same endpoint as company metadata, different grain)
  - 37 fields: valuation ratios, profitability, leverage, growth, dividends, absolute values
  - Load strategy: daily append (builds time series of PE, margins, etc.)
  - All tickers including ETFs (ETFs have some fields like beta)

### New dbt models
- Staging: `stg_financial_statements`, `stg_valuation_metrics`
- Intermediate: `int_fundamentals_pivoted` (pivots EAV into ~35 named columns + derived margins)
- Marts: `fact_fundamentals` (grain: ticker + period_end_date + frequency), `fact_valuation_snapshot` (grain: ticker + snapshot_date)
- Tests: 3 new singular tests (no future fundamentals, no negative revenue, no negative assets)
  + accepted_values tests on statement_type and frequency

### New Prefect flows (in pipeline_fundamentals.py)
- `fundamentals-test-pipeline/test-fundamentals` — 13 tickers, manual trigger, validates schema
- `fundamentals-backfill-pipeline/backfill-fundamentals` — all equities, batched, one-time
- `fundamentals-ingestion-pipeline/weekly-fundamentals-ingestion` — Saturday 10am cron
- `valuation-snapshot-pipeline/daily-valuation-snapshot` — weekday 9am cron

### Operational notes
- ETFs are filtered out for financial statements using get_etf_tickers() exclusion list
- Financial statements use full overwrite because yfinance returns a fixed ~4yr/8Q window
  and this catches retroactive restatements automatically
- Valuation metrics append daily to build time series (unique_key: ticker + snapshot_date)
- Adding new tickers to fundamentals: re-run fundamentals pipeline, then
  `dbt build --select +fact_fundamentals --full-refresh`
- yfinance line item names are CamelCase (e.g. TotalRevenue, NetIncome) — preserved as-is
  in RAW and staging, referenced in the pivot intermediate model

### Run order for Phase 1
```bash
# 1. Start Prefect
python deploy.py

# 2. Run test pipeline (13 tickers, ~30 seconds)
prefect deployment run 'fundamentals-test-pipeline/test-fundamentals'

# 3. Check RAW tables in Snowflake
# SELECT COUNT(*), COUNT(DISTINCT ticker) FROM EQUITY_ANALYTICS.RAW.FINANCIAL_STATEMENTS;
# SELECT COUNT(*), COUNT(DISTINCT ticker) FROM EQUITY_ANALYTICS.RAW.VALUATION_METRICS;

# 4. Run dbt (builds new models + tests)
dbt build --select +fact_fundamentals +fact_valuation_snapshot

# 5. If tests pass, proceed to Phase 2 backfill
prefect deployment run 'fundamentals-backfill-pipeline/backfill-fundamentals'
dbt build --select +fact_fundamentals +fact_valuation_snapshot --full-refresh
```
