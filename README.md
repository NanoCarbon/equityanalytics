# Equity Analytics Pipeline

A production-style ELT pipeline and AI-powered analytics application built as a portfolio project for data engineering roles in financial services. Ingests **12,500+ global securities** — full S&P Composite 1500, all US exchange-listed equities and ETFs via NASDAQ Trader flat files, and 5 international indices (FTSE 100, TSX 60, ASX 200, Nikkei 225, DAX 40) — plus 6,100+ Federal Reserve macro indicators, complete fundamental financial data (income statements, balance sheets, cash flow, and valuation metrics), supplemental equity data (dividends, earnings history, analyst ratings, price targets), and a full FRED series catalog. Models everything into a Kimball dimensional warehouse and exposes the data through a natural language chat interface that generates SQL and interactive charts on demand.

## Live Demo

[equity-analytics.streamlit.app](https://equity-analytics.streamlit.app)

---

## Architecture

```
12,500+ global securities      FRED macro indicators       Financial statements
 (yfinance + exchange pages)       (FRED API)               + valuation metrics
          |                             |                       (yfinance)
  Python ingestion            Python ingestion             Python ingestion
  Airflow orchestrated        Airflow orchestrated         Airflow orchestrated
  (Docker Compose)
          |                             |                            |
       Snowflake RAW schema (append-only / overwrite landing zone)
                        |
            dbt transformations (Docker-local)
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
| Ingestion | Python + FRED API | 6,100+ FRED macro series (catalog-driven, fully expanded) + full FRED series catalog |
| Orchestration | Apache Airflow 2.9.3 (Docker Compose, local) | Scheduling, retries, observability |
| Warehouse | Snowflake | Three-schema ELT architecture |
| Transformation | dbt Core | Kimball dimensional modeling |
| Quality | dbt tests + GitHub Actions | 25+ automated tests on every PR |
| AI Code Review | Claude API + GitHub Actions | Automated PR code review comments |
| Application | Streamlit + Claude API | Natural language analytics interface |

---

## Data Coverage

### Equities
- **12,524 tickers** — full S&P Composite 1500 + all US exchange-listed equities and ETFs via NASDAQ Trader flat files + 5 international indices (FTSE 100 `.L`, TSX 60 `.TO`, ASX 200 `.AX`, Nikkei 225 `.T`, DAX 40 `.DE`)
- **7,293 equities** (fundamentals eligible) + **5,231 ETFs/funds**
- S&P index membership fetched live from Wikipedia on each nightly sync — rebalances picked up automatically
- NASDAQ Trader files synced nightly — new listings inserted, delistings soft-deactivated
- International indices synced Monday ET only (quarterly rebalance cadence)
- **25M+ daily OHLCV price rows** — 2010 to present, incremental append
- Company metadata: sector, industry, market cap, exchange, country
- **Rotating fundamentals cohort** — `fundamentals_cohort` (0–3) assigned deterministically per ticker (`ABS(HASH(ticker)) % 4`); drives the 4-week rolling fundamentals schedule; NULL for ETFs

### Fundamentals
- **Financial statements** — income statement, balance sheet, cash flow for ~7,293 equities (ETFs excluded)
  - ~4 years annual + ~8 quarters per ticker from yfinance
  - EAV format in RAW/staging, pivoted to ~35 named columns in marts
  - Cohort-rotation strategy: each week processes one cohort (~1,800 tickers) — full 4-week cycle covers all equities; ≤30-day staleness tolerance
- **Valuation metrics** — 37 point-in-time fields per ticker per day (PE, P/B, EV/EBITDA, margins, growth rates, dividends, beta, etc.)
  - All 12,524 tickers including ETFs
  - Daily append builds a time series of how ratios evolve
- **Dividends and splits** — full corporate action history back to IPO for all tickers (weekly overwrite)
- **Earnings history** — EPS actuals vs. analyst estimates, ~8–20 quarters per equity ticker (weekly overwrite)
- **Analyst recommendations** — upgrade/downgrade history from all major firms (weekly overwrite)
- **Analyst price targets** — mean/high/low/count consensus snapshot appended weekly to build a time series

### Macro Indicators (FRED)

Series selection is **catalog-driven**: `RAW.FRED_SELECTION` is the canonical source of truth for which series to extract. The `FRED_SERIES` dict in `extract_fred.py` is a local fallback name map only. To add or remove series, update `FRED_SELECTION` directly and trigger `fred_new_series_backfill`. Premium index series (SP500, NASDAQCOM, DJIA) require a paid FRED subscription and are auto-deactivated on first backfill.

**6,100 active series** across all FRED categories, driven by `RAW.FRED_SELECTION`. Started from 197 curated series; expanded to catalog-wide coverage via four popularity-tier batches (all complete). Sample categories:

- **Interest rates** (14) — DFF, FEDFUNDS, SOFR, DFEDTARU, DFEDTARL, full Treasury curve DGS1MO → DGS30
- **Yield curve & spreads** (17) — T10Y2Y, T10Y3M, DFII5–DFII30 (TIPS), Treasury–Fed Funds spreads, Aaa/Baa–FF spreads, TEDRATE, HQM corporate bond spot rates
- **Inflation** (20) — CPI headline/core, PCE headline/core, PPI, UMich inflation expectations, CPI sub-components (housing, energy, medical, transport, recreation, durables), Atlanta Fed sticky/flexible CPI, GDP deflator
- **Labor market** (26) — UNRATE, U6RATE, PAYEMS, CIVPART, EMRATIO, CNP16OV, JOLTS (openings, hires, quits, layoffs), jobless claims, manufacturing/services/construction/financial/mining/trade payrolls, average hours, average earnings, ECI wages, productivity (OPHNFB), unit labor costs
- **GDP & growth** (15) — GDP, real GDP, potential GDP, GDP deflator, industrial production (total + manufacturing + capacity utilization), durable/capital goods orders, inventory-to-sales ratio, CFNAI and 3-month MA
- **Consumer** (10) — retail sales (total + ex-auto), PCE (total + durables + nondurables + services), disposable income, saving rate, consumer credit, credit card interest rate
- **Credit & financial conditions** (18) — HY/IG/commercial paper spreads, corporate bond yields (Aaa/Baa), mortgage/residential/business loan delinquency rates, bank loans outstanding, deposits, commercial paper outstanding, SLOOS lending standards and demand (C&I + consumer), NFCI and adjusted NFCI
- **Housing** (13) — mortgage rates (30yr/15yr), housing starts (total + single-family), building permits, new/existing home sales, median sale price, Case-Shiller, homeowner/rental vacancy rates, total construction spending
- **Money supply** (11) — M1, M2, M2 velocity, monetary base, retail/institutional money funds, Fed balance sheet (WALCL), reserve balances, RRPONTSYD
- **Trade & FX** (13) — exports, imports, export prices, broad USD index, 8 major currency pairs (EUR, JPY, GBP, CNY, CAD, BRL, KRW, INR, MXN), wholesale inventories and sales
- **Energy & commodities** (11) — WTI, Brent, gasoline, natural gas, retail electricity, copper, nickel, iron ore, wheat, corn, cotton
- **Market indicators** (4) — VIX, NBER recession indicators (two variants), Empire State manufacturing survey
- **Regional Fed manufacturing** (8) — Philly Fed survey: GACDFSA, NOCDFSA, PPCDFSA, NECDFSA, SHCDFSA; Dallas Fed survey: BACTSAMFRBDAL, PRODSAMFRBDAL, NEMPSAMFRBDAL
- **Government finance & fiscal** (8) — gross federal debt, debt/GDP, monthly Treasury surplus/deficit, federal receipts, expenditures, government net saving, outlays/GDP, revenue/GDP
- **Banking profitability & deposits** (8) — net interest margin (USNIM), ROE, ROA, consumer loan delinquency, total deposits, prime rate, large time deposits (LTDACBM027NBOG), bank equity/assets (EQTA)

### FRED Series Catalog & Selection

| Table | Purpose |
|---|---|
| `RAW.FRED_RELEASES` | One row per FRED statistical release (~300 rows), rebuilt monthly |
| `RAW.FRED_SERIES_CATALOG` | One row per unique FRED series (~800K rows), rebuilt monthly — the universe |
| `RAW.FRED_SELECTION` | **Canonical selection table** — which series we actively extract (`is_active`, `category`, `local_name`). Persists across catalog refreshes. Monthly refresh auto-deactivates entries whose series no longer exist in FRED. |

**Adding a new series:**
```sql
-- 1. Find it in the catalog
SELECT series_id, title, popularity, frequency_short, observation_end
FROM RAW.FRED_SERIES_CATALOG
WHERE title ILIKE '%your search term%'
  AND UPPER(title) NOT LIKE '%DISCONTINUED%'
ORDER BY popularity DESC;

-- 2. Add to selection
INSERT INTO RAW.FRED_SELECTION (series_id, local_name, category, is_active)
VALUES ('SERIES_ID', 'Your descriptive name', 'Category', TRUE);
```
```bash
# 3. Backfill its full history
docker compose exec airflow-webserver airflow dags trigger fred_new_series_backfill
```

---

## Warehouse Structure

```
EQUITY_ANALYTICS
├── RAW
│   ├── TICKER_UNIVERSE         -- canonical ticker list (11,899 tickers), FK source for all RAW tables
│   │                              columns: ticker, source (sp500/sp400/sp600/etf/nasdaq_trader/manual),
│   │                              is_active, is_equity, exchange, country, yfinance_suffix,
│   │                              fundamentals_cohort (0–3, NULL for ETFs), added_at, deactivated_at
│   ├── PRICES                  -- daily OHLCV, 12,524 tickers, ~25M+ rows (2010–present), incremental append
│   ├── COMPANY_INFO            -- company metadata, 12,524 tickers, overwrite on each run
│   ├── MACRO_INDICATORS        -- 788+ FRED series, incremental append
│   ├── FINANCIAL_STATEMENTS    -- EAV format (income/balance/cashflow), weekly overwrite
│   ├── VALUATION_METRICS       -- point-in-time ratios, daily append
│   ├── DIVIDENDS_AND_SPLITS    -- full corporate action history, weekly overwrite
│   ├── EARNINGS_HISTORY        -- EPS actuals vs. estimates, weekly overwrite
│   ├── ANALYST_RECOMMENDATIONS -- upgrade/downgrade history, weekly overwrite
│   ├── ANALYST_PRICE_TARGETS   -- consensus price target snapshot, weekly append
│   ├── FRED_RELEASES           -- FRED publication metadata, monthly overwrite
│   ├── FRED_SERIES_CATALOG     -- all FRED series metadata (~800K rows), monthly overwrite
│   ├── FRED_SELECTION          -- canonical selection: which series to extract (persists across refreshes)
│   └── VW_FRED_HYGIENE         -- view: latest/prev obs date + row-count per series (duplicate detector)
├── STAGING (views)
│   ├── STG_TICKER_UNIVERSE
│   ├── STG_PRICES              -- QUALIFY dedup on (ticker, price_date)
│   ├── STG_COMPANIES
│   ├── STG_MACRO_INDICATORS    -- QUALIFY dedup on (series_id, observation_date)
│   ├── STG_FINANCIAL_STATEMENTS
│   ├── STG_VALUATION_METRICS
│   ├── STG_DIVIDENDS_AND_SPLITS
│   ├── STG_EARNINGS_HISTORY
│   ├── STG_ANALYST_RECOMMENDATIONS
│   ├── STG_ANALYST_PRICE_TARGETS
│   ├── STG_FRED_SELECTION
│   ├── STG_FRED_RELEASES
│   └── STG_FRED_SERIES_CATALOG
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
- Reads active series at runtime from `RAW.FRED_SELECTION` (joined to catalog) — no code changes needed to add or remove series
- 6,100 active series; incremental append with a 7-day overlap to catch FRED revisions
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

**`ticker_universe_sync`** — schedule `0 8 * * 2-6` (3am ET Mon–Fri, one hour before equity_daily)
- Three chained tasks: `sync_sp_indices()` → `sync_nasdaq_trader()` → `sync_international_indices()` (Monday ET only)
- `sync_sp_indices`: scrapes S&P 500/400/600 from Wikipedia; MERGEs into `RAW.TICKER_UNIVERSE`; deactivation scoped to S&P/ETF sources only
- `sync_nasdaq_trader`: downloads NASDAQ Trader flat files; inserts new US-listed tickers, reactivates returning tickers, deactivates delisted tickers
- `sync_international_indices`: re-scrapes FTSE 100, TSX 60, ASX 200 (Wikipedia), Nikkei 225 (JPX official), DAX 40 (Wikipedia); Monday ET only — index membership only changes at quarterly rebalances
- S&P/ETF source priority preserved on every MERGE — nasdaq_trader and international sources never overwrite an S&P label
- All downstream yfinance DAGs read from `TICKER_UNIVERSE` at runtime — no Wikipedia calls in the critical path

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
- Reads from `RAW.FRED_SELECTION` to identify active series not yet in `RAW.MACRO_INDICATORS`
- Fetches full history only for series not yet loaded — appends with `overwrite=False`, never touches existing data
- Auto-deactivates series that return no data (invalid ID or premium) — keeps `FRED_SELECTION` clean
- Idempotent: safe to re-run. Use this after inserting new rows into `RAW.FRED_SELECTION`.

### Transformation Layer

dbt models follow a strict three-tier architecture:

**Staging** — one model per source. Cleans types, renames columns, handles nulls. No business logic.
- `stg_ticker_universe` — passthrough from PK-enforced RAW table; canonical ticker list
- `stg_prices` — converts Unix nanosecond timestamps to dates, casts price fields, QUALIFY dedup on (ticker, price_date) to clean RAW append-only overlap
- `stg_companies` — standardizes sector/industry, coalesces nulls to 'Unknown'
- `stg_macro_indicators` — converts timestamps, casts values, QUALIFY dedup on (series_id, observation_date)
- `stg_financial_statements` — converts timestamps, casts values, filters nulls
- `stg_valuation_metrics` — renames camelCase yfinance fields to snake_case columns
- `stg_dividends_and_splits` — converts epoch, filters zero-value rows, grain: ticker + action_date
- `stg_earnings_history` — converts quarter epoch to date, grain: ticker + period_date
- `stg_analyst_recommendations` — quotes `"INDEX"` reserved keyword → period_index, grain: ticker + period + period_index
- `stg_analyst_price_targets` — quotes `"CURRENT"` reserved keyword → current_target, grain: ticker + snapshot_date
- `stg_fred_selection` — passthrough from PK-enforced RAW table; canonical FRED series selection
- `stg_fred_releases` — FRED release metadata, grain: release_id
- `stg_fred_series_catalog` — full FRED series universe (~800K rows), configured as view

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

100+ automated dbt tests across three layers. Clean `dbt build` result: PASS=108, WARN=1, ERROR=0.

**Staging tests** — `dbt_utils.unique_combination_of_columns` composite grain tests on all 8 multi-column-grain staging models. `not_null` on critical columns, `unique` on single-column PKs, `accepted_values` on statement_type, frequency, and ticker source.

**Mart tests** — `dbt_utils.unique_combination_of_columns` composite grain tests on all four fact tables. `not_null` on all fact columns, `unique` + `not_null` on dimension keys.

**Singular business rule tests**
- `assert_daily_prices_freshness` — MAX(price_date) is within 3 trading days of today
- `assert_macro_readings_freshness` — MAX(observation_date) is recent
- `assert_fundamentals_freshness` — pipeline loaded data within 10 days (checks stg_financial_statements.extracted_at)
- `assert_price_coverage_by_ticker` — no individual ticker is >5 trading days behind the overall max
- `assert_return_bounds` — WARN on daily returns exceeding ±200% (CHRD 2020-11-20 excluded as confirmed merger artifact)
- `assert_no_future_prices` — no price dates beyond today
- `assert_no_negative_prices` — no close price ≤ 0
- `assert_no_negative_volume` — no volume < 0
- `assert_no_future_fundamentals` — no period_end_date beyond today
- `assert_no_negative_revenue` — no total_revenue < 0
- `assert_no_negative_assets` — no total_assets < 0
- `assert_price_history_unchanged` — immutability check on historical prices

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

**Catalog-driven ticker universe (`RAW.TICKER_UNIVERSE`)**
The ticker list is no longer assembled at runtime from Wikipedia scrapes. `RAW.TICKER_UNIVERSE` is the canonical table of which tickers to extract — mirroring the `RAW.FRED_SELECTION` pattern for macro series. A dedicated `ticker_universe_sync` DAG (runs at 3am ET, one hour before `equity_daily`) runs two chained tasks: `sync_sp_indices` scrapes the current S&P 500/400/600 components from Wikipedia; `sync_nasdaq_trader` downloads the official NASDAQ Trader flat files (`nasdaqlisted.txt` + `otherlisted.txt`) covering all US exchange-listed securities. Both tasks MERGE into `TICKER_UNIVERSE`: new tickers are inserted, delisted tickers are soft-deactivated (`is_active=FALSE`) preserving FK references in historical RAW data, returning tickers are reactivated. S&P/ETF source priority is always preserved — `nasdaq_trader` never overwrites a ticker already labelled `sp500`/`sp400`/`sp600`/`etf`. All yfinance DAGs read from `TICKER_UNIVERSE` at runtime via `get_tickers_from_db(conn)`, falling back to the Wikipedia scrape only if the DB is unreachable. `TICKER_UNIVERSE` carries FK constraints referenced by all 8 RAW ticker tables and exposes `exchange`, `country`, `yfinance_suffix`, and `fundamentals_cohort` columns for downstream use.

**Catalog-driven FRED selection (`RAW.FRED_SELECTION`)**
The `FRED_SERIES` Python dict was the original series list; it is now a local fallback name map only. `RAW.FRED_SELECTION` is the canonical table of which series to extract — separate from the catalog so it survives monthly overwrites. The `macro_daily` DAG, `macro_backfill` DAG, and `fred_new_series_backfill` DAG all read from `FRED_SELECTION` at runtime. The monthly `fred_catalog_refresh` DAG auto-deactivates any selected series that disappear from FRED, and refreshes category labels from the latest release names. To add new series: insert into `FRED_SELECTION`, then trigger `fred_new_series_backfill`. No code changes needed.

**FRED catalog as an information schema**
A monthly DAG crawls all ~300 FRED statistical releases and catalogs every series (~800K rows) with popularity scores, frequency, units, and history bounds. Query `RAW.FRED_SERIES_CATALOG` to discover new series. The catalog is a reference; `RAW.FRED_SELECTION` is what actually drives extraction.

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
│   ├── extract_ticker_universe.py    # Ticker universe sources -- fetch_nasdaq_trader_us(); fetch_ftse100/tsx60/asx200/nikkei225/dax40
│   ├── extract_fred.py               # FRED API extraction -- 108 series with rate limiting
│   ├── extract_fred_catalog.py       # FRED releases + series catalog crawler
│   ├── extract_fundamentals.py       # Financial statements (EAV) + valuation metrics
│   ├── seed_ticker_universe.py       # One-shot seeder: S&P/ETF + NASDAQ Trader MERGE, cohort assignment
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
│   │   ├── staging/              # 13 staging models — one per RAW source table
│   │   │                         #   stg_ticker_universe, stg_prices (QUALIFY dedup),
│   │   │                         #   stg_companies, stg_macro_indicators (QUALIFY dedup),
│   │   │                         #   stg_financial_statements, stg_valuation_metrics,
│   │   │                         #   stg_dividends_and_splits, stg_earnings_history,
│   │   │                         #   stg_analyst_recommendations, stg_analyst_price_targets,
│   │   │                         #   stg_fred_selection, stg_fred_releases, stg_fred_series_catalog
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
│   ├── backfill.py                   # Parameterized backfill CLI -- idempotent, all 4 data types, rate-limit tunable
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
├── CLAUDE.md                     # Claude Code project guide (architecture, patterns, pitfalls, current state)
├── docker-compose.yml            # Airflow services (webserver, scheduler, init, postgres)
├── dbt_project.yml               # dbt project config
├── profiles.yml                  # dbt Core connection profile (gitignored)
├── requirements.txt
└── .env.example
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

No code changes needed. Find the series in the catalog, insert into `FRED_SELECTION`, trigger the backfill:

```sql
-- 1. Find the series in the catalog
SELECT series_id, title, popularity, frequency_short, observation_end
FROM EQUITY_ANALYTICS.RAW.FRED_SERIES_CATALOG
WHERE title ILIKE '%breakeven inflation%'
  AND UPPER(title) NOT LIKE '%DISCONTINUED%'
ORDER BY popularity DESC LIMIT 5;

-- 2. Add to selection
INSERT INTO EQUITY_ANALYTICS.RAW.FRED_SELECTION (series_id, local_name, category, is_active)
VALUES ('T10YIE', '10-Year Breakeven Inflation Rate', 'FRB H.15 Selected Interest Rates', TRUE);
```

```bash
# 3. Backfill full history for new series only
docker compose exec airflow-webserver airflow dags trigger fred_new_series_backfill

# 4. Full-refresh the mart to incorporate new series
dbt build --profiles-dir . --select fact_macro_readings --full-refresh
```

The DAG computes `FRED_SELECTION(is_active) - series already in RAW.MACRO_INDICATORS` at runtime — idempotent and safe to re-run. Series returning no data (invalid ID or premium) are auto-deactivated in `FRED_SELECTION`.

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

| Category | Source | Count |
|---|---|---|
| S&P 500 (large-cap) | Wikipedia (live, nightly sync) | ~503 |
| S&P 400 (mid-cap) | Wikipedia (live, nightly sync) | ~400 |
| S&P 600 (small-cap) | Wikipedia (live, nightly sync) | ~603 |
| ETFs (top 100+ by AUM) | Hardcoded list | ~113 |
| NASDAQ-listed equities & ETFs | NASDAQ Trader `nasdaqlisted.txt` | ~5,086 |
| NYSE / NYSE Arca / AMEX / BATS equities & ETFs | NASDAQ Trader `otherlisted.txt` | ~5,194 |
| FTSE 100 | Wikipedia (Monday sync) | 100 |
| TSX 60 | Wikipedia (Monday sync) | 60 |
| ASX 200 | Wikipedia (Monday sync) | 200 |
| Nikkei 225 | JPX official page (Monday sync) | 225 |
| DAX 40 | Wikipedia (Monday sync) | 40 |
| **Total unique global** | | **~12,524** |

Of the 12,524 active tickers: **7,293 equities** (fundamentals eligible) and **5,231 ETFs/funds**. All tickers have price history back to 2010-01-01. S&P and NASDAQ Trader tickers sync nightly; international indices sync Monday ET only (quarterly rebalance cadence).

---

## Macro Indicators Covered

| Category | Series | Key examples |
|---|---|---|
| Interest rates | 14 | DFF, FEDFUNDS, SOFR, DFEDTARU, DFEDTARL, DGS1MO → DGS30 (full curve) |
| Yield curve & spreads | 17 | T10Y2Y, T10Y3M, DFII5–DFII30 (TIPS), T10YFFM, AAAFF, BAAFF, TEDRATE, HQMCB |
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
| Regional Fed manufacturing *(Wave 4)* | 8 | Philly (GACDFSA/NOCDFSA/PPCDFSA/NECDFSA/SHCDFSA), Dallas (BACTSAMFRBDAL/PRODSAMFRBDAL/NEMPSAMFRBDAL) |
| Government finance & fiscal *(Wave 5)* | 8 | GFDEBTN, GFDEGDQ188S, MTSDS133FMS, MTSO133FMS, FGEXPND, GGSAVE, FYONGDA188S, FYFRGDA188S |
| Banking profitability & deposits *(Wave 6)* | 8 | USNIM, USROE, USROA, DRCLACBS, WDTGAL, DPRIME, LTDACBM027NBOG, EQTA |
| **Curated total (pre-expansion)** | **197** | Catalog-driven expansion adds ~5,900 more series across 4 popularity tiers |

---

## Roadmap

### Near-term (pipeline)
- ✅ **Phase 3 — International indices** — FTSE 100, TSX 60, ASX 200, Nikkei 225, DAX 40 live in `TICKER_UNIVERSE`; Monday-only sync in `ticker_universe_sync`
- ✅ **Phase 4 — Global backfill** — all 12,524 tickers backfilled from 2010-01-01 (prices, company info, financials, valuation); `dbt --full-refresh` complete across all fact tables
- **Phase 5 — Fundamentals at scale** — update `fundamentals_weekly` to use `fundamentals_cohort` rotation (`WEEKOFYEAR(CURRENT_DATE()) % 4`); change load strategy from full-table overwrite to per-cohort scoped DELETE + INSERT
- **Historical valuation ratios** — dbt model computing trailing PE, P/B, P/S, dividend yield, and beta from existing `fact_daily_prices` x `fact_fundamentals` join, using point-in-time financial statement dates to avoid look-ahead bias. Extends ratio history back to 2010 without any new data sources.
- **dbt models for supplemental tables** — staging and mart models for `DIVIDENDS_AND_SPLITS`, `EARNINGS_HISTORY`, `ANALYST_RECOMMENDATIONS`, and `ANALYST_PRICE_TARGETS`.

### FRED Series Expansion

Series selection is now catalog-driven via `RAW.FRED_SELECTION`. The wave model below is replaced by **popularity-tier batches** — new series are added in bulk from `FRED_SERIES_CATALOG` by popularity score, confirmed valid, inserted into `FRED_SELECTION`, and backfilled.

| Batch | Popularity | New series seeded | Cumulative RAW rows | Cumulative active selections | Status |
|---|---|---|---|---|---|
| Batch 1 | ≥ 70 | 93 | ~836K | 290 | ✅ Complete |
| Batch 2 | 50–69 | 498 | 1,295,017 | 788 | ✅ Complete |
| Batch 3 | 30–49 | 1,546 | ~1.7M | 2,334 | ✅ Complete |
| Batch 4 | 15–29 | 3,765 | 4,100,030 | 6,100 | ✅ Complete |

All 4 popularity-tier batches are complete. `fact_macro_readings` mart contains **1,836,249 rows** across **6,032 unique series** (68 series have all observations outside the 2009–present dim_date window). Total active selections: **6,100** across 4,101 FRED release categories.

Series marked DISCONTINUED in FRED are auto-deactivated after each monthly catalog refresh. Premium series (403) are auto-deactivated after the first backfill attempt.

---

**Historical waves (pre-catalog-driven architecture):**

**Wave 4 — Regional Federal Reserve Economic Surveys** ✅ COMPLETE (8 valid series)

Five regional Fed banks publish monthly manufacturing and services surveys. We have Chicago (CFNAI, NFCI) and New York (Empire State). Missing:

| Series | Description | Bank |
|---|---|---|
| GACDFSA066MSFRBPHI | Current General Activity (Diffusion Index) | Philadelphia Fed |
| NOCDFSA066MSFRBPHI | Current New Orders (Diffusion Index) | Philadelphia Fed |
| PPCDFSA066MSFRBPHI | Current Prices Paid (Diffusion Index) | Philadelphia Fed |
| NECDFSA066MSFRBPHI | Current Employment (Diffusion Index) | Philadelphia Fed |
| SHCDFSA066MSFRBPHI | Current Shipments (Diffusion Index) | Philadelphia Fed |
| BACTSAMFRBDAL | Current General Business Activity | Dallas Fed |
| PRODSAMFRBDAL | Current Production | Dallas Fed |
| NEMPSAMFRBDAL | Current Employment | Dallas Fed |

Note: Richmond Fed and Kansas City Fed manufacturing surveys are not available in the FRED catalog. Original IDs (PHFRBIND, RMBSIICS, DALLASMI, KANSASMI etc.) were invalid — corrected to catalog-confirmed IDs above.

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
| FYFRGDA188S | Federal Receipts as % of GDP | Annual |

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
- **Additional international indices** — CAC 40 (`.PA`), FTSE 250 (`.L`), Hang Seng (`.HK`) — Phase 3 covers FTSE 100, TSX 60, ASX 200, Nikkei 225, DAX 40
- **OTC / Pink Sheet stocks** — FINRA OTC data source; lower data quality, separate DAG recommended
- **Preferred shares / warrants** — selectively relax the `$`/`^` symbol filter in NASDAQ Trader parsing
- **SEC EDGAR filings** — 8-K, 10-K, 10-Q structured data via EDGAR API for event-driven analysis
