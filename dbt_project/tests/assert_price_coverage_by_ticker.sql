-- Assert that no individual ticker is significantly staler than the overall
-- maximum price date in fact_daily_prices.
--
-- PURPOSE
-- The freshness test (assert_daily_prices_freshness) only checks MAX(price_date)
-- across the entire table. A pipeline failure that loads prices for 615 of 616
-- tickers would pass that test. This test catches per-ticker staleness.
--
-- HOW IT WORKS
-- Computes the overall max price_date as the reference point, then flags any
-- ticker whose own max price_date lags more than 5 trading days behind.
-- 5 days (~1 week) is chosen because:
--   - Some tickers are occasionally delisted or halted mid-week
--   - yfinance bulk download occasionally misses a ticker silently
--   - We don't want to alert on 1-2 day gaps (holidays, late data)
--   - A 5-day gap reliably signals a real ingestion problem for that ticker
--
-- PERFORMANCE NOTE
-- This is a heavier query than the simple freshness check — it scans the full
-- fact table and groups by ticker. Run it in dbt as part of the full test suite,
-- but consider excluding it from time-sensitive CI runs using a dbt tag:
--
--   {{ config(tags=['slow', 'coverage']) }}
--   dbt test --exclude tag:slow   # fast CI
--   dbt test                      # full suite
--
-- EXPECTED FAILURES
-- Tickers that are genuinely delisted or acquired will always fail this test.
-- Once confirmed, add them to the exclusion list below and document the reason.

with max_date as (
    select max(price_date) as overall_max
    from {{ ref('fact_daily_prices') }}
),

ticker_max as (
    select
        ticker,
        max(price_date) as ticker_max_date
    from {{ ref('fact_daily_prices') }}
    group by ticker
),

-- Tickers known to be delisted, acquired, or excluded from yfinance.
-- Add entries here when a staleness alert is investigated and confirmed benign.
known_exclusions as (
    select * from (values
        ('FDS')   -- occasionally missing from yfinance bulk download
    ) as t (ticker)
)

select
    t.ticker,
    t.ticker_max_date,
    m.overall_max                                           as reference_max_date,
    datediff(day, t.ticker_max_date, m.overall_max)         as days_behind
from ticker_max t
cross join max_date m
left join known_exclusions e on t.ticker = e.ticker
where e.ticker is null  -- exclude known benign cases
  and datediff(day, t.ticker_max_date, m.overall_max) > 5
order by days_behind desc
