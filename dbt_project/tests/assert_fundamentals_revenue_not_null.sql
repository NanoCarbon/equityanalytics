-- Assert that AAPL has non-null total_revenue for annual rows.
--
-- PURPOSE
-- int_fundamentals_pivoted pivots yfinance line items using hardcoded CASE
-- expressions (e.g. line_item = 'Total Revenue'). If yfinance renames a field
-- (e.g. 'Total Revenue' → 'Revenue'), the pivot silently returns NULL for all
-- tickers. This test catches that regression immediately after a dbt run.
--
-- AAPL is used as the canary: it always reports revenue and has continuous
-- annual filing history. If AAPL annual revenue is NULL, the yfinance field
-- name has changed or data extraction has broken.
--
-- The test FAILS (returns rows) when any AAPL annual row has NULL total_revenue.

select ticker, period_end_date, frequency
from {{ ref('int_fundamentals_pivoted') }}
where ticker    = 'AAPL'
  and frequency = 'annual'
  and total_revenue is null
