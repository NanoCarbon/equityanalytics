-- Assert no duplicate grain rows in fact_fundamentals.
-- Grain: ticker + period_end_date + frequency.
--
-- PURPOSE
-- The unique_key in the incremental model config handles deduplication via
-- Snowflake MERGE, but it cannot prevent duplicates if source data from
-- int_fundamentals_pivoted already contains them (e.g. yfinance returning
-- the same period twice with slightly different values, causing two rows
-- to satisfy the incremental filter in the same run).
--
-- This test runs against the final mart table and catches any grain violations
-- that slip through the incremental logic. Failures indicate either:
--   - A bug in int_fundamentals_pivoted producing duplicate (ticker, period, frequency) rows
--   - A unique_key MERGE failure (Snowflake connector or dbt version regression)
--   - Source data returning duplicate reporting periods with different values

select
    ticker,
    period_end_date,
    frequency,
    count(*) as row_count
from {{ ref('fact_fundamentals') }}
group by ticker, period_end_date, frequency
having count(*) > 1
