-- Assert that fact_daily_prices has been refreshed recently.
--
-- WHAT THIS CHECKS
-- The most recent price_date in the table must be within 3 calendar days of today.
-- This accounts for weekends (Friday close loaded by Monday morning) and US market
-- holidays, but will catch a broken equity pipeline that hasn't loaded in days.
--
-- WHAT THIS DOES NOT CHECK
-- Whether every ticker has a recent row. A pipeline that loads prices for only
-- 10 of 616 tickers would still pass this test. See assert_price_coverage_by_ticker
-- for per-ticker staleness (a more expensive check, run less frequently).
--
-- THRESHOLD
-- 3 days: covers Fri → Mon gap (2 days) plus 1 day buffer for pipeline run timing.
-- Increase to 4 around long weekends if you get false alarms.

select
    max(price_date)                             as latest_price_date,
    current_date()                              as today,
    datediff(day, max(price_date), current_date()) as days_since_last_load
from {{ ref('fact_daily_prices') }}
having datediff(day, max(price_date), current_date()) > 3
