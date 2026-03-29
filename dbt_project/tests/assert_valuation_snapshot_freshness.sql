-- Assert that fact_valuation_snapshot has been refreshed recently.
--
-- WHAT THIS CHECKS
-- The most recent snapshot_date must be within 3 calendar days of today.
-- Valuation metrics are snapshotted daily alongside equity prices, so they
-- should have the same freshness profile as fact_daily_prices.
--
-- THRESHOLD
-- 3 days: mirrors fact_daily_prices threshold (weekends + 1 day buffer).

select
    max(snapshot_date)                              as latest_snapshot_date,
    current_date()                                  as today,
    datediff(day, max(snapshot_date), current_date()) as days_since_last_load
from {{ ref('fact_valuation_snapshot') }}
having datediff(day, max(snapshot_date), current_date()) > 3
