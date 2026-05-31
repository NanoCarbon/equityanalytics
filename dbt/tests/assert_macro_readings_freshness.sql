-- Assert that fact_macro_readings has been refreshed recently.
--
-- WHAT THIS CHECKS
-- The most recent observation_date across ALL series must be within 5 calendar days.
-- We use a wider window than daily prices because:
--   - Some FRED series are monthly (GDP, CPI) — the observation_date won't advance daily
--   - We're checking that the pipeline ran, not that every series has a new data point
--   - Daily series like DFF and VIX will always push the max forward on trading days
--
-- DESIGN NOTE
-- MAX(observation_date) is dominated by the highest-frequency daily series (DFF, VIX, etc.).
-- As long as those series are current, the pipeline ran. Monthly series being "stale"
-- by their observation_date is expected and correct — that's the FRED release cadence.

select
    max(observation_date)                               as latest_observation_date,
    current_date()                                      as today,
    datediff(day, max(observation_date), current_date()) as days_since_last_load
from {{ ref('fact_macro_readings') }}
having datediff(day, max(observation_date), current_date()) > 5
