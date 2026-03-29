-- Assert that fact_fundamentals has been refreshed recently.
--
-- CRITICAL DESIGN DISTINCTION
-- This test checks when the pipeline LAST LOADED data, not when the most recent
-- fiscal period ended. These are completely different things:
--
--   - MAX(period_end_date): the most recent 10-K or 10-Q period in the warehouse.
--     This advances only when companies report new earnings — quarterly at best.
--     Testing this would produce constant false alarms between earnings seasons.
--
--   - MAX(extracted_at): the timestamp written by the ingestion pipeline when it
--     last ran. This should advance every Saturday (weekly fundamentals schedule).
--     THIS is the correct freshness signal for pipeline health.
--
-- THRESHOLD
-- 10 days: the pipeline runs weekly (Saturday 10am). 7 days is the expected cadence;
-- 10 days allows for one missed Saturday run before alerting. Adjust to 8 if you
-- want tighter alerting.
--
-- NOTE
-- extracted_at is written at ingestion time (datetime.utcnow() in extract_fundamentals.py)
-- and flows through stg_financial_statements → int_fundamentals_pivoted → fact_fundamentals.
-- If extracted_at is ever NULL, this test will pass silently — add a not_null test
-- on extracted_at in staging schema.yml if that becomes a concern.

select
    max(extracted_at)                                           as latest_extracted_at,
    current_timestamp()                                         as now,
    datediff(day, max(extracted_at)::date, current_date())      as days_since_last_load,
    max(period_end_date)                                        as latest_period_end_date
from {{ ref('fact_fundamentals') }}
having datediff(day, max(extracted_at)::date, current_date()) > 10
