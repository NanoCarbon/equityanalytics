-- Assert that the financial statements pipeline has loaded data recently.
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
-- WHY stg_financial_statements AND NOT fact_fundamentals
-- extracted_at is written at ingestion time (datetime.utcnow() in extract_fundamentals.py)
-- and is available on stg_financial_statements. It is intentionally dropped during the
-- int_fundamentals_pivoted GROUP BY pivot (no sensible aggregate applies across
-- heterogeneous line items). Checking stg ensures we see the actual load timestamp
-- rather than a surrogate computed further downstream.
--
-- THRESHOLD
-- 10 days: the pipeline runs weekly (Saturday 10am). 7 days is the expected cadence;
-- 10 days allows for one missed Saturday run before alerting. Adjust to 8 if you
-- want tighter alerting.

select
    max(extracted_at)                                            as latest_extracted_at,
    current_timestamp()                                          as now,
    datediff(day, max(extracted_at)::date, current_date())       as days_since_last_load,
    max(period_end_date)                                         as latest_period_end_date
from {{ ref('stg_financial_statements') }}
having datediff(day, max(extracted_at)::date, current_date()) > 10
