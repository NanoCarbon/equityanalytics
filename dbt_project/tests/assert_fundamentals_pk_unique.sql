-- Assert no duplicate (ticker, period_end_date, frequency) combinations in fact_fundamentals.
-- Three-column compound key — cannot be checked with the built-in dbt unique test.

select
    ticker,
    period_end_date,
    frequency,
    count(*) as row_count
from {{ ref('fact_fundamentals') }}
group by ticker, period_end_date, frequency
having count(*) > 1
