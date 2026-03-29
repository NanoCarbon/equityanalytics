-- Assert no duplicate (ticker, price_date) combinations in fact_daily_prices.
-- The dbt built-in unique test operates on a single column; this test checks
-- the compound natural key that defines the grain of this fact table.

select
    ticker,
    price_date,
    count(*) as row_count
from {{ ref('fact_daily_prices') }}
group by ticker, price_date
having count(*) > 1
