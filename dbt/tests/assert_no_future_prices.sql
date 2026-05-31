select price_date
from {{ ref('fact_daily_prices') }}
where price_date > current_date()