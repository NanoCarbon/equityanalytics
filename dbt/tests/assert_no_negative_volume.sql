select ticker, price_date, volume
from {{ ref('fact_daily_prices') }}
where volume < 0