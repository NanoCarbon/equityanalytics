select ticker, price_date, close_price
from {{ ref('fact_daily_prices') }}
where close_price <= 0