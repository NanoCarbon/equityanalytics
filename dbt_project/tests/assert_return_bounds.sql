select ticker, price_date, daily_return
from {{ ref('fact_daily_prices') }}
where daily_return > 0.5 or daily_return < -0.5