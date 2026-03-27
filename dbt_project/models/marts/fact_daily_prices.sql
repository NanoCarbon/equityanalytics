with returns as (
    select * from {{ ref('int_daily_returns') }}
),

dim_security as (
    select ticker from {{ ref('dim_security') }}
),

dim_date as (
    select date_key from {{ ref('dim_date') }}
)

select
    returns.ticker,
    returns.price_date,
    returns.close_price,
    returns.volume,
    returns.daily_return,
    returns.rolling_30d_vol_annualized,
    returns.week_52_high,
    returns.week_52_low,
    round(returns.close_price / nullif(returns.week_52_high, 0), 4) as pct_of_52w_high

from returns
inner join dim_security using (ticker)
inner join dim_date on returns.price_date = dim_date.date_key
