{{ config(
    materialized='incremental',
    unique_key=['ticker', 'price_date'],
    on_schema_change='sync_all_columns'
) }}

with returns as (
    select * from {{ ref('int_daily_returns') }}
    {% if is_incremental() %}
    where price_date > (select max(price_date) from {{ this }})
    {% endif %}
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