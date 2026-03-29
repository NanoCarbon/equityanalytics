{{ config(schema='staging') }}

with prices as (
    select * from {{ ref('stg_prices') }}
),

with_daily_return as (
    select
        ticker,
        price_date,
        close_price,
        volume,

        (close_price - lag(close_price) over (
            partition by ticker order by price_date
        )) / nullif(lag(close_price) over (
            partition by ticker order by price_date
        ), 0)                                               as daily_return,

        max(close_price) over (
            partition by ticker
            order by price_date
            rows between 251 preceding and current row
        )                                                   as week_52_high,

        min(close_price) over (
            partition by ticker
            order by price_date
            rows between 251 preceding and current row
        )                                                   as week_52_low

    from prices
),

with_volatility as (
    select
        *,
        stddev(daily_return) over (
            partition by ticker
            order by price_date
            rows between 29 preceding and current row
        ) * sqrt(252)                                       as rolling_30d_vol_annualized

    from with_daily_return
)

select * from with_volatility
where daily_return is not null