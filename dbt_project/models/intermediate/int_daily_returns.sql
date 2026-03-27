with prices as (
    select * from {{ ref('stg_prices') }}
),

with_returns as (
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

        stddev(
            (close_price - lag(close_price) over (
                partition by ticker order by price_date
            )) / nullif(lag(close_price) over (
                partition by ticker order by price_date
            ), 0)
        ) over (
            partition by ticker
            order by price_date
            rows between 29 preceding and current row
        ) * sqrt(252)                                       as rolling_30d_vol_annualized,

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
)

select * from with_returns
where daily_return is not null