-- Grain: one row per (ticker, price_date).
-- RAW.PRICES is append-only with a 5-day overlap window on every incremental run,
-- so duplicates on (ticker, price_date) are expected at the RAW layer.
-- QUALIFY keeps only the most-recently extracted row per (ticker, price_date),
-- making the grain clean for all downstream models and tests.

with source as (
    select * from {{ source('raw', 'prices') }}
),

renamed as (
    select
        ticker,
        to_date(dateadd(second, date / 1000000000, '1970-01-01')) as price_date,
        cast(open as float)                     as open_price,
        cast(high as float)                     as high_price,
        cast(low as float)                      as low_price,
        cast(close as float)                    as close_price,
        cast(volume as bigint)                  as volume,
        to_timestamp(extracted_at / 1000000000) as extracted_at
    from source
    where close is not null
      and volume > 0
    qualify row_number() over (
        partition by ticker, to_date(dateadd(second, date / 1000000000, '1970-01-01'))
        order by extracted_at desc
    ) = 1
)

select * from renamed