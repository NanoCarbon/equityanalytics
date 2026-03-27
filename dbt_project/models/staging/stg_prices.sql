with source as (
    select * from {{ source('raw', 'prices') }}
),

renamed as (
    select
        ticker,
        cast(date as date)                  as price_date,
        cast(open as float)                 as open_price,
        cast(high as float)                 as high_price,
        cast(low as float)                  as low_price,
        cast(close as float)                as close_price,
        cast(volume as bigint)              as volume,
        cast(extracted_at as timestamp_ntz) as extracted_at
    from source
    where close is not null
      and volume > 0
)

select * from renamed