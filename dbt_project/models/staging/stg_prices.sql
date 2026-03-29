with source as (
    select * from {{ source('raw', 'prices') }}
),

renamed as (
    select
        ticker,
        to_date(dateadd(second, date / 1000000000, '1970-01-01'))  as price_date,
<<<<<<< HEAD
        cast(open as float)                                   as open_price,
        cast(high as float)                                   as high_price,
        cast(low as float)                                    as low_price,
        cast(close as float)                                  as close_price,
        cast(volume as bigint)                                as volume,
=======
        cast(open as float)                                         as open_price,
        cast(high as float)                                         as high_price,
        cast(low as float)                                          as low_price,
        cast(close as float)                                        as close_price,
        cast(volume as bigint)                                      as volume,
>>>>>>> 326b8723906609b6e0f7731800f5fded716ee22b
        to_timestamp(extracted_at / 1000000000)                     as extracted_at
    from source
    where close is not null
      and volume > 0
)

select * from renamed