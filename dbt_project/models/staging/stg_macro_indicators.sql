with source as (
    select * from {{ source('raw', 'macro_indicators') }}
),

renamed as (
    select
        series_id,
        series_name,
        to_date(dateadd(second, date / 1000000000, '1970-01-01')) as observation_date,
        cast(value as float)                                       as value,
        to_timestamp(extracted_at / 1000000000)                   as extracted_at
    from source
    where value is not null
)

select * from renamed