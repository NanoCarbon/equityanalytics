-- Grain: one row per (series_id, observation_date).
-- RAW.MACRO_INDICATORS is append-only with a 7-day overlap window on every
-- incremental run. QUALIFY keeps the most-recently extracted row per
-- (series_id, observation_date) so the grain is clean for downstream models.

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
    qualify row_number() over (
        partition by series_id, to_date(dateadd(second, date / 1000000000, '1970-01-01'))
        order by extracted_at desc
    ) = 1
)

select * from renamed