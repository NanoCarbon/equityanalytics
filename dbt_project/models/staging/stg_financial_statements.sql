{{ config(schema='staging') }}

with source as (
    select * from {{ source('raw', 'financial_statements') }}
),

cleaned as (
    select
        ticker,
        statement_type,
        frequency,
        to_date(dateadd(second, period_end_date / 1000000000, '1970-01-01')) as period_end_date,
        line_item,
        cast(value as float)                              as value,
        to_timestamp(extracted_at / 1000000000)           as extracted_at

    from source
    where value is not null
      and ticker is not null
      and line_item is not null
)

select * from cleaned
