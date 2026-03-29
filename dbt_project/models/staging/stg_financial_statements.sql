{{ config(schema='staging') }}

with source as (
    select * from {{ source('raw', 'financial_statements') }}
),

cleaned as (
    select
        ticker,
        statement_type,
        frequency,
        to_date(period_end_date)                          as period_end_date,
        line_item,
        cast(value as float)                              as value,
        cast(extracted_at as timestamp_ntz)               as extracted_at

    from source
    where value is not null
      and ticker is not null
      and line_item is not null
)

select * from cleaned
