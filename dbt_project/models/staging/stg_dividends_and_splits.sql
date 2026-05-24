-- Grain: one row per (ticker, action_date).
-- Full overwrite on each run — no deduplication needed.

with source as (
    select * from {{ source('raw', 'dividends_and_splits') }}
),

cleaned as (
    select
        ticker,
        to_date(dateadd(second, date / 1000000000, '1970-01-01')) as action_date,
        cast(dividends    as float)                                as dividend_amount,
        cast(stock_splits as float)                                as split_ratio,
        to_timestamp(extracted_at / 1000000000)                    as extracted_at
    from source
    where ticker is not null
      and (dividends != 0 or stock_splits != 0)
)

select * from cleaned
