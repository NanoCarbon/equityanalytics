-- Grain: one row per (ticker, period, period_index).
-- period is a month label (e.g. "0m", "1m", "2m", "3m" = last 4 months).
-- period_index is the sequential row number within each period.
-- Full overwrite on each run — no deduplication needed.
-- Note: INDEX is a reserved word in Snowflake — aliased to period_index.

with source as (
    select * from {{ source('raw', 'analyst_recommendations') }}
),

cleaned as (
    select
        ticker,
        period,
        "INDEX"             as period_index,
        strongbuy           as strong_buy,
        buy,
        hold,
        sell,
        strongsell          as strong_sell,
        to_timestamp(extracted_at / 1000000000) as extracted_at
    from source
    where ticker is not null
)

select * from cleaned
