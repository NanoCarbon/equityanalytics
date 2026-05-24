-- Grain: one row per (ticker, snapshot_date).
-- Weekly append — each run adds one new consensus snapshot per ticker.
-- No deduplication needed (snapshot_date is a calendar date, not a timestamp).

with source as (
    select * from {{ source('raw', 'analyst_price_targets') }}
),

cleaned as (
    select
        ticker,
        cast(snapshot_date as date)   as snapshot_date,
        cast("CURRENT" as float)      as current_target,
        cast(high     as float)       as high_target,
        cast(low      as float)       as low_target,
        cast(mean     as float)       as mean_target,
        cast(median   as float)       as median_target,
        to_timestamp(extracted_at / 1000000000) as extracted_at
    from source
    where ticker is not null
)

select * from cleaned
