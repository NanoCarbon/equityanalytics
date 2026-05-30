-- Grain: one row per series_id.
-- Full overwrite monthly — no deduplication needed.
-- ~800K rows; materialized as a view (passthrough with light typing).

with source as (
    select * from {{ source('raw', 'fred_series_catalog') }}
),

cleaned as (
    select
        series_id,
        title,
        frequency,
        frequency_short,
        units,
        units_short,
        seasonal_adjustment,
        cast(popularity       as integer) as popularity,
        cast(group_popularity as integer) as group_popularity,
        observation_start,
        observation_end,
        last_updated,
        cast(release_id       as integer) as release_id,
        release_name,
        notes,
        to_timestamp(extracted_at / 1000000000) as extracted_at
    from source
    where series_id is not null
)

select * from cleaned
