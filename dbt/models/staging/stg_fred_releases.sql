-- Grain: one row per release_id.
-- Full overwrite monthly — no deduplication needed.

with source as (
    select * from {{ source('raw', 'fred_releases') }}
),

cleaned as (
    select
        cast(release_id    as integer) as release_id,
        release_name,
        press_release,
        link,
        notes,
        to_timestamp(extracted_at / 1000000000) as extracted_at
    from source
    where release_id is not null
)

select * from cleaned
