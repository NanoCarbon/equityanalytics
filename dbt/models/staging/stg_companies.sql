with source as (
    select * from {{ source('raw', 'company_info') }}
),

renamed as (
    select
        ticker,
        company_name,
        coalesce(sector, 'Unknown')         as sector,
        coalesce(industry, 'Unknown')       as industry,
        cast(market_cap as bigint)          as market_cap_usd,
        cast(extracted_at as timestamp_ntz) as extracted_at
    from source
),

deduped as (
    -- RAW.COMPANY_INFO is append-only; backfill + DAG can insert the same ticker
    -- multiple times. Keep only the most recently extracted row per ticker.
    select *
    from renamed
    qualify row_number() over (
        partition by ticker
        order by extracted_at desc
    ) = 1
)

select * from deduped