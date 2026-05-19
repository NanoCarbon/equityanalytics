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
)

select * from renamed