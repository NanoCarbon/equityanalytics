with companies as (
    select * from {{ ref('stg_companies') }}
),

prices as (
    select
        ticker,
        min(price_date) as first_trading_date,
        max(price_date) as last_trading_date
    from {{ ref('stg_prices') }}
    group by ticker
)

select
    companies.ticker,
    companies.company_name,
    companies.sector,
    companies.industry,
    companies.market_cap_usd,
    prices.first_trading_date,
    prices.last_trading_date,
    companies.extracted_at                as last_updated_at

from companies
left join prices using (ticker)