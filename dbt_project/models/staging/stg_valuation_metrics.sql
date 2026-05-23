{{ config(schema='staging') }}

with source as (
    select * from {{ source('raw', 'valuation_metrics') }}
),

cleaned as (
    select
        ticker,
        to_date(snapshot_date)                                   as snapshot_date,

        -- Valuation ratios
        try_cast(trailingpe as float)                            as trailing_pe,
        try_cast(forwardpe as float)                             as forward_pe,
        try_cast(pricetobook as float)                           as price_to_book,
        try_cast(pricetosalestrailing12months as float)          as price_to_sales,
        try_cast(enterprisetoebitda as float)                    as ev_to_ebitda,
        try_cast(enterprisetorevenue as float)                   as ev_to_revenue,
        try_cast(pegratio as float)                              as peg_ratio,

        -- Profitability (stored as decimals, e.g. 0.45 = 45%)
        try_cast(grossmargins as float)                          as gross_margin,
        try_cast(operatingmargins as float)                      as operating_margin,
        try_cast(profitmargins as float)                         as profit_margin,
        try_cast(ebitdamargins as float)                         as ebitda_margin,
        try_cast(returnonequity as float)                        as return_on_equity,
        try_cast(returnonassets as float)                        as return_on_assets,

        -- Leverage & liquidity
        try_cast(debttoequity as float)                          as debt_to_equity,
        try_cast(currentratio as float)                          as current_ratio,
        try_cast(quickratio as float)                            as quick_ratio,

        -- Per-share
        try_cast(trailingeps as float)                           as trailing_eps,
        try_cast(forwardeps as float)                            as forward_eps,
        try_cast(bookvalue as float)                             as book_value_per_share,
        try_cast(revenuepershare as float)                       as revenue_per_share,

        -- Growth (stored as decimals)
        try_cast(earningsgrowth as float)                        as earnings_growth,
        try_cast(revenuegrowth as float)                         as revenue_growth,
        try_cast(earningsquarterlygrowth as float)               as earnings_quarterly_growth,

        -- Dividends
        try_cast(dividendyield as float)                         as dividend_yield,
        try_cast(payoutratio as float)                           as payout_ratio,
        try_cast(trailingannualdividendyield as float)           as trailing_annual_dividend_yield,

        -- Absolute values (try_cast guards against yfinance string sentinels)
        try_cast(marketcap as bigint)                            as market_cap,
        try_cast(enterprisevalue as bigint)                      as enterprise_value,
        try_cast(totalrevenue as bigint)                         as total_revenue,
        try_cast(ebitda as bigint)                               as ebitda,
        try_cast(freecashflow as bigint)                         as free_cash_flow,
        try_cast(operatingcashflow as bigint)                    as operating_cash_flow,
        try_cast(totaldebt as bigint)                            as total_debt,
        try_cast(totalcash as bigint)                            as total_cash,

        -- Risk
        try_cast(beta as float)                                  as beta,

        cast(extracted_at as timestamp_ntz)                      as extracted_at

    from source
    where ticker is not null
)

select * from cleaned
