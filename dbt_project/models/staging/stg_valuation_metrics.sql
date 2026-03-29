{{ config(schema='staging') }}

with source as (
    select * from {{ source('raw', 'valuation_metrics') }}
),

cleaned as (
    select
        ticker,
        to_date(snapshot_date)                              as snapshot_date,

        -- Valuation ratios
        cast(trailingpe as float)                           as trailing_pe,
        cast(forwardpe as float)                            as forward_pe,
        cast(pricetobook as float)                          as price_to_book,
        cast(pricetosalestrailing12months as float)         as price_to_sales,
        cast(enterprisetoebitda as float)                   as ev_to_ebitda,
        cast(enterprisetorevenue as float)                  as ev_to_revenue,
        cast(pegratio as float)                             as peg_ratio,

        -- Profitability (stored as decimals, e.g. 0.45 = 45%)
        cast(grossmargins as float)                         as gross_margin,
        cast(operatingmargins as float)                     as operating_margin,
        cast(profitmargins as float)                        as profit_margin,
        cast(ebitdamargins as float)                        as ebitda_margin,
        cast(returnonequity as float)                       as return_on_equity,
        cast(returnonassets as float)                       as return_on_assets,

        -- Leverage & liquidity
        cast(debttoequity as float)                         as debt_to_equity,
        cast(currentratio as float)                         as current_ratio,
        cast(quickratio as float)                           as quick_ratio,

        -- Per-share
        cast(trailingeps as float)                          as trailing_eps,
        cast(forwardeps as float)                           as forward_eps,
        cast(bookvalue as float)                            as book_value_per_share,
        cast(revenuepershare as float)                      as revenue_per_share,

        -- Growth (stored as decimals)
        cast(earningsgrowth as float)                       as earnings_growth,
        cast(revenuegrowth as float)                        as revenue_growth,
        cast(earningsquarterlygrowth as float)              as earnings_quarterly_growth,

        -- Dividends
        cast(dividendyield as float)                        as dividend_yield,
        cast(payoutratio as float)                          as payout_ratio,
        cast(trailingannualdividendyield as float)          as trailing_annual_dividend_yield,

        -- Absolute values
        cast(marketcap as bigint)                           as market_cap,
        cast(enterprisevalue as bigint)                     as enterprise_value,
        cast(totalrevenue as bigint)                        as total_revenue,
        cast(ebitda as bigint)                              as ebitda,
        cast(freecashflow as bigint)                        as free_cash_flow,
        cast(operatingcashflow as bigint)                   as operating_cash_flow,
        cast(totaldebt as bigint)                           as total_debt,
        cast(totalcash as bigint)                           as total_cash,

        -- Risk
        cast(beta as float)                                 as beta,

        cast(extracted_at as timestamp_ntz)                 as extracted_at

    from source
    where ticker is not null
)

select * from cleaned
