{{ config(
    materialized='incremental',
    unique_key=['ticker', 'snapshot_date'],
    on_schema_change='sync_all_columns',
    schema='marts',
    cluster_by=['snapshot_date', 'ticker']
) }}

{#
    Fact table for point-in-time valuation and ratio metrics.
    Grain: one row per ticker × snapshot_date.

    These come from yfinance .info and represent the market's current
    valuation of each security. By snapshotting daily, we build a time
    series of how PE, margins, etc. evolve — useful for questions like
    "When was AAPL's PE ratio at its lowest in the last year?"

    Incremental: append new snapshot dates only.

    Clustering: snapshot_date first because most queries filter by date range
    across many tickers (matching the daily_prices access pattern).
#}

with valuations as (
    select * from {{ ref('stg_valuation_metrics') }}
    {% if is_incremental() %}
    where snapshot_date > (select max(snapshot_date) from {{ this }})
    {% endif %}
),

dim_security as (
    select ticker from {{ ref('dim_security') }}
)

select
    v.ticker,
    v.snapshot_date,

    -- Valuation ratios
    v.trailing_pe,
    v.forward_pe,
    v.price_to_book,
    v.price_to_sales,
    v.ev_to_ebitda,
    v.ev_to_revenue,
    v.peg_ratio,

    -- Profitability
    v.gross_margin,
    v.operating_margin,
    v.profit_margin,
    v.ebitda_margin,
    v.return_on_equity,
    v.return_on_assets,

    -- Leverage & liquidity
    v.debt_to_equity,
    v.current_ratio,
    v.quick_ratio,

    -- Per-share
    v.trailing_eps,
    v.forward_eps,
    v.book_value_per_share,
    v.revenue_per_share,

    -- Growth
    v.earnings_growth,
    v.revenue_growth,
    v.earnings_quarterly_growth,

    -- Dividends
    v.dividend_yield,
    v.payout_ratio,

    -- Absolute values
    v.market_cap,
    v.enterprise_value,
    v.total_revenue,
    v.ebitda,
    v.free_cash_flow,
    v.operating_cash_flow,
    v.total_debt,
    v.total_cash,

    -- Risk
    v.beta

from valuations v
inner join dim_security using (ticker)