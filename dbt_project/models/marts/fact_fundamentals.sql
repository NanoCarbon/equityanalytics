{{ config(
    materialized='incremental',
    unique_key=['ticker', 'period_end_date', 'frequency'],
    on_schema_change='sync_all_columns',
    schema='marts'
) }}

{#
    Fact table for fundamental financial data.
    Grain: one row per ticker × reporting period × frequency (annual/quarterly).

    Joins to dim_security for referential integrity (only tickers we track).
    Joins to dim_date to confirm the period_end_date exists in our calendar.

    Incremental strategy: append new reporting periods only.
    New tickers require --full-refresh (same pattern as fact_daily_prices).
#}

with fundamentals as (
    select * from {{ ref('int_fundamentals_pivoted') }}
    {% if is_incremental() %}
    where period_end_date > (select max(period_end_date) from {{ this }})
    {% endif %}
),

dim_security as (
    select ticker from {{ ref('dim_security') }}
),

dim_date as (
    select date_key from {{ ref('dim_date') }}
)

select
    f.ticker,
    f.period_end_date,
    f.frequency,

    -- Income statement
    f.total_revenue,
    f.cost_of_revenue,
    f.gross_profit,
    f.operating_income,
    f.net_income,
    f.ebitda,
    f.ebit,
    f.research_and_development,
    f.selling_general_admin,
    f.interest_expense,
    f.diluted_eps,
    f.basic_eps,
    f.diluted_shares,
    f.tax_provision,

    -- Balance sheet
    f.total_assets,
    f.total_liabilities,
    f.stockholders_equity,
    f.cash_and_equivalents,
    f.cash_and_short_term_investments,
    f.total_debt,
    f.net_debt,
    f.current_assets,
    f.current_liabilities,
    f.inventory,
    f.accounts_receivable,
    f.accounts_payable,
    f.retained_earnings,
    f.net_ppe,

    -- Cash flow
    f.operating_cash_flow,
    f.capital_expenditure,
    f.free_cash_flow,
    f.investing_cash_flow,
    f.financing_cash_flow,
    f.depreciation_and_amortization,
    f.stock_based_compensation,
    f.dividends_paid,
    f.share_repurchases,

    -- Derived margins
    f.gross_margin,
    f.operating_margin,
    f.net_margin

from fundamentals f
inner join dim_security using (ticker)
inner join dim_date on f.period_end_date = dim_date.date_key
