-- Grain: one row per ticker (PK-enforced in RAW).
-- Canonical ticker source for all yfinance DAGs.
-- FK parent for PRICES, COMPANY_INFO, FINANCIAL_STATEMENTS, VALUATION_METRICS,
-- DIVIDENDS_AND_SPLITS, EARNINGS_HISTORY, ANALYST_RECOMMENDATIONS, ANALYST_PRICE_TARGETS.

with source as (
    select * from {{ source('raw', 'ticker_universe') }}
)

select
    ticker,
    source,
    is_active,
    is_equity,
    added_at,
    deactivated_at,
    deactivation_reason
from source
