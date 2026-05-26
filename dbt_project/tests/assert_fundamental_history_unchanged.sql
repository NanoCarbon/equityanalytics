-- Assert that core fundamental values are not null for a deterministic ~1% sample
-- of equity tickers across their two most recent annual reporting periods.
--
-- PURPOSE
-- Catches unintended data corruption across a broad, representative sample:
--   - A pipeline bug that zeros out or corrupts pivoted columns
--   - A dbt model change that causes pivoted columns to silently go null
--   - A yfinance API change that renames line items, breaking the EAV pivot
--   - Regression from changes to int_fundamentals_pivoted
--
-- SAMPLING STRATEGY
-- ABS(HASH(ticker)) % 67 = 0 selects ~1/67 of tickers deterministically.
-- From ~6,668 equities this yields ~100 tickers. The sample is stable across
-- runs (same tickers selected every time) and requires no manual maintenance.
-- Increase or decrease the modulus to adjust sample size.
--
-- VIOLATION CONDITION
-- Both total_revenue AND net_income are null for a sampled ticker's recent period.
-- A single null column is tolerable (some companies genuinely have no revenue or
-- negative net income). Both null together strongly indicates a pivot failure.

with sampled_tickers as (
    -- Deterministic ~1% sample of equity tickers present in fact_fundamentals
    select distinct ticker
    from {{ ref('fact_fundamentals') }}
    where frequency = 'annual'
      and abs(hash(ticker)) % 67 = 0
),

recent_annual_periods as (
    -- Two most recent annual periods per sampled ticker
    select
        f.ticker,
        f.period_end_date,
        f.frequency,
        f.total_revenue,
        f.net_income,
        f.diluted_eps,
        f.total_assets
    from {{ ref('fact_fundamentals') }} f
    inner join sampled_tickers s using (ticker)
    where f.frequency = 'annual'
    qualify row_number() over (
        partition by f.ticker
        order by f.period_end_date desc
    ) <= 2
),

violations as (
    select
        ticker,
        period_end_date,
        frequency,
        total_revenue,
        net_income,
        diluted_eps,
        total_assets
    from recent_annual_periods
    where
        -- Both income statement pivots null = strong signal of corruption or rename
        (total_revenue is null and net_income is null)
        or
        -- Balance sheet pivot also missing = full pivot failure
        (total_revenue is null and total_assets is null)
)

select * from violations
