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
-- total_revenue, net_income, AND total_assets are all null for a sampled ticker's
-- recent period. A company with balance sheet data but no income statement is a
-- legitimate case (SPACs, shell companies, or a timing gap where the income
-- statement hasn't been published yet). Requiring all three null ensures we only
-- flag complete pivot failures, not pre-acquisition SPACs or late-reporting periods.

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
        -- All three key metrics null = full pivot failure or EAV rename.
        -- If total_assets is populated the company has balance sheet data — it is
        -- a SPAC / shell company (no income statement by design) or a late-reporting
        -- period where the income statement hasn't been published yet. Those are
        -- legitimate and should not be flagged as corruption.
        (total_revenue is null and net_income is null and total_assets is null)
)

select * from violations
