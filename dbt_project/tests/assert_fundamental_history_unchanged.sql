-- Assert that historical fundamental values have not changed for a fixed set of
-- anchor observations.
--
-- PURPOSE
-- Financial statements use a full-overwrite load strategy precisely because
-- yfinance can return retroactively restated values. This test intentionally
-- does NOT catch all restatements — that would defeat the purpose of the overwrite.
-- Instead it catches unintended data corruption:
--   - A pipeline bug that zeros out or corrupts specific rows
--   - A dbt model change that alters how values are pivoted or cast
--   - A yfinance API change that renames line items, causing pivoted columns
--     to silently go null for historical periods
--
-- For annual periods, revenue and net_income are stable enough to baseline.
-- EPS is more volatile due to share count restatements — use with wider tolerance.
--
-- MAINTENANCE
-- Run the query below to populate baseline values after the initial backfill:
--
--   SELECT ticker, period_end_date, frequency,
--          ROUND(total_revenue, 0) AS total_revenue,
--          ROUND(net_income, 0)    AS net_income,
--          ROUND(diluted_eps, 4)   AS diluted_eps
--   FROM EQUITY_ANALYTICS.MARTS.FACT_FUNDAMENTALS
--   WHERE ticker IN ('AAPL', 'JPM', 'XOM')
--     AND frequency = 'annual'
--     AND period_end_date BETWEEN '2021-01-01' AND '2023-12-31'
--   ORDER BY ticker, period_end_date;

with expected_fundamentals as (
    select * from (values
        -- ticker,  period_end_date,      frequency,  exp_revenue,  exp_net_income,  exp_diluted_eps
        ('AAPL', '2022-09-24'::date, 'annual', null::float, null::float, null::float),
        ('AAPL', '2021-09-25'::date, 'annual', null::float, null::float, null::float),
        ('JPM',  '2022-12-31'::date, 'annual', null::float, null::float, null::float),
        ('JPM',  '2021-12-31'::date, 'annual', null::float, null::float, null::float),
        ('XOM',  '2022-12-31'::date, 'annual', null::float, null::float, null::float),
        ('XOM',  '2021-12-31'::date, 'annual', null::float, null::float, null::float)
    ) as t (ticker, period_end_date, frequency, exp_revenue, exp_net_income, exp_diluted_eps)
    where exp_revenue is not null  -- skip until baseline is populated
),

violations as (
    select
        f.ticker,
        f.period_end_date,
        f.frequency,
        f.total_revenue      as actual_revenue,
        e.exp_revenue        as expected_revenue,
        f.net_income         as actual_net_income,
        e.exp_net_income     as expected_net_income,
        f.diluted_eps        as actual_diluted_eps,
        e.exp_diluted_eps    as expected_diluted_eps
    from {{ ref('fact_fundamentals') }} f
    inner join expected_fundamentals e
        on  f.ticker         = e.ticker
        and f.period_end_date = e.period_end_date
        and f.frequency      = e.frequency
    where
        -- Revenue tolerance: 0.1% of expected (restatements are larger than rounding)
        abs(f.total_revenue - e.exp_revenue) > abs(e.exp_revenue) * 0.001
        or
        -- Net income tolerance: 0.1% (more volatile than revenue)
        abs(f.net_income - e.exp_net_income) > abs(e.exp_net_income) * 0.001
        or
        -- EPS tolerance: 0.01 per share (covers minor share count restatements)
        abs(f.diluted_eps - e.exp_diluted_eps) > 0.01
)

select * from violations
