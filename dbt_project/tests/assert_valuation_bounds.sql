{{ config(severity='warn') }}

-- Assert that valuation ratio outliers are within tolerable bounds.
-- Flags rows where PE > 1000 (data error, not genuine) or margins outside [-1, 1].
-- Severity: warn — extreme but real values (e.g. deeply distressed companies) can exist.

select
    ticker,
    snapshot_date,
    trailing_pe,
    forward_pe,
    gross_margin,
    operating_margin,
    profit_margin
from {{ ref('fact_valuation_snapshot') }}
where
    (trailing_pe    is not null and trailing_pe    > 1000)
    or (forward_pe  is not null and forward_pe     > 1000)
    or (gross_margin     is not null and gross_margin     not between -1.0 and 1.0)
    or (operating_margin is not null and operating_margin not between -1.0 and 1.0)
    or (profit_margin    is not null and profit_margin    not between -1.0 and 1.0)
