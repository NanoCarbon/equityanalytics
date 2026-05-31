-- Grain: one row per (ticker, period_date).
-- Full overwrite on each run — no deduplication needed.

with source as (
    select * from {{ source('raw', 'earnings_history') }}
),

cleaned as (
    select
        ticker,
        to_date(dateadd(second, quarter / 1000000000, '1970-01-01')) as period_date,
        cast(epsactual      as float) as eps_actual,
        cast(epsestimate    as float) as eps_estimate,
        cast(epsdifference  as float) as eps_difference,
        cast(surprisepercent as float) as surprise_pct,
        to_timestamp(extracted_at / 1000000000) as extracted_at
    from source
    where ticker is not null
)

select * from cleaned
