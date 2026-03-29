{{ config(materialized='table', schema='marts') }}

with date_spine as (
    select dateadd(day, seq4(), '2010-01-01'::date) as date_day
    from table(generator(rowcount => 6000))
)

select
    date_day                                            as date_key,
    extract(year from date_day)                        as year,
    extract(quarter from date_day)                     as quarter,
    extract(month from date_day)                       as month,
    extract(week from date_day)                        as week_of_year,
    extract(dayofweek from date_day)                   as day_of_week,
    dayname(date_day)                                  as day_name,
    monthname(date_day)                                as month_name,
    case when dayofweek(date_day) in (0, 6)
         then false else true end                      as is_weekday,
    'Q' || extract(quarter from date_day)
         || ' ' || extract(year from date_day)         as fiscal_quarter_label

from date_spine
```

Two changes — start date moved from `2020-01-01` to `2010-01-01` and rowcount bumped from `3000` to `6000` to cover the longer range.

Save, then run:
```
dbt build --select dim_date fact_daily_prices --full-refresh