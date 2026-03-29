-- Assert no duplicate (series_id, observation_date) combinations in fact_macro_readings.

select
    series_id,
    observation_date,
    count(*) as row_count
from {{ ref('fact_macro_readings') }}
group by series_id, observation_date
having count(*) > 1
