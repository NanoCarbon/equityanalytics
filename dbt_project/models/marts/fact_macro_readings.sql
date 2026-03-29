{{ config(
    materialized='incremental',
    unique_key=['series_id', 'observation_date'],
    on_schema_change='sync_all_columns'
) }}

with macro as (
    select * from {{ ref('stg_macro_indicators') }}
    {% if is_incremental() %}
    where observation_date > (select max(observation_date) from {{ this }})
    {% endif %}
),

dim_date as (
    select date_key from {{ ref('dim_date') }}
)

select
    macro.series_id,
    macro.series_name,
    macro.observation_date,
    macro.value,
    macro.extracted_at

from macro
inner join dim_date on macro.observation_date = dim_date.date_key