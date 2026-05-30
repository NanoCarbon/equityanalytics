-- Grain: one row per series_id (PK-enforced in RAW).
-- Canonical selection table for FRED extraction — persists across catalog refreshes.

with source as (
    select * from {{ source('raw', 'fred_selection') }}
)

select
    series_id,
    category,
    local_name,
    is_active,
    added_at,
    deactivated_at,
    deactivation_reason
from source
