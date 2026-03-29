-- Assert no duplicate (ticker, snapshot_date) combinations in fact_valuation_snapshot.

select
    ticker,
    snapshot_date,
    count(*) as row_count
from {{ ref('fact_valuation_snapshot') }}
group by ticker, snapshot_date
having count(*) > 1
