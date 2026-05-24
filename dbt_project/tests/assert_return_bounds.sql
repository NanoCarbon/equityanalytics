{{ config(severity='warn') }}

-- Warn when a daily return exceeds ±200% and is not a documented artifact.
--
-- SEVERITY: WARN (not ERROR)
-- This test produces investigative signal rather than blocking the build.
-- Returns in the 50-200% range are common for index-constituent biotechs,
-- meme stocks (GME), and COVID-event names — all legitimate. The ±200%
-- threshold catches only truly extreme moves that almost certainly indicate
-- a data quality issue (merger artifacts, reverse-split boundary rows, etc.).
--
-- HOW IT WORKS
-- Returns any row in fact_daily_prices where |daily_return| > 200%, excluding
-- ticker+date pairs documented as confirmed artifacts below.
-- The test WARNS when a new extreme return appears that has not been acknowledged.
--
-- THRESHOLD RATIONALE
-- ±200% was chosen because:
--   - Legitimate 1-day moves of 50-200% occur regularly in S&P SmallCap 600
--     (biotech drug-trial readouts, meme-stock events, COVID re-openings)
--   - Returns exceeding ±200% in normal equities are almost always an artifact:
--     merger/spin-off boundary rows, reversed stock splits, near-zero prior prices
--   - The 22,525% CHRD artifact, 1,753% ASTH spike, and 706% TGTX move are all
--     well above 200%; none require any special-casing at this threshold
--
-- ADDING KNOWN EXCLUSIONS
-- When the test warns on a new ticker, investigate:
--   1. Was there a confirmed stock split? Verify yfinance returned correct
--      split-adjusted prices for ALL historical dates (not just recent ones).
--   2. Was there a corporate action (spin-off, merger, ticker reassignment)?
--   3. Is the prior-day close price suspiciously close to zero?
-- Once confirmed as a data artifact and not a legitimate extreme move, add the
-- entry here with the date and reason, then re-run to confirm it passes.

with known_exclusions as (
    select ticker, price_date from (values
        -- CHRD (Chord Energy) formed 2022-08 from Oasis Petroleum + Whiting Petroleum merger.
        -- RAW.PRICES contains Oasis pre-merger price history (near-zero micro-cap era);
        -- the LAG window carries forward a $0.11 close into the first CHRD day,
        -- producing a 22,525% return artifact. This is the only confirmed bad row.
        ('CHRD', '2020-11-20'::date)
    ) as t (ticker, price_date)
)

select
    f.ticker,
    f.price_date,
    f.daily_return
from {{ ref('fact_daily_prices') }} f
left join known_exclusions e
    on  f.ticker     = e.ticker
    and f.price_date = e.price_date
where e.ticker is null
  and (f.daily_return > 2.0 or f.daily_return < -2.0)
order by abs(f.daily_return) desc
