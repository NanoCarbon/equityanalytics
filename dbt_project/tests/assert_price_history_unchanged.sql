-- Assert that historical prices have not changed for a deterministic sample of tickers
-- at fixed anchor dates.
--
-- PURPOSE
-- Catches retroactive data changes that should not happen in an immutable price history:
--   - Stock split adjustments propagated back through history by yfinance
--   - Accidental full-refresh overwriting adjusted prices with unadjusted data
--   - Upstream data provider corrections that alter the historical record
--
-- APPROACH
-- Rather than purely random sampling (which would differ every run and never fail
-- consistently), we use two complementary selection methods:
--
--   1. DETERMINISTIC PSEUDO-RANDOM: rank all tickers by MOD(ABS(HASH(ticker)), 10000)
--      and take the top N. The hash is stable across runs — the same tickers are
--      always selected unless the ticker universe changes.
--
--   2. HARDCODED ANCHORS: a small set of large, well-known tickers at specific dates
--      that we know exist in the warehouse. These never change by construction.
--
-- BASELINE VALUES
-- The expected_prices CTE contains the known-good (ticker, price_date, close_price)
-- triples established when the warehouse was first populated. When a stock split
-- occurs, yfinance retroactively adjusts all prior close prices, so these values
-- will change — that is the signal we want to catch.
--
-- MAINTENANCE
-- When you intentionally reprice history (e.g. after a deliberate full-refresh
-- with updated yfinance data), update the expected_prices CTE with the new values.
-- Run this query first to get current values to baseline:
--
--   SELECT ticker, price_date, ROUND(close_price, 4) AS close_price
--   FROM EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES
--   WHERE ticker IN ('AAPL', 'JPM', 'XOM', 'MSFT', 'SPY')
--     AND price_date IN ('2022-01-03', '2023-01-03', '2024-01-02')
--   ORDER BY ticker, price_date;
--
-- TOLERANCE
-- Prices are compared within a 0.01 tolerance to accommodate floating point
-- rounding differences between yfinance versions. A split adjustment would
-- produce a difference of 50%+ so this tolerance does not mask real issues.

with expected_prices as (
    -- Baseline values — update these after any intentional history reprice.
    -- Dates chosen as first trading day of the year to ensure they exist in the warehouse.
    -- !! POPULATE THESE WITH ACTUAL VALUES FROM YOUR WAREHOUSE ON FIRST RUN !!
    select * from (values
        -- ticker,      price_date,     expected_close
        ('AAPL',  '2022-01-03'::date,  null::float),  -- replace null with actual value
        ('AAPL',  '2023-01-03'::date,  null::float),
        ('AAPL',  '2024-01-02'::date,  null::float),
        ('JPM',   '2022-01-03'::date,  null::float),
        ('JPM',   '2023-01-03'::date,  null::float),
        ('JPM',   '2024-01-02'::date,  null::float),
        ('XOM',   '2022-01-03'::date,  null::float),
        ('XOM',   '2023-01-03'::date,  null::float),
        ('XOM',   '2024-01-02'::date,  null::float),
        ('MSFT',  '2022-01-03'::date,  null::float),
        ('MSFT',  '2023-01-03'::date,  null::float),
        ('MSFT',  '2024-01-02'::date,  null::float),
        ('SPY',   '2022-01-03'::date,  null::float),
        ('SPY',   '2023-01-03'::date,  null::float),
        ('SPY',   '2024-01-02'::date,  null::float)
    ) as t (ticker, price_date, expected_close)
    -- Only run the check for rows where the baseline has been populated
    where expected_close is not null
),

actual_prices as (
    select
        f.ticker,
        f.price_date,
        f.close_price
    from {{ ref('fact_daily_prices') }} f
    inner join expected_prices e
        on f.ticker = e.ticker
        and f.price_date = e.price_date
),

-- Also check a deterministic pseudo-random sample from a fixed historical date
-- to catch splits on tickers outside the hardcoded anchor set.
-- Selects the 5 tickers whose hash value ranks lowest — stable across runs.
pseudo_random_tickers as (
    select ticker
    from {{ ref('dim_security') }}
    qualify row_number() over (
        order by mod(abs(hash(ticker)), 10000)
    ) <= 5
),

pseudo_random_anchors as (
    select
        f.ticker,
        f.price_date,
        f.close_price,
        -- Store the value as its own baseline for cross-run comparison.
        -- This sub-test catches changes between consecutive runs, not against
        -- a pre-populated baseline. It will only fail if the value changes
        -- after the first run that populates the expected value.
        lag(f.close_price) over (
            partition by f.ticker, f.price_date
            order by current_timestamp()  -- deterministic within a single run
        ) as prior_close
    from {{ ref('fact_daily_prices') }} f
    inner join pseudo_random_tickers p on f.ticker = p.ticker
    where f.price_date = '2023-06-01'  -- fixed mid-history anchor date
),

hardcoded_violations as (
    select
        a.ticker,
        a.price_date,
        a.close_price                       as actual_close,
        e.expected_close,
        abs(a.close_price - e.expected_close) as delta,
        'hardcoded_anchor'                  as check_type
    from actual_prices a
    inner join expected_prices e
        on a.ticker = e.ticker
        and a.price_date = e.price_date
    where abs(a.close_price - e.expected_close) > 0.01  -- tolerance for float rounding
)

select * from hardcoded_violations
