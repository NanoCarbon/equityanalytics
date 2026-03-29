import os
import json
import logging
import hashlib
import time
import streamlit as st
import plotly.express as px
import pandas as pd
import snowflake.connector
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

client = Anthropic()

SYSTEM_PROMPT = """You are a financial data analyst assistant with access to an equity analytics data warehouse.

The warehouse has the following tables in EQUITY_ANALYTICS.MARTS schema:

FACT_DAILY_PRICES - grain: one row per ticker per trading day
- TICKER: varchar — stock ticker e.g. 'SPY', 'AAPL', 'BND'
- PRICE_DATE: date — trading date
- CLOSE_PRICE: float — adjusted closing price
- VOLUME: bigint — shares traded
- DAILY_RETURN: float — daily return as decimal e.g. 0.012 means 1.2%
- ROLLING_30D_VOL_ANNUALIZED: float — annualized 30-day rolling volatility
- WEEK_52_HIGH: float — 52 week high
- WEEK_52_LOW: float — 52 week low
- PCT_OF_52W_HIGH: float — close as % of 52 week high

DIM_SECURITY - grain: one row per security
- TICKER: varchar
- COMPANY_NAME: varchar
- SECTOR: varchar
- INDUSTRY: varchar
- MARKET_CAP_USD: bigint

DIM_DATE - grain: one row per calendar day
- DATE_KEY: date
- YEAR: int
- QUARTER: int
- MONTH: int
- DAY_NAME: varchar
- IS_WEEKDAY: boolean
- FISCAL_QUARTER_LABEL: varchar

EQUITY_ANALYTICS.MARTS.FACT_MACRO_READINGS - grain: one row per series per observation date
- SERIES_ID: varchar — indicator code e.g. 'DFF', 'CPIAUCSL', 'T10Y2Y', 'UNRATE'
- SERIES_NAME: varchar — full name e.g. 'Fed Funds Rate', 'CPI Inflation'
- OBSERVATION_DATE: date — date of the observation
- VALUE: float — the indicator value

Series reference:
- DFF: Federal Funds Rate (daily, %)
- CPIAUCSL: Consumer Price Index (monthly, index level)
- T10Y2Y: 10yr minus 2yr Treasury spread (daily, %) — negative = inverted yield curve
- UNRATE: Unemployment Rate (monthly, %)

FACT_FUNDAMENTALS - grain: one row per ticker per reporting period per frequency
- TICKER: varchar — stock ticker e.g. 'AAPL', 'MSFT'
- PERIOD_END_DATE: date — fiscal period end date e.g. '2024-09-28'
- FREQUENCY: varchar — 'annual' or 'quarterly'
- TOTAL_REVENUE: float — total revenue in USD
- COST_OF_REVENUE: float
- GROSS_PROFIT: float
- OPERATING_INCOME: float
- NET_INCOME: float
- EBITDA: float
- EBIT: float
- RESEARCH_AND_DEVELOPMENT: float
- SELLING_GENERAL_ADMIN: float
- INTEREST_EXPENSE: float
- DILUTED_EPS: float — diluted earnings per share
- BASIC_EPS: float
- DILUTED_SHARES: float — diluted share count
- TAX_PROVISION: float
- TOTAL_ASSETS: float
- TOTAL_LIABILITIES: float
- STOCKHOLDERS_EQUITY: float
- CASH_AND_EQUIVALENTS: float
- CASH_AND_SHORT_TERM_INVESTMENTS: float
- TOTAL_DEBT: float
- NET_DEBT: float
- CURRENT_ASSETS: float
- CURRENT_LIABILITIES: float
- INVENTORY: float
- ACCOUNTS_RECEIVABLE: float
- ACCOUNTS_PAYABLE: float
- RETAINED_EARNINGS: float
- NET_PPE: float — net property, plant & equipment
- OPERATING_CASH_FLOW: float
- CAPITAL_EXPENDITURE: float — typically negative
- FREE_CASH_FLOW: float
- INVESTING_CASH_FLOW: float
- FINANCING_CASH_FLOW: float
- DEPRECIATION_AND_AMORTIZATION: float
- STOCK_BASED_COMPENSATION: float
- DIVIDENDS_PAID: float — typically negative
- SHARE_REPURCHASES: float — typically negative
- GROSS_MARGIN: float — decimal e.g. 0.45 = 45%
- OPERATING_MARGIN: float
- NET_MARGIN: float

FACT_VALUATION_SNAPSHOT - grain: one row per ticker per snapshot date
- TICKER: varchar
- SNAPSHOT_DATE: date
- TRAILING_PE: float — trailing 12-month P/E ratio
- FORWARD_PE: float — forward P/E ratio
- PRICE_TO_BOOK: float
- PRICE_TO_SALES: float
- EV_TO_EBITDA: float — enterprise value / EBITDA
- EV_TO_REVENUE: float
- PEG_RATIO: float — PE / earnings growth rate
- GROSS_MARGIN: float — decimal
- OPERATING_MARGIN: float
- PROFIT_MARGIN: float
- EBITDA_MARGIN: float
- RETURN_ON_EQUITY: float
- RETURN_ON_ASSETS: float
- DEBT_TO_EQUITY: float
- CURRENT_RATIO: float
- QUICK_RATIO: float
- TRAILING_EPS: float
- FORWARD_EPS: float
- BOOK_VALUE_PER_SHARE: float
- REVENUE_PER_SHARE: float
- EARNINGS_GROWTH: float — decimal
- REVENUE_GROWTH: float
- EARNINGS_QUARTERLY_GROWTH: float
- DIVIDEND_YIELD: float — decimal
- PAYOUT_RATIO: float
- MARKET_CAP: bigint
- ENTERPRISE_VALUE: bigint
- TOTAL_REVENUE: bigint
- EBITDA: bigint
- FREE_CASH_FLOW: bigint
- OPERATING_CASH_FLOW: bigint
- TOTAL_DEBT: bigint
- TOTAL_CASH: bigint
- BETA: float

Available tickers: Everything in the S&P 500 plus major ETFs like SPY, QQQ, IWM, TLT, GLD.

Rules:
- Return ONLY valid Snowflake SQL, no markdown, no backticks, no explanation
- Always use fully qualified table names: EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES
- For cumulative return charts, use: EXP(SUM(LN(1 + DAILY_RETURN)) OVER (PARTITION BY TICKER ORDER BY PRICE_DATE)) - 1
- For normalized price charts starting at same point, divide each price by the first price for that ticker
- Date range in the warehouse is approximately the last 365 days
- Always include TICKER in SELECT when querying multiple tickers
- Order results by PRICE_DATE ASC for time series charts
- Generate EXACTLY ONE SQL statement — never multiple SELECTs separated by semicolons   # ← ADD
- To compare multiple tickers or time periods, use a single query with IN() clauses, CTEs, or UNION ALL   # ← ADD
- Do not end your SQL with a semicolon
"""

# ── Snowflake connection (cached for the session) ─────────────────────────────

@st.cache_resource
def get_snowflake_connection():
    """
    Create and cache a single Snowflake connection for the Streamlit session.
    st.cache_resource keeps this alive across reruns without reconnecting.
    """
    logger.info("Opening Snowflake connection")
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse="TRANSFORM_WH",
        database="EQUITY_ANALYTICS",
        schema="MARTS",
        authenticator="programmatic_access_token",
        token=os.environ["SNOWFLAKE_TOKEN"],
        network_timeout=30,      # fail fast if network is unreachable
        login_timeout=15,
    )
    logger.info("Snowflake connection established")
    return conn


def execute_sql(sql: str) -> pd.DataFrame:
    conn = get_snowflake_connection()
    QUERY_TIMEOUT_SECONDS = 30

    # Split and execute multiple statements if Claude generates more than one
    statements = [s.strip() for s in sql.strip().split(';') if s.strip()]

    start = time.monotonic()
    try:
        cursor = conn.cursor()
        cursor.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {QUERY_TIMEOUT_SECONDS}")

        if len(statements) == 1:
            cursor.execute(statements[0])
            df = cursor.fetch_pandas_all()
        else:
            frames = []
            for stmt in statements:
                cursor.execute(stmt)
                frames.append(cursor.fetch_pandas_all())
            df = pd.concat(frames, ignore_index=True)

        df.columns = [c.lower() for c in df.columns]
        elapsed = time.monotonic() - start
        logger.info("Query completed in %.2fs, returned %d rows", elapsed, len(df))
        return df
    except snowflake.connector.errors.ProgrammingError as e:
        logger.error("Snowflake query error: %s | SQL: %.200s", e, sql)
        raise
    except Exception as e:
        logger.error("Unexpected error during query: %s", e)
        get_snowflake_connection.clear()
        raise


# ── Query result cache ────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def execute_sql_cached(sql: str) -> pd.DataFrame:
    """
    Thin cache wrapper around execute_sql.
    Identical SQL strings return the cached DataFrame for up to 5 minutes,
    avoiding redundant round-trips for repeated prompts.
    """
    return execute_sql(sql)


# ── LLM calls ─────────────────────────────────────────────────────────────────

def generate_sql(messages: list) -> str:
    start = time.monotonic()
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        system=SYSTEM_PROMPT,
        messages=messages,
        timeout=30,  # seconds — prevents indefinite hang
    )
    elapsed = time.monotonic() - start
    sql = response.content[0].text.strip()
    logger.info("SQL generation completed in %.2fs", elapsed)
    logger.debug("Generated SQL: %.300s", sql)
    return sql


def _parse_chart_config(raw: str) -> dict:
    """
    Parse Claude's chart configuration response to a dict.

    Handles three cases in order:
      1. Raw JSON (ideal)
      2. JSON wrapped in markdown code fences
      3. Partial/malformed JSON — falls back to a safe line-chart default
    """
    text = raw.strip()

    # Strip markdown fences if present
    if text.startswith("```"):
        lines = text.split("\n")
        # Drop opening fence (and optional language tag) and closing fence
        inner = [l for l in lines[1:] if l.strip() != "```"]
        text = "\n".join(inner).strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.warning("Could not parse chart config JSON, using fallback. Raw: %.200s", raw)
        return {
            "chart_type": "line",
            "x": None,       # resolved below in generate_chart
            "y": None,
            "color": None,
            "title": "Chart"
        }


def generate_chart(df: pd.DataFrame, user_prompt: str):
    """Ask Claude how to visualize the data, then build the Plotly figure."""
    col_info = {col: str(df[col].dtype) for col in df.columns}

    start = time.monotonic()
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=300,
        system="""You are a data visualization expert. Given a DataFrame and a user request,
        return ONLY a valid JSON object with no explanation, no markdown, no backticks.
        The JSON must have these exact keys:
        - chart_type: one of 'line', 'bar', 'scatter'
        - x: column name for x axis
        - y: column name for y axis
        - color: column name for color grouping (or null if single series)
        - title: a short descriptive chart title
        Use only column names that exist in the provided columns list.
        Return raw JSON only. Example: {"chart_type": "line", "x": "price_date", "y": "cumulative_return", "color": "ticker", "title": "Cumulative Returns"}""",
        messages=[{
            "role": "user",
            "content": (
                f"User request: {user_prompt}\n"
                f"Available columns: {list(df.columns)}\n"
                f"Sample data: {df.head(2).to_dict()}"
            )
        }],
        timeout=15,
    )
    elapsed = time.monotonic() - start
    logger.info("Chart config generation completed in %.2fs", elapsed)

    config = _parse_chart_config(response.content[0].text)

    # Resolve None axes — fall back to first suitable columns
    cols = list(df.columns)
    if not config.get("x") or config["x"] not in df.columns:
        config["x"] = cols[0]
        logger.warning("x axis not resolved; falling back to '%s'", config["x"])
    if not config.get("y") or config["y"] not in df.columns:
        # Pick first numeric column that isn't x
        numeric_cols = df.select_dtypes("number").columns.tolist()
        config["y"] = next((c for c in numeric_cols if c != config["x"]), cols[-1])
        logger.warning("y axis not resolved; falling back to '%s'", config["y"])
    if config.get("color") and config["color"] not in df.columns:
        config["color"] = None

    chart_builders = {
        "line": px.line,
        "bar": px.bar,
        "scatter": px.scatter,
    }
    build = chart_builders.get(config["chart_type"], px.line)

    fig = build(
        df,
        x=config["x"],
        y=config["y"],
        color=config.get("color"),
        title=config.get("title", ""),
        template="plotly_white"
    )

    fig.update_layout(
        xaxis_title=config["x"].replace("_", " ").title(),
        yaxis_title=config["y"].replace("_", " ").title(),
        legend_title="Ticker" if config.get("color") == "ticker" else ""
    )

    return fig


# ── Streamlit UI ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Equity Analytics Assistant",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Equity Analytics Assistant")

tab1, tab2 = st.tabs(["Chat", "Event Study"])

with tab1:
    with st.sidebar:
        st.header("Example prompts")
        examples = [
            "Compare cumulative returns for SPY, QQQ and IWM over the last year",
            "Show me the 30-day rolling volatility for AAPL, MSFT and GOOGL",
            "Which sector had the highest average daily return last month?",
            "Show me the top 5 tickers by average volume",
            "Compare JPM and GS closing prices over the last 6 months",
            "Show me tickers trading closest to their 52-week high",
            "How did SPY perform during periods when the yield curve was inverted?",
            "Compare SPY daily returns against the Fed funds rate over the last year",
            "Show me the Fed funds rate trend over the last year",
            "Show me AAPL's revenue and net income trend over the last 4 years",
            "Which S&P 500 stocks have the lowest trailing PE ratio?",
            "Compare operating margins for AAPL, MSFT, GOOGL and META",
            "Show me the top 10 stocks by free cash flow yield",
            "How has JPM's return on equity changed over time?",
            "Compare debt-to-equity ratios across bank stocks",
            "Which stocks have the highest revenue growth?",
        ]
        for example in examples:
            if st.button(example, use_container_width=True):
                st.session_state.pending_prompt = example

    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "pending_prompt" not in st.session_state:
        st.session_state.pending_prompt = None

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            if msg["role"] == "assistant":
                if "chart" in msg:
                    st.plotly_chart(msg["chart"], use_container_width=True)
                if "sql" in msg:
                    with st.expander("Generated SQL"):
                        st.code(msg["sql"], language="sql")
                if "text" in msg:
                    st.write(msg["text"])
            else:
                st.write(msg["content"])

    prompt = st.chat_input("Ask a question or request a chart...")

    if st.session_state.pending_prompt:
        prompt = st.session_state.pending_prompt
        st.session_state.pending_prompt = None

    if prompt:
        with st.chat_message("user"):
            st.write(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})
        logger.info("User prompt: %.200s", prompt)

        with st.chat_message("assistant"):
            sql = None
            try:
                with st.spinner("Generating SQL..."):
                    claude_messages = [
                        {"role": m["role"], "content": m["content"]}
                        for m in st.session_state.messages
                        if m["role"] == "user"
                    ]
                    sql = generate_sql(claude_messages)

                with st.spinner("Querying Snowflake..."):
                    df = execute_sql_cached(sql)

                if df.empty:
                    st.write("The query returned no results. Try rephrasing your request.")
                    logger.info("Query returned empty result set")
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": "No results returned.",
                        "text": "The query returned no results. Try rephrasing your request.",
                        "sql": sql
                    })
                else:
                    with st.spinner("Building chart..."):
                        fig = generate_chart(df, prompt)

                    st.plotly_chart(fig, use_container_width=True)

                    with st.expander("Generated SQL"):
                        st.code(sql, language="sql")

                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": prompt,
                        "chart": fig,
                        "sql": sql
                    })

            except Exception as e:
                logger.error("Error handling prompt '%s': %s", prompt[:100], e)
                st.error(f"Something went wrong: {str(e)}")
                with st.expander("Generated SQL"):
                    st.code(sql if sql else "SQL not generated", language="sql")

with tab2:
    st.header("Event Study")
    st.caption("Find all historical occurrences of an event and measure forward returns")

    col1, col2, col3 = st.columns(3)

    with col1:
        ticker = st.text_input("Ticker", value="SPY").upper().strip()

    with col2:
        condition = st.selectbox(
            "Event condition",
            [
                "Daily return ≥ X%",
                "Daily return ≤ -X%",
                "Price within X% of 52-week high",
                "Price within X% of 52-week low",
                "Volume spike ≥ X× average"
            ]
        )

    with col3:
        threshold = st.number_input(
            "Threshold (X)",
            min_value=0.1,
            max_value=50.0,
            value=3.0,
            step=0.5
        )

    run_study = st.button("Run Event Study", type="primary")

    if run_study:
        # Validate ticker — alphanumeric + hyphen only (e.g. BRK-B)
        import re
        if not re.match(r'^[A-Z0-9\-]{1,10}$', ticker):
            st.error("Invalid ticker. Use letters, numbers, and hyphens only (e.g. AAPL, BRK-B).")
            st.stop()

        # threshold comes from st.number_input — already a float, no injection risk
        # condition comes from st.selectbox — fixed set of strings
        threshold_decimal = threshold / 100

        if condition == "Daily return ≥ X%":
            where_clause = f"daily_return >= {threshold_decimal}"
        elif condition == "Daily return ≤ -X%":
            where_clause = f"daily_return <= -{threshold_decimal}"
        elif condition == "Price within X% of 52-week high":
            where_clause = f"pct_of_52w_high >= {1 - threshold_decimal}"
        elif condition == "Price within X% of 52-week low":
            where_clause = f"close_price <= week_52_low * {1 + threshold_decimal}"
        else:
            where_clause = f"volume >= {threshold}"

        # ticker is validated above; safe to interpolate
        event_sql = f"""
        WITH events AS (
            SELECT
                ticker,
                price_date AS event_date,
                daily_return AS event_return,
                close_price AS event_close
            FROM EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES
            WHERE ticker = '{ticker}'
              AND {where_clause}
        ),
        forward_returns AS (
            SELECT
                e.event_date,
                e.event_return,
                f.price_date AS forward_date,
                DATEDIFF(day, e.event_date, f.price_date) AS days_forward,
                (f.close_price - e.event_close) / e.event_close AS cum_return
            FROM events e
            JOIN EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES f
                ON f.ticker = e.ticker
                AND f.price_date > e.event_date
                AND f.price_date <= DATEADD(day, 63, e.event_date)
        ),
        pivoted AS (
            SELECT
                event_date,
                ROUND(event_return * 100, 2) AS event_return_pct,
                MAX(CASE WHEN days_forward = 1  THEN cum_return END) AS d1,
                MAX(CASE WHEN days_forward = 2  THEN cum_return END) AS d2,
                MAX(CASE WHEN days_forward = 3  THEN cum_return END) AS d3,
                MAX(CASE WHEN days_forward = 5  THEN cum_return END) AS d5,
                MAX(CASE WHEN days_forward = 10 THEN cum_return END) AS d10,
                MAX(CASE WHEN days_forward = 21 THEN cum_return END) AS d21,
                MAX(CASE WHEN days_forward = 42 THEN cum_return END) AS d42,
                MAX(CASE WHEN days_forward = 63 THEN cum_return END) AS d63
            FROM forward_returns
            GROUP BY event_date, event_return
        )
        SELECT * FROM pivoted
        ORDER BY event_date DESC
        """

        logger.info("Event study: ticker=%s condition=%s threshold=%s", ticker, condition, threshold)

        with st.spinner(f"Finding all {condition} events for {ticker}..."):
            try:
                df = execute_sql_cached(event_sql)

                if df.empty:
                    st.warning(f"No events found for {ticker} with condition: {condition} {threshold}%")
                else:
                    fwd_cols = ['d1', 'd2', 'd3', 'd5', 'd10', 'd21', 'd42', 'd63']
                    labels = ['1D', '2D', '3D', '5D', '10D', '21D', '42D', '63D']

                    st.subheader(f"Summary — {len(df)} events found")

                    summary_data = {
                        'Horizon': labels,
                        'Avg Return %': [round(df[c].mean() * 100, 2) for c in fwd_cols],
                        'Median Return %': [round(df[c].median() * 100, 2) for c in fwd_cols],
                        '% Positive': [round((df[c] > 0).sum() * 100 / df[c].notna().sum(), 1) for c in fwd_cols],
                        'Best %': [round(df[c].max() * 100, 2) for c in fwd_cols],
                        'Worst %': [round(df[c].min() * 100, 2) for c in fwd_cols],
                    }
                    summary_df = pd.DataFrame(summary_data)
                    st.dataframe(summary_df, hide_index=True, use_container_width=True)

                    st.subheader("Return distribution over time")
                    import plotly.graph_objects as go

                    horizon_days = [1, 2, 3, 5, 10, 21, 42, 63]
                    medians = [df[c].median() * 100 for c in fwd_cols]
                    p25 = [df[c].quantile(0.25) * 100 for c in fwd_cols]
                    p75 = [df[c].quantile(0.75) * 100 for c in fwd_cols]
                    p10 = [df[c].quantile(0.10) * 100 for c in fwd_cols]
                    p90 = [df[c].quantile(0.90) * 100 for c in fwd_cols]

                    fig = go.Figure()

                    fig.add_trace(go.Scatter(
                        x=labels + labels[::-1],
                        y=p90 + p10[::-1],
                        fill='toself',
                        fillcolor='rgba(99,110,250,0.1)',
                        line=dict(color='rgba(255,255,255,0)'),
                        name='10th-90th percentile'
                    ))

                    fig.add_trace(go.Scatter(
                        x=labels + labels[::-1],
                        y=p75 + p25[::-1],
                        fill='toself',
                        fillcolor='rgba(99,110,250,0.2)',
                        line=dict(color='rgba(255,255,255,0)'),
                        name='25th-75th percentile'
                    ))

                    fig.add_trace(go.Scatter(
                        x=labels,
                        y=medians,
                        line=dict(color='rgb(99,110,250)', width=2),
                        name='Median',
                        mode='lines+markers'
                    ))

                    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

                    fig.update_layout(
                        title=f"{ticker} — Forward returns after {condition} {threshold}% ({len(df)} events)",
                        xaxis_title="Trading horizon",
                        yaxis_title="Cumulative return %",
                        template="plotly_white",
                        hovermode="x unified"
                    )

                    st.plotly_chart(fig, use_container_width=True)

                    with st.expander(f"All {len(df)} event instances"):
                        display_df = df.copy()
                        for c in fwd_cols:
                            display_df[c] = (display_df[c] * 100).round(2)
                        display_df.columns = ['Event Date', 'Event Return %'] + labels
                        st.dataframe(display_df, hide_index=True, use_container_width=True)

                    with st.expander("SQL"):
                        st.code(event_sql, language="sql")

            except Exception as e:
                logger.error("Event study error: ticker=%s error=%s", ticker, e)
                st.error(f"Error running event study: {str(e)}")