import os
import json
import streamlit as st
import plotly.express as px
import pandas as pd
import snowflake.connector
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

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

Available tickers: AAPL, MSFT, GOOGL, AMZN, META, JPM, GS, MS, BLK, BX, 
BRK-B, V, MA, HD, UNH, SPY, QQQ, IWM, VTI, AGG

Rules:
- Return ONLY valid Snowflake SQL, no markdown, no backticks, no explanation
- Always use fully qualified table names: EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES
- For cumulative return charts, use: EXP(SUM(LN(1 + DAILY_RETURN)) OVER (PARTITION BY TICKER ORDER BY PRICE_DATE)) - 1
- For normalized price charts starting at same point, divide each price by the first price for that ticker
- Date range in the warehouse is approximately the last 365 days
- Always include TICKER in SELECT when querying multiple tickers
- Order results by PRICE_DATE ASC for time series charts
"""

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse="TRANSFORM_WH",
        database="EQUITY_ANALYTICS",
        schema="MARTS",
        authenticator="programmatic_access_token",
        token=os.environ["SNOWFLAKE_TOKEN"]
    )

def execute_sql(sql: str) -> pd.DataFrame:
    conn = get_snowflake_connection()
    try:
        df = pd.read_sql(sql, conn)
        df.columns = [c.lower() for c in df.columns]
        return df
    finally:
        conn.close()

def generate_sql(messages: list) -> str:
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        system=SYSTEM_PROMPT,
        messages=messages
    )
    return response.content[0].text.strip()

def generate_chart(df: pd.DataFrame, user_prompt: str):
    """Ask Claude how to visualize the data."""
    col_info = {col: str(df[col].dtype) for col in df.columns}
    
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
            "content": f"User request: {user_prompt}\nAvailable columns: {list(df.columns)}\nSample data: {df.head(2).to_dict()}"
        }]
    )
    
    raw = response.content[0].text.strip()
    
    # Strip markdown if Claude wrapped it anyway
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()
    
    config = json.loads(raw)
    
    if config["chart_type"] == "line":
        fig = px.line(
            df,
            x=config["x"],
            y=config["y"],
            color=config.get("color"),
            title=config.get("title", ""),
            template="plotly_white"
        )
    elif config["chart_type"] == "bar":
        fig = px.bar(
            df,
            x=config["x"],
            y=config["y"],
            color=config.get("color"),
            title=config.get("title", ""),
            template="plotly_white"
        )
    else:
        fig = px.scatter(
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
    # Sidebar with example prompts
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
            "Show me the Fed funds rate trend over the last year"
        ]
        for example in examples:
            if st.button(example, use_container_width=True):
                st.session_state.pending_prompt = example

    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "pending_prompt" not in st.session_state:
        st.session_state.pending_prompt = None

    # Display chat history
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

    # Handle input
    prompt = st.chat_input("Ask a question or request a chart...")

    # Use sidebar button prompt if set
    if st.session_state.pending_prompt:
        prompt = st.session_state.pending_prompt
        st.session_state.pending_prompt = None

    if prompt:
        # Display user message
        with st.chat_message("user"):
            st.write(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})

        with st.chat_message("assistant"):
            with st.spinner("Generating SQL..."):
                # Build message history for Claude (text only)
                claude_messages = [
                    {"role": m["role"], "content": m["content"]}
                    for m in st.session_state.messages
                    if m["role"] == "user"
                ]
                
                try:
                    # Generate SQL
                    sql = generate_sql(claude_messages)
                    
                    # Execute against Snowflake
                    with st.spinner("Querying Snowflake..."):
                        df = execute_sql(sql)
                    
                    if df.empty:
                        st.write("The query returned no results. Try rephrasing your request.")
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": "No results returned.",
                            "text": "The query returned no results. Try rephrasing your request.",
                            "sql": sql
                        })
                    else:
                        # Generate chart
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
                    error_msg = f"Something went wrong: {str(e)}"
                    st.error(error_msg)
                    with st.expander("Generated SQL"):
                        st.code(sql if 'sql' in locals() else "SQL not generated", language="sql")
with tab2:
    st.header("Event Study")
    st.caption("Find all historical occurrences of an event and measure forward returns")

    col1, col2, col3 = st.columns(3)

    with col1:
        ticker = st.text_input("Ticker", value="SPY").upper()

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
        # Build the WHERE clause based on condition
        if condition == "Daily return ≥ X%":
            where_clause = f"daily_return >= {threshold / 100}"
        elif condition == "Daily return ≤ -X%":
            where_clause = f"daily_return <= -{threshold / 100}"
        elif condition == "Price within X% of 52-week high":
            where_clause = f"pct_of_52w_high >= {1 - threshold / 100}"
        elif condition == "Price within X% of 52-week low":
            where_clause = f"close_price <= week_52_low * {1 + threshold / 100}"
        else:
            where_clause = f"volume >= {threshold}"

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

        with st.spinner(f"Finding all {condition} events for {ticker}..."):
            try:
                df = execute_sql(event_sql)

                if df.empty:
                    st.warning(f"No events found for {ticker} with condition: {condition} {threshold}%")
                else:
                    # Forward return columns
                    fwd_cols = ['d1', 'd2', 'd3', 'd5', 'd10', 'd21', 'd42', 'd63']
                    labels = ['1D', '2D', '3D', '5D', '10D', '21D', '42D', '63D']

                    # Summary stats
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

                    # Fan chart
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

                    # Instance table
                    with st.expander(f"All {len(df)} event instances"):
                        display_df = df.copy()
                        for c in fwd_cols:
                            display_df[c] = (display_df[c] * 100).round(2)
                        display_df.columns = ['Event Date', 'Event Return %'] + labels
                        st.dataframe(display_df, hide_index=True, use_container_width=True)

                    with st.expander("SQL"):
                        st.code(event_sql, language="sql")

            except Exception as e:
                st.error(f"Error running event study: {str(e)}")