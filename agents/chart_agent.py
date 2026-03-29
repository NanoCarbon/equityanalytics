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
st.caption("Ask questions about your equity data or request charts")

# Sidebar with example prompts
with st.sidebar:
    st.header("Example prompts")
    examples = [
        "Compare cumulative returns for SPY, QQQ and IWM over the last year",
        "Show me the 30-day rolling volatility for AAPL, MSFT and GOOGL",
        "Which sector had the highest average daily return last month?",
        "Show me the top 5 tickers by average volume",
        "Compare JPM and GS closing prices over the last 6 months",
        "Show me tickers trading closest to their 52-week high"
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