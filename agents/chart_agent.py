import os
import json
import logging
import time
import re
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
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

# ── System prompt ─────────────────────────────────────────────────────────────

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

FACT_FUNDAMENTALS - grain: one row per ticker per reporting period per frequency
- TICKER: varchar
- PERIOD_END_DATE: date
- FREQUENCY: varchar — 'annual' or 'quarterly'
- TOTAL_REVENUE, GROSS_PROFIT, OPERATING_INCOME, NET_INCOME, EBITDA: float
- DILUTED_EPS, BASIC_EPS, DILUTED_SHARES: float
- TOTAL_ASSETS, TOTAL_LIABILITIES, STOCKHOLDERS_EQUITY: float
- CASH_AND_EQUIVALENTS, TOTAL_DEBT, NET_DEBT: float
- OPERATING_CASH_FLOW, FREE_CASH_FLOW, CAPITAL_EXPENDITURE: float
- GROSS_MARGIN, OPERATING_MARGIN, NET_MARGIN: float (decimals, 0.45 = 45%)

FACT_VALUATION_SNAPSHOT - grain: one row per ticker per snapshot date
- TICKER: varchar
- SNAPSHOT_DATE: date
- TRAILING_PE, FORWARD_PE, PRICE_TO_BOOK, PRICE_TO_SALES: float
- EV_TO_EBITDA, EV_TO_REVENUE, PEG_RATIO: float
- GROSS_MARGIN, OPERATING_MARGIN, PROFIT_MARGIN, EBITDA_MARGIN: float
- RETURN_ON_EQUITY, RETURN_ON_ASSETS: float
- DEBT_TO_EQUITY, CURRENT_RATIO, QUICK_RATIO: float
- EARNINGS_GROWTH, REVENUE_GROWTH: float (decimals)
- DIVIDEND_YIELD, PAYOUT_RATIO: float
- MARKET_CAP, ENTERPRISE_VALUE, TOTAL_DEBT, TOTAL_CASH: bigint
- BETA: float

Available tickers: Full S&P 500 + major ETFs (SPY, QQQ, IWM, TLT, GLD, etc.)

Rules:
- Return ONLY valid Snowflake SQL, no markdown, no backticks, no explanation
- Always use fully qualified table names: EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES
- For cumulative return charts: EXP(SUM(LN(1 + DAILY_RETURN)) OVER (PARTITION BY TICKER ORDER BY PRICE_DATE)) - 1
- Date range in the warehouse is approximately 2010 to present
- Always include TICKER in SELECT when querying multiple tickers
- Order results by PRICE_DATE ASC for time series charts
"""

# ── Snowflake ─────────────────────────────────────────────────────────────────

@st.cache_resource
def get_snowflake_connection():
    logger.info("Opening Snowflake connection")
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse="TRANSFORM_WH",
        database="EQUITY_ANALYTICS",
        schema="MARTS",
        authenticator="programmatic_access_token",
        token=os.environ["SNOWFLAKE_TOKEN"],
        network_timeout=30,
        login_timeout=15,
    )


def execute_sql(sql: str) -> pd.DataFrame:
    conn = get_snowflake_connection()
    start = time.monotonic()
    try:
        cursor = conn.cursor()
        cursor.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 30")
        cursor.execute(sql)
        df = cursor.fetch_pandas_all()
        df.columns = [c.lower() for c in df.columns]
        logger.info("Query %.2fs → %d rows", time.monotonic() - start, len(df))
        return df
    except snowflake.connector.errors.ProgrammingError as e:
        logger.error("Snowflake error: %s", e)
        raise
    except Exception as e:
        logger.error("Unexpected query error: %s", e)
        get_snowflake_connection.clear()
        raise


@st.cache_data(ttl=300, show_spinner=False)
def execute_sql_cached(sql: str) -> pd.DataFrame:
    return execute_sql(sql)


# ── Data explorer loaders ─────────────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def load_securities() -> pd.DataFrame:
    return execute_sql("""
        SELECT ticker, company_name, sector, industry,
               market_cap_usd, first_trading_date, last_trading_date
        FROM EQUITY_ANALYTICS.MARTS.DIM_SECURITY
        ORDER BY market_cap_usd DESC NULLS LAST
    """)


@st.cache_data(ttl=3600, show_spinner=False)
def load_macro_series() -> pd.DataFrame:
    return execute_sql("""
        SELECT series_id, series_name,
               MIN(observation_date) AS first_observation,
               MAX(observation_date) AS last_observation,
               COUNT(*)              AS observation_count
        FROM EQUITY_ANALYTICS.MARTS.FACT_MACRO_READINGS
        GROUP BY series_id, series_name
        ORDER BY series_id
    """)


@st.cache_data(ttl=3600, show_spinner=False)
def load_summary_stats() -> dict:
    df = execute_sql("""
        SELECT
            (SELECT COUNT(DISTINCT ticker)   FROM EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES)      AS equity_count,
            (SELECT COUNT(DISTINCT series_id) FROM EQUITY_ANALYTICS.MARTS.FACT_MACRO_READINGS)   AS macro_series_count,
            (SELECT COUNT(*)                 FROM EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES)      AS price_rows,
            (SELECT COUNT(*)                 FROM EQUITY_ANALYTICS.MARTS.FACT_FUNDAMENTALS)      AS fundamental_rows,
            (SELECT MIN(price_date)          FROM EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES)      AS price_start,
            (SELECT MAX(price_date)          FROM EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES)      AS price_end
    """)
    return df.iloc[0].to_dict() if not df.empty else {}


# ── LLM helpers ───────────────────────────────────────────────────────────────

def generate_sql(messages: list) -> str:
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        system=SYSTEM_PROMPT,
        messages=messages,
        timeout=30,
    )
    return response.content[0].text.strip()


def _parse_chart_config(raw: str) -> dict:
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(l for l in lines[1:] if l.strip() != "```").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.warning("Chart config parse failed, using fallback")
        return {"chart_type": "line", "x": None, "y": None, "color": None, "title": "Chart"}


def generate_chart(df: pd.DataFrame, user_prompt: str):
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=300,
        system="""Return ONLY a JSON object with keys: chart_type (line/bar/scatter),
        x (column name), y (column name), color (column name or null), title (string).
        Use only column names from the provided list. No markdown, no explanation.""",
        messages=[{"role": "user", "content":
            f"Request: {user_prompt}\nColumns: {list(df.columns)}\nSample: {df.head(2).to_dict()}"}],
        timeout=15,
    )
    config = _parse_chart_config(response.content[0].text)
    cols = list(df.columns)

    if not config.get("x") or config["x"] not in df.columns:
        config["x"] = cols[0]
    if not config.get("y") or config["y"] not in df.columns:
        numeric = df.select_dtypes("number").columns.tolist()
        config["y"] = next((c for c in numeric if c != config["x"]), cols[-1])
    if config.get("color") and config["color"] not in df.columns:
        config["color"] = None

    build = {"line": px.line, "bar": px.bar, "scatter": px.scatter}.get(config["chart_type"], px.line)
    fig = build(df, x=config["x"], y=config["y"], color=config.get("color"),
                title=config.get("title", ""), template="plotly_white")
    fig.update_layout(
        xaxis_title=config["x"].replace("_", " ").title(),
        yaxis_title=config["y"].replace("_", " ").title(),
        legend_title="Ticker" if config.get("color") == "ticker" else "",
        font=dict(family="DM Mono, monospace", size=11),
    )
    return fig


# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Equity Analytics",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=DM+Sans:ital,wght@0,300;0,400;0,500;0,600;1,300&display=swap');

/*
  Palette — warm parchment, single ink color
  --bg:       #faf7f2   page background
  --bg-card:  #fdf9f4   card / input surfaces
  --bg-inset: #f3ede4   stack items, sidebar
  --border:   #e8ddd0   all borders
  --ink:      #1a1a1a   ALL text — single black, no warm mid-tones
  --ink-muted #666666   secondary labels only (tab inactive, metric label)
  --accent:   #1d4ed8   blue eyebrows and card tabs
*/

/* ── Global reset ── */
html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    background-color: #faf7f2;
    color: #1a1a1a;
}
.stApp { background-color: #faf7f2; color: #1a1a1a; }

/* Force Streamlit's own text elements to use black */
p, span, div, label, h1, h2, h3, h4, h5, h6,
.stMarkdown, .stText, .stCaption { color: #1a1a1a !important; }

#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 1.5rem; padding-bottom: 2rem; max-width: 1200px; }

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    gap: 0; border-bottom: 1px solid #e8ddd0; background: transparent; margin-bottom: 1.5rem;
}
.stTabs [data-baseweb="tab"] {
    font-family: 'DM Mono', monospace; font-size: 0.72rem; font-weight: 500;
    letter-spacing: 0.1em; text-transform: uppercase; padding: 0.75rem 1.75rem;
    color: #666666 !important; border-bottom: 2px solid transparent; background: transparent;
}
.stTabs [aria-selected="true"] {
    color: #1a1a1a !important; border-bottom: 2px solid #1a1a1a !important; background: transparent !important;
}

/* ── Hero ── */
.hero { padding: 1.5rem 0 2rem 0; border-bottom: 1px solid #e8ddd0; margin-bottom: 2rem; }
.hero-eyebrow {
    font-family: 'DM Mono', monospace; font-size: 0.68rem; font-weight: 500;
    letter-spacing: 0.18em; text-transform: uppercase; color: #1d4ed8; margin-bottom: 0.75rem;
}
.hero-title {
    font-size: 2.75rem; font-weight: 600; color: #1a1a1a;
    letter-spacing: -0.025em; line-height: 1.08; margin-bottom: 1rem;
}
.hero-sub { font-size: 0.95rem; color: #1a1a1a; font-weight: 300; max-width: 580px; line-height: 1.7; }

/* ── Metric strip ── */
.metric-strip {
    display: grid; grid-template-columns: repeat(4, 1fr);
    gap: 1px; background: #e8ddd0; border: 1px solid #e8ddd0;
    border-radius: 8px; overflow: hidden; margin-bottom: 2.5rem;
}
.metric-cell { background: #fdf9f4; padding: 1.25rem 1.5rem; }
.metric-value {
    font-family: 'DM Mono', monospace; font-size: 1.9rem;
    font-weight: 400; color: #1a1a1a; line-height: 1; letter-spacing: -0.02em;
}
.metric-label {
    font-size: 0.67rem; font-weight: 500; letter-spacing: 0.1em;
    text-transform: uppercase; color: #666666; margin-top: 0.3rem;
}

/* ── Feature cards ── */
.card-grid {
    display: grid; grid-template-columns: repeat(3, 1fr);
    gap: 1px; background: #e8ddd0; border: 1px solid #e8ddd0;
    border-radius: 8px; overflow: hidden; margin-bottom: 2rem;
}
.card { background: #fdf9f4; padding: 1.5rem; }
.card-tab {
    font-family: 'DM Mono', monospace; font-size: 0.62rem; font-weight: 500;
    letter-spacing: 0.14em; text-transform: uppercase; color: #1d4ed8; margin-bottom: 0.5rem;
}
.card-title { font-size: 0.95rem; font-weight: 600; color: #1a1a1a; margin-bottom: 0.5rem; }
.card-body { font-size: 0.82rem; color: #1a1a1a; line-height: 1.6; }
.card-pills { margin-top: 0.85rem; display: flex; flex-direction: column; gap: 0.3rem; }
.card-pill {
    font-family: 'DM Mono', monospace; font-size: 0.68rem; color: #1a1a1a;
    background: #f3ede4; border: 1px solid #e8ddd0; border-radius: 3px; padding: 0.25rem 0.5rem;
}

/* ── Stack grid ── */
.stack-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 0.6rem; margin-top: 0.75rem; margin-bottom: 2rem; }
.stack-item { background: #f3ede4; border: 1px solid #e8ddd0; border-radius: 6px; padding: 0.75rem 0.9rem; }
.stack-layer {
    font-family: 'DM Mono', monospace; font-size: 0.6rem;
    letter-spacing: 0.1em; text-transform: uppercase; color: #666666; margin-bottom: 0.2rem;
}
.stack-tech { font-size: 0.82rem; font-weight: 500; color: #1a1a1a; }

/* ── Section header ── */
.section-header {
    font-family: 'DM Mono', monospace; font-size: 0.67rem; font-weight: 500;
    letter-spacing: 0.14em; text-transform: uppercase; color: #666666;
    padding-bottom: 0.6rem; border-bottom: 1px solid #e8ddd0;
    margin-bottom: 1.25rem; margin-top: 1.75rem;
}

/* ── Dataframe ── */
.stDataFrame { font-family: 'DM Mono', monospace !important; }
[data-testid="stDataFrame"] > div { background-color: #fdf9f4 !important; }

/* ── All input fields — white bg, black text ── */
.stTextInput input,
.stTextInput input:focus,
.stTextInput input::placeholder {
    background-color: #ffffff !important;
    border-color: #e8ddd0 !important;
    color: #1a1a1a !important;
    font-family: 'DM Mono', monospace;
}
.stTextInput input::placeholder { color: #999999 !important; }

/* Selectbox */
.stSelectbox > div > div,
.stSelectbox > div > div > div {
    background-color: #ffffff !important;
    border-color: #e8ddd0 !important;
    color: #1a1a1a !important;
}
/* Selectbox selected value text */
.stSelectbox [data-baseweb="select"] span,
.stSelectbox [data-baseweb="select"] div { color: #1a1a1a !important; }

/* Number input */
.stNumberInput input {
    background-color: #ffffff !important;
    border-color: #e8ddd0 !important;
    color: #1a1a1a !important;
    font-family: 'DM Mono', monospace;
}
.stNumberInput > div { background-color: #ffffff !important; border-color: #e8ddd0 !important; }

/* Chat input — remove dark shadow/container, clean border only */
.stChatInput {
    background-color: transparent !important;
    box-shadow: none !important;
    border: none !important;
}
.stChatInput > div {
    background-color: #ffffff !important;
    border: 1px solid #d4c9bc !important;
    border-radius: 8px !important;
    box-shadow: none !important;
}
.stChatInput textarea {
    background-color: #ffffff !important;
    color: #1a1a1a !important;
    box-shadow: none !important;
}
.stChatInput textarea::placeholder { color: #999999 !important; }

/* Chat message avatars */
[data-testid="stChatMessageAvatarUser"] {
    background-color: #a8d5b5 !important;
}
[data-testid="stChatMessageAvatarAssistant"] {
    background-color: #a8c4e0 !important;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background-color: #f3ede4;
    font-family: 'DM Mono', monospace;
    font-size: 0.8rem;
    color: #1a1a1a;
}
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span,
section[data-testid="stSidebar"] div { color: #1a1a1a !important; }
section[data-testid="stSidebar"] .stButton button {
    background-color: #fdf9f4;
    border: 1px solid #e8ddd0;
    color: #1a1a1a !important;
    text-align: left;
}
section[data-testid="stSidebar"] .stButton button:hover {
    background-color: #efe8de;
    border-color: #d4c4b0;
}

/* ── Expanders — keep warm background in all states ── */
[data-testid="stExpander"] {
    background-color: #fdf9f4 !important;
    border: 1px solid #e8ddd0 !important;
    border-radius: 6px !important;
}
[data-testid="stExpander"] summary,
[data-testid="stExpander"] summary:hover,
[data-testid="stExpander"] summary:focus,
[data-testid="stExpander"] summary:active,
[data-testid="stExpander"][open] summary {
    background-color: #fdf9f4 !important;
    color: #1a1a1a !important;
}
[data-testid="stExpander"] summary:hover {
    background-color: #f0ece6 !important;
}
[data-testid="stExpander"] > div {
    background-color: #fdf9f4 !important;
}
/* Expander header text and arrow */
[data-testid="stExpander"] summary p,
[data-testid="stExpander"] summary span,
[data-testid="stExpander"] summary svg {
    color: #1a1a1a !important;
    fill: #1a1a1a !important;
}

/* ── Code blocks (SQL expander) — white background, dark text ── */
/* Overrides dark syntax highlighting theme that clashes with warm bg */
.stCode, [data-testid="stCode"] {
    background-color: #ffffff !important;
}
.stCode pre, [data-testid="stCode"] pre {
    background-color: #ffffff !important;
    color: #1a1a1a !important;
    border: 1px solid #e8ddd0 !important;
    border-radius: 6px !important;
}
/* Syntax tokens — keep readable on white */
.stCode code, [data-testid="stCode"] code,
.stCode span, [data-testid="stCode"] span {
    background-color: #ffffff !important;
}

/* ── Dataframe toolbar tooltips and hover overlays ── */
/* The download button tooltip and column menu appear dark — force white */
[data-testid="stDataFrameResizeHandle"],
[data-testid="stDataFrame"] button,
[data-testid="stDataFrame"] button svg,
[data-testid="stDataFrame"] [role="tooltip"],
[data-testid="stDataFrame"] [data-testid="stTooltipHoverTarget"] {
    color: #1a1a1a !important;
    background-color: #ffffff !important;
}
/* Toolbar icon buttons (download, fullscreen, search) */
[data-testid="stDataFrame"] .dvn-scroller ~ div button,
[data-testid="stElementToolbar"] {
    background-color: #ffffff !important;
    border: 1px solid #e8ddd0 !important;
    border-radius: 4px !important;
}
[data-testid="stElementToolbar"] button {
    background-color: #ffffff !important;
    color: #1a1a1a !important;
}
[data-testid="stElementToolbar"] button:hover {
    background-color: #f0ece6 !important;
}
[data-testid="stElementToolbar"] button svg path {
    fill: #1a1a1a !important;
    stroke: #1a1a1a !important;
}
/* Column header hover menu and sort arrows */
[data-testid="stDataFrame"] th button {
    background-color: transparent !important;
    color: #1a1a1a !important;
}
</style>
""", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab_overview, tab_chat, tab_events = st.tabs([
    "01 · Overview",
    "02 · AI Analytics",
    "03 · Event Study",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════

with tab_overview:

    st.markdown("""
    <div class="hero">
        <div class="hero-eyebrow">Portfolio Project · Data Engineering</div>
        <div class="hero-title">Equity Analytics Pipeline</div>
        <div class="hero-sub">
            A production-style ELT pipeline covering the full S&amp;P 500 universe,
            95 Federal Reserve macro indicators, and complete fundamental financial data —
            modeled into a Kimball dimensional warehouse and exposed through a
            natural language analytics interface powered by Claude.
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Live stats
    with st.spinner(""):
        stats = load_summary_stats()

    if stats:
        equity_count = int(stats.get("equity_count", 0))
        macro_count  = int(stats.get("macro_series_count", 0))
        price_rows   = int(stats.get("price_rows", 0))
        price_start  = str(stats.get("price_start", ""))[:4]
        price_end    = str(stats.get("price_end", ""))[:4]

        st.markdown(f"""
        <div class="metric-strip">
            <div class="metric-cell">
                <div class="metric-value">{equity_count:,}</div>
                <div class="metric-label">Securities</div>
            </div>
            <div class="metric-cell">
                <div class="metric-value">{macro_count}</div>
                <div class="metric-label">FRED series</div>
            </div>
            <div class="metric-cell">
                <div class="metric-value">{price_rows/1_000_000:.1f}M</div>
                <div class="metric-label">Price observations</div>
            </div>
            <div class="metric-cell">
                <div class="metric-value">{price_start}–{price_end}</div>
                <div class="metric-label">History</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # Feature cards
    st.markdown("""
    <div class="card-grid">
        <div class="card">
            <div class="card-tab">Tab 02</div>
            <div class="card-title">AI Analytics Chat</div>
            <div class="card-body">
                Ask questions in plain English. Claude translates your prompt into
                Snowflake SQL, executes it against the mart layer, and renders
                an interactive chart — no SQL required.
            </div>
            <div class="card-pills">
                <div class="card-pill">"Compare cumulative returns for SPY, QQQ, IWM"</div>
                <div class="card-pill">"Show AAPL revenue over 4 years"</div>
                <div class="card-pill">"Which stocks have the highest FCF yield?"</div>
            </div>
        </div>
        <div class="card">
            <div class="card-tab">Tab 03</div>
            <div class="card-title">Event Study</div>
            <div class="card-body">
                Define a market condition and measure forward returns across all
                historical occurrences. Returns a fan chart with median, IQR,
                and 10th–90th percentile bands out to 63 trading days.
            </div>
            <div class="card-pills">
                <div class="card-pill">"SPY daily return ≥ 3%"</div>
                <div class="card-pill">"AAPL within 2% of 52-week high"</div>
                <div class="card-pill">"QQQ daily return ≤ -5%"</div>
            </div>
        </div>
        <div class="card">
            <div class="card-tab">Below</div>
            <div class="card-title">Data Explorer</div>
            <div class="card-body">
                Search the full universe of securities and macro indicators.
                Filter by sector, market cap, or FRED category to discover
                what's available before writing a prompt.
            </div>
            <div class="card-pills">
                <div class="card-pill">S&amp;P 500 + top 100 ETFs by AUM</div>
                <div class="card-pill">95 FRED series across 11 categories</div>
                <div class="card-pill">Income stmt · Balance sheet · Cash flow</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Stack
    st.markdown('<div class="section-header">Stack</div>', unsafe_allow_html=True)
    st.markdown("""
    <div class="stack-grid">
        <div class="stack-item"><div class="stack-layer">Ingestion</div><div class="stack-tech">Python · yfinance · FRED API</div></div>
        <div class="stack-item"><div class="stack-layer">Orchestration</div><div class="stack-tech">Prefect Cloud</div></div>
        <div class="stack-item"><div class="stack-layer">Warehouse</div><div class="stack-tech">Snowflake</div></div>
        <div class="stack-item"><div class="stack-layer">Transformation</div><div class="stack-tech">dbt Cloud · Kimball model</div></div>
        <div class="stack-item"><div class="stack-layer">Quality</div><div class="stack-tech">54 dbt tests · CI gate</div></div>
        <div class="stack-item"><div class="stack-layer">CI/CD</div><div class="stack-tech">GitHub Actions · RSA auth</div></div>
        <div class="stack-item"><div class="stack-layer">AI</div><div class="stack-tech">Claude API · Anthropic</div></div>
        <div class="stack-item"><div class="stack-layer">Application</div><div class="stack-tech">Streamlit · Plotly</div></div>
    </div>
    """, unsafe_allow_html=True)

    # ── Data Explorer ──────────────────────────────────────────────────────────

    st.markdown('<div class="section-header">Data Explorer</div>', unsafe_allow_html=True)

    FRED_CATEGORIES = {
        "Interest Rates":        ["DFF","DGS1MO","DGS3MO","DGS6MO","DGS1","DGS2","DGS5","DGS7","DGS10","DGS30"],
        "Yield Curve & Real":    ["T10Y2Y","T10Y3M","T5YIFR","DFII5","DFII10"],
        "Inflation":             ["CPIAUCSL","CPILFESL","PCEPI","PCEPILFE","PPIACO","MICH","UMCSENT"],
        "Labor Market":          ["UNRATE","U6RATE","PAYEMS","CIVPART","JTSJOL","JTSHIL","ICSA","CCSA","AWHMAN","CES0500000003"],
        "GDP & Growth":          ["GDP","GDPC1","GDPCA","INDPRO","TCU","IPB50001N","DGORDER","NEWORDER","ISRATIO","MNFCTRIRSA"],
        "Consumer":              ["RETAILSMNSA","RSXFS","PCE","DSPIC96","PSAVERT","TOTALSL"],
        "Credit & Financial":    ["BAMLH0A0HYM2","BAMLC0A0CM","DAAA","DBAA","TEDRATE","DRCCLACBS","BUSLOANS","DPSACBW027SBOG"],
        "Housing":               ["MORTGAGE30US","MORTGAGE15US","HOUST","HOUST1F","PERMIT","HSN1F","EXHOSLUSM495S","MSACSR","CSUSHPISA","MSPUS","EVACANTUSQ176N","RRVRUSQ156N"],
        "Money Supply":          ["M1SL","M2SL","BOGMBASE","AMBSL","WRMFSL"],
        "Trade & FX":            ["BOPTEXP","BOPTIMP","XTEXVA01USM667S","DEXUSEU","DEXJPUS","DEXUSUK","DEXCHUS","DEXCAUS","DEXBZUS","DEXKOUS","DEXINUS","DEXMXUS"],
        "Energy & Commodities":  ["DCOILWTICO","DCOILBRENTEU","GASREGCOVW","DHHNGSP","APU000072610"],
        "Market Indicators":     ["VIXCLS","SP500","NASDAQCOM","DJIA","WILL5000PR","NIKKEI225"],
    }
    series_to_cat = {s: cat for cat, series in FRED_CATEGORIES.items() for s in series}

    ex_eq, ex_macro = st.tabs(["Equities & ETFs", "Macro Indicators"])

    # ── Equities explorer ──────────────────────────────────────────────────────
    with ex_eq:
        with st.spinner(""):
            sec_df = load_securities()

        if not sec_df.empty:
            c1, c2, c3 = st.columns([3, 2, 2])
            with c1:
                search = st.text_input("", placeholder="Search ticker, company, sector, industry…",
                                       key="eq_search", label_visibility="collapsed")
            with c2:
                sectors = ["All sectors"] + sorted(sec_df["sector"].dropna().unique().tolist())
                sel_sector = st.selectbox("", sectors, key="eq_sector", label_visibility="collapsed")
            with c3:
                cap_opts = ["All sizes", "Mega (>$200B)", "Large ($10–200B)", "Mid ($2–10B)", "Small (<$2B)", "ETFs / N/A"]
                sel_cap = st.selectbox("", cap_opts, key="eq_cap", label_visibility="collapsed")

            filt = sec_df.copy()
            if search:
                m = (filt["ticker"].str.contains(search.upper(), na=False) |
                     filt["company_name"].str.contains(search, case=False, na=False) |
                     filt["sector"].str.contains(search, case=False, na=False) |
                     filt["industry"].str.contains(search, case=False, na=False))
                filt = filt[m]
            if sel_sector != "All sectors":
                filt = filt[filt["sector"] == sel_sector]
            if sel_cap == "Mega (>$200B)":
                filt = filt[filt["market_cap_usd"] >= 200e9]
            elif sel_cap == "Large ($10–200B)":
                filt = filt[(filt["market_cap_usd"] >= 10e9) & (filt["market_cap_usd"] < 200e9)]
            elif sel_cap == "Mid ($2–10B)":
                filt = filt[(filt["market_cap_usd"] >= 2e9) & (filt["market_cap_usd"] < 10e9)]
            elif sel_cap == "Small (<$2B)":
                filt = filt[filt["market_cap_usd"] < 2e9]
            elif sel_cap == "ETFs / N/A":
                filt = filt[filt["market_cap_usd"].isna()]

            disp = filt.copy()
            disp["market_cap_usd"] = disp["market_cap_usd"].apply(
                lambda x: f"${x/1e9:.1f}B" if pd.notna(x) else "—")
            disp["first_trading_date"] = pd.to_datetime(disp["first_trading_date"]).dt.strftime("%Y-%m-%d")
            disp["last_trading_date"]  = pd.to_datetime(disp["last_trading_date"]).dt.strftime("%Y-%m-%d")
            disp.columns = ["Ticker", "Company", "Sector", "Industry", "Mkt Cap", "First", "Last"]

            st.caption(f"{len(filt):,} of {len(sec_df):,} securities")
            st.dataframe(disp, use_container_width=True, hide_index=True, height=400,
                column_config={
                    "Ticker":  st.column_config.TextColumn(width="small"),
                    "Mkt Cap": st.column_config.TextColumn(width="small"),
                    "First":   st.column_config.TextColumn(width="small"),
                    "Last":    st.column_config.TextColumn(width="small"),
                })

            sector_counts = (sec_df[sec_df["sector"].notna()]
                             .groupby("sector").size().reset_index(name="n")
                             .sort_values("n", ascending=True))
            fig_s = px.bar(sector_counts, x="n", y="sector", orientation="h",
                           title="Securities by Sector", template="plotly_white",
                           color_discrete_sequence=["#2563eb"])
            fig_s.update_layout(height=340, margin=dict(l=0,r=0,t=40,b=0),
                                xaxis_title="", yaxis_title="", showlegend=False,
                                font=dict(family="DM Mono, monospace", size=11),
                                title_font=dict(size=12, color="#64748b"))
            st.plotly_chart(fig_s, use_container_width=True)

    # ── Macro explorer ─────────────────────────────────────────────────────────
    with ex_macro:
        with st.spinner(""):
            macro_df = load_macro_series()

        if not macro_df.empty:
            macro_df["category"] = macro_df["series_id"].map(series_to_cat).fillna("Other")

            mc1, mc2 = st.columns([3, 2])
            with mc1:
                msearch = st.text_input("", placeholder="Search series ID or name…",
                                        key="macro_search", label_visibility="collapsed")
            with mc2:
                mcats = ["All categories"] + sorted(macro_df["category"].unique().tolist())
                sel_cat = st.selectbox("", mcats, key="macro_cat", label_visibility="collapsed")

            mfilt = macro_df.copy()
            if msearch:
                mm = (mfilt["series_id"].str.contains(msearch.upper(), na=False) |
                      mfilt["series_name"].str.contains(msearch, case=False, na=False) |
                      mfilt["category"].str.contains(msearch, case=False, na=False))
                mfilt = mfilt[mm]
            if sel_cat != "All categories":
                mfilt = mfilt[mfilt["category"] == sel_cat]

            mdisp = mfilt.copy()
            mdisp["first_observation"] = pd.to_datetime(mdisp["first_observation"]).dt.strftime("%Y-%m-%d")
            mdisp["last_observation"]  = pd.to_datetime(mdisp["last_observation"]).dt.strftime("%Y-%m-%d")
            mdisp["observation_count"] = mdisp["observation_count"].apply(lambda x: f"{x:,}")
            mdisp = mdisp[["series_id","series_name","category","first_observation","last_observation","observation_count"]]
            mdisp.columns = ["Series ID","Name","Category","First Obs.","Last Obs.","Observations"]

            st.caption(f"{len(mfilt):,} of {len(macro_df):,} series")
            st.dataframe(mdisp, use_container_width=True, hide_index=True, height=400,
                column_config={
                    "Series ID":    st.column_config.TextColumn(width="small"),
                    "Observations": st.column_config.TextColumn(width="small"),
                })

            cat_counts = (macro_df.groupby("category").size()
                          .reset_index(name="n").sort_values("n", ascending=True))
            fig_m = px.bar(cat_counts, x="n", y="category", orientation="h",
                           title="Series by Category", template="plotly_white",
                           color_discrete_sequence=["#2563eb"])
            fig_m.update_layout(height=340, margin=dict(l=0,r=0,t=40,b=0),
                                xaxis_title="", yaxis_title="", showlegend=False,
                                font=dict(family="DM Mono, monospace", size=11),
                                title_font=dict(size=12, color="#64748b"))
            st.plotly_chart(fig_m, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — AI ANALYTICS CHAT
# ══════════════════════════════════════════════════════════════════════════════

EXAMPLE_PROMPTS = [
    "Compare cumulative returns for SPY, QQQ and IWM over the last year",
    "How did SPY perform during periods when the yield curve was inverted?",
    "Show me AAPL's revenue and net income trend over the last 4 years",
    "Which S&P 500 stocks have the lowest trailing PE ratio?",
    "Compare operating margins for AAPL, MSFT, GOOGL and META",
]

with tab_chat:

    # Force suggestion buttons to look like light chips regardless of OS theme
    st.markdown("""
    <style>
    div[data-testid="stHorizontalBlock"] .stButton button {
        background-color: #f0ece6 !important;
        color: #1a1a1a !important;
        border: 1px solid #d4c9bc !important;
        border-radius: 6px !important;
        font-family: 'DM Mono', monospace !important;
        font-size: 0.75rem !important;
        font-weight: 400 !important;
        padding: 0.5rem 0.75rem !important;
        white-space: normal !important;
        text-align: left !important;
        line-height: 1.4 !important;
        transition: background-color 0.15s, border-color 0.15s !important;
        min-height: 3rem !important;
    }
    div[data-testid="stHorizontalBlock"] .stButton button:hover {
        background-color: #ddd6cc !important;
        border-color: #b8ada0 !important;
        color: #1a1a1a !important;
    }
    div[data-testid="stHorizontalBlock"] .stButton button:active {
        background-color: #ccc4ba !important;
    }
    </style>
    """, unsafe_allow_html=True)

    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "pending_prompt" not in st.session_state:
        st.session_state.pending_prompt = None

    # ── Chat history ───────────────────────────────────────────────────────────
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

    # ── Prompt suggestions (shown only when no conversation yet) ──────────────
    if not st.session_state.messages:
        st.markdown("**Ask a question or choose a prompt:**")
        st.write("")
        cols = st.columns(len(EXAMPLE_PROMPTS))
        for col, p in zip(cols, EXAMPLE_PROMPTS):
            with col:
                if st.button(p, key=f"suggestion_{p[:40]}", use_container_width=True):
                    st.session_state.pending_prompt = p
                    st.rerun()

    # ── Chat input ─────────────────────────────────────────────────────────────
    prompt = st.chat_input("Type here...")
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
                with st.spinner("Generating SQL…"):
                    claude_messages = [{"role": m["role"], "content": m["content"]}
                                       for m in st.session_state.messages if m["role"] == "user"]
                    sql = generate_sql(claude_messages)
                with st.spinner("Querying Snowflake…"):
                    df = execute_sql_cached(sql)

                if df.empty:
                    st.write("The query returned no results. Try rephrasing your request.")
                    st.session_state.messages.append({
                        "role": "assistant", "content": "No results.",
                        "text": "The query returned no results.", "sql": sql
                    })
                else:
                    with st.spinner("Building chart…"):
                        fig = generate_chart(df, prompt)
                    st.plotly_chart(fig, use_container_width=True)
                    with st.expander("Generated SQL"):
                        st.code(sql, language="sql")
                    st.session_state.messages.append({
                        "role": "assistant", "content": prompt, "chart": fig, "sql": sql
                    })
            except Exception as e:
                logger.error("Prompt error: %s", e)
                st.error(f"Something went wrong: {str(e)}")
                with st.expander("Generated SQL"):
                    st.code(sql or "SQL not generated", language="sql")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — EVENT STUDY
# ══════════════════════════════════════════════════════════════════════════════

with tab_events:

    st.markdown("### Event Study")
    st.caption("Define a market event condition and measure forward returns across all historical occurrences.")

    col1, col2, col3 = st.columns(3)
    with col1:
        ticker = st.text_input("Ticker", value="SPY").upper().strip()
    with col2:
        condition = st.selectbox("Event condition", [
            "Daily return ≥ X%", "Daily return ≤ -X%",
            "Price within X% of 52-week high", "Price within X% of 52-week low",
            "Volume spike ≥ X× average"
        ])
    with col3:
        threshold = st.number_input("Threshold (X)", min_value=0.1, max_value=50.0, value=3.0, step=0.5)

    if st.button("Run Event Study", type="primary"):
        if not re.match(r'^[A-Z0-9\-]{1,10}$', ticker):
            st.error("Invalid ticker. Use letters, numbers, and hyphens only (e.g. AAPL, BRK-B).")
            st.stop()

        td = threshold / 100
        if condition == "Daily return ≥ X%":            where_clause = f"daily_return >= {td}"
        elif condition == "Daily return ≤ -X%":         where_clause = f"daily_return <= -{td}"
        elif condition == "Price within X% of 52-week high": where_clause = f"pct_of_52w_high >= {1 - td}"
        elif condition == "Price within X% of 52-week low":  where_clause = f"close_price <= week_52_low * {1 + td}"
        else:                                            where_clause = f"volume >= {threshold}"

        event_sql = f"""
        WITH events AS (
            SELECT ticker, price_date AS event_date,
                   daily_return AS event_return, close_price AS event_close
            FROM EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES
            WHERE ticker = '{ticker}' AND {where_clause}
        ),
        forward_returns AS (
            SELECT e.event_date, e.event_return,
                   DATEDIFF(day, e.event_date, f.price_date) AS days_forward,
                   (f.close_price - e.event_close) / e.event_close AS cum_return
            FROM events e
            JOIN EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES f
                ON f.ticker = e.ticker
                AND f.price_date > e.event_date
                AND f.price_date <= DATEADD(day, 63, e.event_date)
        ),
        pivoted AS (
            SELECT event_date, ROUND(event_return * 100, 2) AS event_return_pct,
                   MAX(CASE WHEN days_forward = 1  THEN cum_return END) AS d1,
                   MAX(CASE WHEN days_forward = 2  THEN cum_return END) AS d2,
                   MAX(CASE WHEN days_forward = 3  THEN cum_return END) AS d3,
                   MAX(CASE WHEN days_forward = 5  THEN cum_return END) AS d5,
                   MAX(CASE WHEN days_forward = 10 THEN cum_return END) AS d10,
                   MAX(CASE WHEN days_forward = 21 THEN cum_return END) AS d21,
                   MAX(CASE WHEN days_forward = 42 THEN cum_return END) AS d42,
                   MAX(CASE WHEN days_forward = 63 THEN cum_return END) AS d63
            FROM forward_returns GROUP BY event_date, event_return
        )
        SELECT * FROM pivoted ORDER BY event_date DESC
        """

        logger.info("Event study: ticker=%s condition=%s threshold=%s", ticker, condition, threshold)

        with st.spinner(f"Scanning history for {ticker}…"):
            try:
                df = execute_sql_cached(event_sql)
                if df.empty:
                    st.warning(f"No events found for {ticker} · {condition} · {threshold}%")
                else:
                    fwd_cols = ['d1','d2','d3','d5','d10','d21','d42','d63']
                    labels   = ['1D','2D','3D','5D','10D','21D','42D','63D']

                    st.markdown(f"**{len(df)} events** · {ticker} · {condition} · threshold {threshold}%")

                    summary = {
                        'Horizon':      labels,
                        'Avg %':        [round(df[c].mean() * 100, 2) for c in fwd_cols],
                        'Median %':     [round(df[c].median() * 100, 2) for c in fwd_cols],
                        '% Positive':   [round((df[c] > 0).sum() * 100 / df[c].notna().sum(), 1) for c in fwd_cols],
                        'Best %':       [round(df[c].max() * 100, 2) for c in fwd_cols],
                        'Worst %':      [round(df[c].min() * 100, 2) for c in fwd_cols],
                    }
                    st.dataframe(pd.DataFrame(summary), hide_index=True, use_container_width=True)

                    medians = [df[c].median() * 100 for c in fwd_cols]
                    p25 = [df[c].quantile(0.25) * 100 for c in fwd_cols]
                    p75 = [df[c].quantile(0.75) * 100 for c in fwd_cols]
                    p10 = [df[c].quantile(0.10) * 100 for c in fwd_cols]
                    p90 = [df[c].quantile(0.90) * 100 for c in fwd_cols]

                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=labels+labels[::-1], y=p90+p10[::-1],
                        fill='toself', fillcolor='rgba(37,99,235,0.07)',
                        line=dict(color='rgba(0,0,0,0)'), name='10th–90th pct'))
                    fig.add_trace(go.Scatter(x=labels+labels[::-1], y=p75+p25[::-1],
                        fill='toself', fillcolor='rgba(37,99,235,0.14)',
                        line=dict(color='rgba(0,0,0,0)'), name='25th–75th pct'))
                    fig.add_trace(go.Scatter(x=labels, y=medians,
                        line=dict(color='#2563eb', width=2),
                        name='Median', mode='lines+markers',
                        marker=dict(size=5, color='#2563eb')))
                    fig.add_hline(y=0, line_dash="dash", line_color="#cbd5e1", opacity=0.8)
                    fig.update_layout(
                        title=f"{ticker} — forward returns after {condition} {threshold}% ({len(df)} events)",
                        xaxis_title="Trading horizon", yaxis_title="Cumulative return %",
                        template="plotly_white", hovermode="x unified",
                        font=dict(family="DM Mono, monospace", size=11),
                        title_font=dict(size=12, color="#64748b"),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    with st.expander(f"All {len(df)} event instances"):
                        ddisp = df.copy()
                        for c in fwd_cols:
                            ddisp[c] = (ddisp[c] * 100).round(2)
                        ddisp.columns = ['Event Date', 'Event Return %'] + labels
                        st.dataframe(ddisp, hide_index=True, use_container_width=True)
                    with st.expander("SQL"):
                        st.code(event_sql, language="sql")

            except Exception as e:
                logger.error("Event study error: %s", e)
                st.error(f"Error running event study: {str(e)}")