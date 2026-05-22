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

FACT_MACRO_READINGS - grain: one row per series per observation date
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

Available tickers: The full S&P 500 universe plus ~100 ETFs including but not limited to
SPY, IVV, VOO, QQQ, VTI, IWM, BND, AGG, TLT, IEF, LQD, HYG, GLD, IAU, SLV,
VEA, VWO, EFA, XLF, XLK, XLV, XLE, SCHD, VIG, DVY, ARKK, TQQQ, and many more.
If the user mentions a ticker, use it exactly as given — do not substitute other tickers.
If a ticker genuinely does not exist in the warehouse, the query will return no rows.

Rules:
- Return ONLY a single valid Snowflake SQL statement, no markdown, no backticks, no explanation
- Never use semicolons — return exactly one SELECT statement
- Always use fully qualified table names: EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES
- For cumulative return charts: EXP(SUM(LN(1 + DAILY_RETURN)) OVER (PARTITION BY TICKER ORDER BY PRICE_DATE)) - 1
- Always use the exact tickers the user requests — never substitute or replace them
- Date range in the warehouse is 2010 to present — honour multi-year requests fully
  e.g. "10 year" → DATEADD(YEAR, -10, CURRENT_DATE()), "5 year" → DATEADD(YEAR, -5, CURRENT_DATE())
- Always include TICKER in SELECT when querying multiple tickers
- Order results by PRICE_DATE ASC for time series charts
"""

EXAMPLE_PROMPTS = [
    "Compare cumulative returns for SPY, QQQ and IWM over the last year",
    "Show me the 30-day rolling volatility for AAPL, MSFT and GOOGL",
    "Show me AAPL's revenue and net income trend over the last 4 years",
    "Which S&P 500 stocks have the lowest trailing PE ratio?",
    "Compare operating margins for AAPL, MSFT, GOOGL and META",
]

FRED_CATEGORIES = {
    # ── Original categories (expanded) ───────────────────────────────────────
    "Interest Rates":        ["DFF","FEDFUNDS","DFEDTARU","DFEDTARL","SOFR",
                              "DGS1MO","DGS3MO","DGS6MO","DGS1","DGS2","DGS5","DGS7","DGS10","DGS30"],
    "Yield Curve & Spreads": ["T10Y2Y","T10Y3M","T5YIFR","DFII2","DFII5","DFII10","DFII30",
                              "T10YFFM","T5YFFM","T1YFFM","TB3SMFFM",
                              "AAAFF","BAAFF","CPFF","TEDRATE",
                              "HQMCB5YR","HQMCB10YR","HQMCB30YR"],
    "Inflation":             ["CPIAUCSL","CPILFESL","PCEPI","PCEPILFE","PPIACO","MICH","UMCSENT",
                              "CPIHOSSL","CPIENGSL","CPIMEDSL","CPITRNSL","CPIRECSL",
                              "CUSR0000SAC","CUSR0000SAS","CUSR0000SAD","GDPDEF",
                              "STICKCPIM157SFRBATL","CORESTICKM157SFRBATL",
                              "FLEXCPIM157SFRBATL","COREFLEXCPIM157SFRBATL"],
    "Labor Market":          ["UNRATE","U6RATE","PAYEMS","CIVPART","EMRATIO","CNP16OV",
                              "JTSJOL","JTSHIL","JTSQUR","JTSLDR",
                              "ICSA","CCSA","AWHMAN","AWHAETP","CES0500000003","ECIWAG",
                              "OPHNFB","ULCNFB","PRS85006152","ULCMFG",
                              "MANEMP","SRVPRD","USCONS","USFIRE","USMINE","USTPU"],
    "GDP & Growth":          ["GDP","GDPC1","GDPCA","GDPPOT","GDPDEF",
                              "INDPRO","IPGMFN","TCU","IPB50001N",
                              "DGORDER","NEWORDER","ISRATIO","MNFCTRIRSA",
                              "CFNAI","CFNAIMA3"],
    "Consumer":              ["RETAILSMNSA","RSXFS","PCE","PCEDG","PCEND","PCES",
                              "DSPIC96","PSAVERT","TOTALSL","TERMCBCCALLNS"],
    "Credit & Financial":    ["BAMLH0A0HYM2","BAMLH0A3HYM2","BAMLC0A0CM",
                              "DAAA","DBAA","DRCCLACBS","DRSFRMACBS","DRBLACBS",
                              "BUSLOANS","LOANS","DPSACBW027SBOG","COMPOUT",
                              "DRTSCILM","DRTSCIS","DRSDCILM","DRIWCIL",
                              "NFCI","ANFCI"],
    "Housing":               ["MORTGAGE30US","MORTGAGE15US","HOUST","HOUST1F","PERMIT",
                              "HSN1F","EXHOSLUSM495S","MSACSR","CSUSHPISA","MSPUS",
                              "EVACANTUSQ176N","RRVRUSQ156N","TTLCON"],
    "Money Supply":          ["M1SL","M2SL","M2V","BOGMBASE","AMBSL",
                              "WRMFSL","WRMFNS","WALCL","WTREGEN","TOTRESNS","RRPONTSYD"],
    "Trade & FX":            ["BOPTEXP","BOPTIMP","XTEXVA01USM667S","DTWEXBGS",
                              "DEXUSEU","DEXJPUS","DEXUSUK","DEXCHUS",
                              "DEXCAUS","DEXBZUS","DEXKOUS","DEXINUS","DEXMXUS",
                              "WHLSLRIMSA","WHLSLRSMSA"],
    "Energy & Commodities":  ["DCOILWTICO","DCOILBRENTEU","GASREGCOVW","DHHNGSP",
                              "APU000072610","GOLDAMGBD228NLBM",
                              "PCOPPUSDM","PNICKUSDM","PIORECRUSDM",
                              "PWHEAMTUSDM","PMAIZMTUSDM","PCOTTINDUSDM"],
    "Market Indicators":     ["VIXCLS","USREC","USRECM","GACDISA066MSFRBNY"],
}