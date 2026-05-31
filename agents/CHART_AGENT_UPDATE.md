## Chart Agent System Prompt Addition

Add these table definitions to the SYSTEM_PROMPT in agents/chart_agent.py,
after the existing FACT_MACRO_READINGS block:

---

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

---

Also add these example prompts to the sidebar:

- "Show me AAPL's revenue and net income trend over the last 4 years"
- "Which S&P 500 stocks have the lowest trailing PE ratio?"
- "Compare operating margins for AAPL, MSFT, GOOGL and META"
- "Show me the top 10 stocks by free cash flow yield"
- "How has JPM's return on equity changed over time?"
- "Compare debt-to-equity ratios across bank stocks"
- "Which stocks have the highest revenue growth?"
