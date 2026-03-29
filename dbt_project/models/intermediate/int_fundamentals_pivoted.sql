{{ config(schema='staging') }}

{#
    Pivots the EAV financial statement data into a wide format with one row
    per ticker + period + frequency, with the ~25 most important line items
    as named columns.

    This is the analyst-friendly view. The full EAV remains in staging
    for ad-hoc queries on less common line items.

    Line item names come from yfinance and use CamelCase (e.g. "TotalRevenue").
    We pivot using conditional aggregation which is safe for the EAV grain.
#}

with statements as (
    select * from {{ ref('stg_financial_statements') }}
),

pivoted as (
    select
        ticker,
        period_end_date,
        frequency,

       -- Income statement
        max(case when line_item = 'Total Revenue' then value end)                          as total_revenue,
        max(case when line_item = 'Cost Of Revenue' then value end)                        as cost_of_revenue,
        max(case when line_item = 'Gross Profit' then value end)                           as gross_profit,
        max(case when line_item = 'Operating Income' then value end)                       as operating_income,
        max(case when line_item = 'Net Income' then value end)                             as net_income,
        max(case when line_item = 'EBITDA' then value end)                                 as ebitda,
        max(case when line_item = 'EBIT' then value end)                                   as ebit,
        max(case when line_item = 'Research And Development' then value end)                as research_and_development,
        max(case when line_item = 'Selling General And Administration' then value end)      as selling_general_admin,
        max(case when line_item = 'Interest Expense' then value end)                       as interest_expense,
        max(case when line_item = 'Diluted EPS' then value end)                            as diluted_eps,
        max(case when line_item = 'Basic EPS' then value end)                              as basic_eps,
        max(case when line_item = 'Diluted Average Shares' then value end)                 as diluted_shares,
        max(case when line_item = 'Tax Provision' then value end)                          as tax_provision,

        -- Balance sheet
        max(case when line_item = 'Total Assets' then value end)                           as total_assets,
        max(case when line_item = 'Total Liabilities Net Minority Interest' then value end) as total_liabilities,
        max(case when line_item = 'Stockholders Equity' then value end)                    as stockholders_equity,
        max(case when line_item = 'Cash And Cash Equivalents' then value end)              as cash_and_equivalents,
        max(case when line_item = 'Cash Cash Equivalents And Short Term Investments' then value end) as cash_and_short_term_investments,
        max(case when line_item = 'Total Debt' then value end)                             as total_debt,
        max(case when line_item = 'Net Debt' then value end)                               as net_debt,
        max(case when line_item = 'Current Assets' then value end)                         as current_assets,
        max(case when line_item = 'Current Liabilities' then value end)                    as current_liabilities,
        max(case when line_item = 'Inventory' then value end)                              as inventory,
        max(case when line_item = 'Accounts Receivable' then value end)                    as accounts_receivable,
        max(case when line_item = 'Accounts Payable' then value end)                       as accounts_payable,
        max(case when line_item = 'Retained Earnings' then value end)                      as retained_earnings,
        max(case when line_item = 'Net PPE' then value end)                                as net_ppe,

        -- Cash flow
        max(case when line_item = 'Operating Cash Flow' then value end)                    as operating_cash_flow,
        max(case when line_item = 'Capital Expenditure' then value end)                    as capital_expenditure,
        max(case when line_item = 'Free Cash Flow' then value end)                         as free_cash_flow,
        max(case when line_item = 'Investing Cash Flow' then value end)                    as investing_cash_flow,
        max(case when line_item = 'Financing Cash Flow' then value end)                    as financing_cash_flow,
        max(case when line_item = 'Depreciation And Amortization' then value end)          as depreciation_and_amortization,
        max(case when line_item = 'Stock Based Compensation' then value end)               as stock_based_compensation,
        max(case when line_item = 'Common Stock Dividend Paid' then value end)             as dividends_paid,
        max(case when line_item = 'Repurchase Of Capital Stock' then value end)            as share_repurchases,

        -- Derived margins
        case when max(case when line_item = 'Total Revenue' then value end) > 0
             then max(case when line_item = 'Gross Profit' then value end)
                  / max(case when line_item = 'Total Revenue' then value end)
        end                                                                                as gross_margin,

        case when max(case when line_item = 'Total Revenue' then value end) > 0
             then max(case when line_item = 'Operating Income' then value end)
                  / max(case when line_item = 'Total Revenue' then value end)
        end                                                                                as operating_margin,

        case when max(case when line_item = 'Total Revenue' then value end) > 0
             then max(case when line_item = 'Net Income' then value end)
                  / max(case when line_item = 'Total Revenue' then value end)
        end                                                                                as net_margin                                                                        as net_margin

    from statements
    group by ticker, period_end_date, frequency
)

select * from pivoted
where total_revenue is not null or total_assets is not null or operating_cash_flow is not null
