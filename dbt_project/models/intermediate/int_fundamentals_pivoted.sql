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
        max(case when line_item = 'TotalRevenue' then value end)                          as total_revenue,
        max(case when line_item = 'CostOfRevenue' then value end)                         as cost_of_revenue,
        max(case when line_item = 'GrossProfit' then value end)                           as gross_profit,
        max(case when line_item = 'OperatingIncome' then value end)                       as operating_income,
        max(case when line_item = 'NetIncome' then value end)                             as net_income,
        max(case when line_item = 'EBITDA' then value end)                                as ebitda,
        max(case when line_item = 'EBIT' then value end)                                  as ebit,
        max(case when line_item = 'ResearchAndDevelopment' then value end)                as research_and_development,
        max(case when line_item = 'SellingGeneralAndAdministration' then value end)       as selling_general_admin,
        max(case when line_item = 'InterestExpense' then value end)                       as interest_expense,
        max(case when line_item = 'DilutedEPS' then value end)                            as diluted_eps,
        max(case when line_item = 'BasicEPS' then value end)                              as basic_eps,
        max(case when line_item = 'DilutedAverageShares' then value end)                  as diluted_shares,
        max(case when line_item = 'TaxProvision' then value end)                          as tax_provision,

        -- Balance sheet
        max(case when line_item = 'TotalAssets' then value end)                           as total_assets,
        max(case when line_item = 'TotalLiabilitiesNetMinorityInterest' then value end)   as total_liabilities,
        max(case when line_item = 'StockholdersEquity' then value end)                    as stockholders_equity,
        max(case when line_item = 'CashAndCashEquivalents' then value end)                as cash_and_equivalents,
        max(case when line_item = 'CashCashEquivalentsAndShortTermInvestments' then value end) as cash_and_short_term_investments,
        max(case when line_item = 'TotalDebt' then value end)                             as total_debt,
        max(case when line_item = 'NetDebt' then value end)                               as net_debt,
        max(case when line_item = 'CurrentAssets' then value end)                         as current_assets,
        max(case when line_item = 'CurrentLiabilities' then value end)                    as current_liabilities,
        max(case when line_item = 'Inventory' then value end)                             as inventory,
        max(case when line_item = 'AccountsReceivable' then value end)                    as accounts_receivable,
        max(case when line_item = 'AccountsPayable' then value end)                       as accounts_payable,
        max(case when line_item = 'RetainedEarnings' then value end)                      as retained_earnings,
        max(case when line_item = 'NetPPE' then value end)                                as net_ppe,

        -- Cash flow
        max(case when line_item = 'OperatingCashFlow' then value end)                     as operating_cash_flow,
        max(case when line_item = 'CapitalExpenditure' then value end)                    as capital_expenditure,
        max(case when line_item = 'FreeCashFlow' then value end)                          as free_cash_flow,
        max(case when line_item = 'InvestingCashFlow' then value end)                     as investing_cash_flow,
        max(case when line_item = 'FinancingCashFlow' then value end)                     as financing_cash_flow,
        max(case when line_item = 'DepreciationAndAmortization' then value end)           as depreciation_and_amortization,
        max(case when line_item = 'StockBasedCompensation' then value end)                as stock_based_compensation,
        max(case when line_item = 'CommonStockDividendPaid' then value end)               as dividends_paid,
        max(case when line_item = 'RepurchaseOfCapitalStock' then value end)              as share_repurchases,

        -- Derived margins (computed here so marts don't repeat the logic)
        case when max(case when line_item = 'TotalRevenue' then value end) > 0
             then max(case when line_item = 'GrossProfit' then value end)
                  / max(case when line_item = 'TotalRevenue' then value end)
        end                                                                               as gross_margin,

        case when max(case when line_item = 'TotalRevenue' then value end) > 0
             then max(case when line_item = 'OperatingIncome' then value end)
                  / max(case when line_item = 'TotalRevenue' then value end)
        end                                                                               as operating_margin,

        case when max(case when line_item = 'TotalRevenue' then value end) > 0
             then max(case when line_item = 'NetIncome' then value end)
                  / max(case when line_item = 'TotalRevenue' then value end)
        end                                                                               as net_margin

    from statements
    group by ticker, period_end_date, frequency
)

select * from pivoted
where total_revenue is not null or total_assets is not null or operating_cash_flow is not null
