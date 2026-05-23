"""
One-time script to seed FRED_SELECTION from the corrected series dict.
Run inside the Airflow container:
  docker compose exec airflow-webserver python3 /opt/airflow/ingestion/seed_fred_selection.py
"""
import sys
sys.path.insert(0, '/opt/airflow')
from ingestion.load import get_connection

SELECTIONS = {
    # Interest Rates
    "DFF":           "Fed Funds Rate (Daily Effective)",
    "FEDFUNDS":      "Fed Funds Rate (Monthly Effective)",
    "DFEDTARU":      "Fed Funds Target Rate Upper Bound",
    "DFEDTARL":      "Fed Funds Target Rate Lower Bound",
    "SOFR":          "Secured Overnight Financing Rate",
    "DGS1MO":        "1-Month Treasury Yield",
    "DGS3MO":        "3-Month Treasury Yield",
    "DGS6MO":        "6-Month Treasury Yield",
    "DGS1":          "1-Year Treasury Yield",
    "DGS2":          "2-Year Treasury Yield",
    "DGS5":          "5-Year Treasury Yield",
    "DGS7":          "7-Year Treasury Yield",
    "DGS10":         "10-Year Treasury Yield",
    "DGS30":         "30-Year Treasury Yield",
    # Yield Curve
    "T10Y2Y":        "10Y-2Y Treasury Spread",
    "T10Y3M":        "10Y-3M Treasury Spread",
    "T5YIFR":        "5-Year Forward Inflation Rate",
    "DFII2":         "2-Year Real Treasury Yield (TIPS)",
    "DFII5":         "5-Year Real Treasury Yield (TIPS)",
    "DFII10":        "10-Year Real Treasury Yield (TIPS)",
    "DFII30":        "30-Year Real Treasury Yield (TIPS)",
    # Inflation
    "CPIAUCSL":      "CPI All Items",
    "CPILFESL":      "Core CPI ex Food and Energy",
    "PCEPI":         "PCE Price Index",
    "PCEPILFE":      "Core PCE Price Index",
    "PPIACO":        "Producer Price Index (All Commodities)",
    "MICH":          "UMich 1-Year Inflation Expectations",
    "UMCSENT":       "UMich Consumer Sentiment Index",
    # Labor Market
    "UNRATE":        "Unemployment Rate",
    "U6RATE":        "U-6 Unemployment Rate (Underemployment)",
    "PAYEMS":        "Nonfarm Payrolls",
    "CIVPART":       "Labor Force Participation Rate",
    "JTSJOL":        "Job Openings (JOLTS)",
    "JTSHIL":        "Hires Level (JOLTS)",
    "ICSA":          "Initial Jobless Claims",
    "CCSA":          "Continuing Jobless Claims",
    "AWHMAN":        "Avg Weekly Hours Manufacturing",
    "CES0500000003": "Avg Hourly Earnings All Employees",
    # GDP & Growth
    "GDP":           "Gross Domestic Product (Nominal)",
    "GDPC1":         "Real GDP",
    "GDPCA":         "Real GDP Growth Rate",
    "GDPPOT":        "Real Potential GDP",
    "INDPRO":        "Industrial Production Index",
    "TCU":           "Capacity Utilization Total Industry",
    "IPB50001N":     "Business Equipment Production Index",
    "DGORDER":       "Durable Goods New Orders",
    "NEWORDER":      "Manufacturing New Orders",
    "ISRATIO":       "Total Business Inventory-to-Sales Ratio",
    "MNFCTRIRSA":    "Manufacturing Inventories",
    # Consumer
    "RETAILSMNSA":   "Retail and Food Services Sales",
    "RSXFS":         "Retail Sales ex Auto",
    "PCE":           "Personal Consumption Expenditures",
    "DSPIC96":       "Real Disposable Personal Income",
    "PSAVERT":       "Personal Savings Rate",
    "TOTALSL":       "Total Consumer Credit Outstanding",
    # Credit & Financial Conditions
    "BAMLH0A0HYM2":  "High Yield OAS Spread",
    "BAMLH0A3HYM2":  "CCC and Lower HY OAS Spread",
    "BAMLC0A0CM":    "Investment Grade OAS Spread",
    "DAAA":          "Moody's AAA Corporate Bond Yield",
    "DBAA":          "Moody's BAA Corporate Bond Yield",
    "TEDRATE":       "TED Spread",
    "DRCCLACBS":     "Credit Card Delinquency Rate",
    "DRSFRMACBS":    "Residential Mortgage Delinquency Rate",
    "BUSLOANS":      "Commercial and Industrial Loans",
    "LOANS":         "Total Loans and Leases at Commercial Banks",
    "DPSACBW027SBOG": "Bank Deposits",
    # Housing
    "MORTGAGE30US":  "30-Year Fixed Mortgage Rate",
    "MORTGAGE15US":  "15-Year Fixed Mortgage Rate",
    "HOUST":         "Total Housing Starts",
    "HOUST1F":       "Single Family Housing Starts",
    "PERMIT":        "Building Permits",
    "HSN1F":         "New Single Family Home Sales",
    "EXHOSLUSM495S": "Existing Home Sales",
    "MSACSR":        "Monthly Supply of New Houses",
    "CSUSHPISA":     "Case-Shiller Home Price Index (20-City)",
    "MSPUS":         "Median Sales Price of Existing Homes",
    "EVACANTUSQ176N": "Homeowner Vacancy Rate",
    "RRVRUSQ156N":   "Rental Vacancy Rate",
    # Money Supply & Fed Balance Sheet
    "M1SL":          "M1 Money Supply",
    "M2SL":          "M2 Money Supply",
    "M2V":           "M2 Money Velocity",
    "BOGMBASE":      "Monetary Base",
    "AMBSL":         "St. Louis Adjusted Monetary Base",
    "WRMFSL":        "Money Market Funds Total Assets",
    "WALCL":         "Fed Total Assets (Balance Sheet)",
    "WTREGEN":       "Reserve Balances with Federal Reserve",
    "TOTRESNS":      "Total Reserves of Depository Institutions",
    "RRPONTSYD":     "Overnight Reverse Repo Operations",
    # Trade & International
    "BOPTEXP":       "Exports of Goods and Services",
    "BOPTIMP":       "Imports of Goods and Services",
    "XTEXVA01USM667S": "US Export Value Index",
    "DTWEXBGS":      "Nominal Broad US Dollar Index",
    "DEXUSEU":       "USD/EUR Exchange Rate",
    "DEXJPUS":       "USD/JPY Exchange Rate",
    "DEXUSUK":       "USD/GBP Exchange Rate",
    "DEXCHUS":       "USD/CNY Exchange Rate",
    "DEXCAUS":       "USD/CAD Exchange Rate",
    "DEXBZUS":       "USD/BRL Exchange Rate",
    "DEXKOUS":       "USD/KRW Exchange Rate",
    "DEXINUS":       "USD/INR Exchange Rate",
    "DEXMXUS":       "USD/MXN Exchange Rate",
    # Energy & Commodities
    "DCOILWTICO":    "WTI Crude Oil Price",
    "DCOILBRENTEU":  "Brent Crude Oil Price",
    "GASREGCOVW":    "Regular Gasoline Price (US Average)",
    "DHHNGSP":       "Henry Hub Natural Gas Spot Price",
    "APU000072610":  "Average Electricity Price",
    "GOLDAMGBD228NLBM": "Gold Price (London AM Fix)",
    # Market Risk
    "VIXCLS":        "CBOE VIX Volatility Index",
    # Lending Standards (SLOOS)
    "DRTSCILM":      "SLOOS: C&I Loan Tightening Standards (Large/Mid)",
    "DRTSCIS":       "SLOOS: C&I Loan Tightening Standards (Small Firms)",
    "DRSDCILM":      "SLOOS: C&I Loan Demand (Large/Mid)",
    "DRIWCIL":       "SLOOS: Willingness to Make Consumer Installment Loans",
    # Recession Indicators
    "USREC":         "NBER Recession Indicator (Post-Peak)",
    "USRECM":        "NBER Recession Indicator (Peak Through Trough)",
    # Financial Conditions
    "CFNAI":         "Chicago Fed National Activity Index",
    "CFNAIMA3":      "Chicago Fed National Activity Index (3-Month MA)",
    "NFCI":          "Chicago Fed National Financial Conditions Index",
    "ANFCI":         "Chicago Fed Adjusted National Financial Conditions Index",
    # CPI Sub-components
    "CPIHOSSL":      "CPI: Housing and Shelter",
    "CPIENGSL":      "CPI: Energy",
    "CPIMEDSL":      "CPI: Medical Care",
    "CPITRNSL":      "CPI: Transportation",
    "CPIRECSL":      "CPI: Recreation",
    "CUSR0000SAC":   "CPI: Commodities",
    "CUSR0000SAS":   "CPI: Services",
    "CUSR0000SAD":   "CPI: Durables",
    # Atlanta Fed Sticky/Flexible CPI
    "STICKCPIM157SFRBATL":    "Atlanta Fed Sticky Price CPI",
    "CORESTICKM157SFRBATL":   "Atlanta Fed Core Sticky Price CPI",
    "FLEXCPIM157SFRBATL":     "Atlanta Fed Flexible Price CPI",
    "COREFLEXCPIM157SFRBATL": "Atlanta Fed Core Flexible Price CPI",
    # Productivity & Costs
    "OPHNFB":        "Nonfarm Business Labor Productivity",
    "ULCNFB":        "Nonfarm Business Unit Labor Costs",
    "PRS85006152":   "Nonfarm Business Real Hourly Compensation",
    "ULCMFG":        "Manufacturing Unit Labor Costs",
    "ECIWAG":        "Employment Cost Index: Wages and Salaries",
    # JOLTS Additional
    "JTSQUR":        "JOLTS: Quits Rate",
    "JTSLDR":        "JOLTS: Layoffs and Discharges",
    # Treasury/Rate Spreads
    "T10YFFM":       "10Y Treasury Minus Fed Funds Rate",
    "T5YFFM":        "5Y Treasury Minus Fed Funds Rate",
    "T1YFFM":        "1Y Treasury Minus Fed Funds Rate",
    "TB3SMFFM":      "3-Month T-Bill Minus Fed Funds Rate",
    "AAAFF":         "Moody's Aaa Corporate Minus Fed Funds Rate",
    "BAAFF":         "Moody's Baa Corporate Minus Fed Funds Rate",
    "CPFF":          "3-Month Commercial Paper Minus Fed Funds Rate",
    # PCE Sub-components
    "PCEDG":         "PCE: Durable Goods",
    "PCEND":         "PCE: Nondurable Goods",
    "PCES":          "PCE: Services",
    "GDPDEF":        "GDP Implicit Price Deflator",
    # HQM Corporate Bond
    "HQMCB5YR":      "5-Year HQM Corporate Bond Spot Rate",
    "HQMCB10YR":     "10-Year HQM Corporate Bond Spot Rate",
    "HQMCB30YR":     "30-Year HQM Corporate Bond Spot Rate",
    "DRBLACBS":      "Business Loan Delinquency Rate",
    # Payrolls by Sector
    "MANEMP":        "All Employees: Manufacturing",
    "SRVPRD":        "All Employees: Service-Providing Industries",
    "USCONS":        "All Employees: Construction",
    "USFIRE":        "All Employees: Financial Activities",
    "USMINE":        "All Employees: Mining and Logging",
    "USTPU":         "All Employees: Trade, Transportation & Utilities",
    "AWHAETP":       "Avg Weekly Hours: Total Private",
    "EMRATIO":       "Employment-Population Ratio",
    "CNP16OV":       "Civilian Noninstitutional Population",
    # Global Commodity Prices
    "PCOPPUSDM":     "Global Price of Copper",
    "PNICKUSDM":     "Global Price of Nickel",
    "PIORECRUSDM":   "Global Price of Iron Ore",
    "PWHEAMTUSDM":   "Global Price of Wheat",
    "PMAIZMTUSDM":   "Global Price of Corn",
    "PCOTTINDUSDM":  "Global Price of Cotton",
    "IPGMFN":        "Industrial Production: Manufacturing (NAICS)",
    # Manufacturing Surveys
    "GACDISA066MSFRBNY": "Empire State Mfg Survey: General Business Conditions",
    # Construction & Wholesale
    "TTLCON":        "Total Construction Spending",
    "WHLSLRIMSA":    "Merchant Wholesalers Inventories",
    "WHLSLRSMSA":    "Merchant Wholesalers Sales",
    # Consumer Credit & Money
    "TERMCBCCALLNS": "Credit Card Interest Rate",
    "WRMFNS":        "Retail Money Market Funds",
    "COMPOUT":       "Commercial Paper Outstanding",
    # Wave 4 — Regional Fed Manufacturing (corrected IDs from catalog)
    "GACDFSA066MSFRBPHI": "Philly Fed: Current General Activity (Diffusion Index)",
    "NOCDFSA066MSFRBPHI": "Philly Fed: Current New Orders (Diffusion Index)",
    "PPCDFSA066MSFRBPHI": "Philly Fed: Current Prices Paid (Diffusion Index)",
    "NECDFSA066MSFRBPHI": "Philly Fed: Current Employment (Diffusion Index)",
    "SHCDFSA066MSFRBPHI": "Philly Fed: Current Shipments (Diffusion Index)",
    "BACTSAMFRBDAL":      "Dallas Fed: Current General Business Activity (Diffusion Index)",
    "PRODSAMFRBDAL":      "Dallas Fed: Current Production (Diffusion Index)",
    "NEMPSAMFRBDAL":      "Dallas Fed: Current Employment (Diffusion Index)",
    # Wave 5 — Government Finance
    "GFDEBTN":       "Federal Debt: Gross Federal Debt Outstanding",
    "GFDEGDQ188S":   "Federal Debt as Percent of GDP",
    "MTSDS133FMS":   "Monthly Treasury Statement: Surplus or Deficit",
    "MTSO133FMS":    "Monthly Treasury Statement: Total Receipts",
    "FGEXPND":       "Federal Government Current Expenditures",
    "GGSAVE":        "Government Net Saving",
    "FYONGDA188S":   "Federal Net Outlays as Percent of GDP",
    "FYFRGDA188S":   "Federal Receipts as Percent of GDP",
    # Wave 6 — Banking
    "USNIM":         "Net Interest Margin: All U.S. Banks",
    "USROE":         "Return on Equity: All U.S. Banks",
    "USROA":         "Return on Assets: All U.S. Banks",
    "DRCLACBS":      "Consumer Loan Delinquency Rate: All Commercial Banks",
    "WDTGAL":        "Total Deposits at All Commercial Banks",
    "DPRIME":        "Bank Prime Loan Rate",
    "LTDACBM027NBOG": "Large Time Deposits: All Commercial Banks",
    "EQTA":          "Total Equity to Total Assets for Banks",
}


def seed():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE EQUITY_ANALYTICS.RAW.FRED_SELECTION")
    print(f"Truncated FRED_SELECTION")

    # Build multi-row insert
    value_rows = []
    for sid, nm in SELECTIONS.items():
        sid_esc = sid.replace("'", "''")
        nm_esc  = nm.replace("'", "''")
        value_rows.append(f"('{sid_esc}', '{nm_esc}', TRUE)")

    chunk = 200
    total_inserted = 0
    for i in range(0, len(value_rows), chunk):
        sql = "INSERT INTO EQUITY_ANALYTICS.RAW.FRED_SELECTION (series_id, local_name, is_active) VALUES " \
              + ", ".join(value_rows[i:i+chunk])
        cur.execute(sql)
        total_inserted += len(value_rows[i:i+chunk])

    print(f"Inserted {total_inserted} rows")

    # Populate category from catalog release_name
    cur.execute("""
        UPDATE EQUITY_ANALYTICS.RAW.FRED_SELECTION s
        SET category = c.release_name
        FROM EQUITY_ANALYTICS.RAW.FRED_SERIES_CATALOG c
        WHERE c.series_id = s.series_id
          AND s.category IS NULL
    """)
    print("Categories populated from catalog release_name")

    # Check for any series not in catalog
    cur.execute("""
        SELECT s.series_id, s.local_name
        FROM EQUITY_ANALYTICS.RAW.FRED_SELECTION s
        LEFT JOIN EQUITY_ANALYTICS.RAW.FRED_SERIES_CATALOG c ON c.series_id = s.series_id
        WHERE c.series_id IS NULL
        ORDER BY s.series_id
    """)
    missing = cur.fetchall()
    if missing:
        print(f"\nWARNING: {len(missing)} series not found in FRED catalog:")
        for r in missing:
            print(f"  {r[0]}  {r[1]}")
    else:
        print("All selected series confirmed in FRED catalog")

    # Summary by category
    cur.execute("""
        SELECT COALESCE(category, 'Uncategorized') AS cat, COUNT(*) AS cnt
        FROM EQUITY_ANALYTICS.RAW.FRED_SELECTION
        GROUP BY cat ORDER BY cnt DESC LIMIT 20
    """)
    print("\nTop categories:")
    for r in cur.fetchall():
        print(f"  {r[1]:>4}  {r[0]}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    seed()
