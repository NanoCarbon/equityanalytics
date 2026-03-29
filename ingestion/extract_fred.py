import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

FRED_SERIES = {
    # Interest rates
    "DFF": "Fed Funds Rate",
    "DGS10": "10-Year Treasury Yield",
    "DGS2": "2-Year Treasury Yield",
    "DGS30": "30-Year Treasury Yield",
    "DGS1MO": "1-Month Treasury Yield",
    "DGS3MO": "3-Month Treasury Yield",
    "DGS6MO": "6-Month Treasury Yield",
    "DGS1": "1-Year Treasury Yield",
    "DGS5": "5-Year Treasury Yield",
    "DGS7": "7-Year Treasury Yield",
    # Yield curve spreads
    "T10Y2Y": "10Y-2Y Treasury Spread",
    "T10Y3M": "10Y-3M Treasury Spread",
    "T5YIFR": "5-Year Forward Inflation Rate",
    # Inflation
    "CPIAUCSL": "CPI All Items",
    "CPILFESL": "Core CPI ex Food Energy",
    "PCEPI": "PCE Price Index",
    "PCEPILFE": "Core PCE Price Index",
    "MICH": "UMich Inflation Expectations",
    "DFII10": "10-Year Real Treasury Yield",
    "DFII5": "5-Year Real Treasury Yield",
    # Labor market
    "UNRATE": "Unemployment Rate",
    "U6RATE": "U-6 Unemployment Rate",
    "PAYEMS": "Nonfarm Payrolls",
    "CIVPART": "Labor Force Participation",
    "JTSJOL": "Job Openings",
    "ICSA": "Initial Jobless Claims",
    "CCSA": "Continuing Jobless Claims",
    "AWHMAN": "Avg Weekly Hours Manufacturing",
    "CES0500000003": "Avg Hourly Earnings",
    "JTSHIL": "Hires Level",
    # GDP and growth
    "GDP": "Gross Domestic Product",
    "GDPC1": "Real GDP",
    "GDPCA": "Real GDP Growth Rate",
    "INDPRO": "Industrial Production Index",
    "TCU": "Capacity Utilization",
    "RETAILSMNSA": "Retail Sales",
    "RSXFS": "Retail Sales ex Auto",
    "DSPIC96": "Real Disposable Income",
    "PCE": "Personal Consumption Expenditures",
    "PSAVERT": "Personal Savings Rate",
    # Credit and financial conditions
    "BAMLH0A0HYM2": "High Yield Spread",
    "BAMLC0A0CM": "Investment Grade Spread",
    "TEDRATE": "TED Spread",
    "MORTGAGE30US": "30-Year Mortgage Rate",
    "MORTGAGE15US": "15-Year Mortgage Rate",
    "DRCCLACBS": "Credit Card Delinquency Rate",
    "BUSLOANS": "Commercial Loans",
    "CONSUMERSENTIMENT": "Consumer Sentiment",
    "TOTALSL": "Total Consumer Credit",
    "DPSACBW027SBOG": "Bank Deposits",
    # Housing
    "HOUST": "Housing Starts",
    "PERMIT": "Building Permits",
    "HSN1F": "New Home Sales",
    "EXHOSLUSM495S": "Existing Home Sales",
    "CSUSHPISA": "Case-Shiller Home Price Index",
    "MSPUS": "Median Home Price",
    "EVACANTUSQ176N": "Homeowner Vacancy Rate",
    "RRVRUSQ156N": "Rental Vacancy Rate",
    "FIXHAI": "Housing Affordability Index",
    "NHSDPTS": "New Home Supply",
    # Money supply
    "M2SL": "M2 Money Supply",
    "M1SL": "M1 Money Supply",
    "BOGMBASE": "Monetary Base",
    "WRMFSL": "Money Market Funds",
    "AMBSL": "St Louis Adjusted Monetary Base",
    # Business and sentiment
    "UMCSENT": "Consumer Sentiment Index",
    "BOPTEXP": "Exports",
    "BOPTIMP": "Imports",
    "XTEXVA01USM667S": "Export Value Index",
    "DGORDER": "Durable Goods Orders",
    "NEWORDER": "Manufacturing New Orders",
    "ISRATIO": "Inventory to Sales Ratio",
    "MNFCTRIRSA": "Manufacturing Inventories",
    "PPIACO": "Producer Price Index",
    "IPB50001N": "Business Equipment Production",
    # Energy
    "DCOILWTICO": "WTI Crude Oil Price",
    "DCOILBRENTEU": "Brent Crude Oil Price",
    "GASREGCOVW": "Regular Gas Price",
    "DHHNGSP": "Natural Gas Price",
    "ELECTPSM": "Electricity Price",
    # International
    "DEXUSEU": "USD/EUR Exchange Rate",
    "DEXJPUS": "USD/JPY Exchange Rate",
    "DEXUSUK": "USD/GBP Exchange Rate",
    "DEXCHUS": "USD/CNY Exchange Rate",
    "DEXCAUS": "USD/CAD Exchange Rate",
    "DEXBZUS": "USD/BRL Exchange Rate",
    "DEXKOUS": "USD/KRW Exchange Rate",
    "DEXINUS": "USD/INR Exchange Rate",
    # Market indicators
    "VIXCLS": "CBOE VIX",
    "DEXMXUS": "USD/MXN Exchange Rate",
    "NASDAQCOM": "NASDAQ Composite",
    "SP500": "S&P 500 Index",
    "DJIA": "Dow Jones Industrial Average",
    "WILL5000PR": "Wilshire 5000",
    "NIKKEI225": "Nikkei 225",
    "DAAA": "Moody AAA Corporate Bond Yield",
    "DBAA": "Moody BAA Corporate Bond Yield",
    "OBMMIFHA30YF": "FHA 30-Year Mortgage Rate",
}

def extract_fred_series(
    api_key: str,
    series_id: str,
    start_date: str | None = None,
    lookback_days: int = 365
) -> pd.DataFrame:
    """
    Extract a single FRED series.
    Returns a DataFrame with columns: series_id, date, value, extracted_at
    """
    if start_date is None:
        start_date = (datetime.today() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date,
        "sort_order": "asc"
    }

    response = requests.get(FRED_BASE_URL, params=params)
    response.raise_for_status()
    
    data = response.json()
    observations = data.get("observations", [])

    if not observations:
        print(f"No data returned for {series_id}")
        return pd.DataFrame()

    df = pd.DataFrame(observations)[["date", "value"]]
    
    # FRED uses "." for missing values
    df = df[df["value"] != "."]
    df["value"] = df["value"].astype(float)
    df["series_id"] = series_id
    df["series_name"] = FRED_SERIES.get(series_id, series_id)
    df["date"] = pd.to_datetime(df["date"])
    df["extracted_at"] = datetime.utcnow()

    return df[["series_id", "series_name", "date", "value", "extracted_at"]]


def extract_all_fred_series(
    api_key: str,
    start_date: str | None = None,
    lookback_days: int = 365
) -> pd.DataFrame:
    """Extract all configured FRED series and combine into one DataFrame."""
    frames = []
    for series_id in FRED_SERIES:
        print(f"Fetching {series_id}...")
        df = extract_fred_series(api_key, series_id, start_date, lookback_days)
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)