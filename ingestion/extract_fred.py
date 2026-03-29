# TEST Branch protection rules
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import List

logger = logging.getLogger(__name__)

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# Timeout for FRED API calls (seconds)
HTTP_TIMEOUT = 30

FRED_SERIES = {
    # Interest rates
    "DFF": "Fed Funds Rate",
    "DGS1MO": "1-Month Treasury Yield",
    "DGS3MO": "3-Month Treasury Yield",
    "DGS6MO": "6-Month Treasury Yield",
    "DGS1": "1-Year Treasury Yield",
    "DGS2": "2-Year Treasury Yield",
    "DGS5": "5-Year Treasury Yield",
    "DGS7": "7-Year Treasury Yield",
    "DGS10": "10-Year Treasury Yield",
    "DGS30": "30-Year Treasury Yield",

    # Yield curve and real rates
    "T10Y2Y": "10Y-2Y Treasury Spread",
    "T10Y3M": "10Y-3M Treasury Spread",
    "T5YIFR": "5-Year Forward Inflation Rate",
    "DFII5": "5-Year Real Treasury Yield",
    "DFII10": "10-Year Real Treasury Yield",

    # Inflation
    "CPIAUCSL": "CPI All Items",
    "CPILFESL": "Core CPI ex Food Energy",
    "PCEPI": "PCE Price Index",
    "PCEPILFE": "Core PCE Price Index",
    "PPIACO": "Producer Price Index",
    "MICH": "UMich Inflation Expectations",
    "UMCSENT": "Consumer Sentiment Index",

    # Labor market
    "UNRATE": "Unemployment Rate",
    "U6RATE": "U-6 Unemployment Rate",
    "PAYEMS": "Nonfarm Payrolls",
    "CIVPART": "Labor Force Participation",
    "JTSJOL": "Job Openings",
    "JTSHIL": "Hires Level",
    "ICSA": "Initial Jobless Claims",
    "CCSA": "Continuing Jobless Claims",
    "AWHMAN": "Avg Weekly Hours Manufacturing",
    "CES0500000003": "Avg Hourly Earnings",

    # GDP and growth
    "GDP": "Gross Domestic Product",
    "GDPC1": "Real GDP",
    "GDPCA": "Real GDP Growth Rate",
    "INDPRO": "Industrial Production Index",
    "TCU": "Capacity Utilization",
    "IPB50001N": "Business Equipment Production",
    "DGORDER": "Durable Goods Orders",
    "NEWORDER": "Manufacturing New Orders",
    "ISRATIO": "Inventory to Sales Ratio",
    "MNFCTRIRSA": "Manufacturing Inventories",

    # Consumer
    "RETAILSMNSA": "Retail Sales",
    "RSXFS": "Retail Sales ex Auto",
    "PCE": "Personal Consumption Expenditures",
    "DSPIC96": "Real Disposable Income",
    "PSAVERT": "Personal Savings Rate",
    "TOTALSL": "Total Consumer Credit",

    # Credit and financial conditions
    "BAMLH0A0HYM2": "High Yield Spread",
    "BAMLC0A0CM": "Investment Grade Spread",
    "DAAA": "Moody AAA Corporate Bond Yield",
    "DBAA": "Moody BAA Corporate Bond Yield",
    "TEDRATE": "TED Spread",
    "DRCCLACBS": "Credit Card Delinquency Rate",
    "BUSLOANS": "Commercial Loans",
    "DPSACBW027SBOG": "Bank Deposits",

    # Mortgage and housing
    "MORTGAGE30US": "30-Year Mortgage Rate",
    "MORTGAGE15US": "15-Year Mortgage Rate",
    "HOUST": "Housing Starts",
    "HOUST1F": "Single Family Housing Starts",
    "PERMIT": "Building Permits",
    "HSN1F": "New Home Sales",
    "EXHOSLUSM495S": "Existing Home Sales",
    "MSACSR": "Monthly Supply of New Houses",
    "CSUSHPISA": "Case-Shiller Home Price Index",
    "MSPUS": "Median Home Price",
    "EVACANTUSQ176N": "Homeowner Vacancy Rate",
    "RRVRUSQ156N": "Rental Vacancy Rate",

    # Money supply
    "M1SL": "M1 Money Supply",
    "M2SL": "M2 Money Supply",
    "BOGMBASE": "Monetary Base",
    "AMBSL": "St Louis Adjusted Monetary Base",
    "WRMFSL": "Money Market Funds",

    # Trade and international
    "BOPTEXP": "Exports",
    "BOPTIMP": "Imports",
    "XTEXVA01USM667S": "Export Value Index",
    "DEXUSEU": "USD/EUR Exchange Rate",
    "DEXJPUS": "USD/JPY Exchange Rate",
    "DEXUSUK": "USD/GBP Exchange Rate",
    "DEXCHUS": "USD/CNY Exchange Rate",
    "DEXCAUS": "USD/CAD Exchange Rate",
    "DEXBZUS": "USD/BRL Exchange Rate",
    "DEXKOUS": "USD/KRW Exchange Rate",
    "DEXINUS": "USD/INR Exchange Rate",
    "DEXMXUS": "USD/MXN Exchange Rate",

    # Energy and commodities
    "DCOILWTICO": "WTI Crude Oil Price",
    "DCOILBRENTEU": "Brent Crude Oil Price",
    "GASREGCOVW": "Regular Gas Price",
    "DHHNGSP": "Natural Gas Price",
    "APU000072610": "Electricity Price",

    # Market indicators
    "VIXCLS": "CBOE VIX",
    "SP500": "S&P 500 Index",
    "NASDAQCOM": "NASDAQ Composite",
    "DJIA": "Dow Jones Industrial Average",
    "WILL5000PR": "Wilshire 5000",
    "NIKKEI225": "Nikkei 225",
}


def extract_fred_series(
    api_key: str,
    series_id: str,
    start_date: str | None = None,
    lookback_days: int = 365
) -> pd.DataFrame:
    """
    Extract a single FRED series.
    Returns empty DataFrame if series not found or request fails.
    Raises on unexpected errors so callers can decide whether to abort.
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

    try:
        response = requests.get(FRED_BASE_URL, params=params, timeout=HTTP_TIMEOUT)

        if response.status_code == 400:
            logger.warning("Skipping %s — invalid series ID (400)", series_id)
            return pd.DataFrame()

        response.raise_for_status()

        data = response.json()
        observations = data.get("observations", [])

        if not observations:
            logger.info("No data returned for %s", series_id)
            return pd.DataFrame()

        df = pd.DataFrame(observations)[["date", "value"]]
        df = df[df["value"] != "."]
        df["value"] = df["value"].astype(float)
        df["series_id"] = series_id
        df["series_name"] = FRED_SERIES.get(series_id, series_id)
        df["date"] = pd.to_datetime(df["date"])
        df["extracted_at"] = datetime.utcnow()

        return df[["series_id", "series_name", "date", "value", "extracted_at"]]

    except requests.Timeout:
        logger.warning("FRED request timed out for %s after %ds", series_id, HTTP_TIMEOUT)
        return pd.DataFrame()
    except Exception as e:
        logger.warning("Could not fetch %s: %s", series_id, e)
        return pd.DataFrame()


def extract_all_fred_series(
    api_key: str,
    start_date: str | None = None,
    lookback_days: int = 365
) -> pd.DataFrame:
    """Extract all configured FRED series and combine into one DataFrame."""
    frames = []
    skipped = 0

    for series_id in FRED_SERIES:
        logger.info("Fetching %s...", series_id)
        df = extract_fred_series(api_key, series_id, start_date, lookback_days)
        if not df.empty:
            frames.append(df)
        else:
            skipped += 1

    logger.info(
        "FRED extraction complete: %d series fetched, %d skipped/empty",
        len(frames), skipped
    )

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)