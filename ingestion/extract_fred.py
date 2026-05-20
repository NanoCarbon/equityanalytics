import time
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# Timeout for FRED API calls (seconds)
HTTP_TIMEOUT = 30

# Delay between API calls to avoid 429 rate-limit errors
RATE_LIMIT_DELAY = 0.5

FRED_SERIES = {
    # ── Interest rates ────────────────────────────────────────────
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

    # ── Yield curve and real rates ────────────────────────────────
    "T10Y2Y":        "10Y-2Y Treasury Spread",
    "T10Y3M":        "10Y-3M Treasury Spread",
    "T5YIFR":        "5-Year Forward Inflation Rate",
    "DFII2":         "2-Year Real Treasury Yield (TIPS)",
    "DFII5":         "5-Year Real Treasury Yield (TIPS)",
    "DFII10":        "10-Year Real Treasury Yield (TIPS)",
    "DFII30":        "30-Year Real Treasury Yield (TIPS)",

    # ── Inflation ─────────────────────────────────────────────────
    "CPIAUCSL":      "CPI All Items",
    "CPILFESL":      "Core CPI ex Food and Energy",
    "PCEPI":         "PCE Price Index",
    "PCEPILFE":      "Core PCE Price Index",
    "PPIACO":        "Producer Price Index (All Commodities)",
    "MICH":          "UMich 1-Year Inflation Expectations",
    "UMCSENT":       "UMich Consumer Sentiment Index",

    # ── Labor market ──────────────────────────────────────────────
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

    # ── GDP and growth ────────────────────────────────────────────
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

    # ── Consumer ──────────────────────────────────────────────────
    "RETAILSMNSA":   "Retail and Food Services Sales",
    "RSXFS":         "Retail Sales ex Auto",
    "PCE":           "Personal Consumption Expenditures",
    "DSPIC96":       "Real Disposable Personal Income",
    "PSAVERT":       "Personal Savings Rate",
    "TOTALSL":       "Total Consumer Credit Outstanding",

    # ── Credit and financial conditions ───────────────────────────
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

    # ── Mortgage and housing ──────────────────────────────────────
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

    # ── Money supply and Fed balance sheet ────────────────────────
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

    # ── Trade and international ───────────────────────────────────
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

    # ── Energy and commodities ────────────────────────────────────
    "DCOILWTICO":    "WTI Crude Oil Price",
    "DCOILBRENTEU":  "Brent Crude Oil Price",
    "GASREGCOVW":    "Regular Gasoline Price (US Average)",
    "DHHNGSP":       "Henry Hub Natural Gas Spot Price",
    "APU000072610":  "Average Electricity Price",
    "GOLDAMGBD228NLBM": "Gold Price (London AM Fix, USD/Troy oz)",

    # ── Market risk ───────────────────────────────────────────────
    "VIXCLS":        "CBOE VIX Volatility Index",
}

# Series that require a paid FRED subscription — excluded to avoid 403 errors
# SP500, NASDAQCOM, DJIA, WILL5000PR, NIKKEI225
# Use yfinance (^GSPC, ^IXIC, ^DJI, etc.) for equity index prices instead.


def extract_fred_series(
    api_key: str,
    series_id: str,
    start_date: str | None = None,
    lookback_days: int = 365,
) -> pd.DataFrame:
    """
    Extract a single FRED series.

    start_date overrides lookback_days when provided.
    Pass start_date='1900-01-01' to fetch full available history.
    Returns empty DataFrame if series not found or request fails.
    """
    if start_date is None:
        start_date = (datetime.today() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    params = {
        "series_id":        series_id,
        "api_key":          api_key,
        "file_type":        "json",
        "observation_start": start_date,
        "sort_order":       "asc",
    }

    try:
        response = requests.get(FRED_BASE_URL, params=params, timeout=HTTP_TIMEOUT)

        if response.status_code == 400:
            logger.warning("Skipping %s — invalid series ID (400)", series_id)
            return pd.DataFrame()

        if response.status_code == 403:
            logger.warning("Skipping %s — premium series, free key not authorized (403)", series_id)
            return pd.DataFrame()

        response.raise_for_status()

        observations = response.json().get("observations", [])
        if not observations:
            logger.info("No data returned for %s", series_id)
            return pd.DataFrame()

        df = pd.DataFrame(observations)[["date", "value"]]
        df = df[df["value"] != "."]
        df["value"] = df["value"].astype(float)
        df["series_id"]   = series_id
        df["series_name"] = FRED_SERIES.get(series_id, series_id)
        df["date"]        = pd.to_datetime(df["date"])
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
    lookback_days: int = 365,
) -> pd.DataFrame:
    """
    Extract all configured FRED series and combine into one DataFrame.

    Pass start_date='1900-01-01' to fetch full available history for backfill.
    A 0.5-second delay between calls prevents 429 rate-limit errors.
    """
    frames = []
    skipped = 0
    total = len(FRED_SERIES)

    for i, series_id in enumerate(FRED_SERIES, 1):
        logger.info("Fetching %s (%d/%d)...", series_id, i, total)
        df = extract_fred_series(api_key, series_id, start_date, lookback_days)
        if not df.empty:
            frames.append(df)
        else:
            skipped += 1

        if i < total:
            time.sleep(RATE_LIMIT_DELAY)

    logger.info(
        "FRED extraction complete: %d series fetched, %d skipped/empty",
        len(frames), skipped,
    )

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
