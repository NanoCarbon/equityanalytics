import time
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# Timeout for FRED API calls (seconds)
HTTP_TIMEOUT = 30

class _RateLimitError(Exception):
    """Raised when FRED API returns HTTP 429 — signals the caller to back off."""


_DELAY_BASE     = 0.5   # seconds between series requests
_DELAY_MAX      = 30.0  # cap on backed-off delay
_BACKOFF_FACTOR = 2.0   # multiply delay on 429
_RECOVER_FACTOR = 0.9   # multiply delay on each successful fetch

CIRCUIT_BREAKER_THRESHOLD = 5

# ---------------------------------------------------------------------------
# FRED_SERIES — fallback name map used when no DB connection is available.
# Canonical series selection lives in RAW.FRED_SELECTION (Snowflake).
# Add new series there; update this dict only to keep local names in sync.
# ---------------------------------------------------------------------------
FRED_SERIES = {
    # ════════════════════════════════════════════════════════════════
    # ORIGINAL SERIES
    # ════════════════════════════════════════════════════════════════

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
    "BAMLH0A3HYC":   "ICE BofA CCC and Lower HY OAS Spread",
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

    # ── Market risk ───────────────────────────────────────────────
    "VIXCLS":        "CBOE VIX Volatility Index",


    # ════════════════════════════════════════════════════════════════
    # TIER 1 — High priority: market-moving, widely followed
    # ════════════════════════════════════════════════════════════════

    # ── Senior Loan Officer Opinion Survey (SLOOS) ────────────────
    # Quarterly survey of bank lending standards — best leading
    # indicator for credit availability and corporate borrowing costs.
    "DRTSCILM":      "SLOOS: C&I Loan Tightening Standards (Large/Mid Firms)",
    "DRTSCIS":       "SLOOS: C&I Loan Tightening Standards (Small Firms)",
    "DRSDCILM":      "SLOOS: C&I Loan Demand (Large/Mid Firms)",
    "DRIWCIL":       "SLOOS: Willingness to Make Consumer Installment Loans",

    # ── Recession indicators (NBER) ───────────────────────────────
    # Binary 0/1 flag. Essential for overlaying recession shading on
    # any chart and for conditioning factor returns on cycle phase.
    "USREC":         "NBER Recession Indicator (Post-Peak Through Trough)",
    "USRECM":        "NBER Recession Indicator (Peak Through Trough)",

    # ── Chicago Fed macro/financial conditions ────────────────────
    # CFNAI = best single GDP nowcast (85 indicators → 1 number).
    # NFCI/ANFCI = financial conditions independently of VIX.
    "CFNAI":         "Chicago Fed National Activity Index",
    "CFNAIMA3":      "Chicago Fed National Activity Index (3-Month MA)",
    "NFCI":          "Chicago Fed National Financial Conditions Index",
    "ANFCI":         "Chicago Fed Adjusted National Financial Conditions Index",

    # ── CPI sub-components ────────────────────────────────────────
    # Shelter (~33% of CPI) is the stickiest component and the key
    # driver of the "last mile" inflation debate. Services vs.
    # commodities split is critical for regime analysis.
    "CPIHOSSL":      "CPI: Housing and Shelter",
    "CPIENGSL":      "CPI: Energy",
    "CPIMEDSL":      "CPI: Medical Care",
    "CPITRNSL":      "CPI: Transportation",
    "CPIRECSL":      "CPI: Recreation",
    "CUSR0000SAC":   "CPI: Commodities",
    "CUSR0000SAS":   "CPI: Services",
    "CUSR0000SAD":   "CPI: Durables",

    # ── Atlanta Fed Sticky / Flexible Price CPI ───────────────────
    # Modern decomposition: sticky items reset prices infrequently
    # (shelter, medical); flexible items reset frequently (gas, food).
    # Sticky CPI is a better measure of entrenched inflation.
    "STICKCPIM157SFRBATL":    "Atlanta Fed Sticky Price CPI",
    "CORESTICKM157SFRBATL":   "Atlanta Fed Core Sticky Price CPI",
    "FLEXCPIM157SFRBATL":     "Atlanta Fed Flexible Price CPI",
    "COREFLEXCPIM157SFRBATL": "Atlanta Fed Core Flexible Price CPI",


    # ════════════════════════════════════════════════════════════════
    # TIER 2 — Important for comprehensive macro coverage
    # ════════════════════════════════════════════════════════════════

    # ── Productivity and costs (BLS) ──────────────────────────────
    # Rising unit labor costs squeeze corporate margins. Productivity
    # growth is the fundamental driver of non-inflationary wage gains.
    "OPHNFB":        "Nonfarm Business Labor Productivity (Output per Hour)",
    "ULCNFB":        "Nonfarm Business Unit Labor Costs",
    "PRS85006152":   "Nonfarm Business Real Hourly Compensation",
    "ULCMFG":        "Manufacturing Unit Labor Costs",

    # ── Employment Cost Index (BLS) ───────────────────────────────
    # Broader than AHE — includes benefits, not just wages. The Fed
    # watches this closely as a measure of labor cost inflation.
    "ECIWAG":        "Employment Cost Index: Wages and Salaries (Private)",

    # ── JOLTS additional sub-series ───────────────────────────────
    # Quits rate = workers' confidence in finding a new job.
    # Spiked during Great Resignation; declining quits = caution.
    "JTSQUR":        "JOLTS: Quits Rate (Total Nonfarm)",
    "JTSLDR":        "JOLTS: Layoffs and Discharges (Total Nonfarm)",

    # ── Interest rate spreads (additional) ───────────────────────
    # Treasury-Fed Funds spreads collapse when the curve inverts vs.
    # the policy rate. Corporate-FF spreads detect credit stress.
    "T10YFFM":       "10-Year Treasury Minus Fed Funds Rate",
    "T5YFFM":        "5-Year Treasury Minus Fed Funds Rate",
    "T1YFFM":        "1-Year Treasury Minus Fed Funds Rate",
    "TB3SMFFM":      "3-Month T-Bill Minus Fed Funds Rate",
    "AAAFF":         "Moody's Aaa Corporate Minus Fed Funds Rate",
    "BAAFF":         "Moody's Baa Corporate Minus Fed Funds Rate",
    "CPFF":          "3-Month Commercial Paper Minus Fed Funds Rate",

    # ── PCE sub-components ────────────────────────────────────────
    # Decompose consumer spending: goods vs. services rotation is a
    # key theme post-COVID and in early cycle / late cycle analysis.
    "PCEDG":         "PCE: Durable Goods",
    "PCEND":         "PCE: Nondurable Goods",
    "PCES":          "PCE: Services",

    # ── GDP deflator ──────────────────────────────────────────────
    "GDPDEF":        "GDP Implicit Price Deflator",

    # ── HQM Corporate Bond Yield Curve ────────────────────────────
    # Treasury yields used for pension discount rates. Relevant for
    # insurance, financials, and pension-funded corporate analysis.
    "HQMCB5YR":      "5-Year HQM Corporate Bond Spot Rate",
    "HQMCB10YR":     "10-Year HQM Corporate Bond Spot Rate",
    "HQMCB30YR":     "30-Year HQM Corporate Bond Spot Rate",

    # ── Business loan delinquency ────────────────────────────────
    "DRBLACBS":      "Business Loan Delinquency Rate (All Commercial Banks)",


    # ════════════════════════════════════════════════════════════════
    # TIER 3 — Sector depth and analytical completeness
    # ════════════════════════════════════════════════════════════════

    # ── Payroll breakdown by sector ───────────────────────────────
    # Total nonfarm (PAYEMS) is in Tier 1. These sector splits enable
    # analysis of which industries are driving job gains/losses.
    "MANEMP":        "All Employees: Manufacturing",
    "SRVPRD":        "All Employees: Service-Providing Industries",
    "USCONS":        "All Employees: Construction",
    "USFIRE":        "All Employees: Financial Activities",
    "USMINE":        "All Employees: Mining and Logging",
    "USTPU":         "All Employees: Trade, Transportation & Utilities",
    "AWHAETP":       "Avg Weekly Hours: Total Private (Broader than Mfg)",

    # ── Labor force additional ────────────────────────────────────
    # EMRATIO is less manipulable than unemployment rate —
    # doesn't exclude discouraged workers the same way.
    "EMRATIO":       "Employment-Population Ratio",
    "CNP16OV":       "Civilian Noninstitutional Population",

    # ── Global commodity prices (IMF) ────────────────────────────
    # Copper is the most-watched single commodity for global growth.
    # Industrial metals and agricultural prices feed into PPI and CPI.
    "PCOPPUSDM":     "Global Price of Copper (USD/Metric Ton)",
    "PNICKUSDM":     "Global Price of Nickel (USD/Metric Ton)",
    "PIORECRUSDM":   "Global Price of Iron Ore (USD/Dry Metric Ton)",
    "PWHEAMTUSDM":   "Global Price of Wheat (USD/Metric Ton)",
    "PMAIZMTUSDM":   "Global Price of Corn (USD/Metric Ton)",
    "PCOTTINDUSDM":  "Global Price of Cotton (USD/Kilogram)",

    # ── Industrial production detail ──────────────────────────────
    "IPGMFN":        "Industrial Production: Manufacturing (NAICS)",

    # ── Empire State Manufacturing Survey ─────────────────────────
    # Released first business day of each month — earliest regional
    # survey and a reliable early read on manufacturing momentum.
    "GACDISA066MSFRBNY": "Empire State Mfg Survey: General Business Conditions (SA)",

    # ── Construction and wholesale ────────────────────────────────
    "TTLCON":        "Total Construction Spending",
    "WHLSLRIMSA":    "Merchant Wholesalers Inventories",
    "WHLSLRSMSA":    "Merchant Wholesalers Sales",

    # ── Consumer credit and retail money ─────────────────────────
    "TERMCBCCALLNS": "Credit Card Interest Rate (All Accounts)",
    "WRMFNS":        "Retail Money Market Funds",
    "COMPOUT":       "Commercial Paper Outstanding",

    # ══════════════════════════════════════════════════════════════
    # WAVE 4 — Regional Fed Manufacturing Surveys
    # Series IDs sourced from FRED_SERIES_CATALOG (verified valid).
    # Richmond and Kansas City Fed surveys are not available on FRED.
    # Empire State (NY Fed) already loaded: GACDISA066MSFRBNY
    # ══════════════════════════════════════════════════════════════

    # Philadelphia Fed Manufacturing Business Outlook Survey
    "GACDFSA066MSFRBPHI": "Philly Fed: Current General Activity (Diffusion Index)",
    "NOCDFSA066MSFRBPHI": "Philly Fed: Current New Orders (Diffusion Index)",
    "PPCDFSA066MSFRBPHI": "Philly Fed: Current Prices Paid (Diffusion Index)",
    "NECDFSA066MSFRBPHI": "Philly Fed: Current Employment (Diffusion Index)",
    "SHCDFSA066MSFRBPHI": "Philly Fed: Current Shipments (Diffusion Index)",

    # Dallas Fed Texas Manufacturing Outlook Survey
    "BACTSAMFRBDAL":      "Dallas Fed: Current General Business Activity (Diffusion Index)",
    "PRODSAMFRBDAL":      "Dallas Fed: Current Production (Diffusion Index)",
    "NEMPSAMFRBDAL":      "Dallas Fed: Current Employment (Diffusion Index)",

    # ══════════════════════════════════════════════════════════════
    # WAVE 5 — Government Finance / Fiscal
    # ══════════════════════════════════════════════════════════════

    "GFDEBTN":       "Federal Debt: Gross Federal Debt Outstanding",
    "GFDEGDQ188S":   "Federal Debt as Percent of GDP",
    "MTSDS133FMS":   "Monthly Treasury Statement: Surplus or Deficit",
    "MTSO133FMS":    "Monthly Treasury Statement: Total Receipts",
    "FGEXPND":       "Federal Government Current Expenditures",
    "GGSAVE":        "Government Net Saving",
    "FYONGDA188S":   "Federal Net Outlays as Percent of GDP",
    "FYFRGDA188S":   "Federal Receipts as Percent of GDP",

    # ══════════════════════════════════════════════════════════════
    # WAVE 6 — Banking Profitability & Deposits
    # ══════════════════════════════════════════════════════════════

    "USNIM":          "Net Interest Margin: All U.S. Banks",
    "USROE":          "Return on Equity: All U.S. Banks",
    "USROA":          "Return on Assets: All U.S. Banks",
    "DRCLACBS":       "Consumer Loan Delinquency Rate: All Commercial Banks",
    "WDTGAL":         "Total Deposits at All Commercial Banks",
    "DPRIME":         "Bank Prime Loan Rate",
    "LTDACBM027NBOG": "Large Time Deposits: All Commercial Banks",
    "EQTA":           "Total Equity to Total Assets for Banks",
}

# Series that require a paid FRED subscription — excluded to avoid 403 errors
# SP500, NASDAQCOM, DJIA, WILL5000PR, NIKKEI225
# Use yfinance (^GSPC, ^IXIC, ^DJI, etc.) for equity index prices instead.


def get_selected_fred_series(conn) -> dict[str, str]:
    """
    Return {series_id: local_name} for all active series in RAW.FRED_SELECTION.

    This is the canonical source for which series to extract.  The catalog
    refresh auto-deactivates entries whose series_id disappears from FRED.
    Falls back to the module-level FRED_SERIES dict on any DB error so that
    the daily DAG never fails purely due to a metadata lookup problem.
    """
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT s.series_id, COALESCE(s.local_name, c.title, s.series_id) AS local_name
            FROM   EQUITY_ANALYTICS.RAW.FRED_SELECTION s
            LEFT   JOIN EQUITY_ANALYTICS.RAW.FRED_SERIES_CATALOG c
                   ON c.series_id = s.series_id
            WHERE  s.is_active = TRUE
            ORDER  BY s.series_id
        """)
        result = {row[0]: row[1] for row in cur.fetchall()}
        cur.close()
        logger.info("Loaded %d active series from FRED_SELECTION", len(result))
        return result
    except Exception as exc:
        logger.warning(
            "Could not load FRED_SELECTION from DB (%s) — falling back to local FRED_SERIES dict",
            exc,
        )
        return dict(FRED_SERIES)


def extract_fred_series(
    api_key: str,
    series_id: str,
    start_date: str | None = None,
    lookback_days: int = 365,
    series_name: str | None = None,
) -> pd.DataFrame:
    """
    Extract a single FRED series.

    start_date overrides lookback_days when provided.
    Pass start_date='1900-01-01' to fetch full available history.
    series_name: human-readable label written to the series_name column.
                 Falls back to FRED_SERIES dict, then series_id if omitted.
    Returns empty DataFrame if series not found or request fails.
    Returns None on network failure (signals circuit breaker in caller).
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

        if response.status_code == 429:
            logger.warning("FRED rate limit (429) for %s", series_id)
            raise _RateLimitError(series_id)

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
        df["series_name"] = series_name or FRED_SERIES.get(series_id, series_id)
        df["date"]        = pd.to_datetime(df["date"])
        df["extracted_at"] = datetime.utcnow()

        return df[["series_id", "series_name", "date", "value", "extracted_at"]]

    except _RateLimitError:
        raise  # propagate for caller's backoff loop
    except requests.Timeout:
        logger.warning("FRED request timed out for %s after %ds", series_id, HTTP_TIMEOUT)
        return None  # network failure — signals circuit breaker
    except requests.ConnectionError as e:
        logger.warning("FRED connection error for %s: %s", series_id, type(e).__name__)
        return None  # network failure — signals circuit breaker
    except Exception as e:
        logger.warning("Could not fetch %s: %s", series_id, e)
        return pd.DataFrame()


def extract_all_fred_series(
    api_key: str,
    start_date: str | None = None,
    lookback_days: int = 365,
    series_dict: dict[str, str] | None = None,
) -> pd.DataFrame:
    """
    Extract all selected FRED series and combine into one DataFrame.

    series_dict: {series_id: local_name} to extract.  If None, falls back to
                 the module-level FRED_SERIES dict (no DB connection required).
    Pass start_date='1900-01-01' to fetch full available history for backfill.
    A 0.5-second delay between calls prevents 429 rate-limit errors.
    """
    active_series = series_dict if series_dict is not None else FRED_SERIES
    frames = []
    skipped = 0
    total = len(active_series)
    consecutive_failures = 0
    delay = _DELAY_BASE

    for i, (series_id, series_name) in enumerate(active_series.items(), 1):
        logger.info("Fetching %s (%d/%d)... (delay=%.2fs)", series_id, i, total, delay)

        while True:  # retry loop: only retries on 429
            try:
                result = extract_fred_series(
                    api_key, series_id, start_date, lookback_days, series_name
                )
                break
            except _RateLimitError:
                delay = min(delay * _BACKOFF_FACTOR, _DELAY_MAX)
                logger.warning("Rate limited — sleeping %.1fs before retry", delay)
                time.sleep(delay)

        if result is None:
            consecutive_failures += 1
            skipped += 1
            if consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD:
                logger.error(
                    "Circuit breaker: %d consecutive connection failures — aborting",
                    consecutive_failures,
                )
                break
        else:
            consecutive_failures = 0
            if not result.empty:
                frames.append(result)
                delay = max(_DELAY_BASE, delay * _RECOVER_FACTOR)
            else:
                skipped += 1

        if i < total:
            time.sleep(delay)

    logger.info(
        "FRED extraction complete: %d series fetched, %d skipped/empty",
        len(frames), skipped,
    )

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
