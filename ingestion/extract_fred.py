import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

FRED_SERIES = {
    "DFF": "Fed Funds Rate",
    "CPIAUCSL": "CPI Inflation",
    "T10Y2Y": "Yield Curve Spread",
    "UNRATE": "Unemployment Rate"
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