"""
FRED Series Catalog Extractor

Crawls all FRED statistical releases (~300 publications) and collects
metadata for every series in each release. Stores the result in
RAW.FRED_SERIES_CATALOG — one row per unique FRED series with:

  - series_id, title, frequency, units, seasonal_adjustment
  - popularity (FRED's 0–100 score — use this to prioritize what to ingest)
  - observation_start / observation_end (how far back history goes)
  - release_id / release_name (e.g. "Consumer Price Index", "GDP", etc.)

After the catalog loads, run this in Snowflake to find high-value gaps:

    SELECT release_name, series_id, title, popularity, frequency, units,
           observation_start
    FROM   RAW.FRED_SERIES_CATALOG
    WHERE  series_id NOT IN (SELECT DISTINCT series_id FROM RAW.MACRO_INDICATORS)
      AND  popularity >= 50
    ORDER  BY popularity DESC;

Release-based approach covers the ~300 major statistical publications that
account for the overwhelming majority of analytically useful FRED series.
Each release maps to a named source (BLS, BEA, Federal Reserve, etc.),
making the catalog easy to browse and reason about.

Rate limiting: 0.5s between calls. Full crawl takes 5–15 minutes depending
on the number of series per release. Run monthly — series metadata rarely
changes, but popularity scores and observation_end dates update regularly.
"""

import time
import logging
from datetime import datetime

import requests
import pandas as pd

logger = logging.getLogger(__name__)

FRED_BASE    = "https://api.stlouisfed.org/fred"
HTTP_TIMEOUT = 30
RATE_DELAY   = 0.5   # seconds between API calls


# ─────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────

def _get(api_key: str, endpoint: str, **params) -> dict:
    """
    Make one FRED API request. Retries once on 429.
    Returns empty dict on any error so callers can skip gracefully.
    """
    params.update({"api_key": api_key, "file_type": "json"})
    url = f"{FRED_BASE}/{endpoint}"
    try:
        r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        if r.status_code == 429:
            logger.warning("FRED rate limit — waiting 15s before retry")
            time.sleep(15)
            r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        if not r.ok:
            logger.debug("FRED %s returned HTTP %d", endpoint, r.status_code)
            return {}
        return r.json()
    except requests.Timeout:
        logger.warning("FRED request timed out: %s", endpoint)
        return {}
    except Exception as e:
        logger.warning("FRED request error (%s): %s", endpoint, e)
        return {}


def _parse_series(raw: dict, release_id: int, release_name: str) -> dict:
    """Flatten one FRED series record into a catalog row."""
    notes = raw.get("notes") or ""
    return {
        "series_id":           raw.get("id", ""),
        "title":               raw.get("title", ""),
        "frequency":           raw.get("frequency", ""),
        "frequency_short":     raw.get("frequency_short", ""),
        "units":               raw.get("units", ""),
        "units_short":         raw.get("units_short", ""),
        "seasonal_adjustment": raw.get("seasonal_adjustment", ""),
        "popularity":          raw.get("popularity", 0),
        "group_popularity":    raw.get("group_popularity", 0),
        "observation_start":   raw.get("observation_start", ""),
        "observation_end":     raw.get("observation_end", ""),
        "last_updated":        raw.get("last_updated", ""),
        "release_id":          release_id,
        "release_name":        release_name,
        "notes":               notes[:500],  # truncate — some are very long
    }


# ─────────────────────────────────────────────────────────────────
# Public functions
# ─────────────────────────────────────────────────────────────────

def get_all_releases(api_key: str) -> pd.DataFrame:
    """
    Fetch all FRED statistical releases.
    Returns a DataFrame with release_id, release_name, press_release, link.
    Typically ~300 releases covering all major source agencies.
    """
    data = _get(api_key, "releases", limit=1000, order_by="release_id", sort_order="asc")
    releases = data.get("releases", [])

    if not releases:
        logger.warning("No releases returned from FRED API")
        return pd.DataFrame()

    rows = [
        {
            "release_id":     r["id"],
            "release_name":   r["name"],
            "press_release":  r.get("press_release", False),
            "link":           r.get("link", ""),
            "notes":          (r.get("notes") or "")[:300],
            "extracted_at":   datetime.utcnow(),
        }
        for r in releases
    ]
    df = pd.DataFrame(rows)
    logger.info("Fetched %d FRED releases", len(df))
    return df


def get_release_series(api_key: str, release_id: int, release_name: str) -> list[dict]:
    """
    Fetch all series metadata for one FRED release (paginated).
    Returns a list of catalog row dicts.
    """
    all_series = []
    offset = 0
    page_size = 1000

    while True:
        data = _get(
            api_key, "release/series",
            release_id=release_id,
            limit=page_size,
            offset=offset,
            order_by="popularity",
            sort_order="desc",
        )
        time.sleep(RATE_DELAY)

        batch = data.get("seriess", [])
        if not batch:
            break

        all_series.extend(_parse_series(s, release_id, release_name) for s in batch)
        offset += len(batch)

        if len(batch) < page_size:
            break   # last page

    return all_series


def extract_fred_catalog(api_key: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build the full FRED series catalog by iterating every release.

    Returns:
        releases_df   — one row per FRED release (~300 rows)
        catalog_df    — one row per unique series with release context

    Series that appear in multiple releases are deduplicated — the row with
    the highest popularity score is kept.

    Typical counts after deduplication: 50,000–150,000 unique series.
    Run time with 0.5s rate limiting: ~10–20 minutes.
    """
    releases_df = get_all_releases(api_key)
    if releases_df.empty:
        logger.error("Cannot build catalog — release list is empty")
        return pd.DataFrame(), pd.DataFrame()

    all_series = []
    total = len(releases_df)

    for i, row in enumerate(releases_df.itertuples(), 1):
        logger.info("Processing release %d/%d: %s", i, total, row.release_name)
        batch = get_release_series(api_key, row.release_id, row.release_name)
        all_series.extend(batch)

        if i % 25 == 0:
            logger.info(
                "Catalog progress: %d/%d releases done, %d series collected so far",
                i, total, len(all_series),
            )

    if not all_series:
        logger.warning("No series collected — catalog is empty")
        return releases_df, pd.DataFrame()

    catalog_df = pd.DataFrame(all_series)

    # Deduplicate: if a series appears in multiple releases, keep the row
    # where its own popularity is highest (most canonical assignment).
    catalog_df = (
        catalog_df
        .sort_values("popularity", ascending=False)
        .drop_duplicates(subset=["series_id"])
        .reset_index(drop=True)
    )
    catalog_df["extracted_at"] = datetime.utcnow()

    logger.info(
        "Catalog complete: %d unique series across %d releases",
        len(catalog_df), catalog_df["release_id"].nunique(),
    )
    return releases_df, catalog_df
