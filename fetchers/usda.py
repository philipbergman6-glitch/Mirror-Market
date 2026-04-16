"""
Layer 2 — USDA crop and supply/demand data via the NASS QuickStats API.

The USDA publishes reports (WASDE, crop progress, export inspections)
that are the #1 driver of soybean price moves.  This fetcher pulls
structured data from their free API.

Sign up for a key at: https://quickstats.nass.usda.gov/api

Set it as an environment variable:
    export USDA_API_KEY="your-key-here"

Key concepts for learning:
    - REST APIs: you send an HTTP GET with query parameters,
      the server sends back JSON data.
    - requests.get() returns a Response object; .json() parses it.
    - try/except catches errors so one bad request doesn't crash everything.
    - Retry logic handles temporary network problems automatically.
"""

import json
import logging
import time

import requests
import pandas as pd

from config import (
    USDA_API_KEY, USDA_BASE_URL,
    USDA_CROP_PROGRESS_COMMODITIES,
    WASDE_COMMODITIES, WASDE_STAT_CATEGORIES,
    INSPECTIONS_URL,
    REQUEST_TIMEOUT, MAX_RETRIES, RETRY_DELAY,
)

logger = logging.getLogger(__name__)


def fetch_usda(
    commodity: str,
    year_start: int = 2020,
    year_end: int = 2026,
    stat_category: str = "PRODUCTION",
) -> pd.DataFrame:
    """
    Pull annual survey data for a commodity from USDA QuickStats.

    Parameters
    ----------
    commodity : str
        e.g. "SOYBEANS", "COFFEE"
    year_start, year_end : int
        Range of crop years to request.
    stat_category : str
        "PRODUCTION", "AREA HARVESTED", "YIELD", etc.

    Returns
    -------
    pd.DataFrame   (empty if the API key is missing or request fails)
    """
    if not USDA_API_KEY:
        logger.warning("USDA_API_KEY not set — skipping USDA fetch.")
        logger.info("  Get a free key: https://quickstats.nass.usda.gov/api")
        return pd.DataFrame()

    params = {
        "key":                USDA_API_KEY,
        "commodity_desc":     commodity,
        "statisticcat_desc":  stat_category,
        "agg_level_desc":     "NATIONAL",
        "source_desc":        "SURVEY",
        "year__GE":           str(year_start),
        "year__LE":           str(year_end),
        "format":             "JSON",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Requesting USDA data for %s / %s (attempt %d) ...",
                        commodity, stat_category, attempt)
            resp = requests.get(USDA_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)

            if resp.status_code != 200:
                logger.warning("HTTP %d: %s", resp.status_code, resp.text[:200])
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                    continue
                return pd.DataFrame()

            payload = resp.json()

            # The API wraps rows in a "data" key
            rows = payload.get("data", [])
            if not rows:
                logger.info("No rows returned for %s / %s.", commodity, stat_category)
                return pd.DataFrame()

            df = pd.DataFrame(rows)

            # Keep only the most useful columns
            keep = [
                "year", "short_desc", "Value", "unit_desc",
                "state_name", "reference_period_desc",
            ]
            keep = [c for c in keep if c in df.columns]
            df = df[keep]

            logger.info("Got %d rows for %s / %s.", len(df), commodity, stat_category)
            return df

        except (requests.RequestException, json.JSONDecodeError) as exc:
            logger.warning(
                "Attempt %d/%d failed for USDA %s/%s: %s",
                attempt, MAX_RETRIES, commodity, stat_category, exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    logger.error("All %d attempts failed for USDA %s/%s — returning empty DataFrame",
                 MAX_RETRIES, commodity, stat_category)
    return pd.DataFrame()


def fetch_soybean_overview() -> dict[str, pd.DataFrame]:
    """Convenience: pull production + area harvested + yield for soybeans."""
    results = {}
    for stat in ("PRODUCTION", "AREA HARVESTED", "YIELD"):
        results[stat] = fetch_usda("SOYBEANS", stat_category=stat)
    return results


def fetch_crop_progress(
    commodity: str = "SOYBEANS",
    year_start: int = 2020,
    year_end: int = 2026,
) -> pd.DataFrame:
    """
    Fetch weekly crop condition and progress data from USDA NASS.

    This is the most price-moving weekly report for US crops. It tells you:
        - What % of the crop is planted, emerged, blooming, mature, harvested
        - What % of the crop is rated good/excellent vs poor/very poor

    A drop in good/excellent % = potential yield loss = price rally.

    Parameters
    ----------
    commodity : str
        e.g. "SOYBEANS", "CORN"
    year_start, year_end : int
        Range of years to request.

    Returns
    -------
    pd.DataFrame
        Columns: year, week_ending, short_desc, Value, unit_desc, state_name
        Empty DataFrame if API key is missing or request fails.
    """
    if not USDA_API_KEY:
        logger.warning("USDA_API_KEY not set — skipping crop progress fetch.")
        return pd.DataFrame()

    all_rows = []
    for stat_cat in ("PROGRESS", "CONDITION"):
        params = {
            "key":                USDA_API_KEY,
            "commodity_desc":     commodity,
            "statisticcat_desc":  stat_cat,
            "agg_level_desc":     "NATIONAL",
            "source_desc":        "SURVEY",
            "freq_desc":          "WEEKLY",
            "year__GE":           str(year_start),
            "year__LE":           str(year_end),
            "format":             "JSON",
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(
                    "Requesting USDA %s %s (attempt %d) ...",
                    commodity, stat_cat, attempt,
                )
                resp = requests.get(USDA_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)

                if resp.status_code != 200:
                    logger.warning("HTTP %d: %s", resp.status_code, resp.text[:200])
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY)
                        continue
                    break

                payload = resp.json()
                rows = payload.get("data", [])
                if rows:
                    all_rows.extend(rows)
                    logger.info("Got %d rows for %s/%s.", len(rows), commodity, stat_cat)
                else:
                    logger.info("No rows for %s/%s.", commodity, stat_cat)
                break

            except (requests.RequestException, json.JSONDecodeError) as exc:
                logger.warning(
                    "Attempt %d/%d failed for USDA %s/%s: %s",
                    attempt, MAX_RETRIES, commodity, stat_cat, exc,
                )
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # Keep useful columns
    keep = [
        "year", "week_ending", "short_desc", "Value",
        "unit_desc", "state_name", "statisticcat_desc",
    ]
    keep = [c for c in keep if c in df.columns]
    df = df[keep]

    logger.info("Total crop progress/condition rows for %s: %d", commodity, len(df))
    return df


def fetch_all_crop_progress() -> dict[str, pd.DataFrame]:
    """
    Fetch crop progress/condition for all configured commodities.

    Returns dict keyed by commodity name (e.g. "SOYBEANS", "CORN").
    """
    results = {}
    for commodity in USDA_CROP_PROGRESS_COMMODITIES:
        results[commodity] = fetch_crop_progress(commodity)
    return results


def fetch_wasde_estimates(
    year_start: int = 2020,
    year_end: int = 2026,
) -> dict[str, pd.DataFrame]:
    """
    Fetch WASDE monthly forecast estimates from USDA NASS QuickStats.

    WASDE (World Agricultural Supply and Demand Estimates) is THE most
    market-moving USDA report. Uses the same API as fetch_usda() but with
    source_desc="FORECAST" instead of "SURVEY" to get projections.

    Returns dict keyed by "COMMODITY/STAT_CATEGORY" (e.g. "SOYBEANS/PRODUCTION").
    """
    if not USDA_API_KEY:
        logger.warning("USDA_API_KEY not set — skipping WASDE fetch.")
        return {}

    results = {}
    for commodity in WASDE_COMMODITIES:
        for stat_cat in WASDE_STAT_CATEGORIES:
            params = {
                "key":                USDA_API_KEY,
                "commodity_desc":     commodity,
                "statisticcat_desc":  stat_cat,
                "agg_level_desc":     "NATIONAL",
                "source_desc":        "FORECAST",
                "year__GE":           str(year_start),
                "year__LE":           str(year_end),
                "format":             "JSON",
            }

            key = f"{commodity}/{stat_cat}"
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    logger.info("Requesting WASDE %s (attempt %d) ...", key, attempt)
                    resp = requests.get(
                        USDA_BASE_URL, params=params, timeout=REQUEST_TIMEOUT,
                    )

                    if resp.status_code != 200:
                        logger.warning("HTTP %d for WASDE %s", resp.status_code, key)
                        if attempt < MAX_RETRIES:
                            time.sleep(RETRY_DELAY)
                            continue
                        break

                    rows = resp.json().get("data", [])
                    if not rows:
                        logger.info("No WASDE rows for %s.", key)
                        break

                    df = pd.DataFrame(rows)
                    keep = [
                        "commodity_desc", "year", "reference_period_desc",
                        "statisticcat_desc", "Value", "unit_desc",
                    ]
                    keep = [c for c in keep if c in df.columns]
                    df = df[keep]
                    results[key] = df
                    logger.info("Got %d WASDE rows for %s.", len(df), key)
                    break

                except (requests.RequestException, json.JSONDecodeError) as exc:
                    logger.warning("WASDE %s attempt %d failed: %s", key, attempt, exc)
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY)

    return results


def fetch_crush_data(
    year_start: int = 2020,
    year_end: int = 2026,
) -> pd.DataFrame:
    """
    Fetch monthly soybean crush/processing volumes from USDA NASS.

    Uses the same API with statisticcat_desc="PROCESSING" to get
    how many bushels of soybeans were actually crushed.
    """
    return fetch_usda("SOYBEANS", year_start, year_end, stat_category="PROCESSING")


def fetch_export_inspections() -> pd.DataFrame:
    """
    Fetch weekly USDA AMS grain export inspections report.

    This report shows actual bushels loaded on ships (not just commitments).
    Parses the AMS text report for soybean rows.

    Returns a DataFrame with columns: commodity, week_ending, inspections_mt
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Fetching AMS export inspections (attempt %d) ...", attempt)
            resp = requests.get(INSPECTIONS_URL, timeout=REQUEST_TIMEOUT)

            if resp.status_code != 200:
                logger.warning("HTTP %d for inspections", resp.status_code)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                    continue
                return pd.DataFrame()

            text = resp.text
            rows = []

            # The AMS report is a fixed-width text file.
            # Parse lines looking for soybean/corn/wheat data.
            target_crops = {"SOYBEANS", "CORN", "WHEAT"}
            current_crop = None

            for line in text.split("\n"):
                line_upper = line.strip().upper()

                # Detect crop headers
                for crop in target_crops:
                    if crop in line_upper and "INSPECTION" in line_upper:
                        current_crop = crop.title()
                        break

                # Look for lines with numeric data (week ending dates + volumes)
                if current_crop and line.strip():
                    parts = line.split()
                    # Try to find date-like and number-like tokens
                    for i, part in enumerate(parts):
                        try:
                            # Check if this looks like a date (MM/DD/YYYY)
                            test_date = pd.to_datetime(part, format="%m/%d/%Y", errors="raise")
                            # Look for a number after the date
                            for j in range(i + 1, min(i + 4, len(parts))):
                                try:
                                    val = float(parts[j].replace(",", ""))
                                    rows.append({
                                        "commodity": current_crop,
                                        "week_ending": test_date.strftime("%Y-%m-%d"),
                                        "inspections_mt": val,
                                    })
                                    break
                                except ValueError:
                                    continue
                            break
                        except (ValueError, TypeError):
                            continue

            if rows:
                df = pd.DataFrame(rows)
                logger.info("Parsed %d inspection rows.", len(df))
                return df
            else:
                logger.info("No parseable inspection data found.")
                return pd.DataFrame()

        except requests.RequestException as exc:
            logger.warning("Inspections attempt %d failed: %s", attempt, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    return pd.DataFrame()


# ── Quick self-test ─────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_soybean_overview()
    logger.info("=== USDA Soybean Summary ===")
    for stat, df in data.items():
        if df.empty:
            logger.info("  %s: no data (API key missing?)", stat)
        else:
            logger.info("  %s: %d rows", stat, len(df))
            logger.info("\n%s", df.head(3).to_string(index=False))

    progress = fetch_all_crop_progress()
    logger.info("=== USDA Crop Progress ===")
    for crop, df in progress.items():
        if df.empty:
            logger.info("  %s: no data", crop)
        else:
            logger.info("  %s: %d rows", crop, len(df))
