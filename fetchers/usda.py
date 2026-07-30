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
import re
from datetime import date, datetime, timezone

import pandas as pd
import requests

from config import (
    INSPECTIONS_URL,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    USDA_API_KEY,
    USDA_BASE_URL,
    USDA_CROP_PROGRESS_COMMODITIES,
)
from fetchers._backoff import retry_sleep
from pipeline.results import ScraperShapeError

logger = logging.getLogger(__name__)


def _current_crop_year_end() -> int:
    """USDA reports forecasts up to the next marketing year — current year + 1."""
    return datetime.now(timezone.utc).year + 1


def fetch_usda(
    commodity: str,
    year_start: int = 2020,
    year_end: int | None = None,
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

    if year_end is None:
        year_end = _current_crop_year_end()

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
                    retry_sleep(attempt)
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
                retry_sleep(attempt)

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
    year_end: int | None = None,
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

    if year_end is None:
        year_end = _current_crop_year_end()

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
                        retry_sleep(attempt)
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
                    retry_sleep(attempt)

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


def fetch_crush_data(
    year_start: int = 2020,
    year_end: int | None = None,
) -> pd.DataFrame:
    """
    Fetch monthly soybean crush volumes from USDA NASS.

    Uses the same API with statisticcat_desc="CRUSHED" (NASS retired the
    old "PROCESSING" category — it now 400s with "invalid query") to get
    how many tons of soybeans were actually crushed
    (short_desc "SOYBEANS - CRUSHED, MEASURED IN TONS").
    """
    return fetch_usda("SOYBEANS", year_start, year_end, stat_category="CRUSHED")


# Crops we extract from the AMS summary table.
_INSPECTION_CROPS: tuple[str, ...] = ("SOYBEANS", "CORN", "WHEAT")

# How stale the latest week_ending may be before we treat the report as broken.
_INSPECTION_MAX_AGE_DAYS = 60

_DATE_TOKEN_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")
_INT_TOKEN_RE = re.compile(r"^-?[\d,]+$")


def _parse_inspections(
    text: str,
    today: date | None = None,
) -> pd.DataFrame:
    """Column-aware parser for the AMS WA_GR101 export inspections report.

    The report contains a summary table where each header row exposes
    three MM/DD/YYYY week-ending dates followed by two market-year-to-date
    columns. We parse only the three weekly columns — those are the
    rows we store. The parser asserts (a) at least one row produced and
    (b) the most-recent week_ending falls within the last
    ``_INSPECTION_MAX_AGE_DAYS`` days, otherwise the upstream report
    is stale or broken and we raise ScraperShapeError.

    The ``today`` parameter is for testability — callers in production
    leave it as None and use the current UTC date.

    Returns a DataFrame with columns ``commodity``, ``week_ending``
    (ISO ``YYYY-MM-DD``), and ``inspections_mt`` (float).
    """
    lines = text.splitlines()

    header_idx = None
    date_tokens: list[str] = []

    # The header is the line that begins with the literal "GRAIN" anchor and
    # contains three MM/DD/YYYY tokens in order.
    for i, line in enumerate(lines):
        tokens = line.split()
        if not tokens or tokens[0].upper() != "GRAIN":
            continue
        dates = [t for t in tokens if _DATE_TOKEN_RE.match(t)]
        if len(dates) >= 3:
            header_idx = i
            date_tokens = dates[:3]
            break

    if header_idx is None:
        raise ScraperShapeError(
            "AMS inspections: could not locate header row 'GRAIN  MM/DD/YYYY  MM/DD/YYYY  MM/DD/YYYY'"
        )

    week_endings = [
        datetime.strptime(d, "%m/%d/%Y").date().isoformat()
        for d in date_tokens
    ]

    rows: list[dict[str, object]] = []
    for raw_line in lines[header_idx + 1:]:
        stripped = raw_line.strip()
        if not stripped:
            # Blank line still inside the table is tolerated; the table
            # ends at a "Total" row or a non-grain header further down.
            continue
        tokens = stripped.split()
        crop_token = tokens[0].upper()

        if crop_token in ("TOTAL", "CROP", "INCLUDES"):
            break
        if crop_token not in _INSPECTION_CROPS:
            continue

        # Pull the first three integer-like tokens after the crop name —
        # those are the values for the three week-ending columns.
        numeric_tokens = [t for t in tokens[1:] if _INT_TOKEN_RE.match(t)]
        if len(numeric_tokens) < 3:
            raise ScraperShapeError(
                f"AMS inspections: row '{stripped}' has fewer than 3 numeric columns"
            )

        for week_ending, value_token in zip(week_endings, numeric_tokens[:3], strict=False):
            try:
                value = float(value_token.replace(",", ""))
            except ValueError as exc:
                raise ScraperShapeError(
                    f"AMS inspections: could not parse '{value_token}' as a number"
                ) from exc
            rows.append({
                "commodity": crop_token.title(),
                "week_ending": week_ending,
                "inspections_mt": value,
            })

    if not rows:
        raise ScraperShapeError(
            "AMS inspections: header located but no soybean/corn/wheat rows beneath it"
        )

    df = pd.DataFrame(rows)

    today = today or datetime.now(timezone.utc).date()
    latest = max(datetime.strptime(d, "%Y-%m-%d").date() for d in df["week_ending"])
    age_days = (today - latest).days
    if age_days > _INSPECTION_MAX_AGE_DAYS:
        raise ScraperShapeError(
            f"AMS inspections: latest week_ending {latest} is {age_days} days old "
            f"(threshold {_INSPECTION_MAX_AGE_DAYS}) — upstream report appears stale"
        )

    return df


def fetch_export_inspections() -> pd.DataFrame:
    """Fetch the weekly USDA AMS grain export inspections report.

    Returns a DataFrame with ``commodity``, ``week_ending``,
    ``inspections_mt``. Returns an empty DataFrame when the report
    cannot be downloaded or the upstream layout has changed.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Fetching AMS export inspections (attempt %d) ...", attempt)
            resp = requests.get(INSPECTIONS_URL, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            logger.warning("Inspections attempt %d failed: %s", attempt, exc)
            if attempt < MAX_RETRIES:
                retry_sleep(attempt)
            continue

        if resp.status_code != 200:
            logger.warning("HTTP %d for inspections", resp.status_code)
            if attempt < MAX_RETRIES:
                retry_sleep(attempt)
                continue
            return pd.DataFrame()

        try:
            df = _parse_inspections(resp.text)
        except ScraperShapeError as exc:
            logger.error("AMS inspections: shape changed — %s", exc)
            return pd.DataFrame()

        logger.info("Parsed %d inspection rows.", len(df))
        return df

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
