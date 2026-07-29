"""
Layer 10 — USDA FAS Export Sales Reporting (ESR).

Weekly export sales data is the #1 indicator of demand pace — every grain
trader checks this every Thursday.  It answers: who is buying, how much,
and is the pace accelerating or slowing?

Key concepts for learning:
    - REST API with JSON responses
    - Market years (e.g. 2025/26 soybeans start Sep 1)
    - "Net sales" = new sales minus cancellations
    - Graceful degradation: if FAS_API_KEY is missing we skip silently
    - Retry logic wraps each HTTP call to handle transient failures
"""

import json
import logging
from datetime import date

import pandas as pd
import requests

from config import (
    EXPORT_SALES_COMMODITIES,
    FAS_API_KEY,
    FAS_BASE_URL,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
)
from fetchers._backoff import retry_sleep

logger = logging.getLogger(__name__)

# api.data.gov gateway auth header (2026 ESRQS migration)
_AUTH_HEADER = "X-Api-Key"


def _current_market_year() -> int:
    """
    Return the current USDA marketing year.

    Most grain marketing years start in September, so:
        - Sep 2025 → Aug 2026 = marketing year 2026
        - Sep 2024 → Aug 2025 = marketing year 2025
    """
    today = date.today()
    if today.month >= 9:
        return today.year + 1
    return today.year


def _fas_get(endpoint: str) -> dict | list | None:
    """
    Make an authenticated GET request to the FAS API with retry logic.

    Returns the parsed JSON or None on failure.
    """
    if not FAS_API_KEY:
        return None

    url = f"{FAS_BASE_URL}{endpoint}"
    headers = {_AUTH_HEADER: FAS_API_KEY}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, json.JSONDecodeError, ValueError) as exc:
            logger.warning(
                "FAS API attempt %d/%d failed for %s: %s",
                attempt, MAX_RETRIES, endpoint, exc,
            )
            if attempt < MAX_RETRIES:
                retry_sleep(attempt)

    logger.error("All %d attempts failed for FAS endpoint %s", MAX_RETRIES, endpoint)
    return None


def fetch_country_map() -> dict[int, str]:
    """
    Fetch the ESR country reference table → {countryCode: countryName}.

    The export endpoints return numeric country codes only; this map turns
    them into readable names. Empty dict on failure.
    """
    data = _fas_get("/countries")
    if not isinstance(data, list):
        return {}

    mapping: dict[int, str] = {}
    for row in data:
        if not isinstance(row, dict):
            continue
        code = row.get("countryCode")
        name = row.get("countryName") or row.get("countryDescription")
        if code is None or not name:
            continue
        try:
            mapping[int(code)] = str(name).strip()
        except (TypeError, ValueError):
            continue
    return mapping


# ESR API field → our column. Listed in fallback order per target column:
# the first source field present wins.
_FIELD_SOURCES = {
    "week_ending": ["weekEndingDate"],
    "net_sales": ["currentMYNetSales", "netSales"],
    "weekly_exports": ["weeklyExports", "currentWeekExports"],
    "accumulated_exports": ["accumulatedExports"],
    "outstanding_sales": ["outstandingSales"],
}


def fetch_export_sales(
    commodity_code: str,
    market_year: int | None = None,
    country_map: dict[int, str] | None = None,
) -> pd.DataFrame:
    """
    Fetch weekly export sales for a single commodity.

    Parameters
    ----------
    commodity_code : str
        ESR commodity code (e.g. "801" for soybeans — see /api/esr/commodities).
    market_year : int or None
        Marketing year to fetch. Defaults to current marketing year.
    country_map : dict or None
        {countryCode: countryName} from fetch_country_map(). Fetched on
        demand if not supplied.

    Returns
    -------
    pd.DataFrame
        Columns: week_ending, country, net_sales, weekly_exports,
                 accumulated_exports, outstanding_sales
        Empty DataFrame if the request fails or no API key is set.
    """
    if not FAS_API_KEY:
        logger.info("FAS_API_KEY not set — skipping export sales")
        return pd.DataFrame()

    if market_year is None:
        market_year = _current_market_year()

    endpoint = f"/exports/commodityCode/{commodity_code}/allCountries/marketYear/{market_year}"
    data = _fas_get(endpoint)

    if not data:
        return pd.DataFrame()

    try:
        df = pd.DataFrame(data)
        if df.empty:
            return df

        out = pd.DataFrame(index=df.index)
        for target, sources in _FIELD_SOURCES.items():
            source = next((s for s in sources if s in df.columns), None)
            if source is None:
                logger.error(
                    "ESR response for code %s missing expected field for '%s' "
                    "(looked for %s) — got columns %s",
                    commodity_code, target, sources, list(df.columns),
                )
                return pd.DataFrame()
            out[target] = df[source]

        # Country arrives as a numeric code; translate to a name.
        if "countryDescription" in df.columns:
            out["country"] = df["countryDescription"]
        elif "countryCode" in df.columns:
            if country_map is None:
                country_map = fetch_country_map()
            codes = pd.to_numeric(df["countryCode"], errors="coerce")
            out["country"] = [
                country_map.get(int(c), str(int(c))) if pd.notna(c) else "Unknown"
                for c in codes
            ]
        else:
            logger.error(
                "ESR response for code %s has no country field — got columns %s",
                commodity_code, list(df.columns),
            )
            return pd.DataFrame()

        return out[[
            "week_ending", "country", "net_sales", "weekly_exports",
            "accumulated_exports", "outstanding_sales",
        ]]

    except (ValueError, KeyError, TypeError) as exc:
        logger.error("Error parsing export sales for code %s: %s", commodity_code, exc)
        return pd.DataFrame()


def fetch_all_export_sales() -> dict[str, pd.DataFrame]:
    """
    Fetch weekly export sales for all commodities in config.

    Returns
    -------
    dict
        {commodity_name: DataFrame} — one entry per commodity.
        Returns empty dict if FAS_API_KEY is not set.
    """
    if not FAS_API_KEY:
        logger.info("FAS_API_KEY not set — skipping all export sales")
        return {}

    results = {}
    market_year = _current_market_year()
    country_map = fetch_country_map()
    if not country_map:
        logger.warning("ESR country lookup failed — country codes will be shown raw")

    for name, code in EXPORT_SALES_COMMODITIES.items():
        logger.info("Fetching export sales for %s (code %s, MY %d) ...", name, code, market_year)
        df = fetch_export_sales(code, market_year, country_map)
        results[name] = df
        if not df.empty:
            logger.info("  Got %d rows for %s", len(df), name)
        else:
            logger.warning("  No export sales data for %s", name)

    return results


# ── Quick self-test ────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_all_export_sales()
    for name, df in data.items():
        if df.empty:
            logger.info("%s: NO DATA", name)
        else:
            logger.info("%s: %d rows", name, len(df))
