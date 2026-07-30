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
    EXPORT_SALES_DEFAULT_MY_START,
    EXPORT_SALES_MY_START_MONTH,
    FAS_API_KEY,
    FAS_BASE_URL,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
)
from fetchers._backoff import retry_sleep

logger = logging.getLogger(__name__)

# api.data.gov gateway auth header (2026 ESRQS migration)
_AUTH_HEADER = "X-Api-Key"


def _current_market_year(start_month: int = EXPORT_SALES_DEFAULT_MY_START) -> int:
    """
    Return the current USDA marketing year for a given MY start month.

    The soy complex and corn start Sep 1 (Sep 2025 → Aug 2026 = MY 2026);
    wheat starts Jun 1 and cotton Aug 1 (see EXPORT_SALES_MY_START_MONTH),
    so their MY rolls forward earlier in the calendar.
    """
    today = date.today()
    if today.month >= start_month:
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


def fetch_unit_map() -> dict[int, str]:
    """
    Fetch the ESR units-of-measure reference → {unitId: unitName}.

    Export rows carry a numeric unitId; without translating it, cotton's
    running bales were being displayed as if they were metric tons.
    Empty dict on failure.
    """
    data = _fas_get("/unitsOfMeasure")
    if not isinstance(data, list):
        return {}

    mapping: dict[int, str] = {}
    for row in data:
        if not isinstance(row, dict):
            continue
        code = row.get("unitId")
        name = row.get("unitNames") or row.get("unitName") or row.get("unitDescription")
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
    unit_map: dict[int, str] | None = None,
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
                 accumulated_exports, outstanding_sales, unit
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

        # Unit of measure — translate unitId so non-MT commodities
        # (cotton reports running bales) aren't mislabeled downstream.
        if "unitId" in df.columns:
            if unit_map is None:
                unit_map = fetch_unit_map()
            unit_ids = pd.to_numeric(df["unitId"], errors="coerce")
            out["unit"] = [
                unit_map.get(int(u), str(int(u))) if pd.notna(u) else ""
                for u in unit_ids
            ]
        else:
            out["unit"] = ""

        return out[[
            "week_ending", "country", "net_sales", "weekly_exports",
            "accumulated_exports", "outstanding_sales", "unit",
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
    country_map = fetch_country_map()
    if not country_map:
        logger.warning("ESR country lookup failed — country codes will be shown raw")
    unit_map = fetch_unit_map()
    if not unit_map:
        logger.warning("ESR unit lookup failed — unit ids will be shown raw")

    for name, code in EXPORT_SALES_COMMODITIES.items():
        start_month = EXPORT_SALES_MY_START_MONTH.get(name, EXPORT_SALES_DEFAULT_MY_START)
        market_year = _current_market_year(start_month)
        logger.info("Fetching export sales for %s (code %s, MY %d) ...", name, code, market_year)
        df = fetch_export_sales(code, market_year, country_map, unit_map)
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
