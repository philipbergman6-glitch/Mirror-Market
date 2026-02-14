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

import logging
import time
from datetime import date

import pandas as pd
import requests

from config import (
    FAS_API_KEY,
    FAS_BASE_URL,
    EXPORT_SALES_COMMODITIES,
    REQUEST_TIMEOUT,
    MAX_RETRIES,
    RETRY_DELAY,
)

logger = logging.getLogger(__name__)


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
    headers = {"API_KEY": FAS_API_KEY}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.warning(
                "FAS API attempt %d/%d failed for %s: %s",
                attempt, MAX_RETRIES, endpoint, exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    logger.error("All %d attempts failed for FAS endpoint %s", MAX_RETRIES, endpoint)
    return None


def fetch_export_sales(commodity_code: str, market_year: int | None = None) -> pd.DataFrame:
    """
    Fetch weekly export sales for a single commodity.

    Parameters
    ----------
    commodity_code : str
        USDA FAS commodity code (e.g. "2222000" for soybeans).
    market_year : int or None
        Marketing year to fetch. Defaults to current marketing year.

    Returns
    -------
    pd.DataFrame
        Columns: weekEndingDate, commodity, country, netSales, exports,
                 accumulatedExports, outstandingSales
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

        # Keep the columns we care about (API returns many fields)
        keep_cols = [
            "weekEndingDate", "countryDescription",
            "netSales", "currentWeekExports",
            "accumulatedExports", "outstandingSales",
        ]
        present = [c for c in keep_cols if c in df.columns]
        df = df[present].copy()

        # Rename for consistency
        rename = {
            "weekEndingDate": "week_ending",
            "countryDescription": "country",
            "currentWeekExports": "weekly_exports",
            "netSales": "net_sales",
            "accumulatedExports": "accumulated_exports",
            "outstandingSales": "outstanding_sales",
        }
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

        return df

    except Exception as exc:
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

    for name, code in EXPORT_SALES_COMMODITIES.items():
        logger.info("Fetching export sales for %s (code %s, MY %d) ...", name, code, market_year)
        df = fetch_export_sales(code, market_year)
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
