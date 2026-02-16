"""
Layer 13 — EIA (Energy Information Administration) biofuel and energy data.

Soybean oil increasingly goes to renewable diesel (~40% of US soy oil demand).
Tracking ethanol production, biodiesel production, and diesel prices shows
how biofuel demand is pulling on the soy complex.

Source: EIA API v2 at https://api.eia.gov/v2/
Sign up for a free key at: https://www.eia.gov/opendata/register.php

Set it as an environment variable:
    export EIA_API_KEY="your-key-here"

Key concepts for learning:
    - EIA API v2 uses a route-based system: you specify a data route
      (like "petroleum/sum/sndw") and filter with query parameters.
    - The API returns JSON with a "response.data" array.
    - Same retry pattern as our other fetchers.
"""

import json
import logging
import time

import requests
import pandas as pd

from config import (
    EIA_API_KEY, EIA_BASE_URL, EIA_SERIES,
    REQUEST_TIMEOUT, MAX_RETRIES, RETRY_DELAY,
)

logger = logging.getLogger(__name__)


def fetch_eia_series(
    name: str,
    route: str,
    series_id: str,
    frequency: str = "weekly",
    start_date: str = "2020-01-01",
) -> pd.DataFrame:
    """
    Fetch a single EIA data series via API v2.

    Parameters
    ----------
    name : str
        Human-readable name (e.g. "Ethanol Production").
    route : str
        API route (e.g. "petroleum/sum/sndw").
    series_id : str
        The series identifier within that route.
    frequency : str
        "weekly" or "monthly".
    start_date : str
        ISO date for start of range.

    Returns
    -------
    pd.DataFrame
        Columns: Date, value, unit.  Empty if key is missing or request fails.
    """
    if not EIA_API_KEY:
        logger.warning("EIA_API_KEY not set — skipping EIA fetch.")
        logger.info("  Get a free key: https://www.eia.gov/opendata/register.php")
        return pd.DataFrame()

    url = f"{EIA_BASE_URL}{route}"

    params = {
        "api_key":   EIA_API_KEY,
        "frequency": frequency,
        "start":     start_date,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length":    5000,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Requesting EIA %s (attempt %d) ...", name, attempt)
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)

            if resp.status_code != 200:
                logger.warning("HTTP %d for EIA %s: %s", resp.status_code, name, resp.text[:200])
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                    continue
                return pd.DataFrame()

            payload = resp.json()
            data = payload.get("response", {}).get("data", [])

            if not data:
                logger.info("No data returned for EIA %s.", name)
                return pd.DataFrame()

            rows = []
            for item in data:
                period = item.get("period", "")
                value = item.get("value")
                unit = item.get("unit", item.get("units", ""))

                if value is None:
                    continue

                try:
                    value = float(value)
                except (ValueError, TypeError):
                    continue

                rows.append({
                    "Date": period,
                    "value": value,
                    "unit": str(unit),
                })

            if not rows:
                logger.info("No valid rows for EIA %s.", name)
                return pd.DataFrame()

            df = pd.DataFrame(rows)
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.sort_values("Date").reset_index(drop=True)
            logger.info("Got %d observations for EIA %s.", len(df), name)
            return df

        except (requests.RequestException, json.JSONDecodeError, ValueError) as exc:
            logger.warning(
                "Attempt %d/%d failed for EIA %s: %s",
                attempt, MAX_RETRIES, name, exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    logger.error("All %d attempts failed for EIA %s", MAX_RETRIES, name)
    return pd.DataFrame()


def fetch_all_eia() -> dict[str, pd.DataFrame]:
    """
    Fetch all configured EIA series.

    Returns dict keyed by series name (e.g. "Ethanol Production").
    """
    if not EIA_API_KEY:
        logger.info("EIA_API_KEY not set — skipping all EIA fetches.")
        return {}

    results = {}
    for name, spec in EIA_SERIES.items():
        results[name] = fetch_eia_series(
            name=name,
            route=spec["route"],
            series_id=spec["series"],
            frequency=spec["frequency"],
        )
    return results


# ── Quick self-test ─────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_all_eia()
    logger.info("=== EIA Summary ===")
    for name, df in data.items():
        if df.empty:
            logger.info("  %s: no data (API key missing?)", name)
        else:
            logger.info("  %s: %d rows, latest = %s", name, len(df), df.iloc[-1]["Date"])
