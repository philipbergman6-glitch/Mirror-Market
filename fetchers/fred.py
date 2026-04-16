"""
Layer 3 — Economic context data from the FRED API (Federal Reserve).

FRED provides macro indicators (dollar strength, inflation, interest
rates) that explain *why* commodity prices move.  A strong dollar, for
example, makes US exports more expensive and can push soybean prices down.

Sign up for a key at: https://fred.stlouisfed.org/docs/api/api_key.html

Set it as an environment variable:
    export FRED_API_KEY="your-key-here"

Key concepts for learning:
    - Same REST / JSON pattern as the USDA fetcher.
    - We convert the JSON into a pandas Series indexed by date.
    - try/except + retry makes the fetcher resilient to network hiccups.
"""

import json
import logging
import time

import requests
import pandas as pd

from config import (
    FRED_API_KEY, FRED_BASE_URL, FRED_SERIES,
    REQUEST_TIMEOUT, MAX_RETRIES, RETRY_DELAY,
)

logger = logging.getLogger(__name__)


def fetch_series(
    series_id: str,
    start_date: str = "2020-01-01",
) -> pd.Series:
    """
    Fetch a single FRED time series.

    Parameters
    ----------
    series_id : str
        FRED series ID, e.g. "DTWEXBGS"
    start_date : str
        ISO date string for the beginning of the range.

    Returns
    -------
    pd.Series   (index = date, values = float)
                 Empty Series if key is missing or request fails.
    """
    if not FRED_API_KEY:
        logger.warning("FRED_API_KEY not set — skipping FRED fetch.")
        logger.info("  Get a free key: https://fred.stlouisfed.org/docs/api/api_key.html")
        return pd.Series(dtype=float)

    params = {
        "series_id":        series_id,
        "api_key":          FRED_API_KEY,
        "file_type":        "json",
        "observation_start": start_date,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Requesting FRED series %s (attempt %d) ...", series_id, attempt)
            resp = requests.get(FRED_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)

            if resp.status_code != 200:
                logger.warning("HTTP %d: %s", resp.status_code, resp.text[:200])
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                    continue
                return pd.Series(dtype=float)

            observations = resp.json().get("observations", [])
            if not observations:
                logger.info("No observations for %s.", series_id)
                return pd.Series(dtype=float)

            # Build a clean Series: date index, numeric values
            dates = []
            values = []
            for obs in observations:
                if obs["value"] == ".":          # FRED uses "." for missing
                    continue
                dates.append(pd.Timestamp(obs["date"]))
                values.append(float(obs["value"]))

            series = pd.Series(values, index=dates, name=series_id)
            logger.info("Got %d observations for %s.", len(series), series_id)
            return series

        except (requests.RequestException, json.JSONDecodeError, ValueError) as exc:
            logger.warning(
                "Attempt %d/%d failed for FRED %s: %s",
                attempt, MAX_RETRIES, series_id, exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    logger.error("All %d attempts failed for FRED %s — returning empty Series",
                 MAX_RETRIES, series_id)
    return pd.Series(dtype=float)


def fetch_all_series() -> dict[str, pd.Series]:
    """Fetch every series listed in config.FRED_SERIES."""
    results = {}
    for name, series_id in FRED_SERIES.items():
        logger.info("Fetching %s (%s) ...", name, series_id)
        results[name] = fetch_series(series_id)
    return results


# ── Quick self-test ─────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_all_series()
    logger.info("=== FRED Summary ===")
    for name, s in data.items():
        if s.empty:
            logger.info("  %s: no data (API key missing?)", name)
        else:
            logger.info("  %s: %d obs, latest = %.2f (%s)",
                        name, len(s), s.iloc[-1], s.index[-1].date())
