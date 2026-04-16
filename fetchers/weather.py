"""
Layer 5 — Weather data for key growing regions via Open-Meteo.

Weather is one of the biggest price drivers for agricultural commodities:
    - Drought in the US Midwest can cut soybean yields dramatically
    - Frost in Brazil destroys coffee crops (July 2021 → prices spiked 50%)
    - Excess rain during harvest delays field work and reduces quality

Open-Meteo is free, requires no API key, and covers the whole globe.

Key concepts for learning:
    - REST APIs with query parameters (same pattern as FRED/USDA fetchers)
    - Working with geographic coordinates (latitude/longitude)
    - Building a DataFrame from a JSON response
    - The same try/except + retry pattern used in every other fetcher
"""

import json
import logging
import time

import requests
import pandas as pd

from config import (
    OPENMETEO_FORECAST_URL, GROWING_REGIONS, WEATHER_DAILY_VARS,
    REQUEST_TIMEOUT, MAX_RETRIES, RETRY_DELAY,
)

logger = logging.getLogger(__name__)


def fetch_region_weather(
    region_name: str,
    lat: float,
    lon: float,
    past_days: int = 30,
    forecast_days: int = 7,
) -> pd.DataFrame:
    """
    Fetch recent history + forecast for a single growing region.

    Open-Meteo's forecast endpoint can return both past data and
    future forecasts in a single request — very convenient.

    Parameters
    ----------
    region_name : str
        Human-readable name (for logging).
    lat, lon : float
        WGS84 coordinates of the region center.
    past_days : int
        Days of recent history to include (max 92).
    forecast_days : int
        Days of forecast to include (max 16).

    Returns
    -------
    pd.DataFrame
        Columns: Date, temp_max, temp_min, precipitation
        Empty DataFrame on failure.
    """
    params = {
        "latitude":      lat,
        "longitude":     lon,
        "daily":         WEATHER_DAILY_VARS,
        "past_days":     past_days,
        "forecast_days": forecast_days,
        "timezone":      "auto",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(
                "Requesting weather for %s (attempt %d) ...",
                region_name, attempt,
            )
            resp = requests.get(
                OPENMETEO_FORECAST_URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if resp.status_code != 200:
                logger.warning("HTTP %d: %s", resp.status_code, resp.text[:200])
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                    continue
                return pd.DataFrame()

            data = resp.json()
            daily = data.get("daily", {})

            if not daily or "time" not in daily:
                logger.warning("No daily data in response for %s", region_name)
                return pd.DataFrame()

            df = pd.DataFrame({
                "Date":          daily["time"],
                "temp_max":      daily.get("temperature_2m_max"),
                "temp_min":      daily.get("temperature_2m_min"),
                "precipitation": daily.get("precipitation_sum"),
            })

            logger.info(
                "Got %d days of weather for %s (%.1f°, %.1f°)",
                len(df), region_name, lat, lon,
            )
            return df

        except (requests.RequestException, json.JSONDecodeError) as exc:
            logger.warning(
                "Attempt %d/%d failed for weather %s: %s",
                attempt, MAX_RETRIES, region_name, exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    logger.error(
        "All %d attempts failed for weather %s — returning empty DataFrame",
        MAX_RETRIES, region_name,
    )
    return pd.DataFrame()


def fetch_all_regions() -> dict[str, pd.DataFrame]:
    """
    Fetch weather for every growing region in config.GROWING_REGIONS.

    Returns
    -------
    dict[str, pd.DataFrame]
        {region_name: DataFrame} — past 30 days + 7-day forecast.
    """
    results = {}
    for region_name, coords in GROWING_REGIONS.items():
        df = fetch_region_weather(
            region_name,
            lat=coords["lat"],
            lon=coords["lon"],
        )
        results[region_name] = df
    return results


# ── Quick self-test ─────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_all_regions()
    logger.info("=== Weather Summary ===")
    for region, df in data.items():
        if df.empty:
            logger.info("  %s: no data", region)
        else:
            latest = df.iloc[-1]
            logger.info(
                "  %s: %d days, latest: %.1f°C / %.1f°C, precip: %.1f mm",
                region, len(df),
                latest["temp_max"], latest["temp_min"],
                latest["precipitation"],
            )
