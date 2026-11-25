"""
Layer 5 — Weather data for key growing regions via Open-Meteo.

Weather is one of the biggest price drivers for agricultural commodities:
    - Drought in the US Midwest can cut soybean yields dramatically
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
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd
import requests

from config import (
    GROWING_REGIONS,
    MAX_RETRIES,
    OPENMETEO_BASIS,
    OPENMETEO_FORECAST_PATH,
    OPENMETEO_FORECAST_URL,
    OPENMETEO_PROXY_TOKEN_ENV,
    OPENMETEO_PROXY_URL_ENV,
    REQUEST_TIMEOUT,
    WEATHER_DAILY_VARS,
    WEATHER_SECOND_PASS_DELAY_S,
)
from fetchers._backoff import retry_sleep
from pipeline.results import UpstreamRateLimited

logger = logging.getLogger(__name__)

# The Worker contract (infra/openmeteo-worker/src/worker.js): the shared
# secret travels in this header, and the Worker marks its *own* refusals
# with the second one so they can never be read as Open-Meteo's.
PROXY_TOKEN_HEADER = "X-Proxy-Token"
PROXY_ERROR_HEADER = "X-Mirror-Proxy-Error"


class OpenMeteoProxyError(RuntimeError):
    """The Worker route is misconfigured — half-set, not https, or refused by the Worker.

    A configuration error, not weather: raised, never retried, and never
    answered by quietly going direct, which would put the layer back on the
    shared quota while the config claims otherwise (invariant 1). The
    message never carries the token.
    """


@dataclass(frozen=True)
class _Route:
    """Where the one batched request goes, and what its quota is."""

    url: str
    label: str
    quota: str
    via_worker: bool
    headers: dict[str, str] = field(default_factory=dict, repr=False)


def _route() -> _Route:
    """Read the proxy configuration now (not at import), both or neither (#637)."""
    base = (os.getenv(OPENMETEO_PROXY_URL_ENV) or "").strip()
    token = (os.getenv(OPENMETEO_PROXY_TOKEN_ENV) or "").strip()
    if not base and not token:
        return _Route(
            url=OPENMETEO_FORECAST_URL,
            label="direct to api.open-meteo.com (free quota shared with every tenant of this IP)",
            quota="the free quota is shared per outgoing IP, and GitHub runners share IPs",
            via_worker=False,
        )
    if not (base and token):
        missing = OPENMETEO_PROXY_TOKEN_ENV if base else OPENMETEO_PROXY_URL_ENV
        raise OpenMeteoProxyError(
            f"Open-Meteo Worker route needs both {OPENMETEO_PROXY_URL_ENV} and "
            f"{OPENMETEO_PROXY_TOKEN_ENV}; {missing} is unset. Set both, or unset "
            "both to go direct on the shared quota."
        )
    parts = urlsplit(base)
    if parts.scheme != "https" or not parts.hostname:
        raise OpenMeteoProxyError(
            f"{OPENMETEO_PROXY_URL_ENV} must be an https:// URL (got scheme "
            f"{parts.scheme or '(none)'!r}); the token is never sent in clear"
        )
    return _Route(
        url=base.rstrip("/") + OPENMETEO_FORECAST_PATH,
        label=f"via Worker {parts.hostname} (this Worker's own Open-Meteo quota)",
        quota=f"the quota is Worker {parts.hostname}'s own, not the runner IP's",
        via_worker=True,
        headers={PROXY_TOKEN_HEADER: token},
    )


def fetch_region_weather(
    region_name: str,
    lat: float,
    lon: float,
    past_days: int = 30,
    forecast_days: int = 7,
    *,
    now: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Fetch recent history + forecast for a single growing region.

    A batch of one (see `_fetch_batch`): the production path asks for every
    pin in one request, this is the single-pin view of the same parse.

    Returns
    -------
    pd.DataFrame
        Columns: Date, temp_max, temp_min, precipitation, is_forecast
        (1 for rows dated on or after the pin's local today — forecast or
        an unfinished day; 0 for complete past days), basis
        (`config.OPENMETEO_BASIS` on every row: past days are model
        analysis, never observations). Empty DataFrame on transport failure.

    Raises
    ------
    ValueError
        `now` is naive, or the response names no resolvable timezone — the
        day boundary cannot be placed without one.
    UpstreamRateLimited
        Open-Meteo refused the request for quota (HTTP 429) — never retried.
    """
    regions = {region_name: {"lat": lat, "lon": lon}}
    return _fetch_batch(
        regions, now, past_days=past_days, forecast_days=forecast_days,
    )[region_name]


def _fetch_batch(
    regions: Mapping[str, Mapping[str, float]],
    now: pd.Timestamp | None,
    *,
    past_days: int = 30,
    forecast_days: int = 7,
) -> dict[str, pd.DataFrame]:
    """One Open-Meteo request for every pin in `regions` (#637).

    Open-Meteo takes comma-separated coordinates and answers a list in
    request order, each entry carrying its own `timezone` (`timezone=auto`
    resolves per location) and, from the second entry on, a `location_id`.
    The quota cost is unchanged — weight is summed per location, 1 call per
    pin at 3 variables × 37 days — but a run is 1 HTTP request instead of
    24, and a refused run 1 instead of 144.

    Transport failures (timeout, 5xx, a concurrency 429) are retried with
    backoff; after MAX_RETRIES every pin comes back empty for the caller's
    second pass. A quota 429 raises `UpstreamRateLimited` at once.
    """
    if now is None:
        now = pd.Timestamp.now(tz="UTC")
    if now.tzinfo is None:
        raise ValueError("fetch_region_weather: `now` must be timezone-aware")

    route = _route()
    names = list(regions)
    params: dict[str, str | float | int] = {
        "latitude":      ",".join(str(regions[n]["lat"]) for n in names),
        "longitude":     ",".join(str(regions[n]["lon"]) for n in names),
        "daily":         WEATHER_DAILY_VARS,
        "past_days":     past_days,
        "forecast_days": forecast_days,
        "timezone":      "auto",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(
                "Requesting weather for %d region(s) in one request (attempt %d), %s ...",
                len(names), attempt, route.label,
            )
            resp = requests.get(
                route.url,
                params=params,
                timeout=REQUEST_TIMEOUT,
                headers=route.headers,
            )

            proxy_error = resp.headers.get(PROXY_ERROR_HEADER) if route.via_worker else None
            if proxy_error:
                raise OpenMeteoProxyError(
                    f"the Open-Meteo Worker refused the request itself: HTTP "
                    f"{resp.status_code} {proxy_error} — check {OPENMETEO_PROXY_URL_ENV} "
                    f"and that {OPENMETEO_PROXY_TOKEN_ENV} matches the Worker's PROXY_TOKEN secret"
                )

            if resp.status_code == 429:
                upstream = _upstream_reason(resp)
                if _is_concurrency_rejection(upstream):
                    # Queue pressure on the shared IP, not a quota: uncharged,
                    # and it clears as earlier requests finish.
                    logger.warning("HTTP 429 (concurrency): %s", upstream)
                    if attempt < MAX_RETRIES:
                        retry_sleep(attempt)
                        continue
                    break
                raise UpstreamRateLimited("weather", _rate_limit_reason(resp, upstream, route))

            if resp.status_code != 200:
                logger.warning("HTTP %d: %s", resp.status_code, resp.text[:200])
                if attempt < MAX_RETRIES:
                    retry_sleep(attempt)
                    continue
                break

            _log_quota(resp)
            return _parse_batch(names, regions, resp.json(), now)

        except (requests.RequestException, json.JSONDecodeError) as exc:
            logger.warning(
                "Attempt %d/%d failed for weather (%d regions): %s",
                attempt, MAX_RETRIES, len(names), exc,
            )
            if attempt < MAX_RETRIES:
                retry_sleep(attempt)

    logger.error(
        "All %d attempts failed for weather (%s) — returning empty DataFrames",
        MAX_RETRIES, ", ".join(names),
    )
    return {name: pd.DataFrame() for name in names}


def _parse_batch(
    names: list[str],
    regions: Mapping[str, Mapping[str, float]],
    payload: object,
    now: pd.Timestamp,
) -> dict[str, pd.DataFrame]:
    """Map a (multi-)location response back onto the pins, by position.

    One location answers an object, several a list. Hard-fails on a count
    or `location_id` that does not match the request: a mis-mapped entry
    would put one region's rain on another's card (invariant 11).
    """
    entries = payload if isinstance(payload, list) else [payload]
    if len(entries) != len(names):
        raise ValueError(
            f"Open-Meteo answered {len(entries)} location(s) for {len(names)} "
            "requested; the pins cannot be matched to the response"
        )
    out: dict[str, pd.DataFrame] = {}
    for index, (name, entry) in enumerate(zip(names, entries, strict=True)):
        if not isinstance(entry, dict):
            raise ValueError(f"Open-Meteo entry {index} for {name} is not an object")
        location_id = entry.get("location_id")
        if location_id is not None and location_id != index:
            raise ValueError(
                f"Open-Meteo entry {index} for {name} carries location_id "
                f"{location_id!r}; the response is not in request order"
            )
        out[name] = _region_frame(name, regions[name], entry, now)
    return out


def _region_frame(
    region_name: str,
    coords: Mapping[str, float],
    data: dict,
    now: pd.Timestamp,
) -> pd.DataFrame:
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

    # `time` is the pin's local calendar (timezone=auto), so today is
    # the pin's today, not the runner's. Today itself is a running
    # total — unfinished max, partial precip sum — so it is flagged
    # with the forecast rows: the elapsed side ends at D−1, and the
    # 08:00 and 23:30 UTC slots no longer disagree about an
    # "observed" day. Downstream z-scores and alerts exclude both.
    pin_today = _pin_today(region_name, data.get("timezone"), now)
    df["is_forecast"] = (
        pd.to_datetime(df["Date"]) >= pin_today
    ).astype(int)
    df["basis"] = OPENMETEO_BASIS

    logger.info(
        "Got %d days of weather for %s (%.1f°, %.1f°)",
        len(df), region_name, coords["lat"], coords["lon"],
    )
    return df


def _upstream_reason(resp: requests.Response) -> str:
    """Open-Meteo's own `reason` text, or the raw body when it sent none."""
    try:
        body = resp.json()
    except (ValueError, json.JSONDecodeError):
        body = None
    if isinstance(body, dict) and isinstance(body.get("reason"), str):
        return body["reason"]
    return (resp.text or "").strip()[:200] or "(no reason given)"


def _is_concurrency_rejection(reason: str) -> bool:
    return "concurrent" in reason.lower()


def _rate_limit_reason(resp: requests.Response, upstream: str, route: _Route) -> str:
    """The `rate_limited` reason a reader sees: what, until when, and why it is shared."""
    parts = [f"Open-Meteo HTTP 429: {upstream}"]
    retry_after = resp.headers.get("Retry-After")
    if retry_after:
        parts.append(f"Retry-After {retry_after} s")
    window = resp.headers.get("X-RateLimit-Window")
    if window:
        parts.append(f"window {window} s")
    parts.append(route.quota)
    return " — ".join(parts)


def _log_quota(resp: requests.Response) -> None:
    """Record what the request cost and what the shared quota has left.

    Open-Meteo reports both on every free-API answer (`X-RateLimit-Cost`,
    `X-RateLimit-Remaining`, `X-RateLimit-Window`); logging them is how a
    run says how close the shared IP was to the wall.
    """
    headers = resp.headers
    cost = headers.get("X-RateLimit-Cost")
    remaining = headers.get("X-RateLimit-Remaining")
    if cost is None and remaining is None:
        return
    logger.info(
        "Open-Meteo quota: this request cost %s call(s); %s remaining in the %s s window",
        cost or "?", remaining or "?", headers.get("X-RateLimit-Window") or "?",
    )


def _pin_today(region_name: str, timezone: object, now: pd.Timestamp) -> pd.Timestamp:
    """The pin's local calendar date at `now`, as a naive midnight."""
    if not isinstance(timezone, str) or not timezone:
        raise ValueError(
            f"Open-Meteo response for {region_name} names no timezone "
            f"(got {timezone!r}); its daily dates cannot be placed"
        )
    try:
        tz = ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(
            f"Open-Meteo response for {region_name} names an unknown "
            f"timezone {timezone!r}"
        ) from exc
    return now.tz_convert(tz).normalize().tz_localize(None)


def fetch_all_regions(*, now: pd.Timestamp | None = None) -> dict[str, pd.DataFrame]:
    """
    Fetch weather for every growing region in config.GROWING_REGIONS.

    One batched request (#637), then — only for pins it left empty — one
    more batched request after `WEATHER_SECOND_PASS_DELAY_S` (#577).

    Returns
    -------
    dict[str, pd.DataFrame]
        {region_name: DataFrame} — past 30 days + 7-day forecast.

    Raises
    ------
    UpstreamRateLimited
        A quota 429 on either pass; it carries the rows the first pass got.
    """
    results = _fetch_batch(GROWING_REGIONS, now)

    # Second pass (#577): transient upstream throttling, not the region,
    # explains a key lost to all retries — so retry it once the window has
    # had time to clear. A key still empty after this stays empty. A quota
    # refusal never gets here: retrying it answers the same 429.
    missing = [name for name, df in results.items() if df.empty]
    if missing:
        logger.warning(
            "Weather: %d region(s) empty after first pass (%s) — retrying in %ds",
            len(missing), ", ".join(missing), WEATHER_SECOND_PASS_DELAY_S,
        )
        time.sleep(WEATHER_SECOND_PASS_DELAY_S)
        try:
            results.update(_fetch_batch({n: GROWING_REGIONS[n] for n in missing}, now))
        except UpstreamRateLimited as exc:
            raise UpstreamRateLimited(
                exc.layer, exc.reason,
                data={n: df for n, df in results.items() if not df.empty},
            ) from exc
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
