"""
Layer 7 — ECB euro reference rates, as USD crosses (B25 #495).

For a pair in ``config.FX_ECB_CROSS_PAIRS`` the publisher of record is the
ECB, not Yahoo: Yahoo's ``CNYUSD=X`` print is a day behind its own label by
an inconstant lag (see the config comment). The ECB publishes each currency
per euro on every TARGET day (concertation ~14:10 CET, published ~16:00 CET),
keyless, via SDMX REST; the USD
cross is ``(USD per EUR) / (unit per EUR)`` on a day *both* legs printed.

The frame is shaped like ``fetchers.yfinance.download_bars`` output — one
print per day, so Open = High = Low = Close, Volume 0 — and is unguarded:
``fetchers.yfinance.fetch_currencies`` applies the same ``FX_SESSION`` rule
it applies to every Yahoo pair.

Reuse: ECB statistics may be reused free of charge with the source cited
("Source: ECB").
"""

from __future__ import annotations

import io
import logging
import re
from datetime import date, datetime, timezone

import pandas as pd
import requests

from config import DEFAULT_HISTORY_PERIOD, ECB_EXR_URL, MAX_RETRIES, REQUEST_TIMEOUT
from fetchers._backoff import retry_sleep

logger = logging.getLogger(__name__)

_PERIOD = re.compile(r"^(\d+)(d|mo|y)$")
_REQUIRED_COLUMNS = ("CURRENCY", "TIME_PERIOD", "OBS_VALUE")


class EcbShapeError(ValueError):
    """The ECB answered, but not with the series this cross is built from."""


def period_start(period: str, today: date) -> date | None:
    """The first calendar date a yfinance-style ``period`` covers; ``None`` for "max"."""
    if period == "max":
        return None
    match = _PERIOD.match(period)
    if match is None:
        raise ValueError(f"ECB cross: unsupported period {period!r} (expected Nd, Nmo, Ny or max)")
    count, unit = int(match.group(1)), match.group(2)
    offset = {
        "d": pd.DateOffset(days=count),
        "mo": pd.DateOffset(months=count),
        "y": pd.DateOffset(years=count),
    }[unit]
    return (pd.Timestamp(today) - offset).date()


def download_ecb_cross(
    currency: str,
    period: str = DEFAULT_HISTORY_PERIOD,
    max_retries: int = MAX_RETRIES,
    today: date | None = None,
) -> pd.DataFrame:
    """USD per ``currency`` from the ECB reference rates, one row per TARGET day.

    Returns an empty frame after ``max_retries`` transport failures (the
    caller logs the miss, as for a Yahoo outage). Raises ``EcbShapeError``
    when the answer is not the two series asked for, or carries a
    non-positive rate — a cross built from it would be a wrong number.
    """
    today = today or datetime.now(timezone.utc).date()
    start = period_start(period, today)
    params = {"format": "csvdata", "detail": "dataonly"}
    if start is not None:
        params["startPeriod"] = start.isoformat()
    url = ECB_EXR_URL.format(series=f"USD+{currency}")

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return _cross(response.text, currency)
        except requests.RequestException as exc:
            logger.warning("ECB %s/USD attempt %d/%d failed: %s", currency, attempt, max_retries, exc)
            if attempt < max_retries:
                retry_sleep(attempt)

    logger.error("ECB %s/USD: all %d attempts failed — returning empty DataFrame", currency, max_retries)
    return pd.DataFrame()


def _cross(text: str, currency: str) -> pd.DataFrame:
    try:
        raw = pd.read_csv(io.StringIO(text))
    except pd.errors.EmptyDataError as exc:
        # An HTTP-200 empty body: an answer, but not the series asked for.
        raise EcbShapeError(f"ECB {currency}/USD: empty response body") from exc
    missing = [column for column in _REQUIRED_COLUMNS if column not in raw.columns]
    if missing:
        raise EcbShapeError(f"ECB {currency}/USD: response lacks column(s) {missing}")
    duplicated = raw.duplicated(subset=["TIME_PERIOD", "CURRENCY"], keep=False)
    if duplicated.any():
        # Two values for one leg on one day: picking either is a guess.
        pairs = sorted({(row.TIME_PERIOD, row.CURRENCY) for row in raw[duplicated].itertuples()})
        raise EcbShapeError(f"ECB {currency}/USD: duplicated (date, currency) rows {pairs}")
    legs = raw.pivot(index="TIME_PERIOD", columns="CURRENCY", values="OBS_VALUE")
    absent = [leg for leg in ("USD", currency) if leg not in legs.columns]
    if absent:
        raise EcbShapeError(f"ECB {currency}/USD: response carries no {absent} series")
    # Both legs from the same TARGET day, or no cross for that day (invariant 8).
    legs = legs[["USD", currency]].dropna()
    if (legs <= 0).any().any():
        bad = sorted(legs.index[(legs <= 0).any(axis=1)])
        raise EcbShapeError(f"ECB {currency}/USD: non-positive reference rate on {bad} — rates must be positive")
    close = legs["USD"] / legs[currency]
    index = pd.DatetimeIndex(pd.to_datetime(close.index), name="Date")
    return pd.DataFrame(
        {"Open": close.values, "High": close.values, "Low": close.values, "Close": close.values, "Volume": 0},
        index=index,
    ).sort_index()
