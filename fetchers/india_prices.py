"""
Layer 16 — India domestic soybean prices, composed from two publishers.

``fetch_india_domestic`` returns ``{series: frame}`` for every series in
``config.INDIA_DOMESTIC_SERIES``:

* ``Soybean (NCDEX Indore spot)`` — ``fetchers.ncdex_spot`` (headline);
* ``Soybean (Mandi MH)`` — ``fetchers.msamb``.

The two are independent measurements and are never pooled. Grading follows
the #212 rule the Agmarknet build had: a guard failure or exhausted
transport on *either* series grades the layer ``failed``, while the series
that did arrive is still saved (``FetchResult.partial``) — one leg dark
must not stamp a fresh ``last_success``. A series whose newest completed
day is older than the layer's age budget fails the layer too, rows kept.

Agmarknet (``fetchers.mandi``) is dormant since its report API began
demanding a captcha (#449); ``Soybean (Mandi MP)`` history stays as stored.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from datetime import date, timedelta
from functools import partial

import pandas as pd
import requests

from config import (
    INDIA_DOMESTIC_SERIES,
    LAYER_MAX_DATA_AGE_DAYS,
    MANDI_MIN_AGE_DAYS,
    MANDI_SERIES_MH,
    NCDEX_SPOT_SYMBOLS,
)
from fetchers.mandi import _ist_today
from fetchers.msamb import fetch_msamb_soybean
from fetchers.ncdex_spot import fetch_ncdex_spot
from pipeline.results import FetchResult, ScraperShapeError

logger = logging.getLogger(__name__)


def _sources(today: date) -> dict[str, Callable[[], pd.DataFrame]]:
    sources: dict[str, Callable[[], pd.DataFrame]] = {
        series: partial(fetch_ncdex_spot, series, sym, centre, today)
        for series, (sym, centre) in NCDEX_SPOT_SYMBOLS.items()
    }
    sources[MANDI_SERIES_MH] = partial(fetch_msamb_soybean, today)
    missing = set(INDIA_DOMESTIC_SERIES) - set(sources)
    if missing:
        raise RuntimeError(f"Layer 16: no source wired for {sorted(missing)}")
    return {series: sources[series] for series in INDIA_DOMESTIC_SERIES}


def fetch_india_domestic(today: date | None = None) -> FetchResult:
    today = today or _ist_today()
    floor = today - timedelta(days=MANDI_MIN_AGE_DAYS + LAYER_MAX_DATA_AGE_DAYS["india_domestic"])
    data: dict[str, pd.DataFrame] = {}
    errors: list[str] = []
    for series, fetch in _sources(today).items():
        try:
            df = fetch()
        except (ScraperShapeError, requests.RequestException) as exc:
            logger.error("Layer 16: %s — %s", series, exc)
            errors.append(f"{series}: {exc}")
            continue
        data[series] = df
        newest = date.fromisoformat(df["Date"].iloc[-1])
        if newest < floor:
            errors.append(f"{series}: newest completed day {newest} is older than {floor}")
    if errors:
        reason = "; ".join(errors)
        return FetchResult.partial(data, reason) if data else FetchResult.failed(reason)
    return FetchResult.ok(data)


__all__: Sequence[str] = ("fetch_india_domestic",)


if __name__ == "__main__":
    from config import setup_logging

    setup_logging()
    result = fetch_india_domestic()
    logger.info("Layer 16: %s %s", result.status, result.error or "")
    for name, frame in result.data.items():
        logger.info("%s:\n%s", name, frame.tail(5).to_string(index=False))
