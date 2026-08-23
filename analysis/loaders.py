"""
Shared data loaders for the analysis layer.

Both the daily briefing (`analysis/briefing/`) and the soy analytics desk
(`analysis/soy_analytics.py`) need the same DatetimeIndex-keyed dict of
price and currency frames. This module holds the canonical loader so the
two consumers don't drift.

The loaders are `@lru_cache`d because a single dashboard run calls them
5+ times. `clear_loader_cache()` resets the caches between pipeline runs
or in tests.
"""

from functools import lru_cache
from typing import Any

import pandas as pd

from analysis.technical import compute_all_technicals
from pipeline.query import read_contract_bars, read_currencies, read_prices

#: df.attrs key naming which series the indicators were computed on.
SERIES_KIND_ATTR = "series_kind"
#: Indicators computed on the ratio-adjusted named-contract series (A4).
NAMED_RATIO = "named_ratio"
#: Indicators computed on the provider's own front-month series — the
#: contaminated fallback; close-derived signals near an estimated roll are
#: suppressed downstream (analysis.signals.suppress_near_roll_signals).
PROVIDER_FRONT_MONTH = "provider_front_month"


def enrich_with_technicals(commodity: str, raw: pd.DataFrame) -> pd.DataFrame:
    """Indicator columns on the honest series for this commodity (A4 #301).

    Preferred input: the ratio-adjusted continuous series stitched from
    Layer 11b's named-contract bars — returns and volatility are continuous
    across rolls, every bar names its contract, and the newest segment's
    levels are the front contract's own real closes. Where that series does
    not exist (no curve config, or history below the floor), the provider
    front-month frame is enriched as before and *labelled* as the
    contaminated series it is, so downstream suppression can key off it.

    The answer's provenance rides on ``df.attrs``:
        series_kind      — NAMED_RATIO or PROVIDER_FRONT_MONTH
        adjustment_note  — the series' own method sentence
        roll_dates       — ISO dates the named series changed contract
    and, on the named series only, a ``contract`` column names the bar.
    """
    series = None
    try:
        from analysis.futures.continuous import build_from_bars

        bars = read_contract_bars(commodity)
        if not bars.empty:
            series = build_from_bars(bars, commodity, adjustment="ratio")
        # The switch threshold is higher than the series' own existence
        # floor: at a handful of sessions every indicator is NaN, and the
        # labelled, roll-suppressed provider series teaches more than a
        # screen of blanks. The named series takes over on its own once
        # config.TECHNICALS_MIN_SESSIONS have accrued (A4 #301).
        import config

        if series is not None and len(series.points) < config.TECHNICALS_MIN_SESSIONS:
            series = None
    except Exception:  # noqa: BLE001 — a broken stitch must not take down prices
        import logging

        logging.getLogger(__name__).warning(
            "named continuous series failed for %s — falling back to provider series",
            commodity, exc_info=True,
        )
        series = None

    if series is None:
        enriched = compute_all_technicals(raw.copy())
        enriched.attrs[SERIES_KIND_ATTR] = PROVIDER_FRONT_MONTH
        enriched.attrs["adjustment_note"] = (
            "unadjusted provider front-month series; the provider does not "
            "publish its roll dates, so roll-day gaps are indistinguishable "
            "from moves"
        )
        enriched.attrs["roll_dates"] = []
        return enriched

    index = pd.DatetimeIndex([pd.Timestamp(day) for day, _ in series.points], name="Date")
    frame = pd.DataFrame(
        {
            "Close": [price for _, price in series.points],
            "contract": [symbol for _, symbol in series.contract_by_date],
        },
        index=index,
    )
    # The assigned contract's own session volume, where Layer 11b holds it.
    # Volumes are counts, not prices — never adjusted.
    volume = _volume_by_session(commodity, dict(series.contract_by_date))
    frame["Volume"] = [volume.get(day.date()) for day in index]
    enriched = compute_all_technicals(frame)
    enriched.attrs[SERIES_KIND_ATTR] = NAMED_RATIO
    enriched.attrs["adjustment_note"] = series.adjustment_note
    enriched.attrs["roll_dates"] = [d.isoformat() for d in series.roll_dates]
    return enriched


def _volume_by_session(commodity: str, contract_by_date: dict) -> dict:
    bars = read_contract_bars(commodity)
    if bars.empty or "Volume" not in bars.columns:
        return {}
    out: dict = {}
    for row in bars.itertuples(index=False):
        date_any: Any = row.Date
        day = pd.Timestamp(date_any).date()
        if contract_by_date.get(day) == row.ticker and pd.notna(row.Volume):
            volume_any: Any = row.Volume
            out[day] = float(volume_any)
    return out


def adjusted_commodities(frames: dict[str, pd.DataFrame]) -> frozenset[str]:
    """Which of these enriched frames ride the named adjusted series.

    Signals detected on those frames need no roll suppression — their roll
    dates are our own rule and the adjustment already removed the gap.
    """
    return frozenset(
        name for name, df in frames.items()
        if df.attrs.get(SERIES_KIND_ATTR) == NAMED_RATIO
    )


@lru_cache(maxsize=2)
def load_prices(*, with_technicals: bool = False) -> dict[str, pd.DataFrame]:
    """Return a dict[commodity] -> DataFrame indexed by Date.

    Args:
        with_technicals: When True, applies `enrich_with_technicals()` so
            consumers get SMA/RSI/MACD columns — computed on the named
            ratio-adjusted series where Layer 11b's history allows, on the
            provider front-month frame (labelled as such) otherwise. The
            two cache slots hold the with/without-technicals variants.
    """
    all_prices = read_prices()
    result: dict[str, pd.DataFrame] = {}
    if all_prices.empty:
        return result

    for commodity in all_prices["commodity"].unique():
        subset = all_prices[all_prices["commodity"] == commodity].copy()
        subset["Date"] = pd.to_datetime(subset["Date"])
        subset = subset.set_index("Date").sort_index()
        if with_technicals:
            subset = enrich_with_technicals(commodity, subset)
        result[commodity] = subset
    return result


@lru_cache(maxsize=1)
def load_currencies() -> dict[str, pd.DataFrame]:
    """Return a dict[pair] -> DataFrame indexed by Date."""
    all_currencies = read_currencies()
    result: dict[str, pd.DataFrame] = {}
    if all_currencies.empty:
        return result

    for pair in all_currencies["pair"].unique():
        subset = all_currencies[all_currencies["pair"] == pair].copy()
        subset["Date"] = pd.to_datetime(subset["Date"])
        subset = subset.set_index("Date").sort_index()
        result[pair] = subset
    return result


def clear_loader_cache() -> None:
    """Reset both loader caches. Call between pipeline runs or in tests."""
    load_prices.cache_clear()
    load_currencies.cache_clear()
