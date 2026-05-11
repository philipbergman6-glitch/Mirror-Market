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

import pandas as pd

from analysis.technical import compute_all_technicals
from pipeline.query import read_currencies, read_prices


@lru_cache(maxsize=2)
def load_prices(*, with_technicals: bool = False) -> dict[str, pd.DataFrame]:
    """Return a dict[commodity] -> DataFrame indexed by Date.

    Args:
        with_technicals: When True, applies `compute_all_technicals()` so
            consumers get SMA/RSI/MACD columns alongside OHLCV. The two
            cache slots hold the with/without-technicals variants.
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
            subset = compute_all_technicals(subset)
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
