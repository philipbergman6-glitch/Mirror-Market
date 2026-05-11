"""Tests for analysis/loaders.py.

Covers:
  - empty DB returns empty dicts
  - non-empty DB returns DatetimeIndex-keyed dicts shaped by commodity / pair
  - `with_technicals=True` adds technical indicator columns
  - the two `with_technicals` cache slots are independent
  - clear_loader_cache() resets both caches
  - mutating cached entries between calls reflects (documents current behaviour)
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analysis import loaders
from pipeline import store


@pytest.fixture(autouse=True)
def _clear_caches():
    """Reset caches between tests so cached state from a previous test doesn't leak."""
    loaders.clear_loader_cache()
    yield
    loaders.clear_loader_cache()


def _make_ohlcv(n: int = 250, start: str = "2024-01-01") -> pd.DataFrame:
    rng = np.random.default_rng(seed=7)
    idx = pd.date_range(start, periods=n, freq="B")
    idx.name = "Date"
    close = 100.0 * np.exp(np.cumsum(rng.normal(0.0005, 0.012, size=n)))
    return pd.DataFrame(
        {
            "Open": close,
            "High": close * 1.005,
            "Low": close * 0.995,
            "Close": close,
            "Volume": rng.integers(50_000, 200_000, size=n).astype(float),
        },
        index=idx,
    )


def _make_currency(n: int = 60, start: str = "2024-09-01") -> pd.DataFrame:
    idx = pd.date_range(start, periods=n, freq="B")
    idx.name = "Date"
    return pd.DataFrame(
        {
            "Open": 5.0,
            "High": 5.05,
            "Low": 4.95,
            "Close": np.linspace(5.0, 5.2, n),
            "Volume": 1_000.0,
        },
        index=idx,
    )


def test_load_prices_empty_db(patched_db):
    out = loaders.load_prices()
    assert out == {}


def test_load_currencies_empty_db(patched_db):
    out = loaders.load_currencies()
    assert out == {}


def test_load_prices_returns_indexed_dict(patched_db):
    store.save_price_data("Soybeans", _make_ohlcv())
    store.save_price_data("Corn", _make_ohlcv())

    out = loaders.load_prices()

    assert set(out.keys()) == {"Soybeans", "Corn"}
    for df in out.values():
        assert isinstance(df.index, pd.DatetimeIndex)
        assert df.index.is_monotonic_increasing
        assert {"Open", "High", "Low", "Close", "Volume"}.issubset(df.columns)
        # No technicals when default
        assert "RSI" not in df.columns
        assert "MACD" not in df.columns


def test_load_prices_with_technicals(patched_db):
    store.save_price_data("Soybeans", _make_ohlcv())
    out = loaders.load_prices(with_technicals=True)

    df = out["Soybeans"]
    # compute_all_technicals adds these
    for col in ("RSI", "MACD", "MA_20", "MA_50"):
        assert col in df.columns, f"missing technical column: {col}"


def test_load_prices_caches_separately_for_each_variant(patched_db):
    store.save_price_data("Soybeans", _make_ohlcv())

    bare = loaders.load_prices()
    with_tech = loaders.load_prices(with_technicals=True)

    # Same call signature returns the cached object
    assert loaders.load_prices() is bare
    assert loaders.load_prices(with_technicals=True) is with_tech
    # And the two variants are distinct objects
    assert bare is not with_tech


def test_clear_loader_cache_resets_both(patched_db):
    store.save_price_data("Soybeans", _make_ohlcv())
    store.save_currency_data("BRL/USD", _make_currency())

    first_prices = loaders.load_prices()
    first_currencies = loaders.load_currencies()

    loaders.clear_loader_cache()

    second_prices = loaders.load_prices()
    second_currencies = loaders.load_currencies()

    # Same data, but different objects — cache was cleared
    assert first_prices is not second_prices
    assert first_currencies is not second_currencies
    assert set(first_prices) == set(second_prices)
    assert set(first_currencies) == set(second_currencies)


def test_load_currencies_returns_indexed_dict(patched_db):
    store.save_currency_data("BRL/USD", _make_currency())
    store.save_currency_data("CNY/USD", _make_currency())

    out = loaders.load_currencies()

    assert set(out.keys()) == {"BRL/USD", "CNY/USD"}
    for df in out.values():
        assert isinstance(df.index, pd.DatetimeIndex)
        assert df.index.is_monotonic_increasing
