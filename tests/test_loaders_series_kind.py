"""The technicals input seam (A4 #301): named adjusted series first,
labelled provider fallback second, and the label drives suppression.

`analysis.loaders.enrich_with_technicals` is the one place the technical
stack's input series is chosen; everything downstream (soy analytics, the
briefing, signal suppression) keys off the `series_kind` attr it stamps.
"""

from __future__ import annotations

import pandas as pd
import pytest

from analysis import loaders
from analysis.loaders import (
    NAMED_RATIO,
    PROVIDER_FRONT_MONTH,
    SERIES_KIND_ATTR,
    adjusted_commodities,
    enrich_with_technicals,
)

# Sessions around the Sep 2026 soybean roll — same fixture logic as
# tests/test_continuous_from_bars.py.
PRE_ROLL = ["2026-08-24", "2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28",
            "2026-08-31", "2026-09-01", "2026-09-02", "2026-09-03"]
POST_ROLL = ["2026-09-04", "2026-09-07", "2026-09-08", "2026-09-09",
             "2026-09-10", "2026-09-11"]
U26, X26 = "ZSU26.CBT", "ZSX26.CBT"


def _contract_bars() -> pd.DataFrame:
    rows = []
    for i, d in enumerate(PRE_ROLL + POST_ROLL):
        rows.append({"commodity": "Soybeans", "ticker": U26, "Date": d,
                     "Close": 1000.0 + i, "Volume": 50.0})
        rows.append({"commodity": "Soybeans", "ticker": X26, "Date": d,
                     "Close": 1010.0 + i, "Volume": 60.0})
    return pd.DataFrame(rows)


def _provider_frame() -> pd.DataFrame:
    idx = pd.DatetimeIndex(pd.to_datetime(PRE_ROLL + POST_ROLL), name="Date")
    n = len(idx)
    return pd.DataFrame(
        {"Open": [1000.0] * n, "High": [1005.0] * n, "Low": [995.0] * n,
         "Close": [1002.0] * n, "Volume": [10.0] * n},
        index=idx,
    )


def test_named_series_is_preferred_and_labelled(monkeypatch):
    monkeypatch.setattr(loaders, "read_contract_bars", lambda c=None: _contract_bars())
    out = enrich_with_technicals("Soybeans", _provider_frame())
    assert out.attrs[SERIES_KIND_ATTR] == NAMED_RATIO
    assert "NOT tradeable" in out.attrs["adjustment_note"]
    assert out.attrs["roll_dates"] == ["2026-09-04"]
    # Every bar names its contract; the newest is the real front's close.
    assert out["contract"].iloc[-1] == X26
    assert out["Close"].iloc[-1] == 1010.0 + len(PRE_ROLL + POST_ROLL) - 1
    # The assigned contract's own session volume rode along.
    assert out["Volume"].iloc[-1] == 60.0
    assert "RSI" in out.columns and "MA_20" in out.columns


def test_provider_fallback_is_labelled_contaminated(monkeypatch):
    monkeypatch.setattr(loaders, "read_contract_bars", lambda c=None: pd.DataFrame())
    out = enrich_with_technicals("Soybeans", _provider_frame())
    assert out.attrs[SERIES_KIND_ATTR] == PROVIDER_FRONT_MONTH
    assert "does not publish its roll dates" in out.attrs["adjustment_note"]
    assert "contract" not in out.columns
    assert "RSI" in out.columns


def test_uncurved_commodity_falls_back(monkeypatch):
    # Palm has no contract spec — the named path cannot exist for it.
    monkeypatch.setattr(loaders, "read_contract_bars", lambda c=None: pd.DataFrame())
    out = enrich_with_technicals("Palm Oil (CME)", _provider_frame())
    assert out.attrs[SERIES_KIND_ATTR] == PROVIDER_FRONT_MONTH


def test_short_history_falls_back_not_pads(monkeypatch):
    short = _contract_bars().head(6)  # 3 sessions per contract — below floor
    monkeypatch.setattr(loaders, "read_contract_bars", lambda c=None: short)
    out = enrich_with_technicals("Soybeans", _provider_frame())
    assert out.attrs[SERIES_KIND_ATTR] == PROVIDER_FRONT_MONTH


def test_broken_stitch_never_takes_down_prices(monkeypatch):
    def boom(c=None):
        raise RuntimeError("stitch exploded")
    monkeypatch.setattr(loaders, "read_contract_bars", boom)
    out = enrich_with_technicals("Soybeans", _provider_frame())
    assert out.attrs[SERIES_KIND_ATTR] == PROVIDER_FRONT_MONTH


def test_adjusted_commodities_reads_the_label(monkeypatch):
    monkeypatch.setattr(loaders, "read_contract_bars", lambda c=None: _contract_bars())
    named = enrich_with_technicals("Soybeans", _provider_frame())
    monkeypatch.setattr(loaders, "read_contract_bars", lambda c=None: pd.DataFrame())
    fallback = enrich_with_technicals("Corn", _provider_frame())
    assert adjusted_commodities({"Soybeans": named, "Corn": fallback}) == frozenset({"Soybeans"})
