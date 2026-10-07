"""Tests for Layer 11's forward-curve fetcher (#61 / audit finding F8).

Two defects are pinned here:

1. The curve dropped the *current* delivery month while it was still
   trading, so for the first half of every delivery month the nearby leg —
   the one that carries the structure — was missing (observed 2026-08-12:
   Lean Hogs Aug 95.93 vs Oct 83.30, a 12.6-point front inversion the curve
   could not see).
2. Legs were taken from each contract's own last bar, so a thin deferred
   days behind the liquid months entered the same curve (observed
   2026-08-12: every Soybean Oil leg on 08-11 except the expiring Aug
   contract, stuck on 08-10). A curve stitched from two sessions reports
   yesterday's move as term structure.

The clock is injected — these must not depend on when they run.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from fetchers import forward_curve as fc

# Every curve test is built as if run on this session.
NOW = date(2026, 8, 12)


def _bars(last_date: str, close: float) -> pd.DataFrame:
    """A yfinance-shaped frame whose last bar is on `last_date`."""
    idx = pd.DatetimeIndex(pd.to_datetime([last_date]), name="Date")
    return pd.DataFrame({"Close": [close], "Volume": [100]}, index=idx)


# ── Front month: the current delivery month is a candidate ──────────

def test_current_delivery_month_is_included():
    contracts = _build_soybeans(today=date(2026, 8, 12))
    assert contracts[0]["label"] == "Aug 2026"
    assert contracts[0]["ticker"] == "ZSQ26.CBT"


def test_past_months_are_still_skipped():
    contracts = _build_soybeans(today=date(2026, 8, 12))
    months = [c["contract_month"] for c in contracts]
    assert all(m >= date(2026, 8, 1) for m in months)
    assert months == sorted(months)


def test_non_trading_current_month_starts_at_next_traded_month():
    # Soybeans do not trade a June contract; June 2026 must roll to July.
    contracts = _build_soybeans(today=date(2026, 6, 10))
    assert contracts[0]["label"] == "Jul 2026"


def _build_soybeans(today: date) -> list[dict]:
    return fc._build_contract_tickers(
        root="ZS", exchange="CBT", trading_months=[1, 3, 5, 7, 8, 9, 11],
        num_contracts=6, today=today,
    )


# ── Single observation date per curve ───────────────────────────────

def test_stale_leg_is_dropped_and_curve_carries_one_date(monkeypatch, caplog):
    prices = {
        "ZSQ26.CBT": _bars("2026-08-10", 1147.25),   # a session behind
        "ZSU26.CBT": _bars("2026-08-11", 1150.25),
        "ZSX26.CBT": _bars("2026-08-11", 1167.75),
    }
    _patch_fetch(monkeypatch, prices)

    with caplog.at_level("WARNING"):
        out = fc.fetch_forward_curve("Soybeans", today=NOW)

    assert out["observation_date"].nunique() == 1
    assert out["observation_date"].iloc[0] == "2026-08-11"
    assert "ZSQ26.CBT" not in out["ticker"].tolist()
    assert "ZSQ26.CBT" in caplog.text  # the drop is announced, never silent


def test_live_front_month_survives_and_leads_the_curve(monkeypatch):
    prices = {
        "HEQ26.CME": _bars("2026-08-11", 95.93),
        "HEV26.CME": _bars("2026-08-11", 83.30),
        "HEZ26.CME": _bars("2026-08-11", 74.07),
    }
    _patch_fetch(monkeypatch, prices)

    out = fc.fetch_forward_curve("Lean Hogs", today=NOW)

    assert out["ticker"].iloc[0] == "HEQ26.CME"
    assert out["close"].iloc[0] == pytest.approx(95.93)
    # The front inversion the old skip erased is now visible.
    assert out["close"].iloc[0] > out["close"].iloc[1]


def test_expired_front_month_drops_out_on_an_empty_frame(monkeypatch):
    # Yahoo delists an expired contract outright (verified live 2026-08-12
    # for ZSN26/ZCN26/LEM26/ZSK26/SBN26), so an empty frame is how expiry
    # reaches this fetcher.
    prices = {
        "ZSQ26.CBT": pd.DataFrame(),
        "ZSU26.CBT": _bars("2026-08-20", 1150.25),
        "ZSX26.CBT": _bars("2026-08-20", 1167.75),
    }
    _patch_fetch(monkeypatch, prices)

    out = fc.fetch_forward_curve("Soybeans", today=NOW)

    assert out["ticker"].tolist() == ["ZSU26.CBT", "ZSX26.CBT"]
    assert out["observation_date"].tolist() == ["2026-08-20", "2026-08-20"]


def test_all_legs_stale_still_yields_one_dated_curve(monkeypatch):
    # A holiday: nothing traded today, but every leg agrees on the last
    # session. That is a valid curve — just dated yesterday.
    prices = {
        "ZSQ26.CBT": _bars("2026-08-10", 1147.25),
        "ZSU26.CBT": _bars("2026-08-10", 1150.25),
    }
    _patch_fetch(monkeypatch, prices)

    out = fc.fetch_forward_curve("Soybeans", today=NOW)

    assert len(out) == 2
    assert set(out["observation_date"]) == {"2026-08-10"}


def test_undated_frame_hard_fails(monkeypatch):
    bad = pd.DataFrame({"Close": [1150.0]}, index=["not-a-date"])
    _patch_fetch(monkeypatch, {"ZSU26.CBT": bad})

    with pytest.raises(ValueError, match="non-datetime index"):
        fc.fetch_forward_curve("Soybeans", today=NOW)


def _patch_fetch(monkeypatch, prices: dict[str, pd.DataFrame]) -> None:
    """Serve `prices` by ticker; any other ticker is 'not listed'."""
    monkeypatch.setattr(
        fc, "fetch_one",
        lambda ticker, period="5d": prices.get(ticker, pd.DataFrame()).copy(),
    )


# ── #331: the fetch is split from the parse ─────────────────────────
#
# ``curve_contracts`` names the tickers and ``build_curve`` parses frames
# that were already fetched, so a caller that downloads once (the DT-16
# reconciler) can run v1's parse over the same bars the trusted adapter
# captured. ``fetch_forward_curve`` is the two recomposed over ``fetch_one``.

def test_curve_contracts_names_the_tickers_fetch_forward_curve_asks_for(monkeypatch):
    asked: list[str] = []

    def fetch(ticker, period="5d"):
        asked.append(ticker)
        return pd.DataFrame()

    monkeypatch.setattr(fc, "fetch_one", fetch)
    fc.fetch_forward_curve("Soybeans", today=NOW)

    assert [c["ticker"] for c in fc.curve_contracts("Soybeans", today=NOW)] == asked


def test_curve_contracts_hard_fails_on_an_unconfigured_commodity():
    with pytest.raises(KeyError, match="Tulips"):
        fc.curve_contracts("Tulips", today=NOW)


def test_build_curve_over_prefetched_frames_matches_fetch_forward_curve(monkeypatch):
    prices = {
        "ZSQ26.CBT": _bars("2026-08-12", 1147.25),
        "ZSU26.CBT": _bars("2026-08-12", 1150.25),
        "ZSX26.CBT": _bars("2026-08-11", 1160.00),  # stale leg, dropped on both routes
    }
    _patch_fetch(monkeypatch, prices)
    fetched = fc.fetch_forward_curve("Soybeans", today=NOW)

    contracts = fc.curve_contracts("Soybeans", today=NOW)
    frames = {c["ticker"]: prices.get(c["ticker"], pd.DataFrame()).copy() for c in contracts}
    built = fc.build_curve("Soybeans", contracts, frames)

    pd.testing.assert_frame_equal(built, fetched)
    assert list(built["ticker"]) == ["ZSQ26.CBT", "ZSU26.CBT"]


def test_build_curve_refuses_a_contract_it_was_handed_no_frame_for():
    """Absence is not an empty answer: a ticker missing from ``frames`` was
    never asked for, and treating it as delisted would shorten the curve
    silently."""
    contracts = fc.curve_contracts("Soybeans", today=NOW)
    frames = {contracts[0]["ticker"]: _bars("2026-08-12", 1147.25)}

    with pytest.raises(KeyError, match=contracts[1]["ticker"]):
        fc.build_curve("Soybeans", contracts, frames)


def test_build_curve_with_every_frame_empty_is_an_empty_curve():
    contracts = fc.curve_contracts("Soybeans", today=NOW)
    frames = {c["ticker"]: pd.DataFrame() for c in contracts}

    assert fc.build_curve("Soybeans", contracts, frames).empty


def test_the_v1_fetch_path_is_the_settlement_guarded_one():
    """``fetch_forward_curve`` must keep fetching through ``fetch_one``. The
    split exists so the *reconciler* can apply the guard itself over an
    unguarded download; v1 itself never sees an unfinished bar."""
    import fetchers.yfinance

    assert fc.fetch_one is fetchers.yfinance.fetch_one
