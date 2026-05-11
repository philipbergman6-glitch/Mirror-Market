"""Smoke tests for analysis.spreads.compute_crush_spread."""

from __future__ import annotations

import pandas as pd
import pandas.testing as pdt
import pytest

from analysis.spreads import compute_crush_spread


def _close_df(dates: list[str], closes: list[float]) -> pd.DataFrame:
    idx = pd.to_datetime(dates)
    idx.name = "Date"
    return pd.DataFrame({"Close": closes}, index=idx)


def test_compute_crush_spread_formula():
    # crush_spread = oil*11 + meal*2.2 - beans
    # 50*11 + 300*2.2 - 1000 = 550 + 660 - 1000 = 210
    dates = ["2026-01-01", "2026-01-02"]
    beans = _close_df(dates, [1000.0, 1100.0])
    oil = _close_df(dates, [50.0, 55.0])
    meal = _close_df(dates, [300.0, 320.0])

    out = compute_crush_spread(beans, oil, meal)

    assert list(out.columns) == [
        "Date", "soybeans_close", "oil_close", "meal_close",
        "crush_spread", "oil_value_share",
    ]
    assert out.iloc[0]["crush_spread"] == pytest.approx(50.0 * 11 + 300.0 * 2.2 - 1000.0)
    assert out.iloc[1]["crush_spread"] == pytest.approx(55.0 * 11 + 320.0 * 2.2 - 1100.0)


def test_compute_crush_spread_aligns_on_intersection():
    # Only 2026-01-02 is in all three series.
    beans = _close_df(["2026-01-01", "2026-01-02"], [1000.0, 1100.0])
    oil = _close_df(["2026-01-02", "2026-01-03"], [50.0, 55.0])
    meal = _close_df(["2026-01-02", "2026-01-04"], [300.0, 310.0])

    out = compute_crush_spread(beans, oil, meal)

    assert len(out) == 1
    assert out.iloc[0]["Date"] == pd.Timestamp("2026-01-02")


def test_compute_crush_spread_disjoint_dates_returns_empty():
    beans = _close_df(["2026-01-01"], [1000.0])
    oil = _close_df(["2026-02-01"], [50.0])
    meal = _close_df(["2026-03-01"], [300.0])

    out = compute_crush_spread(beans, oil, meal)

    assert out.empty


def test_compute_crush_spread_oil_value_share_in_range():
    dates = ["2026-01-01", "2026-01-02"]
    beans = _close_df(dates, [1000.0, 1100.0])
    oil = _close_df(dates, [50.0, 55.0])
    meal = _close_df(dates, [300.0, 320.0])

    out = compute_crush_spread(beans, oil, meal)

    assert "oil_value_share" in out.columns
    assert out["oil_value_share"].between(0, 1, inclusive="both").all()


def test_compute_crush_spread_does_not_mutate_inputs():
    dates = ["2026-01-01", "2026-01-02"]
    beans = _close_df(dates, [1000.0, 1100.0])
    oil = _close_df(dates, [50.0, 55.0])
    meal = _close_df(dates, [300.0, 320.0])

    before_beans = beans.copy(deep=True)
    before_oil = oil.copy(deep=True)
    before_meal = meal.copy(deep=True)

    compute_crush_spread(beans, oil, meal)

    pdt.assert_frame_equal(beans, before_beans)
    pdt.assert_frame_equal(oil, before_oil)
    pdt.assert_frame_equal(meal, before_meal)
