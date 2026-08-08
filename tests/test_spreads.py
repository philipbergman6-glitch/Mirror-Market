"""Smoke tests for analysis.spreads.compute_crush_spread."""

from __future__ import annotations

import pandas as pd
import pandas.testing as pdt
import pytest

from analysis.spreads import compute_crush_spread, compute_dce_crush_margin


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


# ---------------------------------------------------------------------------
# DCE board crush (China) — analysis.spreads.compute_dce_crush_margin
# ---------------------------------------------------------------------------


def _dce_long_df(rows: list[tuple[str, str, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "commodity": [r[0] for r in rows],
            "Date": pd.to_datetime([r[1] for r in rows]),
            "Close": [r[2] for r in rows],
        }
    )


def test_compute_dce_crush_margin_formula():
    # crush = oil*(11/60) + meal*(44/60) - beans, all CNY/MT
    # 8000*(11/60) + 3000*(44/60) - 4000 = 1466.67 + 2200 - 4000 = -333.33
    df = _dce_long_df([
        ("DCE Soybean", "2026-08-06", 4000.0),
        ("DCE Soybean Oil", "2026-08-06", 8000.0),
        ("DCE Soybean Meal", "2026-08-06", 3000.0),
    ])

    out = compute_dce_crush_margin(df)

    assert list(out.columns) == [
        "Date", "bean_close_cny", "oil_close_cny", "meal_close_cny",
        "crush_cny_mt", "oil_value_share",
    ]
    assert len(out) == 1
    expected = 8000.0 * (11 / 60) + 3000.0 * (44 / 60) - 4000.0
    assert out.iloc[0]["crush_cny_mt"] == pytest.approx(expected)
    assert 0 <= out.iloc[0]["oil_value_share"] <= 1


def test_compute_dce_crush_margin_aligns_on_intersection():
    df = _dce_long_df([
        ("DCE Soybean", "2026-08-05", 4000.0),
        ("DCE Soybean", "2026-08-06", 4100.0),
        ("DCE Soybean Oil", "2026-08-06", 8000.0),
        ("DCE Soybean Meal", "2026-08-06", 3000.0),
        ("DCE Soybean Meal", "2026-08-07", 3050.0),
    ])

    out = compute_dce_crush_margin(df)

    assert len(out) == 1
    assert out.iloc[0]["Date"] == pd.Timestamp("2026-08-06")


def test_compute_dce_crush_margin_missing_leg_returns_empty():
    df = _dce_long_df([
        ("DCE Soybean", "2026-08-06", 4000.0),
        ("DCE Soybean Oil", "2026-08-06", 8000.0),
    ])
    assert compute_dce_crush_margin(df).empty


def test_compute_dce_crush_margin_empty_input_returns_empty():
    out = compute_dce_crush_margin(pd.DataFrame())
    assert out.empty
    assert "crush_cny_mt" in out.columns


def test_compute_dce_crush_margin_missing_columns_raises():
    with pytest.raises(KeyError):
        compute_dce_crush_margin(pd.DataFrame({"commodity": ["DCE Soybean"]}))
