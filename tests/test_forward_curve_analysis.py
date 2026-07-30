"""Tests for analysis.forward_curve.analyze_curve structure classification.

Regression: a net-inverted curve (back < front) must never be labeled
contango, regardless of how many small sequential up-moves it contains.
"""

from __future__ import annotations

import pandas as pd

from analysis.forward_curve import analyze_curve


def _curve(closes: list[float]) -> pd.DataFrame:
    return pd.DataFrame({
        "contract_month": [f"2026-{m:02d}-01" for m in range(1, len(closes) + 1)],
        "close": closes,
        "label": [f"M{m}" for m in range(1, len(closes) + 1)],
    })


def test_net_inverted_curve_is_backwardation_even_with_up_moves():
    # Mostly small up-moves but one large drop → net -13% front-to-back.
    # The old move-counting logic called this "mild contango".
    out = analyze_curve(_curve([102.97, 103.5, 104.0, 104.5, 89.38]))
    assert out["spread"] < 0
    assert "backwardation" in out["structure"]
    assert "contango" not in out["structure"]


def test_monotone_rising_curve_is_contango():
    out = analyze_curve(_curve([100.0, 101.0, 102.0, 103.0]))
    assert out["structure"] == "contango"
    assert out["spread"] > 0


def test_rising_with_dip_is_mild_contango():
    out = analyze_curve(_curve([100.0, 102.0, 101.0, 104.0]))
    assert out["structure"] == "mild contango"


def test_monotone_falling_curve_is_backwardation():
    out = analyze_curve(_curve([104.0, 103.0, 102.0, 100.0]))
    assert out["structure"] == "backwardation"


def test_flat_curve_is_flat():
    out = analyze_curve(_curve([100.0, 100.0, 100.0]))
    assert out["structure"] == "flat"


def test_structure_never_contradicts_spread_sign():
    cases = [
        [100, 99, 101, 98],
        [50, 55, 52, 60],
        [300, 310, 290, 280],
        [10, 10.5, 9.5, 10.2],
    ]
    for closes in cases:
        out = analyze_curve(_curve([float(c) for c in closes]))
        if out["spread"] > 0:
            assert "backwardation" not in out["structure"]
        elif out["spread"] < 0:
            assert "contango" not in out["structure"]
