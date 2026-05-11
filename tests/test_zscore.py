"""Tests for analysis.zscore — the helper used by COT and weather sections."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from analysis.zscore import format_zscore, zscore


def test_zscore_simple_distribution() -> None:
    baseline = pd.Series(range(100))  # mean=49.5, std≈29.0
    z = zscore(108.5, baseline)
    assert z is not None
    assert math.isclose(z, (108.5 - baseline.mean()) / baseline.std(), rel_tol=1e-9)


def test_zscore_negative_deviation() -> None:
    baseline = pd.Series([10.0] * 30 + [12.0] * 30)
    z = zscore(8.0, baseline)
    assert z is not None
    assert z < 0


def test_zscore_returns_none_when_history_too_short() -> None:
    baseline = pd.Series([1.0, 2.0, 3.0])  # under default min_observations
    assert zscore(10.0, baseline) is None


def test_zscore_returns_none_when_std_is_zero() -> None:
    baseline = pd.Series([5.0] * 50)
    assert zscore(5.0, baseline) is None


def test_zscore_returns_none_when_latest_is_nan() -> None:
    baseline = pd.Series(np.linspace(0, 100, 100))
    assert zscore(float("nan"), baseline) is None


def test_zscore_drops_nans_from_baseline() -> None:
    baseline = pd.Series([1.0, float("nan"), 2.0, float("nan"), 3.0] * 20)
    z = zscore(2.0, baseline)
    assert z is not None
    # Same as if NaNs were never there.
    expected_baseline = pd.Series([1.0, 2.0, 3.0] * 20)
    assert math.isclose(z, zscore(2.0, expected_baseline), rel_tol=1e-9)


def test_zscore_respects_custom_min_observations() -> None:
    baseline = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    assert zscore(10.0, baseline) is None
    z = zscore(10.0, baseline, min_observations=5)
    assert z is not None and z > 0


@pytest.mark.parametrize(
    ("z", "expected"),
    [
        (None, ""),
        (0.0, "+0.0σ"),
        (2.3, "+2.3σ"),
        (-1.4, "-1.4σ"),
        (0.04, "+0.0σ"),  # rounds to one decimal
    ],
)
def test_format_zscore(z: float | None, expected: str) -> None:
    assert format_zscore(z) == expected
