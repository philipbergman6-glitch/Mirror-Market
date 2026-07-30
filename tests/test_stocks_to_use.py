"""Unit tests for analysis.stocks_to_use.

Covers:
  - `compute_stocks_to_use` ratio math, country filtering, missing-component
    handling, and "latest row wins" pivot behavior.
  - `detect_tight_supply` alert-fires-and-doesn't-fire cases, sample-size
    guard, signal-dict shape.
"""

from __future__ import annotations

import pandas as pd
import pytest

from analysis.stocks_to_use import (
    HISTORY_WINDOW,
    MIN_HISTORY_YEARS,
    compute_stocks_to_use,
    detect_tight_supply,
)

REQUIRED_SIGNAL_KEYS = {"date", "commodity", "signal_type", "severity", "description"}


def _psd_row(
    *,
    commodity: str,
    country: str,
    year: int,
    attribute: str,
    value: float,
) -> dict:
    return {
        "commodity": commodity,
        "country": country,
        "year": year,
        "attribute": attribute,
        "value": value,
        "unit": "(1000 MT)",
    }


def _psd_frame(
    commodity: str = "Soybeans",
    country: str = "United States",
    pairs: list[tuple[int, float, float]] | None = None,
) -> pd.DataFrame:
    """Build a PSD-shaped frame from (year, ending_stocks, total_use) tuples.

    Total use is split 80/20 into Domestic Consumption and Exports — the
    two attributes `compute_stocks_to_use` sums for its denominator.
    """
    pairs = pairs or []
    rows: list[dict] = []
    for year, stocks, use in pairs:
        rows.append(_psd_row(
            commodity=commodity, country=country, year=year,
            attribute="Ending Stocks", value=stocks,
        ))
        rows.append(_psd_row(
            commodity=commodity, country=country, year=year,
            attribute="Domestic Consumption", value=use * 0.8,
        ))
        rows.append(_psd_row(
            commodity=commodity, country=country, year=year,
            attribute="Exports", value=use * 0.2,
        ))
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# compute_stocks_to_use
# ---------------------------------------------------------------------------


def test_compute_returns_empty_for_empty_input():
    out = compute_stocks_to_use(pd.DataFrame())
    assert out.empty
    assert list(out.columns) == [
        "commodity", "year", "ending_stocks", "total_use", "ratio",
    ]


def test_compute_basic_ratio_math():
    df = _psd_frame(pairs=[(2024, 8840.0, 129155.0), (2025, 9523.0, 125509.0)])
    out = compute_stocks_to_use(df)

    assert len(out) == 2
    row_2025 = out[out["year"] == 2025].iloc[0]
    assert row_2025["ending_stocks"] == pytest.approx(9523.0)
    assert row_2025["total_use"] == pytest.approx(125509.0)
    assert row_2025["ratio"] == pytest.approx(9523.0 / 125509.0)


def test_compute_filters_by_country():
    us = _psd_frame(country="United States", pairs=[(2025, 100.0, 1000.0)])
    br = _psd_frame(country="Brazil", pairs=[(2025, 500.0, 5000.0)])
    df = pd.concat([us, br], ignore_index=True)

    us_out = compute_stocks_to_use(df, country="United States")
    assert len(us_out) == 1
    assert us_out.iloc[0]["ratio"] == pytest.approx(0.1)

    br_out = compute_stocks_to_use(df, country="Brazil")
    assert len(br_out) == 1
    assert br_out.iloc[0]["ratio"] == pytest.approx(0.1)


def test_compute_drops_rows_missing_a_component():
    rows = [
        _psd_row(commodity="Soybeans", country="United States", year=2025,
                 attribute="Ending Stocks", value=100.0),
        # Domestic Consumption + Exports missing for 2025
        _psd_row(commodity="Soybeans", country="United States", year=2024,
                 attribute="Ending Stocks", value=80.0),
        _psd_row(commodity="Soybeans", country="United States", year=2024,
                 attribute="Domestic Consumption", value=800.0),
        _psd_row(commodity="Soybeans", country="United States", year=2024,
                 attribute="Exports", value=200.0),
    ]
    out = compute_stocks_to_use(pd.DataFrame(rows))
    assert len(out) == 1
    assert out.iloc[0]["year"] == 2024


def test_compute_drops_zero_total_use():
    df = _psd_frame(pairs=[(2024, 100.0, 1000.0), (2025, 50.0, 0.0)])
    out = compute_stocks_to_use(df)
    assert list(out["year"]) == [2024]


def test_compute_ignores_unrelated_attributes():
    df = _psd_frame(pairs=[(2025, 100.0, 1000.0)])
    extra = pd.DataFrame([_psd_row(
        commodity="Soybeans", country="United States", year=2025,
        attribute="Production", value=99999.0,
    )])
    out = compute_stocks_to_use(pd.concat([df, extra], ignore_index=True))
    assert len(out) == 1
    assert out.iloc[0]["ratio"] == pytest.approx(0.1)


# ---------------------------------------------------------------------------
# detect_tight_supply
# ---------------------------------------------------------------------------


def _stu_frame(commodity: str, ratios: list[tuple[int, float]]) -> pd.DataFrame:
    """Build a `compute_stocks_to_use`-shaped frame directly from ratios."""
    return pd.DataFrame([
        {
            "commodity": commodity, "year": year,
            "ending_stocks": ratio * 1000.0,
            "total_use": 1000.0,
            "ratio": ratio,
        }
        for year, ratio in ratios
    ])


def test_detect_fires_when_current_below_prior_low():
    stu = _stu_frame("Soybeans", [
        (2019, 0.09), (2020, 0.11), (2021, 0.10),
        (2022, 0.12), (2023, 0.095), (2024, 0.10),
        (2025, 0.04),
    ])
    sigs = detect_tight_supply(stu, today="2026-05-11")

    assert len(sigs) == 1
    sig = sigs[0]
    assert REQUIRED_SIGNAL_KEYS.issubset(sig.keys())
    assert sig["severity"] == "alert"
    assert sig["signal_type"] == "tight_supply_wasde"
    assert sig["commodity"] == "Soybeans"
    assert sig["date"] == "2026-05-11"
    assert "below" in sig["description"]
    assert "4.0%" in sig["description"]


def test_detect_silent_when_current_above_prior_low():
    stu = _stu_frame("Soybeans", [
        (2020, 0.05), (2021, 0.06), (2022, 0.07),
        (2023, 0.08), (2024, 0.09), (2025, 0.10),
    ])
    assert detect_tight_supply(stu) == []


def test_detect_silent_with_insufficient_history():
    # 2 prior years + current = below MIN_HISTORY_YEARS prior
    stu = _stu_frame("Soybeans", [(2023, 0.10), (2024, 0.09), (2025, 0.01)])
    assert MIN_HISTORY_YEARS == 3
    assert detect_tight_supply(stu) == []


def test_detect_requires_min_history_exactly():
    # MIN_HISTORY_YEARS prior + current → can fire
    ratios = [(2022, 0.10), (2023, 0.10), (2024, 0.10), (2025, 0.04)]
    sigs = detect_tight_supply(_stu_frame("Soybeans", ratios))
    assert len(sigs) == 1
    assert f"{MIN_HISTORY_YEARS}-yr prior low" in sigs[0]["description"]


def test_detect_window_is_capped_at_history_window():
    # 8 prior years; only the most-recent HISTORY_WINDOW=5 count toward the low.
    # Years 2015–2017 hold ratios below current — they must be excluded so the
    # alert still fires against the window minimum (2018–2022 → min 0.06).
    ratios = [
        (2015, 0.01),
        (2016, 0.01),
        (2017, 0.01),
        (2018, 0.06),
        (2019, 0.07),
        (2020, 0.08),
        (2021, 0.09),
        (2022, 0.10),
        (2023, 0.04),  # current — below window-min 0.06, above historic-min 0.01
    ]
    sigs = detect_tight_supply(_stu_frame("Soybeans", ratios))
    assert HISTORY_WINDOW == 5
    assert len(sigs) == 1
    assert "5-yr prior low of 6.0%" in sigs[0]["description"]


def test_detect_restricts_to_commodities_arg():
    stu = pd.concat([
        _stu_frame("Soybeans", [
            (2020, 0.10), (2021, 0.10), (2022, 0.10),
            (2023, 0.10), (2024, 0.10), (2025, 0.04),
        ]),
        _stu_frame("Corn", [
            (2020, 0.20), (2021, 0.20), (2022, 0.20),
            (2023, 0.20), (2024, 0.20), (2025, 0.05),
        ]),
    ], ignore_index=True)

    only_soy = detect_tight_supply(stu, commodities=["Soybeans"])
    assert [s["commodity"] for s in only_soy] == ["Soybeans"]


def test_detect_handles_empty_frame():
    assert detect_tight_supply(pd.DataFrame()) == []
