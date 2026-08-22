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
    WORLD,
    WORLD_LESS_CHINA,
    compute_stocks_to_use,
    denominator_note,
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
# The world denominator (M15 #237)
# ---------------------------------------------------------------------------

# USDA PSD world row, soybeans MY2025, July-2026 vintage (WASDE-673), 1000 MT.
_WORLD_SOY_2025 = {
    "Ending Stocks": 125_325.0,
    "Domestic Consumption": 429_333.0,
    "Exports": 187_078.0,
    "Imports": 186_342.0,
}
# PSD world row, wheat MY2025, same vintage. WASDE's grain tables adjust
# world use by (exports − imports); its oilseed tables do not.
_WORLD_WHEAT_2025 = {
    "Ending Stocks": 279_035.0,
    "Domestic Consumption": 819_541.0,
    "Exports": 227_084.0,
    "Imports": 222_013.0,
}


def _world_frame(commodity: str, year: int, values: dict[str, float],
                 country: str = WORLD) -> pd.DataFrame:
    return pd.DataFrame([
        _psd_row(commodity=commodity, country=country, year=year,
                 attribute=attribute, value=value)
        for attribute, value in values.items()
    ])


def test_world_denominator_is_consumption_only():
    """Soybeans MY2025 = 29.19%, not the 20.33% the US formula would give.

    World exports are already inside some importer's domestic consumption,
    so adding them to a world denominator double-counts every traded tonne.
    Both numbers look like plausible percentages — that is why this is pinned.
    """
    df = _world_frame("Soybeans", 2025, _WORLD_SOY_2025)

    out = compute_stocks_to_use(df, country=WORLD)

    assert len(out) == 1
    row = out.iloc[0]
    assert row["total_use"] == pytest.approx(429_333.0)
    assert row["ratio"] == pytest.approx(0.2919, abs=5e-5)
    # The US formula applied to the world — the trap.
    assert row["ratio"] != pytest.approx(0.2033, abs=5e-4)


def test_world_less_china_uses_the_same_consumption_only_denominator():
    df = _world_frame(
        "Soybeans", 2025,
        {"Ending Stocks": 80_956.0, "Domestic Consumption": 295_433.0,
         "Exports": 186_958.0, "Imports": 73_342.0},
        country=WORLD_LESS_CHINA,
    )

    out = compute_stocks_to_use(df, country=WORLD_LESS_CHINA)

    assert out.iloc[0]["ratio"] == pytest.approx(0.2740, abs=5e-5)


def test_single_country_denominator_still_includes_exports():
    df = _psd_frame(pairs=[(2025, 100.0, 1000.0)])

    out = compute_stocks_to_use(df, country="United States")

    assert out.iloc[0]["total_use"] == pytest.approx(1000.0)


def test_world_ratio_needs_no_exports_row():
    """Consumption-only means an absent Exports row is not a missing input."""
    df = _world_frame("Soybeans", 2025, {
        "Ending Stocks": 125_325.0, "Domestic Consumption": 429_333.0,
    })

    out = compute_stocks_to_use(df, country=WORLD)

    assert out.iloc[0]["ratio"] == pytest.approx(0.2919, abs=5e-5)


def test_grain_adjustment_reproduces_the_wasde_printed_wheat_ratio():
    """WASDE footnote 2/: world use adjusted for the import/export gap.

    819,541 + (227,084 − 222,013) = 824,612, which is the 824.61 WASDE-673
    prints — and moves wheat S/U from 34.05% to 33.84%.
    """
    df = _world_frame("Wheat", 2025, _WORLD_WHEAT_2025)

    raw = compute_stocks_to_use(df, country=WORLD)
    adjusted = compute_stocks_to_use(df, country=WORLD, wasde_grain_adjustment=True)

    assert raw.iloc[0]["ratio"] == pytest.approx(0.3405, abs=5e-5)
    assert adjusted.iloc[0]["total_use"] == pytest.approx(824_612.0)
    assert adjusted.iloc[0]["ratio"] == pytest.approx(0.3384, abs=5e-5)


def test_grain_adjustment_leaves_oilseeds_alone():
    """USDA applies the adjustment in its grain tables only."""
    df = _world_frame("Soybeans", 2025, _WORLD_SOY_2025)

    adjusted = compute_stocks_to_use(df, country=WORLD, wasde_grain_adjustment=True)

    assert adjusted.iloc[0]["total_use"] == pytest.approx(429_333.0)


def test_grain_adjustment_is_withheld_without_an_imports_row():
    # DC + Exports − Imports with a missing Imports is not "no adjustment",
    # it is an unanswerable question. Withhold the row rather than print the
    # unadjusted figure under an adjusted label.
    df = _world_frame("Wheat", 2025, {
        "Ending Stocks": 279_035.0, "Domestic Consumption": 819_541.0,
        "Exports": 227_084.0,
    })

    assert compute_stocks_to_use(
        df, country=WORLD, wasde_grain_adjustment=True
    ).empty
    assert not compute_stocks_to_use(df, country=WORLD).empty


def test_grain_adjustment_is_rejected_for_a_single_country():
    """The adjustment is a property of USDA's *world* table, not a country's."""
    df = _psd_frame(pairs=[(2025, 100.0, 1000.0)])

    with pytest.raises(ValueError, match="aggregate region"):
        compute_stocks_to_use(
            df, country="United States", wasde_grain_adjustment=True
        )


def test_denominator_note_states_region_denominator_and_adjustment():
    world = denominator_note(WORLD, wasde_grain_adjustment=True)
    assert "Domestic Consumption" in world
    assert "every PSD country" in world
    assert "WASDE" in world

    raw = denominator_note(WORLD, wasde_grain_adjustment=False)
    assert "raw PSD" in raw

    us = denominator_note("United States")
    assert "Exports" in us


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
