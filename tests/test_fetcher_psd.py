"""Unit tests for fetchers.psd — the world aggregate (M15 #237).

The world total is already inside the bulk CSVs Layer 6 downloads; the
country filter destroyed it. Summing *before* that filter reconstructs
USDA's own World row. These tests pin the three ways that summation can
go quietly wrong:

  - summing only the countries we happen to track (wheat is 5.7 pp off),
  - summing a *rate* attribute (Yield, Stocks-to-Use) into nonsense,
  - inventing a World-less-China row where no China row exists.
"""

from __future__ import annotations

import pandas as pd
import pytest

from fetchers.psd import WORLD, WORLD_LESS_CHINA, _filter_psd, _is_additive_unit

_SOY = "2222000"  # config.PSD_TARGET_COMMODITIES["Soybeans"]


def _raw(rows: list[dict]) -> pd.DataFrame:
    """Build a bulk-CSV-shaped frame from partial row dicts."""
    defaults = {
        "Commodity_Code": _SOY,
        "Country_Name": "United States",
        "Market_Year": 2025,
        "Attribute_Description": "Ending Stocks",
        "Value": 0.0,
        "Unit_Description": "(1000 MT)",
    }
    return pd.DataFrame([{**defaults, **row} for row in rows])


def _value(df: pd.DataFrame, country: str, attribute: str) -> float:
    match = df[(df["country"] == country) & (df["attribute"] == attribute)]
    assert len(match) == 1, f"expected one {country}/{attribute} row, got {len(match)}"
    return float(match.iloc[0]["value"])


# ---------------------------------------------------------------------------
# The World row sums every country, not the tracked ones
# ---------------------------------------------------------------------------


def test_world_row_sums_countries_outside_the_target_list():
    # Russia and Egypt are real PSD countries that PSD_TARGET_COUNTRIES omits.
    # A world total that skipped them would be the 28-country sum, which is a
    # different (and wrong) statistic.
    raw = _raw([
        {"Country_Name": "United States", "Value": 100.0},
        {"Country_Name": "China", "Value": 40.0},
        {"Country_Name": "Russia", "Value": 7.0},
        {"Country_Name": "Egypt", "Value": 3.0},
    ])

    out = _filter_psd(raw)

    assert _value(out, WORLD, "Ending Stocks") == 150.0
    # ...and the country filter still applies to the non-world rows.
    assert set(out["country"]) == {WORLD, WORLD_LESS_CHINA, "United States", "China"}


def test_world_less_china_is_the_world_row_minus_china():
    raw = _raw([
        {"Country_Name": "United States", "Value": 100.0},
        {"Country_Name": "China", "Value": 40.0},
        {"Country_Name": "Russia", "Value": 7.0},
    ])

    out = _filter_psd(raw)

    assert _value(out, WORLD_LESS_CHINA, "Ending Stocks") == 107.0


def test_world_less_china_is_withheld_when_china_has_no_row():
    # NULL means "never learned". A missing China row is not a China zero,
    # so the subtraction is not performed at all.
    raw = _raw([
        {"Country_Name": "United States", "Value": 100.0},
        {"Country_Name": "Russia", "Value": 7.0},
    ])

    out = _filter_psd(raw)

    assert _value(out, WORLD, "Ending Stocks") == 107.0
    assert WORLD_LESS_CHINA not in set(out["country"])


def test_world_rows_are_emitted_per_marketing_year():
    raw = _raw([
        {"Country_Name": "Brazil", "Market_Year": 2024, "Value": 10.0},
        {"Country_Name": "China", "Market_Year": 2024, "Value": 4.0},
        {"Country_Name": "Brazil", "Market_Year": 2025, "Value": 20.0},
        {"Country_Name": "China", "Market_Year": 2025, "Value": 5.0},
    ])

    out = _filter_psd(raw)
    world = out[out["country"] == WORLD].set_index("year")["value"]

    assert world.loc[2024] == 14.0
    assert world.loc[2025] == 25.0


def test_world_row_carries_the_source_unit():
    raw = _raw([
        {"Country_Name": "United States", "Value": 100.0, "Unit_Description": "(1000 MT)"},
        {"Country_Name": "Brazil", "Value": 50.0, "Unit_Description": "(1000 MT)"},
    ])

    out = _filter_psd(raw)
    world = out[out["country"] == WORLD]

    assert set(world["unit"]) == {"(1000 MT)"}


# ---------------------------------------------------------------------------
# Rate attributes are never aggregated
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("unit", ["(MT/HA)", "(KG/HA)", "(PERCENT)", "(RATIO)"])
def test_a_rate_unit_is_never_aggregated(unit):
    # Summing country Yield for soybeans MY2025 gives 95.39 MT/HA against
    # USDA's World 3.01; PSD's own attr-195 world value is 17 pp from the
    # ratio USDA prints. The gate is the *unit*, so a rate attribute cannot
    # slip through even if it is added to PSD_TARGET_ATTRIBUTES.
    raw = _raw([
        {"Country_Name": "United States", "Unit_Description": unit, "Value": 3.4},
        {"Country_Name": "Brazil", "Unit_Description": unit, "Value": 3.6},
    ])

    out = _filter_psd(raw)

    assert out[out["country"].isin({WORLD, WORLD_LESS_CHINA})].empty


@pytest.mark.parametrize(
    ("unit", "additive"),
    [
        ("(1000 MT)", True),
        ("1000 MT", True),
        ("(1000 HA)", True),
        ("1000 480 lb. Bales", True),
        ("(MT/HA)", False),
        ("(KG/HA)", False),
        ("(PERCENT)", False),
        ("(RATIO)", False),
        ("something new", False),
    ],
)
def test_additive_unit_gate(unit, additive):
    # Every Unit_Description observed across the three 2026 bulk CSVs.
    # An unrecognised unit is withheld, not guessed at.
    assert _is_additive_unit(unit) is additive


def test_cotton_bales_are_additive():
    raw = _raw([
        {"Country_Name": "United States", "Unit_Description": "1000 480 lb. Bales",
         "Value": 3_000.0},
        {"Country_Name": "China", "Unit_Description": "1000 480 lb. Bales",
         "Value": 36_562.0},
    ])

    out = _filter_psd(raw)

    assert _value(out, WORLD, "Ending Stocks") == 39_562.0


# ---------------------------------------------------------------------------
# Missing values are withheld, never zeroed
# ---------------------------------------------------------------------------


def test_all_null_values_yield_no_world_row():
    raw = _raw([
        {"Country_Name": "United States", "Value": None},
        {"Country_Name": "Brazil", "Value": None},
    ])

    out = _filter_psd(raw)

    assert out[out["country"] == WORLD].empty


def test_a_short_country_roster_withholds_the_world_row():
    """A group missing countries its siblings report is a partial sum.

    `min_count=1` only withholds an all-NULL group; 1 of 3 countries still
    sums to a plausible-looking world total. That is the 28-country-sum
    failure arriving through a different door.
    """
    raw = _raw([
        {"Country_Name": "United States", "Attribute_Description": "Ending Stocks",
         "Value": 100.0},
        {"Country_Name": "Brazil", "Attribute_Description": "Ending Stocks",
         "Value": 50.0},
        {"Country_Name": "United States", "Attribute_Description": "Production",
         "Value": 400.0},
        {"Country_Name": "Brazil", "Attribute_Description": "Production",
         "Value": 200.0},
        # Exports reported by one of the two countries only.
        {"Country_Name": "United States", "Attribute_Description": "Exports",
         "Value": 60.0},
    ])

    out = _filter_psd(raw)
    world = out[out["country"] == WORLD]

    assert set(world["attribute"]) == {"Ending Stocks", "Production"}
    assert _value(out, WORLD, "Ending Stocks") == 150.0


def test_a_rate_unit_row_inside_a_quantity_attribute_withholds_that_row():
    # An upstream change stamping some Ending Stocks rows (PERCENT) drops
    # them at the unit gate. The attribute name survives in what remains, so
    # only the roster check can notice the total is now short.
    raw = _raw([
        {"Country_Name": "United States", "Value": 100.0},
        {"Country_Name": "Brazil", "Value": 50.0, "Unit_Description": "(PERCENT)"},
        {"Country_Name": "United States", "Attribute_Description": "Production",
         "Value": 400.0},
        {"Country_Name": "Brazil", "Attribute_Description": "Production",
         "Value": 200.0},
    ])

    out = _filter_psd(raw)

    assert "Ending Stocks" not in set(out[out["country"] == WORLD]["attribute"])
    assert _value(out, WORLD, "Production") == 600.0


def test_duplicate_rows_hard_fail_rather_than_double_the_world():
    """The country path survives a duplicate; a summed world row would not.

    Storage upserts on (commodity, country, year, attribute), so a second
    vintage of the same row overwrites harmlessly there — but summed, it
    doubles that country's contribution with nothing in the result's shape
    marking it.
    """
    raw = _raw([
        {"Country_Name": "United States", "Value": 100.0},
        {"Country_Name": "United States", "Value": 100.0},
    ])

    with pytest.raises(ValueError, match="duplicate"):
        _filter_psd(raw)


def test_a_rate_attribute_added_to_the_target_list_still_never_aggregates(
    monkeypatch,
):
    """The unit gate is the backstop behind the attribute filter.

    Yield and Stocks-to-Use are excluded today because
    PSD_TARGET_ATTRIBUTES does not list them. This pins what happens the
    day someone adds one: soybean world Yield is 3.01 MT/HA and the
    country-sum is 95.39.
    """
    import fetchers.psd as psd_module

    monkeypatch.setattr(
        psd_module, "PSD_TARGET_ATTRIBUTES",
        [*psd_module.PSD_TARGET_ATTRIBUTES, "Yield", "Stocks to Use Ratio"],
    )
    raw = _raw([
        {"Country_Name": "United States", "Attribute_Description": "Yield",
         "Unit_Description": "(MT/HA)", "Value": 3.4},
        {"Country_Name": "Brazil", "Attribute_Description": "Yield",
         "Unit_Description": "(MT/HA)", "Value": 3.6},
        {"Country_Name": "United States",
         "Attribute_Description": "Stocks to Use Ratio",
         "Unit_Description": "(PERCENT)", "Value": 89.01},
        {"Country_Name": "Brazil",
         "Attribute_Description": "Stocks to Use Ratio",
         "Unit_Description": "(PERCENT)", "Value": 12.5},
        {"Country_Name": "United States", "Value": 100.0},
        {"Country_Name": "Brazil", "Value": 50.0},
    ])

    out = _filter_psd(raw)
    world = out[out["country"] == WORLD]

    assert set(world["attribute"]) == {"Ending Stocks"}
    assert _value(out, WORLD, "Ending Stocks") == 150.0
    # The country rows keep them — only the aggregate refuses.
    assert "Yield" in set(out[out["country"] == "United States"]["attribute"])


def test_a_single_attribute_with_two_units_is_skipped():
    # Averaging or summing across units would invent a number. Two units on
    # one (commodity, year, attribute) is a source change we refuse to guess at.
    raw = _raw([
        {"Country_Name": "United States", "Value": 100.0, "Unit_Description": "(1000 MT)"},
        {"Country_Name": "Brazil", "Value": 50.0, "Unit_Description": "1000 480 lb. Bales"},
    ])

    out = _filter_psd(raw)

    assert out[out["country"].isin({WORLD, WORLD_LESS_CHINA})].empty
