"""Formatting defects a reader saw on the live site (QA 2026-11-16).

Each test pins one rendered string: a heading that was a storage key, a number
truncated to an integer, a revision that read "revised UP 0", a row repeated
once per report vintage, a lane printed with a missing side, a unit printed as
its key, and float noise on a native price. Expected strings are written out
by hand from the source values, never recomputed the way the code does it.
"""

from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest
from opportunity_fixtures import CHINA, GULF, make_evidence, make_opportunity

from analysis.briefing.sections import economic, usda
from analysis.futures.crush import NamedCrushLeg
from analysis.futures.domain import (
    YFINANCE_DELAYED,
    ContractQuote,
    PriceType,
    named_contract,
    spec_for,
)
from pipeline.units import unit_key_label
from scripts.generate_html import _build_demand, _build_supply

# ---------------------------------------------------------------------------
# 1. Headline WASDE block
# ---------------------------------------------------------------------------


def _wasde(**attrs) -> dict:
    return {"value": attrs["value"], "unit": attrs["unit"], "period": "2026-11-10",
            "marketing_year": "2026/27", **({"revision": attrs["revision"]} if "revision" in attrs else {})}


def test_wasde_headings_are_names_not_storage_keys() -> None:
    html = _build_supply({"wasde": {
        "SOYBEAN_MEAL": {"Production": _wasde(value=54_000.0, unit="Thousand Short Tons")},
        "SOYBEAN_OIL": {"Production": _wasde(value=30_500.0, unit="Million Pounds")},
        "CORN": {"Production": _wasde(value=16_752.0, unit="Million Bushels")},
    }})
    soy = html["wasde_html"]
    assert ">Soybean Meal<" in soy
    assert ">Soybean Oil<" in soy
    assert "SOYBEAN_MEAL" not in soy and "SOYBEAN_OIL" not in soy
    assert ">Corn<" in html["competing_html"]


def test_wasde_values_keep_the_precision_wasde_prints() -> None:
    html = _build_supply({"wasde": {"SOYBEANS": {
        "Avg. Farm Price ($/bu)": _wasde(value=12.1, unit="$/bu", revision=0.0),
        "Yield per Harvested Acre": _wasde(value=53.3, unit="Bushels", revision=0.2),
        "Production": _wasde(value=4262.0, unit="Million Bushels", revision=-38.0),
    }}})["wasde_html"]
    assert "<strong style=\"color:var(--text)\">12.10</strong> $/bu" in html
    assert "<strong style=\"color:var(--text)\">53.3</strong> Bushels" in html
    assert "<strong style=\"color:var(--text)\">4,262</strong> Million Bushels" in html
    assert "(revised UP 0.2)" in html
    assert "(revised DOWN 38)" in html


def test_a_zero_or_rounding_noise_revision_reads_unchanged() -> None:
    html = _build_supply({"wasde": {"SOYBEANS": {
        "Avg. Farm Price ($/bu)": _wasde(value=12.1, unit="$/bu", revision=0.0),
        "Yield per Harvested Acre": _wasde(value=53.3, unit="Bushels", revision=1e-9),
    }}})["wasde_html"]
    assert "revised UP 0" not in html
    assert "revised DOWN 0" not in html
    assert html.count("(unchanged)") == 2


# ---------------------------------------------------------------------------
# 5. Energy cards carry the unit EIA stored with the series
# ---------------------------------------------------------------------------


def test_biofuel_card_prints_the_stored_unit_and_none_when_absent() -> None:
    html = _build_demand({"biofuel": {
        "Diesel Retail Price": {"value": 6.13, "unit": "$/GAL", "date": date(2026, 11, 9), "chg_pct": 1.0},
        "Ethanol Production": {"value": 1130.0, "unit": "", "date": date(2026, 11, 7), "chg_pct": -0.5},
    }})["biofuel_html"]
    assert '6.13 <span class="caption">$/GAL</span>' in html
    assert '1,130.00</div>' in html  # no unit stored → none invented


# ---------------------------------------------------------------------------
# 2. Briefing — USDA FUNDAMENTALS and the Fed Funds line
# ---------------------------------------------------------------------------

PROD = "SOYBEANS - PRODUCTION, MEASURED IN BU"
YIELD = "SOYBEANS - YIELD, MEASURED IN BU / ACRE"


def _seed_usda(db: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(str(db))
    conn.executemany(
        "INSERT INTO usda (stat_category, year, short_desc, Value, unit_desc,"
        " state_name, reference_period_desc) VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()


def test_usda_prints_one_row_per_statistic_from_the_latest_report(patched_db: Path) -> None:
    rows = [
        ("PRODUCTION", "2026", PROD, "4,574,000,000", "BU", "US TOTAL", "YEAR - AUG FORECAST"),
        ("PRODUCTION", "2026", PROD, "4,519,000,000", "BU", "US TOTAL", "YEAR - SEP FORECAST"),
        ("PRODUCTION", "2026", PROD, "4,562,000,000", "BU", "US TOTAL", "YEAR - NOV FORECAST"),
        ("PRODUCTION", "2026", PROD, "4,535,000,000", "BU", "US TOTAL", "YEAR - OCT FORECAST"),
        ("PRODUCTION", "2025", PROD, "4,300,000,000", "BU", "US TOTAL", "YEAR"),
        ("PRODUCTION", "2025", PROD, "4,350,000,000", "BU", "US TOTAL", "YEAR - NOV FORECAST"),
        ("YIELD", "2026", YIELD, "53.0", "BU / ACRE", "US TOTAL", "YEAR - OCT FORECAST"),
        ("YIELD", "2026", YIELD, "53.3", "BU / ACRE", "US TOTAL", "YEAR - NOV FORECAST"),
    ]
    _seed_usda(patched_db, rows)

    text = usda.format()
    lines = text.splitlines()

    assert sum(PROD in line for line in lines) == 1
    assert sum(YIELD in line for line in lines) == 1
    prod = next(line for line in lines if PROD in line)
    # Nov is the latest 2026 vintage even though Oct sorts after it in the table.
    assert "4,562,000,000 BU" in prod
    assert "2026 Nov forecast" in prod
    assert "+27,000,000 vs Oct forecast 4,535,000,000" in prod
    # 4,562 / 4,300 (2025 final, not its Nov forecast) = +6.1%
    assert "+6.1% YoY vs 2025 final" in prod
    yld = next(line for line in lines if YIELD in line)
    assert "53.3 BU / ACRE" in yld
    assert "+0.3 vs Oct forecast 53" in yld


def test_usda_withholds_an_unrankable_vintage_with_a_reason(patched_db: Path) -> None:
    _seed_usda(patched_db, [
        ("PRODUCTION", "2026", PROD, "4,562,000,000", "BU", "US TOTAL", "YEAR - NOV FORECAST"),
        ("PRODUCTION", "2026", PROD, "4,600,000,000", "BU", "US TOTAL", "SOMETHING NEW"),
    ])
    text = usda.format()
    assert "4,600,000,000" not in text
    assert "withheld" in text and "'SOMETHING NEW'" in text


def _seed_fed_funds(db: Path, rows: list[tuple[str, float]]) -> None:
    conn = sqlite3.connect(str(db))
    conn.executemany(
        "INSERT INTO economic (series_name, Date, value) VALUES ('Fed Funds Rate', ?, ?)", rows
    )
    conn.commit()
    conn.close()


def test_an_unchanged_fed_funds_rate_reads_unchanged_not_a_stance(patched_db: Path) -> None:
    _seed_fed_funds(patched_db, [("2026-09-01", 3.88), ("2026-10-01", 3.88)])
    text = economic.format_fred()
    assert "Fed Funds Rate: 3.88% (unchanged vs 2026-09)" in text
    assert "tightening" not in text and "easing" not in text


def test_a_fed_funds_move_is_stated_in_basis_points(patched_db: Path) -> None:
    _seed_fed_funds(patched_db, [("2026-09-01", 4.13), ("2026-10-01", 3.88)])
    assert "Fed Funds Rate: 3.88% (-25 bp vs 2026-09)" in economic.format_fred()


# ---------------------------------------------------------------------------
# 3. Opportunities — lane and units
# ---------------------------------------------------------------------------


def test_a_two_sided_opportunity_keeps_its_lane() -> None:
    assert make_opportunity().lane == "US→CN"


def test_a_one_sided_opportunity_names_its_market_never_a_half_lane() -> None:
    crush = replace(make_opportunity(), origin=None, destination=CHINA)
    fx = replace(make_opportunity(), origin=GULF, destination=None)
    assert crush.lane == "China"
    assert fx.lane == "United States"
    assert "—" not in crush.lane and "→" not in fx.lane


def test_opportunity_units_render_as_labels() -> None:
    payload = make_opportunity(evidence=(make_evidence(),)).to_dict()
    evidence = payload["evidence"][0]
    assert evidence["unit"] == "usd_per_mt"
    assert evidence["unit_label"] == "USD/MT"


def test_unit_key_labels_and_unknown_keys_fail_loudly() -> None:
    assert unit_key_label("usd_per_mt") == "USD/MT"
    assert unit_key_label("cents_per_pound") == "cents/lb"
    assert unit_key_label("usd_per_short_ton") == "$/short ton"
    assert unit_key_label("pct") == "%"
    with pytest.raises(ValueError, match="no display label"):
        unit_key_label("furlongs_per_fortnight")


# ---------------------------------------------------------------------------
# 4. Workstation board crush — tick precision and unit labels
# ---------------------------------------------------------------------------


def _leg(role: str, commodity: str, month: int, price: float, yield_mt: float | None) -> NamedCrushLeg:
    return NamedCrushLeg(
        role=role,
        quote=ContractQuote(
            contract=named_contract(commodity, 2026, month),
            price=price,
            price_type=PriceType.DELAYED_CLOSE,
            observation_date=date(2026, 11, 13),
            provider=YFINANCE_DELAYED,
        ),
        yield_mt=yield_mt,
    )


def test_native_prices_print_at_tick_precision_with_the_unit_label() -> None:
    meal = _leg("meal", "Soybean Meal", 12, 327.799988, 0.785)
    oil = _leg("oil", "Soybean Oil", 12, 64.050003, 0.19)
    bean = _leg("bean", "Soybeans", 11, 1150.25, None)

    assert meal.native_display == "327.8"
    assert oil.native_display == "64.05"
    assert bean.native_display == "1,150.25"
    assert meal.native_unit_label == "$/short ton"
    assert oil.native_unit_label == "cents/lb"
    assert bean.native_unit_label == "cents/bu"
    assert meal.to_dict()["native_unit_label"] == "$/short ton"
    assert "327.8 $/short ton" in meal.working()
    assert "usd_per_short_ton" not in meal.working()


def test_tick_precision_is_the_ticks_own_decimals() -> None:
    assert spec_for("Soybeans").price_decimals == 2       # 0.25
    assert spec_for("Soybean Meal").price_decimals == 1   # 0.10
    assert spec_for("Soybean Oil").price_decimals == 2    # 0.01
