"""Tests for the display-layer briefing additions.

Covers:
- export sales pace (commitments vs WASDE forecast, year-ago accumulated)
- port flows (Gulf/PNW/Interior tonnage in the inspections section)
- NASS crush (analysis/nass_crush.py + crush section line)
- Argentina PSD highlights
- Soybean Oil/Meal in the stocks-to-use section
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from analysis.briefing.sections import crush, export_sales, inspections, psd, stocks_to_use
from analysis.nass_crush import latest_crush

# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_export_sales(db: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(str(db))
    conn.executemany(
        "INSERT INTO export_sales (commodity, week_ending, country, net_sales,"
        " weekly_exports, accumulated_exports, outstanding_sales)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()


def _seed_wasde_soy_exports(db: Path, value: float, unit: str = "Million Bushels") -> None:
    conn = sqlite3.connect(str(db))
    conn.execute(
        "INSERT INTO wasde (commodity, year, attribute, value, unit, reference_period)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        ("SOYBEANS", "2025/26", "Exports", value, unit, "2026-07-15"),
    )
    conn.commit()
    conn.close()


def _seed_port_flows(db: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(str(db))
    conn.executemany(
        "INSERT INTO inspection_port_flows (week_ending, region, port_area,"
        " commodity, inspections_mt) VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()


def _seed_crush(db: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(str(db))
    conn.executemany(
        "INSERT INTO usda (stat_category, year, short_desc, Value, unit_desc,"
        " state_name, reference_period_desc) VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()


def _seed_psd(db: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(str(db))
    conn.executemany(
        "INSERT INTO psd (commodity, country, year, attribute, value, unit)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Export sales pace
# ---------------------------------------------------------------------------


def test_export_sales_pace_with_wasde_forecast(patched_db: Path) -> None:
    _seed_export_sales(patched_db, [
        ("Soybeans", "2026-07-23", "CHINA, PEOPLES REPUBLIC OF", 500_000.0, 400_000.0, 30_000_000.0, 8_000_000.0),
        ("Soybeans", "2026-07-23", "MEXICO", 200_000.0, 100_000.0, 10_000_000.0, 2_000_000.0),
        # same week one year earlier
        ("Soybeans", "2025-07-24", "CHINA, PEOPLES REPUBLIC OF", 1.0, 1.0, 38_000_000.0, 1_000_000.0),
    ])
    _seed_wasde_soy_exports(patched_db, 1_660.0)  # 1660 M bu = 45.18 MMT

    text = export_sales.format()

    assert "Pace (MY 2025/26)" in text
    assert "accumulated 40.00 MMT" in text
    assert "outstanding 10.00 MMT" in text
    assert "50.00 MMT committed" in text
    # 50.0 / (1660 * 1e6 / 36.7437 = 45.177 MMT) = 110.7% -> rounds to 111%
    assert "(111% of WASDE 45.18 MMT export forecast)" in text
    # year-ago accumulated: 38.0 MMT, +5.3%
    assert "vs year-ago accumulated (w/e 07/24/25): 38.00 MMT (+5.3%)" in text


def test_export_sales_pace_unit_mismatch_shows_raw_not_pct(patched_db: Path) -> None:
    _seed_export_sales(patched_db, [
        ("Soybeans", "2026-07-23", "MEXICO", 200_000.0, 100_000.0, 10_000_000.0, 2_000_000.0),
    ])
    _seed_wasde_soy_exports(patched_db, 400.0, unit="Million Pounds")

    text = export_sales.format()

    assert "12.00 MMT committed" in text
    assert "% of WASDE" not in text
    assert "WASDE export forecast: 400 Million Pounds (unit mismatch — no % computed)" in text


def test_export_sales_pace_degrades_without_wasde_or_prior_year(patched_db: Path) -> None:
    _seed_export_sales(patched_db, [
        ("Soybeans", "2026-07-23", "MEXICO", 200_000.0, 100_000.0, 10_000_000.0, 2_000_000.0),
    ])

    text = export_sales.format()

    assert "Pace (MY 2025/26): accumulated 10.00 MMT" in text
    assert "WASDE" not in text
    assert "year-ago" not in text


def test_export_sales_no_pace_line_for_other_commodities(patched_db: Path) -> None:
    _seed_export_sales(patched_db, [
        ("Corn", "2026-07-23", "MEXICO", 200_000.0, 100_000.0, 10_000_000.0, 2_000_000.0),
    ])
    text = export_sales.format()
    assert "Pace" not in text


def test_soy_marketing_year_boundaries() -> None:
    import pandas as pd

    assert export_sales.soy_marketing_year(pd.Timestamp("2026-07-23")) == "2025/26"
    assert export_sales.soy_marketing_year(pd.Timestamp("2026-09-04")) == "2026/27"
    assert export_sales.soy_marketing_year(pd.Timestamp("2026-01-02")) == "2025/26"


# ---------------------------------------------------------------------------
# Port flows
# ---------------------------------------------------------------------------

_FLOW_ROWS = [
    # latest week
    ("2026-07-24", "GULF", "SUBTOTAL", "Soybeans", 600_000.0),
    ("2026-07-24", "PACIFIC", "SUBTOTAL", "Soybeans", 300_000.0),
    ("2026-07-24", "INTERIOR", "SUBTOTAL", "Soybeans", 100_000.0),
    # non-SUBTOTAL port rows must be ignored
    ("2026-07-24", "GULF", "MISSISSIPPI RIVER", "Soybeans", 400_000.0),
    # prior week
    ("2026-07-17", "GULF", "SUBTOTAL", "Soybeans", 500_000.0),
    ("2026-07-17", "PACIFIC", "SUBTOTAL", "Soybeans", 400_000.0),
]


def test_inspections_section_includes_port_flows(patched_db: Path) -> None:
    conn = sqlite3.connect(str(patched_db))
    conn.execute(
        "INSERT INTO inspections (commodity, week_ending, inspections_mt) VALUES (?, ?, ?)",
        ("Soybeans", "2026-07-24", 1_000_000.0),
    )
    conn.commit()
    conn.close()
    _seed_port_flows(patched_db, _FLOW_ROWS)

    text = inspections.format()

    assert "US export flows by port region (AMS tonnage, not basis):" in text
    # Shares over the SUBTOTAL total (1,000,000), WoW vs prior week.
    assert "Gulf 600,000 MT (60%, WoW +20.0%)" in text
    assert "PNW 300,000 MT (30%, WoW -25.0%)" in text
    # Interior has no prior-week row -> share only.
    assert "Interior 100,000 MT (10%)" in text


def test_port_flows_shown_even_without_inspection_rows(patched_db: Path) -> None:
    _seed_port_flows(patched_db, _FLOW_ROWS)
    text = inspections.format()
    assert "Gulf 600,000 MT" in text


def test_inspections_section_degrades_without_port_flows(patched_db: Path) -> None:
    assert inspections.format() == "EXPORT INSPECTIONS: No data"


# ---------------------------------------------------------------------------
# NASS crush
# ---------------------------------------------------------------------------

# NOTE: the usda table PK is (stat_category, year, short_desc), so at
# most one CRUSHED row per year survives storage — seed accordingly.
_CRUSH_ROWS = [
    ("CRUSHED", "2026", "SOYBEANS - CRUSHED, MEASURED IN TONS", "6,143,000", "TONS", "US TOTAL", "MAY"),
    ("CRUSHED", "2025", "SOYBEANS - CRUSHED, MEASURED IN TONS", "5,850,000", "TONS", "US TOTAL", "MAY"),
]


def test_latest_crush_returns_latest_month_with_yoy(patched_db: Path) -> None:
    _seed_crush(patched_db, _CRUSH_ROWS)

    crush = latest_crush()

    assert crush is not None
    assert crush["value"] == 6_143_000.0
    assert crush["unit"] == "TONS"
    assert crush["year"] == 2026
    assert crush["period"] == "MAY"
    assert crush["yoy_pct"] == pytest.approx((6_143_000 - 5_850_000) / 5_850_000 * 100)


def test_summarize_crush_orders_months_and_skips_withheld() -> None:
    import pandas as pd

    from analysis.nass_crush import summarize_crush

    df = pd.DataFrame({
        "stat_category": ["CRUSHED"] * 4,
        "year": ["2026", "2026", "2025", "2026"],
        "short_desc": ["SOYBEANS - CRUSHED, MEASURED IN TONS"] * 4,
        "Value": ["6,143,000", "5,900,000", "5,850,000", "(D)"],
        "unit_desc": ["TONS"] * 4,
        "state_name": ["US TOTAL"] * 4,
        "reference_period_desc": ["MAY", "APR", "MAY", "JUN"],
    })

    crush = summarize_crush(df)

    # (D) JUN row is unparseable and dropped; MAY 2026 wins over APR.
    assert crush["period"] == "MAY"
    assert crush["value"] == 6_143_000.0
    assert crush["yoy_pct"] == pytest.approx(5.0085, rel=1e-3)


def test_summarize_crush_yoy_none_on_month_mismatch() -> None:
    import pandas as pd

    from analysis.nass_crush import summarize_crush

    df = pd.DataFrame({
        "stat_category": ["CRUSHED"] * 2,
        "year": ["2026", "2025"],
        "short_desc": ["SOYBEANS - CRUSHED, MEASURED IN TONS"] * 2,
        "Value": ["6,143,000", "6,000,000"],
        "unit_desc": ["TONS"] * 2,
        "state_name": ["US TOTAL"] * 2,
        "reference_period_desc": ["MAY", "DEC"],
    })

    crush = summarize_crush(df)

    assert crush["value"] == 6_143_000.0
    assert crush["yoy_pct"] is None


def test_latest_crush_none_without_rows(patched_db: Path) -> None:
    assert latest_crush() is None


def test_crush_section_appends_nass_line(patched_db: Path) -> None:
    _seed_crush(patched_db, _CRUSH_ROWS)

    text = crush.format({})  # no price data -> spread degrades, NASS line remains

    assert text.startswith("CRUSH SPREAD: Insufficient data")
    assert "US crush (NASS): 6,143,000 TONS in MAY 2026 (+5.0% YoY)" in text


def test_usda_section_excludes_monthly_crush_rows(patched_db: Path) -> None:
    from analysis.briefing.sections import usda

    _seed_crush(patched_db, _CRUSH_ROWS)
    assert usda.format() == "USDA FUNDAMENTALS: No data"


# ---------------------------------------------------------------------------
# Argentina PSD highlights
# ---------------------------------------------------------------------------


def test_psd_section_shows_argentina_with_yoy(patched_db: Path) -> None:
    _seed_psd(patched_db, [
        ("Soybeans", "Argentina", 2026, "Production", 52_000.0, "(1000 MT)"),
        ("Soybeans", "Argentina", 2025, "Production", 50_000.0, "(1000 MT)"),
        ("Soybean Meal", "Argentina", 2026, "Exports", 29_400.0, "(1000 MT)"),
        ("Soybean Meal", "Argentina", 2025, "Exports", 28_000.0, "(1000 MT)"),
        ("Soybean Oil", "Argentina", 2026, "Exports", 6_650.0, "(1000 MT)"),
    ])

    text = psd.format()

    assert "Argentina (top soymeal/oil exporter):" in text
    assert "Soybeans production: 52,000 (1000 MT) (+4.0% YoY)" in text
    assert "Soybean Meal exports: 29,400 (1000 MT) (+5.0% YoY)" in text
    # No prior year -> level only, no YoY suffix.
    assert "Soybean Oil exports: 6,650 (1000 MT)\n" in text + "\n"


def test_psd_section_no_argentina_block_when_absent(patched_db: Path) -> None:
    _seed_psd(patched_db, [
        ("Soybeans", "Brazil", 2026, "Production", 170_000.0, "(1000 MT)"),
    ])
    assert "Argentina" not in psd.format()


# ---------------------------------------------------------------------------
# Stocks-to-use includes soy products
# ---------------------------------------------------------------------------


def test_stocks_to_use_section_includes_meal_and_oil(patched_db: Path) -> None:
    rows = []
    for commodity, stocks, use in [
        ("Soybeans", 8_435.0, 131_448.0),
        ("Soybean Meal", 500.0, 60_000.0),
        ("Soybean Oil", 800.0, 13_000.0),
    ]:
        rows.extend([
            (commodity, "United States", 2026, "Ending Stocks", stocks, "(1000 MT)"),
            (commodity, "United States", 2026, "Domestic Consumption", use * 0.8, "(1000 MT)"),
            (commodity, "United States", 2026, "Exports", use * 0.2, "(1000 MT)"),
        ])
    _seed_psd(patched_db, rows)

    text, signals = stocks_to_use.format()

    assert "Soybean Meal: 0.8% (MY 2026)" in text
    assert "Soybean Oil: 6.2% (MY 2026)" in text
    assert signals == []  # single year -> no tight-supply detection
