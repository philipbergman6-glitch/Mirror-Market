"""Tests for the v2 snapshot extractor (analysis/briefing/snapshot.py).

Covers the schema contract the briefings archive depends on:
- schema_version marks v2 rows (absence = v1)
- blocks degrade to None/{} on empty data instead of raising
- seeded DB rows surface in the right blocks with plain JSON-native types
- json.dumps succeeds without default=str (no numpy scalars / Timestamps leak)
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from analysis.briefing.snapshot import SCHEMA_VERSION, build_snapshot
from analysis.briefing.types import BriefingData
from analysis.loaders import clear_loader_cache
from analysis.technical import compute_all_technicals


@pytest.fixture(autouse=True)
def _patch_health(patched_db: Path, monkeypatch: pytest.MonkeyPatch):
    """Point analysis.health at the temp DB (it connects directly)."""

    def _connect() -> sqlite3.Connection:
        return sqlite3.connect(str(patched_db))

    monkeypatch.setattr("analysis.health.get_connection", _connect)
    clear_loader_cache()
    yield
    clear_loader_cache()


def _empty_briefing() -> BriefingData:
    return BriefingData(text="")


def test_empty_db_yields_v2_skeleton_without_raising(patched_db: Path) -> None:
    snapshot = build_snapshot(_empty_briefing())

    assert snapshot["schema_version"] == SCHEMA_VERSION
    assert snapshot["prices"] == {}
    assert snapshot["crush_spread"] is None
    assert snapshot["brazil_basis"] is None
    assert snapshot["economic"] == {}
    assert snapshot["yield_curve"] is None
    assert snapshot["cot"] == {}
    assert snapshot["weather"] == {}
    assert snapshot["port_flows"] == {}
    assert snapshot["nass_crush"] is None
    # Whole thing must be JSON-native without default=str.
    json.dumps(snapshot)


def test_prices_block_reads_enriched_technicals(
    patched_db: Path, synthetic_ohlcv: pd.DataFrame
) -> None:
    enriched = {"Soybeans": compute_all_technicals(synthetic_ohlcv)}
    snapshot = build_snapshot(BriefingData(text="", enriched=enriched))

    row = snapshot["prices"]["Soybeans"]
    assert row["close"] == pytest.approx(float(synthetic_ohlcv["Close"].iloc[-1]))
    for key in ("rsi", "macd_histogram", "ma_200", "bb_width", "hv_20", "weekly_pct_change"):
        assert isinstance(row[key], float)
    # Soybeans converts cents/bu -> USD/MT.
    assert row["close_usd_mt"] == pytest.approx(row["close"] * 0.367437, rel=1e-3)
    json.dumps(snapshot)


def test_economic_and_yield_curve_blocks(patched_db: Path) -> None:
    conn = sqlite3.connect(str(patched_db))
    rows = [
        ("2026-07-27", "Treasury 2Y", 4.10),
        ("2026-07-28", "Treasury 2Y", 4.20),
        ("2026-07-27", "Treasury 10Y", 4.00),
        ("2026-07-28", "Treasury 10Y", 3.90),
        ("2026-07-27", "US Dollar Index", 100.0),
        ("2026-07-28", "US Dollar Index", 101.0),
    ]
    conn.executemany(
        "INSERT INTO economic (Date, series_name, value) VALUES (?, ?, ?)", rows
    )
    conn.commit()
    conn.close()

    snapshot = build_snapshot(_empty_briefing())

    dollar = snapshot["economic"]["US Dollar Index"]
    assert dollar["value"] == 101.0
    assert dollar["chg"] == pytest.approx(1.0)
    assert dollar["chg_pct"] == pytest.approx(1.0)
    assert dollar["date"] == "2026-07-28"

    curve = snapshot["yield_curve"]
    assert curve["t2y"] == 4.20
    assert curve["t10y"] == 3.90
    assert curve["spread"] == pytest.approx(-0.30)


def test_cot_block_includes_open_interest_and_zscores(patched_db: Path) -> None:
    conn = sqlite3.connect(str(patched_db))
    dates = pd.date_range("2024-01-02", periods=110, freq="W-TUE")
    rng = np.random.default_rng(seed=7)
    for i, d in enumerate(dates):
        conn.execute(
            "INSERT INTO cot (commodity, Date, commercial_net, noncommercial_net,"
            " total_open_interest) VALUES (?, ?, ?, ?, ?)",
            (
                "Soybeans",
                str(d.date()),
                float(rng.normal(-50_000, 20_000)),
                float(rng.normal(60_000, 25_000)) + (200_000 if i == len(dates) - 1 else 0),
                800_000 + i,
            ),
        )
    conn.commit()
    conn.close()

    snapshot = build_snapshot(_empty_briefing())
    row = snapshot["cot"]["Soybeans"]

    assert row["date"] == str(dates[-1].date())
    assert row["total_open_interest"] == 800_000 + len(dates) - 1
    # 109 weekly baseline obs >= 26 -> z-scores must exist; the spiked
    # spec net must register as strongly positive.
    assert row["commercial_z3y"] is not None
    assert row["noncommercial_z3y"] > 3
    json.dumps(snapshot)


def test_export_sales_block_aggregates_china_share(patched_db: Path) -> None:
    conn = sqlite3.connect(str(patched_db))
    rows = [
        ("Soybeans", "2026-07-23", "CHINA, PEOPLES REPUBLIC OF", 600_000.0, 300_000.0, 5e6, 2e6),
        ("Soybeans", "2026-07-23", "MEXICO", 300_000.0, 100_000.0, 3e6, 1e6),
        ("Soybeans", "2026-07-23", "JAPAN", 100_000.0, 50_000.0, 1e6, 5e5),
        # older week must be ignored
        ("Soybeans", "2026-07-16", "CHINA, PEOPLES REPUBLIC OF", 9e9, 9e9, 9e9, 9e9),
    ]
    conn.executemany(
        "INSERT INTO export_sales (commodity, week_ending, country, net_sales,"
        " weekly_exports, accumulated_exports, outstanding_sales)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()

    snapshot = build_snapshot(_empty_briefing())
    row = snapshot["export_sales"]["Soybeans"]

    assert row["week_ending"] == "2026-07-23"
    assert row["total_net_sales_mt"] == pytest.approx(1_000_000.0)
    assert row["china_net_sales_mt"] == pytest.approx(600_000.0)
    assert row["china_pct"] == pytest.approx(60.0)
    assert row["top_buyers"][0]["country"].startswith("CHINA")
    assert len(row["top_buyers"]) == 3


def test_stocks_to_use_block_labels_each_commodity_unit(patched_db: Path) -> None:
    """Archived levels carry PSD's unit — cotton is bales, soybeans 1000 MT.

    `ending_stocks`/`total_use` are levels, and this block is written to the
    briefings archive, so an unlabelled cotton row would sit beside five
    1000-MT rows forever with nothing marking the difference (#238).
    """
    conn = sqlite3.connect(str(patched_db))
    rows = [
        ("Soybeans", "United States", 2026, "Ending Stocks", 8_435.0, "(1000 MT)"),
        ("Soybeans", "United States", 2026, "Domestic Consumption", 66_000.0, "(1000 MT)"),
        ("Soybeans", "United States", 2026, "Exports", 50_000.0, "(1000 MT)"),
        ("Cotton", "United States", 2026, "Ending Stocks", 4_100.0,
         "(1000 480 lb. Bales)"),
        ("Cotton", "United States", 2026, "Domestic Use", 1_600.0,
         "(1000 480 lb. Bales)"),
        ("Cotton", "United States", 2026, "Exports", 12_300.0,
         "(1000 480 lb. Bales)"),
    ]
    conn.executemany(
        "INSERT INTO psd (commodity, country, year, attribute, value, unit)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()

    block = build_snapshot(_empty_briefing())["stocks_to_use"]

    assert block["Cotton"]["unit"] == "(1000 480 lb. Bales)"
    assert block["Soybeans"]["unit"] == "(1000 MT)"
    assert block["Cotton"]["ratio"] == pytest.approx(4_100.0 / 13_900.0)


def test_export_sales_block_soy_pace_fields(patched_db: Path) -> None:
    conn = sqlite3.connect(str(patched_db))
    conn.executemany(
        "INSERT INTO export_sales (commodity, week_ending, country, net_sales,"
        " weekly_exports, accumulated_exports, outstanding_sales)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("Soybeans", "2026-07-23", "MEXICO", 2e5, 1e5, 40e6, 10e6),
            ("Soybeans", "2025-07-24", "MEXICO", 1.0, 1.0, 38e6, 1e6),
            ("Corn", "2026-07-23", "MEXICO", 2e5, 1e5, 40e6, 10e6),
        ],
    )
    conn.execute(
        "INSERT INTO wasde (commodity, year, attribute, value, unit, reference_period)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        ("SOYBEANS", "2025/26", "Exports", 1660.0, "Million Bushels", "2026-07-15"),
    )
    conn.commit()
    conn.close()

    snapshot = build_snapshot(_empty_briefing())
    row = snapshot["export_sales"]["Soybeans"]

    assert row["marketing_year"] == "2025/26"
    assert row["wasde_export_forecast_raw"] == 1660.0
    assert row["wasde_export_forecast_unit"] == "Million Bushels"
    assert row["wasde_export_forecast_mt"] == pytest.approx(1660.0 * 1e6 / 36.7437)
    assert row["yr_ago_week_ending"] == "2025-07-24"
    assert row["yr_ago_accumulated_exports_mt"] == pytest.approx(38e6)
    # commitments % is derived downstream, never stored
    assert "commitments_pct" not in row
    # non-soybean rows don't get pace fields
    assert "marketing_year" not in snapshot["export_sales"]["Corn"]
    json.dumps(snapshot)


def test_port_flows_block_stores_region_subtotals(patched_db: Path) -> None:
    conn = sqlite3.connect(str(patched_db))
    conn.executemany(
        "INSERT INTO inspection_port_flows (week_ending, region, port_area,"
        " commodity, inspections_mt) VALUES (?, ?, ?, ?, ?)",
        [
            ("2026-07-24", "GULF", "SUBTOTAL", "Soybeans", 600_000.0),
            ("2026-07-24", "PACIFIC", "SUBTOTAL", "Soybeans", 300_000.0),
            # port-level rows must not leak into the block
            ("2026-07-24", "GULF", "MISSISSIPPI RIVER", "Soybeans", 400_000.0),
            ("2026-07-17", "GULF", "SUBTOTAL", "Soybeans", 500_000.0),
        ],
    )
    conn.commit()
    conn.close()

    snapshot = build_snapshot(_empty_briefing())
    row = snapshot["port_flows"]["Soybeans"]

    assert row["week_ending"] == "2026-07-24"
    assert row["regions_mt"] == {"GULF": 600_000.0, "PACIFIC": 300_000.0}
    assert row["prev_week_ending"] == "2026-07-17"
    assert row["prev_regions_mt"] == {"GULF": 500_000.0}
    json.dumps(snapshot)


def test_nass_crush_block_and_usda_block_exclusion(patched_db: Path) -> None:
    conn = sqlite3.connect(str(patched_db))
    conn.executemany(
        "INSERT INTO usda (stat_category, year, short_desc, Value, unit_desc,"
        " state_name, reference_period_desc) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("CRUSHED", "2026", "SOYBEANS - CRUSHED, MEASURED IN TONS",
             "6,143,000", "TONS", "US TOTAL", "MAY"),
            ("CRUSHED", "2025", "SOYBEANS - CRUSHED, MEASURED IN TONS",
             "5,850,000", "TONS", "US TOTAL", "MAY"),
        ],
    )
    conn.commit()
    conn.close()

    snapshot = build_snapshot(_empty_briefing())

    row = snapshot["nass_crush"]
    assert row["value"] == 6_143_000.0
    assert row["unit"] == "TONS"
    assert row["year"] == 2026
    assert row["month"] == 5
    assert row["yoy_pct"] == pytest.approx(5.0085, rel=1e-3)
    # monthly crush rows must not pollute the annual usda block
    assert snapshot["usda"] == {}
    json.dumps(snapshot)


def test_psd_highlights_include_argentina(patched_db: Path) -> None:
    conn = sqlite3.connect(str(patched_db))
    conn.executemany(
        "INSERT INTO psd (commodity, country, year, attribute, value, unit)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("Soybean Meal", "Argentina", 2026, "Exports", 29_400.0, "(1000 MT)"),
            ("Soybean Oil", "Argentina", 2026, "Exports", 6_650.0, "(1000 MT)"),
            ("Soybeans", "Argentina", 2026, "Production", 52_000.0, "(1000 MT)"),
        ],
    )
    conn.commit()
    conn.close()

    snapshot = build_snapshot(_empty_briefing())
    highlights = snapshot["psd_highlights"]

    assert highlights["argentina_meal_exports"]["value"] == 29_400.0
    assert highlights["argentina_oil_exports"]["value"] == 6_650.0
    assert highlights["argentina_soy_production"]["year"] == 2026


def test_wasde_block_keeps_string_marketing_years(patched_db: Path) -> None:
    conn = sqlite3.connect(str(patched_db))
    conn.executemany(
        "INSERT INTO wasde (commodity, year, attribute, value, unit, reference_period)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("SOYBEANS", "2025/26", "Ending Stocks", 300.0, "Million Bushels", "2026-06-15"),
            ("SOYBEANS", "2025/26", "Ending Stocks", 320.0, "Million Bushels", "2026-07-15"),
            ("SOYBEANS", "2024/25", "Ending Stocks", 999.0, "Million Bushels", "2026-07-15"),
        ],
    )
    conn.commit()
    conn.close()

    snapshot = build_snapshot(_empty_briefing())
    row = snapshot["wasde"]["SOYBEANS"]["Ending Stocks"]

    # Latest marketing year pinned ("2025/26" > "2024/25"), MoM revision vs June.
    assert row["marketing_year"] == "2025/26"
    assert row["value"] == 320.0
    assert row["mom_revision"] == pytest.approx(20.0)


def test_conab_block_stores_legs_not_gap(patched_db: Path) -> None:
    conn = sqlite3.connect(str(patched_db))
    conn.executemany(
        "INSERT INTO brazil_estimates (source, commodity, crop_year, attribute,"
        " value, unit, report_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("CONAB", "Soybeans", "2025/26", "Production", 170_000.0, "1000 t", "2026-07-10"),
            ("CONAB", "Soybeans", "2025/26", "Area", 48_000.0, "1000 ha", "2026-07-10"),
        ],
    )
    conn.execute(
        "INSERT INTO psd (commodity, country, attribute, year, value, unit)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        ("Soybeans", "Brazil", "Production", 2026, 169_000.0, "1000 MT"),
    )
    conn.commit()
    conn.close()

    snapshot = build_snapshot(_empty_briefing())
    row = snapshot["conab"]["Soybeans"]

    assert row["crop_year"] == "2025/26"
    assert row["attributes"]["Production"] == {"value": 170_000.0, "unit": "1000 t"}
    assert row["usda_production"]["value"] == 169_000.0
    assert row["usda_production"]["unit"] == "1000 MT"
    assert "gap" not in row


def test_currencies_block_uses_session_changes(patched_db: Path) -> None:
    idx = pd.date_range("2026-06-01", periods=30, freq="B")
    closes = pd.Series(np.linspace(5.0, 5.29, 30), index=idx)
    fx = pd.DataFrame({"Close": closes})
    data = BriefingData(text="", currency_data={"BRL/USD": fx})

    snapshot = build_snapshot(data)
    row = snapshot["currencies"]["BRL/USD"]

    assert row["close"] == pytest.approx(5.29)
    expected_5d = (5.29 - closes.iloc[-6]) / closes.iloc[-6] * 100
    expected_21d = (5.29 - closes.iloc[-22]) / closes.iloc[-22] * 100
    assert row["chg_5d_pct"] == pytest.approx(expected_5d)
    assert row["chg_21d_pct"] == pytest.approx(expected_21d)


def test_snapshot_is_fully_json_native(patched_db: Path, synthetic_ohlcv: pd.DataFrame) -> None:
    """numpy scalars and Timestamps must never leak into the snapshot."""
    conn = sqlite3.connect(str(patched_db))
    conn.execute(
        "INSERT INTO weather (region, Date, precipitation, temp_max, temp_min)"
        " VALUES (?, ?, ?, ?, ?)",
        ("US Midwest", "2026-07-28", 25.0, 39.0, 22.0),
    )
    conn.commit()
    conn.close()

    enriched = {"Soybeans": compute_all_technicals(synthetic_ohlcv)}
    snapshot = build_snapshot(
        BriefingData(text="", enriched=enriched, price_data={"Soybeans": synthetic_ohlcv})
    )

    def walk(node):
        if isinstance(node, dict):
            for k, v in node.items():
                assert isinstance(k, str)
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)
        else:
            assert node is None or isinstance(node, (bool, int, float, str)), repr(node)

    walk(snapshot)
    json.dumps(snapshot)
