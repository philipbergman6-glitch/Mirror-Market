"""Tests for the snapshot extractor (analysis/briefing/snapshot.py).

Covers the schema contract the briefings archive depends on:
- schema_version marks v2+ rows (absence = v1)
- blocks degrade to None/{} on empty data instead of raising
- seeded DB rows surface in the right blocks with plain JSON-native types
- json.dumps succeeds without default=str (no numpy scalars / Timestamps leak)
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from analysis.briefing.snapshot import SCHEMA_VERSION, build_snapshot
from analysis.briefing.types import BriefingData
from analysis.loaders import clear_loader_cache
from analysis.origins.sources import all_sailed_reason
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
    # The crush archives *why* it has no number rather than a bare None: on an
    # empty database "no curve" and "the section blew up" would otherwise look
    # identical six months later, and the whole point of naming the contracts
    # is that a stored crush can be interrogated.
    assert snapshot["crush_spread"] == {
        "withheld": "no_curve",
        "reason": snapshot["crush_spread"]["reason"],
    }
    assert "Soybeans" in snapshot["crush_spread"]["reason"]
    assert snapshot["brazil_basis"] is None
    assert snapshot["economic"] == {}
    assert snapshot["yield_curve"] is None
    assert snapshot["cot"] == {}
    assert snapshot["weather"] == {}
    assert snapshot["port_flows"] == {}
    assert snapshot["nass_crush"] is None
    # An empty table is no data, not a failure (#558): nothing raised.
    assert snapshot["failed_blocks"] == {}
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
        # PSD Market_Year 2025 is USDA's "2025/26" — CONAB's crop (#403).
        ("Soybeans", "Brazil", "Production", 2025, 169_000.0, "1000 MT"),
    )
    conn.execute(
        "INSERT INTO psd (commodity, country, attribute, year, value, unit)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        # The 2026/27 projection: newer, and a different crop — never paired.
        ("Soybeans", "Brazil", "Production", 2026, 186_000.0, "1000 MT"),
    )
    conn.commit()
    conn.close()

    snapshot = build_snapshot(_empty_briefing())
    row = snapshot["conab"]["Soybeans"]

    assert row["crop_year"] == "2025/26"
    assert row["usda_psd_year"] == 2025
    assert row["attributes"]["Production"] == {"value": 170_000.0, "unit": "1000 t"}
    assert row["usda_production"] == {"value": 169_000.0, "unit": "1000 MT", "year": 2025}
    assert "gap" not in row


def test_conab_block_withholds_usda_leg_when_psd_lacks_the_mapped_year(patched_db: Path) -> None:
    conn = sqlite3.connect(str(patched_db))
    conn.execute(
        "INSERT INTO brazil_estimates (source, commodity, crop_year, attribute,"
        " value, unit, report_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("CONAB", "Soybeans", "2025/26", "Production", 170_000.0, "1000 t", "2026-07-10"),
    )
    conn.execute(
        "INSERT INTO psd (commodity, country, attribute, year, value, unit)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        ("Soybeans", "Brazil", "Production", 2026, 186_000.0, "1000 MT"),
    )
    conn.commit()
    conn.close()

    row = build_snapshot(_empty_briefing())["conab"]["Soybeans"]
    assert row["usda_psd_year"] == 2025
    assert row["usda_production"] is None


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


def test_conab_block_withholds_usda_leg_for_a_calendar_year_label(patched_db: Path) -> None:
    """CONAB wheat is labelled '2025', not '2025/26'. No PSD mapping exists for
    that convention, so the USDA leg is withheld with a reason — and the
    snapshot builds instead of raising (deploy 2026-10-07 regression)."""
    conn = sqlite3.connect(str(patched_db))
    conn.execute(
        "INSERT INTO brazil_estimates (source, commodity, crop_year, attribute,"
        " value, unit, report_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("CONAB", "Wheat", "2025", "Production", 7_900.0, "1000 t", "2026-09-11"),
    )
    conn.execute(
        "INSERT INTO psd (commodity, country, attribute, year, value, unit)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        ("Wheat", "Brazil", "Production", 2025, 8_000.0, "1000 MT"),
    )
    conn.commit()
    conn.close()

    row = build_snapshot(_empty_briefing())["conab"]["Wheat"]
    assert row["crop_year"] == "2025"
    assert row["usda_psd_year"] is None
    assert row["usda_production"] is None
    assert row["usda_reason"] == "calendar_year_label_unmapped"


# B40 follow-up #549: MAGyP prints two bands sharing a start month on one
# session (2026-09 and 2026-09..2026-10, both "Sep loading"). Keyed on
# ship_from alone, one price overwrote the other and the spot was whichever
# row sorted first — here the sailed August band.
_MAGYP_SESSION = [
    # Inserted out of order on purpose: the spot must not depend on row order.
    ("2026-09-15", "Soybeans", "12019000190C", "2026-11", "2027-07", 395.0),
    ("2026-09-15", "Soybeans", "12019000190C", "2026-09", "2026-10", 402.0),
    ("2026-09-15", "Soybeans", "12019000190C", "2026-08", "2026-08", 399.0),
    ("2026-09-15", "Soybeans", "12019000190C", "2026-09", "2026-09", 404.0),
    ("2026-09-14", "Soybeans", "12019000190C", "2026-09", "2026-09", 390.0),
]


def _seed_magyp(db: Path, rows) -> None:
    conn = sqlite3.connect(str(db))
    conn.executemany(
        "INSERT INTO argentina_fob (date, product, position, ship_from, ship_to, price_usd_mt)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()


def test_argentina_fob_curve_keeps_same_start_bands(patched_db: Path) -> None:
    _seed_magyp(patched_db, _MAGYP_SESSION)

    block = build_snapshot(_empty_briefing())["argentina_fob"]["Soybeans"]

    assert block["date"] == "2026-09-15"
    # Keyed on the whole band, in the circular's own form; both Sep bands kept.
    assert block["curve"] == {
        "2026-08": 399.0,
        "2026-09": 404.0,
        "2026-09..2026-10": 402.0,
        "2026-11..2027-07": 395.0,
    }
    json.dumps(block)


def test_argentina_fob_spot_follows_nearest_window_rule(patched_db: Path) -> None:
    _seed_magyp(patched_db, _MAGYP_SESSION)

    block = build_snapshot(_empty_briefing())["argentina_fob"]["Soybeans"]

    # Nearest open band (B30 #504): August has sailed by 2026-09-15; of the two
    # Sep-loading bands the earlier-ending one is the prompt cargo.
    assert block["spot_usd_mt"] == 404.0
    assert block["spot_ship_from"] == "2026-09"
    assert block["spot_ship_to"] == "2026-09"
    assert "withheld" not in block and "reason" not in block


def test_argentina_fob_all_sailed_session_archives_no_spot_and_its_reason(
    patched_db: Path,
) -> None:
    """B44 #560: the snapshot applies B41's rule (#550). A session whose every
    band had ended carries no spot — neither the latest-ending sailed band nor
    an older session's open one — and says why; the curve is kept whole
    because its bands are real prints. Withheld is not failed."""
    _seed_magyp(patched_db, [
        ("2026-09-15", "Soybeans", "12019000190C", "2026-07", "2026-07", 397.0),
        ("2026-09-15", "Soybeans", "12019000190C", "2026-08", "2026-08", 399.0),
        ("2026-09-14", "Soybeans", "12019000190C", "2026-09", "2026-09", 390.0),
    ])

    snapshot = build_snapshot(_empty_briefing())
    block = snapshot["argentina_fob"]["Soybeans"]

    assert block == {
        "date": "2026-09-15",
        "withheld": "spot",
        "reason": all_sailed_reason(date(2026, 9, 15)),
        "curve": {"2026-07": 397.0, "2026-08": 399.0},
    }
    assert snapshot["failed_blocks"] == {}
    json.dumps(block)


def test_argentina_fob_two_rows_for_one_band_withhold_only_that_product(
    patched_db: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """No survivor is chosen (#549), and the failure is the product's alone (#558):
    the other products still archive, and the row names what failed."""
    _seed_magyp(patched_db, [
        ("2026-09-15", "Soybeans", "12019000190C", "2026-09", "2026-09", 404.0),
        ("2026-09-15", "Soybeans", "12019000190X", "2026-09", "2026-09", 401.0),
        ("2026-09-15", "Soybean Oil", "15071000", "2026-09", "2026-09", 1050.0),
    ])

    with caplog.at_level("WARNING", logger="analysis.briefing.snapshot"):
        snapshot = build_snapshot(_empty_briefing())

    assert "Soybeans" not in snapshot["argentina_fob"]
    assert snapshot["argentina_fob"]["Soybean Oil"]["spot_usd_mt"] == 1050.0
    reason = snapshot["failed_blocks"]["argentina_fob.Soybeans"]
    assert "two rows for band 2026-09" in reason and reason.startswith("ValueError")
    assert any("argentina_fob.Soybeans" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# failed_blocks (#558) — a builder that raised is not a block with no data
# ---------------------------------------------------------------------------


def test_a_raising_builder_is_recorded_in_failed_blocks_and_warned(
    patched_db: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def boom() -> dict:
        raise RuntimeError("cot table unreadable")

    monkeypatch.setattr("analysis.briefing.snapshot._cot_block", boom)

    with caplog.at_level("WARNING", logger="analysis.briefing.snapshot"):
        snapshot = build_snapshot(_empty_briefing())

    # The block still degrades to its default, so the rest of the day archives…
    assert snapshot["cot"] == {}
    # …but the row now says it failed, which an empty block cannot.
    assert snapshot["failed_blocks"] == {"cot": "RuntimeError: cot table unreadable"}
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert any("cot" in r.getMessage() and "unreadable" in r.getMessage() for r in warnings)
    json.dumps(snapshot)


def test_a_nested_block_failure_is_recorded_under_its_dotted_name(
    patched_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(_df: pd.DataFrame) -> dict:
        raise ValueError("too few years")

    monkeypatch.setattr("analysis.briefing.snapshot.current_vs_seasonal", boom)
    data = BriefingData(text="", price_data={"Soybeans": pd.DataFrame({"Close": [1.0]})})

    snapshot = build_snapshot(data)

    # The parent block survives (empty); the failure is named under its child.
    assert snapshot["seasonal"] == {}
    assert snapshot["failed_blocks"] == {"seasonal.Soybeans": "ValueError: too few years"}


def test_failures_do_not_leak_between_snapshots(
    patched_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom() -> dict:
        raise RuntimeError("once")

    with monkeypatch.context() as patch:
        patch.setattr("analysis.briefing.snapshot._cot_block", boom)
        assert "cot" in build_snapshot(_empty_briefing())["failed_blocks"]

    assert build_snapshot(_empty_briefing())["failed_blocks"] == {}


@pytest.mark.parametrize("version", [3, 4, SCHEMA_VERSION])
def test_the_archive_reader_works_on_old_and_new_rows(
    patched_db: Path, version: int
) -> None:
    """v3 rows carry no ``failed_blocks``; v4+ rows do; a v5 Argentina product
    may carry ``withheld``/``reason`` and no spot (#560). Both archive readers
    (``scripts/backfill_brazil_spot`` and ``pipeline.query.read_briefing``)
    read every shape."""
    import importlib.util

    from pipeline.store import save_briefing

    snapshot: dict = {
        "schema_version": version,
        "brazil_basis": {
            "paranagua_fob": {"spot_usd_mt": 400.0, "brl_usd": 0.2},
            "cepea_parana": None,
        },
    }
    if version >= 4:
        snapshot["failed_blocks"] = {"brazil_basis.cepea_parana": "ValueError: x"}
    if version >= 5:
        snapshot["argentina_fob"] = {
            "Soybeans": {
                "date": "2026-09-15",
                "withheld": "spot",
                "reason": all_sailed_reason(date(2026, 9, 15)),
                "curve": {"2026-08": 399.0},
            }
        }
    save_briefing(briefing_date="2026-09-15", text="briefing", signals=[], snapshot=snapshot)

    spec = importlib.util.spec_from_file_location(
        "backfill_brazil_spot",
        Path(__file__).resolve().parent.parent / "scripts" / "backfill_brazil_spot.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.get_connection = lambda: sqlite3.connect(str(patched_db))  # type: ignore[attr-defined]

    assert module.backfill() == 1
    from pipeline import query

    archived = query.read_briefing("2026-09-15")
    assert archived is not None and archived["snapshot"] == snapshot


def test_a_nan_ship_to_withholds_the_product(
    patched_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A NaN end is unreadable, never a single-month band (#558): the snapshot
    withholds the product, as the FOB board withholds its leg."""
    fob = pd.DataFrame({
        "date": pd.to_datetime(["2026-09-15"]),
        "product": ["Soybeans"],
        "ship_from": ["2026-09"],
        "ship_to": [float("nan")],
        "price_usd_mt": [404.0],
    })
    monkeypatch.setattr("analysis.briefing.snapshot.read_argentina_fob", lambda: fob)

    snapshot = build_snapshot(_empty_briefing())

    assert snapshot["argentina_fob"] == {}
    assert "unreadable band" in snapshot["failed_blocks"]["argentina_fob.Soybeans"]


# ---------------------------------------------------------------------------
# failure_reason — what of an exception is archived and printed
# ---------------------------------------------------------------------------


def test_failure_reason_keeps_type_and_first_line() -> None:
    from analysis.briefing.failures import failure_reason

    exc = ValueError("two rows for band 2026-09\nTraceback detail\nmore")
    assert failure_reason(exc) == "ValueError: two rows for band 2026-09"
    assert failure_reason(RuntimeError()) == "RuntimeError"


def test_failure_reason_drops_everything_from_a_path_on() -> None:
    from analysis.briefing.failures import failure_reason

    for message, kept in [
        ("unable to open database file /Users/someone/data/mirror.db", "unable to open database file"),
        ("[Errno 2] No such file or directory: '/home/runner/work/x.csv'", "[Errno 2] No such file or directory"),
        ("cannot read ~/secrets/key.txt now", "cannot read"),
        ("cannot read ./data/raw.csv", "cannot read"),
        ("bad file C:\\Users\\me\\x.db", "bad file"),
    ]:
        assert failure_reason(OSError(message)) == f"OSError: {kept}"
    # A unit is not a path.
    assert failure_reason(ValueError("price 404 USD/MT out of range")) == (
        "ValueError: price 404 USD/MT out of range"
    )


def test_failure_reason_truncates_a_long_first_line() -> None:
    from analysis.briefing.failures import MAX_REASON_CHARS, failure_reason

    reason = failure_reason(ValueError("x" * 500))
    message = reason.removeprefix("ValueError: ")
    assert len(message) == MAX_REASON_CHARS and message.endswith("…")


def test_failed_blocks_archive_the_sanitised_reason(
    patched_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom() -> dict:
        raise sqlite3.OperationalError("unable to open database file /Users/someone/x.db")

    monkeypatch.setattr("analysis.briefing.snapshot._cot_block", boom)

    snapshot = build_snapshot(_empty_briefing())

    assert snapshot["failed_blocks"] == {"cot": "OperationalError: unable to open database file"}
