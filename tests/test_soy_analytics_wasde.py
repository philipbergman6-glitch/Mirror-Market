"""Regression test for the dashboard WASDE marketing-year pin.

WASDE releases carry multiple marketing years per report (e.g. 2024/25 Est.
and 2025/26 Proj. in the same July file). Before the pin, supply_analysis()
sorted only by reference_period, so DB insertion order decided which MY row
surfaced — showing old-crop planted acres and computing a cross-year
"revision" (1,892 − 1,520 = +372 Mbu) presented as a MoM change.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from analysis.soy_analytics import supply_analysis


def test_supply_analysis_pins_wasde_to_latest_marketing_year(patched_db: Path) -> None:
    conn = sqlite3.connect(str(patched_db))
    # Old-crop rows inserted LAST so a reference_period-only stable sort
    # would surface them (the original bug's tie-break order).
    conn.executemany(
        "INSERT INTO wasde (commodity, year, attribute, value, unit, reference_period)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("SOYBEANS", "2025/26", "Exports", 1_530.0, "Million Bushels", "2026-06-15"),
            ("SOYBEANS", "2025/26", "Exports", 1_520.0, "Million Bushels", "2026-07-15"),
            ("SOYBEANS", "2024/25", "Exports", 1_882.0, "Million Bushels", "2026-06-15"),
            ("SOYBEANS", "2024/25", "Exports", 1_892.0, "Million Bushels", "2026-07-15"),
        ],
    )
    conn.commit()
    conn.close()

    exports = supply_analysis()["wasde"]["SOYBEANS"]["Exports"]

    assert exports["marketing_year"] == "2025/26"
    assert exports["value"] == 1_520.0
    # MoM within the pinned MY — never the +372 cross-year artifact.
    assert exports["revision"] == pytest.approx(-10.0)
    assert exports["prev_value"] == 1_530.0
