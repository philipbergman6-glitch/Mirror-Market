"""Tests for fetchers/wasde.py — parser and backfill loop."""

from __future__ import annotations

from datetime import date

import pandas as pd

from fetchers import wasde as wasde_mod
from fetchers.wasde import (
    _assign_reference_periods,
    _build_url,
    _clean_label,
    _coerce_float,
    _find_my_header,
    _find_section_start,
    _months_to_attempt,
    _parse_section,
    _previous_month_iso,
    fetch_wasde_estimates,
)

# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_build_url_pads_two_digit_month_and_year():
    assert _build_url(2026, 4) == "https://www.usda.gov/oce/commodity/wasde/wasde0426.xls"
    assert _build_url(2025, 11) == "https://www.usda.gov/oce/commodity/wasde/wasde1125.xls"


def test_months_to_attempt_walks_backwards_across_year_boundary():
    months = _months_to_attempt(4, date(2026, 2, 15))
    assert months == [(2026, 2), (2026, 1), (2025, 12), (2025, 11)]


def test_clean_label_strips_footnote_marker_and_whitespace():
    assert _clean_label("  Avg. Farm Price ($/bu)  2/") == "Avg. Farm Price ($/bu)"
    assert _clean_label("   Supply, Total") == "Supply, Total"
    assert _clean_label("Production") == "Production"


def test_coerce_float_handles_commas_and_blank():
    assert _coerce_float("4,262") == 4262.0
    assert _coerce_float(53.0) == 53.0
    assert _coerce_float(None) is None
    assert _coerce_float("") is None
    assert _coerce_float("n/a") is None


def test_assign_reference_periods_splits_duplicate_marketing_year_into_two_snapshots():
    # Col 3 + col 4 both labelled "2025/26" → the leftmost gets the previous
    # month's reference period so a single file already yields a MoM-comparable
    # pair downstream.
    out = _assign_reference_periods(
        {1: "2023/24", 2: "2024/25", 3: "2025/26", 4: "2025/26"},
        report_date="2026-04-15",
    )
    assert out == {
        1: ("2023/24", "2026-04-15"),
        2: ("2024/25", "2026-04-15"),
        3: ("2025/26", "2026-03-15"),
        4: ("2025/26", "2026-04-15"),
    }


def test_previous_month_iso_handles_january_rollover():
    assert _previous_month_iso("2026-01-15") == "2025-12-15"
    assert _previous_month_iso("2026-04-15") == "2026-03-15"


# ---------------------------------------------------------------------------
# Section parsing (hand-crafted DataFrame mimics one WASDE sheet)
# ---------------------------------------------------------------------------


def _fake_soybean_sheet() -> pd.DataFrame:
    """A 5-column DataFrame mirroring Page 15's SOYBEANS sub-table layout."""
    rows = [
        ["April 2026",  None,           None,            None,           None],
        [None,          None,           None,            None,           None],
        [None,          None,           "WASDE - 670 - 15", None,         None],
        [None,          None,           None,            None,           None],
        ["U.S. Soybeans and Products Supply and Use", None, None, None, None],
        [None,          None,           None,            None,           None],
        [None,          None,           None,            None,           None],
        [None,          None,           None,            None,           None],
        ["SOYBEANS",    "2023/24",      "2024/25 Est.",  "2025/26 Proj.","2025/26 Proj."],
        [None,          None,           None,            "Mar",          "Apr"],
        ["Filler",      "Filler",       "Filler",        "Filler",       "Filler"],
        [None,          None,           None,            "Million Acres",None],
        ["Area Planted", 83.6,          87.3,            81.2,           81.2],
        ["Area Harvested", 82.3,        86.2,            80.4,           80.4],
        [None,          None,           None,            "Bushels",      None],
        ["Yield per Harvested Acre", 50.6, 50.7,         53.0,           53.0],
        [None,          None,           None,            "Million Bushels", None],
        ["Beginning Stocks", 264,       342,             325,            325],
        ["Production",  4162,           4374,            4262,           4262],
        ["Ending Stocks", 342,          325,             350,            350],
        ["Avg. Farm Price ($/bu)  2/", 12.4, 10.0,       10.2,           10.3],
        # Sentinel: next sub-section starts here
        ["SOYBEAN OIL", "2023/24", "2024/25 Est.", "2025/26 Proj.", "2025/26 Proj."],
        ["Production",  27093,          29218,           29920,          30330],
        ["Note: Totals may not add", None, None, None,               None],
    ]
    return pd.DataFrame(rows)


def test_parse_section_extracts_all_attributes_and_marketing_years():
    df = _fake_soybean_sheet()
    rows = _parse_section(
        df,
        commodity="SOYBEANS",
        header_text="SOYBEANS",
        stop_labels={"SOYBEAN OIL", "SOYBEAN MEAL"},
        report_date="2026-04-15",
    )

    # 7 attributes × 4 MY columns (the current-MY duplicate splits into Mar/Apr
    # reference periods) = 28 rows.
    assert len(rows) == 28

    # Spot-check the current marketing year — both Mar and Apr snapshots present.
    current_my = [
        r for r in rows
        if r["statisticcat_desc"] == "Production" and r["year"] == "2025/26"
    ]
    assert len(current_my) == 2
    by_ref = {r["reference_period_desc"]: r["Value"] for r in current_my}
    assert by_ref == {"2026-03-15": 4262.0, "2026-04-15": 4262.0}

    # Historical MYs have one snapshot each.
    historical = [
        r for r in rows
        if r["statisticcat_desc"] == "Production" and r["year"] in ("2023/24", "2024/25")
    ]
    assert {r["year"] for r in historical} == {"2023/24", "2024/25"}
    assert all(r["reference_period_desc"] == "2026-04-15" for r in historical)

    # Cross-attribute unit spot-checks.
    by_attr = {(r["statisticcat_desc"], r["year"], r["reference_period_desc"]): r for r in rows}
    assert by_attr[("Area Planted", "2025/26", "2026-04-15")]["unit_desc"] == "Million Acres"
    assert by_attr[("Yield per Harvested Acre", "2025/26", "2026-04-15")]["unit_desc"] == "Bushels"
    assert by_attr[("Production", "2025/26", "2026-04-15")]["unit_desc"] == "Million Bushels"
    assert by_attr[("Avg. Farm Price ($/bu)", "2025/26", "2026-04-15")]["Value"] == 10.3

    assert all(r["commodity_desc"] == "SOYBEANS" for r in rows)


def test_parse_section_stops_at_next_subsection_header():
    df = _fake_soybean_sheet()
    rows = _parse_section(
        df,
        commodity="SOYBEANS",
        header_text="SOYBEANS",
        stop_labels={"SOYBEAN OIL"},
        report_date="2026-04-15",
    )
    # Without the stop, the SOYBEAN OIL "Production" row would also appear,
    # which would corrupt SOYBEANS's Production series.
    soybean_production = [r for r in rows if r["statisticcat_desc"] == "Production"]
    # 4 column-snapshots × 1 attribute, no leakage from SOYBEAN OIL.
    assert len(soybean_production) == 4
    assert max(r["Value"] for r in soybean_production) == 4374.0  # 2024/25 Est. value


def test_find_section_start_returns_none_when_header_missing():
    df = _fake_soybean_sheet()
    assert _find_section_start(df, "NONEXISTENT") is None


def test_find_section_start_returns_zero_for_whole_sheet_sections():
    df = _fake_soybean_sheet()
    # When header_text is None (e.g. Wheat or Cotton pages), we begin at row 0
    # and rely on the MY-header scanner to find the real start.
    assert _find_section_start(df, None) == 0


def test_find_my_header_locates_columns_with_marketing_years():
    df = _fake_soybean_sheet()
    header_idx, my_cols = _find_my_header(df, start_row=8)
    assert header_idx == 8
    # All four MY columns survive — duplicates are split into distinct
    # reference periods later by _assign_reference_periods.
    assert my_cols == {1: "2023/24", 2: "2024/25", 3: "2025/26", 4: "2025/26"}


# ---------------------------------------------------------------------------
# End-to-end fetcher with mocked download
# ---------------------------------------------------------------------------


def test_fetch_wasde_estimates_skips_unpublished_months(monkeypatch):
    """If every download returns None (no published files), we get an empty dict
    without raising."""
    monkeypatch.setattr(wasde_mod, "_download", lambda y, m: None)
    monkeypatch.setattr(wasde_mod, "_esmis_xls_index", lambda: {})

    result = fetch_wasde_estimates(backfill_months=3, today=date(2026, 5, 5))
    assert result == {}


def test_fetch_wasde_estimates_accumulates_rows_across_months(monkeypatch):
    """Two months parse successfully → keys hold rows from both reports."""
    download_calls: list[tuple[int, int]] = []

    def fake_download(year: int, month: int) -> bytes | None:
        download_calls.append((year, month))
        # Sentinel-byte: only the date matters; we mock _parse_xls below.
        return f"{year}-{month}".encode()

    def fake_parse_xls(content: bytes, report_date: str) -> list[dict]:
        # One row per call so we can verify accumulation.
        return [{
            "commodity_desc": "SOYBEANS",
            "statisticcat_desc": "Production",
            "year": "2025/26",
            "Value": 4262.0 + len(content),
            "unit_desc": "Million Bushels",
            "reference_period_desc": report_date,
        }]

    monkeypatch.setattr(wasde_mod, "_download", fake_download)
    monkeypatch.setattr(wasde_mod, "_parse_xls", fake_parse_xls)

    result = fetch_wasde_estimates(backfill_months=2, today=date(2026, 4, 15))

    assert download_calls == [(2026, 4), (2026, 3)]
    assert "SOYBEANS/Production" in result
    df = result["SOYBEANS/Production"]
    assert list(df["reference_period_desc"]) == ["2026-04-15", "2026-03-15"]


def test_fetch_wasde_estimates_keeps_going_when_one_parse_fails(monkeypatch, caplog):
    """A poison XLS in the middle of the backfill loop must not abort the rest."""
    monkeypatch.setattr(wasde_mod, "_download", lambda y, m: b"x")

    def flaky_parse(content: bytes, report_date: str) -> list[dict]:
        if report_date == "2026-03-15":
            raise ValueError("simulated corrupt XLS")
        return [{
            "commodity_desc": "WHEAT",
            "statisticcat_desc": "Production",
            "year": "2025/26",
            "Value": 1985.0,
            "unit_desc": "Million Bushels",
            "reference_period_desc": report_date,
        }]

    monkeypatch.setattr(wasde_mod, "_parse_xls", flaky_parse)

    result = fetch_wasde_estimates(backfill_months=3, today=date(2026, 4, 15))

    # April and February survive; March was dropped on parse failure.
    df = result["WHEAT/Production"]
    assert set(df["reference_period_desc"]) == {"2026-04-15", "2026-02-15"}


# ---------------------------------------------------------------------------
# ESMIS archive fallback (usda.gov 404s months older than ~4 months)
# ---------------------------------------------------------------------------


def test_fetch_wasde_estimates_falls_back_to_esmis_for_404_months(monkeypatch):
    """Months missing from usda.gov are re-fetched via their exact ESMIS URL;
    the index is fetched once, and months absent from it are skipped."""
    index_calls: list[bool] = []
    fallback_urls: list[str] = []

    def fake_direct(year: int, month: int) -> bytes | None:
        # Only the newest month survives on usda.gov.
        return b"direct" if (year, month) == (2026, 4) else None

    def fake_index() -> dict[tuple[int, int], str]:
        index_calls.append(True)
        # 2026-02 absent — mimics a cancelled release (e.g. Oct 2025 shutdown).
        return {(2026, 3): "https://esmis.example/wasde0326v2.xls"}

    def fake_download_url(url: str, year: int, month: int) -> bytes | None:
        fallback_urls.append(url)
        return b"esmis"

    parsed_dates: list[str] = []

    def fake_parse_xls(content: bytes, report_date: str) -> list[dict]:
        parsed_dates.append(report_date)
        return [{
            "commodity_desc": "CORN",
            "statisticcat_desc": "Production",
            "year": "2025/26",
            "Value": 15000.0,
            "unit_desc": "Million Bushels",
            "reference_period_desc": report_date,
        }]

    monkeypatch.setattr(wasde_mod, "_download", fake_direct)
    monkeypatch.setattr(wasde_mod, "_esmis_xls_index", fake_index)
    monkeypatch.setattr(wasde_mod, "_download_url", fake_download_url)
    monkeypatch.setattr(wasde_mod, "_parse_xls", fake_parse_xls)

    result = fetch_wasde_estimates(backfill_months=3, today=date(2026, 4, 15))

    assert index_calls == [True]  # fetched once despite two misses
    assert fallback_urls == ["https://esmis.example/wasde0326v2.xls"]
    assert parsed_dates == ["2026-04-15", "2026-03-15"]  # Feb skipped
    assert "CORN/Production" in result


def test_esmis_xls_index_maps_release_month_to_xls_url(monkeypatch):
    """The index keys on release_datetime month and picks the .xls file —
    including v2 reissue filenames the URL template cannot construct."""
    payload = {
        "results": [
            {
                "release_datetime": "2026-06-11T12:00:00+0000",
                "files": [
                    "https://esmis.example/f/wasde0626v2.pdf",
                    "https://esmis.example/f/wasde0626v2.xls",
                ],
            },
            {
                "release_datetime": "2025-08-12T12:00:00+0000",
                "files": ["https://esmis.example/f/wasde0825.xls"],
            },
            # No .xls at all → excluded from the index.
            {
                "release_datetime": "2025-07-11T12:00:00+0000",
                "files": ["https://esmis.example/f/wasde0725.pdf"],
            },
            # Malformed date → excluded.
            {"release_datetime": "", "files": ["https://esmis.example/f/x.xls"]},
        ]
    }

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json() -> dict:
            return payload

    monkeypatch.setattr(
        wasde_mod.requests, "get", lambda url, timeout: FakeResponse()
    )

    index = wasde_mod._esmis_xls_index()
    assert index == {
        (2026, 6): "https://esmis.example/f/wasde0626v2.xls",
        (2025, 8): "https://esmis.example/f/wasde0825.xls",
    }


def test_esmis_xls_index_returns_empty_on_http_error(monkeypatch):
    class FakeResponse:
        status_code = 503

        @staticmethod
        def json() -> dict:
            return {}

    monkeypatch.setattr(
        wasde_mod.requests, "get", lambda url, timeout: FakeResponse()
    )
    monkeypatch.setattr(wasde_mod, "retry_sleep", lambda attempt: None)

    assert wasde_mod._esmis_xls_index() == {}
