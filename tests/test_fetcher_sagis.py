"""Layer 23 — SAGIS weekly producer deliveries (fetchers/sagis.py) end to end.

Covers the four things that can silently break this layer:

  1. **Link resolution.** The DT-SWP URL is week-stamped, so a stale deep
     link keeps returning HTTP 200 with a frozen week. The listing page must
     be re-read every run and the *highest* (season, week) must win — not
     document order, and not the upload path's own date.
  2. **Week dating.** WeekEnd is a day-first range string. Reading it
     month-first would land 06/03/2026 in June, and dropping an unparseable
     week would read downstream as a week of zero deliveries.
  3. **Component preservation.** first_published / adjustments / week_total
     are all stored; the progressive total is derived at read time, never
     stored. Negative adjustments and zero-delivery weeks survive cleaning.
  4. **Pace comparison.** YoY is computed at the same *week number*, which
     is SAGIS's own convention — a season's week 1 can start in February or
     March, so date-matching would compare different points of the harvest.

Numbers below are verbatim from the live 2026-08-11 fetch of
`DT-SWP-Soybeans_2026_22.xlsx` (week 22, season 2026/27).
"""

from __future__ import annotations

import io

import pandas as pd
import pytest

from analysis.soy_analytics import _sagis_delivery_pace
from config import SAGIS_COMMODITIES
from fetchers.sagis import _parse_dt_export, _parse_week_end, _resolve_dt_links
from pipeline.clean import clean_sagis_deliveries
from pipeline.history import HISTORY_TABLES
from pipeline.query import read_sagis_deliveries
from pipeline.results import ScraperShapeError
from pipeline.store import save_sagis_deliveries

_UPLOADS = "https://www.sagis.org.za/wp-content/uploads"

# Verbatim rows from the live workbook, including week 1's negative
# adjustment (3350 delivered, −666 revision → 2684 total).
_LIVE_ROWS = [
    ("Soybeans", "Soybeans", 2026, "Active", 1, "28/02 - 06/03/2026", 3350, -666, 2684),
    ("Soybeans", "Soybeans", 2026, "Active", 2, "07/03 - 13/03/2026", 12723, 372, 13095),
    ("Soybeans", "Soybeans", 2026, "Active", 3, "14/03 - 20/03/2026", 20480, 1526, 22006),
    ("Soybeans", "Soybeans", 2025, "Final", 1, "01/03 - 07/03/2025", 1338, 0, 1338),
    ("Soybeans", "Soybeans", 2025, "Final", 2, "08/03 - 14/03/2025", 6718, 0, 6718),
    ("Soybeans", "Soybeans", 2025, "Final", 3, "15/03 - 21/03/2025", 6838, 0, 6838),
    # A later week of the prior season: must NOT count toward the same-week
    # comparison when the current season has only reached week 3.
    ("Soybeans", "Soybeans", 2025, "Final", 4, "22/03 - 28/03/2025", 47862, 0, 47862),
]

_COLUMNS = [
    "Commodity", "SubCereal", "SeasonYear", "SeasonStatus", "WeekNumber",
    "WeekEnd", "FirstPublished", "Adjustments", "AdjustedWeekTotal",
]


def _workbook(rows=None, columns=None) -> bytes:
    """Build a real .xlsx in memory so the parser reads through openpyxl."""
    frame = pd.DataFrame(rows if rows is not None else _LIVE_ROWS,
                         columns=columns or _COLUMNS)
    buf = io.BytesIO()
    frame.to_excel(buf, index=False, sheet_name="Soybeans")
    return buf.getvalue()


def _listing(*filenames: str) -> str:
    links = "".join(f'<a href="{_UPLOADS}/{name}">x</a>' for name in filenames)
    return f"<html><body>{links}</body></html>"


_FULL_LISTING = _listing(
    "2026/08/DT-SWP-Soybeans_2026_22.xlsx",
    "2026/08/DT-SWP-Sunflower_2026_22.xlsx",
)


# ── Link resolution ─────────────────────────────────────────────────────────

def test_resolves_newest_week_per_commodity():
    links = _resolve_dt_links(_FULL_LISTING)
    assert set(links) == set(SAGIS_COMMODITIES)
    assert links["Soybeans"].endswith("DT-SWP-Soybeans_2026_22.xlsx")


def test_newest_wins_over_document_order_and_upload_path():
    """Week 22 wins even though week 21 appears later and sits in a folder
    whose path sorts higher — the version is in the filename, not the URL."""
    listing = _listing(
        "2026/08/DT-SWP-Soybeans_2026_22.xlsx",
        "2026/09/DT-SWP-Soybeans_2026_21.xlsx",
        "2026/08/DT-SWP-Sunflower_2026_22.xlsx",
    )
    assert _resolve_dt_links(listing)["Soybeans"].endswith("_2026_22.xlsx")


def test_newer_season_beats_higher_week_number():
    """Season 2026 week 3 is newer than season 2025 week 52."""
    listing = _listing(
        "2025/08/DT-SWP-Soybeans_2025_52.xlsx",
        "2026/03/DT-SWP-Soybeans_2026_3.xlsx",
        "2026/03/DT-SWP-Sunflower_2026_3.xlsx",
    )
    assert _resolve_dt_links(listing)["Soybeans"].endswith("_2026_3.xlsx")


def test_untracked_commodities_are_ignored():
    """Maize and Wheat are published on the same page; we don't track them."""
    listing = _listing(
        "2026/08/DT-SWP-Maize_2026_14.xlsx",
        "2026/08/DT-SWP-Wheat_2026_44.xlsx",
        "2026/08/DT-SWP-Soybeans_2026_22.xlsx",
        "2026/08/DT-SWP-Sunflower_2026_22.xlsx",
    )
    assert set(_resolve_dt_links(listing)) == set(SAGIS_COMMODITIES)


def test_missing_commodity_link_hard_fails():
    """A tracked commodity with no export is a shape change, not an empty
    result — the file is published weekly without exception."""
    with pytest.raises(ScraperShapeError, match="Sunflower"):
        _resolve_dt_links(_listing("2026/08/DT-SWP-Soybeans_2026_22.xlsx"))


# ── Week dating ─────────────────────────────────────────────────────────────

def test_week_end_is_day_first():
    """06/03/2026 is 6 March, not 3 June."""
    assert _parse_week_end("28/02 - 06/03/2026") == "2026-03-06"


def test_week_end_spanning_the_new_year():
    """Season 2018 week 49 ends in 2019 — only the end half carries a year."""
    assert _parse_week_end("26/01 - 01/02/2019") == "2019-02-01"


@pytest.mark.parametrize("bad", ["", "week 22", "28/02 - 06/03", None])
def test_unparseable_week_end_hard_fails(bad):
    """Dropping the row instead would read downstream as a zero-delivery week."""
    with pytest.raises(ScraperShapeError):
        _parse_week_end(bad)


# ── Parsing ─────────────────────────────────────────────────────────────────

def test_parses_live_workbook_shape():
    out = _parse_dt_export(_workbook(), "Soybeans")

    assert list(out.columns) == [
        "commodity", "season_year", "season_status", "week_number", "week_end",
        "first_published", "adjustments", "week_total", "unit",
    ]
    assert set(out["commodity"]) == {"Soybeans (SAGIS)"}
    assert set(out["unit"]) == {"MT"}

    week1 = out[(out["season_year"] == 2026) & (out["week_number"] == 1)].iloc[0]
    assert week1["week_end"] == "2026-03-06"
    assert week1["first_published"] == 3350
    assert week1["adjustments"] == -666
    assert week1["week_total"] == 2684


def test_progressive_total_is_not_stored():
    """The source's Prog. Total is a running sum of week_total — derived, so
    Layer 23 keeps components only and recomputes it at read time."""
    out = _parse_dt_export(_workbook(), "Soybeans")
    assert not any("prog" in c.lower() for c in out.columns)


def test_missing_column_hard_fails():
    columns = [c for c in _COLUMNS if c != "Adjustments"]
    rows = [tuple(v for i, v in enumerate(r) if _COLUMNS[i] != "Adjustments")
            for r in _LIVE_ROWS]
    with pytest.raises(ScraperShapeError, match="Adjustments"):
        _parse_dt_export(_workbook(rows, columns), "Soybeans")


def test_empty_export_hard_fails():
    """The DT file always carries every season, so zero rows is never
    'nothing happened this week' — it is a broken source."""
    with pytest.raises(ScraperShapeError, match="no rows"):
        _parse_dt_export(_workbook([]), "Soybeans")


def test_unreadable_bytes_hard_fail():
    with pytest.raises(ScraperShapeError, match="unreadable"):
        _parse_dt_export(b"not an xlsx", "Soybeans")


# ── Cleaning ────────────────────────────────────────────────────────────────

def test_clean_keeps_zero_and_negative_components():
    """A zero-delivery week is real (the season is 52 weeks, the harvest is
    ~5 months), and adjustments are signed."""
    raw = _parse_dt_export(_workbook(), "Soybeans")
    raw.loc[0, "week_total"] = 0.0
    cleaned = clean_sagis_deliveries(raw)

    assert len(cleaned) == len(raw)
    assert (cleaned["adjustments"] < 0).any()
    assert (cleaned["week_total"] == 0).any()


def test_clean_sorts_by_season_then_week_and_dates_week_end():
    cleaned = clean_sagis_deliveries(_parse_dt_export(_workbook(), "Soybeans"))
    assert pd.api.types.is_datetime64_any_dtype(cleaned["week_end"])
    keys = list(zip(cleaned["season_year"], cleaned["week_number"], strict=True))
    assert keys == sorted(keys)


def test_clean_does_not_mutate_input():
    raw = _parse_dt_export(_workbook(), "Soybeans")
    before = raw.copy()
    clean_sagis_deliveries(raw)
    pd.testing.assert_frame_equal(raw, before)


# ── Storage round-trip ──────────────────────────────────────────────────────

def test_store_and_read_round_trip(patched_db):
    cleaned = clean_sagis_deliveries(_parse_dt_export(_workbook(), "Soybeans"))
    save_sagis_deliveries("Soybeans (SAGIS)", cleaned)

    out = read_sagis_deliveries("Soybeans (SAGIS)")
    assert len(out) == len(cleaned)
    week1 = out[(out["season_year"] == 2026) & (out["week_number"] == 1)].iloc[0]
    assert week1["first_published"] == 3350
    assert week1["adjustments"] == -666
    assert pd.Timestamp(week1["week_end"]).date().isoformat() == "2026-03-06"


def test_revisions_overwrite_rather_than_duplicate(patched_db):
    """SAGIS revises past weeks for months, including in closed seasons —
    the upsert key has to absorb that without forking the week."""
    cleaned = clean_sagis_deliveries(_parse_dt_export(_workbook(), "Soybeans"))
    save_sagis_deliveries("Soybeans (SAGIS)", cleaned)

    revised = cleaned.copy()
    revised.loc[revised["week_number"] == 1, "adjustments"] = 999.0
    revised.loc[revised["week_number"] == 1, "week_total"] = 4349.0
    save_sagis_deliveries("Soybeans (SAGIS)", revised)

    out = read_sagis_deliveries("Soybeans (SAGIS)")
    assert len(out) == len(cleaned)
    week1 = out[(out["season_year"] == 2026) & (out["week_number"] == 1)].iloc[0]
    assert week1["week_total"] == 4349.0


def test_registered_for_history_round_trip():
    """The DT export's season window is fixed-width; whatever rolls off is
    unrecoverable from an ephemeral CI database."""
    assert HISTORY_TABLES["sagis_deliveries"] == (
        "commodity", "season_year", "week_number",
    )


# ── Pace comparison ─────────────────────────────────────────────────────────

def test_pace_compares_at_the_same_week_number():
    """Current season is at week 3 (2684+13095+22006 = 37785). The prior
    season's weeks 1-3 total 14894 — its week 4 (47862) must be excluded,
    or the comparison would read −56% instead of +154%."""
    pace = _sagis_delivery_pace(
        clean_sagis_deliveries(_parse_dt_export(_workbook(), "Soybeans"))
    )

    assert pace["week_number"] == 3
    assert pace["progressive_mt"] == 37785
    assert pace["prev_season_progressive_mt"] == 14894
    assert pace["yoy_pct"] == pytest.approx(153.7, abs=0.1)


def test_pace_reports_the_latest_week_components():
    pace = _sagis_delivery_pace(
        clean_sagis_deliveries(_parse_dt_export(_workbook(), "Soybeans"))
    )
    assert pace["week_total_mt"] == 22006
    assert pace["week_first_published_mt"] == 20480
    assert pace["week_adjustments_mt"] == 1526
    assert pace["week_end"] == "2026-03-20"
    assert pace["season_label"] == "2026/2027"
    assert pace["season_status"] == "Active"


def test_pace_omits_three_year_average_without_three_prior_seasons():
    """Only 2025 precedes 2026 in the fixture — no partial average is shown."""
    pace = _sagis_delivery_pace(
        clean_sagis_deliveries(_parse_dt_export(_workbook(), "Soybeans"))
    )
    assert "vs_avg3_pct" not in pace


def test_pace_computes_three_year_average_when_available():
    rows = list(_LIVE_ROWS)
    for season, total in ((2024, 5000), (2023, 9000)):
        rows.append(
            ("Soybeans", "Soybeans", season, "Final", 1,
             f"01/03 - 07/03/{season}", total, 0, total)
        )
    pace = _sagis_delivery_pace(
        clean_sagis_deliveries(_parse_dt_export(_workbook(rows), "Soybeans"))
    )
    # 2023/2024/2025 weeks 1-3: 9000, 5000, 14894 → mean 9631.33
    assert pace["avg3_seasons"] == [2023, 2024, 2025]
    assert pace["avg3_progressive_mt"] == pytest.approx(9631.3, abs=0.1)
    assert pace["vs_avg3_pct"] == pytest.approx(292.3, abs=0.5)


def test_pace_empty_frame_returns_empty_dict():
    assert _sagis_delivery_pace(pd.DataFrame()) == {}
