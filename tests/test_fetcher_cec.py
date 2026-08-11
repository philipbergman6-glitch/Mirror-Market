"""Layer 25 — CEC South Africa crop estimates (fetchers/cec.py) end to end.

The fixtures are the real releases, stripped to the pages carrying the
summary table, and every number asserted below is verbatim from the source.
They were chosen to pin the five shapes this layer has to survive:

  1. **The standard forecast** (July 2026, 6th) — area, current forecast and
     previous forecast, the shape 7 of 11 releases a season take.
  2. **The first forecast** (February 2026) — no previous-forecast column,
     *and* a units row that labels the tons column "Ha". A parser trusting
     the units row stores hectares as production here.
  3. **The area-only releases** (January's preliminary area, October's
     intentions) — no production number exists, which must store as NULL and
     never as zero. October also carries two seasons in one release.
  4. **The CELC final crop** (February 2026, for the 2025 crop) — a
     different table on a different page, whose figure supersedes November's
     final estimate and is the one USDA later carries.
  5. **A release whose declared change formula is wrong** (February 2025
     heads its column "(B) ÷ (D)" and prints "(A) ÷ (C)").

The change-formula and yield checks are the parse's own guardrails, so they
are tested by breaking a row rather than only by parsing a good one.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from analysis.soy_analytics import _cec_estimate_view
from config import CEC_HISTORY_START, CEC_PSD_YEAR_OFFSET
from fetchers.cec import (
    _check_change,
    _check_levels,
    _parse_release_date,
    _parse_report,
    _resolve_report_links,
)
from pipeline.query import read_cec_estimates
from pipeline.results import ScraperShapeError
from pipeline.store import save_cec_estimates

FIXTURES = Path(__file__).parent / "fixtures"
_UPLOADS = "https://www.sagis.org.za/wp-content/uploads"


def _records(fixture: str, url: str | None = None) -> list[dict]:
    raw = (FIXTURES / fixture).read_bytes()
    return _parse_report(raw, url or f"{_UPLOADS}/2026/07/{fixture}")


def _by_crop(records: list[dict], crop: str, season: int) -> dict:
    matches = [r for r in records if r["commodity"] == crop and r["season_year"] == season]
    assert len(matches) == 1, f"expected one {crop} row for {season}, got {matches}"
    return matches[0]


# ── The standard forecast ────────────────────────────────────────────────────

def test_standard_forecast_parses_area_and_production():
    """July 2026: 1 212 700 ha, 3 043 825 t soybeans — the 6th forecast."""
    records = _records("cec_2026-07-28_summary.pdf")
    soy = _by_crop(records, "Soybeans (CEC)", 2026)

    assert soy["release_date"] == "2026-07-28"
    assert soy["area_ha"] == 1_212_700
    assert soy["production_t"] == 3_043_825
    assert soy["estimate_kind"] == "forecast"
    assert soy["forecast_seq"] == 6


def test_sunflower_rides_along_on_the_same_table():
    """One extra crop off the same page — no extra request, own key."""
    sun = _by_crop(_records("cec_2026-07-28_summary.pdf"), "Sunflower Seed (CEC)", 2026)
    assert (sun["area_ha"], sun["production_t"]) == (570_100, 874_805)


def test_previous_forecast_column_is_not_stored_as_this_release():
    """The 5th forecast sits beside the 6th on the same row.

    Both are the same season and both are production columns; the leftmost
    (current) one must win. In this release they happen to be equal, so the
    row is checked against the *previous* release's own stored value instead.
    """
    july = _by_crop(_records("cec_2026-07-28_summary.pdf"), "Soybeans (CEC)", 2026)
    february = _by_crop(
        _records("cec_2026-02-26_first_forecast.pdf"), "Soybeans (CEC)", 2026
    )
    # February's 1st forecast, not July's prior-year column (2 800 000).
    assert february["production_t"] == 2_661_425
    assert july["production_t"] == 3_043_825


# ── The header rows the source gets wrong ────────────────────────────────────

def test_units_row_is_ignored_where_it_contradicts_the_label():
    """February 2026 heads its tons column "Ha"; the label says "1st forecast".

    2 661 425 is tons of soybeans, and 1 212 500 is hectares. Reading the
    units row would swap them and imply a 0.46 t/ha crop.
    """
    soy = _by_crop(
        _records("cec_2026-02-26_first_forecast.pdf"), "Soybeans (CEC)", 2026
    )
    assert soy["area_ha"] == 1_212_500
    assert soy["production_t"] == 2_661_425
    assert soy["production_t"] / soy["area_ha"] == pytest.approx(2.19, abs=0.01)


def test_declared_change_formula_may_be_wrong_without_failing(caplog):
    """February 2025 declares (B) ÷ (D) over a column printing (A) ÷ (C).

    The check exists to prove the row was split correctly, so it falls back
    to any pair that reproduces the printed figure — and says so in the log
    rather than swallowing the discrepancy.
    """
    with caplog.at_level(logging.WARNING):
        records = _records("cec_2025-02-27_wrong_formula.pdf")

    soy = _by_crop(records, "Soybeans (CEC)", 2025)
    assert (soy["area_ha"], soy["production_t"]) == (1_151_000, 2_325_225)
    assert "heads its change column" in caplog.text


# ── Area-only releases ───────────────────────────────────────────────────────

def test_preliminary_area_release_stores_no_production():
    """January carries hectares only — NULL production, never zero.

    Its second column is *intentions*, also in hectares and also for the
    same season, so a positional read would store 1 179 200 ha as this
    release's area and the prior season's 2 771 225 t as its production.
    """
    soy = _by_crop(
        _records("cec_2026-01-27_preliminary_area.pdf"), "Soybeans (CEC)", 2026
    )
    assert soy["estimate_kind"] == "preliminary_area"
    assert soy["area_ha"] == 1_185_000
    assert soy["production_t"] is None


def test_october_release_carries_two_seasons():
    """Intentions for the next crop *and* the 9th forecast for this one."""
    records = _records("cec_2025-10-28_intentions.pdf")

    intentions = _by_crop(records, "Soybeans (CEC)", 2026)
    assert intentions["estimate_kind"] == "intentions"
    assert intentions["area_ha"] == 1_179_200
    assert intentions["production_t"] is None

    ninth = _by_crop(records, "Soybeans (CEC)", 2025)
    assert (ninth["forecast_seq"], ninth["production_t"]) == (9, 2_753_125)
    # Same release date on both — the PK is (commodity, season, release).
    assert intentions["release_date"] == ninth["release_date"] == "2025-10-28"


# ── The CELC final crop ──────────────────────────────────────────────────────

def test_celc_final_crop_supersedes_the_november_estimate():
    """2 800 000 t, not November's 2 771 225 — and it is what USDA carries."""
    soy = _by_crop(_records("cec_2026-02-12_final_crop.pdf"), "Soybeans (CEC)", 2025)
    assert soy["estimate_kind"] == "final_crop"
    assert soy["production_t"] == 2_800_000
    assert soy["area_ha"] == 1_151_000


def test_final_crop_release_ignores_the_deliveries_breakdown_below_it():
    """The same page's second table lists the same crops in tons.

    Its soybean row (2 800 000 total = 2 709 892 SAGIS deliveries + 43 608
    projected + 46 500 on-farm) would parse as four plausible columns.
    """
    records = _records("cec_2026-02-12_final_crop.pdf")
    assert len(records) == 2  # one per crop, not two per crop


# ── Guardrails ───────────────────────────────────────────────────────────────

def test_change_check_rejects_a_row_no_pair_explains():
    """The printed change must be reproducible from the parsed numbers."""
    with pytest.raises(ScraperShapeError, match="no pair of the parsed row"):
        _check_change(
            "Soybeans (CEC)", 2026, "sixth forecast",
            values=[1_212_700, 3_043_825, 2_800_000],
            change=-4.95,
            letters=["A", "B", "C"],
            formula=("B", "C"),
        )


def test_yield_band_rejects_an_impossible_crop():
    """A hectares column read as tons implies a yield nothing grows."""
    with pytest.raises(ScraperShapeError, match="t/ha"):
        _check_levels(
            "Soybeans (CEC)", 2026, "sixth forecast",
            area=1_212_700, production=25_000_000,
        )


def test_a_release_that_no_longer_parses_raises():
    """Not-a-PDF is a hard failure, never an empty result."""
    with pytest.raises(ScraperShapeError, match="unreadable"):
        _parse_report(b"<html>maintenance</html>", f"{_UPLOADS}/2026/07/CEC.pdf")


# ── Listing page and dating ──────────────────────────────────────────────────

_LISTING = f"""
<a href="{_UPLOADS}/2026/07/CEC_2026-07-28.pdf">July</a>
<a href="{_UPLOADS}/2026/07/CEC_2026-07-28.pdf">July again</a>
<a href="{_UPLOADS}/2025/09/CEC_2025-09.pdf">September, no day</a>
<a href="{_UPLOADS}/2025/07/CEC-28-Aug-2025.pdf">August</a>
<a href="{_UPLOADS}/2026/05/CEC-2024-12.doc">legacy .doc</a>
<a href="{_UPLOADS}/2025/06/01/CEC-Aug-2020.pdf">pre-window</a>
<a href="{_UPLOADS}/2026/01/CEC_Dates_2026.pdf">release calendar</a>
"""


def test_listing_takes_every_in_window_pdf_and_nothing_else():
    """Four filename conventions, two formats, one release calendar."""
    links = _resolve_report_links(_LISTING)

    assert links == sorted({
        f"{_UPLOADS}/2026/07/CEC_2026-07-28.pdf",
        f"{_UPLOADS}/2025/09/CEC_2025-09.pdf",
        f"{_UPLOADS}/2025/07/CEC-28-Aug-2025.pdf",
    })
    assert date(2025, 1, 1) == CEC_HISTORY_START


def test_empty_listing_is_a_shape_error_not_an_empty_result():
    with pytest.raises(ScraperShapeError, match="no report PDF"):
        _resolve_report_links("<html>nothing here</html>")


def test_release_date_comes_from_the_body_not_the_filename(caplog):
    """The August 2025 release is uploaded as the 28th and dated the 27th."""
    text = "SUMMER CROPS\n27 August / Augustus 2025\n"
    with caplog.at_level(logging.WARNING):
        stamped = _parse_release_date(text, f"{_UPLOADS}/2025/07/CEC-28-Aug-2025.pdf")

    assert stamped == "2025-08-27"
    assert not caplog.text  # one day apart is the upload lag, not an error


def test_a_wildly_misnamed_release_is_logged(caplog):
    with caplog.at_level(logging.WARNING):
        stamped = _parse_release_date(
            "12 February 2026\n", f"{_UPLOADS}/2026/07/CEC_2026-07-28.pdf"
        )
    assert stamped == "2026-02-12"
    assert "using the body" in caplog.text


def test_an_undated_release_raises():
    with pytest.raises(ScraperShapeError, match="no release date"):
        _parse_release_date("no date anywhere", f"{_UPLOADS}/x/CEC_2025-09.pdf")


# ── Storage ──────────────────────────────────────────────────────────────────

def test_store_round_trip_and_revision_upsert(patched_db):
    """Each release is its own row; re-reading a release rewrites it."""
    frame = pd.DataFrame(_records("cec_2026-07-28_summary.pdf"))
    frame["unit"] = "MT"
    soy = frame[frame["commodity"] == "Soybeans (CEC)"]

    save_cec_estimates("Soybeans (CEC)", soy)
    save_cec_estimates("Soybeans (CEC)", soy)  # same release, re-read

    stored = read_cec_estimates("Soybeans (CEC)")
    assert len(stored) == 1
    assert stored.iloc[0]["production_t"] == 3_043_825

    june = soy.copy()
    june["release_date"] = "2026-06-25"
    june["production_t"] = 3_043_825
    june["forecast_seq"] = 5
    save_cec_estimates("Soybeans (CEC)", june)
    assert len(read_cec_estimates("Soybeans (CEC)")) == 2  # revisions accumulate


# ── The analyst view ─────────────────────────────────────────────────────────

def _view_frame() -> pd.DataFrame:
    """A season's path plus the prior season's two competing finals."""
    return pd.DataFrame([
        # Prior season: November's estimate, then the CELC recalculation.
        {"commodity": "Soybeans (CEC)", "season_year": 2025,
         "release_date": "2025-11-27", "estimate_kind": "final_estimate",
         "forecast_seq": None, "forecast_label": "Final estimate",
         "area_ha": 1_151_000, "production_t": 2_771_225},
        {"commodity": "Soybeans (CEC)", "season_year": 2025,
         "release_date": "2026-02-12", "estimate_kind": "final_crop",
         "forecast_seq": None, "forecast_label": "Final crop",
         "area_ha": 1_151_000, "production_t": 2_800_000},
        # Current season: an area-only release, then two forecasts.
        {"commodity": "Soybeans (CEC)", "season_year": 2026,
         "release_date": "2026-01-27", "estimate_kind": "preliminary_area",
         "forecast_seq": None, "forecast_label": "Preliminary area",
         "area_ha": 1_185_000, "production_t": None},
        {"commodity": "Soybeans (CEC)", "season_year": 2026,
         "release_date": "2026-06-25", "estimate_kind": "forecast",
         "forecast_seq": 5, "forecast_label": "Fifth forecast",
         "area_ha": 1_212_700, "production_t": 2_911_200},
        {"commodity": "Soybeans (CEC)", "season_year": 2026,
         "release_date": "2026-07-28", "estimate_kind": "forecast",
         "forecast_seq": 6, "forecast_label": "Sixth forecast",
         "area_ha": 1_212_700, "production_t": 3_043_825},
    ])


def test_view_reports_the_revision_and_the_year_on_year_crop():
    view = _cec_estimate_view(_view_frame(), None)

    assert view["production_t"] == 3_043_825
    assert view["yield_t_ha"] == pytest.approx(2.51, abs=0.01)
    # Revision skips January's area-only release and steps forecast→forecast.
    assert view["prev_release_date"] == "2026-06-25"
    assert view["revision_pct"] == pytest.approx(4.56, abs=0.01)
    # YoY is against the CELC final, not November's superseded estimate.
    assert view["prior_season_kind"] == "final_crop"
    assert view["prior_season_t"] == 2_800_000
    assert view["yoy_pct"] == pytest.approx(8.7, abs=0.1)


def test_view_maps_psd_a_year_back_and_scales_it():
    """PSD keys the 2026 crop to 2025 and publishes in 1000 MT."""
    psd = pd.DataFrame([
        {"country": "South Africa", "attribute": "Production", "year": 2025,
         "value": 3050.0, "unit": "(1000 MT)"},
        {"country": "South Africa", "attribute": "Production", "year": 2026,
         "value": 2800.0, "unit": "(1000 MT)"},
        {"country": "Brazil", "attribute": "Production", "year": 2025,
         "value": 175000.0, "unit": "(1000 MT)"},
    ])
    view = _cec_estimate_view(_view_frame(), psd)

    assert CEC_PSD_YEAR_OFFSET == -1
    assert view["usda_psd_year"] == 2025
    assert view["usda_psd_t"] == 3_050_000
    assert view["vs_usda_pct"] == pytest.approx(-0.2, abs=0.05)


def test_view_of_an_area_only_season_reports_no_production():
    """Between October and February the season has hectares and nothing else."""
    frame = _view_frame()
    view = _cec_estimate_view(frame[frame["release_date"] <= "2026-01-27"], None)

    assert view["season_year"] == 2026
    assert view["area_ha"] == 1_185_000
    assert view["production_t"] is None
    assert "yoy_pct" not in view
    assert "revision_pct" not in view


def test_view_of_an_empty_frame_is_empty():
    assert _cec_estimate_view(pd.DataFrame(), None) == {}
