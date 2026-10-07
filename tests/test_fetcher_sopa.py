"""Layer 33 — SOPA all-India state-wise soybean crop estimate (private).

``sopa_2025.html`` is the live page captured 2026-10-07 (kharif 2025,
revised: 110.267 lakh t); ``sopa_2026_no_posts.html`` is the same page asked
for a year SOPA has not yet estimated. Both are verbatim, preamble included.
The failure cases below are derived from the real page in memory — nothing
is hand-authored.
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pytest

import config
from fetchers.sopa import CropEstimate, fetch_sopa_estimates, parse_crop_table
from pipeline.history import HISTORY_TABLES
from pipeline.results import ScraperShapeError

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def page() -> str:
    return (FIXTURES / "sopa_2025.html").read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def no_posts_page() -> str:
    return (FIXTURES / "sopa_2026_no_posts.html").read_text(encoding="utf-8")


# --- the parser --------------------------------------------------------------


def test_the_live_page_parses_to_states_and_an_all_india_total(page) -> None:
    estimate = parse_crop_table(page, 2025)

    assert isinstance(estimate, CropEstimate)
    assert estimate.crop_year == 2025
    by_state = {row.state: row for row in estimate.rows}
    assert by_state["Maharashtra"].area_lakh_ha == 44.683
    assert by_state["Maharashtra"].yield_kg_ha == 1169
    assert by_state["Maharashtra"].production_lakh_t == 52.229
    assert by_state["Madhya Pradesh"].production_lakh_t == 43.247
    total = by_state[config.SOPA_ALL_INDIA]
    assert (total.area_lakh_ha, total.yield_kg_ha, total.production_lakh_t) == (112.140, 983, 110.267)
    # Eight numbered states plus the total; the ~115 division and district
    # sub-rows are not stored.
    assert len(estimate.rows) == 9
    assert "Amravati Division" not in by_state and "Amravati" not in by_state


def test_a_year_sopa_has_not_estimated_is_no_publication_not_a_failure(no_posts_page) -> None:
    """Until the Soy Conclave (mid-October) the next kharif's page has no
    table and says so; that is SOPA not having published, not us failing."""
    assert parse_crop_table(no_posts_page, 2026) is None


def test_no_table_without_the_no_posts_marker_is_a_shape_error(page) -> None:
    """A page with neither the table nor SOPA's own 'found 0 posts' comment
    is a page we no longer understand — never a quiet no-publication."""
    stripped = re.sub(r"<table class=\"export-table\">.*?</table>", "", page, flags=re.S)
    assert "Found 0 posts" not in stripped

    with pytest.raises(ScraperShapeError, match="export-table"):
        parse_crop_table(stripped, 2025)


def test_the_header_must_name_the_year_asked_for(page) -> None:
    """The page is one URL per year; a header naming another year is the
    wrong table served under the right address."""
    with pytest.raises(ScraperShapeError, match="Kharif 2025"):
        parse_crop_table(page, 2024)


def test_renamed_columns_fail_the_parse(page) -> None:
    renamed = page.replace("Expected Yield", "Expected Yield (q/ha)")

    with pytest.raises(ScraperShapeError, match="Expected Yield"):
        parse_crop_table(renamed, 2025)


def test_the_units_are_inferred_so_each_row_must_reproduce_its_own_arithmetic(page) -> None:
    """SOPA prints no units. lakh ha × kg/ha / 1000 = lakh t is the inference,
    and every row has to satisfy it (to the printed rounding) or the whole
    table is refused — a column restated in quintals or hectares would still
    parse as numbers."""
    requoted = page.replace("<strong>52.229</strong>", "<strong>522.29</strong>", 1)
    assert requoted != page

    with pytest.raises(ScraperShapeError, match="Maharashtra"):
        parse_crop_table(requoted, 2025)


def test_the_states_must_add_up_to_the_total(page) -> None:
    """A state row dropped or doubled leaves a total that no longer
    reconciles; the sum is the shape check for the numbered rows."""
    without_rajasthan = re.sub(
        r"<tr class='state-row' data-state='Rajasthan'>.*?</tr>", "", page, count=1, flags=re.S,
    )
    assert without_rajasthan != page

    with pytest.raises(ScraperShapeError, match="sum"):
        parse_crop_table(without_rajasthan, 2025)


def test_a_blank_cell_is_never_a_zero(page) -> None:
    blanked = page.replace("<strong>43.247</strong>", "<strong></strong>", 1)
    assert blanked != page

    with pytest.raises(ScraperShapeError, match="Madhya Pradesh"):
        parse_crop_table(blanked, 2025)


# --- the fetch ---------------------------------------------------------------


class _Response:
    def __init__(self, status_code: int = 200, text: str = "") -> None:
        self.status_code = status_code
        self.text = text


def _serve(monkeypatch, pages: dict[int, str | int]) -> list[int]:
    asked: list[int] = []

    def fake_get(url, params=None, headers=None, timeout=None):
        assert url == config.SOPA_URL
        year = int(params["select_year"])
        asked.append(year)
        answer = pages[year]
        if isinstance(answer, int):
            return _Response(answer)
        return _Response(text=answer)

    monkeypatch.setattr("fetchers.sopa.requests.get", fake_get)
    monkeypatch.setattr("fetchers.sopa.retry_sleep", lambda attempt: None)
    return asked


def test_the_fetch_reads_the_current_and_previous_kharif(monkeypatch, page, no_posts_page) -> None:
    """In October 2026 the 2025 table is live (revised) and 2026 is still
    unpublished: rows for 2025 only, stamped with the day they were read."""
    asked = _serve(monkeypatch, {2025: page, 2026: no_posts_page})

    result = fetch_sopa_estimates(today=date(2026, 10, 7))

    assert asked == [2025, 2026]
    assert result.status == "ok"
    assert list(result.data) == ["kharif_2025"]
    frame = result.data["kharif_2025"]
    assert set(frame["crop_year"]) == {2025}
    assert set(frame["fetched_date"]) == {"2026-10-07"}
    assert list(frame.columns) == [
        "crop_year", "state", "fetched_date",
        "area_lakh_ha", "yield_kg_ha", "production_lakh_t",
    ]
    total = frame[frame["state"] == config.SOPA_ALL_INDIA].iloc[0]
    assert total["production_lakh_t"] == 110.267


def test_nothing_published_for_any_year_asked_is_empty_not_failed(monkeypatch, no_posts_page) -> None:
    _serve(monkeypatch, {2025: no_posts_page, 2026: no_posts_page})

    result = fetch_sopa_estimates(today=date(2026, 10, 7))

    assert result.status == "empty"
    assert "not yet published" in (result.error or "")


def test_a_year_that_will_not_download_fails_the_run_but_keeps_the_rest(monkeypatch, page) -> None:
    _serve(monkeypatch, {2025: page, 2026: 503})

    result = fetch_sopa_estimates(today=date(2026, 10, 7))

    assert result.status == "failed"
    assert list(result.data) == ["kharif_2025"]
    assert "2026" in (result.error or "")


def test_a_year_whose_table_will_not_parse_fails_the_run(monkeypatch, page) -> None:
    _serve(monkeypatch, {2025: page.replace("Expected Yield", "Yield"), 2026: page})

    result = fetch_sopa_estimates(today=date(2026, 10, 7))

    assert result.status == "failed"
    assert "2025" in (result.error or "")


def test_the_private_table_never_reaches_the_public_history_export() -> None:
    """SOPA reserves all rights not otherwise claimed and the repo is public:
    a committed CSV is a publication. Only derived figures (a YoY change,
    state shares) and at most one attributed headline total may be shown,
    and the raw table stays out of data/history/ until SOPA_PUBLISH flips."""
    assert config.SOPA_PUBLISH is False
    assert "sopa_crop_estimates" not in HISTORY_TABLES
