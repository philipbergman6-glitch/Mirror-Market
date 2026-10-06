"""Layer 32 — SEA India weekly comparative rates (#72).

The sheets below are synthetic: SEA's layout (section headings, label
spelling, the seven-column current / % / week-ago / % / month-ago / % /
year-average line, Indian digit grouping) with invented numbers. SEA's own
PDFs are all-rights-reserved and are not committed to this public repo.
"""

from __future__ import annotations

from datetime import date

import pytest

from fetchers.sea import RateSheet, fetch_sea_rates, parse_rate_sheet
from pipeline.results import ScraperShapeError


def _line(n: int, label: str, cur: str, wk: str, mo: str, yr: str) -> str:
    """One sheet line with SEA's own % columns computed from the values."""
    def pct(a: str, b: str) -> str:
        if a == "NQ" or b == "NQ":
            return "--"
        x, y = float(a.replace(",", "")), float(b.replace(",", ""))
        return f"{(x / y - 1) * 100:.2f}"
    return f"{n}.  {label} {cur} {pct(cur, wk)} {wk} {pct(cur, mo)} {mo} {pct(cur, yr)} {yr}"


def _sheet(
    as_on: str = "1st Oct 2026",
    *,
    fas: str = "520",
    cif: str = "1,300",
    se_oil: str = "1,34,000",
    override: dict[str, str] | None = None,
) -> str:
    lines = {
        "seed": _line(2, "Soyabean seed (Indore)", "55,000", "55,000", "59,000", "44,350"),
        "meal_indore": _line(4, "  Soya Ext.( Ex-Indore) 48/2.5", "49,000", "46,000", "47,000", "32,271"),
        "fas": _line(1, "Soyabean Ext(Bulk)Yellow (Ex-Kandla)48/2.5", fas, "515", "530", "398"),
        "for": _line(1, "Soyabean Ext.(Bulk)Yellow(Ex-Kandla) 48/2.5", "49,000", "48,500", "49,500", "33,354"),
        "cif": _line(5, "Soya Degum Oil(Crude) CIF Mumbai", cif, "1,305", "1,310", "1,182"),
        "ex_mumbai": _line(2, "Crude Degummed Soybean Oil (Ex-Mumbai)", "1,36,000", "1,35,500", "1,42,000", "1,21,325"),
        "se_oil": _line(1, "  SE Soyabean Oil (Indore)", se_oil, "1,33,500", "1,39,500", "1,17,396"),
        "refined": _line(3, "Refined Soyabean Oil", "1,41,500", "1,42,500", "1,49,000", "1,26,813"),
    }
    lines.update(override or {})
    return "\n".join([
        "1st CHANGE 25th CHANGE 1st CHANGE AVERAGE",
        "Oct. '26  % Sep. '26 % Sep. '26 % Oct'25",
        "I. OILSEEDS (Rs./M.T) Ex-Mandi",
        _line(1, "Groundnut seed Kernel (Saurashtra) Crushing Quality", "75,000", "75,000", "75,000", "50,083"),
        lines["seed"],
        "IV. EXTRACTIONS",
        "(A) LOCAL EX-MILL (Rs./MT) O & A/S & S",
        _line(1, "  Groundnut Ext. (Ex-Saurashtra) 45/2.5", "38,000", "38,000", "36,700", "22,042"),
        "3.   Kardi Ext.(Ex-Maharashtra) 20/2.5 NQ -- NQ -- NQ -- NQ",
        lines["meal_indore"],
        "(B)  EXPORT (FAS) (US$ / MT)",
        lines["fas"],
        _line(2, " Rapeseed Ext. (Bulk) (Ex-Kandla)38/2.5", "253", "253", "253", "198"),
        "(C)  EXPORT (FOR) Ports (Rs./MT)",
        lines["for"],
        "V.  INTERNATIONAL OILS(US$/M.T)    ",
        _line(4, "Crude Palm Oil(CPO)C&F Mumbai", "1,245", "1,255", "1,300", "1,164"),
        lines["cif"],
        "COMPARATIVE  RATE AS REGISTERED AS ON",
        f"{as_on},  A WEEK BEFORE; A MONTH & ONE YEAR BEFORE",
        "THE SOLVENT EXTRACTORS' ASSOCIATION OF INDIA",
        "Cont..2",
        "VI.LOCAL RATE FOR DOMESTIC & IMPORTED OILS (Rs./M.T.) Oct. '26  % Sep. '26 % Sep. '26 % Oct'25",
        " (b) Imported Oils (Rs./M.T.)",
        _line(1, "RBD Palmolein", "1,41,000", "1,46,500", "1,50,000", "1,26,775"),
        lines["ex_mumbai"],
        "VII. SOLVENT EXTRACTED OILS (Rs./MT.)",
        lines["se_oil"],
        "VIII.  REFINED OIL (Excl.ST) (Rs./MT)",
        _line(1, "SE Refined Cottonseed Oil", "1,51,000", "1,57,000", "1,68,000", "1,32,375"),
        lines["refined"],
    ])


# --- the rate sheet -------------------------------------------------------


def test_a_sheet_yields_every_series_at_its_printed_date_and_unit() -> None:
    sheet = parse_rate_sheet(_sheet())

    assert sheet == RateSheet(
        as_on=date(2026, 10, 1),
        values={
            "Soybean Indore": 55_000.0,
            "Soybean Meal Ex-Indore": 49_000.0,
            "Soybean Meal FAS Kandla": 520.0,
            "Soybean Meal FOR Kandla": 49_000.0,
            "Soybean Oil CIF Mumbai": 1_300.0,
            "Soybean Oil Ex-Mumbai": 136_000.0,
            "Soybean Oil SE Indore": 134_000.0,
            "Soybean Oil Refined": 141_500.0,
        },
    )


def test_a_quote_marked_nq_is_absent_never_zero() -> None:
    nq = "5.  Soya Degum Oil(Crude) CIF Mumbai NQ -- 1,305 -- 1,310 -- 1,182"

    sheet = parse_rate_sheet(_sheet(override={"cif": nq}))

    assert "Soybean Oil CIF Mumbai" not in sheet.values
    assert sheet.values["Soybean Meal FAS Kandla"] == 520.0


def test_a_line_whose_printed_changes_do_not_reproduce_is_refused() -> None:
    """A misread digit group (1,34,000 read as 134) or a shifted column still
    looks like a number; SEA's own % columns are what catch it."""
    shifted = "1.   SE Soyabean Oil (Indore) 1,34,000 0.37 1,33,500 -3.94 1,39,500 99.99 1,17,396"

    with pytest.raises(ScraperShapeError, match="SE Indore"):
        parse_rate_sheet(_sheet(override={"se_oil": shifted}))


def test_a_blank_current_column_is_refused() -> None:
    """The broken 26 Jun 2026 upload: no current value, every change -100.00."""
    blank = "1.  Soyabean Ext(Bulk)Yellow (Ex-Kandla)48/2.5 -100.00 607 -100.00 655 -100.00 389"

    with pytest.raises(ScraperShapeError, match="FAS Kandla"):
        parse_rate_sheet(_sheet(override={"fas": blank}))


def test_a_section_whose_unit_heading_changed_is_refused() -> None:
    """The heading states the unit, so a sheet that moved FAS to rupees fails
    instead of storing rupees as dollars."""
    text = _sheet().replace("(B)  EXPORT (FAS) (US$ / MT)", "(B)  EXPORT (FAS) (Rs./MT)")

    with pytest.raises(ScraperShapeError, match="FAS Kandla"):
        parse_rate_sheet(text)


def test_a_sheet_without_its_printed_date_is_refused() -> None:
    text = _sheet().replace("COMPARATIVE  RATE AS REGISTERED AS ON", "COMPARATIVE RATE")

    with pytest.raises(ScraperShapeError, match="AS ON"):
        parse_rate_sheet(text)


# --- the fetch --------------------------------------------------------------

_MEDIA = "https://seaofindia.com/wp-json/wp/v2/media"


class _Response:
    def __init__(self, status_code: int = 200, *, payload=None, content: bytes = b"") -> None:
        self.status_code = status_code
        self._payload = payload
        self.content = content

    def json(self):
        if self._payload is None:
            raise ValueError("not JSON")
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"HTTP {self.status_code}")


def _item(title: str, uploaded: str, name: str | None = None, mime: str = "application/pdf") -> dict:
    return {
        "date": f"{uploaded}T17:40:15",
        "mime_type": mime,
        "title": {"rendered": title},
        "source_url": f"https://seaofindia.com/wp-content/uploads/x/{name or title}.pdf",
    }


def _serve(monkeypatch, listing, pdfs: dict[str, str | int]) -> list[str]:
    """Stub the media listing and each PDF. A PDF is its sheet's text (the
    PDF→text step is stubbed with it) or an int HTTP status."""
    asked: list[str] = []

    def fake_get(url, params=None, headers=None, timeout=None):
        asked.append(url)
        if url == _MEDIA:
            return _Response(payload=listing)
        name = url.rsplit("/", 1)[1].removesuffix(".pdf")
        answer = pdfs[name]
        if isinstance(answer, int):
            return _Response(answer)
        return _Response(content=answer.encode())

    monkeypatch.setattr("fetchers.sea.requests.get", fake_get)
    monkeypatch.setattr("fetchers.sea.retry_sleep", lambda attempt: None)
    monkeypatch.setattr("fetchers.sea._pdf_text", lambda content: content.decode())
    return asked


def test_the_fetch_stores_each_sheet_under_its_printed_date(monkeypatch) -> None:
    """Identity is the date inside the sheet: DR250102 holds 2 Jan 2026."""
    _serve(
        monkeypatch,
        [_item("DR261001", "2026-10-01"), _item("DR250102", "2026-09-25")],
        {
            "DR261001": _sheet("1st Oct 2026"),
            "DR250102": _sheet("25th Sep 2026", fas="515", cif="1,305"),
        },
    )

    result = fetch_sea_rates(today=date(2026, 10, 6))

    assert result.status == "ok"
    fas = result.data["Soybean Meal FAS Kandla"]
    assert list(fas["Date"]) == ["2026-09-25", "2026-10-01"]
    assert list(fas["value"]) == [515.0, 520.0]
    assert set(fas["unit"]) == {"USD/MT"}
    assert set(result.data["Soybean Oil SE Indore"]["unit"]) == {"INR/MT"}
    assert set(result.data) == {
        "Soybean Indore", "Soybean Meal Ex-Indore", "Soybean Meal FAS Kandla",
        "Soybean Meal FOR Kandla", "Soybean Oil CIF Mumbai", "Soybean Oil Ex-Mumbai",
        "Soybean Oil SE Indore", "Soybean Oil Refined",
    }


def test_only_sheets_inside_the_lookback_and_with_a_dr_title_are_read(monkeypatch) -> None:
    asked = _serve(
        monkeypatch,
        [
            _item("DR261001", "2026-10-01"),
            _item("Dr. B. V Mehta- citation", "2026-09-30", name="citation", mime="image/png"),
            _item("DR-annual-report", "2026-09-29"),
            _item("DR260601", "2026-06-01"),
        ],
        {"DR261001": _sheet("1st Oct 2026")},
    )

    result = fetch_sea_rates(today=date(2026, 10, 6), lookback_days=30)

    assert result.status == "ok"
    assert [u for u in asked if u != _MEDIA] == [
        "https://seaofindia.com/wp-content/uploads/x/DR261001.pdf",
    ]


def test_a_broken_reupload_is_excused_by_its_good_twin(monkeypatch) -> None:
    """26 Jun 2026: one file blank in the current column, its '-1' re-upload
    correct. The good one is stored and the layer is not failed for the bad."""
    broken = _sheet("26th Jun 2026", override={
        "fas": "1.  Soyabean Ext(Bulk)Yellow (Ex-Kandla)48/2.5 -100.00 607 -100.00 655 -100.00 389",
    })
    _serve(
        monkeypatch,
        [
            _item("DR260626", "2026-06-29", name="DR260626-1"),
            _item("DR260626", "2026-06-29", name="DR260626"),
        ],
        {"DR260626-1": _sheet("26th Jun 2026"), "DR260626": broken},
    )

    result = fetch_sea_rates(today=date(2026, 6, 30))

    assert result.status == "ok"
    assert list(result.data["Soybean Meal FAS Kandla"]["Date"]) == ["2026-06-26"]


def test_reuploads_that_disagree_withhold_their_date(monkeypatch) -> None:
    _serve(
        monkeypatch,
        [
            _item("DR261001", "2026-10-02", name="DR261001-1"),
            _item("DR261001", "2026-10-01"),
            _item("DR260925", "2026-09-25"),
        ],
        {
            "DR261001-1": _sheet("1st Oct 2026", fas="525"),
            "DR261001": _sheet("1st Oct 2026"),
            "DR260925": _sheet("25th Sep 2026"),
        },
    )

    result = fetch_sea_rates(today=date(2026, 10, 6))

    assert result.status == "failed"
    assert "2026-10-01" in result.error
    assert list(result.data["Soybean Meal FAS Kandla"]["Date"]) == ["2026-09-25"]


def test_a_sheet_dated_far_from_its_upload_is_not_trusted(monkeypatch) -> None:
    """A DR-titled file whose printed date is weeks from its upload is a
    different document, or a stale one re-filed — never this week's sheet."""
    _serve(
        monkeypatch,
        [_item("DR261001", "2026-10-01"), _item("DR260925", "2026-09-25")],
        {"DR261001": _sheet("2nd Sep 2026"), "DR260925": _sheet("25th Sep 2026")},
    )

    result = fetch_sea_rates(today=date(2026, 10, 6))

    assert result.status == "failed"
    assert "DR261001" in result.error
    assert list(result.data["Soybean Meal FAS Kandla"]["Date"]) == ["2026-09-25"]


def test_an_unreadable_sheet_with_no_twin_fails_the_run_but_keeps_the_rest(monkeypatch) -> None:
    _serve(
        monkeypatch,
        [_item("DR261001", "2026-10-01"), _item("DR260925", "2026-09-25")],
        {"DR261001": "a different report entirely", "DR260925": _sheet("25th Sep 2026")},
    )

    result = fetch_sea_rates(today=date(2026, 10, 6))

    assert result.status == "failed"
    assert "DR261001" in result.error
    assert list(result.data["Soybean Meal FAS Kandla"]["Date"]) == ["2026-09-25"]


def test_a_pdf_that_will_not_download_fails_the_run_but_keeps_the_rest(monkeypatch) -> None:
    _serve(
        monkeypatch,
        [_item("DR261001", "2026-10-01"), _item("DR260925", "2026-09-25")],
        {"DR261001": 503, "DR260925": _sheet("25th Sep 2026")},
    )

    result = fetch_sea_rates(today=date(2026, 10, 6))

    assert result.status == "failed"
    assert list(result.data["Soybean Meal FAS Kandla"]["Date"]) == ["2026-09-25"]


@pytest.mark.parametrize("listing", [{"code": "rest_no_route"}, [], [_item("DR250101", "2025-01-01")]])
def test_a_listing_with_no_sheet_in_the_window_is_a_failure(monkeypatch, listing) -> None:
    """SEA has published every week since 2018; no sheet in the window means
    the listing broke, never a quiet week."""
    _serve(monkeypatch, listing, {})

    result = fetch_sea_rates(today=date(2026, 10, 6))

    assert result.status == "failed"
    assert not result.has_rows


# --- the privacy gate -------------------------------------------------------


def test_the_private_table_never_reaches_the_public_history_export() -> None:
    """SEA's sheets are all-rights-reserved and the repo is public: a
    committed CSV is a publication. Until SEA_PUBLISH is flipped (with SEA's
    written permission), the table must stay out of the export."""
    import config
    from pipeline.history import HISTORY_TABLES

    assert config.SEA_PUBLISH is False
    assert "sea_india_rates" not in HISTORY_TABLES
