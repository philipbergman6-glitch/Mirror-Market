"""Layer 24 — SAGIS monthly soybean supply & demand, end to end.

Covers the five things that can silently break this layer:

  1. **Link resolution.** The season workbook is re-published every month
     under a new filename and only three seasons are ever listed, so a
     hardcoded link would serve a frozen vintage at HTTP 200 — the World
     Bank CMO trap (Layer 8) and the DT-SWP trap (Layer 23) again.
  2. **Unreported months.** The workbook prints months that have not been
     reported yet as a hard `0`, not as a blank. Storing those zeros would
     publish a fabricated collapse in crush, trade and stocks every time a
     season opens. The report's own `SMD-MMYYYY` tag is what cuts the frame.
  3. **Section anchoring.** The sheet repeats "Oil and oilcake", "Animal
     feed", "Opening stock" and "Total Processed for commercial use" in more
     than one section with *different* numbers. A row read from the wrong
     section still parses — it is just the wrong number — so the parser
     checks the report's own balance, (a)+(b)−(c)−(d)−(e) = (f).
  4. **Label drift.** Line-item wording changes between vintages
     ("(i+j)" → "(i+j): (iv)", "Opening stock" → "Openening stock").
  5. **Same-position comparison.** Season-to-date pace is compared at the
     same *month position in the season*, never against a full prior season.

Numbers below are verbatim from the live 2026-08-12 fetch of
`Sojabone20262027_2026-07-24.xlsx` (March and April 2026, season 2026/27),
and they balance — which is what makes them usable as a parser fixture.
"""

from __future__ import annotations

import io

import pandas as pd
import pytest

from analysis.soy_analytics import _sagis_smd_pace
from config import LAYER_MAX_DATA_AGE_DAYS, SAGIS_SMD_COMMODITIES
from fetchers.sagis import _parse_smd_workbook, _resolve_smd_links
from pipeline.clean import clean_sagis_smd
from pipeline.history import HISTORY_TABLES
from pipeline.query import read_sagis_supply_demand
from pipeline.results import ScraperShapeError
from pipeline.store import save_sagis_smd

_UPLOADS = "https://www.sagis.org.za/wp-content/uploads"

# (label column, label text) → (March 2026, April 2026), verbatim.
_LIVE_ROWS: list[tuple[int, str, float, float]] = [
    (0, "(a) Opening Stock", 286120, 151888),
    (0, "(b) Acquisition", 72397, 493033),
    (1, "Deliveries directly from farms", 72397, 493033),
    (1, "Imports destined for RSA", 0, 0),
    (0, "(c) Utilisation", 181915, 199488),
    (1, "Total Processed for commercial use (i+j): (iv)", 181912, 199347),
    (2, "Human consumption (i)", 1920, 1785),
    (2, "Animal feed (ii)", 8951, 8646),
    (2, "Oil and oilcake (iii)", 171041, 188916),
    (1, "Withdrawn by producers", 0, 0),
    (1, "Released to end-consumer(s)", 3, 1),
    (1, "Seed for planting purposes", 0, 140),
    (0, "(d) RSA Exports (3)", 28344, 22562),
    (1, "Whole soybeans", 28344, 22562),
    (2, "Border posts", 28344, 19051),
    (2, "Harbours", 0, 3511),
    (0, "(e) Sundries", -3630, -1112),
    (1, "Net dispatches(+)/Receipts(-)", -233, -41),
    (1, "Surplus(-)/Deficit(+)", -3397, -1071),
    (0, "(f) Unutilised stock (a+b-c-d-e)", 151888, 423983),
    (0, "(g) Stock stored at:(4)", 151888, 423983),
    (1, "Storers and Traders", 95982, 367143),
    (1, "Processors", 55906, 56840),
    # (h) is a transit block explicitly excluded from everything above, and
    # it repeats "Opening stock"/"Closing stock" with different numbers.
    (0, "(h) Imports destined for exports not included in the above information", 0, 0),
    (1, "Opening stock", 111, 222),
    (1, "Imported", 0, 0),
    (1, "Exported", 0, 0),
    (1, "Closing stock", 333, 444),
    (0, "Total Processed for commercial use (i+j): (iv)", 181912, 199347),
    # (i) repeats the (c) sub-labels with *local market only* numbers.
    (0, "(i) Soybeans Processed for the local market ", 158783, 188476),
    (2, "Human consumption (i)", 1920, 1785),
    (2, "Animal feed (ii)", 8951, 8646),
    (2, "Oil and oilcake (iii)", 147912, 178045),
    (0, "(j) Soybeans Equivalent of products exported  ", 23129, 10871),
    (2, "African countries", 11307, 10871),
    (2, "Other countries", 11822, 0),
]

_MONTH_HEADERS = [
    "Mar/Mrt 2026", "Apr 2026", "May/Mei 2026", "Jun 2026", "Jul 2026",
    "Aug 2026", "Sep 2026", "Oct/Okt 2026", "Nov 2026", "Dec/Des 2026",
    "Jan 2027", "Feb 2027",
]


def _workbook(
    rows: list[tuple[int, str, float, float]] | None = None,
    *,
    tag: str | None = "SMD-052026",
    months: list[str] | None = None,
) -> bytes:
    """Build a season SMD workbook in memory, in the live sheet geometry.

    Labels sit in columns 0–2 by indent level, the twelve months run across
    columns 3–14, and column 15 is the progressive total. Months after the
    reported one are written as `0`, exactly as SAGIS writes them.
    """
    grid: list[list] = [[None] * 17 for _ in range(10)]
    grid[0][3] = "SOYBEANS/SOJABONE"
    if tag is not None:
        grid[2][16] = tag
    for offset, header in enumerate(months or _MONTH_HEADERS):
        grid[7][3 + offset] = header
    grid[7][15] = "Mar/Mrt - Apr 2026"

    for column, label, march, april in (rows if rows is not None else _LIVE_ROWS):
        line: list = [None] * 17
        line[column] = label
        line[3] = march
        line[4] = april
        for index in range(5, 15):
            line[index] = 0
        line[15] = march + april
        grid.append(line)

    buf = io.BytesIO()
    pd.DataFrame(grid).to_excel(
        buf, index=False, header=False, sheet_name="Soybeans.Sojabone"
    )
    return buf.getvalue()


def _listing(*filenames: str) -> str:
    links = "".join(f'<a href="{_UPLOADS}/{name}">x</a>' for name in filenames)
    return f"<html><body>{links}</body></html>"


# ── Link resolution ─────────────────────────────────────────────────────────

def test_resolves_every_listed_season_newest_first():
    links = _resolve_smd_links(_listing(
        "2026/07/Sojabone20262027_2026-07-24.xlsx",
        "2026/04/Sojabone20252026_2026-04-26F.xlsx",
        "2025/08/Sojabone20242025_2025-08-26_F-2.xlsx",
    ))
    assert [season for season, _ in links["Sojabone"]] == [2026, 2025, 2024]
    assert links["Sojabone"][0][1].endswith("Sojabone20262027_2026-07-24.xlsx")


def test_newest_publication_of_a_season_wins():
    """The current season is re-issued monthly; last month's file is still
    on the page and still returns HTTP 200."""
    links = _resolve_smd_links(_listing(
        "2026/06/Sojabone20262027_2026-06-26.xlsx",
        "2026/07/Sojabone20262027_2026-07-24.xlsx",
    ))
    assert links["Sojabone"] == [(
        2026, f"{_UPLOADS}/2026/07/Sojabone20262027_2026-07-24.xlsx",
    )]


def test_monthly_announcement_files_are_not_season_files():
    """`Sojabone20260724.xlsx` is also eight digits after the token but holds
    two months, not a season — the `_<date>` suffix is what separates them."""
    with pytest.raises(ScraperShapeError, match="Sojabone"):
        _resolve_smd_links(_listing("2026/07/Sojabone20260724.xlsx"))


def test_untracked_commodities_are_ignored():
    """Maize, wheat, sunflower and barley publish the same workbook shape."""
    links = _resolve_smd_links(_listing(
        "2026/07/Mielies20262027_2026-07-24.xlsx",
        "2026/07/Sonneblom20262027_2026-07-24.xlsx",
        "2026/07/Sojabone20262027_2026-07-24.xlsx",
    ))
    assert set(links) == set(SAGIS_SMD_COMMODITIES)


def test_missing_season_workbook_hard_fails():
    with pytest.raises(ScraperShapeError, match="Sojabone"):
        _resolve_smd_links(_listing("2026/07/Mielies20262027_2026-07-24.xlsx"))


# ── Parsing ─────────────────────────────────────────────────────────────────

def test_parses_reported_months_only():
    """Tag SMD-052026 reports through April; May onward is printed as 0 and
    must not be stored as a month of zero crush, zero trade, zero stock."""
    out = _parse_smd_workbook(_workbook(), "Sojabone")

    assert list(out["month_end"]) == ["2026-03-31", "2026-04-30"]
    assert list(out["month_number"]) == [1, 2]
    assert set(out["season_year"]) == {2026}
    assert set(out["report_month"]) == {"2026-05-01"}
    assert set(out["unit"]) == {"MT"}


def test_crush_and_trade_come_from_the_right_sections():
    """(c) "Oil and oilcake" is commercial-use crush (171,041 t in March);
    (i)'s identically-labelled row is local-market only (147,912 t). Reading
    the wrong one understates South African crush by ~14%."""
    march = _parse_smd_workbook(_workbook(), "Sojabone").iloc[0]

    assert march["processed_oil_oilcake"] == 171041
    assert march["processed_total"] == 181912
    assert march["opening_stock"] == 286120          # (a), not (h)'s 111
    assert march["unutilised_stock"] == 151888       # (f), not (h)'s 333
    assert march["exports_whole"] == 28344
    assert march["exports_border_posts"] == 28344
    assert march["exports_harbours"] == 0
    assert march["products_exported"] == 23129
    assert march["stock_processors"] == 55906


def test_section_totals_are_not_stored():
    """(b) Acquisition, (c) Utilisation and (e) Sundries are sums of stored
    components — keeping them would store the same fact twice and make the
    balance check circular."""
    out = _parse_smd_workbook(_workbook(), "Sojabone")
    assert not {"acquisition", "utilisation", "sundries"} & set(out.columns)
    assert not any("progressive" in c.lower() for c in out.columns)


def test_older_vintage_labels_still_parse():
    """The 2024/25 final says "(i+j)" where 2026/27 says "(i+j): (iv)", and
    misspells (h)'s "Opening stock" as "Openening stock"."""
    rows = [
        (col, label.replace("(i+j): (iv)", "(i+j)").replace(
            "Opening stock", "Openening stock"), march, april)
        for col, label, march, april in _LIVE_ROWS
    ]
    out = _parse_smd_workbook(_workbook(rows), "Sojabone")
    assert out.iloc[0]["processed_total"] == 181912


def test_mis_sectioned_row_is_caught_by_the_balance_check():
    """Simulate reading (h)'s closing stock as (f): the numbers still parse,
    and only the report's own arithmetic can tell that they are wrong."""
    rows = [
        (col, label, 333 if label.startswith("(f)") else march, april)
        for col, label, march, april in _LIVE_ROWS
    ]
    with pytest.raises(ScraperShapeError, match="does not balance"):
        _parse_smd_workbook(_workbook(rows), "Sojabone")


def test_missing_report_tag_hard_fails():
    """Without the vintage stamp, an unreported month and a month of zero
    are the same cell."""
    with pytest.raises(ScraperShapeError, match="report tag"):
        _parse_smd_workbook(_workbook(tag=None), "Sojabone")


def test_missing_line_item_hard_fails():
    rows = [r for r in _LIVE_ROWS if r[1] != "Imports destined for RSA"]
    with pytest.raises(ScraperShapeError, match="imports"):
        _parse_smd_workbook(_workbook(rows), "Sojabone")


def test_short_header_row_hard_fails():
    with pytest.raises(ScraperShapeError, match="month columns"):
        _parse_smd_workbook(_workbook(months=_MONTH_HEADERS[:6]), "Sojabone")


def test_non_march_season_start_hard_fails():
    """The March–February marketing season is what season_year and
    month_number mean; a shifted season would silently re-base both."""
    shifted = [h.replace("2026", "2027") if h.startswith(("Jan", "Feb")) else h
               for h in _MONTH_HEADERS]
    shifted = ["Apr 2026" if h.startswith("Mar") else h for h in shifted]
    with pytest.raises(ScraperShapeError):
        _parse_smd_workbook(_workbook(months=shifted), "Sojabone")


def test_unreadable_bytes_hard_fail():
    with pytest.raises(ScraperShapeError, match="unreadable"):
        _parse_smd_workbook(b"not an xlsx", "Sojabone")


# ── Cleaning ────────────────────────────────────────────────────────────────

def test_clean_keeps_zeros_and_signed_sundries():
    """Zero imports is the normal South African month; sundries are signed."""
    cleaned = clean_sagis_smd(_parse_smd_workbook(_workbook(), "Sojabone"))

    assert (cleaned["imports"] == 0).all()
    assert (cleaned["sundries_surplus_deficit"] < 0).all()
    assert len(cleaned) == 2


def test_clean_dates_and_sorts():
    cleaned = clean_sagis_smd(_parse_smd_workbook(_workbook(), "Sojabone"))
    assert pd.api.types.is_datetime64_any_dtype(cleaned["month_end"])
    assert pd.api.types.is_datetime64_any_dtype(cleaned["report_month"])
    keys = list(zip(cleaned["season_year"], cleaned["month_number"], strict=True))
    assert keys == sorted(keys)


def test_clean_does_not_mutate_input():
    raw = _parse_smd_workbook(_workbook(), "Sojabone")
    before = raw.copy()
    clean_sagis_smd(raw)
    pd.testing.assert_frame_equal(raw, before)


# ── Storage round-trip ──────────────────────────────────────────────────────

def test_store_and_read_round_trip(patched_db):
    cleaned = clean_sagis_smd(_parse_smd_workbook(_workbook(), "Sojabone"))
    save_sagis_smd("Soybeans (SAGIS)", cleaned)

    out = read_sagis_supply_demand("Soybeans (SAGIS)")
    assert len(out) == 2
    march = out[out["month_number"] == 1].iloc[0]
    assert march["processed_oil_oilcake"] == 171041
    assert march["exports_harbours"] == 0
    assert pd.Timestamp(march["month_end"]).date().isoformat() == "2026-03-31"


def test_revisions_overwrite_rather_than_duplicate(patched_db):
    """SAGIS restates months for a year or more — a month first published as
    preliminary comes back revised, and must not fork into a second row."""
    cleaned = clean_sagis_smd(_parse_smd_workbook(_workbook(), "Sojabone"))
    save_sagis_smd("Soybeans (SAGIS)", cleaned)

    revised = cleaned.copy()
    revised.loc[revised["month_number"] == 1, "processed_oil_oilcake"] = 175000.0
    save_sagis_smd("Soybeans (SAGIS)", revised)

    out = read_sagis_supply_demand("Soybeans (SAGIS)")
    assert len(out) == 2
    assert out[out["month_number"] == 1].iloc[0]["processed_oil_oilcake"] == 175000.0


def test_registered_for_history_round_trip():
    """Only the current season plus two finals are ever listed; older
    seasons exist as per-month files this layer does not fetch, so a season
    that scrolls off is unrecoverable from an ephemeral CI database."""
    assert HISTORY_TABLES["sagis_supply_demand"] == (
        "commodity", "season_year", "month_number",
    )


def test_recency_budget_survives_a_missed_publication():
    """Published ~24th for the previous month: the newest month_end is ~55
    days old the day before the next release, ~85 with one skipped."""
    assert LAYER_MAX_DATA_AGE_DAYS["sagis_smd"] >= 85


# ── Pace comparison ─────────────────────────────────────────────────────────

def _two_seasons() -> pd.DataFrame:
    """Season 2026 reported through month 2; season 2025 complete (4 months).

    Prior-season months 3 and 4 must be excluded from the comparison, or a
    season-to-date total would be measured against a longer stretch of the
    season before it.
    """
    rows = []
    for season, values in (
        (2026, [(1, 171041, 0, 28344), (2, 188916, 0, 22562)]),
        (2025, [(1, 100000, 500, 10000), (2, 120000, 700, 12000),
                (3, 130000, 900, 14000), (4, 140000, 1100, 16000)]),
    ):
        for month_number, crush, imports, exports in values:
            month_end = pd.Timestamp(year=season, month=3, day=1) \
                + pd.DateOffset(months=month_number - 1) + pd.offsets.MonthEnd(0)
            rows.append({
                "commodity": "Soybeans (SAGIS)",
                "season_year": season,
                "month_number": month_number,
                "month_end": month_end,
                "report_month": pd.Timestamp("2026-05-01"),
                "processed_oil_oilcake": crush,
                "processed_total": crush + 10000,
                "imports": imports,
                "exports_whole": exports,
                "exports_border_posts": exports,
                "exports_harbours": 0,
                "unutilised_stock": 423983,
                "stock_processors": 56840,
            })
    return pd.DataFrame(rows)


def test_pace_compares_at_the_same_month_position():
    pace = _sagis_smd_pace(_two_seasons())

    assert pace["month_number"] == 2
    assert pace["month_label"] == "Apr 2026"
    assert pace["crush_season_to_date_mt"] == 359957        # 171041 + 188916
    assert pace["crush_prev_season_mt"] == 220000           # months 1-2 only
    assert pace["crush_yoy_pct"] == pytest.approx(63.6, abs=0.1)
    assert pace["exports_whole_prev_season_mt"] == 22000


def test_pace_reports_the_latest_month_and_stock_split():
    pace = _sagis_smd_pace(_two_seasons())

    assert pace["crush_mt"] == 188916
    assert pace["closing_stock_mt"] == 423983
    assert pace["stock_processors_share_pct"] == pytest.approx(13.4, abs=0.1)
    assert pace["season_label"] == "2026/2027"


def test_pace_omits_yoy_when_the_prior_season_is_short():
    """A prior season with fewer reported months than the current one cannot
    be compared at the same position — no ratio is better than a wrong one."""
    frame = _two_seasons()
    frame = frame[~((frame["season_year"] == 2025) & (frame["month_number"] == 2))]
    pace = _sagis_smd_pace(frame)

    assert pace["crush_season_to_date_mt"] == 359957
    assert "crush_yoy_pct" not in pace


def test_pace_empty_frame_returns_empty_dict():
    assert _sagis_smd_pace(pd.DataFrame()) == {}
