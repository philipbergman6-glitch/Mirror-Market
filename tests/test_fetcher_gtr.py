"""Layers 26 / 26b — USDA AMS Grain Transportation Report.

These two layers close the transport gap: what a cargo costs to move (ocean
freight, monthly) and whether the boats are moving (vessel lineups, weekly).
Both are read off supporting workbooks of the same weekly AMS report, and
both are addressed by *column index* because neither sheet has a single
header row to key on. That is the whole risk, and it is why these tests
exist: a shifted column parses cleanly and restates one series as another.

Four failure modes are pinned here, each observed in the real files:

1.  **Mixed-type period labels.** Figure 20 stores its early history as the
    string "96-Jan" and its recent rows as datetimes, and the string tokens
    mix abbreviations with full names ("Sep", but "June" and "July"). A
    parser that handles one form silently loses thirty years or the last five.

2.  **Summary blocks that look like data.** Both sheets end in year-on-year
    *ratios* and averages printed under the same columns as the values. Read
    as rows, those are a freight market at thirty cents and a port with 0.2
    ships in it.

3.  **Column drift.** Guarded by each sheet's own published arithmetic —
    the Gulf-vs-PNW spread must equal gulf - pnw, and vessels in port must
    equal loading + waiting to load wherever all three print.

4.  **A truncated download served as HTTP 200.** Observed live on
    2026-08-19 against these files: 634,667 bytes arrived, the zip central
    directory did not, and the result still passed `file(1)` as a workbook.
    openpyxl raises zipfile.BadZipFile, which is not an OSError.

The structural tests read *trimmed real workbooks* (tests/fixtures/gtr_*),
which keep the published banner layout, both date forms and both summary
blocks. They call the parsers directly, so no fixture can expire. The guard
tests build synthetic workbooks with today-relative dates instead, so the
recency budget is exercised without a fixture that goes stale on a calendar.

No network: every download is monkeypatched.
"""

from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import pytest

import config
import fetchers.gtr as gtr
import main
from config import (
    GTR_OCEAN_ATTRIBUTION,
    GTR_OCEAN_CADENCE,
    GTR_OCEAN_QUOTE_KIND,
    GTR_OCEAN_ROUTES,
    GTR_PORT_REGIONS,
    GTR_VESSEL_UNIT,
    LAYER_MAX_DATA_AGE_DAYS,
    LAYER_MIN_KEYS,
)
from pipeline import query, store
from pipeline.clean import clean_ocean_freight, clean_port_vessel_activity

FIXTURES = Path(__file__).parent / "fixtures"
GULF_ROUTE = GTR_OCEAN_ROUTES[1]
PNW_ROUTE = GTR_OCEAN_ROUTES[3]


def _fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _sheet(rows: list[list], sheet_name: str = "Data") -> bytes:
    """Serialise a headerless sheet the way both workbooks are laid out."""
    buf = io.BytesIO()
    frame = pd.DataFrame(rows)
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        frame.to_excel(writer, sheet_name=sheet_name, header=False, index=False)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Synthetic workbook builders — today-relative so no test expires
# ---------------------------------------------------------------------------

def _banner(width: int) -> list:
    """A banner row carrying a label in its last column.

    The real sheets are wider than the columns they use, and openpyxl drops
    trailing all-blank columns when a workbook is written. Without a value
    out at the right edge, a test whose data columns happen to be blank
    would produce a narrower sheet and trip the parser's width guard —
    testing the wrong thing.
    """
    row = [None] * width
    row[-1] = "banner"
    return row


_OCEAN_WIDTH = 7


def _ocean_row(date, gulf: float, pnw: float, spread=None) -> list:
    row = [None] * _OCEAN_WIDTH
    row[0] = date
    row[1] = gulf
    row[3] = pnw
    row[5] = (gulf - pnw) if spread is None else spread
    return row


def _ocean_workbook(rows: list[list], *, banner_rows: int = 6) -> bytes:
    return _sheet([_banner(_OCEAN_WIDTH) for _ in range(banner_rows)] + rows)


def _fresh_months(count: int = 4) -> list[pd.Timestamp]:
    """`count` month-starts ending with the current month."""
    end = pd.Timestamp.today().normalize().replace(day=1)
    return list(pd.date_range(end=end, periods=count, freq="MS"))


_VESSEL_WIDTH = 19


def _vessel_row(date, gulf: tuple, pnw: tuple) -> list:
    """(loading, waiting, in_port, loaded_7day, due_10day) per region."""
    row = [None] * _VESSEL_WIDTH
    row[0] = date
    for (loading, waiting, in_port, loaded, due), columns in (
        (gulf, GTR_PORT_REGIONS["US Gulf"]),
        (pnw, GTR_PORT_REGIONS["Pacific Northwest"]),
    ):
        row[columns["loading"]] = loading
        row[columns["waiting_to_load"]] = waiting
        row[columns["in_port"]] = in_port
        row[columns["loaded_7day"]] = loaded
        row[columns["due_10day"]] = due
    return row


def _vessel_workbook(rows: list[list], *, banner_rows: int = 3) -> bytes:
    return _sheet([_banner(_VESSEL_WIDTH) for _ in range(banner_rows)] + rows)


def _fresh_weeks(count: int = 4) -> list[pd.Timestamp]:
    end = pd.Timestamp.today().normalize()
    return list(pd.date_range(end=end, periods=count, freq="7D"))


# ---------------------------------------------------------------------------
# Structural — the real published layout
# ---------------------------------------------------------------------------


def test_ocean_freight_parses_every_period_format_in_the_real_file():
    """Thirty years of the sheet's maintainers, seven layouts, one column.

    Counted live on 2026-08-19 across all 367 data rows: `96-Jan` (42),
    `July_99` (30), `Jan. 02` (139), `May  02` with a doubled space (2),
    `June 02` (46), `Aug '17` (22) and real datetimes (86). The fixture
    carries one of each. Handling a subset does not fail — it silently
    returns a shorter series, which is invisible in a row count nobody
    checks: the first cut of this parser handled two forms and stored 128
    of 367 months while logging success.
    """
    parsed = gtr._parse_ocean_freight(_fixture_bytes("gtr_figure20_ocean_freight.xlsx"))

    assert set(parsed) == {GULF_ROUTE, PNW_ROUTE}
    stored = set(parsed[GULF_ROUTE]["Date"])

    for month in (
        "1996-01-01",  # 96-Jan
        "1996-06-01",  # 96-June   — full name where the column elsewhere abbreviates
        "1999-07-01",  # July_99   — month first, underscore
        "2002-01-01",  # Jan. 02   — month first, period
        "2002-05-01",  # May  02   — doubled space
        "2002-06-01",  # June 02
        "2017-08-01",  # Aug '17   — apostrophe
        "2026-07-01",  # a real datetime
    ):
        assert pd.Timestamp(month) in stored, month


def test_ocean_freight_parses_the_sept_spelling():
    """`Sept` is neither `%b` nor `%B`, and it is every September 2002-2016.

    strptime accepts "Sep" and "September" and rejects "Sept", so a parser
    built on those two format codes loses exactly one month a year for
    fifteen years — a gap small enough to look like the source's own.
    """
    parsed = gtr._parse_ocean_freight(_fixture_bytes("gtr_figure20_ocean_freight.xlsx"))

    assert pd.Timestamp("2002-09-01") in set(parsed[GULF_ROUTE]["Date"])


def test_ocean_freight_refuses_the_publishers_1919_typo():
    """Seven 2019 months are published as 1919, between "May '19" and 2020-01.

    The sequence proves the intent, but rewriting a published year is
    inventing data and storing 1919 puts a century-old rate at the front of
    every chart. The gap is the deliberate third option.
    """
    parsed = gtr._parse_ocean_freight(_fixture_bytes("gtr_figure20_ocean_freight.xlsx"))

    for frame in parsed.values():
        assert frame["Date"].min().year >= config.GTR_MIN_OBSERVATION_YEAR
        assert pd.Timestamp("2019-06-01") not in set(frame["Date"])


def test_ocean_freight_pins_the_published_values():
    """July 2026 is $68.95 Gulf / $35.95 PNW in the published workbook.

    Pinned rather than merely non-empty: this is what catches a one-column
    shift that still parses, still passes the band, and quietly reports the
    four-year average as the rate.
    """
    parsed = gtr._parse_ocean_freight(_fixture_bytes("gtr_figure20_ocean_freight.xlsx"))

    def rate(route: str, month: str) -> float:
        frame = parsed[route]
        return float(frame.loc[frame["Date"] == pd.Timestamp(month), "rate_usd_mt"].iloc[0])

    assert rate(GULF_ROUTE, "2026-07-01") == pytest.approx(68.95)
    assert rate(PNW_ROUTE, "2026-07-01") == pytest.approx(35.95)


def test_ocean_freight_excludes_the_trailing_ratio_block():
    """The summary rows print 0.33/0.21 under the rate columns.

    Stored, those are a freight market that collapsed by two orders of
    magnitude. The fixture deliberately includes that block.
    """
    parsed = gtr._parse_ocean_freight(_fixture_bytes("gtr_figure20_ocean_freight.xlsx"))

    for frame in parsed.values():
        assert frame["rate_usd_mt"].min() >= config.GTR_OCEAN_MIN_USD_MT


def test_vessel_activity_pins_the_published_values():
    """Week ending 2026-08-06: Gulf 22 in port / 33 loaded / 30 due, PNW 14.

    Cross-checked against the workbook's own printed `New Table 19` sheet,
    which is a different sheet from the one parsed — so this pins the column
    mapping against the publisher's own presentation of the same week.
    """
    parsed = gtr._parse_vessel_activity(
        _fixture_bytes("gtr_table19_vessel_activity.xlsx")
    )

    assert set(parsed) == set(GTR_PORT_REGIONS)
    week = pd.Timestamp("2026-08-06")

    gulf = parsed["US Gulf"]
    row = gulf.loc[gulf["week_ending"] == week].iloc[0]
    assert row["in_port"] == 22
    assert row["loaded_7day"] == 33
    assert row["due_10day"] == 30

    pnw = parsed["Pacific Northwest"]
    assert pnw.loc[pnw["week_ending"] == week, "in_port"].iloc[0] == 14


def test_vessel_activity_keeps_the_1990s_rows_that_carry_only_a_total():
    """`in_port` is stored, not derived, and this is why.

    The 1995 rows publish the in-port total with no loading/waiting split.
    Deriving in_port from its two components would delete them.
    """
    parsed = gtr._parse_vessel_activity(
        _fixture_bytes("gtr_table19_vessel_activity.xlsx")
    )
    gulf = parsed["US Gulf"]
    early = gulf.loc[gulf["week_ending"] == pd.Timestamp("1995-01-04")].iloc[0]

    assert early["in_port"] == 39
    assert pd.isna(early["loading"])
    assert pd.isna(early["waiting_to_load"])


def test_vessel_activity_excludes_the_trailing_summary_block():
    """Percent-change and 2015-average rows sit under the count columns."""
    parsed = gtr._parse_vessel_activity(
        _fixture_bytes("gtr_table19_vessel_activity.xlsx")
    )
    for frame in parsed.values():
        assert frame["week_ending"].min() >= pd.Timestamp("1995-01-01")
        assert frame["in_port"].dropna().min() >= 1


# ---------------------------------------------------------------------------
# Column drift — each sheet's own arithmetic is the guard
# ---------------------------------------------------------------------------


def test_ocean_freight_rejects_rows_whose_spread_does_not_reconcile():
    """A shifted column is a wrong number, not a missing one.

    The published spread is struck on the columns the sheet means; if it
    does not equal gulf - pnw as we read them, we are reading different
    columns and the row must not be stored.
    """
    months = _fresh_months(30)
    rows = [_ocean_row(month, 60.0, 30.0) for month in months]
    # Spread says 20 while the columns we read imply 30 — one bad row, kept
    # under the column-shift threshold so this exercises the row-level drop
    # rather than the whole-workbook one.
    rows[4] = _ocean_row(months[4], 61.0, 31.0, spread=20.0)

    parsed = gtr._parse_ocean_freight(_ocean_workbook(rows))

    stored = set(parsed[GULF_ROUTE]["Date"])
    assert months[4] not in stored
    assert len(stored) == len(months) - 1


def test_ocean_freight_accepts_a_row_whose_spread_column_is_blank():
    """A missing check is not a failed check.

    Some rows publish no spread. Dropping them would throw away data over
    the absence of a cross-check rather than over a contradiction.
    """
    months = _fresh_months(1)
    # _ocean_row derives the spread when none is given, so blank it directly.
    rows = [_ocean_row(months[0], 60.0, 30.0)]
    rows[0][5] = None
    parsed = gtr._parse_ocean_freight(_ocean_workbook(rows))

    assert float(parsed[GULF_ROUTE]["rate_usd_mt"].iloc[0]) == pytest.approx(60.0)


def test_a_few_contradictory_rows_are_noise_and_the_rest_is_kept():
    """8 of 1,649 published vessel weeks contradict their own components.

    Measured live on 2026-08-19, scattered across 2018-2026 and off by 1-12
    vessels — the publisher's typing, not our mapping. Discarding the layer
    over that would take a thirty-year series down for a rounding argument.
    """
    weeks = _fresh_weeks(30)
    rows = [
        _vessel_row(week, (6, 16, 22, 33, 30), (5, 9, 14, 8, 11))
        for week in weeks
    ]
    rows[7] = _vessel_row(weeks[7], (6, 16, 30, 33, 30), (5, 9, 14, 8, 11))

    parsed = gtr._parse_vessel_activity(_vessel_workbook(rows))

    stored = set(parsed["US Gulf"]["week_ending"])
    assert weeks[7] not in stored
    assert len(stored) == len(weeks) - 1


def test_a_wholesale_arithmetic_failure_discards_the_workbook():
    """Above the threshold the mapping moved, so every row is suspect.

    This is the difference between the check as an audit of the publisher
    and the check as a detector of our own column drift. It is the second,
    and a shifted column makes the rows that happen to reconcile no more
    trustworthy than the ones that do not.
    """
    weeks = _fresh_weeks(10)
    rows = [
        _vessel_row(week, (6, 16, 99, 33, 30), (5, 9, 14, 8, 11))
        for week in weeks
    ]

    assert gtr._parse_vessel_activity(_vessel_workbook(rows)) == {}


def test_wholesale_spread_failure_discards_the_freight_workbook():
    months = _fresh_months(10)
    rows = [_ocean_row(month, 60.0, 30.0, spread=20.0) for month in months]

    assert gtr._parse_ocean_freight(_ocean_workbook(rows)) == {}


def test_vessel_activity_keeps_a_region_that_did_not_report_that_week():
    """An all-blank region is a series that did not exist, not a zero."""
    weeks = _fresh_weeks(1)
    row = _vessel_row(weeks[0], (6, 16, 22, 33, 30), (None, None, None, None, None))
    parsed = gtr._parse_vessel_activity(_vessel_workbook([row]))

    # The PNW block parsed to nothing, so the workbook is discarded whole
    # rather than published as a half-dark layer.
    assert parsed == {}


# ---------------------------------------------------------------------------
# Plausibility bands
# ---------------------------------------------------------------------------


def test_ocean_freight_rejects_rates_outside_the_band():
    months = _fresh_months(2)
    rows = [
        _ocean_row(months[0], 60.0, 30.0),
        _ocean_row(months[1], 0.33, 0.24),  # the summary block's ratios
    ]
    parsed = gtr._parse_ocean_freight(_ocean_workbook(rows))

    assert set(parsed[GULF_ROUTE]["Date"]) == {months[0]}


def test_vessel_activity_rejects_counts_outside_the_band():
    weeks = _fresh_weeks(2)
    rows = [
        _vessel_row(weeks[0], (6, 16, 22, 33, 30), (5, 9, 14, 8, 11)),
        _vessel_row(
            weeks[1],
            (6, 16, 22, config.GTR_VESSEL_MAX_COUNT + 1, 30),
            (5, 9, 14, 8, 11),
        ),
    ]
    parsed = gtr._parse_vessel_activity(_vessel_workbook(rows))

    assert set(parsed["US Gulf"]["week_ending"]) == {weeks[0]}


# ---------------------------------------------------------------------------
# Unusable payloads
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "parse", [gtr._parse_ocean_freight, gtr._parse_vessel_activity]
)
def test_truncated_download_is_a_failure_not_a_crash(parse):
    """The live failure mode: a zip that stops before its central directory.

    `zipfile.BadZipFile` is not an OSError, so an `except OSError` would let
    it escape and take the run down mid-pipeline.
    """
    truncated = _fixture_bytes("gtr_figure20_ocean_freight.xlsx")[:2048]

    assert parse(truncated) == {}


@pytest.mark.parametrize(
    "parse", [gtr._parse_ocean_freight, gtr._parse_vessel_activity]
)
def test_missing_data_sheet_is_a_failure(parse):
    renamed = _sheet([[None] * 8, [1, 2, 3]], sheet_name="Summary")

    assert parse(renamed) == {}


def test_ocean_freight_discards_the_workbook_when_only_one_route_parses():
    """Partial is worse than empty when both keys come from one download.

    One route surviving alone means the mapping moved — which makes the
    survivor exactly as suspect as the casualty.
    """
    months = _fresh_months(1)
    row = _ocean_row(months[0], 60.0, 30.0)
    row[3] = None  # PNW column blank
    row[5] = None

    assert gtr._parse_ocean_freight(_ocean_workbook([row])) == {}


# ---------------------------------------------------------------------------
# Recency — the frozen workbook behind a fixed filename
# ---------------------------------------------------------------------------


def test_fresh_ocean_workbook_is_returned(monkeypatch):
    months = _fresh_months(3)
    payload = _ocean_workbook([_ocean_row(m, 60.0, 30.0) for m in months])
    monkeypatch.setattr(gtr, "_download", lambda url, label: payload)

    result = gtr.fetch_gtr_ocean_freight()

    assert set(result) == {GULF_ROUTE, PNW_ROUTE}


def test_stale_ocean_workbook_is_discarded_whole(monkeypatch):
    """A frozen file keeps answering 200 forever — nothing else would notice.

    Storing it and logging a note would leave the layer green on dead
    numbers, so the payload is dropped rather than written.
    """
    budget = LAYER_MAX_DATA_AGE_DAYS["gtr_ocean_freight"]
    stale_month = (
        pd.Timestamp.today().normalize().replace(day=1)
        - pd.Timedelta(days=budget + 40)
    ).replace(day=1)
    payload = _ocean_workbook([_ocean_row(stale_month, 60.0, 30.0)])
    monkeypatch.setattr(gtr, "_download", lambda url, label: payload)

    assert gtr.fetch_gtr_ocean_freight() == {}


def test_stale_vessel_workbook_is_discarded_whole(monkeypatch):
    budget = LAYER_MAX_DATA_AGE_DAYS["gtr_vessels"]
    stale_week = pd.Timestamp.today().normalize() - pd.Timedelta(days=budget + 7)
    payload = _vessel_workbook(
        [_vessel_row(stale_week, (6, 16, 22, 33, 30), (5, 9, 14, 8, 11))]
    )
    monkeypatch.setattr(gtr, "_download", lambda url, label: payload)

    assert gtr.fetch_gtr_vessel_activity() == {}


@pytest.mark.parametrize(
    "fetch", [gtr.fetch_gtr_ocean_freight, gtr.fetch_gtr_vessel_activity]
)
def test_failed_download_returns_empty(fetch, monkeypatch):
    monkeypatch.setattr(gtr, "_download", lambda url, label: b"")

    assert fetch() == {}


# ---------------------------------------------------------------------------
# Cleaners
# ---------------------------------------------------------------------------


def test_clean_ocean_freight_drops_rateless_rows_and_sorts():
    frame = pd.DataFrame({
        "Date": pd.to_datetime(["2026-03-01", "2026-01-01", "2026-02-01"]),
        "rate_usd_mt": [62.0, 60.0, None],
    })

    cleaned = clean_ocean_freight(frame)

    assert list(cleaned["Date"]) == [
        pd.Timestamp("2026-01-01"), pd.Timestamp("2026-03-01")
    ]


def test_clean_vessel_activity_preserves_individual_missing_counts():
    """A blank count means the series did not exist, never that the port was empty."""
    frame = pd.DataFrame({
        "week_ending": pd.to_datetime(["1995-01-04", "2026-08-06"]),
        "loading": [None, 6.0],
        "waiting_to_load": [None, 16.0],
        "in_port": [39.0, 22.0],
        "loaded_7day": [58.0, 33.0],
        "due_10day": [62.0, 30.0],
    })

    cleaned = clean_port_vessel_activity(frame)

    assert len(cleaned) == 2
    assert pd.isna(cleaned.loc[0, "loading"])
    assert cleaned.loc[0, "in_port"] == 39.0


def test_clean_vessel_activity_drops_a_week_with_no_counts_at_all():
    frame = pd.DataFrame({
        "week_ending": pd.to_datetime(["2026-08-06", "2026-08-13"]),
        "loading": [6.0, None],
        "waiting_to_load": [16.0, None],
        "in_port": [22.0, None],
        "loaded_7day": [33.0, None],
        "due_10day": [30.0, None],
    })

    cleaned = clean_port_vessel_activity(frame)

    assert list(cleaned["week_ending"]) == [pd.Timestamp("2026-08-06")]


# ---------------------------------------------------------------------------
# Storage round-trip — lineage reaches the row
# ---------------------------------------------------------------------------


def test_ocean_freight_rows_carry_their_attribution(patched_db):
    """The rate is assessed by a broker and republished by USDA.

    Stamped per row rather than resolved at display time: a surface showing
    this beside the AMS Gulf bids would otherwise credit both to USDA.
    """
    frame = pd.DataFrame({
        "Date": pd.to_datetime(["2026-07-01"]),
        "rate_usd_mt": [68.95],
    })
    store.save_ocean_freight(GULF_ROUTE, frame)

    stored = query.read_ocean_freight_rates(GULF_ROUTE)

    assert len(stored) == 1
    assert stored.loc[0, "attribution"] == GTR_OCEAN_ATTRIBUTION
    assert stored.loc[0, "cadence"] == GTR_OCEAN_CADENCE
    assert stored.loc[0, "quote_kind"] == GTR_OCEAN_QUOTE_KIND
    assert stored.loc[0, "rate_usd_mt"] == pytest.approx(68.95)


def test_vessel_activity_round_trips_with_its_unit(patched_db):
    frame = pd.DataFrame({
        "week_ending": pd.to_datetime(["2026-08-06"]),
        "loading": [6.0],
        "waiting_to_load": [16.0],
        "in_port": [22.0],
        "loaded_7day": [33.0],
        "due_10day": [30.0],
    })
    store.save_port_vessel_activity("US Gulf", frame)

    stored = query.read_port_vessel_activity("US Gulf")

    assert len(stored) == 1
    assert stored.loc[0, "unit"] == GTR_VESSEL_UNIT
    assert stored.loc[0, "in_port"] == 22.0


def test_ocean_freight_save_is_an_upsert(patched_db):
    """The workbook is re-read whole every run, so every row is rewritten."""
    first = pd.DataFrame({
        "Date": pd.to_datetime(["2026-07-01"]), "rate_usd_mt": [68.95],
    })
    revised = pd.DataFrame({
        "Date": pd.to_datetime(["2026-07-01"]), "rate_usd_mt": [69.10],
    })
    store.save_ocean_freight(GULF_ROUTE, first)
    store.save_ocean_freight(GULF_ROUTE, revised)

    stored = query.read_ocean_freight_rates(GULF_ROUTE)

    assert len(stored) == 1
    assert stored.loc[0, "rate_usd_mt"] == pytest.approx(69.10)


# ---------------------------------------------------------------------------
# Pipeline wiring
# ---------------------------------------------------------------------------


def test_both_layers_are_in_the_production_inventory():
    keys = {row[0] for row in config.PRODUCTION_LAYERS}

    assert {"gtr_ocean_freight", "gtr_vessels"} <= keys


@pytest.mark.parametrize("layer", ["gtr_ocean_freight", "gtr_vessels"])
def test_both_layers_carry_a_key_floor_and_a_recency_budget(layer):
    """Neither can pass on half its keys, and neither can freeze and stay green.

    The floor is the full key count on purpose: both keys come out of one
    download of one sheet, so one key alone is a parse fault, never an
    off-day.
    """
    assert LAYER_MIN_KEYS[layer] == len(config.LAYER_KEY_CATALOGS[layer])
    assert layer in LAYER_MAX_DATA_AGE_DAYS


@pytest.mark.parametrize("layer", ["gtr_ocean_freight", "gtr_vessels"])
def test_both_layers_are_wired_into_the_dict_layer_table(layer):
    wired = {entry.key for entry in main._build_dict_layers()}

    assert layer in wired


# ---------------------------------------------------------------------------
# Briefing surface
# ---------------------------------------------------------------------------


def _seed_transport(patched_db) -> None:
    for route, rates in (
        (GULF_ROUTE, [55.00, 68.95]),
        (PNW_ROUTE, [29.00, 35.95]),
    ):
        store.save_ocean_freight(route, pd.DataFrame({
            "Date": pd.to_datetime(["2026-06-01", "2026-07-01"]),
            "rate_usd_mt": rates,
        }))
    store.save_port_vessel_activity("US Gulf", pd.DataFrame({
        "week_ending": pd.to_datetime(["2026-07-30", "2026-08-06"]),
        "loading": [10.0, 6.0],
        "waiting_to_load": [21.0, 16.0],
        "in_port": [31.0, 22.0],
        "loaded_7day": [27.0, 33.0],
        "due_10day": [28.0, 30.0],
    }))


def test_transport_section_reports_the_spread_and_names_the_cadence(patched_db):
    """The decision on this block is which US coast is the cheaper way out.

    The cadence caveat rides on the block because the reader arrives from
    the daily price sections, where a flat line means a flat market. Here it
    means the month has not turned.
    """
    from analysis.briefing.sections import transport

    _seed_transport(patched_db)
    text = transport.format()

    assert "$68.95/mt" in text
    assert "Gulf over PNW: $33.00/mt" in text
    assert "monthly freight assessment" in text
    assert "not a route-specific quote" in text
    assert "in port 22 (-9)" in text
    assert "week ending 2026-08-06" in text


def test_transport_section_is_silent_with_no_data(patched_db):
    """An unrun layer leaves no trace — the freshness block names failures."""
    from analysis.briefing.sections import transport

    assert transport.format() == ""


# ---------------------------------------------------------------------------
# Briefings archive
# ---------------------------------------------------------------------------


def test_snapshot_archives_both_legs_with_their_own_as_of(patched_db):
    """The two cadences differ by an order of magnitude, so each leg is dated.

    A block carrying one date for both would age a monthly freight rate by
    three weeks — or, worse, present a three-week-old freight print as this
    week's, which is the archive equivalent of plotting a weekly assessment
    on a daily axis.
    """
    from analysis.briefing.snapshot import _transport_block

    _seed_transport(patched_db)
    block = _transport_block()

    assert block["ocean_freight_usd_mt"][GULF_ROUTE] == {
        "as_of": "2026-07-01", "rate_usd_mt": 68.95,
    }
    assert block["vessels"]["US Gulf"]["week_ending"] == "2026-08-06"
    assert block["vessels"]["US Gulf"]["in_port"] == 22.0


def test_snapshot_stores_the_legs_and_not_the_spread(patched_db):
    """Both legs are present, so a stored spread is a second place to disagree.

    Same rule the DB follows: raw numbers and components, never a derived
    value whose inputs are already in the row.
    """
    from analysis.briefing.snapshot import _transport_block

    _seed_transport(patched_db)
    block = _transport_block()

    assert set(block["ocean_freight_usd_mt"]) == {GULF_ROUTE, PNW_ROUTE}
    assert "spread" not in str(block)


def test_snapshot_transport_block_is_empty_without_data(patched_db):
    """Every block degrades to {} so a partial pipeline day still archives."""
    from analysis.briefing.snapshot import _transport_block

    assert _transport_block() == {}
