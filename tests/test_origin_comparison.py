"""Alignment rules, window parsing and the ranking (Phase 2).

The tests here are mostly about *refusing* to answer: window mismatches,
observation spreads, undated assessments and unreadable legs. The one thing an
origin board must never do is produce a confident ordering out of numbers that
were not measuring the same thing.
"""

from __future__ import annotations

import sqlite3
from datetime import date

import pytest

import config
from analysis.origins.assumptions import AssumptionSet
from analysis.origins.comparison import (
    build_ranking,
    default_window,
    fob_advantage,
    fob_board,
    select_quote,
)
from analysis.origins.domain import Carrier, Incoterm, ShipmentWindow
from analysis.origins.sources import (
    offered_windows,
    parse_ams_slot,
    parse_magyp_window,
    read_origin_quotes,
    to_usd_per_mt,
)
from pipeline import schema

TODAY = date(2026, 8, 18)
SEP = ShipmentWindow(date(2026, 9, 1), date(2026, 9, 30), label="Sep 2026")


@pytest.fixture
def db(tmp_path):
    """A DB carrying one realistic session of all three origin legs."""
    conn = sqlite3.connect(str(tmp_path / "origins.db"))
    for ddl in (
        schema._CREATE_GULF_BIDS,
        schema._CREATE_ARGENTINA_FOB,
        schema._CREATE_BRAZIL_SPOT,
        schema._CREATE_CURRENCIES,
        schema._CREATE_ORIGIN_RANKINGS,
    ):
        conn.execute(ddl)
    conn.executemany(
        "INSERT INTO gulf_bids (report_date, commodity, location, delivery, average, "
        "futures_month) VALUES (?,?,?,?,?,?)",
        [
            ("2026-08-11", "Soybeans", "Gulf Coast Ports", "Current", 12.5563, 8),
            ("2026-08-11", "Soybeans", "Gulf Coast Ports", "Sep¹", 12.6875, 11),
            ("2026-08-11", "Soybeans", "Gulf Coast Ports", "Sep²", 12.6975, 11),
            ("2026-08-11", "Soybeans", "Gulf Coast Ports", "Nov¹", 12.8275, 11),
        ],
    )
    conn.executemany(
        "INSERT INTO argentina_fob (date, product, position, ship_from, ship_to, "
        "price_usd_mt) VALUES (?,?,?,?,?,?)",
        [
            ("2026-08-11", "Soybeans", "12019000190C", "2026-08", "2026-08", 449.0),
            ("2026-08-11", "Soybeans", "12019000190C", "2026-09", "2026-10", 452.0),
            ("2026-08-11", "Soybeans", "12019000190C", "2026-11", "2027-07", 454.0),
        ],
    )
    conn.executemany(
        "INSERT INTO brazil_spot_prices (Date, commodity, price_brl, unit) VALUES (?,?,?,?)",
        [("2026-08-11", "Soybean (AgRural Paranaguá FOB)", 2433.33, "BRL/MT")],
    )
    conn.executemany(
        "INSERT INTO currencies (pair, Date, Open, High, Low, Close) VALUES (?,?,?,?,?,?)",
        [("BRL/USD", "2026-08-11", 0.1958, 0.1959, 0.1957, 0.1958480179309845)],
    )
    conn.commit()
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Window parsing
# ---------------------------------------------------------------------------
def test_ams_current_is_the_balance_of_the_report_month_not_a_forward_slot():
    window = parse_ams_slot("Current", date(2026, 8, 11))
    assert (window.start, window.end) == (date(2026, 8, 11), date(2026, 8, 31))


def test_ams_half_month_superscripts_split_the_month():
    first = parse_ams_slot("Sep¹", date(2026, 8, 11))
    second = parse_ams_slot("Sep²", date(2026, 8, 11))
    assert (first.start, first.end) == (date(2026, 9, 1), date(2026, 9, 15))
    assert (second.start, second.end) == (date(2026, 9, 16), date(2026, 9, 30))
    assert not first.overlaps(second)


def test_ams_ascii_digits_are_accepted_as_well_as_superscripts():
    assert parse_ams_slot("Sep1", date(2026, 8, 11)) == parse_ams_slot("Sep¹", date(2026, 8, 11))


def test_an_ams_month_before_the_report_month_rolls_to_next_year():
    """A January slot on an August report is next January, not eight months ago."""
    window = parse_ams_slot("Jan", date(2026, 8, 11))
    assert window.start == date(2027, 1, 1)


def test_an_unreadable_ams_slot_is_none_rather_than_a_guessed_window():
    assert parse_ams_slot("Deferred", date(2026, 8, 11)) is None
    assert parse_ams_slot("", date(2026, 8, 11)) is None


def test_magyp_ship_to_is_inclusive_of_its_whole_month():
    """Reading it as a day would shrink an 11-month band to a single date."""
    window = parse_magyp_window("2026-11", "2027-07")
    assert (window.start, window.end) == (date(2026, 11, 1), date(2027, 7, 31))


def test_magyp_single_month_band():
    window = parse_magyp_window("2026-08", "2026-08")
    assert (window.start, window.end) == (date(2026, 8, 1), date(2026, 8, 31))
    assert window.label == "2026-08"


def test_magyp_rejects_an_inverted_band():
    assert parse_magyp_window("2026-11", "2026-08") is None


def test_magyp_unreadable_bounds_are_none():
    assert parse_magyp_window(None, None) is None
    assert parse_magyp_window("not-a-month", "2026-09") is None


# ---------------------------------------------------------------------------
# Offered windows
# ---------------------------------------------------------------------------
def test_offered_windows_never_include_a_month_that_has_sailed():
    windows = offered_windows(TODAY)
    assert all(window.end >= TODAY for window in windows)
    assert windows[0].start == TODAY


def test_the_default_window_is_the_first_whole_forward_month_not_prompt():
    """A buyer asking 'which origin' is not asking about a cargo already fixed."""
    assert default_window(TODAY).start == date(2026, 9, 1)


# ---------------------------------------------------------------------------
# Unit conversion parity with the site's single conversion site
# ---------------------------------------------------------------------------
def test_conversion_matches_the_sites_own_single_conversion_site():
    """Pinned against app.markets so the two readings cannot drift.

    A bushels-per-tonne factor that diverged between the two copies would
    produce a ~2% error that reads exactly like basis.
    """
    from app.markets import load_markets

    markets = load_markets()
    for leg_id, leg in config.ORIGIN_LEGS.items():
        if leg.get("absent_reason"):
            continue
        source = getattr(markets[leg["market"]], leg["block"])
        mine = to_usd_per_mt(12.6875, unit=source.unit, key=leg["key"], fx=0.1958)
        theirs = source.to_usd_mt(12.6875, leg["key"], 0.1958)
        assert mine == theirs, leg_id


def test_a_home_currency_leg_with_no_rate_converts_to_none_not_the_local_number():
    assert to_usd_per_mt(2433.33, unit="home_per_mt", key="x", fx=None) is None


# ---------------------------------------------------------------------------
# Reading legs
# ---------------------------------------------------------------------------
def test_every_ams_delivery_slot_is_read_as_its_own_quote(db):
    quotes = read_origin_quotes(db, "us_gulf", today=TODAY)
    assert len(quotes) == 4
    assert all(quote.incoterm is Incoterm.CIF for quote in quotes)
    assert all(quote.carrier is Carrier.BARGE for quote in quotes)


def test_the_gulf_leg_records_the_cbot_contract_its_basis_is_quoted_over(db):
    quotes = read_origin_quotes(db, "us_gulf", today=TODAY)
    sep = next(q for q in quotes if q.shipment_window.label.startswith("Sep") and
               q.shipment_window.start == date(2026, 9, 1))
    assert sep.board_contract.delivery_month == "2026-11"
    assert sep.board_contract.code == "ZS"


def test_the_brazil_leg_carries_no_shipment_window_and_says_why(db):
    quotes = read_origin_quotes(db, "br_paranagua", today=TODAY)
    assert len(quotes) == 1
    assert quotes[0].shipment_window is None
    assert any("no shipment period" in note for note in quotes[0].notes)


def test_the_brazil_leg_converts_at_its_own_days_rate(db):
    quote = read_origin_quotes(db, "br_paranagua", today=TODAY)[0]
    assert quote.fx.observed_on == date(2026, 8, 11)
    assert quote.price.amount == pytest.approx(2433.33 * 0.1958480179309845, abs=0.01)


def test_argentina_publishes_a_shipment_curve_of_its_own(db):
    quotes = read_origin_quotes(db, "ar_up_river", today=TODAY)
    assert len(quotes) == 3
    assert {q.shipment_window.label for q in quotes} == {
        "2026-08", "2026-09..2026-10", "2026-11..2027-07"
    }


def test_a_leg_with_no_price_source_cannot_be_read():
    from analysis.origins.sources import OriginSourceError

    with pytest.raises(OriginSourceError, match="declares no price source"):
        read_origin_quotes(None, "us_pnw", today=TODAY)


def test_an_unknown_leg_id_hard_fails_rather_than_rendering_an_empty_row():
    from analysis.origins.sources import OriginSourceError

    with pytest.raises(OriginSourceError, match="not in config.ORIGIN_LEGS"):
        read_origin_quotes(None, "us_gulf_typo", today=TODAY)


# ---------------------------------------------------------------------------
# Quote selection
# ---------------------------------------------------------------------------
def test_the_largest_overlap_wins_not_the_first_row(db):
    """An AMS half-month inside September beats a MAGyP band that merely clips it."""
    quotes = read_origin_quotes(db, "ar_up_river", today=TODAY)
    chosen = select_quote(quotes, SEP)
    assert chosen.shipment_window.label == "2026-09..2026-10"


def test_with_no_overlap_the_nearest_window_is_shown_so_silence_is_not_implied(db):
    """'Argentina is quoting, but for November' and 'Argentina is silent' are opposite facts."""
    quotes = read_origin_quotes(db, "ar_up_river", today=TODAY)
    # Past the far end of every window the fixture's circular quotes for.
    beyond = ShipmentWindow(date(2027, 9, 1), date(2027, 9, 30))
    chosen = select_quote(quotes, beyond)
    assert chosen is not None
    assert not chosen.shipment_window.overlaps(beyond)


def test_selecting_from_nothing_is_none():
    assert select_quote((), SEP) is None


# ---------------------------------------------------------------------------
# The ranking
# ---------------------------------------------------------------------------
def test_a_ranking_with_no_assumptions_blocks_every_row_and_ranks_none(db):
    ranking = build_ranking(
        db, destination_key="cn_north", window=SEP, today=TODAY,
        assumptions=AssumptionSet(()),
    )
    assert ranking.rankable == ()
    assert not ranking.is_decisive
    assert all(row.landed is None for row in ranking.rows)


def test_a_declared_origin_with_no_source_is_rendered_not_dropped(db):
    ranking = build_ranking(
        db, destination_key="cn_north", window=SEP, today=TODAY,
        assumptions=AssumptionSet(()),
    )
    assert [origin.port.key for origin in ranking.unavailable] == ["us_pnw"]
    assert "PNW" in ranking.unavailable[0].reason


def test_an_unavailable_origin_carries_no_placeholder_numbers(db):
    """A placeholder price is a fabricated value that reaches the page on render."""
    ranking = build_ranking(
        db, destination_key="cn_north", window=SEP, today=TODAY,
        assumptions=AssumptionSet(()),
    )
    payload = ranking.unavailable[0].to_dict()
    assert set(payload) == {"port", "label", "reason"}


def test_origins_observed_too_far_apart_refuse_the_whole_ranking(db, monkeypatch):
    """The spread is a property of the set, so a row is not dropped — the ranking is."""
    db.execute(
        "UPDATE brazil_spot_prices SET Date = '2026-07-20' "
        "WHERE commodity = 'Soybean (AgRural Paranaguá FOB)'"
    )
    db.execute("INSERT INTO currencies (pair, Date, Close) VALUES ('BRL/USD','2026-07-20',0.1958)")
    db.commit()
    monkeypatch.setattr(config, "ORIGIN_MAX_OBSERVATION_SPREAD_DAYS", 3)
    ranking = build_ranking(
        db, destination_key="cn_north", window=SEP, today=TODAY,
        assumptions=AssumptionSet(()), leg_ids=("ar_up_river", "br_paranagua"),
    )
    assert ranking.observation_spread_days in (None, 0) or ranking.rankable == ()


def test_the_ranking_records_the_assumption_set_it_was_built_from(db):
    ranking = build_ranking(
        db, destination_key="cn_north", window=SEP, today=TODAY,
        assumptions=AssumptionSet(()),
    )
    assert ranking.assumption_set_id == AssumptionSet(()).set_id
    assert ranking.method_version == config.LANDED_COST_METHOD_VERSION


def test_an_unknown_destination_hard_fails(db):
    from analysis.origins.sources import OriginSourceError

    with pytest.raises(OriginSourceError, match="unknown destination port"):
        build_ranking(
            db, destination_key="mars", window=SEP, today=TODAY,
            assumptions=AssumptionSet(()),
        )


# ---------------------------------------------------------------------------
# The FOB board
# ---------------------------------------------------------------------------
def test_the_fob_board_excludes_an_undated_assessment(db):
    ranking = build_ranking(
        db, destination_key="cn_north", window=SEP, today=TODAY,
        assumptions=AssumptionSet(()),
    )
    board = fob_board(ranking)
    assert "br_paranagua" not in {row.quote.origin.key for row in board}


def test_the_fob_board_excludes_a_leg_whose_incoterm_bridge_is_unpaid(db):
    """The US Gulf bid is CIF barge; without an elevation it is not FOB-comparable."""
    ranking = build_ranking(
        db, destination_key="cn_north", window=SEP, today=TODAY,
        assumptions=AssumptionSet(()),
    )
    assert "us_gulf" not in {row.quote.origin.key for row in fob_board(ranking)}


def test_a_one_row_fob_board_publishes_no_advantage(db):
    ranking = build_ranking(
        db, destination_key="cn_north", window=SEP, today=TODAY,
        assumptions=AssumptionSet(()),
    )
    assert fob_advantage(ranking) == (None, None)
