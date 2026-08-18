"""Reading stored rows as named contracts, and refusing a curve that is not one session.

The regression this file exists for: ``forward_curve`` is keyed
``(commodity, contract_month, fetched_date)`` and was never deleted from, so two
runs on one day left the earlier run's legs standing. The committed history for
2026-08-11 carries exactly that — six Soybean legs stamped that session and
``ZSN27.CBT`` left over undated. The fixture below is that shape.
"""

from __future__ import annotations

import sqlite3
from datetime import date

import pytest

from analysis.futures.curve import analyse_curve, build_histories
from analysis.futures.providers import SqliteQuoteProvider, describe_provider, open_provider
from pipeline import schema

AS_OF = date(2026, 8, 18)


@pytest.fixture
def conn() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    for ddl in schema.ALL_SCHEMAS:
        connection.execute(ddl)
    yield connection
    connection.close()


def insert(conn, rows):
    conn.executemany(
        "INSERT INTO forward_curve "
        "(commodity, contract_month, label, ticker, close, observation_date, volume, "
        " open_interest, fetched_date) VALUES (?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()


COHERENT = [
    ("Soybeans", "2026-11-01", "Nov 2026", "ZSX26.CBT", 1167.75, "2026-08-11", 4_210, None, "2026-08-11"),
    ("Soybeans", "2027-01-01", "Jan 2027", "ZSF27.CBT", 1183.00, "2026-08-11", 1_880, None, "2026-08-11"),
    ("Soybeans", "2027-03-01", "Mar 2027", "ZSH27.CBT", 1191.25, "2026-08-11", 640, None, "2026-08-11"),
]


def test_rows_resolve_to_named_contracts_with_expiry_metadata(conn):
    insert(conn, COHERENT)
    observation = open_provider(conn).curve("Soybeans", as_of=AS_OF)
    assert [leg.contract.symbol for leg in observation.legs] == ["ZSX26", "ZSF27", "ZSH27"]
    front = observation.legs[0]
    assert front.contract.last_trade == date(2026, 11, 13)
    assert front.contract.first_notice == date(2026, 10, 30)
    assert front.price_type.value == "delayed_close"
    assert front.is_settlement_proven is False
    assert front.price_label == "delayed close"
    assert front.volume == 4_210
    assert front.open_interest is None


def test_a_coherent_curve_is_coherent(conn):
    insert(conn, COHERENT)
    observation = open_provider(conn).curve("Soybeans", as_of=AS_OF)
    assert observation.coherent is True
    assert observation.coherence_note == ""
    assert observation.observation_date == date(2026, 8, 11)


def test_an_undated_leg_from_an_earlier_run_is_dropped_and_breaks_coherence(conn):
    """The 2026-08-11 shape, exactly."""
    insert(conn, [
        *COHERENT,
        ("Soybeans", "2027-07-01", "Jul 2027", "ZSN27.CBT", 1206.25, None, None, None, "2026-08-11"),
    ])
    observation = open_provider(conn).curve("Soybeans", as_of=AS_OF)
    assert [leg.contract.symbol for leg in observation.legs] == ["ZSX26", "ZSF27", "ZSH27"]
    assert observation.coherent is False
    assert "ZSN27" in observation.coherence_note
    assert "undated" in observation.coherence_note


def test_a_leg_from_an_earlier_session_is_dropped_and_named(conn):
    insert(conn, [
        *COHERENT,
        ("Soybeans", "2027-05-01", "May 2027", "ZSK27.CBT", 1200.0, "2026-08-10", None, None, "2026-08-11"),
    ])
    observation = open_provider(conn).curve("Soybeans", as_of=AS_OF)
    assert "ZSK27" not in [leg.contract.symbol for leg in observation.legs]
    assert "2026-08-10" in observation.coherence_note
    assert observation.coherent is False


def test_a_ticker_that_disagrees_with_its_contract_month_is_refused(conn):
    insert(conn, [
        ("Soybeans", "2026-11-01", "Nov 2026", "ZSF27.CBT", 1167.75, "2026-08-11", None, None, "2026-08-11"),
        *COHERENT[1:],
    ])
    observation = open_provider(conn).curve("Soybeans", as_of=AS_OF)
    assert [leg.contract.symbol for leg in observation.legs] == ["ZSF27", "ZSH27"]


def test_no_rows_is_an_empty_observation_with_a_reason(conn):
    observation = open_provider(conn).curve("Soybeans", as_of=AS_OF)
    assert observation.is_empty
    assert observation.coherent is False
    assert "no curve rows" in observation.coherence_note


def test_freshness_grades_on_layer_ones_budget(conn):
    insert(conn, [
        ("Soybeans", "2026-11-01", "Nov 2026", "ZSX26.CBT", 1167.75, "2026-06-01", None, None, "2026-06-01"),
    ])
    observation = open_provider(conn).curve("Soybeans", as_of=AS_OF)
    assert observation.freshness.value == "stale"
    assert observation.age_days == (AS_OF - date(2026, 6, 1)).days


def test_only_the_latest_fetched_date_is_the_current_curve(conn):
    insert(conn, [
        ("Soybeans", "2026-11-01", "Nov 2026", "ZSX26.CBT", 1150.00, "2026-08-10", None, None, "2026-08-10"),
        *COHERENT,
    ])
    observation = open_provider(conn).curve("Soybeans", as_of=AS_OF)
    assert observation.legs[0].price == 1167.75
    assert observation.fetched_date == date(2026, 8, 11)


def test_curve_history_returns_only_coherent_snapshots_oldest_first(conn):
    insert(conn, [
        ("Soybeans", "2026-11-01", "Nov 2026", "ZSX26.CBT", 1150.0, "2026-08-10", None, None, "2026-08-10"),
        ("Soybeans", "2027-01-01", "Jan 2027", "ZSF27.CBT", 1165.0, "2026-08-10", None, None, "2026-08-10"),
        *COHERENT,
    ])
    history = open_provider(conn).curve_history("Soybeans", as_of=AS_OF, sessions=10)
    assert [snapshot.observation_date for snapshot in history] == [
        date(2026, 8, 10), date(2026, 8, 11),
    ]


def test_spread_history_needs_both_legs_and_withholds_a_thin_percentile(conn):
    insert(conn, [
        ("Soybeans", "2026-11-01", "Nov 2026", "ZSX26.CBT", 1150.0, "2026-08-10", None, None, "2026-08-10"),
        ("Soybeans", "2027-01-01", "Jan 2027", "ZSF27.CBT", 1165.0, "2026-08-10", None, None, "2026-08-10"),
        *COHERENT,
    ])
    provider = open_provider(conn)
    current = provider.curve("Soybeans", as_of=AS_OF)
    history = provider.curve_history("Soybeans", as_of=AS_OF, sessions=10)
    spreads = build_histories(current, history)
    by_symbol = {entry.symbols: entry for entry in spreads}
    assert by_symbol["ZSX26-ZSF27"].sample_size == 2
    assert by_symbol["ZSX26-ZSF27"].percentile is None
    assert "at least 20" in by_symbol["ZSX26-ZSF27"].withheld_reason
    # ZSH27 never appears on 08-10, so that spread has one session, not a zero.
    assert by_symbol["ZSF27-ZSH27"].sample_size == 1


def test_percentile_is_computed_once_the_sample_is_deep_enough(conn):
    rows = []
    for day in range(1, 26):
        stamp = date(2026, 7, day).isoformat()
        rows.append(("Soybeans", "2026-11-01", "Nov 2026", "ZSX26.CBT", 1150.0, stamp, None, None, stamp))
        rows.append(("Soybeans", "2027-01-01", "Jan 2027", "ZSF27.CBT", 1150.0 + day, stamp, None, None, stamp))
    insert(conn, rows)
    provider = open_provider(conn)
    current = provider.curve("Soybeans", as_of=AS_OF)
    history = provider.curve_history("Soybeans", as_of=AS_OF, sessions=60)
    entry = build_histories(current, history)[0]
    assert entry.sample_size == 25
    # The newest session carries the widest spread in the sample, so it ranks
    # at the top — 24 below it and itself counted half.
    assert entry.percentile == pytest.approx(98.0)


def test_the_analysis_carries_the_coherence_verdict_through(conn):
    insert(conn, [
        *COHERENT,
        ("Soybeans", "2027-07-01", "Jul 2027", "ZSN27.CBT", 1206.25, None, None, None, "2026-08-11"),
    ])
    analysis = analyse_curve(open_provider(conn).curve("Soybeans", as_of=AS_OF), as_of=AS_OF)
    assert analysis.coherent is False
    assert analysis.open_interest_available is False
    assert analysis.volume_available is True
    assert "no honest way to derive it" in analysis.open_interest_note


def test_fx_rate_carries_its_own_observation_date(conn):
    conn.executemany(
        "INSERT INTO currencies (pair, Date, Open, High, Low, Close) VALUES (?,?,?,?,?,?)",
        [("BRL/USD", "2026-08-10", None, None, None, 0.1950),
         ("BRL/USD", "2026-08-11", None, None, None, 0.1958)],
    )
    conn.commit()
    assert open_provider(conn).fx_rate("BRL/USD", on=AS_OF) == (date(2026, 8, 11), 0.1958)
    assert open_provider(conn).fx_rate("ZAR/USD", on=AS_OF) is None


def test_a_database_without_the_liquidity_columns_reports_them_as_unknown():
    """A DB predating the migration answers 'never learned', not zero."""
    legacy = sqlite3.connect(":memory:")
    legacy.execute(
        "CREATE TABLE forward_curve (commodity TEXT, contract_month TEXT, label TEXT, "
        "ticker TEXT, close REAL, observation_date TEXT, fetched_date TEXT)"
    )
    legacy.execute(
        "INSERT INTO forward_curve VALUES ('Soybeans','2026-11-01','Nov 2026','ZSX26.CBT',"
        "1167.75,'2026-08-11','2026-08-11')"
    )
    legacy.commit()
    observation = SqliteQuoteProvider(conn=legacy).curve("Soybeans", as_of=AS_OF)
    assert observation.legs[0].volume is None
    assert observation.legs[0].open_interest is None
    legacy.close()


def test_a_database_that_never_ran_the_pipeline_reports_emptiness_not_an_exception():
    """No ``forward_curve`` table at all is the same fact as no rows in it, and
    must not take the page down with it."""
    bare = sqlite3.connect(":memory:")
    provider = SqliteQuoteProvider(conn=bare)
    observation = provider.curve("Soybeans", as_of=AS_OF)
    assert observation.is_empty
    assert "no curve rows" in observation.coherence_note
    assert provider.curve_history("Soybeans", as_of=AS_OF) == ()
    assert provider.continuous("Soybeans", as_of=AS_OF) is None
    assert provider.fx_rate("BRL/USD", on=AS_OF) is None
    bare.close()


def test_the_provider_declares_that_it_is_delayed_and_not_authoritative(conn):
    described = describe_provider(open_provider(conn).provider)
    assert described["delayed"] is True
    assert described["settlement_authoritative"] is False
    assert "never as an official settlement" in described["note"]


def test_the_stored_front_month_series_is_labelled_as_the_providers_own_roll(conn):
    conn.executemany(
        "INSERT INTO prices (commodity, Date, Open, High, Low, Close, Volume) VALUES (?,?,?,?,?,?,?)",
        [("Soybeans", f"2026-08-{day:02d}", None, None, None, 1100.0 + day, None) for day in range(1, 12)],
    )
    conn.commit()
    series = open_provider(conn).continuous("Soybeans", as_of=AS_OF)
    assert series.roll_method.value == "provider_front_month"
    assert series.is_hedgeable is False
    assert series.contract_by_date == ()      # the provider does not say
    assert "does not publish its roll dates" in series.adjustment_note
