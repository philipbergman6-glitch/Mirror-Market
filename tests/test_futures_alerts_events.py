"""Exposure alerts and the release calendar.

The distinction these tests exist to hold: an alert here is about *this book*,
not about the market. A moving average crossing is not an alert; a contract
this hedge holds reaching first notice day is.
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta

import pytest

from analysis.futures.alerts import (
    Alert,
    basis_alerts,
    build_alerts,
    curve_alerts,
    expiry_alerts,
    limit_alerts,
    roll_alerts,
    slippage_alerts,
    sort_alerts,
    staleness_alerts,
)
from analysis.futures.curve import analyse_curve
from analysis.futures.domain import Freshness, Side, named_contract
from analysis.futures.events import (
    EVENT_SOURCES,
    EventConfidence,
    build_calendar,
    events_within,
    next_monthly,
    next_weekly,
)
from analysis.futures.hedge import PhysicalUnit, build_hedge, propose_hedge, size_leg
from analysis.futures.positions import (
    Book,
    Fill,
    FuturesPosition,
    Limit,
    PhysicalPosition,
    value_book,
)
from analysis.futures.providers import CurveObservation
from pipeline import schema
from tests.test_futures_hedge import AS_OF, BEANS, curve, exposure, quote

# ---------------------------------------------------------------------------
# Alert plumbing
# ---------------------------------------------------------------------------


def test_an_alert_must_carry_a_known_severity():
    with pytest.raises(ValueError, match="is not one of"):
        Alert(kind="x", severity="critical", subject="ZSX26", message="")


def test_alerts_sort_most_severe_first():
    ordered = sort_alerts((
        Alert("a", "info", "z", "m"),
        Alert("b", "alert", "y", "m"),
        Alert("c", "warning", "x", "m"),
    ))
    assert [a.severity for a in ordered] == ["alert", "warning", "info"]


# ---------------------------------------------------------------------------
# Expiry and roll — the two dates a physical hedger has to act on
# ---------------------------------------------------------------------------


def hedge(**kwargs):
    return propose_hedge(exposure(Side.LONG, **kwargs), curve("Soybeans", BEANS), as_of=AS_OF)


def test_a_contract_inside_the_expiry_window_raises_an_alert():
    """ZSX26 last trades 13 Nov 2026; from 2 Nov that is 9 sessions."""
    alerts = expiry_alerts(hedge(), as_of=date(2026, 11, 2))
    assert [a.kind for a in alerts] == ["approaching_expiry"]
    assert alerts[0].severity == "warning"
    assert "session(s) to last trade" in alerts[0].message
    assert alerts[0].detail["last_trade"] == "2026-11-13"


def test_expiry_escalates_to_alert_in_the_last_three_sessions():
    assert expiry_alerts(hedge(), as_of=date(2026, 11, 11))[0].severity == "alert"


def test_a_hedge_on_an_already_expired_contract_says_so_plainly():
    alerts = expiry_alerts(hedge(), as_of=date(2026, 11, 20))
    assert alerts[0].severity == "alert"
    assert "references an expired contract" in alerts[0].message


def test_a_far_dated_contract_raises_nothing():
    assert expiry_alerts(hedge(), as_of=AS_OF) == ()


def test_a_product_with_no_encoded_expiry_is_refused_a_hedge_month_rather_than_guessed():
    """Sugar has no encoded rule, so no month can be shown to be still trading."""
    sugar = curve("Sugar", [quote("Sugar", 2027, 3, 18.5), quote("Sugar", 2027, 5, 18.8)])
    proposal = propose_hedge(
        exposure(Side.LONG, commodity="Sugar", basis_usd_per_mt=0.0), sugar, as_of=AS_OF,
    )
    assert proposal.legs == ()
    assert any(w.code == "no_hedge_month" for w in proposal.warnings)
    assert any("expiry rule is not encoded" in w.message for w in proposal.warnings)


def test_a_leg_with_no_encoded_expiry_warns_that_no_alert_can_fire_for_it():
    """Hedged in Sugar anyway (a leg named by hand): silence must not read as safety."""
    leg = size_leg(quote("Sugar", 2027, 3, 18.5), side=Side.SHORT, physical_mt=1_000)
    proposal = build_hedge(
        exposure(Side.LONG, commodity="Sugar", basis_usd_per_mt=0.0), (leg,), as_of=AS_OF,
    )
    alerts = expiry_alerts(proposal, as_of=AS_OF)
    assert alerts and alerts[0].severity == "warning"
    assert "not encoded" in alerts[0].message
    assert "track it manually" in alerts[0].message
    # And no roll alert can fire either, because there is no FND to compute.
    assert roll_alerts(proposal, as_of=AS_OF) == ()


def test_the_roll_alert_fires_on_first_notice_not_on_last_trade():
    """ZSX26: FND 30 Oct 2026, last trade 13 Nov. On 25 Oct the roll is due
    and the expiry alert has not yet fired — which is the whole point."""
    on = date(2026, 10, 25)
    rolls = roll_alerts(hedge(), as_of=on)
    assert rolls and rolls[0].kind == "roll_window"
    assert rolls[0].detail["first_notice"] == "2026-10-30"
    assert "roll or close before then" in rolls[0].message
    assert expiry_alerts(hedge(), as_of=on) == ()


def test_being_past_first_notice_is_an_alert_about_delivery():
    alerts = roll_alerts(hedge(), as_of=date(2026, 11, 3))
    assert alerts[0].severity == "alert"
    assert "exposed to delivery" in alerts[0].message


# ---------------------------------------------------------------------------
# Slippage and basis
# ---------------------------------------------------------------------------


def test_a_whole_hedge_raises_no_slippage():
    assert slippage_alerts(hedge()) == ()


def test_coverage_drift_beyond_the_tolerance_names_the_unhedged_tonnes():
    """500 MT against a 136.08 MT contract: 3.67 -> 4 lots, over-hedged."""
    alerts = slippage_alerts(hedge(quantity=500))
    assert alerts and alerts[0].kind == "hedge_slippage"
    assert "over-hedged" in alerts[0].message
    assert alerts[0].detail["residual_mt"] < 0


def test_an_under_hedge_says_unhedged_rather_than_over_hedged():
    """300 MT / 136.08 = 2.20 lots -> 2, leaving 28 MT open."""
    alerts = slippage_alerts(hedge(quantity=300))
    assert alerts and "unhedged" in alerts[0].message
    assert alerts[0].detail["residual_mt"] > 0


def test_a_basis_alert_needs_the_trader_to_have_set_a_level():
    """No default band: a 'normal' basis is a market judgement, not ours."""
    assert basis_alerts(hedge(), current_basis_usd_mt=-40.0) == ()


def test_a_basis_below_the_floor_is_priced_in_dollars_against_the_position():
    alerts = basis_alerts(hedge(), current_basis_usd_mt=-30.0, floor_usd_mt=-20.0)
    assert alerts and alerts[0].severity == "alert"
    assert "100,000 USD against the position" in alerts[0].message


def test_a_basis_above_the_ceiling_is_a_warning_not_an_alert():
    alerts = basis_alerts(hedge(), current_basis_usd_mt=5.0, ceiling_usd_mt=0.0)
    assert alerts and alerts[0].severity == "warning"


def test_an_unknown_basis_raises_nothing_rather_than_assuming_zero():
    assert basis_alerts(hedge(), current_basis_usd_mt=None, floor_usd_mt=0.0) == ()


# ---------------------------------------------------------------------------
# Curve and staleness
# ---------------------------------------------------------------------------


def inverted():
    return curve("Soybeans", [
        quote("Soybeans", 2026, 11, 1200.0), quote("Soybeans", 2027, 1, 1150.0),
    ])


def test_an_inverted_curve_says_what_it_costs_a_storage_position():
    alerts = curve_alerts(inverted())
    assert alerts and alerts[0].kind == "curve_inversion"
    assert "no longer being paid to carry" in alerts[0].message
    assert alerts[0].detail["front"] == "ZSX26"


def test_an_incoherent_curve_suppresses_the_inversion_reading_rather_than_reporting_it():
    """Two sessions stitched together can manufacture an inversion. The alert
    that fires must be the data one, not the market one."""
    analysis = curve("Soybeans", [
        quote("Soybeans", 2026, 11, 1200.0), quote("Soybeans", 2027, 1, 1150.0),
    ], coherent=False)
    alerts = curve_alerts(analysis)
    assert [a.kind for a in alerts] == ["data_stale"]
    assert "every spread and every structure reading on it is unreliable" in alerts[0].message


def stale_curve(observed: date, freshness: Freshness = Freshness.STALE):
    legs = (quote("Soybeans", 2026, 11, 1167.75, observed=observed),)
    return analyse_curve(
        CurveObservation(
            commodity="Soybeans", legs=legs, observation_date=observed, fetched_date=observed,
            coherent=True, freshness=freshness, age_days=(AS_OF - observed).days,
        ),
        as_of=AS_OF,
    )


def test_a_curve_past_its_layers_budget_says_every_number_on_it_is_struck_on_a_stale_price():
    alerts = staleness_alerts(stale_curve(date(2026, 6, 1)))
    assert alerts and alerts[0].severity == "alert"
    assert "past this layer's recency budget" in alerts[0].message


def test_a_curve_merely_behind_cadence_is_a_warning_not_an_alert():
    alerts = staleness_alerts(stale_curve(AS_OF - timedelta(days=6), Freshness.BEHIND))
    assert alerts and alerts[0].severity == "warning"
    assert "inside the layer's budget" in alerts[0].message


def test_a_current_curve_raises_nothing():
    assert staleness_alerts(stale_curve(AS_OF, Freshness.CURRENT)) == ()


# ---------------------------------------------------------------------------
# Limits, and assembly
# ---------------------------------------------------------------------------


def valued_book(limit_max: float = 1_000.0):
    book = Book(
        physical=(PhysicalPosition(
            commodity="Soybeans", quantity=30_000, unit=PhysicalUnit.METRIC_TON, side=Side.LONG,
            average_cost_usd_mt=400.0, mark_contract="ZSX26", current_basis_usd_mt=-12.5,
        ),),
        futures=(FuturesPosition(
            contract=named_contract("Soybeans", 2026, 11),
            fills=(Fill(date(2026, 8, 10), Side.SHORT, 73, 1150.0),),
        ),),
        limits=(Limit("net_mt", "Soybeans", limit_max),),
    )
    return value_book(
        book, as_of=AS_OF,
        quote_for=lambda commodity, symbol: (
            quote("Soybeans", 2026, 11, 1167.75) if symbol == "ZSX26" else None
        ),
    )


def test_a_limit_breach_becomes_an_alert_that_names_the_line_and_the_reading():
    alerts = limit_alerts(valued_book())
    assert alerts and alerts[0].kind == "limit_breach"
    assert alerts[0].subject == "net_mt:Soybeans"
    assert "limit for Soybeans is 1,000" in alerts[0].message


def test_a_book_inside_its_limits_raises_nothing():
    assert limit_alerts(valued_book(limit_max=1_000_000)) == ()


def test_build_alerts_joins_every_source_and_sorts_by_severity():
    alerts = build_alerts(
        as_of=date(2026, 11, 2),
        proposals=(hedge(quantity=500),),
        curves=(inverted(),),
        valuation=valued_book(),
        basis_bounds={"Soybeans": (-10.0, None)},
    )
    kinds = {a.kind for a in alerts}
    assert {"approaching_expiry", "roll_window", "hedge_slippage", "basis_breach",
            "limit_breach", "curve_inversion"} <= kinds
    severities = [a.severity for a in alerts]
    assert severities == sorted(severities, key=lambda s: ["alert", "warning", "info"].index(s))


def test_the_same_fact_raised_from_two_directions_appears_once():
    """A stale commodity can raise from its curve and from a position marked
    against it. One line per fact."""
    stale = stale_curve(date(2026, 6, 1))
    alerts = build_alerts(as_of=AS_OF, curves=(stale, stale))
    assert len([a for a in alerts if a.kind == "data_stale"]) == 1


def test_an_unmarkable_position_is_itself_a_staleness_alert():
    book = Book(futures=(FuturesPosition(
        contract=named_contract("Soybean Oil", 2026, 12),
        fills=(Fill(date(2026, 8, 10), Side.LONG, 5, 68.0),),
    ),))
    valuation = value_book(book, as_of=AS_OF, quote_for=lambda c, s: None)
    alerts = build_alerts(as_of=AS_OF, valuation=valuation)
    assert alerts and alerts[0].kind == "data_stale"
    assert "could not be marked" in alerts[0].message


def test_no_inputs_raises_no_alerts():
    assert build_alerts(as_of=AS_OF) == ()


# ---------------------------------------------------------------------------
# Event calendar
# ---------------------------------------------------------------------------


def test_every_calendar_entry_belongs_to_a_layer_this_project_ingests():
    """The scope rule. A calendar of reports we do not read is a research note."""
    import config

    layers = {row[0] for row in config.PRODUCTION_LAYERS}
    for source in EVENT_SOURCES:
        assert source.layer in layers, f"{source.key} names a layer this project does not run"


def test_no_report_we_do_not_ingest_is_listed():
    keys = {source.key for source in EVENT_SOURCES}
    for absent in ("nopa", "statscan", "abares", "cftc_cit"):
        assert absent not in keys


def test_the_weekly_rule_lands_on_the_named_weekday():
    """COT is a Friday release; 18 Aug 2026 is a Tuesday."""
    assert next_weekly(4, date(2026, 8, 18)) == date(2026, 8, 21)
    # Asked on the day itself, the answer is that day.
    assert next_weekly(4, date(2026, 8, 21)) == date(2026, 8, 21)


def test_a_rule_date_landing_on_a_holiday_shifts_forward():
    """Monday 25 May 2026 is Memorial Day, so a Monday release moves to the 26th."""
    assert next_weekly(0, date(2026, 5, 25)) == date(2026, 5, 26)


def test_the_monthly_rule_finds_the_next_business_day_inside_its_window():
    # WASDE window is the 9th-12th; 9 Sep 2026 is a Wednesday.
    assert next_monthly((9, 12), date(2026, 9, 1)) == date(2026, 9, 9)
    # Asked after the window closes, it rolls to the next month.
    assert next_monthly((9, 12), date(2026, 9, 13)) == date(2026, 10, 9)


@pytest.fixture
def conn():
    connection = sqlite3.connect(":memory:")
    for ddl in schema.ALL_SCHEMAS:
        connection.execute(ddl)
    yield connection
    connection.close()


def test_the_calendar_works_with_no_database_at_all():
    """A fresh clone has no observations; the schedule still stands."""
    events = build_calendar(None, as_of=AS_OF)
    assert events
    assert all(event.confidence is EventConfidence.RULE for event in events)
    assert all(event.last_observed is None for event in events)


def test_events_are_ordered_soonest_first_and_bounded_by_the_horizon():
    events = build_calendar(None, as_of=AS_OF, horizon_days=10)
    assert [e.expected_date for e in events] == sorted(e.expected_date for e in events)
    assert all(e.days_away <= 10 for e in events)
    assert len(events) < len(build_calendar(None, as_of=AS_OF, horizon_days=45))


def test_an_observed_date_is_read_from_our_own_rows(conn):
    conn.executemany(
        "INSERT INTO cot (commodity, Date, commercial_long) VALUES (?,?,?)",
        [("Soybeans", stamp, 1)
         for stamp in ("2026-07-17", "2026-07-24", "2026-07-31", "2026-08-07", "2026-08-14")],
    )
    conn.commit()
    event = next(e for e in build_calendar(conn, as_of=AS_OF) if e.source.key == "cot")
    assert event.last_observed == date(2026, 8, 14)
    assert event.observed_gap_days == 7
    assert event.stale is False


def test_two_cadences_of_silence_marks_the_rule_date_as_not_evidence(conn):
    conn.executemany(
        "INSERT INTO cot (commodity, Date, commercial_long) VALUES (?,?,?)",
        [("Soybeans", d, 1) for d in ("2026-06-19", "2026-06-26", "2026-07-03")],
    )
    conn.commit()
    event = next(e for e in build_calendar(conn, as_of=AS_OF) if e.source.key == "cot")
    assert event.stale is True
    assert "a rule, not evidence" in event.note
    # And the expected date keeps ticking forward regardless, which is the trap
    # the observed column exists to expose.
    assert event.expected_date > AS_OF


def test_a_source_with_no_observation_table_still_gets_its_rule_date(conn):
    event = next(e for e in build_calendar(conn, as_of=AS_OF) if e.source.key == "nass_crush")
    assert event.last_observed is None
    assert event.stale is False


def test_events_within_narrows_an_existing_calendar():
    events = build_calendar(None, as_of=AS_OF, horizon_days=45)
    assert all(e.days_away <= 7 for e in events_within(events, 7))


def test_a_seasonal_source_carries_its_own_caveat():
    progress = next(s for s in EVENT_SOURCES if s.key == "crop_progress")
    assert "not published between roughly December and March" in progress.seasonal_note
    event = next(e for e in build_calendar(None, as_of=AS_OF) if e.source.key == "crop_progress")
    assert event.to_dict()["seasonal_note"] == progress.seasonal_note
