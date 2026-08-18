"""The proposed-trade ticket, and the continuous series that is not a contract.

Two rules are asserted here above everything else: a ticket says it was not
routed, in every format it emits; and a stitched series never silently stands
in for a named contract.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta

import pytest

from analysis.futures.continuous import (
    MIN_SESSIONS,
    active_contract,
    build_continuous,
    describe_provider_series,
    roll_schedule,
)
from analysis.futures.domain import ContinuousSeries, NamedContract, RollMethod, Side
from analysis.futures.hedge import propose_crush_hedge, propose_hedge
from analysis.futures.providers import open_provider
from analysis.futures.scenarios import Scenario, futures_shock, run_scenario
from analysis.futures.ticket import (
    DISCLAIMER,
    NOT_ROUTED_BANNER,
    build_ticket,
    render_text,
)
from pipeline import schema
from tests.test_futures_hedge import AS_OF, BEANS, MEAL, OIL, curve, exposure, quote

GENERATED = datetime(2026, 8, 18, 21, 30, 0)


def proposal(**kwargs):
    return propose_hedge(exposure(Side.LONG, **kwargs), curve("Soybeans", BEANS), as_of=AS_OF)


def ticket(**kwargs):
    return build_ticket(proposal(**kwargs), generated_at=GENERATED)


# ---------------------------------------------------------------------------
# It is a proposal, and it says so
# ---------------------------------------------------------------------------


def test_the_not_routed_banner_is_the_first_thing_in_every_format():
    made = ticket()
    assert made.status == NOT_ROUTED_BANNER
    assert made.to_dict()["status"] == NOT_ROUTED_BANNER
    text = render_text(made)
    assert NOT_ROUTED_BANNER in text.split("\n")[1]
    assert json.loads(made.to_json())["status"] == NOT_ROUTED_BANNER


def test_the_disclaimer_denies_both_routing_and_settlement_authority():
    text = render_text(ticket())
    assert DISCLAIMER in text
    assert "has not been sent to any venue" in DISCLAIMER
    assert "no capability to do so" in DISCLAIMER
    assert "not proven exchange settlements" in DISCLAIMER


def test_no_field_anywhere_looks_like_an_order_instruction():
    payload = ticket().to_dict()
    for forbidden in ("order_type", "tif", "account", "venue", "route", "limit_price"):
        assert forbidden not in payload
        assert forbidden not in payload["lines"][0]


# ---------------------------------------------------------------------------
# What a ticket has to carry
# ---------------------------------------------------------------------------


def test_a_line_names_the_contract_not_the_commodity():
    line = ticket().to_dict()["lines"][0]
    assert line["instrument"] == "ZSX26"
    assert line["description"] == "CBOT Soybeans Nov 2026"
    assert line["side"] == "SELL"          # hedging a long physical
    assert line["contracts"] == 73


def test_quantity_is_given_in_contracts_and_in_tonnes():
    line = ticket().to_dict()["lines"][0]
    assert line["contracts"] == 73
    assert line["tonnes"] == pytest.approx(73 * 136.0777494, rel=1e-6)


def test_the_reference_price_carries_its_type_its_unit_and_its_session():
    line = ticket().to_dict()["lines"][0]
    assert line["reference_price"] == 1167.75
    assert line["reference_price_unit"] == "cents_per_bushel"
    assert line["reference_price_label"] == "delayed close"
    assert line["observation_date"] == AS_OF.isoformat()
    # 1167.75 c/bu x 0.367437 MT-factor = 429.0746 USD/MT
    assert line["reference_price_usd_mt"] == pytest.approx(429.07, abs=0.01)


def test_a_line_carries_both_termination_dates():
    line = ticket().to_dict()["lines"][0]
    assert line["last_trade"] == "2026-11-13"
    assert line["first_notice"] == "2026-10-30"


def test_notional_is_the_contract_size_arithmetic():
    """73 x 1167.75 c/bu x 5,000 bu / 100 = 4,262,287.50 USD."""
    assert ticket().to_dict()["lines"][0]["notional_usd"] == pytest.approx(4_262_287.50)


def test_coverage_and_residual_are_stated_rather_than_implied():
    payload = ticket().to_dict()
    assert payload["expected_coverage_pct"] == pytest.approx(99.34, abs=0.01)
    assert payload["residual_mt"] == pytest.approx(66.324, abs=0.01)
    assert "unhedged" in render_text(ticket())


def test_every_input_the_sizing_depended_on_is_written_down():
    assumptions = " | ".join(ticket().assumptions)
    assert "method version" in assumptions
    assert "contract counts rounded nearest" in assumptions
    assert "basis convention: basis_over_futures" in assumptions
    assert "-12.50 USD/MT (AMS CIF NOLA)" in assumptions
    assert "contract size 5,000 bushel = 136.0777 MT" in assumptions
    assert "reference price is a delayed close" in assumptions


def test_an_exposure_with_no_basis_says_so_in_the_assumptions_rather_than_assuming_zero():
    assumptions = " | ".join(ticket(basis_usd_per_mt=None).assumptions)
    assert "no basis level supplied" in assumptions


def test_warnings_are_carried_verbatim_at_their_own_severity():
    made = ticket(quantity=500)
    codes = {w["code"] for w in made.warnings}
    assert "residual_exposure" in codes
    text = render_text(made)
    assert "WARNINGS" in text
    for warning in made.warnings:
        assert f"[{warning['severity'].upper()}]" in text
        assert warning["message"] in text


def test_scenarios_ride_along_when_they_are_given():
    hedge = proposal()
    result = run_scenario(hedge, Scenario("board +5%", (futures_shock(5.0),)))
    made = build_ticket(hedge, generated_at=GENERATED, scenarios=(result,))
    assert made.to_dict()["scenarios"][0]["scenario"]["name"] == "board +5%"
    assert "SCENARIOS" in render_text(made)


def test_an_extra_assumption_from_the_user_is_appended_not_merged():
    made = build_ticket(
        proposal(), generated_at=GENERATED, extra_assumptions=("vessel nominated 12 Sep",),
    )
    assert made.assumptions[-1] == "vessel nominated 12 Sep"


def test_a_crush_ticket_labels_each_product_leg_with_its_yield():
    hedge = propose_crush_hedge(
        exposure(Side.LONG, quantity=1_000), curve("Soybeans", BEANS),
        curve("Soybean Meal", MEAL), curve("Soybean Oil", OIL), as_of=AS_OF,
    )
    made = build_ticket(hedge, generated_at=GENERATED)
    instruments = [line["instrument"] for line in made.to_dict()["lines"]]
    assert instruments[0].startswith("ZS")
    assert any(i.startswith("ZM") for i in instruments)
    assert any(i.startswith("ZL") for i in instruments)
    assert all(
        line["cross_hedge_note"] for line in made.to_dict()["lines"]
        if not line["instrument"].startswith("ZS")
    )


def test_a_hedge_with_no_selectable_month_still_produces_a_readable_ticket():
    sugar = curve("Sugar", [quote("Sugar", 2027, 3, 18.5)])
    made = build_ticket(
        propose_hedge(
            exposure(Side.LONG, commodity="Sugar", basis_usd_per_mt=0.0), sugar, as_of=AS_OF,
        ),
        generated_at=GENERATED,
    )
    assert made.lines == ()
    assert "no contract month could be selected" in render_text(made)


# ---------------------------------------------------------------------------
# Reproducibility
# ---------------------------------------------------------------------------


def test_the_ticket_id_is_the_trade_not_the_moment_it_was_printed():
    early = build_ticket(proposal(), generated_at=datetime(2026, 8, 18, 9, 0))
    late = build_ticket(proposal(), generated_at=datetime(2026, 8, 18, 21, 0))
    assert early.ticket_id == late.ticket_id
    assert early.generated_at != late.generated_at


def test_a_different_trade_gets_a_different_id():
    assert ticket().ticket_id != ticket(quantity=5_000).ticket_id


def test_the_json_export_is_ordered_and_complete():
    payload = json.loads(ticket().to_json())
    assert list(payload)[:3] == ["status", "disclaimer", "ticket_id"]
    for key in ("exposure", "lines", "expected_coverage_pct", "assumptions", "warnings"):
        assert key in payload


# ---------------------------------------------------------------------------
# Continuous series: never a substitute for a named contract
# ---------------------------------------------------------------------------


def test_a_continuous_series_is_not_a_named_contract_and_is_never_hedgeable():
    series = ContinuousSeries(
        commodity="Soybeans", roll_method=RollMethod.CALENDAR_ROLL_DIFFERENCE,
        points=((date(2026, 8, 11), 1147.25),),
        adjustment_note="back-adjusted",
    )
    assert not isinstance(series, NamedContract)
    assert series.is_hedgeable is False
    assert describe_provider_series(series)["hedgeable"] is False


def test_the_roll_rule_leaves_the_front_before_first_notice():
    """ZSX26 last trades 13 Nov; the rule rolls out 5 sessions before that (6 Nov),
    which is after FND on 30 Oct — so the *rolled* series is still in ZSX26 then,
    and this is the number a reader has to be able to see."""
    assert active_contract("Soybeans", date(2026, 8, 18)).symbol == "ZSU26"
    assert active_contract("Soybeans", date(2026, 9, 9)).symbol == "ZSX26"


def test_a_product_with_no_encoded_expiry_has_no_roll_and_says_none():
    assert active_contract("Sugar", date(2026, 8, 18)) is None
    assert roll_schedule("Sugar", date(2026, 8, 10), date(2026, 8, 18)) == ()


def test_the_roll_schedule_names_the_contract_behind_every_session():
    schedule = roll_schedule("Soybeans", date(2026, 8, 26), date(2026, 9, 10))
    assert schedule[0] == (date(2026, 8, 26), "ZSU26")
    assert date(2026, 8, 29) not in [day for day, _ in schedule]       # a Saturday
    assert date(2026, 9, 7) not in [day for day, _ in schedule]        # Labor Day
    # ZSU26 last trades 14 Sep; five sessions before that, on 4 Sep, the rule
    # rolls into ZSX26 — ahead of first notice on 31 Aug for the September
    # contract, which is the whole point of rolling early.
    assert schedule[-1][1] == "ZSX26"
    assert dict(schedule)[date(2026, 9, 3)] == "ZSU26"
    assert dict(schedule)[date(2026, 9, 4)] == "ZSX26"


def test_a_short_history_is_withheld_rather_than_padded_with_the_provider_series():
    conn = sqlite3.connect(":memory:")
    for ddl in schema.ALL_SCHEMAS:
        conn.execute(ddl)
    conn.executemany(
        "INSERT INTO forward_curve (commodity, contract_month, label, ticker, close, "
        "observation_date, fetched_date) VALUES (?,?,?,?,?,?,?)",
        [("Soybeans", "2026-11-01", "Nov 2026", "ZSX26.CBT", 1160.0 + day,
          f"2026-08-{day:02d}", f"2026-08-{day:02d}") for day in (3, 4, 5)],
    )
    conn.commit()
    series = build_continuous(open_provider(conn), "Soybeans", as_of=AS_OF)
    assert series is None
    described = describe_provider_series(series)
    assert described["available"] is False
    assert f"shorter than {MIN_SESSIONS} sessions" in described["reason"]
    assert "does not pad it with the provider's own front-month series" in described["reason"]
    conn.close()


def test_an_unknown_adjustment_raises_rather_than_defaulting_to_unadjusted():
    conn = sqlite3.connect(":memory:")
    for ddl in schema.ALL_SCHEMAS:
        conn.execute(ddl)
    with pytest.raises(ValueError, match="unknown adjustment"):
        build_continuous(open_provider(conn), "Soybeans", as_of=AS_OF, adjustment="panama")
    conn.close()


def test_a_stitched_series_carries_its_method_and_its_warning():
    conn = sqlite3.connect(":memory:")
    for ddl in schema.ALL_SCHEMAS:
        conn.execute(ddl)
    # A fortnight of sessions inside ZSX26's own window, so no roll occurs.
    rows = []
    for offset, day in enumerate(range(1, 19)):
        stamp = date(2026, 9, 14) + timedelta(days=day)
        if stamp.weekday() >= 5:
            continue
        rows.append(("Soybeans", "2026-11-01", "Nov 2026", "ZSX26.CBT", 1160.0 + offset,
                     stamp.isoformat(), stamp.isoformat()))
        rows.append(("Soybeans", "2027-01-01", "Jan 2027", "ZSF27.CBT", 1180.0 + offset,
                     stamp.isoformat(), stamp.isoformat()))
    conn.executemany(
        "INSERT INTO forward_curve (commodity, contract_month, label, ticker, close, "
        "observation_date, fetched_date) VALUES (?,?,?,?,?,?,?)", rows,
    )
    conn.commit()
    series = build_continuous(
        open_provider(conn), "Soybeans", as_of=date(2026, 10, 1), adjustment="difference",
    )
    conn.close()
    if series is None:
        pytest.skip("fewer sessions than the floor on this calendar")
    assert series.roll_method is RollMethod.CALENDAR_ROLL_DIFFERENCE
    assert "NOT tradeable prices" in series.adjustment_note
    assert series.is_hedgeable is False
    assert len(series.contract_by_date) == len(series.points)


def _rolling_curve_db():
    """Sessions spanning the ZSU26 -> ZSX26 roll, both legs printed each day."""
    conn = sqlite3.connect(":memory:")
    for ddl in schema.ALL_SCHEMAS:
        conn.execute(ddl)
    rows = []
    day = date(2026, 8, 24)
    step = 0
    while day <= date(2026, 9, 11):
        if day.weekday() < 5 and day != date(2026, 9, 7):     # skip Labor Day
            for month, label, ticker, base in (
                ("2026-09-01", "Sep 2026", "ZSU26.CBT", 1100.0),
                ("2026-11-01", "Nov 2026", "ZSX26.CBT", 1130.0),
            ):
                rows.append(("Soybeans", month, label, ticker, base + step,
                             day.isoformat(), day.isoformat()))
            step += 1
        day += timedelta(days=1)
    conn.executemany(
        "INSERT INTO forward_curve (commodity, contract_month, label, ticker, close, "
        "observation_date, fetched_date) VALUES (?,?,?,?,?,?,?)", rows,
    )
    conn.commit()
    return conn


def test_an_unadjusted_series_shows_the_roll_gap_rather_than_hiding_it():
    """The rule rolls on 4 Sep: 1100-series before, 1130-series from then on,
    so the unadjusted series steps +30 on the roll day. That step is not a move
    and the series says as much in its method."""
    conn = _rolling_curve_db()
    series = build_continuous(
        open_provider(conn), "Soybeans", as_of=date(2026, 9, 11), adjustment="unadjusted",
    )
    conn.close()
    assert series.roll_method is RollMethod.CALENDAR_ROLL_UNADJUSTED
    assert series.roll_dates == (date(2026, 9, 4),)
    points = dict(series.points)
    assert points[date(2026, 9, 3)] == pytest.approx(1108.0)     # ZSU26
    assert points[date(2026, 9, 4)] == pytest.approx(1139.0)     # ZSX26, +31 with the day's drift
    assert "every level is a real price" in series.adjustment_note


def test_a_difference_adjusted_series_uses_the_same_session_gap_not_consecutive_prints():
    """On 4 Sep the two contracts print 1109 and 1139, a same-session gap of +30.
    Pre-roll levels shift by exactly that; using consecutive prints instead would
    fold the day's own +1 move into the adjustment."""
    conn = _rolling_curve_db()
    series = build_continuous(
        open_provider(conn), "Soybeans", as_of=date(2026, 9, 11), adjustment="difference",
    )
    conn.close()
    points = dict(series.points)
    assert points[date(2026, 9, 3)] == pytest.approx(1108.0 + 30.0)
    assert points[date(2026, 9, 4)] == pytest.approx(1139.0)     # newest segment untouched
    # Differences are continuous across the roll: +1 a session, no +31 step.
    ordered = [price for _, price in series.points]
    assert max(b - a for a, b in zip(ordered, ordered[1:], strict=False)) == pytest.approx(1.0)
    assert "NOT tradeable prices" in series.adjustment_note


def test_a_ratio_adjusted_series_scales_instead_of_shifting():
    conn = _rolling_curve_db()
    series = build_continuous(
        open_provider(conn), "Soybeans", as_of=date(2026, 9, 11), adjustment="ratio",
    )
    conn.close()
    points = dict(series.points)
    factor = 1139.0 / 1109.0
    assert points[date(2026, 9, 3)] == pytest.approx(1108.0 * factor)
    assert points[date(2026, 9, 4)] == pytest.approx(1139.0)
    assert series.roll_method is RollMethod.CALENDAR_ROLL_RATIO
