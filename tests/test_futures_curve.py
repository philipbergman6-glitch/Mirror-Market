"""Term structure over named contracts: spreads, carry, structure, month choice."""

from __future__ import annotations

from datetime import date

import pytest

from analysis.futures.curve import (
    BUSINESS_DAYS_PER_YEAR,
    CurveStructure,
    analyse_curve,
    build_spread,
    hedge_month_candidates,
)
from analysis.futures.domain import spec_for
from analysis.futures.providers import CurveObservation
from tests.test_futures_hedge import AS_OF, quote, unencoded_quote


def observation(commodity, legs, *, coherent=True):
    return CurveObservation(
        commodity=commodity, legs=tuple(legs),
        observation_date=legs[0].observation_date, fetched_date=legs[0].observation_date,
        coherent=coherent, coherence_note="" if coherent else "mixed sessions",
    )


CONTANGO = [
    quote("Soybeans", 2026, 9, 1150.25, observed=date(2026, 8, 11)),
    quote("Soybeans", 2026, 11, 1167.75, observed=date(2026, 8, 11)),
    quote("Soybeans", 2027, 1, 1183.00, observed=date(2026, 8, 11)),
]


def test_spread_arithmetic_is_hand_checkable():
    """ZSU26 1150.25 -> ZSX26 1167.75.

    Native spread      = +17.50 c/bu
    USD/MT             = 17.50 x 0.367437       = +6.4301
    Business days      = 14 Sep 2026 -> 13 Nov 2026 = 44 sessions
    Annualised on near = (17.50 / 1150.25) / (44/252) x 100 = +8.71%
    """
    spread = build_spread(CONTANGO[0], CONTANGO[1])
    assert spread.value == pytest.approx(17.50)
    assert spread.usd_per_mt == pytest.approx(17.50 * 36.7437 / 100, rel=1e-9)
    assert spread.business_days == 44
    expected = (17.50 / 1150.25) / (44 / BUSINESS_DAYS_PER_YEAR) * 100
    assert spread.annualised_pct == pytest.approx(expected, rel=1e-9)
    assert spread.calendar_months == 2
    assert spread.is_carry is True
    assert spread.same_session is True
    assert spread.symbols == "ZSU26-ZSX26"


def test_carry_is_annualised_on_expiry_dates_not_month_labels():
    """Nov -> Jan is 'two months' by label and 41 sessions by termination date.

    Using the labels would annualise on 42 sessions (2/12 of 252) and overstate
    the carry by ~2.4%.
    """
    spread = build_spread(CONTANGO[1], CONTANGO[2])
    assert spread.calendar_months == 2
    assert spread.business_days == 41
    label_based = (15.25 / 1167.75) / (2 / 12) * 100
    assert spread.annualised_pct != pytest.approx(label_based, rel=1e-6)


def test_a_product_without_an_encoded_expiry_gets_no_annualised_carry():
    legs = [unencoded_quote("Sugar", 2027, 3, 18.50), unencoded_quote("Sugar", 2027, 5, 18.80)]
    spread = build_spread(*legs)
    assert spread.value == pytest.approx(0.30)
    assert spread.business_days is None
    assert spread.annualised_pct is None
    assert spread.usd_per_mt_per_month is None


def test_a_monotone_rise_is_plain_contango():
    analysis = analyse_curve(observation("Soybeans", CONTANGO), as_of=AS_OF)
    assert analysis.structure is CurveStructure.CONTANGO
    assert analysis.is_inverted is False
    assert "pays to carry" in analysis.structure.implication


def test_a_single_leg_has_no_structure_and_no_spreads():
    analysis = analyse_curve(observation("Soybeans", CONTANGO[:1]), as_of=AS_OF)
    assert analysis.structure is CurveStructure.UNDETERMINED
    assert analysis.spreads == ()
    assert analysis.slope_per_month_usd_mt is None


def test_slope_is_usd_per_mt_per_calendar_month_front_to_back():
    analysis = analyse_curve(observation("Soybeans", CONTANGO), as_of=AS_OF)
    spec = spec_for("Soybeans")
    expected = (spec.native_to_usd_per_mt(1183.00) - spec.native_to_usd_per_mt(1150.25)) / 4
    assert analysis.slope_per_month_usd_mt == pytest.approx(expected, rel=1e-9)


def test_front_spreads_are_the_front_against_every_deferred():
    analysis = analyse_curve(observation("Soybeans", CONTANGO), as_of=AS_OF)
    assert [s.symbols for s in analysis.front_spreads] == ["ZSU26-ZSX26", "ZSU26-ZSF27"]
    assert [s.symbols for s in analysis.spreads] == ["ZSU26-ZSX26", "ZSX26-ZSF27"]
    assert analysis.spread("ZSU26", "ZSF27") is not None


def test_days_to_expiry_is_measured_from_the_observation_session():
    analysis = analyse_curve(observation("Soybeans", CONTANGO), as_of=date(2026, 8, 11))
    # 11 Aug -> 14 Sep 2026 is 24 weekdays, less Labor Day (7 Sep) = 23 sessions.
    assert analysis.legs[0].days_to_expiry == 23
    assert analysis.legs[0].calendar_days_to_expiry == 34
    assert analysis.legs[0].is_front is True


def test_hedge_month_candidates_exclude_expired_and_unencoded_months():
    analysis = analyse_curve(observation("Soybeans", CONTANGO), as_of=AS_OF)
    # A window closing 20 Oct: Sep no longer trades, Nov and Jan do.
    symbols = [leg.contract.symbol for leg in hedge_month_candidates(
        analysis, pricing_end=date(2026, 10, 20)
    )]
    assert symbols == ["ZSX26", "ZSF27"]

    # A product with no encoded termination rule offers no month at all: a
    # hedge month is chosen by proving the contract still trades through the
    # pricing window, and that proof needs a last trade date.
    unencoded = analyse_curve(observation("Sugar", [
        unencoded_quote("Sugar", 2027, 3, 18.5), unencoded_quote("Sugar", 2027, 5, 18.8),
    ]), as_of=AS_OF)
    assert hedge_month_candidates(unencoded, pricing_end=date(2026, 10, 20)) == ()

    # Sugar itself, now that ICE Rule 11.06(a) is encoded, does offer months.
    sugar = analyse_curve(observation("Sugar", [
        quote("Sugar", 2027, 3, 18.5), quote("Sugar", 2027, 5, 18.8),
    ]), as_of=AS_OF)
    assert [leg.contract.symbol for leg in hedge_month_candidates(
        sugar, pricing_end=date(2026, 10, 20)
    )] == ["SBH27", "SBK27"]


def test_a_month_too_close_to_expiry_is_not_offered():
    analysis = analyse_curve(observation("Soybeans", CONTANGO), as_of=date(2026, 9, 11))
    # ZSU26 has 1 session left on 11 Sep; the default floor is 5.
    symbols = [leg.contract.symbol for leg in hedge_month_candidates(
        analysis, pricing_end=date(2026, 9, 12)
    )]
    assert "ZSU26" not in symbols


def test_an_incoherent_curve_is_still_rendered_but_stamped():
    analysis = analyse_curve(observation("Soybeans", CONTANGO, coherent=False), as_of=AS_OF)
    assert analysis.coherent is False
    assert analysis.legs
    assert analysis.to_dict()["coherent"] is False


def test_a_spread_across_two_sessions_says_so():
    legs = [
        quote("Soybeans", 2026, 9, 1150.25, observed=date(2026, 8, 10)),
        quote("Soybeans", 2026, 11, 1167.75, observed=date(2026, 8, 11)),
    ]
    spread = build_spread(*legs)
    assert spread.same_session is False
    assert spread.observation_date is None


def test_a_spread_across_two_sessions_strikes_no_number():
    """Invariant 8: the intervening move is not term structure — refuse, don't footnote."""
    legs = [
        quote("Soybeans", 2026, 9, 1150.25, observed=date(2026, 8, 10)),
        quote("Soybeans", 2026, 11, 1167.75, observed=date(2026, 8, 11)),
    ]
    spread = build_spread(*legs)
    assert spread.value is None
    assert spread.usd_per_mt is None
    assert spread.annualised_pct is None
    assert spread.usd_per_mt_per_month is None
    assert spread.is_carry is None
    # The legs themselves still render: a trader still wants to see them.
    assert spread.near_price == 1150.25 and spread.far_price == 1167.75
    payload = spread.to_dict()
    assert payload["value"] is None and payload["usd_per_mt"] is None


# ---------------------------------------------------------------------------
# #671 — every curve reading names its pair, and structure is segment-aware
# ---------------------------------------------------------------------------

SESSION_2026_11_25 = date(2026, 11, 25)

#: The stored CBOT soybean strip on session 2026-11-25 (cents/bu), as quoted in
#: #671. Jan→Jul is carry (+8.25¢); only the Jul→Aug old/new-crop step is lower.
STRIP_2026_11_25 = [
    quote("Soybeans", 2027, 1, 1243.25, observed=SESSION_2026_11_25),
    quote("Soybeans", 2027, 3, 1244.25, observed=SESSION_2026_11_25),
    quote("Soybeans", 2027, 5, 1248.50, observed=SESSION_2026_11_25),
    quote("Soybeans", 2027, 7, 1251.50, observed=SESSION_2026_11_25),
    quote("Soybeans", 2027, 8, 1231.75, observed=SESSION_2026_11_25),
]


def test_carry_nearby_with_a_lower_deferred_month_is_not_backwardation():
    """The real 2026-11-25 shape. The legacy analyser read it as "mild
    backwardation — tight nearby supply" because it compared first against last
    fetched month. The nearby pair is in carry; the inversion is at Jul→Aug."""
    analysis = analyse_curve(observation("Soybeans", STRIP_2026_11_25), as_of=SESSION_2026_11_25)
    assert analysis.structure is CurveStructure.NEARBY_CARRY
    assert "backwardation" not in analysis.structure.value
    assert "tight" not in analysis.structure.implication
    assert analysis.is_inverted is False

    nearby = analysis.nearby
    assert nearby is not None
    assert (nearby.near.symbol, nearby.far.symbol) == ("ZSF27", "ZSH27")
    assert nearby.is_carry is True

    assert [(s.near.symbol, s.far.symbol) for s in analysis.inversions] == [("ZSN27", "ZSQ27")]
    assert "Jul 2027→Aug 2027" in analysis.reading
    assert "inverted" in analysis.reading


def test_the_nearby_pair_sentence_names_both_legs_units_and_session():
    """Mar − Jan = +1.00 cents/bu; × 0.367437 USD/MT per cent/bu = +0.37 USD/MT."""
    analysis = analyse_curve(observation("Soybeans", STRIP_2026_11_25), as_of=SESSION_2026_11_25)
    assert analysis.nearby is not None
    assert analysis.nearby.sentence() == (
        "ZSF27 1243.25 / ZSH27 1244.25: Mar +1.00 cents/bu (+0.37 USD/MT) over Jan, "
        "session 2026-11-25"
    )


def test_an_inverted_pair_reads_under_not_over():
    """Aug − Jul = −19.75 cents/bu = −7.26 USD/MT."""
    analysis = analyse_curve(observation("Soybeans", STRIP_2026_11_25), as_of=SESSION_2026_11_25)
    assert analysis.inversions[0].sentence() == (
        "ZSN27 1251.50 / ZSQ27 1231.75: Aug −19.75 cents/bu (−7.26 USD/MT) under Jul, "
        "session 2026-11-25"
    )


def test_a_cross_session_pair_sentence_strikes_nothing():
    """Invariant 8: two sessions' legs print both prices and refuse the spread."""
    spread = build_spread(
        quote("Soybeans", 2027, 1, 1243.25, observed=SESSION_2026_11_25),
        quote("Soybeans", 2027, 3, 1240.00, observed=date(2026, 11, 24)),
    )
    sentence = spread.sentence()
    assert sentence.startswith("ZSF27 1243.25 / ZSH27 1240.00: ")
    assert "not struck" in sentence
    assert "under" not in sentence and "over" not in sentence


def test_an_inverted_nearby_pair_with_carry_beyond_is_a_nearby_inversion():
    legs = [
        quote("Soybeans", 2026, 11, 1300.00, observed=date(2026, 8, 11)),
        quote("Soybeans", 2027, 1, 1150.00, observed=date(2026, 8, 11)),
        quote("Soybeans", 2027, 3, 1155.00, observed=date(2026, 8, 11)),
    ]
    analysis = analyse_curve(observation("Soybeans", legs), as_of=AS_OF)
    assert analysis.structure is CurveStructure.NEARBY_INVERSION
    assert analysis.is_inverted is True
    assert "Nov 2026→Jan 2027" in analysis.reading


def test_every_pair_lower_is_backwardation():
    legs = [
        quote("Soybeans", 2026, 11, 1300.00, observed=date(2026, 8, 11)),
        quote("Soybeans", 2027, 1, 1250.00, observed=date(2026, 8, 11)),
        quote("Soybeans", 2027, 3, 1200.00, observed=date(2026, 8, 11)),
    ]
    analysis = analyse_curve(observation("Soybeans", legs), as_of=AS_OF)
    assert analysis.structure is CurveStructure.BACKWARDATION
    assert analysis.is_inverted is True


def test_a_carry_reading_says_it_is_gross_of_storage_and_financing():
    analysis = analyse_curve(observation("Soybeans", CONTANGO), as_of=AS_OF)
    assert "gross of storage and financing" in analysis.structure.implication
