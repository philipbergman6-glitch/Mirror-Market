"""Contract identity, specifications, unit conversion and expiry rules.

The hand-checkable core of Phase 3. Every number asserted here was worked out
independently of the code — a contract size times a price is the one piece of
arithmetic in a hedge that nobody re-derives once it looks plausible.
"""

from __future__ import annotations

from datetime import date

import pytest

from analysis.futures.domain import (
    CONTRACT_SPECS,
    MONTH_CODES,
    NO_NOTICE_DAY,
    ContinuousSeries,
    NamedContract,
    RollMethod,
    UnknownContract,
    business_days_between,
    contracts_from,
    exchange_holidays,
    fingerprint,
    first_notice_date,
    is_business_day,
    last_trade_date,
    named_contract,
    nth_business_day_of_month,
    parse_symbol,
    spec_for,
    trading_months,
)
from pipeline.units import CONVERSION_FACTORS
from tests.test_futures_hedge import unencoded_contract

# ---------------------------------------------------------------------------
# Contract specifications — hand-checked
# ---------------------------------------------------------------------------

# (commodity, native price, expected notional USD, expected USD/MT, expected MT)
HAND_CHECKED = [
    # ZS: 1147.25 c/bu x 5,000 bu / 100 = $57,362.50; 5000/36.7437 = 136.0777 MT
    ("Soybeans", 1147.25, 57_362.50, 421.54209825, 136.0777494),
    # ZM: $300/short ton x 100 short tons = $30,000; 100 x 0.907185 = 90.7185 MT
    ("Soybean Meal", 300.0, 30_000.00, 330.69329850, 90.718500),
    # ZL: 50 c/lb x 60,000 lb / 100 = $30,000; 60000/2204.62 = 27.21556 MT
    ("Soybean Oil", 50.0, 30_000.00, 1102.31000000, 27.2155746),
    # ZC: 450 c/bu x 5,000 bu / 100 = $22,500; 5000/39.3683 = 127.0057 MT (56-lb bushel)
    ("Corn", 450.0, 22_500.00, 177.15735000, 127.0057381),
]


@pytest.mark.parametrize(("commodity", "price", "notional", "usd_mt", "mt"), HAND_CHECKED)
def test_contract_arithmetic_matches_hand_calculation(commodity, price, notional, usd_mt, mt):
    spec = spec_for(commodity)
    assert spec.value_usd(price) == pytest.approx(notional, rel=1e-9)
    assert spec.native_to_usd_per_mt(price) == pytest.approx(usd_mt, rel=1e-9)
    assert spec.mt_per_contract == pytest.approx(mt, rel=1e-6)


@pytest.mark.parametrize(("commodity", "price", "_notional", "_usd_mt", "_mt"), HAND_CHECKED)
def test_the_two_routes_to_usd_agree(commodity, price, _notional, _usd_mt, _mt):
    """price -> USD/MT -> x tonnes must equal price -> notional.

    These are the two routes a hedge P&L can take to the same money, and a
    divergence between them would be invisible in review and wrong in every
    scenario.
    """
    spec = spec_for(commodity)
    assert spec.native_to_usd_per_mt(price) * spec.mt_per_contract == pytest.approx(
        spec.value_usd(price), rel=1e-12
    )


def test_conversion_factors_match_the_display_layer():
    """One table of densities, not two.

    pipeline/units.py converts for display; this package converts for money.
    A drift between them would show a trader one price and hedge them at
    another.
    """
    for commodity, spec in CONTRACT_SPECS.items():
        expected = CONVERSION_FACTORS.get(commodity)
        if expected is None:
            continue
        assert spec.native_to_usd_per_mt(100.0) == pytest.approx(100.0 * expected, rel=1e-9), (
            f"{commodity} disagrees with pipeline.units"
        )


def test_tick_value_is_the_contract_size_times_the_tick():
    for commodity, spec in CONTRACT_SPECS.items():
        implied = spec.value_usd(spec.tick_size) - spec.value_usd(0.0)
        assert implied == pytest.approx(spec.tick_value_usd, rel=1e-9), commodity


def test_unknown_commodity_raises_rather_than_defaulting():
    with pytest.raises(UnknownContract):
        spec_for("Rapeseed")


def test_trading_months_come_from_the_fetcher_config():
    from config import FORWARD_CURVE_CONTRACTS

    for commodity in CONTRACT_SPECS:
        assert trading_months(commodity) == tuple(
            sorted(FORWARD_CURVE_CONTRACTS[commodity]["months"])
        )


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------


def test_symbol_and_provider_symbol():
    contract = named_contract("Soybeans", 2026, 11)
    assert contract.symbol == "ZSX26"
    assert contract.provider_symbol == "ZSX26.CBT"
    assert contract.label == "Nov 2026"
    assert contract.delivery_month == "2026-11"
    assert contract.contract_month_date == date(2026, 11, 1)


def test_parse_symbol_round_trips_with_and_without_the_provider_suffix():
    for text in ("ZLZ26", "ZLZ26.CBT", "zlz26.cbt"):
        contract = parse_symbol(text)
        assert contract.symbol == "ZLZ26"
        assert contract.spec.name == "Soybean Oil"


def test_parse_symbol_rejects_nonsense():
    for text in ("", "ZS", "ZSQQ6", "ZS126"):
        with pytest.raises((UnknownContract, ValueError)):
            parse_symbol(text)


def test_a_month_the_product_does_not_list_is_refused():
    """ZS does not list October. A hedge in a month that is not listed cannot fill."""
    with pytest.raises(ValueError, match="not a listed month"):
        named_contract("Soybeans", 2026, 10)


def test_trust_identity_shape_matches_the_trust_domain_contract():
    from trust.domain import ContractIdentity

    payload = named_contract("Soybean Meal", 2027, 1).trust_identity()
    identity = ContractIdentity.from_dict(payload)
    assert identity.code == "ZMF27"
    assert identity.delivery_month == "2027-01"


def test_month_codes_are_the_exchange_convention():
    assert MONTH_CODES[11] == "X"
    assert MONTH_CODES[7] == "N"
    assert MONTH_CODES[12] == "Z"


# ---------------------------------------------------------------------------
# Expiry and roll edges
# ---------------------------------------------------------------------------


def test_cbot_grain_last_trade_is_the_business_day_before_the_15th():
    # 15 Aug 2026 is a Saturday, so the last trade day is Friday the 14th.
    assert last_trade_date(spec_for("ZS"), 2026, 8) == date(2026, 8, 14)
    # 15 Nov 2026 is a Sunday -> Friday the 13th.
    assert last_trade_date(spec_for("ZS"), 2026, 11) == date(2026, 11, 13)
    # 15 Jan 2027 is a Friday -> Thursday the 14th.
    assert last_trade_date(spec_for("ZS"), 2027, 1) == date(2027, 1, 14)


def test_expiry_respects_an_exchange_holiday():
    """15 Jan 2024 was Martin Luther King Jr Day; the 14th was a Sunday.

    A weekday-only calendar would answer Monday the 15th (a closed session);
    the correct answer is Friday the 12th.
    """
    assert last_trade_date(spec_for("ZS"), 2024, 1) == date(2024, 1, 12)


def test_first_notice_is_the_last_business_day_of_the_prior_month():
    assert first_notice_date(spec_for("ZS"), 2026, 11) == date(2026, 10, 30)
    # January's prior month is the previous December.
    assert first_notice_date(spec_for("ZS"), 2027, 1) == date(2026, 12, 31)


def test_livestock_rules():
    # Live Cattle: last business day of the contract month.
    assert last_trade_date(spec_for("LE"), 2026, 12) == date(2026, 12, 31)
    # Lean Hogs: the 10th business day.
    assert last_trade_date(spec_for("HE"), 2026, 12) == date(2026, 12, 14)
    assert first_notice_date(spec_for("LE"), 2026, 12) is None


def test_ice_sugar_terminates_on_the_month_before_delivery():
    """ICE Rule 11.06(a): the last full trading day of the month preceding delivery.

    SBH27 (March 2027) therefore stops trading on the last business day of
    February 2027 = Friday 26 Feb. SBH26 lands on Friday 27 Feb 2026, and
    SBK26 (May) on Thursday 30 Apr 2026 — a month-end that is a business day,
    which is the ordinary case.
    """
    sugar = spec_for("SB")
    assert last_trade_date(sugar, 2027, 3) == date(2027, 2, 26)
    assert last_trade_date(sugar, 2026, 3) == date(2026, 2, 27)
    assert last_trade_date(sugar, 2026, 5) == date(2026, 4, 30)
    assert spec_for("Sugar").expiry_confidence.value == "documented"


def test_ice_sugar_has_no_first_notice_day_because_the_contract_has_none():
    """Absence with a reason, not an omission.

    Sugar No. 11 runs no notice-day mechanism: Rule 11.06(b) obliges every open
    short to issue a Memo of Deliverer after the close on the *last trading
    day*. The delivery obligation attaches then, which is stricter than an FND,
    so a synthesised one would hand a hedger days they do not have.
    """
    assert spec_for("SB").first_notice_rule == NO_NOTICE_DAY
    assert first_notice_date(spec_for("SB"), 2027, 3) is None
    # And that is a different state from "we have not encoded it", which is
    # what a None here would mean — Live Cattle's FND rule genuinely is not
    # encoded, and the two must not render alike.
    assert spec_for("LE").first_notice_rule is None


def test_ice_cotton_terminates_seventeen_business_days_from_the_month_end():
    """Two published statements of one rule, and they must agree.

    The contract summary says "seventeen business days from end of spot month".
    The rulebook says Last Trading Day is the 10th business day before Last
    Delivery Day (10.02(a)(ix)) and Last Delivery Day is the 7th-last business
    day of the month (10.02(a)(vii)) — 10 + 7 = 17 counting back with the last
    business day as 1. CTZ24: December 2024's business days run to the 31st,
    and the 17th counted back is Friday 6 December.
    """
    cotton = spec_for("CT")
    assert last_trade_date(cotton, 2024, 12) == date(2024, 12, 6)
    assert last_trade_date(cotton, 2026, 12) == date(2026, 12, 8)
    assert last_trade_date(cotton, 2026, 3) == date(2026, 3, 9)


def test_ice_cotton_first_notice_day_falls_before_its_last_trading_day():
    """The reason this project keys roll alerts on FND rather than last trade.

    Cotton's First Delivery Day is the first business day of the delivery month
    and First Notice Day is five business days before it (10.02(a)(v)+(vi)), so
    for CTZ24 notice starts 22 Nov 2024 — a fortnight *before* the 6 Dec last
    trade. A hedger reading last trade as their deadline is already deliverable.
    """
    cotton = spec_for("CT")
    fnd = first_notice_date(cotton, 2024, 12)
    assert fnd == date(2024, 11, 22)
    ltd = last_trade_date(cotton, 2024, 12)
    assert ltd is not None and fnd is not None and fnd < ltd


def test_every_carried_product_now_has_a_documented_termination_rule():
    """The state of the world, asserted so a regression is loud.

    Not a claim that NOT_ENCODED is dead — the mechanism is still tested, on a
    spec built for the purpose (``unencoded_contract``). This asserts only that
    no product we actually carry is relying on it today.
    """
    undocumented = [
        name for name, spec in CONTRACT_SPECS.items()
        if spec.expiry_confidence.value != "documented"
    ]
    assert undocumented == []


def test_unencoded_expiry_is_absent_not_estimated():
    """Where no rule is encoded, nothing downstream may invent one."""
    contract = unencoded_contract("Sugar", 2027, 3)
    assert contract.last_trade is None
    assert contract.days_to_expiry(date(2026, 8, 18)) is None
    assert contract.is_expired(date(2026, 8, 18)) is None
    assert contract.expiry_confidence.value == "not_encoded"


def test_current_delivery_month_is_a_candidate_until_it_expires():
    """The #61 rule, applied exactly because the termination date is encoded."""
    # 10 Aug 2026: ZSQ26 still trades (last trade 14 Aug) and is the front.
    early = contracts_from("Soybeans", date(2026, 8, 10), count=3)
    assert [c.symbol for c in early][:1] == ["ZSQ26"]
    # 18 Aug 2026: it has expired and drops out.
    late = contracts_from("Soybeans", date(2026, 8, 18), count=3)
    assert "ZSQ26" not in [c.symbol for c in late]
    assert late[0].symbol == "ZSU26"


def test_days_to_expiry_counts_sessions_not_calendar_days():
    contract = named_contract("Soybeans", 2026, 8)      # last trade Fri 14 Aug 2026
    assert contract.days_to_expiry(date(2026, 8, 11)) == 3   # Wed, Thu, Fri
    assert contract.calendar_days_to_expiry(date(2026, 8, 11)) == 3
    assert contract.days_to_expiry(date(2026, 8, 14)) == 0
    assert contract.days_to_expiry(date(2026, 8, 17)) == -1
    assert contract.is_expired(date(2026, 8, 17)) is True


def test_business_day_helpers():
    assert business_days_between(date(2026, 8, 11), date(2026, 8, 11)) == 0
    assert business_days_between(date(2026, 8, 14), date(2026, 8, 11)) == -3
    # 4 July 2026 is a Saturday, observed Friday 3 July.
    assert is_business_day(date(2026, 7, 3)) is False
    assert nth_business_day_of_month(2026, 12, -1) == date(2026, 12, 31)
    assert nth_business_day_of_month(2026, 1, 1) == date(2026, 1, 2)  # 1 Jan is a holiday


def test_holiday_set_includes_good_friday_and_the_moving_mondays():
    holidays = exchange_holidays(2026)
    assert date(2026, 4, 3) in holidays        # Good Friday 2026
    assert date(2026, 1, 19) in holidays       # MLK Jr Day, 3rd Monday
    assert date(2026, 11, 26) in holidays      # Thanksgiving, 4th Thursday
    assert date(2026, 5, 25) in holidays       # Memorial Day, last Monday


# ---------------------------------------------------------------------------
# Named contracts are not continuous series
# ---------------------------------------------------------------------------


def test_continuous_series_is_never_hedgeable_and_always_names_its_roll():
    series = ContinuousSeries(
        commodity="Soybeans",
        roll_method=RollMethod.PROVIDER_FRONT_MONTH,
        points=((date(2026, 8, 11), 1147.25),),
    )
    assert series.is_hedgeable is False
    assert series.to_dict()["hedgeable"] is False
    assert "unadjusted" in series.method_description
    assert not isinstance(series, NamedContract)


def test_fingerprint_is_stable_and_order_independent():
    left = fingerprint({"a": 1, "b": [date(2026, 1, 1)]})
    right = fingerprint({"b": [date(2026, 1, 1)], "a": 1})
    assert left == right
    assert left != fingerprint({"a": 2, "b": [date(2026, 1, 1)]})
