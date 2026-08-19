"""The named-contract crush: month convention, coherence, and every refusal.

The behaviour under test is a replacement, not an addition. Before this, the
"board crush" was three provider front-month series — ``ZS=F``, ``ZM=F``,
``ZL=F`` — whose underlying contract changes on Yahoo's own schedule, without
announcement and without adjustment. That number named no contract, so it could
not be reproduced, could not be hedged, and moved on roll days for reasons that
were not economic.

Every test here is one of the two halves of the fix: the calculation reproduces
from named legs, or it is withheld with a reason.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from analysis.futures.crush import (
    MIN_DAYS_TO_EXPIRY,
    SOY_CRUSH_PRODUCT_MONTH,
    SOY_CRUSH_SET,
    ContractBasis,
    CrushLevel,
    CrushWithheld,
    NamedCrush,
    WithheldReason,
    continuous_withheld,
    crush_contract_candidates,
    named_board_crush,
    product_month_for,
)
from analysis.futures.domain import (
    MANUAL_ENTRY,
    YFINANCE_DELAYED,
    ContinuousSeries,
    ContractQuote,
    NamedContract,
    PriceType,
    Provider,
    RollMethod,
    named_contract,
    spec_for,
    trading_months,
)
from analysis.futures.providers import CurveObservation

AS_OF = date(2026, 8, 18)
SESSION = date(2026, 8, 17)

# The 2026-08-17 CBOT session as it is stored in data/history/forward_curve.csv.
# Used verbatim so the worked example on the page and the example in this file
# are the same three numbers.
SEP_BEAN = 1201.00      # ZSU26, cents/bushel
SEP_MEAL = 312.10       # ZMU26, USD/short ton
SEP_OIL = 71.44         # ZLU26, cents/pound


# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------
PROVEN = Provider(
    key="test_authoritative",
    display="Test exchange feed",
    delayed=False,
    settlement_authoritative=True,
    note="test double standing in for an authoritative settlement feed",
)


def quote(
    commodity: str,
    year: int,
    month: int,
    price: float,
    *,
    observed: date = SESSION,
    price_type: PriceType = PriceType.DELAYED_CLOSE,
    provider: Provider = YFINANCE_DELAYED,
) -> ContractQuote:
    return ContractQuote(
        contract=named_contract(commodity, year, month),
        price=price,
        price_type=price_type,
        observation_date=observed,
        provider=provider,
    )


def unencoded_quote(commodity: str, year: int, month: int, price: float) -> ContractQuote:
    """A quote on a contract whose product has no encoded termination rule."""
    spec = replace(spec_for(commodity), expiry_rule=None, first_notice_rule=None)
    contract = NamedContract(spec=spec, year=year, month=month, last_trade=None, first_notice=None)
    return ContractQuote(
        contract=contract,
        price=price,
        price_type=PriceType.DELAYED_CLOSE,
        observation_date=SESSION,
        provider=YFINANCE_DELAYED,
    )


def observation(commodity: str, legs, *, coherent: bool = True) -> CurveObservation:
    legs = tuple(legs)
    return CurveObservation(
        commodity=commodity,
        legs=legs,
        observation_date=legs[0].observation_date if legs else None,
        fetched_date=legs[0].observation_date if legs else None,
        coherent=coherent,
        coherence_note="" if coherent else "an earlier session's leg was dropped",
    )


class FakeProvider:
    """A ``QuoteProvider`` over hand-built curves. No database, no pandas."""

    def __init__(self, curves: dict[str, CurveObservation], *, provider: Provider = YFINANCE_DELAYED):
        self._curves = curves
        self.provider = provider

    def curve(self, commodity: str, *, as_of: date) -> CurveObservation:
        return self._curves.get(commodity) or CurveObservation(
            commodity=commodity, legs=(), observation_date=None, fetched_date=None,
            coherent=False, coherence_note="no curve rows stored for this commodity",
            provider=self.provider,
        )

    def quote(self, contract: NamedContract, *, as_of: date) -> ContractQuote | None:
        return self.curve(contract.spec.name, as_of=as_of).leg(contract.symbol)

    def curve_history(self, commodity: str, *, as_of: date, sessions: int = 60):
        return ()

    def continuous(self, commodity: str, *, as_of: date):
        return None

    def fx_rate(self, pair: str, *, on: date):
        return None

    def aggregate_open_interest(self, commodity: str, *, as_of: date):
        return None


def september_provider(**overrides) -> FakeProvider:
    """The real 2026-08-17 curve, with individual legs overridable per test."""
    curves = {
        "Soybeans": observation("Soybeans", [
            quote("Soybeans", 2026, 9, SEP_BEAN),
            quote("Soybeans", 2026, 11, 1216.00),
            quote("Soybeans", 2027, 1, 1231.00),
        ]),
        "Soybean Meal": observation("Soybean Meal", [
            quote("Soybean Meal", 2026, 9, SEP_MEAL),
            quote("Soybean Meal", 2026, 10, 314.00),
            quote("Soybean Meal", 2026, 12, 318.70),
            quote("Soybean Meal", 2027, 1, 320.60),
        ]),
        "Soybean Oil": observation("Soybean Oil", [
            quote("Soybean Oil", 2026, 9, SEP_OIL),
            quote("Soybean Oil", 2026, 10, 71.21),
            quote("Soybean Oil", 2026, 12, 70.82),
            quote("Soybean Oil", 2027, 1, 70.60),
        ]),
    }
    curves.update(overrides)
    return FakeProvider(curves)


# ---------------------------------------------------------------------------
# The convention
# ---------------------------------------------------------------------------
def test_the_crush_convention_is_the_documented_table():
    """Same delivery month where the products list it; November beans take December.

    ZS lists Jan/Mar/May/Jul/Aug/Sep/Nov and ZM/ZL list
    Jan/Mar/May/Jul/Aug/Sep/Oct/Dec, so six of the seven bean months pair with
    themselves and November — which the products do not list — pairs with the
    first listed product month after it.
    """
    assert SOY_CRUSH_PRODUCT_MONTH == {1: 1, 3: 3, 5: 5, 7: 7, 8: 8, 9: 9, 11: 12}


def test_the_convention_is_derived_from_the_listed_months_not_asserted():
    """Every bean month maps to a month the products actually list."""
    product_months = set(trading_months("Soybean Meal"))
    assert product_months == set(trading_months("Soybean Oil"))
    for bean_month in trading_months("Soybeans"):
        assert SOY_CRUSH_PRODUCT_MONTH[bean_month] in product_months


def test_november_beans_crush_into_december_products_of_the_same_year():
    assert product_month_for(2026, 11) == (2026, 12)
    assert product_month_for(2026, 9) == (2026, 9)


def test_a_bean_month_the_products_do_not_follow_within_the_year_rolls_the_year():
    """December is the last listed product month, so the rule cannot wrap silently.

    Encoded rather than left implicit: the wrap is the branch a future product
    calendar change would take, and a silent year-roll would name a contract a
    year out with a plausible price on it.
    """
    for bean_month, product_month in SOY_CRUSH_PRODUCT_MONTH.items():
        year, month = product_month_for(2026, bean_month)
        assert (year, month) == (2026, product_month)


def test_candidates_are_nearest_first_and_carry_all_three_contracts():
    candidates = crush_contract_candidates(AS_OF, count=3)
    assert [c.bean.symbol for c in candidates] == ["ZSU26", "ZSX26", "ZSF27"]
    assert [c.meal.symbol for c in candidates] == ["ZMU26", "ZMZ26", "ZMF27"]
    assert [c.oil.symbol for c in candidates] == ["ZLU26", "ZLZ26", "ZLF27"]


# ---------------------------------------------------------------------------
# The calculation
# ---------------------------------------------------------------------------
def test_the_september_crush_reproduces_by_hand():
    """ZSU26 1201.00 / ZMU26 312.10 / ZLU26 71.44, all observed 2026-08-17.

        bean  1201.00 c/bu   / 100 x 36.7437  =  441.2918 USD/MT
        meal   312.10 $/ston / 0.907185       =  344.0313 USD/MT
        oil     71.44 c/lb   / 100 x 2204.62  = 1574.9805 USD/MT

        revenue = 1574.9805 x 0.1833333 + 344.0313 x 0.7333333 = 541.0360
        margin  = 541.0360 - 441.2918                          =  99.7442
    """
    result = named_board_crush(september_provider(), as_of=AS_OF)
    assert isinstance(result, NamedCrush)

    assert result.bean.usd_per_mt == pytest.approx(441.291837, abs=1e-6)
    assert result.meal.usd_per_mt == pytest.approx(344.0312615, abs=1e-6)
    assert result.oil.usd_per_mt == pytest.approx(1574.980528, abs=1e-6)
    assert result.revenue_usd_mt == pytest.approx(541.03602, abs=1e-4)
    assert result.bean_cost_usd_mt == pytest.approx(441.291837, abs=1e-6)
    assert result.margin_usd_mt == pytest.approx(99.744186, abs=1e-4)


def test_every_leg_names_its_contract_and_its_provenance():
    result = named_board_crush(september_provider(), as_of=AS_OF)
    assert isinstance(result, NamedCrush)

    for leg, symbol in ((result.bean, "ZSU26"), (result.meal, "ZMU26"), (result.oil, "ZLU26")):
        assert leg.symbol == symbol
        assert leg.contract_month == "2026-09"
        assert leg.observation_date == SESSION
        assert leg.price_type is PriceType.DELAYED_CLOSE
        assert leg.provider.key == "yfinance_delayed"
        assert leg.settlement_proven is False
        payload = leg.to_dict()
        for field_name in (
            "symbol", "contract_month", "observation_date", "price_type",
            "provider", "settlement_proven",
        ):
            assert payload[field_name] is not None


def test_a_delayed_close_crush_is_a_reference_never_a_settlement():
    result = named_board_crush(september_provider(), as_of=AS_OF)
    assert isinstance(result, NamedCrush)
    assert result.level is CrushLevel.BOARD_REFERENCE
    assert result.is_settlement_proven is False
    assert "settlement" in result.level.meaning.lower()
    assert result.contract_basis is ContractBasis.NAMED_CONTRACT
    assert result.is_hedgeable is True


def test_a_proven_settlement_feed_raises_the_level_and_nothing_else():
    curves = september_provider()._curves
    proven = FakeProvider(
        {
            commodity: replace(
                curve,
                legs=tuple(
                    replace(leg, price_type=PriceType.SETTLEMENT, provider=PROVEN)
                    for leg in curve.legs
                ),
            )
            for commodity, curve in curves.items()
        },
        provider=PROVEN,
    )
    result = named_board_crush(proven, as_of=AS_OF)
    assert isinstance(result, NamedCrush)
    assert result.level is CrushLevel.BOARD_SETTLEMENT
    assert result.is_settlement_proven is True
    # The arithmetic is untouched — only the claim about it changed.
    assert result.margin_usd_mt == pytest.approx(99.744186, abs=1e-4)


def test_the_four_levels_are_distinct_and_only_two_are_board_levels():
    assert set(CrushLevel) == {
        CrushLevel.BOARD_REFERENCE,
        CrushLevel.BOARD_SETTLEMENT,
        CrushLevel.GROSS_PHYSICAL,
        CrushLevel.NET_PLANT,
    }
    assert [level for level in CrushLevel if level.is_board] == [
        CrushLevel.BOARD_REFERENCE, CrushLevel.BOARD_SETTLEMENT,
    ]
    labels = {level.label for level in CrushLevel}
    assert len(labels) == 4


def test_the_workings_reproduce_the_printed_margin():
    """Every published figure carries the line that produces it."""
    result = named_board_crush(september_provider(), as_of=AS_OF)
    assert isinstance(result, NamedCrush)
    workings = "\n".join(result.workings())
    assert "ZSU26" in workings and "ZMU26" in workings and "ZLU26" in workings
    assert "2026-08-17" in workings
    assert f"{result.margin_usd_mt:,.2f}" in workings


# ---------------------------------------------------------------------------
# Boundary: roll periods
# ---------------------------------------------------------------------------
def test_inside_the_roll_window_the_crush_moves_to_the_next_month_set():
    """ZSU26 last trades 2026-09-14; four sessions before that it is refused.

    The whole point of naming the contract: a front month with two sessions
    left is not where a crush is struck, and a continuous series cannot even
    ask the question.
    """
    late = date(2026, 9, 10)
    curves = september_provider()._curves
    rolled = FakeProvider({
        commodity: replace(
            curve, legs=tuple(replace(leg, observation_date=late) for leg in curve.legs),
            observation_date=late,
        )
        for commodity, curve in curves.items()
    })
    result = named_board_crush(rolled, as_of=late)
    assert isinstance(result, NamedCrush)
    assert result.bean.symbol == "ZSX26"
    assert result.meal.symbol == "ZMZ26"
    assert result.oil.symbol == "ZLZ26"


def test_outside_the_roll_window_the_prompt_month_is_kept():
    result = named_board_crush(september_provider(), as_of=AS_OF, min_days_to_expiry=MIN_DAYS_TO_EXPIRY)
    assert isinstance(result, NamedCrush)
    assert result.bean.symbol == "ZSU26"
    assert result.bean.days_to_expiry is not None
    assert result.bean.days_to_expiry >= MIN_DAYS_TO_EXPIRY


def test_an_expired_contract_is_never_selected():
    """Past its last trade a leg is not a contract, whatever the table still holds."""
    after_expiry = date(2026, 9, 21)
    curves = september_provider()._curves
    stale = FakeProvider({
        commodity: replace(
            curve,
            legs=tuple(replace(leg, observation_date=after_expiry) for leg in curve.legs),
            observation_date=after_expiry,
        )
        for commodity, curve in curves.items()
    })
    result = named_board_crush(stale, as_of=after_expiry)
    assert isinstance(result, NamedCrush)
    assert result.bean.symbol == "ZSX26"


# ---------------------------------------------------------------------------
# Boundary: missing legs
# ---------------------------------------------------------------------------
def test_a_missing_oil_curve_withholds_and_names_the_leg():
    provider = september_provider(**{"Soybean Oil": observation("Soybean Oil", [])})
    result = named_board_crush(provider, as_of=AS_OF)
    assert isinstance(result, CrushWithheld)
    assert result.code is WithheldReason.NO_CURVE
    assert "Soybean Oil" in result.reason
    assert result.is_ok is False


def test_a_missing_month_falls_through_to_the_next_crush_month():
    """No September meal, but December meal exists — so the November set is used."""
    provider = september_provider(**{
        "Soybean Meal": observation("Soybean Meal", [
            quote("Soybean Meal", 2026, 12, 318.70),
            quote("Soybean Meal", 2027, 1, 320.60),
        ]),
    })
    result = named_board_crush(provider, as_of=AS_OF)
    assert isinstance(result, NamedCrush)
    assert (result.bean.symbol, result.meal.symbol, result.oil.symbol) == ("ZSX26", "ZMZ26", "ZLZ26")


def test_when_no_month_has_all_three_legs_the_crush_is_withheld():
    provider = september_provider(**{
        "Soybean Meal": observation("Soybean Meal", [quote("Soybean Meal", 2027, 3, 322.50)]),
        "Soybean Oil": observation("Soybean Oil", [quote("Soybean Oil", 2026, 10, 71.21)]),
    })
    result = named_board_crush(provider, as_of=AS_OF)
    assert isinstance(result, CrushWithheld)
    assert result.code is WithheldReason.NO_CRUSH_MONTH
    assert result.remedy


# ---------------------------------------------------------------------------
# Boundary: mixed dates
# ---------------------------------------------------------------------------
def test_legs_from_two_sessions_are_withheld_not_averaged():
    """A margin struck across two days is the intervening move, not a margin."""
    provider = september_provider(**{
        "Soybean Oil": observation("Soybean Oil", [
            quote("Soybean Oil", 2026, 9, SEP_OIL, observed=date(2026, 8, 14)),
        ]),
    })
    result = named_board_crush(provider, as_of=AS_OF)
    assert isinstance(result, CrushWithheld)
    assert result.code is WithheldReason.MIXED_SESSIONS
    assert "2026-08-14" in result.reason and "2026-08-17" in result.reason


def test_an_incoherent_curve_is_reported_even_when_the_kept_legs_agree():
    """Dropping a straggler is not the same as never having had one."""
    curves = september_provider()._curves
    provider = FakeProvider({
        **curves,
        "Soybean Oil": replace(curves["Soybean Oil"], coherent=False,
                               coherence_note="dropped ZLH27 observed 2026-08-10"),
    })
    result = named_board_crush(provider, as_of=AS_OF)
    assert isinstance(result, NamedCrush)
    assert any("ZLH27" in note for note in result.curve_notes)


# ---------------------------------------------------------------------------
# Boundary: mixed price types
# ---------------------------------------------------------------------------
def test_legs_of_different_price_types_are_withheld():
    provider = september_provider(**{
        "Soybean Meal": observation("Soybean Meal", [
            quote("Soybean Meal", 2026, 9, SEP_MEAL,
                  price_type=PriceType.MANUAL, provider=MANUAL_ENTRY),
        ]),
    })
    result = named_board_crush(provider, as_of=AS_OF)
    assert isinstance(result, CrushWithheld)
    assert result.code is WithheldReason.MIXED_PRICE_TYPES
    assert "manual" in result.reason and "delayed_close" in result.reason


def test_legs_from_two_providers_are_withheld_even_at_the_same_price_type():
    """A trusted bean and a v1 oil are two provenances inside one number.

    The margin would inherit the weaker of the two and nothing on it would say
    which leg carried it — the exact case ``analysis.futures.trusted_provider``
    refuses to serve half a crush over.
    """
    second = replace(PROVEN, key="second_feed", display="Second feed", settlement_authoritative=False)
    provider = september_provider(**{
        "Soybean Meal": observation("Soybean Meal", [
            quote("Soybean Meal", 2026, 9, SEP_MEAL, provider=second),
        ]),
    })
    result = named_board_crush(provider, as_of=AS_OF)
    assert isinstance(result, CrushWithheld)
    assert result.code is WithheldReason.MIXED_PROVIDERS
    assert "Second feed" in result.reason


def test_a_price_type_that_is_not_a_board_print_is_withheld():
    """Three hand-entered numbers agree with each other and are not a board crush."""
    provider = FakeProvider({
        commodity: observation(commodity, [
            quote(commodity, 2026, 9, price, price_type=PriceType.MANUAL, provider=MANUAL_ENTRY)
        ])
        for commodity, price in (
            ("Soybeans", SEP_BEAN), ("Soybean Meal", SEP_MEAL), ("Soybean Oil", SEP_OIL),
        )
    })
    result = named_board_crush(provider, as_of=AS_OF)
    assert isinstance(result, CrushWithheld)
    assert result.code is WithheldReason.UNSUPPORTED_PRICE_TYPE


def test_a_settlement_claimed_by_an_unauthoritative_provider_is_withheld():
    """The claim is about the provider, not the number (``pricing.semantics``)."""
    provider = FakeProvider({
        commodity: observation(commodity, [
            quote(commodity, 2026, 9, price, price_type=PriceType.SETTLEMENT)
        ])
        for commodity, price in (
            ("Soybeans", SEP_BEAN), ("Soybean Meal", SEP_MEAL), ("Soybean Oil", SEP_OIL),
        )
    })
    result = named_board_crush(provider, as_of=AS_OF)
    assert isinstance(result, CrushWithheld)
    assert result.code is WithheldReason.SETTLEMENT_UNPROVEN


# ---------------------------------------------------------------------------
# Boundary: continuous data is never a substitute
# ---------------------------------------------------------------------------
def test_a_continuous_series_is_never_hedgeable_and_never_a_crush_leg():
    series = ContinuousSeries(
        commodity="Soybeans",
        roll_method=RollMethod.PROVIDER_FRONT_MONTH,
        points=((SESSION, SEP_BEAN),),
        adjustment_note="unadjusted; the provider does not publish its roll dates",
    )
    assert series.is_hedgeable is False
    withheld = continuous_withheld("Soybeans", series)
    assert isinstance(withheld, CrushWithheld)
    assert withheld.code is WithheldReason.CONTINUOUS_SERIES
    assert "front-month" in withheld.reason or "front month" in withheld.reason


def test_the_crush_takes_a_curve_and_has_no_series_entry_point():
    """There is no code path from a stitched series to a crush margin.

    Stated as a test because the absence is the design: the previous board
    crush was exactly that path, and re-adding it would be a two-line change
    nobody would notice in review.
    """
    import analysis.futures.crush as crush_mod

    accepting = [
        name for name in dir(crush_mod)
        if "series" in name.lower() or "continuous" in name.lower()
    ]
    assert accepting == ["continuous_withheld"]


# ---------------------------------------------------------------------------
# Boundary: an unencoded expiry rule
# ---------------------------------------------------------------------------
def test_a_leg_whose_expiry_rule_is_not_encoded_is_withheld():
    """No last trade date means no roll window, so no honest month selection."""
    provider = september_provider(**{
        "Soybean Oil": observation("Soybean Oil", [unencoded_quote("Soybean Oil", 2026, 9, SEP_OIL)]),
    })
    result = named_board_crush(provider, as_of=AS_OF)
    assert isinstance(result, CrushWithheld)
    assert result.code is WithheldReason.EXPIRY_NOT_ENCODED


# ---------------------------------------------------------------------------
# A withheld crush is not a zero
# ---------------------------------------------------------------------------
def test_every_withheld_reason_carries_a_sentence_and_no_number():
    provider = FakeProvider({})
    result = named_board_crush(provider, as_of=AS_OF)
    assert isinstance(result, CrushWithheld)
    assert result.reason.strip()
    payload = result.to_dict()
    assert payload["margin_usd_mt"] is None
    assert payload["code"] == result.code.value


def test_a_withheld_crush_cannot_be_read_as_a_margin():
    result = named_board_crush(FakeProvider({}), as_of=AS_OF)
    assert not hasattr(result, "margin_usd_mt") or result.to_dict()["margin_usd_mt"] is None
    assert result.is_ok is False


def test_the_crush_set_is_the_soy_complex_in_crush_order():
    assert (SOY_CRUSH_SET.bean, SOY_CRUSH_SET.meal, SOY_CRUSH_SET.oil) == (
        "Soybeans", "Soybean Meal", "Soybean Oil"
    )
