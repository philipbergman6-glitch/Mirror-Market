"""Black-76 under explicitly stated assumptions, and the absence of a chain.

The reference values here come from the model's own closed form checked against
put-call parity and published Black-76 figures, not from this implementation.
The second half of the file tests the thing that matters more: that no option
value is produced without a named human source for its volatility, and that a
missing chain reports as missing rather than as empty.
"""

from __future__ import annotations

import math
from datetime import date

import pytest

from analysis.futures.domain import PriceType, named_contract
from analysis.futures.options import (
    NO_CHAIN_REASON,
    ChainUnavailable,
    ManualLadder,
    ManualQuote,
    OptionContract,
    OptionEntryError,
    OptionRight,
    OptionStyle,
    black76_greeks,
    black76_price,
    chain_status,
    fetch_chain,
    implied_volatility,
    load_ladder,
    parse_ladder,
    value_manual_ladder,
    value_option,
)

AS_OF = date(2026, 8, 18)
ZSX26 = named_contract("Soybeans", 2026, 11)


# ---------------------------------------------------------------------------
# The model, against hand-checkable references
# ---------------------------------------------------------------------------


def test_at_the_money_call_matches_the_published_reference_value():
    """F=100, K=100, sigma=0.20, T=1, r=0.05.

    d1 = 0.5 x 0.20 = 0.10, d2 = -0.10
    N(0.10) = 0.539828, N(-0.10) = 0.460172
    C = e^-0.05 x 100 x (0.5398278 - 0.4601722) = 0.9512294 x 7.965567 = 7.577082
    """
    price = black76_price(100.0, 100.0, 0.20, 1.0, 0.05, OptionRight.CALL)
    assert price == pytest.approx(7.577082, abs=1e-5)


def test_an_at_the_money_put_equals_the_call_when_the_forward_is_the_strike():
    call = black76_price(100.0, 100.0, 0.20, 1.0, 0.05, OptionRight.CALL)
    put = black76_price(100.0, 100.0, 0.20, 1.0, 0.05, OptionRight.PUT)
    assert put == pytest.approx(call, rel=1e-12)


@pytest.mark.parametrize("forward", [80.0, 100.0, 130.0])
@pytest.mark.parametrize("years", [0.25, 1.0, 2.5])
def test_put_call_parity_holds(forward, years):
    """C - P = e^{-rT} (F - K) for options on futures."""
    strike, vol, rate = 100.0, 0.28, 0.045
    call = black76_price(forward, strike, vol, years, rate, OptionRight.CALL)
    put = black76_price(forward, strike, vol, years, rate, OptionRight.PUT)
    assert call - put == pytest.approx(
        math.exp(-rate * years) * (forward - strike), rel=1e-10, abs=1e-10
    )


def test_a_zero_volatility_option_is_its_discounted_intrinsic_value():
    price = black76_price(120.0, 100.0, 0.0, 1.0, 0.05, OptionRight.CALL)
    assert price == pytest.approx(math.exp(-0.05) * 20.0)
    assert black76_price(80.0, 100.0, 0.0, 1.0, 0.05, OptionRight.CALL) == 0.0


def test_an_expired_option_is_worth_its_intrinsic_value_and_nothing_else():
    assert black76_price(120.0, 100.0, 0.3, 0.0, 0.05, OptionRight.CALL) == pytest.approx(20.0)
    assert black76_price(120.0, 100.0, 0.3, 0.0, 0.05, OptionRight.PUT) == 0.0


def test_a_non_positive_forward_raises_rather_than_returning_a_number():
    with pytest.raises(ValueError, match="positive forward and strike"):
        black76_price(0.0, 100.0, 0.2, 1.0, 0.05, OptionRight.CALL)


def test_value_rises_with_volatility_and_with_time():
    base = black76_price(100.0, 100.0, 0.20, 1.0, 0.05, OptionRight.CALL)
    assert black76_price(100.0, 100.0, 0.30, 1.0, 0.05, OptionRight.CALL) > base
    assert black76_price(100.0, 100.0, 0.20, 2.0, 0.05, OptionRight.CALL) > base


# ---------------------------------------------------------------------------
# Greeks — checked numerically, in the documented units
# ---------------------------------------------------------------------------


ARGS = (100.0, 100.0, 0.20, 1.0, 0.05)


def test_delta_matches_a_numerical_derivative():
    greeks = black76_greeks(*ARGS, OptionRight.CALL)
    step = 1e-5
    up = black76_price(100.0 + step, 100.0, 0.20, 1.0, 0.05, OptionRight.CALL)
    down = black76_price(100.0 - step, 100.0, 0.20, 1.0, 0.05, OptionRight.CALL)
    assert greeks.delta == pytest.approx((up - down) / (2 * step), rel=1e-5)
    # e^-0.05 x N(0.10) = 0.951229 x 0.539828
    assert greeks.delta == pytest.approx(0.513491, abs=1e-5)


def test_put_delta_is_negative_and_parity_holds_on_the_deltas():
    call = black76_greeks(*ARGS, OptionRight.CALL)
    put = black76_greeks(*ARGS, OptionRight.PUT)
    assert put.delta < 0
    assert call.delta - put.delta == pytest.approx(math.exp(-0.05), rel=1e-10)


def test_gamma_matches_a_numerical_second_derivative():
    greeks = black76_greeks(*ARGS, OptionRight.CALL)
    step = 1e-3
    second = (
        black76_price(100.0 + step, 100.0, 0.20, 1.0, 0.05, OptionRight.CALL)
        - 2 * black76_price(*ARGS, OptionRight.CALL)
        + black76_price(100.0 - step, 100.0, 0.20, 1.0, 0.05, OptionRight.CALL)
    ) / (step * step)
    assert greeks.gamma == pytest.approx(second, rel=1e-4)
    assert black76_greeks(*ARGS, OptionRight.PUT).gamma == pytest.approx(greeks.gamma, rel=1e-12)


def test_vega_is_per_volatility_point_not_per_unit_of_sigma():
    greeks = black76_greeks(*ARGS, OptionRight.CALL)
    one_point = (
        black76_price(100.0, 100.0, 0.205, 1.0, 0.05, OptionRight.CALL)
        - black76_price(100.0, 100.0, 0.195, 1.0, 0.05, OptionRight.CALL)
    )
    assert greeks.vega == pytest.approx(one_point, rel=1e-4)
    assert 0.1 < greeks.vega < 1.0          # a per-point vega, not a x100 one


def test_theta_is_per_calendar_day_and_costs_the_holder():
    greeks = black76_greeks(*ARGS, OptionRight.CALL)
    one_day = (
        black76_price(100.0, 100.0, 0.20, 1.0 - 1 / 365.0, 0.05, OptionRight.CALL)
        - black76_price(*ARGS, OptionRight.CALL)
    )
    assert greeks.theta == pytest.approx(one_day, rel=1e-3)
    assert greeks.theta < 0


def test_rho_is_negative_because_the_premium_is_discounted():
    """For an option on a future the only rate dependence is the discount factor,
    so dV/dr = -T x V exactly. The finite difference over a whole rate point
    carries the curvature of e^{-rT} and lands ~0.5% away, which is the check.
    """
    price = black76_price(*ARGS, OptionRight.CALL)
    greeks = black76_greeks(*ARGS, OptionRight.CALL)
    assert greeks.rho == pytest.approx(-1.0 * price / 100.0, rel=1e-12)
    one_point = black76_price(100.0, 100.0, 0.20, 1.0, 0.06, OptionRight.CALL) - price
    assert greeks.rho == pytest.approx(one_point, rel=1e-2)
    assert greeks.rho < 0


def test_greeks_at_expiry_are_intrinsic_not_a_gamma_spike():
    greeks = black76_greeks(120.0, 100.0, 0.20, 0.0, 0.05, OptionRight.CALL)
    assert greeks.delta == 1.0
    assert (greeks.gamma, greeks.vega, greeks.theta, greeks.rho) == (0.0, 0.0, 0.0, 0.0)
    assert black76_greeks(80.0, 100.0, 0.20, 0.0, 0.05, OptionRight.PUT).delta == -1.0
    assert black76_greeks(120.0, 100.0, 0.20, 0.0, 0.05, OptionRight.PUT).delta == 0.0


# ---------------------------------------------------------------------------
# Implied volatility
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("vol", [0.08, 0.20, 0.45, 1.10])
@pytest.mark.parametrize("right", list(OptionRight))
def test_implied_volatility_round_trips(vol, right):
    premium = black76_price(1167.75, 1200.0, vol, 0.25, 0.04, right)
    assert implied_volatility(premium, 1167.75, 1200.0, 0.25, 0.04, right) == pytest.approx(
        vol, rel=1e-4
    )


def test_a_premium_below_intrinsic_has_no_solution_and_gets_none():
    """An arbitrage violation. Returning a vol for it would be inventing one."""
    assert implied_volatility(5.0, 120.0, 100.0, 1.0, 0.0, OptionRight.CALL) is None


def test_a_premium_above_the_forward_has_no_solution_either():
    assert implied_volatility(500.0, 100.0, 100.0, 1.0, 0.05, OptionRight.CALL) is None


def test_an_expired_or_negative_input_gets_none_rather_than_an_exception():
    assert implied_volatility(5.0, 100.0, 100.0, 0.0, 0.05, OptionRight.CALL) is None
    assert implied_volatility(-1.0, 100.0, 100.0, 1.0, 0.05, OptionRight.CALL) is None


# ---------------------------------------------------------------------------
# The manual workflow: every value names whose number it came from
# ---------------------------------------------------------------------------


def contract(right=OptionRight.CALL, strike=1200.0, style=OptionStyle.AMERICAN):
    return OptionContract(
        underlying=ZSX26, right=right, strike=strike,
        expiry=date(2026, 10, 23), style=style,
    )


def test_value_option_refuses_to_run_without_a_named_volatility_source():
    with pytest.raises(ValueError, match="volatility_source is required"):
        value_option(
            contract(), as_of=AS_OF, forward=1167.75, volatility=0.22, rate=0.04,
            volatility_source="   ",
        )


def test_a_manual_valuation_converts_the_premium_through_the_contract_size():
    """ZS: premium in c/bu x 5,000 bu / 100 = USD per contract."""
    valuation = value_option(
        contract(), as_of=AS_OF, forward=1167.75, volatility=0.22, rate=0.04,
        volatility_source="broker quote, 18 Aug",
    )
    assert valuation.premium == pytest.approx(
        black76_price(1167.75, 1200.0, 0.22, (date(2026, 10, 23) - AS_OF).days / 365.0,
                      0.04, OptionRight.CALL)
    )
    assert valuation.premium_usd == pytest.approx(valuation.premium * 50.0)
    assert valuation.price_type is PriceType.MANUAL
    assert valuation.volatility_source == "broker quote, 18 Aug"
    assert valuation.years == pytest.approx(66 / 365.0)


def test_a_manual_valuation_is_labelled_a_model_value_everywhere():
    payload = value_option(
        contract(), as_of=AS_OF, forward=1167.75, volatility=0.22, rate=0.04,
        volatility_source="broker quote",
    ).to_dict()
    assert payload["model"] == "black76"
    assert any("model value, not a market price" in c for c in payload["caveats"])
    assert payload["price_type"] == "manual"
    assert payload["greeks"]["vega_per_vol_point"] is not None


def test_an_american_option_says_its_value_is_a_floor():
    american = value_option(
        contract(style=OptionStyle.AMERICAN), as_of=AS_OF, forward=1167.75,
        volatility=0.22, rate=0.04, volatility_source="broker",
    )
    european = value_option(
        contract(style=OptionStyle.EUROPEAN), as_of=AS_OF, forward=1167.75,
        volatility=0.22, rate=0.04, volatility_source="broker",
    )
    assert any("floor rather than a price" in c for c in american.caveats)
    assert not any("floor rather than a price" in c for c in european.caveats)


def test_the_option_symbol_names_its_underlying_named_contract():
    assert contract().symbol == "ZSX26 1200 C"
    assert contract().to_dict()["underlying"] == "ZSX26"


# ---------------------------------------------------------------------------
# The chain: unavailable, never empty
# ---------------------------------------------------------------------------


def test_fetch_chain_is_unavailable_and_says_why():
    result = fetch_chain(ZSX26, as_of=AS_OF)
    assert isinstance(result, ChainUnavailable)
    assert result.available is False
    assert "No source ingested by this project publishes an option chain" in result.reason
    assert result.to_dict()["available"] is False


def test_the_unavailable_state_lists_what_a_provider_would_have_to_supply():
    fields = fetch_chain(ZSX26, as_of=AS_OF).required_fields
    assert "strike ladder" in fields
    assert "open interest" in fields
    assert "implied volatility or enough to compute it" in fields


def test_the_page_status_never_offers_an_empty_ladder():
    status = chain_status(ZSX26)
    assert status["available"] is False
    assert status["reason"] == NO_CHAIN_REASON
    assert status["underlying"] == "ZSX26"
    assert "quotes" not in status
    assert len(status["model_assumptions"]) == 4
    assert any("American" in a for a in status["model_assumptions"])


def test_the_page_status_works_before_a_contract_is_chosen():
    status = chain_status(None)
    assert status["underlying"] is None
    assert "Black-76" in status["manual_workflow"]


# ---------------------------------------------------------------------------
# The manual ladder — the workflow the module offered but could not accept
# ---------------------------------------------------------------------------

LADDER_DOC = {
    "options": [
        {
            "underlying": "ZSX26", "right": "call", "strike": 1200,
            "expiry": "2026-10-23", "quoted_on": "2026-08-18",
            "source": "Broker XYZ 15:40 CT", "premium": 24.5,
        },
        {
            "underlying": "ZSX26", "right": "put", "strike": 1100,
            "expiry": "2026-10-23", "quoted_on": "2026-08-18",
            "source": "Broker XYZ 15:40 CT", "implied_volatility": 0.185,
        },
    ]
}

AS_OF_OPT = date(2026, 8, 18)
FORWARDS = {"ZSX26": 1167.75}


def test_no_option_chain_is_available_from_the_incumbent_price_provider():
    """The empirical basis for everything in this section.

    Checked live against yfinance on 2026-08-18: ``Ticker(t).options`` returned
    an empty tuple for ZS=F, ZM=F, ZL=F, ZC=F, SB=F, CT=F and for the named
    contract ZSX26.CBT. The chain is not withheld by choice — there is none to
    fetch — which is why the fill is a hand-entry path rather than a fetcher.
    """
    contract = named_contract("Soybeans", 2026, 11)
    assert fetch_chain(contract, as_of=AS_OF_OPT).available is False
    assert "yfinance serves equity option chains only" in NO_CHAIN_REASON


def test_a_premium_and_a_volatility_are_two_ways_in_and_never_both():
    """Supplying both would let two inconsistent numbers sit on one row."""
    contract = OptionContract(
        underlying=named_contract("Soybeans", 2026, 11), right=OptionRight.CALL,
        strike=1200.0, expiry=date(2026, 10, 23),
    )
    with pytest.raises(OptionEntryError, match="exactly one of premium"):
        ManualQuote(contract=contract, source="x", quoted_on=AS_OF_OPT,
                    premium=24.5, implied_volatility=0.185)
    with pytest.raises(OptionEntryError, match="exactly one of premium"):
        ManualQuote(contract=contract, source="x", quoted_on=AS_OF_OPT)


def test_an_entered_quote_must_say_who_quoted_it():
    contract = OptionContract(
        underlying=named_contract("Soybeans", 2026, 11), right=OptionRight.CALL,
        strike=1200.0, expiry=date(2026, 10, 23),
    )
    with pytest.raises(OptionEntryError, match="source is required"):
        ManualQuote(contract=contract, source="   ", quoted_on=AS_OF_OPT, premium=24.5)


def test_an_entered_premium_is_turned_into_a_volatility_and_back_to_the_same_premium():
    """The round trip that proves the derivation, not just that it ran.

    Entering a premium of 24.5 backs out an implied volatility; re-pricing at
    that volatility must return 24.5. If it does not, the number on the page is
    not the number the broker quoted.
    """
    valued = value_manual_ladder(
        parse_ladder(LADDER_DOC, where="doc"), as_of=AS_OF_OPT, forwards=FORWARDS, rate=0.04,
    )
    call = valued[0]
    assert call["valued"] is True
    assert call["premium"] == pytest.approx(24.5, abs=1e-4)
    assert call["volatility_derived_from"] == "backed out of the entered premium by bisection"
    assert 0.0 < call["volatility"] < 2.0


def test_an_entered_volatility_is_used_as_given_rather_than_re_derived():
    valued = value_manual_ladder(
        parse_ladder(LADDER_DOC, where="doc"), as_of=AS_OF_OPT, forwards=FORWARDS, rate=0.04,
    )
    put = valued[1]
    assert put["volatility"] == pytest.approx(0.185)
    assert put["volatility_derived_from"] == "entered implied volatility"


def test_every_entered_row_is_labelled_a_model_value_carrying_the_human_source():
    valued = value_manual_ladder(
        parse_ladder(LADDER_DOC, where="doc"), as_of=AS_OF_OPT, forwards=FORWARDS, rate=0.04,
    )
    for row in valued:
        assert row["price_type"] == PriceType.MANUAL.value
        assert "Broker XYZ" in row["volatility_source"]
        assert any("model value, not a market price" in c for c in row["caveats"])
        # American style is the default, and the floor caveat must ride with it.
        assert any("early-exercise premium is not modelled" in c for c in row["caveats"])


def test_an_option_on_a_contract_the_board_has_no_price_for_is_not_valued():
    """Refusal with a reason, rather than a forward invented to fill the gap."""
    valued = value_manual_ladder(
        parse_ladder(LADDER_DOC, where="doc"), as_of=AS_OF_OPT, forwards={}, rate=0.04,
    )
    assert all(row["valued"] is False for row in valued)
    assert "no board price for ZSX26" in valued[0]["reason"]


def test_a_premium_outside_the_models_bounds_is_refused_not_clamped():
    """A premium above the forward has no implied volatility. Say so."""
    doc = {"options": [{
        "underlying": "ZSX26", "right": "call", "strike": 1200,
        "expiry": "2026-10-23", "quoted_on": "2026-08-18",
        "source": "typo test", "premium": 5000.0,
    }]}
    row = value_manual_ladder(
        parse_ladder(doc, where="doc"), as_of=AS_OF_OPT, forwards=FORWARDS, rate=0.04,
    )[0]
    assert row["valued"] is False
    assert "arbitrage bounds" in row["reason"]


@pytest.mark.parametrize("missing", ["underlying", "right", "strike", "expiry", "quoted_on"])
def test_a_document_missing_a_required_field_raises_rather_than_defaulting(missing):
    row = dict(LADDER_DOC["options"][0])
    row.pop(missing)
    with pytest.raises(OptionEntryError):
        parse_ladder({"options": [row]}, where="doc")


def test_an_unparseable_underlying_raises_rather_than_being_skipped():
    row = dict(LADDER_DOC["options"][0], underlying="NOTACONTRACT")
    with pytest.raises(OptionEntryError):
        parse_ladder({"options": [row]}, where="doc")


def test_a_missing_directory_is_an_empty_ladder_and_a_malformed_file_raises(tmp_path):
    """The positions rule, applied here: those two states must not look alike."""
    assert load_ladder(str(tmp_path / "nothing-here")).is_empty

    good = tmp_path / "ladder"
    good.mkdir()
    (good / "a.yml").write_text(
        "options:\n"
        "  - underlying: ZSX26\n    right: call\n    strike: 1200\n"
        "    expiry: 2026-10-23\n    quoted_on: 2026-08-18\n"
        "    source: Broker XYZ\n    premium: 24.5\n",
        encoding="utf-8",
    )
    loaded = load_ladder(str(good))
    assert len(loaded.quotes) == 1
    assert loaded.loaded_from == (str(good / "a.yml"),)

    (good / "b.yml").write_text("options:\n  - right: call\n", encoding="utf-8")
    with pytest.raises(OptionEntryError):
        load_ladder(str(good))


def test_an_empty_ladder_is_not_the_same_object_as_an_unavailable_chain():
    """Two different absences, and the page must keep them apart.

    ``ChainUnavailable`` is "the market has options and we cannot see them".
    An empty ``ManualLadder`` is "you have not typed any in". Conflating them
    would make our gap look like the market's.
    """
    assert ManualLadder().is_empty is True
    assert ChainUnavailable(
        underlying=named_contract("Soybeans", 2026, 11), reason=NO_CHAIN_REASON
    ).available is False
