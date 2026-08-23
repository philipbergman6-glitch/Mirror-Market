"""The TradingView symbol mapping: a venue registry, not a code path.

The embedded widget is a labelled exception to the "not a real-time feed"
line (owner decision 2026-08-23, #320). These tests hold the two things that
keep the exception honest: a symbol is only ever built for a venue this
project has *checked*, and an unmapped venue yields nothing at all rather
than a plausible string that would render a chart of the wrong contract.
"""

from __future__ import annotations

from datetime import date

import pytest

from analysis.futures.domain import Exchange, named_contract, spec_for
from app.tradingview import (
    TRADINGVIEW_EXCHANGES,
    tradingview_symbol,
)


def _contract(commodity: str, year: int, month: int):
    return named_contract(commodity, year=year, month=month)


def test_cbot_contract_maps_to_the_venue_prefixed_four_digit_symbol():
    # The exchange's own ZMU26 becomes TradingView's CBOT:ZMU2026 — same root,
    # same month code, four-digit year. Verified against the live symbol page.
    assert tradingview_symbol(_contract("Soybean Meal", 2026, 9)) == "CBOT:ZMU2026"


def test_the_soy_complex_maps_on_every_leg():
    assert tradingview_symbol(_contract("Soybeans", 2026, 11)) == "CBOT:ZSX2026"
    assert tradingview_symbol(_contract("Soybean Meal", 2026, 12)) == "CBOT:ZMZ2026"
    assert tradingview_symbol(_contract("Soybean Oil", 2027, 1)) == "CBOT:ZLF2027"


def test_a_year_past_the_century_keeps_four_digits():
    # The exchange symbol truncates to two digits and would collide across
    # decades; the TradingView one must not inherit that.
    contract = _contract("Soybeans", 2030, 11)
    assert contract.symbol == "ZSX30"
    assert tradingview_symbol(contract) == "CBOT:ZSX2030"


@pytest.mark.parametrize(
    ("commodity", "month"),
    [("Live Cattle", 12), ("Sugar", 3), ("Cotton", 12)],
)
def test_an_unmapped_venue_yields_nothing_rather_than_a_guess(commodity, month):
    """Until a venue's symbols are checked against TradingView itself, a row
    gets no expander — withholding with a reason, never a widget pointed at a
    symbol nobody verified. CME and ICE remain unchecked."""
    spec = spec_for(commodity)
    assert spec.exchange not in TRADINGVIEW_EXCHANGES
    assert tradingview_symbol(_contract(commodity, 2026, month)) is None


def test_the_registry_is_the_only_place_a_venue_is_named():
    """Invariant 5: adding a venue is a registry entry, never a branch.

    Still CBOT-only after #321: the coverage check found no other venue
    TradingView's free embed widget will serve (DCE and SAFEX absent from the
    platform, NCDEX soy suspended to 2027-03-31, MATIF symbol-addressable but
    refused by the widget in a live embed test) — the registry comment
    records each verdict, and #328 tracks the widget refusing CBOT too."""
    assert TRADINGVIEW_EXCHANGES == {Exchange.CBOT: "CBOT"}


def test_every_mapped_exchange_is_a_real_exchange_member():
    for exchange in TRADINGVIEW_EXCHANGES:
        assert isinstance(exchange, Exchange)


def test_symbol_is_pure_code_and_needs_no_date_context():
    # No network, no clock: the same contract maps identically whenever asked.
    contract = _contract("Soybeans", 2026, 11)
    assert tradingview_symbol(contract) == tradingview_symbol(contract)
    assert contract.contract_month_date == date(2026, 11, 1)
