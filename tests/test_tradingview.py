"""The third-party symbol mappings: venue registries, not code paths.

The link-out to TradingView and Barchart is a labelled exception to the "not
a real-time feed" line (owner decision 2026-08-23, #320, narrowed from an
embed to links by #328). These tests hold the two things that keep the
exception honest: a symbol or URL is only ever built for a venue this project
has *checked*, and an unmapped venue yields nothing at all rather than a
plausible string that would link to the wrong contract.
"""

from __future__ import annotations

from datetime import date

import pytest

from analysis.futures.domain import Exchange, named_contract, spec_for
from app.tradingview import (
    BARCHART_EXCHANGES,
    TRADINGVIEW_EXCHANGES,
    barchart_url,
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


def test_barchart_keys_by_the_bare_exchange_symbol():
    # Verified against the live page 2026-08-23 (#328 research): Barchart's
    # per-contract chart lives at /futures/quotes/<exchange symbol>/….
    assert barchart_url(_contract("Soybeans", 2026, 11)) == (
        "https://www.barchart.com/futures/quotes/ZSX26/interactive-chart"
    )
    assert barchart_url(_contract("Soybean Meal", 2026, 12)) == (
        "https://www.barchart.com/futures/quotes/ZMZ26/interactive-chart"
    )


@pytest.mark.parametrize(
    ("commodity", "month"),
    [("Live Cattle", 12), ("Sugar", 3), ("Cotton", 12)],
)
def test_an_unmapped_venue_yields_nothing_rather_than_a_guess(commodity, month):
    """Until a venue's symbols are checked against the third party itself, a
    row gets no link — withholding with a reason, never a URL built on a
    convention nobody verified. CME and ICE remain unchecked, on both
    registries."""
    spec = spec_for(commodity)
    assert spec.exchange not in TRADINGVIEW_EXCHANGES
    assert spec.exchange not in BARCHART_EXCHANGES
    assert tradingview_symbol(_contract(commodity, 2026, month)) is None
    assert barchart_url(_contract(commodity, 2026, month)) is None


def test_the_registry_is_the_only_place_a_venue_is_named():
    """Invariant 5: adding a venue is a registry entry, never a branch.

    Still CBOT-only after #321: the coverage check found no other venue worth
    a row (DCE and SAFEX absent from TradingView, NCDEX soy suspended to
    2027-03-31, MATIF licence-gated) — and #328 then found the embed widget
    refusing CBOT too, which is why both registries now feed links, not a
    widget."""
    assert TRADINGVIEW_EXCHANGES == {Exchange.CBOT: "CBOT"}
    assert frozenset({Exchange.CBOT}) == BARCHART_EXCHANGES


def test_every_mapped_exchange_is_a_real_exchange_member():
    for exchange in list(TRADINGVIEW_EXCHANGES) + list(BARCHART_EXCHANGES):
        assert isinstance(exchange, Exchange)


def test_symbol_is_pure_code_and_needs_no_date_context():
    # No network, no clock: the same contract maps identically whenever asked.
    contract = _contract("Soybeans", 2026, 11)
    assert tradingview_symbol(contract) == tradingview_symbol(contract)
    assert contract.contract_month_date == date(2026, 11, 1)
