"""One vocabulary for what a price *is*, and one rule about what it may claim.

The bug these pin: the futures workstation correctly called a yfinance daily
bar a ``DELAYED_CLOSE`` whose settlement is unproven, while ``analysis/origins``
called the same observation a "board" quote, documented it as "three exchange
settlements", and mapped it to ``Confidence.EXECUTABLE`` — a word the origins
module itself reserves for "a price a hedge can actually be placed against".
The opportunity engine then scored that 100/100 and ranked on it, and the
Origins and Opportunities pages printed it.

Two mechanisms stop it recurring, and both are here rather than in a reviewer's
memory:

* the classification lives in exactly one module (:mod:`pricing.semantics`) and
  every other vocabulary re-exports it rather than defining its own, so a fifth
  surface cannot invent a sixth spelling of "close";
* the confidence a quote kind can support is *derived* from its price type's
  ceiling rather than restated, and the ceiling grants ``EXECUTABLE`` only to a
  price type that is settlement-proven — which today nothing is.
"""

from __future__ import annotations

import pytest

import config
from analysis.futures import domain as futures_domain
from analysis.opportunities import scoring as opportunity_scoring
from analysis.origins import domain as origins_domain
from analysis.origins.assumptions import AssumptionError, parse_assumption
from app import markets as markets_mod
from pricing.semantics import (
    CONFIDENCE_CEILING,
    PROVEN_SETTLEMENT_SOURCES,
    QUOTE_KIND_PRICE_TYPE,
    Confidence,
    PriceType,
    price_type_for_quote_kind,
    worst_confidence,
)


# ---------------------------------------------------------------------------
# The classification itself
# ---------------------------------------------------------------------------
def test_price_type_covers_the_seven_kinds_this_stack_carries():
    """Settlement, attested settlement, delayed close, last trade, assessment, administered, manual.

    Closed on purpose: an eighth kind is a decision about how it may be
    rendered and scored, and that decision belongs in this module rather than
    at whichever call site first needed it. ``attested_settlement`` is the
    seventh and was added deliberately here, for the clearing statements a
    client supplies: an official number for that account, attested by a named
    document rather than proven by a feed.
    """
    assert {member.value for member in PriceType} == {
        "settlement",
        "attested_settlement",
        "delayed_close",
        "last_trade",
        "assessment",
        "administered",
        "manual",
    }


def test_an_attested_settlement_is_official_without_being_settlement_proven():
    """The two halves of the distinction, in one place.

    It is the number the clearer margined the account at — so a P&L struck on
    it is an official figure, not a management estimate — and nothing this
    project ingests proves it, so it cannot buy ``EXECUTABLE`` and cannot put a
    name into ``PROVEN_SETTLEMENT_SOURCES``.
    """
    from pricing.semantics import PROVEN_SETTLEMENT_SOURCES

    attested = PriceType.ATTESTED_SETTLEMENT
    assert attested.is_settlement_proven is False
    assert CONFIDENCE_CEILING[attested] is not Confidence.EXECUTABLE
    assert frozenset() == PROVEN_SETTLEMENT_SOURCES
    assert "attested" in attested.label
    assert "not proven" in attested.caveat


def test_only_an_official_settlement_is_settlement_proven():
    for member in PriceType:
        assert member.is_settlement_proven is (member is PriceType.SETTLEMENT)


def test_nothing_this_stack_ingests_is_a_proven_settlement_source():
    """The empty set is the finding, not an oversight.

    No layer here buys an authoritative settlement feed, so no source may be
    named in this set. The day one is, that is the *only* edit that turns a
    delayed close into a settlement anywhere on the site.
    """
    assert frozenset() == PROVEN_SETTLEMENT_SOURCES
    assert not markets_mod.is_settlement_proven_source("yfinance")
    assert not markets_mod.is_settlement_proven_source("akshare")


# ---------------------------------------------------------------------------
# One vocabulary, not four
# ---------------------------------------------------------------------------
def test_the_futures_workstation_uses_the_canonical_price_type():
    """Same object, not a same-shaped copy — an alias cannot drift."""
    assert futures_domain.PriceType is PriceType


def test_origins_and_opportunities_use_the_canonical_confidence():
    assert origins_domain.Confidence is Confidence
    assert opportunity_scoring.Confidence is Confidence


def test_every_site_registry_quote_kind_has_a_price_type():
    """`app.markets.QUOTE_KINDS` and `origins.QuoteKind` are checked against
    each other and against the canonical mapping, so a new kind cannot be added
    to one of the three and quietly render as whatever its neighbours are."""
    assert {kind.value for kind in origins_domain.QuoteKind} == markets_mod.QUOTE_KINDS
    assert set(QUOTE_KIND_PRICE_TYPE) == markets_mod.QUOTE_KINDS
    for kind in markets_mod.QUOTE_KINDS:
        assert isinstance(price_type_for_quote_kind(kind), PriceType)


def test_an_unknown_quote_kind_raises_rather_than_defaulting():
    with pytest.raises(KeyError):
        price_type_for_quote_kind("board_ish")


# ---------------------------------------------------------------------------
# The rule: executable requires proof
# ---------------------------------------------------------------------------
def test_only_a_settlement_proven_price_type_can_reach_executable():
    for price_type, ceiling in CONFIDENCE_CEILING.items():
        if ceiling is Confidence.EXECUTABLE:
            assert price_type.is_settlement_proven, (
                f"{price_type} is not proven to be a settlement and must not be "
                "granted executable confidence"
            )


def test_every_price_type_has_a_ceiling():
    assert set(CONFIDENCE_CEILING) == set(PriceType)


def test_a_cbot_board_quote_is_a_delayed_close_and_is_not_executable():
    """The exact defect. A yfinance CBOT bar is `board` in the registry, and
    `board` is a delayed daily close whose settlement no provider proves."""
    assert price_type_for_quote_kind("board") is PriceType.DELAYED_CLOSE
    board_confidence = origins_domain.CONFIDENCE_BY_QUOTE_KIND[origins_domain.QuoteKind.BOARD]
    assert board_confidence is not Confidence.EXECUTABLE
    assert board_confidence is Confidence.BOARD_REFERENCE


def test_quote_kind_confidence_is_derived_from_the_ceiling_not_restated():
    """The second copy of a mapping is the second answer waiting to drift."""
    for kind in origins_domain.QuoteKind:
        assert origins_domain.CONFIDENCE_BY_QUOTE_KIND[kind] is CONFIDENCE_CEILING[
            price_type_for_quote_kind(kind.value)
        ]


def test_board_reference_ranks_below_executable_and_above_indicative():
    """A delayed CBOT close is a better number than a physical assessment and a
    worse one than a proven settlement. Worst-wins ordering has to say both."""
    assert worst_confidence(Confidence.EXECUTABLE, Confidence.BOARD_REFERENCE) is Confidence.BOARD_REFERENCE
    assert worst_confidence(Confidence.BOARD_REFERENCE, Confidence.INDICATIVE) is Confidence.INDICATIVE


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------
def test_scoring_covers_every_confidence_level():
    assert set(opportunity_scoring.CONFIDENCE_SCORE) == set(Confidence)


def test_a_delayed_close_does_not_score_full_marks():
    scores = opportunity_scoring.CONFIDENCE_SCORE
    assert scores[Confidence.EXECUTABLE] == 100.0
    assert scores[Confidence.BOARD_REFERENCE] < scores[Confidence.EXECUTABLE]
    assert scores[Confidence.BOARD_REFERENCE] > scores[Confidence.INDICATIVE]


def test_no_detector_may_stamp_executable_on_ingested_evidence():
    """Source-level guard on the engine's own detectors.

    ``signals.py`` is the only SQL-aware module in the opportunity engine and
    therefore the only place a confidence is attached to an ingested number.
    Nothing it ingests is settlement-proven, so the word must not appear.
    """
    from pathlib import Path

    source = Path(config.__file__).resolve().parent / "analysis" / "opportunities" / "signals.py"
    text = source.read_text(encoding="utf-8")
    assert "Confidence.EXECUTABLE" not in text


# ---------------------------------------------------------------------------
# Hand-entered inputs
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("word", ["executable", "board_reference"])
def test_an_entered_assumption_cannot_claim_a_market_price_confidence(word):
    raw = {
        "id": "freight-test",
        "component": "ocean_freight",
        "value": 38.0,
        "unit": "usd_per_mt",
        "basis": "per MT",
        "source": "broker indication",
        "entered_by": "tester",
        "entered_at": "2026-08-01",
        "expires_on": "2026-12-01",
        "confidence": word,
    }
    with pytest.raises(AssumptionError):
        parse_assumption(raw, where="test")


def test_a_hand_entered_input_is_a_manual_price_type():
    assert CONFIDENCE_CEILING[PriceType.MANUAL] is Confidence.PROVISIONAL
