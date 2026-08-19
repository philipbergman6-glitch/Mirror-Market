"""The canonical classification of what a stored number is, and what it may claim.

Standard library only. Everything else in this repo may import it; it imports
nothing from this repo, so it cannot grow a database's or a template's opinions.

Why it exists
-------------
The stack held four vocabularies for the same observation. ``analysis/futures``
called a yfinance daily bar a ``DELAYED_CLOSE`` and said in as many words that
nothing may label it a settlement. ``analysis/origins`` called it ``board`` and
documented it as "three exchange settlements", mapping it to
``Confidence.EXECUTABLE`` — the value its own docstring reserves for "a price a
hedge can actually be placed against". ``analysis/opportunities`` inherited that
confidence, scored it 100/100 and ranked a board on it. ``trust`` stamped
``price_type="settlement"`` into the identity of the same rows.

Every one of those is the same number from the same consumer endpoint. The
disagreement was not a wording slip: it changed what the Opportunities board
told a trader was workable.

The two rules this module encodes
---------------------------------
**A settlement is a claim about the provider, not about the number.** A daily
bar from a delayed consumer feed usually equals the exchange settlement. Usually
is not proven, and nothing here has a provider that proves it, so
:data:`PROVEN_SETTLEMENT_SOURCES` is empty. It is the single edit that turns a
delayed close into a settlement across the whole site, on the day an
authoritative feed is bought.

**Confidence is derived from the price type, never asserted beside it.**
:data:`CONFIDENCE_CEILING` grants ``EXECUTABLE`` only to a price type that is
settlement-proven. A delayed board close gets ``BOARD_REFERENCE``: better than a
physical assessment — it is the venue's own last daily print and the calculation
built on it is genuinely useful — and not a number a hedge is proven to be
placeable against.
"""

from __future__ import annotations

from enum import Enum

__all__ = [
    "CONFIDENCE_CEILING",
    "CONFIDENCE_RANK",
    "NON_PRICE_QUOTE_KINDS",
    "PROVEN_SETTLEMENT_SOURCES",
    "QUOTE_KIND_LABELS",
    "QUOTE_KIND_PRICE_TYPE",
    "Confidence",
    "PriceType",
    "is_price_quote_kind",
    "is_settlement_proven_source",
    "price_type_for_quote_kind",
    "quote_kind_label",
    "worst_confidence",
]


class PriceType(str, Enum):
    """What sort of number a quote is. Closed, and closed on purpose.

    A seventh member is a decision about how that kind of number may be
    rendered and scored, and the decision belongs here — beside the ceiling and
    the caveat — rather than at whichever call site first needed it.
    """

    SETTLEMENT = "settlement"          # official, proven by the provider
    #: The official settlement as printed on a clearing or broker statement the
    #: *user* supplied. It is authoritative for that account — it is the number
    #: the clearer will margin against — and it is still not proven by anything
    #: this project ingests, so it does not make :data:`PROVEN_SETTLEMENT_SOURCES`
    #: non-empty and it does not reach ``EXECUTABLE``. The distinction is the
    #: whole point of the member: a P&L struck on it is an *official* figure,
    #: while a P&L struck on a delayed close is a management estimate, and a
    #: reader who cannot tell them apart will reconcile the wrong one.
    ATTESTED_SETTLEMENT = "attested_settlement"
    DELAYED_CLOSE = "delayed_close"    # delayed daily bar; settlement unproven
    LAST_TRADE = "last_trade"          # last print, explicitly not a settlement
    ASSESSMENT = "assessment"          # a published physical assessment
    ADMINISTERED = "administered"      # set by decree, not by trade
    MANUAL = "manual"                  # hand-entered by the user

    @property
    def is_settlement_proven(self) -> bool:
        return self is PriceType.SETTLEMENT

    @property
    def label(self) -> str:
        """The words a page uses. One spelling per kind, everywhere."""
        return _LABELS[self]

    @property
    def caveat(self) -> str:
        """What the reader is owed beside the number."""
        return _CAVEATS[self]


_LABELS: dict[PriceType, str] = {
    PriceType.SETTLEMENT: "settlement",
    PriceType.ATTESTED_SETTLEMENT: "attested settlement",
    PriceType.DELAYED_CLOSE: "delayed close",
    PriceType.LAST_TRADE: "last traded",
    PriceType.ASSESSMENT: "assessment",
    PriceType.ADMINISTERED: "administered",
    PriceType.MANUAL: "hand entered",
}

_CAVEATS: dict[PriceType, str] = {
    PriceType.SETTLEMENT: (
        "Official settlement, proven by the provider."
    ),
    PriceType.ATTESTED_SETTLEMENT: (
        "The official settlement as printed on a clearing or broker statement supplied "
        "by the user. Authoritative for that account and attested by the named document, "
        "not proven by any feed this project ingests."
    ),
    PriceType.DELAYED_CLOSE: (
        "Settlement not proven — a delayed daily bar from a consumer endpoint. It "
        "usually equals the exchange settlement and is not verified to."
    ),
    PriceType.LAST_TRADE: (
        "Settlement not published for this venue on any free feed — this is the last "
        "print of the session, which is a thinner number than a settlement, not a "
        "synonym for one."
    ),
    PriceType.ASSESSMENT: (
        "A published assessment of where physical business was done. Directional, "
        "not firm to us."
    ),
    PriceType.ADMINISTERED: (
        "Set by decree rather than by trade. Precisely known and legally binding, "
        "and not a price anybody transacted at."
    ),
    PriceType.MANUAL: (
        "Hand entered by a person, with an owner and an expiry. Never a market "
        "observation."
    ),
}


class Confidence(str, Enum):
    """How much weight a number will bear. Ordered worst-last.

    Computed rather than asserted: a row's confidence is the *worst* of its
    inputs', so one hand-entered freight guess drags an otherwise-firm row down
    to ``provisional`` and the page says so.
    """

    EXECUTABLE = "executable"              # a proven settlement a hedge is placeable against
    BOARD_REFERENCE = "board_reference"    # the venue's own delayed daily close
    INDICATIVE = "indicative"              # an assessment or bid — directional, not firm to us
    ADMINISTERED = "administered"          # set by decree, not by trade (Ley 21.453)
    PROVISIONAL = "provisional"            # one input is inferred or entered rather than observed
    UNAVAILABLE = "unavailable"            # no value at all


#: Worst-wins ordering. Explicit rather than definition order, because a
#: reorder for readability must not silently change which input dominates.
CONFIDENCE_RANK: dict[Confidence, int] = {
    Confidence.EXECUTABLE: 0,
    Confidence.BOARD_REFERENCE: 1,
    Confidence.INDICATIVE: 2,
    Confidence.ADMINISTERED: 3,
    Confidence.PROVISIONAL: 4,
    Confidence.UNAVAILABLE: 5,
}


#: The best confidence a quote of each price type can support on its own.
#:
#: ``EXECUTABLE`` is reachable only from ``SETTLEMENT``, and that invariant is
#: asserted in the tests rather than left to the reader of this literal.
#: ``ADMINISTERED`` sits below ``INDICATIVE`` here for the same reason it does
#: in the scoring table: it is not a *weak* number — it is precisely known — it
#: simply is not a traded one, which is a different complaint.
CONFIDENCE_CEILING: dict[PriceType, Confidence] = {
    PriceType.SETTLEMENT: Confidence.EXECUTABLE,
    # Not EXECUTABLE: the statement proves what the account was margined at
    # yesterday, which is a different claim from "a hedge can be placed here".
    # The invariant that only SETTLEMENT reaches EXECUTABLE is asserted in the
    # tests, and this member deliberately does not weaken it.
    PriceType.ATTESTED_SETTLEMENT: Confidence.BOARD_REFERENCE,
    PriceType.DELAYED_CLOSE: Confidence.BOARD_REFERENCE,
    PriceType.LAST_TRADE: Confidence.INDICATIVE,
    PriceType.ASSESSMENT: Confidence.INDICATIVE,
    PriceType.ADMINISTERED: Confidence.ADMINISTERED,
    PriceType.MANUAL: Confidence.PROVISIONAL,
}


#: The site registry's quote kinds, mapped to what kind of number each is.
#:
#: Keyed by the registry's own strings rather than by an enum, so that
#: ``app.markets`` (which validates the registry) and
#: ``analysis.origins.domain`` (which models the economics) can both consult it
#: without either importing the other.
QUOTE_KIND_PRICE_TYPE: dict[str, PriceType] = {
    # CBOT and DCE reach this stack through yfinance and akshare — delayed
    # daily bars. Neither provider publishes, or claims to publish, the
    # exchange's settlement.
    "board": PriceType.DELAYED_CLOSE,
    # SAFEX via Grain SA: the free table carries no settlement column at all,
    # so what is stored is the last trade of the session.
    "board_last_traded": PriceType.LAST_TRADE,
    "physical": PriceType.ASSESSMENT,
    "administered": PriceType.ADMINISTERED,
    "weekly_assessment": PriceType.ASSESSMENT,
}


#: Chips and column labels. ``board`` alone reads as "the exchange's number";
#: the reader has no way to know it is a consumer-endpoint daily bar unless the
#: label says so, and the label is the only thing on a market page that names
#: the animal.
QUOTE_KIND_LABELS: dict[str, str] = {
    "board": "board · delayed close",
    "board_last_traded": "board · last traded",
    "physical": "physical assessment",
    "administered": "administered minimum",
    "weekly_assessment": "weekly assessment",
}


#: Quote kinds that are not prices at all — a tonnage flow, a balance-sheet
#: ratio, a temperature. They have no price type, so they have no confidence
#: ceiling and no claim they could support; a consumer that asks for one is
#: asking the wrong question about the row.
#:
#: Named rather than defaulted, and deliberately *not* folded into
#: :data:`QUOTE_KIND_PRICE_TYPE`: "this is not a price" and "nobody classified
#: this yet" are different facts, and the second must keep raising.
NON_PRICE_QUOTE_KINDS: frozenset[str] = frozenset({"observation"})


def is_price_quote_kind(quote_kind: str) -> bool:
    """Whether this quote kind denotes a price at all."""
    return quote_kind not in NON_PRICE_QUOTE_KINDS


#: Providers whose feed proves an official settlement. Empty, and the emptiness
#: is the finding: this project ingests no authoritative settlement feed. Adding
#: a name here is the one edit that lets ``EXECUTABLE`` reach a page.
PROVEN_SETTLEMENT_SOURCES: frozenset[str] = frozenset()


def is_settlement_proven_source(source: str) -> bool:
    """Whether this named provider proves the settlement it publishes."""
    return source.strip().lower() in PROVEN_SETTLEMENT_SOURCES


def price_type_for_quote_kind(quote_kind: str) -> PriceType:
    """The kind of number a registry quote kind denotes.

    Raises ``KeyError`` on anything unknown rather than defaulting: a quote kind
    nobody classified would silently inherit whichever ceiling the default
    happened to be, which is exactly the bug this module exists to close.
    """
    try:
        return QUOTE_KIND_PRICE_TYPE[quote_kind]
    except KeyError as exc:
        raise KeyError(
            f"unknown quote kind {quote_kind!r} — classify it in "
            f"pricing.semantics.QUOTE_KIND_PRICE_TYPE; known: "
            f"{sorted(QUOTE_KIND_PRICE_TYPE)}"
        ) from exc


def quote_kind_label(quote_kind: str) -> str:
    """The words a chip shows for a quote kind."""
    try:
        return QUOTE_KIND_LABELS[quote_kind]
    except KeyError as exc:
        raise KeyError(f"unknown quote kind {quote_kind!r}") from exc


def confidence_for_quote_kind(quote_kind: str) -> Confidence:
    """The best confidence a quote of this kind can support, before adjustment."""
    return CONFIDENCE_CEILING[price_type_for_quote_kind(quote_kind)]


def worst_confidence(*values: Confidence | None) -> Confidence:
    """The weakest link. ``None`` is ignored; no values at all is UNAVAILABLE."""
    present = [value for value in values if value is not None]
    if not present:
        return Confidence.UNAVAILABLE
    return max(present, key=lambda value: CONFIDENCE_RANK[value])
