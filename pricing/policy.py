"""What a surface may *claim* about a number, and what may reach a trade.

``pricing/semantics.py`` says what a number is. This module says what may then
be done with it — the two halves of one contract, deliberately separate files
because the classification is data and the policy is enforcement.

Standard library only, and it imports nothing from this repo except
``pricing.semantics``. Every consumer — ``analysis``, ``app``, ``trust``,
``scripts`` and the tests — reads the catalogue from here rather than keeping a
list of forbidden words of its own. A phrase list that lives in a test file
protects the surfaces that test happens to render; the same list here also runs
inside the promotion gate, which is what a *contract* means.

Why a catalogue at all
----------------------
The dangerous failure is not a template typo. It is a future feature reusing an
existing number correctly and describing it wrongly: an assessment rendered as
a firm offer, an administered minimum rendered as a traded market price, a
delayed consumer-endpoint bar rendered as the official close. Each of those
parses, prices and looks right. Nothing in the number itself objects.

Two mechanisms, because the claims fail in two different ways.

**Language** (:func:`scan`). Some claims can only be made in prose, so prose is
what has to be checked. The check is per *claim kind*, and a claim is permitted
only when the surface actually renders a price type that supports it — so the
one private surface carrying an attested clearing statement may say
"settlement", and no public page may.

**Structure** (:func:`require_hedgeable`, :func:`require_traded_price`,
:func:`assert_confidence_supported`). Some claims are made by *arithmetic*
rather than by words: sizing a futures leg against an administered minimum
asserts that the minimum is a traded price, whatever the caption says. Those
entry points refuse rather than caption.

Honest denials survive both. "Delayed daily closes, not proven exchange
settlements" has to keep being sayable — banning the word would delete the
sentence that tells the truth — so a claim negated in its own sentence is not a
claim.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum

from pricing.semantics import CONFIDENCE_CEILING, Confidence, PriceType

__all__ = [
    "AUTHORITY_TIER",
    "CLAIM_SUPPORTED_BY",
    "FORBIDDEN_CLAIMS",
    "NEGATIONS",
    "Claim",
    "ClaimKind",
    "NotHedgeable",
    "SemanticContractError",
    "Violation",
    "assert_confidence_supported",
    "assert_language_permitted",
    "may_claim",
    "permitted_claims",
    "require_hedgeable",
    "require_traded_price",
    "scan",
    "visible_text",
]


class SemanticContractError(AssertionError):
    """A surface, or a calculation, claimed more than its inputs support.

    ``AssertionError`` rather than ``ValueError``: every raise here is a broken
    invariant of this repository, not a bad argument from a user.
    """


class NotHedgeable(SemanticContractError):
    """A research artifact was offered where a tradeable instrument is required."""


class ClaimKind(str, Enum):
    """The five things a surface can wrongly say about a number here.

    Closed, like ``PriceType``: a sixth is a decision about what this project
    is willing to assert, and it belongs beside the others rather than in
    whichever template first needed it.
    """

    #: "the exchange settlement" — a claim about the *provider*, not the number.
    SETTLEMENT = "settlement"
    #: "the official close" — the same claim wearing the exchange's authority.
    OFFICIAL_CLOSE = "official_close"
    #: "an executable price" — that a hedge can be placed against it.
    EXECUTABLE = "executable"
    #: "a firm offer" — that a counterparty is bound to it.
    FIRM_OFFER = "firm_offer"
    #: "the traded price" — that trade, rather than decree or assessment, set it.
    TRADED_PRICE = "traded_price"


@dataclass(frozen=True)
class Claim:
    """One phrase that asserts a :class:`ClaimKind`, and why it is dangerous."""

    kind: ClaimKind
    #: Case-insensitive regex, matched against the tag-stripped text of a page.
    pattern: str
    why: str


#: The catalogue. Narrow phrases on purpose: "market price" and "firm" appear
#: honestly all over a commodity site, and a pattern that fires on them would be
#: switched off within a week, which is worse than no pattern at all.
FORBIDDEN_CLAIMS: tuple[Claim, ...] = (
    Claim(
        ClaimKind.SETTLEMENT, r"exchange settlements?",
        "no provider here publishes the exchange's settlement",
    ),
    Claim(
        ClaimKind.SETTLEMENT, r"board settlements?",
        "the board legs are delayed daily bars from a consumer endpoint",
    ),
    Claim(
        ClaimKind.SETTLEMENT, r"futures settlements?",
        "same claim, said about the futures leg",
    ),
    Claim(
        ClaimKind.SETTLEMENT, r"official settlements?",
        "an official settlement is proven by a provider, and none is",
    ),
    Claim(
        ClaimKind.SETTLEMENT, r"settlement[- ]derived",
        "a number derived from a settlement inherits the unproven claim",
    ),
    Claim(
        ClaimKind.SETTLEMENT, r"settlement prices?",
        "the stored close is not verified to equal the settlement",
    ),
    Claim(
        ClaimKind.SETTLEMENT, r"settles? at",
        "asserts the venue's own settlement for a delayed bar",
    ),
    Claim(
        ClaimKind.OFFICIAL_CLOSE, r"official clos(?:e|ing price)",
        "the venue's official close is not ingested anywhere in this stack",
    ),
    Claim(
        ClaimKind.OFFICIAL_CLOSE, r"closing settlements?",
        "same claim, in the other word order",
    ),
    Claim(
        ClaimKind.EXECUTABLE, r"executable",
        "nothing here is proven placeable — see PROVEN_SETTLEMENT_SOURCES",
    ),
    Claim(
        ClaimKind.EXECUTABLE, r"trad(?:e|a)ble prices?",
        "a synonym for executable, and just as unproven",
    ),
    Claim(
        ClaimKind.EXECUTABLE, r"you can (?:trade|hedge|transact) (?:at|on) th(?:is|at)",
        "tells the reader an unproven number is placeable",
    ),
    Claim(
        ClaimKind.FIRM_OFFER, r"firm (?:offers?|bids?|quotes?|prices?)",
        "this project ingests no counterparty quote; every physical leg is an assessment",
    ),
    Claim(
        ClaimKind.FIRM_OFFER, r"binding (?:offers?|quotes?)",
        "same claim; an assessment binds nobody to us",
    ),
    Claim(
        ClaimKind.FIRM_OFFER, r"guaranteed prices?",
        "no price here is guaranteed to anybody",
    ),
    # "last-traded price" is excluded, and the exclusion is the point: SAFEX
    # genuinely publishes one, `PriceType.LAST_TRADE` supports the claim, and a
    # pattern that fired on the honest label would be switched off within a
    # week. What is forbidden is the bare assertion that trade set the level.
    Claim(
        ClaimKind.TRADED_PRICE, r"(?<!last-)(?<!last )traded (?:market )?prices?",
        "an administered minimum and an assessment are not prices anybody traded at",
    ),
    Claim(
        ClaimKind.TRADED_PRICE, r"prices? anybody transacted at",
        "the same statement as the caveat makes, with the denial removed",
    ),
    Claim(
        ClaimKind.TRADED_PRICE, r"where (?:the )?market traded",
        "asserts trade set a level that may have been set by decree",
    ),
)


#: Which price types support each claim. Everything else in this module is
#: derived from this table and from ``pricing.semantics``.
#:
#: ``EXECUTABLE`` is reachable only from a proven ``SETTLEMENT``, matching
#: ``CONFIDENCE_CEILING`` — and since ``PROVEN_SETTLEMENT_SOURCES`` is empty, no
#: number this project ingests reaches it. ``FIRM_OFFER`` is supported by
#: *nothing*: a firm offer requires a counterparty bound to it, and no layer
#: here carries one. ``TRADED_PRICE`` is the discriminating case — a delayed
#: board close and a last trade came out of a trade, an assessment and an
#: administered minimum did not, and a hand-entered number is not a market
#: observation at all.
CLAIM_SUPPORTED_BY: dict[ClaimKind, frozenset[PriceType]] = {
    ClaimKind.SETTLEMENT: frozenset({PriceType.SETTLEMENT, PriceType.ATTESTED_SETTLEMENT}),
    ClaimKind.OFFICIAL_CLOSE: frozenset({PriceType.SETTLEMENT, PriceType.ATTESTED_SETTLEMENT}),
    ClaimKind.EXECUTABLE: frozenset({PriceType.SETTLEMENT}),
    ClaimKind.FIRM_OFFER: frozenset(),
    ClaimKind.TRADED_PRICE: frozenset({
        PriceType.SETTLEMENT,
        PriceType.ATTESTED_SETTLEMENT,
        PriceType.DELAYED_CLOSE,
        PriceType.LAST_TRADE,
    }),
}

#: Price types that came out of a trade in a market. The complement is what
#: :func:`require_traded_price` refuses.
TRADED_PRICE_TYPES: frozenset[PriceType] = CLAIM_SUPPORTED_BY[ClaimKind.TRADED_PRICE]

#: Words that turn a claim into a denial. The denial has to sit in the *same
#: sentence*: "delayed daily closes, not proven exchange settlements" is honest,
#: while "Three exchange settlements. This is what a hedge locks, not what a
#: plant earns" is the origins-page bug — its second sentence denies something
#: else entirely.
NEGATIONS: tuple[str, ...] = (
    "not", "never", "nothing", "unproven", "unverified",
    "rather than", "cannot", "without", "no ",
)


def may_claim(price_type: PriceType, kind: ClaimKind) -> bool:
    """Whether a number of this kind can support this claim on its own."""
    return price_type in CLAIM_SUPPORTED_BY[kind]


def permitted_claims(price_types: Iterable[PriceType]) -> frozenset[ClaimKind]:
    """Every claim the given set of numbers could legitimately support."""
    types = frozenset(price_types)
    return frozenset(
        kind for kind in ClaimKind if CLAIM_SUPPORTED_BY[kind] & types
    )


# ---------------------------------------------------------------------------
# Language
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Violation:
    """One unnegated claim, with the sentence that makes it."""

    surface: str
    kind: ClaimKind
    phrase: str
    sentence: str
    why: str

    def describe(self) -> str:
        return (
            f"{self.surface}: {self.phrase!r} asserts {self.kind.value} — {self.why}; "
            f"sentence: {self.sentence!r}"
        )


_TAG = re.compile(r"<[^>]+>")
_DROPPED_ELEMENTS = re.compile(
    r"<(script|style)\b.*?</\1>|<!--.*?-->", re.IGNORECASE | re.DOTALL
)
_WHITESPACE = re.compile(r"\s+")
_COMPILED: tuple[tuple[Claim, re.Pattern[str]], ...] = tuple(
    (claim, re.compile(claim.pattern, re.IGNORECASE)) for claim in FORBIDDEN_CLAIMS
)


def visible_text(markup: str) -> str:
    """What a reader actually sees: no tags, no script payloads, no comments.

    Chart data is dropped deliberately. A Plotly payload is megabytes of
    strings nobody reads, and scanning it would trade a real check for a
    stream of false positives.
    """
    without_code = _DROPPED_ELEMENTS.sub(" ", markup)
    return _WHITESPACE.sub(" ", _TAG.sub(" ", without_code))


def _sentence_around(text: str, start: int, end: int) -> str:
    left = max(
        text.rfind(".", 0, start),
        text.rfind(";", 0, start),
        text.rfind("!", 0, start),
        text.rfind("?", 0, start),
    )
    right = min(
        (pos for pos in (text.find(".", end), text.find(";", end)) if pos != -1),
        default=len(text),
    )
    return text[left + 1:right].strip()


def scan(
    markup: str,
    *,
    surface: str = "",
    price_types: Iterable[PriceType] = (),
) -> tuple[Violation, ...]:
    """Every claim in ``markup`` that ``price_types`` does not support.

    ``price_types`` is what this surface actually renders — the empty default
    means "nothing that supports any claim", which is the correct posture for a
    public page. Passing a set is how the private book is allowed to say
    "settlement" about the one attested statement in the repository.
    """
    allowed = permitted_claims(price_types)
    text = visible_text(markup)
    lowered = text.lower()
    found: list[Violation] = []
    for claim, pattern in _COMPILED:
        if claim.kind in allowed:
            continue
        for match in pattern.finditer(lowered):
            sentence = _sentence_around(lowered, match.start(), match.end())
            if any(marker in sentence for marker in NEGATIONS):
                continue
            found.append(Violation(
                surface=surface,
                kind=claim.kind,
                phrase=text[match.start():match.end()],
                sentence=sentence,
                why=claim.why,
            ))
    return tuple(found)


def assert_language_permitted(
    markup: str,
    *,
    surface: str,
    price_types: Iterable[PriceType] = (),
) -> None:
    """Raise :class:`SemanticContractError` on the first unsupported claim."""
    violations = scan(markup, surface=surface, price_types=price_types)
    if violations:
        raise SemanticContractError(
            f"{len(violations)} misleading claim(s) on {surface}:\n"
            + "\n".join(f"  - {violation.describe()}" for violation in violations)
        )


# ---------------------------------------------------------------------------
# Structure
# ---------------------------------------------------------------------------
def require_hedgeable(instrument: object, *, calculation: str) -> None:
    """Refuse anything that does not declare itself a hedgeable instrument.

    Opt-in rather than opt-out: ``NamedContract`` and ``ContractQuote`` set
    ``is_hedgeable = True`` and everything else — a ``ContinuousSeries``, a
    bare price, ``None``, whatever the next module invents — is refused by
    default. The alternative (blacklisting ``ContinuousSeries``) is safe only
    until the second research artifact exists.
    """
    if getattr(instrument, "is_hedgeable", False) is not True:
        raise NotHedgeable(
            f"{calculation}: {type(instrument).__name__} is not a hedgeable instrument. "
            "A hedge is placed on a named contract; a stitched or continuous series is a "
            "research artifact whose underlying contract changes on a schedule nobody here "
            "publishes."
        )


def require_traded_price(price_type: PriceType, *, context: str) -> None:
    """Refuse a price that no trade set, where a traded market price is required.

    Sizing a futures leg against an administered minimum, or against a physical
    assessment, asserts by arithmetic that the number is a market price. The
    caption cannot undo that, so the calculation refuses instead.
    """
    if price_type not in TRADED_PRICE_TYPES:
        raise SemanticContractError(
            f"{context}: a {price_type.label} is not a traded market price "
            f"({price_type.caveat})"
        )


#: How much *authority* each confidence claims, which is not the same axis as
#: ``CONFIDENCE_RANK``.
#:
#: ``CONFIDENCE_RANK`` is a worst-wins display ordering, and it deliberately
#: sorts ``administered`` below ``indicative`` so that a row mixing the two
#: reports the administered complaint — "precisely known, and not a traded
#: price" — rather than the vaguer one. That is right for choosing what to say
#: and wrong for deciding what may be claimed: an administered minimum is not a
#: *weaker* number than an assessment, it is a different kind of number. Here
#: they are peers, and only the genuine escalations — to a board reference, to
#: executable — are refused.
AUTHORITY_TIER: dict[Confidence, int] = {
    Confidence.EXECUTABLE: 0,
    Confidence.BOARD_REFERENCE: 1,
    Confidence.INDICATIVE: 2,
    Confidence.ADMINISTERED: 2,
    Confidence.PROVISIONAL: 3,
    Confidence.UNAVAILABLE: 4,
}


def assert_confidence_supported(
    confidence: Confidence, *, price_type: PriceType, context: str
) -> None:
    """Refuse a confidence claiming more authority than the price type has.

    Asserted at construction rather than checked at render: a row that claims
    ``executable`` has already been ranked on by the time a page shows it.
    """
    ceiling = CONFIDENCE_CEILING[price_type]
    if AUTHORITY_TIER[confidence] < AUTHORITY_TIER[ceiling]:
        raise SemanticContractError(
            f"{context}: a {price_type.label} cannot support {confidence.value} confidence — "
            f"its ceiling is {ceiling.value} ({price_type.caveat})"
        )
