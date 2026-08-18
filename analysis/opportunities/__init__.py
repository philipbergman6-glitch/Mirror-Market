"""The opportunity and counterparty engine (Phase 4).

The trader question this package answers: *who might buy or sell what, where,
during which window, why might the opportunity exist now, how strong is the
evidence, and what should I do next?*

Read in this order:

``domain``    the vocabulary — the ladder, the opportunity, blockers, evidence,
              scores, and the private workflow record. Standard library plus the
              Phase 2 value objects.
``signals``   the only module here that knows SQL exists: six deterministic
              detectors over ingested layers, each producing a measurement with
              its own evidence, observation date and validity horizon.
``rules``     measurement → candidate: counterparties from the players base,
              blockers, the ladder rung, and the next action.
``scoring``   five components, shown separately, and the weighted composite that
              is only a sort key.
``registry``  identity, duplicates, expiry, and the archive that makes an
              opportunity id stable across an ephemeral CI database.
``workflow``  the local, private trader record — status, owner, notes, contact
              dates, outcomes — and the privacy boundary.
``engine``    one run, in the order those steps have to happen in.

Three rules run through all of it.

**A price difference is not an arbitrage.** Policy, freight, quality, timing and
liquidity are carried as named blockers, and a hard one caps a candidate at
``LEAD`` by construction rather than by convention.

**Unknown stays unknown.** No counterparty, contact, tonnage, freight rate or
shipment window is invented to complete a row. Where the unknown is load-bearing
it becomes a blocker with a remedy naming who could resolve it.

**Private is a different object, not a different template.** Everything a person
typed lives on ``WorkflowRecord``; the public serialiser never builds the key,
the public page never receives an opportunity carrying one, and the git-committed
archive rejects the field names outright.
"""

from analysis.opportunities.domain import (
    Blocker,
    BlockerCode,
    Confidence,
    Counterparty,
    Evidence,
    Ladder,
    MarketSignal,
    Opportunity,
    OpportunityStatus,
    ScoreCard,
)

__all__ = [
    "Blocker",
    "BlockerCode",
    "Confidence",
    "Counterparty",
    "Evidence",
    "Ladder",
    "MarketSignal",
    "Opportunity",
    "OpportunityStatus",
    "ScoreCard",
]
