"""Identity, duplicates and expiry (Phase 4) — requirement 7.

An opportunity screen that mints a fresh id every morning is a screen nobody can
work: yesterday's note, yesterday's owner and yesterday's decision all detach
from the row they were about. So identity is the first thing this module fixes,
and it fixes it *without reference to any number*.

Three jobs, in the order they matter.

**Identity.** ``(rule, product, origin, destination, window)`` — see
``domain.identity_key``. The same lane on two consecutive days is one
opportunity that moved. The id is that identity plus the date it was **first**
seen, recovered from the ``opportunity_detections`` archive, which is why that
table round-trips through ``data/history/``: on an ephemeral CI runner the
archive is the only memory there is.

**Duplicates, of two kinds.** *Exact* duplicates — the same identity twice in
one run — are a bug and are collapsed, keeping the higher rung. *Near*
duplicates — two different rules firing on the same product and lane — are not
a bug at all; they are corroboration, and collapsing them would throw away the
second rule's evidence. They are linked instead, so the page can say "this is
also showing up as a flow shift" rather than presenting two independent
findings that are one story.

**Expiry.** An opportunity whose evidence has aged past the shortest of its
sources' budgets is expired, and it stays visible for
``config.OPPORTUNITY_EXPIRY_GRACE_DAYS`` marked as such before dropping off
entirely. A screen that silently deletes yesterday's items cannot be checked
against yesterday's decisions — which is the only way anybody finds out whether
the rules are any good.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import replace
from datetime import date, timedelta
from typing import Any

import config
from analysis.opportunities.domain import (
    Ladder,
    Opportunity,
    OpportunityStatus,
    ScoreCard,
)

log = logging.getLogger(__name__)

__all__ = [
    "archive",
    "collapse_exact_duplicates",
    "expired_from_archive",
    "link_related",
    "make_lookup",
    "previous_sightings",
    "prune_expired",
    "related_key",
]


# ---------------------------------------------------------------------------
# Identity persistence
# ---------------------------------------------------------------------------
def previous_sightings(conn) -> dict[str, tuple[str, date]]:
    """``identity -> (opportunity_id, first_detected_on)`` from the archive.

    ``MIN(first_detected_on)`` rather than the newest row's value: a run that
    somehow wrote a later first-seen date must not be able to move the birthday
    forward, because the birthday is half the id.
    """
    if conn is None:
        return {}
    try:
        rows = conn.execute(
            "SELECT identity, opportunity_id, MIN(first_detected_on) "
            "FROM opportunity_detections GROUP BY identity"
        ).fetchall()
    except sqlite3.Error as exc:
        log.debug("opportunity archive unavailable: %s", exc)
        return {}
    out: dict[str, tuple[str, date]] = {}
    for identity, opp_id, first_seen in rows:
        try:
            out[str(identity)] = (str(opp_id), date.fromisoformat(str(first_seen)[:10]))
        except ValueError:
            log.warning("opportunity archive: %r has an unreadable first-seen date", identity)
    return out


def make_lookup(conn):
    """A ``identity -> (id, first_detected_on)`` callable for ``rules.build_candidates``.

    Read once and closed over, rather than one query per candidate: a screen
    with forty candidates would otherwise make forty round trips for a table
    that fits in memory.
    """
    known = previous_sightings(conn)

    def lookup(identity: str) -> tuple[str | None, date | None]:
        found = known.get(identity)
        return (found[0], found[1]) if found else (None, None)

    return lookup


# ---------------------------------------------------------------------------
# Duplicates
# ---------------------------------------------------------------------------
def collapse_exact_duplicates(opportunities: list[Opportunity]) -> list[Opportunity]:
    """One row per identity. Two in a run means a detector emitted twice.

    The survivor is the one on the higher rung, then the one with more evidence
    — never "the last one", which would make the output depend on iteration
    order. A collapse is logged at warning level because it is a detector bug,
    not an expected state.
    """
    best: dict[str, Opportunity] = {}
    for opportunity in opportunities:
        existing = best.get(opportunity.identity)
        if existing is None:
            best[opportunity.identity] = opportunity
            continue
        log.warning(
            "two detections share identity %r (%s, %s) — collapsing",
            opportunity.identity, existing.opportunity_id, opportunity.opportunity_id,
        )
        keep = max(
            (existing, opportunity),
            key=lambda item: (item.ladder.rank, len(item.evidence)),
        )
        best[opportunity.identity] = keep
    return list(best.values())


def related_key(opportunity: Opportunity) -> tuple:
    """What makes two different rules "about the same thing"."""
    parts: list[Any] = []
    for field_name in config.OPPORTUNITY_RELATED_ON:
        if field_name == "product":
            parts.append(opportunity.product)
        elif field_name == "origin":
            parts.append(opportunity.origin.country_iso if opportunity.origin else None)
        elif field_name == "destination":
            parts.append(
                opportunity.destination.country_iso if opportunity.destination else None
            )
    return tuple(parts)


def link_related(opportunities: list[Opportunity]) -> list[Opportunity]:
    """Cross-link candidates on the same product and lane from different rules.

    Not a merge. Two rules agreeing is the most useful thing this engine can
    show — a landed advantage that also shows up as a flow shift is a different
    quality of finding from either alone — and merging them into one row would
    hide the second rule's evidence behind the first rule's headline.

    A key with a ``None`` on both ends of the lane is not linked: "every
    bean signal with no origin and no destination" is not a relationship.
    """
    groups: dict[tuple, list[Opportunity]] = {}
    for opportunity in opportunities:
        key = related_key(opportunity)
        if all(part is None for part in key[1:]):
            continue
        groups.setdefault(key, []).append(opportunity)

    linked_by_id: dict[str, tuple[str, ...]] = {}
    for members in groups.values():
        if len(members) < 2:
            continue
        for member in members:
            linked_by_id[member.opportunity_id] = tuple(sorted(
                other.opportunity_id for other in members
                if other.opportunity_id != member.opportunity_id
            ))
    return [
        replace(opportunity, related_ids=linked_by_id[opportunity.opportunity_id])
        if opportunity.opportunity_id in linked_by_id else opportunity
        for opportunity in opportunities
    ]


# ---------------------------------------------------------------------------
# Expiry
# ---------------------------------------------------------------------------
def prune_expired(
    opportunities: list[Opportunity], *, today: date
) -> tuple[list[Opportunity], list[Opportunity]]:
    """``(live, expired)``. Expired rows are re-stamped, not deleted.

    Anything past its expiry plus the grace period is dropped from both lists —
    the screen is a working surface, and a permanent record of every dead
    candidate belongs in the archive, which has one.
    """
    grace = timedelta(days=config.OPPORTUNITY_EXPIRY_GRACE_DAYS)
    live: list[Opportunity] = []
    expired: list[Opportunity] = []
    for opportunity in opportunities:
        if not opportunity.is_expired(today):
            live.append(opportunity)
            continue
        if today > opportunity.expires_on + grace:
            continue
        expired.append(replace(
            opportunity,
            status=OpportunityStatus.EXPIRED,
            # An expired candidate is no longer actionable whatever it was,
            # and demoting it here rather than in the template means a page
            # that forgets to check cannot render it as live.
            ladder=Ladder.MARKET_SIGNAL if opportunity.ladder is Ladder.ACTIONABLE
            else opportunity.ladder,
            suggested_next_action=(
                "Expired — the evidence behind it is past its own layer's recency budget. "
                "It will re-detect on its own if the condition is still there."
            ),
        ))
    return live, expired


def expired_from_archive(
    conn, *, today: date, seen_identities: set[str]
) -> list[dict[str, Any]]:
    """Identities the archive knows about that did NOT re-detect today.

    Returned as plain rows rather than :class:`Opportunity` values on purpose:
    they were not re-detected, so their evidence, counterparties and economics
    were never recomputed, and building a typed opportunity out of a stale
    snapshot would be presenting yesterday's numbers as today's. The page shows
    them as a short "stopped detecting" list — which is itself information: a
    landed advantage that vanished overnight is news.
    """
    if conn is None:
        return []
    horizon = (today - timedelta(days=config.OPPORTUNITY_EXPIRY_GRACE_DAYS)).isoformat()
    try:
        rows = conn.execute(
            "SELECT identity, opportunity_id, rule_id, product, origin, destination, "
            "       MAX(run_date), MIN(first_detected_on), MAX(composite_score) "
            "FROM opportunity_detections WHERE run_date >= ? GROUP BY identity",
            (horizon,),
        ).fetchall()
    except sqlite3.Error as exc:
        log.debug("opportunity archive unavailable: %s", exc)
        return []
    out: list[dict[str, Any]] = []
    for identity, opp_id, rule_id, product, origin, destination, last_run, first_seen, score in rows:
        if str(identity) in seen_identities:
            continue
        out.append({
            "identity": str(identity),
            "opportunity_id": str(opp_id),
            "rule_id": str(rule_id),
            "rule_label": config.OPPORTUNITY_RULES.get(str(rule_id), {}).get(
                "label", str(rule_id)
            ),
            "product": str(product),
            "origin": origin,
            "destination": destination,
            "last_detected_on": str(last_run)[:10],
            "first_detected_on": str(first_seen)[:10],
            "last_composite": score,
        })
    return sorted(out, key=lambda row: row["last_detected_on"], reverse=True)


# ---------------------------------------------------------------------------
# Archiving
# ---------------------------------------------------------------------------
def archive(
    scored: list[tuple[Opportunity, ScoreCard]],
    *,
    today: date,
) -> int:
    """Persist this run's detections. Returns rows written.

    Serialises with ``audience="public"`` deliberately. The archive is
    git-committed history, so a private note reaching it would be a leak no
    template check could ever catch; ``pipeline.store.save_opportunity_detections``
    rejects the private field names as a second, independent guard.
    """
    from pipeline.store import save_opportunity_detections

    rows = [
        {
            "run_date": today.isoformat(),
            "identity": opportunity.identity,
            "opportunity_id": opportunity.opportunity_id,
            "rule_id": opportunity.rule_id,
            "ladder": opportunity.ladder.value,
            "product": opportunity.product,
            "origin": opportunity.origin.key if opportunity.origin else None,
            "destination": opportunity.destination.key if opportunity.destination else None,
            "window_start": (
                opportunity.shipment_window.start.isoformat()
                if opportunity.shipment_window else None
            ),
            "first_detected_on": opportunity.first_detected_on.isoformat(),
            "expires_on": opportunity.expires_on.isoformat(),
            "confidence": opportunity.confidence.value,
            "composite_score": card.composite,
            "blocker_codes": ",".join(
                sorted({blocker.code.value for blocker in opportunity.blockers})
            ),
            "score_json": json.dumps(card.to_dict()),
            "snapshot_json": json.dumps(
                opportunity.to_dict(today=today), default=str
            ),
            "method_version": config.OPPORTUNITY_METHOD_VERSION,
        }
        for opportunity, card in scored
    ]
    return save_opportunity_detections(rows)
