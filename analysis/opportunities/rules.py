"""From measurement to candidate (Phase 4).

``signals.py`` says what the market did. This module answers the rest of the
trader question — *who*, *where*, *why now*, *how sure*, *what next* — and, more
importantly, decides which rung of the ladder the result has earned.

The rung is not a label applied at the end. It falls out of three tests, in
this order, and each one has a failure mode worth naming:

1. **Is there a lane and a counterparty?** Without both it is a market signal,
   not a lead. A screen that promotes "China's stocks are tight" to an
   opportunity has told the reader nothing they can pick up a phone about.
2. **Can the spread plausibly be closed?** This is requirement 2 and it is the
   whole point of :func:`assess_blockers`. Policy, freight, quality, timing and
   liquidity each get a named blocker, a *hard* one caps the candidate at
   ``LEAD``, and :class:`~analysis.opportunities.domain.Opportunity` refuses to
   be constructed the other way. India is the standing example: its mandi bean
   prints ~+66% over CBOT, it has reached ~2x, and no trade closes it because GM
   imports are banned behind a tariff wall.
3. **Is the evidence fresh and comparable?** Every piece of evidence is judged
   against its *own* layer's recency budget, the same number ``main.py`` grades
   on. A stale input is a hard blocker, not a footnote.

What this module never does is invent. No counterparty is added that the
players knowledge base does not already carry; no contact, volume, freight rate
or shipment window is filled in to make a row look complete. An unknown field
stays ``None`` and, where the unknown is load-bearing, it becomes a blocker with
a remedy naming who could resolve it.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import config
from analysis.opportunities.domain import (
    Blocker,
    BlockerCode,
    Blockers,
    Confidence,
    Counterparty,
    Grade,
    Ladder,
    Opportunity,
    OpportunityStatus,
    PartyRole,
    identity_key,
    opportunity_id,
    worst_confidence,
)
from analysis.opportunities.signals import Detection
from analysis.origins import players as players_mod

log = logging.getLogger(__name__)

__all__ = [
    "assess_blockers",
    "build_candidate",
    "build_candidates",
    "counterparties_for",
    "ladder_for",
    "next_action_for",
]


# ---------------------------------------------------------------------------
# Counterparties
# ---------------------------------------------------------------------------
_ROLE_BY_SIDE = {
    "sell": PartyRole.SELLER,
    "buy": PartyRole.BUYER,
    "terminal": PartyRole.FACILITY,
}


def _convert(entry: players_mod.Counterparty) -> Counterparty:
    """Players-base entry → opportunity-side candidate.

    A conversion rather than a re-derivation: every field here already exists on
    the Phase 2 type, evidence dates included, and reading the YAML a second
    time with slightly different role sets is how two surfaces come to name
    different companies for the same lane.
    """
    return Counterparty(
        name=entry.name,
        country=entry.country,
        role=_ROLE_BY_SIDE[entry.side],
        roles=entry.roles,
        products=entry.products,
        tier=entry.tier,
        lane_evidenced=entry.lane_evidenced,
        lane_note=entry.lane_note,
        confidence=entry.confidence,
        last_verified=entry.last_verified,
        citation=entry.citation,
        footprint=entry.footprint,
        scope=entry.scope,
    )


def counterparties_for(
    detection: Detection,
    *,
    entries: list[dict] | None = None,
    limit: int | None = None,
) -> tuple[tuple[Counterparty, ...], tuple[Counterparty, ...], tuple[Counterparty, ...]]:
    """``(sellers, buyers, facilities)`` for this detection's lane.

    A detection with only one end of the lane is normal and is handled by
    passing a key that matches no country: a tight importer has a destination
    and no origin, and the honest answer is "these are the buyers, and the
    internationally-scoped houses who could sell to them" rather than a fake
    origin. The globally-scoped trading houses are included on the sell side of
    every lane because that is what they are — omitting ADM, Bunge, Cargill,
    Dreyfus and Viterra because their registry country is ``GLOBAL`` would omit
    most of the market.
    """
    limit = limit if limit is not None else config.OPPORTUNITY_COUNTERPARTY_LIMIT
    origin_iso = detection.origin.country_iso if detection.origin else "-"
    destination_iso = detection.destination.country_iso if detection.destination else "-"
    lane = players_mod.counterparties_for_lane(
        origin_iso, destination_iso, entries=entries, limit=limit
    )
    return (
        tuple(_convert(item) for item in lane.sellers),
        tuple(_convert(item) for item in lane.buyers),
        tuple(_convert(item) for item in lane.terminals),
    )


# ---------------------------------------------------------------------------
# Blockers — requirement 2
# ---------------------------------------------------------------------------
def _market_for_iso(iso: str | None) -> tuple[str, dict] | tuple[None, None]:
    if not iso:
        return None, None
    for slug, market in config.MARKETS.items():
        if market.get("players_country") == iso:
            return slug, market
    return None, None


def _policy_blockers(detection: Detection) -> list[Blocker]:
    """Policy barriers declared on the market registry, both ends of the lane.

    Read from ``config.MARKETS[...]['basis']['arbitrage']`` rather than restated
    here. That descriptor already carries ``policy_blocked`` plus a mandatory
    ``caveat`` (M19 #222), so the sentence a reader sees on the market page and
    the sentence they see on this screen are the same sentence.
    """
    out: list[Blocker] = []
    for port in (detection.origin, detection.destination):
        if port is None:
            continue
        slug, market = _market_for_iso(port.country_iso)
        if market is None:
            continue
        basis = market.get("basis") or {}
        if basis.get("arbitrage") != "policy_blocked":
            continue
        out.append(Blocker(
            code=BlockerCode.POLICY_BARRIER,
            message=(
                f"{market.get('name', slug)}: {basis.get('caveat', 'trade is policy-blocked on this lane')}"
            ),
            remedy=(
                "Nothing in the price clears this. It closes only if the policy changes, so "
                "the spread is a measurement, never a trade."
            ),
            detail=f"config.MARKETS['{slug}']['basis']['arbitrage'] = policy_blocked",
        ))
    return out


def _freshness_blockers(detection: Detection, *, today: date) -> list[Blocker]:
    stale = [
        item for item in detection.signal.evidence
        if item.age_days(today) > item.max_age_days
    ]
    if not stale:
        return []
    worst = max(stale, key=lambda item: item.age_days(today))
    return [Blocker(
        code=BlockerCode.EVIDENCE_STALE,
        message=(
            f"{worst.label} was observed {worst.age_days(today)} days ago, past the "
            f"{worst.max_age_days}-day budget Layer '{worst.source.layer}' is graded on."
        ),
        remedy=(
            f"Wait for the next {worst.source.layer} ingest, or check the Layer Freshness "
            "table on the headline — a source that stays past its budget is an outage, not "
            "a quiet market."
        ),
        detail=f"{len(stale)} of {len(detection.signal.evidence)} evidence items are stale",
    )]


def _ingest_blockers(conn, detection: Detection) -> list[Blocker]:
    """Our own outage, named as ours.

    The distinction M1/#212 made on the market pages, applied here: age alone
    cannot tell a rate limit from a market where nobody published, and both
    surface as "no recent number". ``data_freshness`` knows which it was, so a
    candidate weakened by our own ingest failure says so in those words rather
    than implying something about the market.
    """
    if conn is None:
        return []
    layers = sorted({item.source.layer for item in detection.signal.evidence})
    out: list[Blocker] = []
    for layer in layers:
        try:
            row = conn.execute(
                "SELECT status, last_success FROM data_freshness WHERE layer_name = ?",
                (layer,),
            ).fetchone()
        except Exception:  # noqa: BLE001 — a missing table is data, not a crash
            continue
        if not row or row[0] != "failed":
            continue
        out.append(Blocker(
            code=BlockerCode.INGEST_OUTAGE,
            message=(
                f"Our '{layer}' ingest failed on the last run; last good run "
                f"{str(row[1])[:10] or 'unknown'}. This is our outage, not the market's."
            ),
            remedy="Re-run the pipeline for that layer before acting on this.",
        ))
    return out


def _missing_input_blockers(detection: Detection) -> list[Blocker]:
    """Cost inputs Phase 2 could not supply, restated as trade blockers."""
    out: list[Blocker] = []
    if "ocean_freight" in detection.missing:
        out.append(Blocker(
            code=BlockerCode.FREIGHT_UNKNOWN,
            message=(
                "No live ocean-freight assumption covers this route and window, so at least "
                "one origin on this board could not be costed to the berth."
            ),
            remedy=(
                "Enter one with `python scripts/enter_assumption.py` — an entered number "
                "with an owner and an expiry beats a fabricated one with neither."
            ),
            detail="analysis/origins/assumptions.py",
        ))
    if "quality_adjustment" in detection.missing:
        out.append(Blocker(
            code=BlockerCode.QUALITY_UNPRICED,
            message=(
                "No quality differential is entered for this grade pair. US No. 2 Yellow and "
                "Brazilian contract standard are not the same specification, and a crusher "
                "pays for protein."
            ),
            remedy="Enter a quality_adjustment assumption for the route, or price it in the negotiation.",
        ))
    for name in detection.missing:
        if name in ("ocean_freight", "quality_adjustment"):
            continue
        out.append(Blocker(
            code=BlockerCode.FREIGHT_UNKNOWN if "freight" in name else BlockerCode.QUALITY_UNPRICED,
            message=f"The '{name.replace('_', ' ')}' input is absent or lapsed for this route.",
            remedy="Enter it as a dated assumption with an owner and an expiry.",
        ))
    return out


def _window_blockers(detection: Detection) -> list[Blocker]:
    """A cargo needs a shipment period. A margin does not.

    Applied only where both ends of a lane are named — that is what makes the
    detection about a *cargo*. A crush margin or a currency move has no shipment
    window by nature, and demanding one there would block every candidate from a
    rule that was never describing a parcel; the absence is recorded in
    ``missing_information`` instead, where it belongs.
    """
    if detection.window is not None:
        return []
    if detection.origin is None or detection.destination is None:
        return []
    return [Blocker(
        code=BlockerCode.WINDOW_INCOMPATIBLE,
        message=(
            "This lane signal carries no shipment window. A price without a period is not "
            "a price a cargo can be worked against."
        ),
        remedy=(
            "Pin a window before quoting it — two origins quoted for different months "
            "differ by carry as much as by competitiveness."
        ),
    )]


def _liquidity_blockers(detection: Detection) -> list[Blocker]:
    """Whether the numbers behind this are things anybody actually traded.

    An administered minimum export value is precisely known, legally binding,
    and not a price anyone paid; a weekly assessment is somebody's opinion of
    where trade happened. Both are useful and neither is executable, and a
    screen that does not say so is a screen that will be quoted back as a firm
    offer.
    """
    kinds = {item.quote_kind for item in detection.signal.evidence if item.quote_kind}
    soft = kinds & {"administered", "weekly_assessment", "board_last_traded"}
    if not soft:
        return []
    return [Blocker(
        code=BlockerCode.LIQUIDITY_UNPROVEN,
        message=(
            "This rests on " + ", ".join(sorted(soft)).replace("_", " ")
            + " numbers. None of them is a print somebody traded on."
        ),
        remedy="Confirm the level with the counterparty before treating it as workable.",
    )]


def assess_blockers(detection: Detection, *, conn=None, today: date, has_counterparty: bool) -> Blockers:
    """Every reason this spread might not close, deduplicated by code+message.

    Order is stable — policy first, then our own gaps, then the market's — so a
    candidate's blocker list reads the same way twice and a reader learns where
    to look.
    """
    found: list[Blocker] = []
    found.extend(_policy_blockers(detection))
    found.extend(_freshness_blockers(detection, today=today))
    found.extend(_ingest_blockers(conn, detection))
    found.extend(_missing_input_blockers(detection))
    found.extend(_window_blockers(detection))
    found.extend(_liquidity_blockers(detection))
    found.extend(detection.blockers)
    if not has_counterparty:
        found.append(Blocker(
            code=BlockerCode.NO_COUNTERPARTY,
            message=(
                "The players knowledge base carries no candidate on the side this signal "
                "calls for, in either country."
            ),
            remedy=(
                "Research one into data/reference/players/ — this engine will not invent a "
                "counterparty to fill the field."
            ),
        ))
    seen: set[tuple[str, str]] = set()
    ordered: list[Blocker] = []
    for blocker in found:
        key = (blocker.code.value, blocker.message)
        if key in seen:
            continue
        seen.add(key)
        ordered.append(blocker)
    return tuple(ordered)


# ---------------------------------------------------------------------------
# Ladder and next action
# ---------------------------------------------------------------------------
def ladder_for(detection: Detection, blockers: Blockers, *, has_counterparty: bool) -> Ladder:
    """Which rung this has earned. The three tests, in order, and no others.

    ``ACTIONABLE`` requires all of: a lane with both ends named, at least one
    counterparty, priced economics, and no hard blocker. Anything less is a
    ``LEAD`` if there is somebody to call and a ``MARKET_SIGNAL`` if there is
    not — which is most of what a pipeline produces, and saying so is the point.
    """
    if not has_counterparty:
        return Ladder.MARKET_SIGNAL
    if detection.origin is None or detection.destination is None:
        return Ladder.LEAD
    if any(blocker.is_hard for blocker in blockers):
        return Ladder.LEAD
    if detection.economics is None:
        return Ladder.LEAD
    return Ladder.ACTIONABLE


def next_action_for(
    detection: Detection,
    ladder: Ladder,
    blockers: Blockers,
    sellers: tuple[Counterparty, ...],
    buyers: tuple[Counterparty, ...],
) -> str:
    """One sentence: the next thing a person could actually do.

    Derived from the state, never from a template with a blank in it. A blocked
    candidate's next action is to clear the blocker — naming the *first* one,
    because a list of six things to do is a list nobody starts.
    """
    hard = [blocker for blocker in blockers if blocker.is_hard]
    if hard:
        return f"Clear the blocker first: {hard[0].remedy}"
    if ladder is Ladder.MARKET_SIGNAL:
        return (
            "Research a counterparty for this lane into data/reference/players/ before "
            "this can become a lead."
        )
    wants_buyer = detection.role_wanted is PartyRole.BUYER
    shortlist = buyers if wants_buyer else sellers
    if not shortlist:
        shortlist = sellers or buyers
    named = ", ".join(party.name for party in shortlist[:2]) or "the shortlist below"
    side = "buyer" if wants_buyer else "seller"
    window = (
        f" for {detection.window.describe()}" if detection.window else ""
    )
    if ladder is Ladder.ACTIONABLE:
        return (
            f"Sound out the {side} side — {named} — on {detection.product}{window}, and "
            "confirm freight and quality against your own numbers before quoting."
        )
    soft = [blocker for blocker in blockers if not blocker.is_hard]
    if soft:
        # The blocker's own first sentence, verbatim. Splicing it into a longer
        # one lower-cased mid-clause produced sentences that read as though the
        # software did not know what it was saying.
        caveat = soft[0].message.split(". ")[0].rstrip(".")
        return f"Worth a call to {named}. Note first: {caveat}."
    return f"Worth a call to {named} to see whether the flow is repeatable."


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------
def _confidence(detection: Detection, parties: tuple[Counterparty, ...]) -> Confidence:
    """Worst link, across the evidence and the counterparty research.

    Deliberately pessimistic in both directions: an executable board price with
    an ``inferred`` counterparty is not an executable opportunity, and neither is
    a well-researched counterparty attached to an administered reference value.
    """
    evidence = detection.signal.worst_evidence_confidence()
    if any(party.confidence != "observed" for party in parties):
        return worst_confidence(evidence, Confidence.PROVISIONAL)
    return evidence


def _missing_information(
    detection: Detection,
    blockers: Blockers,
    sellers: tuple[Counterparty, ...],
    buyers: tuple[Counterparty, ...],
) -> tuple[str, ...]:
    out: list[str] = []
    if detection.volume is None:
        out.append("cargo size — no line-up, manifest or contract is ingested by this project")
    if detection.window is None:
        out.append("shipment window — this source publishes none")
    if detection.economics is None:
        out.append("a per-tonne edge — this signal is a dislocation, not a priced spread")
    if not sellers:
        out.append("a seller candidate for this origin")
    if not buyers:
        out.append("a buyer candidate for this destination")
    if detection.incoterm is None:
        out.append("delivery terms — nothing in the evidence names one")
    for blocker in blockers:
        if blocker.code is BlockerCode.FREIGHT_UNKNOWN:
            out.append("ocean freight for this route and window")
            break
    return tuple(dict.fromkeys(out))


def _expiry(detection: Detection, blockers: Blockers) -> date:
    """When this stops being worth looking at.

    The *shortest* horizon among the evidence, never the longest: an
    opportunity resting on a daily price and a monthly balance sheet goes stale
    when the price does, because that is the leg a counterparty will check.
    """
    horizons = [detection.signal.expires_on]
    for item in detection.signal.evidence:
        horizons.append(item.observed_on + timedelta(days=item.max_age_days))
    return min(horizons)


def build_candidate(
    detection: Detection,
    *,
    today: date,
    conn=None,
    entries: list[dict] | None = None,
    first_detected_on: date | None = None,
    known_id: str | None = None,
) -> Opportunity:
    """One detection, fully assembled. Never raises on ordinary emptiness.

    ``first_detected_on`` and ``known_id`` come from the registry when this
    identity has been seen before. A fresh detection of a known opportunity
    keeps its original id and its original first-detected date — otherwise the
    workflow link, the age and the expiry clock all reset every morning.
    """
    sellers, buyers, facilities = counterparties_for(detection, entries=entries)
    wants_buyer = detection.role_wanted is PartyRole.BUYER
    relevant = buyers if wants_buyer else sellers
    has_counterparty = bool(relevant)

    blockers = assess_blockers(
        detection, conn=conn, today=today, has_counterparty=has_counterparty
    )
    ladder = ladder_for(detection, blockers, has_counterparty=has_counterparty)
    identity = identity_key(
        rule_id=detection.rule_id,
        product=detection.product,
        origin_key=detection.origin.key if detection.origin else None,
        destination_key=detection.destination.key if detection.destination else None,
        window_start=detection.window.start if detection.window else None,
    )
    first_seen = first_detected_on or today
    economics = detection.economics
    if economics is not None:
        economics = economics.with_volume(detection.volume)

    return Opportunity(
        opportunity_id=known_id or opportunity_id(identity, first_detected=first_seen),
        identity=identity,
        rule_id=detection.rule_id,
        rule_label=config.OPPORTUNITY_RULES.get(detection.rule_id, {}).get(
            "label", detection.rule_id
        ),
        ladder=ladder,
        status=OpportunityStatus.DETECTED,
        product=detection.product,
        grade=detection.grade or Grade(product="soybeans"),
        origin=detection.origin,
        destination=detection.destination,
        incoterm=detection.incoterm,
        shipment_window=detection.window,
        why_now=detection.why_now or detection.signal.headline,
        signals=(detection.signal,),
        evidence=detection.signal.evidence,
        confidence=_confidence(detection, sellers + buyers),
        first_detected_on=first_seen,
        detected_on=today,
        expires_on=_expiry(detection, blockers),
        sellers=sellers,
        buyers=buyers,
        facilities=facilities,
        volume=detection.volume,
        economics=economics,
        dislocation=detection.dislocation,
        blockers=blockers,
        missing_information=_missing_information(detection, blockers, sellers, buyers),
        suggested_next_action=next_action_for(detection, ladder, blockers, sellers, buyers),
    )


def build_candidates(
    detections: list[Detection] | tuple[Detection, ...],
    *,
    today: date,
    conn=None,
    entries: list[dict] | None = None,
    lookup=None,
) -> list[Opportunity]:
    """Assemble every detection, isolating failures per candidate.

    ``lookup`` is ``identity -> (opportunity_id, first_detected_on)`` from the
    registry. Passed in rather than read here so this module stays free of SQL,
    and so a test can pin identity behaviour without a database.
    """
    entries = entries if entries is not None else players_mod.load_entries()
    out: list[Opportunity] = []
    for detection in detections:
        try:
            known_id, first_seen = (None, None)
            if lookup is not None:
                identity = identity_key(
                    rule_id=detection.rule_id,
                    product=detection.product,
                    origin_key=detection.origin.key if detection.origin else None,
                    destination_key=detection.destination.key if detection.destination else None,
                    window_start=detection.window.start if detection.window else None,
                )
                known_id, first_seen = lookup(identity)
            out.append(build_candidate(
                detection,
                today=today,
                conn=conn,
                entries=entries,
                first_detected_on=first_seen,
                known_id=known_id,
            ))
        except Exception:  # noqa: BLE001 — one bad candidate must not empty the screen
            log.warning(
                "could not assemble a candidate from %s", detection.signal.signal_id, exc_info=True
            )
    return out
