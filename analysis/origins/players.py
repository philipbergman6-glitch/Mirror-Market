"""Who could actually do the trade (Phase 2).

A landed-cost ranking says Brazil is 6 dollars cheaper. The next question is
always the same one, and it is not analytical: *who sells it, who buys it, and
which berth does it load over*. This module answers that from the players
knowledge base already in the repo, scoped to the origins and destination of
one comparison.

Three things it is careful about:

**It never invents a counterparty for a lane.** An entry appears against an
origin because its own ``country`` and ``roles`` say so, and against a
destination the same way. Where an entry's own prose names destinations
(``destinations_structured``), matching ones are marked ``lane_evidenced`` —
which is a stronger claim than "is an exporter in the right country" and is
labelled differently on the page. Nothing is inferred from the price data.

**Evidence and its age travel with the name.** Every entry carries
``confidence`` (observed vs inferred), ``as_of``, and citations with their own
``accessed`` dates. A counterparty list with no verification date is a list
somebody will act on in two years. The newest citation date is surfaced as
``last_verified`` and the age is computed against the run date.

**Terminals are read from prose, not modelled.** The knowledge base records
footprints as sentences ("Destrehan, LA export terminal, 6.4M bu storage")
because that is what free sources support. This module surfaces the sentence
attached to the entry rather than parsing a berth list out of it — a structured
facility table built by regex would be exactly the fabricated precision the
rest of Phase 2 refuses.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml

log = logging.getLogger(__name__)

PLAYERS_DIR = Path(__file__).resolve().parents[2] / "data" / "reference" / "players"

# Roles that make an entry a candidate on the sell side of a cargo, and on the
# buy side. Terminal operators appear on both: the berth is where the two meet.
ORIGINATOR_ROLES = frozenset({"originator", "exporter", "crusher"})
BUYER_ROLES = frozenset({"importer", "crusher", "refiner"})
TERMINAL_ROLES = frozenset({"terminal-operator"})

# How long a verification stays useful before the page says so. A year: the
# players base records ownership, JV stakes and terminal control, which move on
# roughly that cadence (Bunge/Viterra, Zen-Noh/Bunge elevators).
VERIFICATION_STALE_DAYS = 365


@dataclass(frozen=True)
class Counterparty:
    """One candidate, with the evidence for it and the age of that evidence."""

    name: str
    country: str
    side: str                     # sell | buy | terminal
    roles: tuple[str, ...]
    products: tuple[str, ...]
    ownership: str | None
    footprint: str | None
    confidence: str               # observed | inferred, from the entry itself
    as_of: date | None
    last_verified: date | None
    tier: int
    lane_evidenced: bool
    lane_note: str | None
    citation: str | None
    # `local` = the entry's own country is this lane's origin (or destination);
    # `global` = an internationally-scoped house that originates everywhere.
    # Kept distinct because the ABCD+ names otherwise fill every origin's list
    # with the identical five companies, and a list that is the same for the US,
    # Brazil and Argentina tells a reader nothing about any of them.
    scope: str = "local"

    def verification_age_days(self, today: date) -> int | None:
        return None if self.last_verified is None else (today - self.last_verified).days

    def is_stale(self, today: date) -> bool:
        age = self.verification_age_days(today)
        return age is None or age > VERIFICATION_STALE_DAYS

    def to_dict(self, today: date | None = None) -> dict[str, Any]:
        return {
            "name": self.name,
            "country": self.country,
            "side": self.side,
            "roles": list(self.roles),
            "products": list(self.products),
            "ownership": self.ownership,
            "footprint": self.footprint,
            "confidence": self.confidence,
            "as_of": self.as_of.isoformat() if self.as_of else None,
            "last_verified": self.last_verified.isoformat() if self.last_verified else None,
            "verification_age_days": self.verification_age_days(today) if today else None,
            "stale": self.is_stale(today) if today else None,
            "tier": self.tier,
            "lane_evidenced": self.lane_evidenced,
            "lane_note": self.lane_note,
            "citation": self.citation,
            "scope": self.scope,
        }


def _as_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def load_entries(players_dir: Path | None = None) -> list[dict]:
    """Every knowledge-base entry. An unreadable base is empty, not fatal.

    Deliberately softer than the assumption loader: the players base is
    context, not arithmetic. A malformed YAML there should cost a reader a
    counterparty list, not a whole page of prices — and
    ``scripts/validate_players.py`` already fails CI on exactly that.
    """
    root = players_dir or PLAYERS_DIR
    if not root.is_dir():
        return []
    entries: list[dict] = []
    for path in sorted(root.glob("*.yml")):
        try:
            document = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError:
            log.warning("players: %s is unreadable — skipped", path.name, exc_info=True)
            continue
        if isinstance(document, list):
            entries.extend(item for item in document if isinstance(item, dict))
    return entries


def _last_verified(entry: dict) -> date | None:
    dates: list[date] = [
        parsed
        for citation in (entry.get("citations") or [])
        if isinstance(citation, dict)
        if (parsed := _as_date(citation.get("accessed"))) is not None
    ]
    own = _as_date(entry.get("as_of"))
    if own is not None:
        dates.append(own)
    return max(dates) if dates else None


def _first_citation(entry: dict) -> str | None:
    for citation in entry.get("citations") or []:
        if isinstance(citation, dict) and citation.get("url"):
            return str(citation["url"])
    return None


def _lane(entry: dict, destination_iso: str | None) -> tuple[bool, str | None]:
    """Whether the entry's own prose evidences a flow to this destination."""
    if not destination_iso:
        return False, None
    for dest in entry.get("destinations_structured") or []:
        if not isinstance(dest, dict):
            continue
        if str(dest.get("code", "")).upper() == destination_iso.upper():
            note = dest.get("note") or f"{destination_iso} named in this entry's own destinations"
            return True, str(note)
    return False, None


def _counterparty(
    entry: dict, *, side: str, destination_iso: str | None, scope: str = "local"
) -> Counterparty:
    lane_evidenced, lane_note = _lane(entry, destination_iso)
    return Counterparty(
        scope=scope,
        name=str(entry.get("name", "")).strip(),
        country=str(entry.get("country", "")),
        side=side,
        roles=tuple(entry.get("roles") or ()),
        products=tuple(entry.get("products") or ()),
        ownership=entry.get("ownership"),
        footprint=entry.get("footprint"),
        confidence=str(entry.get("confidence", "inferred")),
        as_of=_as_date(entry.get("as_of")),
        last_verified=_last_verified(entry),
        tier=1 if entry.get("tier") == 1 else 2,
        lane_evidenced=lane_evidenced,
        lane_note=lane_note,
        citation=_first_citation(entry),
    )


def _rank(counterparty: Counterparty) -> tuple:
    """Lane evidence first, then tier, then observed over inferred, then name.

    An entry whose own research names this destination is a better answer than
    a larger company that merely operates in the right country, and the order
    says so rather than leaving the reader to notice the badge.

    Scope is deliberately *not* in this key — the two scopes are quota'd
    separately in :func:`counterparties_for_lane` instead. Sorting them into one
    list gave the US, Brazilian and Argentine lanes the identical five ABCD
    names, which is true and useless: a reader comparing three origins learns
    nothing from three copies of Cargill.
    """
    return (
        not counterparty.lane_evidenced,
        counterparty.tier,
        counterparty.confidence != "observed",
        counterparty.name.lower(),
    )


@dataclass(frozen=True)
class LaneCounterparties:
    """The counterparty picture for one origin→destination lane."""

    origin_iso: str
    destination_iso: str
    sellers: tuple[Counterparty, ...]
    buyers: tuple[Counterparty, ...]
    terminals: tuple[Counterparty, ...]

    @property
    def lane_evidenced_count(self) -> int:
        return sum(1 for seller in self.sellers if seller.lane_evidenced)

    def to_dict(self, today: date | None = None) -> dict[str, Any]:
        return {
            "origin_iso": self.origin_iso,
            "destination_iso": self.destination_iso,
            "lane_evidenced_count": self.lane_evidenced_count,
            "sellers": [seller.to_dict(today) for seller in self.sellers],
            "buyers": [buyer.to_dict(today) for buyer in self.buyers],
            "terminals": [terminal.to_dict(today) for terminal in self.terminals],
        }


def counterparties_for_lane(
    origin_iso: str,
    destination_iso: str,
    *,
    entries: list[dict] | None = None,
    limit: int = 6,
) -> LaneCounterparties:
    """Candidate sellers at the origin, buyers at the destination, berths at both.

    ``GLOBAL``-scoped trading houses are included on the sell side of every
    origin, which is what they are: ADM, Bunge, Cargill, Dreyfus and Viterra
    originate out of all three of these countries, and an origin list that
    omitted them because their ``country`` is ``GLOBAL`` would omit most of the
    market.
    """
    entries = entries if entries is not None else load_entries()
    origin = origin_iso.upper()
    destination = destination_iso.upper()

    local_sellers: list[Counterparty] = []
    global_sellers: list[Counterparty] = []
    buyers: list[Counterparty] = []
    terminals: list[Counterparty] = []

    for entry in entries:
        country = str(entry.get("country", "")).upper()
        roles = set(entry.get("roles") or ())
        products = set(entry.get("products") or ())
        if "beans" not in products and products:
            # Origin comparison is a bean cargo. A meal-only refiner is a real
            # player and simply not a counterparty for this trade.
            continue
        if roles & ORIGINATOR_ROLES:
            if country == origin:
                local_sellers.append(
                    _counterparty(entry, side="sell", destination_iso=destination)
                )
            elif country == "GLOBAL":
                global_sellers.append(
                    _counterparty(entry, side="sell", destination_iso=destination, scope="global")
                )
        if country == destination and roles & BUYER_ROLES:
            buyers.append(_counterparty(entry, side="buy", destination_iso=destination))
        if country in (origin, destination) and roles & TERMINAL_ROLES:
            terminals.append(
                _counterparty(
                    entry,
                    side="terminal",
                    destination_iso=destination,
                    scope="local" if country == origin else "destination",
                )
            )

    # Origin-specific names get most of the room and come first; the
    # internationally-scoped houses get a fixed minority share so they are
    # present without swamping the list. Without the split every origin's
    # sellers were the same five ABCD entries.
    global_share = max(2, limit // 3)
    local_take = limit - min(global_share, len(global_sellers))
    sellers = (
        sorted(local_sellers, key=_rank)[:local_take]
        + sorted(global_sellers, key=_rank)[:global_share]
    )

    return LaneCounterparties(
        origin_iso=origin,
        destination_iso=destination,
        sellers=tuple(sellers),
        buyers=tuple(sorted(buyers, key=_rank)[:limit]),
        terminals=tuple(sorted(terminals, key=_rank)[:limit]),
    )


def counterparties_for_ranking(ranking, *, entries: list[dict] | None = None, limit: int = 6) -> dict:
    """One lane block per origin in a ranking, cheapest first.

    Ordered by the ranking rather than alphabetically: the counterparty list a
    reader wants first is the one for the origin the page just told them to buy.
    """
    entries = entries if entries is not None else load_entries()
    destination_iso = ranking.destination.country_iso
    ordered = [*ranking.rankable, *ranking.excluded]
    lanes = {}
    for row in ordered:
        origin_iso = row.quote.origin.country_iso
        if origin_iso in lanes:
            continue
        lanes[origin_iso] = counterparties_for_lane(
            origin_iso, destination_iso, entries=entries, limit=limit
        )
    return lanes


__all__ = [
    "BUYER_ROLES",
    "ORIGINATOR_ROLES",
    "PLAYERS_DIR",
    "TERMINAL_ROLES",
    "VERIFICATION_STALE_DAYS",
    "Counterparty",
    "LaneCounterparties",
    "counterparties_for_lane",
    "counterparties_for_ranking",
    "load_entries",
]
