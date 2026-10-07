"""Wayfinding: the "How to read" key and the headline's "deeper" pointers.

Two small things a reader needs that the data itself cannot supply.

**The key** (``how_to_read_vocabulary``) explains the labels the site stamps
on numbers — quote-kind chips, ledger state pills, block empty states, page
tiers and the data-age words from ``LATENCY.md``. It is built *from* the code
that emits those labels (``pricing.semantics``, ``app.block_builders``,
``app.blocks``, ``app.markets``), never retyped, so a new quote kind without
an explanation fails ``tests/test_wayfinding.py`` rather than shipping as an
unexplained chip. Only the *meanings* live here; the vocabulary does not
(invariant 3: one shared vocabulary, no parallel copy).

**The pointers** (``deeper_links``) close every headline section with the
market-page block that carries the same subject in depth. The map is a
declaration, one entry per headline section, and a section with nothing deeper
says so with an empty tuple rather than being missing. Links are filtered by
the target page's tier: a brief renders only ``BRIEF_BLOCK_IDS`` and a stub
renders no blocks, so a pointer at either would be a dead anchor.
"""

from __future__ import annotations

from dataclasses import dataclass

import config
from app.block_builders import (
    LEDGER_STATE_DARK,
    LEDGER_STATE_NO_PRINT,
    LEDGER_STATE_OUT_OF_CADENCE,
    LEDGER_STATE_REPRICED,
)
from app.blocks import BLOCK_SPECS, BRIEF_BLOCK_IDS, STATE_ABSENT, STATE_EMPTY, STATE_OK
from app.markets import TIER_BRIEF, TIER_PAGE, TIER_STUB
from pricing.semantics import QUOTE_KIND_LABELS

# ---------------------------------------------------------------------------
# How to read
# ---------------------------------------------------------------------------

# Meanings for the quote-kind chips. The labels themselves come from
# pricing.semantics.QUOTE_KIND_LABELS; this dict only says what each one is.
# A kind present there and absent here fails the test, on purpose.
_QUOTE_KIND_MEANINGS: dict[str, str] = {
    "board": "An exchange close fetched after the fact and delayed. Not the exchange's "
             "official settlement — nothing on this site is a settlement.",
    "board_last_traded": "The last trade an exchange reported, not a close. It can be hours "
                         "older than the session and may not match any settlement.",
    "physical": "An assessment of the cash market by a reporting agency or a public market "
                "record (CEPEA Paranaguá, AMS Gulf bids, Indian mandi arrivals). An observation "
                "of trading, not a quote you can hit.",
    "administered": "An official price set by decree rather than discovered by trading "
                    "(Argentina's official FOB under Ley 21.453). It moves when the authority "
                    "moves it.",
    "weekly_assessment": "A once-a-week assessment. Flat between prints by construction — "
                         "a weekly leg that has not moved has not been asked yet.",
    "weekly_ask": "A processor's weekly asking price for product (AMS cash oil and meal). "
                  "An ask, not a trade.",
}

_LEDGER_STATE_LABELS: dict[str, str] = {
    LEDGER_STATE_REPRICED: "repriced",
    LEDGER_STATE_NO_PRINT: "no print since",
    LEDGER_STATE_DARK: "dark",
    LEDGER_STATE_OUT_OF_CADENCE: "out of cadence",
}

_LEDGER_STATE_MEANINGS: dict[str, str] = {
    LEDGER_STATE_REPRICED: "This venue printed on the newest session the ledger has. The only "
                           "affirmative state; the only one in green.",
    LEDGER_STATE_NO_PRINT: "This venue has not printed since the date shown. Quiet grey while the "
                           "gap is normal for that leg's cadence; amber once it is overdue. A "
                           "venue that has not printed did not 'hold steady'.",
    LEDGER_STATE_DARK: "No usable observation at all — the source is down, blocked or has no "
                       "such leg. Red because a missing venue is louder than a stale one.",
    LEDGER_STATE_OUT_OF_CADENCE: "This venue publishes on a different rhythm (e.g. weekly), so a "
                                 "daily ledger cannot grade it. Outlined, not filled: neither an "
                                 "outage nor a print, and it carries no value.",
}

_BLOCK_STATE_LABELS: dict[str, str] = {
    STATE_OK: "ok",
    STATE_EMPTY: "empty",
    STATE_ABSENT: "absent",
}

_BLOCK_STATE_MEANINGS: dict[str, str] = {
    STATE_OK: "Current data from a known source. Rendered in full.",
    STATE_EMPTY: "A source exists for this market and gave nothing usable today. Warm left "
                 "rule: something that should be here is not.",
    STATE_ABSENT: "No source exists for this market, by structure (e.g. no crush industry). "
                  "Calm: nothing is missing, and the slot keeps its number so every page "
                  "reads the same.",
}

_TIER_MEANINGS: dict[str, str] = {
    TIER_PAGE: "A daily price leg plus at least three of the four supporting blocks. "
               "All nine blocks render.",
    TIER_BRIEF: "Thinner coverage: a daily leg with fewer than three supporting blocks, or no "
                "daily leg but at least two. One screen; the ledger and the reserved News slot "
                "are dropped.",
    TIER_STUB: "No daily price leg and fewer than two supporting blocks. The page lists "
               "what is missing and why, so the market is visibly not covered rather "
               "than silently absent.",
}

# Data-age words, from LATENCY.md. Not derived from code — they describe the
# masthead and the settlement guard, which have no label registry to read.
_AGE_ENTRIES: tuple[tuple[str, str, str], ...] = (
    ("as_of", "as of <date>",
     "The session the number was observed on. Observation, not generation: a bar dated "
     "yesterday is yesterday's close, however fresh the page."),
    ("generated", "Generated <time>",
     "When this page was built. Routinely a day after the observation beside it — the "
     "settlement guard publishes the previous session's close on any build before the "
     "venue close, and that is intended (invariant 11)."),
    ("priced_from", "Board and FX priced from data <age> old",
     "The age of the oldest board or FX observation feeding the headline. Stated so "
     "'Generated' cannot read as fresher than the numbers are."),
    ("stale", "Stale",
     "A layer past its publishing budget (`LAYER_MAX_DATA_AGE_DAYS`). Its last rows still "
     "render, dated; nothing is padded forward."),
    ("partial", "Partial",
     "A layer that answered fresh and above its floor but short of its full catalog "
     "(one region or contract of many missing). It renders and counts as current data, "
     "tallied amber on the masthead apart from on-schedule; the Keys column names the "
     "shortfall."),
    ("d_minus_1", "D−1",
     "Yesterday's session. The fastest path from a CBOT close to this page is 1 h 15 m, "
     "floored by the settlement guard; nothing here is intraday."),
    ("fx_tag", "FX chip",
     "The USD move includes a currency move. A blue chip, never a colour on the number: "
     "a weaker real is not a cheaper bean."),
)


def how_to_read_vocabulary() -> dict[str, dict]:
    """The key's content, grouped. Each entry: key, label, meaning (+ optional css)."""
    return {
        "kinds": {
            "title": "What sort of price this is",
            "lede": "The small chip beside a price names what the number is. Nothing here is "
                    "a settlement.",
            "entries": [
                {"key": kind, "label": QUOTE_KIND_LABELS[kind],
                 "meaning": _QUOTE_KIND_MEANINGS[kind], "css": "kind"}
                for kind in QUOTE_KIND_LABELS
            ],
        },
        "ledger": {
            "title": "Propagation ledger states",
            "lede": "The pill carries the colour; the number does not. Silence is a state.",
            "entries": [
                {"key": state, "label": _LEDGER_STATE_LABELS[state],
                 "meaning": _LEDGER_STATE_MEANINGS[state], "css": f"pill pill-{state}"}
                for state in _LEDGER_STATE_LABELS
            ],
        },
        "blocks": {
            "title": "Empty states on a market page",
            "lede": "A missing block keeps its number and names its reason. Never a gap, "
                    "never a renumber.",
            "entries": [
                {"key": state, "label": _BLOCK_STATE_LABELS[state],
                 "meaning": _BLOCK_STATE_MEANINGS[state], "css": f"es-label state-{state}"}
                for state in _BLOCK_STATE_LABELS
            ],
        },
        "tiers": {
            "title": "Market page tiers",
            "lede": "Stamped under the masthead of every market page. The URL never changes "
                    "with the tier.",
            "entries": [
                {"key": tier, "label": tier, "meaning": _TIER_MEANINGS[tier],
                 "css": f"tier-pill {tier}"}
                for tier in (TIER_PAGE, TIER_BRIEF, TIER_STUB)
            ],
        },
        "age": {
            "title": "How old a number is",
            "lede": "Every number carries its own date. The product refreshes daily; it does "
                    "not tick.",
            "entries": [
                {"key": key, "label": label, "meaning": meaning, "css": ""}
                for key, label, meaning in _AGE_ENTRIES
            ],
        },
    }


# ---------------------------------------------------------------------------
# Deeper pointers
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Target:
    """One pointer. ``block`` targets a market-page block on the markets in
    ``scope`` (``None`` = every market); ``page`` targets a site page."""

    kind: str                       # "block" | "page"
    block: str | None = None
    scope: tuple[str, ...] | None = None
    page: str | None = None
    label: str | None = None

    def __post_init__(self) -> None:
        if self.kind == "block":
            if not self.block:
                raise ValueError("block target needs a block id")
        elif self.kind == "page":
            if not (self.page and self.label):
                raise ValueError("page target needs a page and a label")
        else:
            raise ValueError(f"unknown target kind {self.kind!r}")


def _block(block: str, *scope: str) -> Target:
    return Target(kind="block", block=block, scope=scope or None)


def _page(page: str, label: str) -> Target:
    return Target(kind="page", page=page, label=label)


# One entry per headline section id (scripts.generate_html.SECTIONS). An empty
# tuple is a decision — "nothing deeper than this" — and the test insists on
# the entry so a new section cannot be forgotten.
DEEPER: dict[str, tuple[Target, ...]] = {
    "overnight": (_block("price", "cbot"),),
    "signals": (),
    "propagation": (_block("ledger"),),
    "crush-board": (_block("crush", *config.CRUSH_BOARD),),
    "relative-value": (
        _block("basis", "brazil", "argentina"),
        _page("origins.html", "Origins — landed cost by origin"),
    ),
    "supply-demand": (_block("supply_demand"),),
    "risk": (_block("currency"), _block("weather")),
    "forward-curves": (_page("workstation.html", "Workstation — every listed month"),),
    "seasonal": (),
    "technicals": (_page("workstation.html", "Workstation — per-contract candles"),),
    "briefing": (_page("briefing.html", "The full briefing"),),
    "about": (),
}

_BLOCK_NO = {bid: f"{i:02d}" for i, (bid, _, _) in enumerate(BLOCK_SPECS, start=1)}
_BLOCK_TITLE = {bid: title for bid, title, _ in BLOCK_SPECS}


def _renders_block(tier: str, block: str) -> bool:
    if tier == TIER_PAGE:
        return True
    if tier == TIER_BRIEF:
        return block in BRIEF_BLOCK_IDS
    if tier == TIER_STUB:
        return False
    raise ValueError(f"unknown tier {tier!r}")


def deeper_links(section_id: str, market_nav: list[dict], *, root: str = "") -> list[dict]:
    """Resolve a section's pointers against the rendered nav (slug, name, href, tier).

    Returns one group per target. A block group carries ``no``, ``title`` and
    the ``markets`` that actually render that block; a block group with no
    eligible market is dropped. A page group carries ``href`` and ``label``.
    Raises ``KeyError`` for a section id the map does not know.
    """
    targets = DEEPER[section_id]
    groups: list[dict] = []
    for target in targets:
        if target.kind == "page":
            groups.append({"kind": "page", "href": f"{root}{target.page}", "label": target.label})
            continue
        assert target.block is not None
        markets = [
            {"name": m["name"], "href": f"{m['href']}#block-{target.block}"}
            for m in market_nav
            if (target.scope is None or m["slug"] in target.scope)
            and _renders_block(m["tier"], target.block)
        ]
        if not markets:
            continue
        groups.append({
            "kind": "block",
            "no": _BLOCK_NO[target.block],
            "title": _BLOCK_TITLE[target.block],
            "markets": markets,
        })
    return groups


__all__ = ["DEEPER", "Target", "deeper_links", "how_to_read_vocabulary"]
