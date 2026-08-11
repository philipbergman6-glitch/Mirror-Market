"""The nine-block envelope every market block returns (M1 #143, M8 #150).

M1 fixed the block set and its order; M8 fixed the return type. This module is
the type, plus the skeleton builder M17 ships and M18 #214 replaces block by
block.

Why the envelope is a type rather than a convention: M1 constraint 2 requires
every empty state to name its reason and to distinguish "no source exists"
(``absent``) from "there is a source and it gave us nothing" (``empty``).
Putting that in the return type means the contract is enforced at construction
— nine builders cannot each remember it — and ``Block`` raises rather than
rendering a blank.

The nine ids and their order are identical on every full page. A missing block
is a labelled empty state, never a gap and never a renumber: section 03 is
crush on every page or it teaches nothing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.markets import Market

STATE_OK = "ok"          # we have the data and it is current
STATE_EMPTY = "empty"    # a source exists, but produced nothing usable today
STATE_ABSENT = "absent"  # no such source exists for this market, by structure
STATES = (STATE_OK, STATE_EMPTY, STATE_ABSENT)

# Reason used when a block builder raises. M8's failure-isolation table: a
# block that blows up renders as an empty state with this reason — the same
# *shape* as a missing source, a deliberately different *reason*.
GENERATION_ERROR = "generation error"

# id, display title, and the right-aligned "why you're looking at this" note.
# Order is the contract. Positioning is deliberately absent — it exists for
# CBOT and nowhere else, so it would be a permanent unfillable blank on seven
# of eight pages; it lives on the CBOT page and the headline instead.
BLOCK_SPECS = (
    ("price", "Local price", "the decision number, USD/MT over home currency"),
    ("ledger", "Propagation ledger", "who has repriced, and who has not printed"),
    ("crush", "Crush margin", "processor economics — board, physical or administered"),
    ("basis", "Basis vs the board", "local minus reference, USD/MT"),
    ("currency", "Currency", "the rate every USD figure here is struck at"),
    ("weather", "Weather", "this market's growing regions"),
    ("supply_demand", "Supply & demand", "annual balance sheet — does not move daily"),
    ("news", "News", "reserved — no text layer is ingested yet"),
    ("players", "Players", "counterparties active in this market"),
)

BLOCK_IDS = tuple(spec[0] for spec in BLOCK_SPECS)

# The brief is one screen, not a page with more hatching (M1 constraint 5): it
# drops the ledger (daily-only, and a brief may have no daily leg) and the
# reserved News slot, which holds a layout that a brief does not have.
BRIEF_BLOCK_IDS = ("price", "crush", "basis", "currency", "weather", "supply_demand", "players")


@dataclass(frozen=True)
class Block:
    """One block's result. Non-``ok`` states must carry a reason."""

    id: str
    no: str
    title: str
    why: str
    state: str
    reason: str = ""
    data: Any = None
    kind: str | None = None   # board / physical / administered / weekly_assessment
    extra: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.state not in STATES:
            raise ValueError(f"block {self.id!r} has state {self.state!r}, not one of {STATES}")
        if self.state != STATE_OK and not self.reason.strip():
            raise ValueError(
                f"block {self.id!r} is {self.state!r} with no reason — every empty state "
                "must name its reason (M1 #143 constraint 2)"
            )

    @property
    def is_ok(self) -> bool:
        return self.state == STATE_OK


def _spec(block_id: str) -> tuple[int, str, str, str]:
    for index, (bid, title, why) in enumerate(BLOCK_SPECS, start=1):
        if bid == block_id:
            return index, bid, title, why
    raise KeyError(f"unknown block id {block_id!r}")


def make_block(block_id: str, *, state: str, reason: str = "", **kwargs) -> Block:
    """Build a block from its id, filling number/title/why from BLOCK_SPECS."""
    index, bid, title, why = _spec(block_id)
    return Block(id=bid, no=f"{index:02d}", title=title, why=why, state=state, reason=reason, **kwargs)


# Why each block is unavailable in the skeleton. M18 #214 replaces these one by
# one; until it does, a page that says "not built yet" is honest and a page
# that renders a blank is not.
_NOT_BUILT = "block builder not built yet — M18 #214"

# Blocks whose source is site-wide rather than per-market, so the registry has
# no per-market absent reason to give.
_SITE_WIDE_REASONS = {
    "ledger": "the propagation ledger needs a daily leg — this market has none",
    "currency": "{market} quotes in {ccy}, the site numeraire — no conversion applies",
    "news": "reserved — no news layer is ingested (out of scope for map #142)",
    "supply_demand": "no PSD country is mapped to this market",
    "players": "no players country is mapped to this market",
}


def skeleton_blocks(market: Market, tier_result, *, block_ids=BLOCK_IDS) -> list[Block]:
    """The block set M17 ships: structure real, contents pending M18 #214.

    A block whose *source* is genuinely missing renders ``absent`` with the
    registry's own reason — that verdict is already correct and does not wait
    on M18. A block that has a source renders ``empty`` pointing at M18, which
    is the truth today: the data exists, the builder does not.
    """
    blocks: list[Block] = []
    for block_id in block_ids:
        absent_reason = _absent_reason(market, block_id, tier_result)
        if absent_reason:
            blocks.append(make_block(block_id, state=STATE_ABSENT, reason=absent_reason))
        else:
            blocks.append(
                make_block(
                    block_id,
                    state=STATE_EMPTY,
                    reason=_NOT_BUILT,
                    kind=_kind(market, block_id),
                )
            )
    return blocks


def _kind(market: Market, block_id: str) -> str | None:
    if block_id == "price" and market.price is not None:
        return market.price.quote_kind
    if block_id == "crush" and market.crush is not None:
        return market.crush.kind
    return None


def _absent_reason(market: Market, block_id: str, tier_result) -> str | None:
    """Non-None when this market structurally has no source for the block."""
    if block_id in ("price", "crush", "basis"):
        source = getattr(market, block_id)
        return None if source is not None else market.absent_reason(block_id)
    if block_id == "ledger":
        if tier_result is not None and tier_result.has_daily_leg:
            return None
        return _SITE_WIDE_REASONS["ledger"]
    if block_id == "currency":
        if market.currency_pair:
            return None
        return _SITE_WIDE_REASONS["currency"].format(market=market.name, ccy=market.home_currency)
    if block_id == "weather":
        if market.weather_regions:
            return None
        return market.absent_reason("weather")
    if block_id == "supply_demand":
        return None if market.psd_country else _SITE_WIDE_REASONS["supply_demand"]
    if block_id == "news":
        return _SITE_WIDE_REASONS["news"]
    if block_id == "players":
        return None if market.players_country else _SITE_WIDE_REASONS["players"]
    raise KeyError(f"unknown block id {block_id!r}")


__all__ = [
    "BLOCK_IDS",
    "BLOCK_SPECS",
    "BRIEF_BLOCK_IDS",
    "GENERATION_ERROR",
    "STATE_ABSENT",
    "STATE_EMPTY",
    "STATE_OK",
    "STATES",
    "Block",
    "make_block",
    "skeleton_blocks",
]
