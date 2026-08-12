"""Market registry — the typed view of ``config.MARKETS`` plus the tier rule.

M8 #150 put the registry *data* in ``config.py`` (every other cross-layer
catalog lives there, and a second config home invites two answers to "where is
this configured"). This module is the registry *behaviour*: it validates those
descriptors, hands them out as frozen dataclasses, derives every market URL,
and computes each market's tier from what the DB actually holds today.

The tier lives here rather than in ``config.py`` for one concrete reason: it
reads the database, and ``config`` is imported *by* ``pipeline`` — a DB probe
in config would invert that dependency.

The tier rule (M1 #143):

    page   daily local price leg AND >=3 of {ledger, crush, basis, weather}
    brief  a daily leg with less than that, OR no daily leg with >=2 blocks
    stub   no daily leg and fewer than 2 blocks

"Present" is never "the descriptor names a source" — it is "that source has an
observation inside its own recency budget". A market whose scraper has been
dark for a fortnight is not a page just because ``config.py`` says it has a
price table (M1 constraint 3). The ledger is not probed separately: it is
daily-only (M10 #151), so it is present exactly when a daily leg is.

The URL is derived from the slug and is stable across tier changes (M8
constraint 3) — a brief is different *contents* at the same address, never a
404 because a scraper broke.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

import config
from analysis.spreads import CRUSH_MEAL_YIELD_MT, CRUSH_OIL_YIELD_MT
from pipeline.connection import get_connection, is_cloud, managed_connection
from pipeline.units import to_metric_tons

log = logging.getLogger(__name__)

# Where a market page is written, relative to docs/. Slug-derived, never
# hand-written, and unchanged by the tier.
MARKET_URL_TEMPLATE = "markets/{slug}.html"

TIER_PAGE = "page"
TIER_BRIEF = "brief"
TIER_STUB = "stub"

# The four blocks the tier rule counts, in M1's order. Crucially NOT all nine:
# FX, S&D, News and Players exist on every page regardless, so counting them
# would make every market a page.
SUPPORTING_BLOCKS = ("ledger", "crush", "basis", "weather")

# Recency budget for a source whose layer has no entry in
# LAYER_MAX_DATA_AGE_DAYS. Two calendar weeks — long enough to clear a holiday
# week on any venue, short enough that a dead scraper demotes the page within
# one sprint rather than never.
DEFAULT_MAX_AGE_DAYS = 14

# Crush yield factors per MT of beans, by named set. The board set is imported
# from analysis/spreads.py rather than restated: M7 #149 established that one
# crush engine serves every market and only the yields, FX and the kind-label
# vary, so there must be exactly one numeric home for these.
CRUSH_YIELD_SETS = {
    "soy_board": {"oil": CRUSH_OIL_YIELD_MT, "meal": CRUSH_MEAL_YIELD_MT},
}

# Every quote kind this stack carries. M3 #145 constraint 4: these are
# genuinely different animals and collapsing them into one "price" label is the
# easiest way to make the product dishonest, so the set is closed and a typo
# fails the build rather than shipping an unlabelled leg.
QUOTE_KINDS = frozenset({
    "board",                # traded exchange settlement (CBOT, DCE)
    "board_last_traded",    # traded, but last-traded rather than settlement (SAFEX)
    "physical",             # cash/spot assessment (CEPEA, mandi)
    "administered",         # official minimum export value (Argentina, Ley 21.453)
    "weekly_assessment",    # weekly physical assessment (EC Oilseeds Observatory)
})

CADENCES = frozenset({"daily", "weekly", "monthly"})

# How the stored number relates to USD/MT. Closed like QUOTE_KINDS, and for the
# same reason: a block builder that guessed would silently publish a price out
# by a factor of 36.7 (cents/bu) or by the exchange rate. The registry states
# it, `Source.to_usd_mt` applies it, and nothing else in the site converts.
#
#   native_exchange  the CBOT/CME native unit for that key — cents/bu, cents/lb
#                    or USD/short ton, per pipeline.units.CONVERSION_FACTORS
#   usd_per_bushel   USD per bushel (AMS 3147 prints flat CIF bids in $/bu)
#   home_per_mt      the market's home currency per MT — needs the FX leg
#   usd_per_mt       already USD/MT — no conversion at all
#   tonnes           a volume, not a price; never converted, never dual-quoted
#   observation      a non-price reading (a temperature) the tier probe only
#                    ever asks the date of
UNITS = frozenset({
    "native_exchange",
    "usd_per_bushel",
    "home_per_mt",
    "usd_per_mt",
    "tonnes",
    "observation",
})

# Units that are a price and therefore have a USD/MT rendering.
PRICE_UNITS = frozenset({"native_exchange", "usd_per_bushel", "home_per_mt", "usd_per_mt"})
# Whether trade connects a basis's two legs (M19 #222). This is not decoration:
# `open` means cargoes move between the two markets, so the spread is bounded
# by freight, quality, duty and FX and a trader can work it. `policy_blocked`
# means no such route exists — India's mandi bean prints +66% over CBOT because
# GM imports are banned behind a tariff wall, and nothing closes it (it reached
# ~2x in 2021). The distinction is closed and load-validated for the same
# reason QUOTE_KINDS is: rendering a policy spread with the same treatment as
# Paranaguá FOB invites a trade that cannot be taken, and a missing label is
# exactly the failure that would never be noticed in review.
ARBITRAGE_KINDS = frozenset({"open", "policy_blocked"})

# M12 #161 decision 3: two selection rules, not one, and each ledger states the
# one it was built under. The sentences live here rather than in the registry
# so eight pages cannot end up with eight paraphrases of the same rule; the
# registry declares only which rule applies.
LEDGER_RULE_STATEMENTS = {
    "origin": (
        "Origin page — who else is offering this cargo. These are competing "
        "physical offers, so the spread is an arbitrage a cargo can close."
    ),
    "destination": (
        "Destination page — what landed supply costs from the origins this "
        "market actually buys from. Not every origin: only the ones connected "
        "to this market by trade."
    ),
}

# Which descriptor sub-blocks a ledger leg may point at. `flows` is tonnage and
# `crush` is a derived margin — neither is a price a row can dual-quote.
LEDGER_LEG_BLOCKS = frozenset({"price", "basis"})


# ---------------------------------------------------------------------------
# Descriptor types
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Source:
    """A pointer at rows in the DB — table, date column, key column, keys.

    Deliberately generic: the tier probe asks every source the same question
    ("what is your newest observation?"), so a new market needs a registry
    entry and no new probe code.
    """

    layer: str
    table: str
    date_column: str
    key_column: str
    keys: tuple[str, ...]
    value_column: str
    unit: str
    cadence: str = "daily"
    quote_kind: str | None = None
    headline_key: str | None = None
    reference: str | None = None
    label: str | None = None
    # Basis sources only. See ARBITRAGE_KINDS; `caveat` is mandatory when the
    # arbitrage is blocked and rides onto the block with the number.
    arbitrage: str | None = None
    caveat: str | None = None

    @property
    def max_age_days(self) -> int:
        """The layer's own recency budget — the same one the pipeline grades on."""
        return config.LAYER_MAX_DATA_AGE_DAYS.get(self.layer, DEFAULT_MAX_AGE_DAYS)

    @property
    def is_price(self) -> bool:
        return self.unit in PRICE_UNITS

    def to_usd_mt(self, value: float | None, key: str, fx: float | None) -> float | None:
        """Convert one stored number to USD/MT, or None when it cannot be.

        The single conversion site for the whole site. ``fx`` is USD per unit
        of home currency (the ``<CCY>/USD`` series' own convention — the stack
        multiplies, see analysis.soy_analytics._latest_aligned_usd) and is only
        consulted for ``home_per_mt``; passing it elsewhere is ignored rather
        than silently double-converting.
        """
        if value is None:
            return None
        if self.unit == "usd_per_mt":
            return float(value)
        if self.unit == "native_exchange":
            return to_metric_tons(float(value), key)
        if self.unit == "usd_per_bushel":
            # $/bu -> cents/bu, then the same bushels-per-MT factor the rest of
            # the stack uses, so there is one table of grain densities.
            return to_metric_tons(float(value) * 100.0, key)
        if self.unit == "home_per_mt":
            if not fx:
                return None
            return float(value) * float(fx)
        return None  # tonnes / observation — no USD/MT rendering exists


@dataclass(frozen=True)
class Crush:
    """Crush legs for one market. ``kind`` is rendered on the block, always."""

    kind: str
    yield_set: str
    table: str
    date_column: str
    key_column: str
    legs: dict[str, str]
    value_column: str
    unit: str
    provisional: bool = False

    @property
    def yields(self) -> dict[str, float]:
        return CRUSH_YIELD_SETS[self.yield_set]

    def as_source(self) -> Source:
        return Source(
            layer=_LAYER_BY_TABLE.get(self.table, self.table),
            table=self.table,
            date_column=self.date_column,
            key_column=self.key_column,
            keys=tuple(self.legs.values()),
            value_column=self.value_column,
            unit=self.unit,
        )


@dataclass(frozen=True)
class LedgerLeg:
    """One row of a propagation ledger (M12 #161).

    A leg is *not* a market. ``us_gulf:cif`` is the AMS CIF NOLA bid living on
    the CBOT market's ``basis`` descriptor and has no market key of its own, so
    the registry now carries two id spaces and this type is the bridge between
    them: it resolves a leg id to the owning market plus the one key inside
    that market's descriptor the row reads.

    Nothing about the *data* is restated here — table, date column, unit, FX
    pair and home currency all come from ``source`` and ``market``, which are
    the owner's own descriptor. A leg says only what the owner cannot: which
    key it is, what to call it, and how its print is proved.
    """

    leg_id: str
    market: Market
    block: str
    key: str
    label: str
    trade_proof_column: str | None
    expected_gap_days: int

    @property
    def source(self) -> Source:
        return getattr(self.market, self.block)

    @property
    def href(self) -> str:
        """The leg's owning page — not always a page named after the leg.

        ``us_gulf:cif`` lands on the CBOT page, which is where the AMS bid is
        already explained (M12 decision 1).
        """
        return self.market.url


@dataclass(frozen=True)
class Ledger:
    """One market page's ledger: the pinned own leg plus its counterparts."""

    rule: str
    note: str
    legs: tuple[LedgerLeg, ...]
    reference_leg_ids: frozenset[str] = frozenset()

    @property
    def own(self) -> LedgerLeg:
        return self.legs[0]

    @property
    def counterparts(self) -> tuple[LedgerLeg, ...]:
        return self.legs[1:]

    @property
    def rule_statement(self) -> str:
        return LEDGER_RULE_STATEMENTS[self.rule]


@dataclass(frozen=True)
class Market:
    """One market. The parameter every block builder takes (M8 constraint 1)."""

    slug: str
    name: str
    venue: str
    home_currency: str
    currency_pair: str | None
    price: Source | None
    crush: Crush | None
    basis: Source | None
    flows: Source | None
    weather_regions: tuple[str, ...]
    psd_country: str | None
    players_country: str | None
    absent_reasons: dict[str, str] = field(default_factory=dict)
    # Wired in a second pass by `load_markets()` — a leg names another market,
    # so every market has to exist before any ledger can be resolved.
    ledger: Ledger | None = None

    @property
    def url(self) -> str:
        return MARKET_URL_TEMPLATE.format(slug=self.slug)

    def absent_reason(self, block: str) -> str:
        """Why ``block`` has no source at all — never an empty string.

        M1 constraint 2 distinguishes "no source exists" from "our ingest is
        broken". This answers the first; the tier probe answers the second.
        """
        return self.absent_reasons.get(block, f"no {block} source is configured for {self.name}")


# Which freshness layer owns a table, for sources that reuse another layer's
# table (the crush legs point at the price table).
_LAYER_BY_TABLE = {
    "prices": "prices",
    "dce_futures": "dce",
    "argentina_fob": "magyp_fob",
    "brazil_spot_prices": "cepea",
    "india_domestic_prices": "india_domestic",
    "safex_prices": "safex",
    "gulf_bids": "gulf_bids",
    "sagis_deliveries": "sagis",
}


# ---------------------------------------------------------------------------
# Loading and validation
# ---------------------------------------------------------------------------
def _source(raw: dict, *, slug: str, block: str) -> Source:
    missing = {"layer", "table", "date_column", "key_column", "keys", "value_column", "unit"} - raw.keys()
    if missing:
        raise ValueError(f"market {slug!r} {block} source missing key(s): {sorted(missing)}")
    keys = tuple(raw["keys"])
    if not keys:
        raise ValueError(f"market {slug!r} {block} source declares no keys")
    cadence = raw.get("cadence", "daily")
    if cadence not in CADENCES:
        raise ValueError(f"market {slug!r} {block} cadence {cadence!r} not in {sorted(CADENCES)}")
    quote_kind = raw.get("quote_kind")
    if quote_kind is not None and quote_kind not in QUOTE_KINDS:
        raise ValueError(
            f"market {slug!r} {block} quote_kind {quote_kind!r} not in {sorted(QUOTE_KINDS)}"
        )
    # Required on EVERY price leg, and enforced here rather than per consumer.
    # It was optional while block 01 was the only reader — one number, one
    # kind, stamped in the block header — so the basis legs shipped without
    # one; the ledger (M12 #161) then put those same descriptors in a shared
    # USD/MT column and had to demand it again in `_ledger_leg`, and block 04
    # prints its local leg beside the CBOT board with the same exposure. The
    # general shape: a descriptor reused by a second consumer may be missing
    # labels the first never needed, so the label belongs to the descriptor.
    # Volumes and observations have no kind and must not invent one.
    if quote_kind is None and raw["unit"] in PRICE_UNITS:
        raise ValueError(
            f"market {slug!r} {block} is a price leg (unit {raw['unit']!r}) and declares no "
            "quote_kind — a board settlement, a physical bid and an administered minimum "
            "are different animals and every surface that renders one must be able to say "
            "which (M3 #145 constraint 4)"
        )
    headline = raw.get("headline_key")
    if headline is not None and headline not in keys:
        raise ValueError(f"market {slug!r} {block} headline_key {headline!r} is not one of its keys")
    if raw["unit"] not in UNITS:
        raise ValueError(f"market {slug!r} {block} unit {raw['unit']!r} not in {sorted(UNITS)}")
    arbitrage = raw.get("arbitrage")
    caveat = raw.get("caveat")
    if block == "basis":
        # Required, not defaulted: an omitted field would silently mean "open",
        # which is the reading that ships a policy spread as a workable basis.
        if arbitrage not in ARBITRAGE_KINDS:
            raise ValueError(
                f"market {slug!r} basis must declare arbitrage in {sorted(ARBITRAGE_KINDS)}, "
                f"got {arbitrage!r} — see ARBITRAGE_KINDS (M19 #222)"
            )
        if arbitrage == "policy_blocked" and not (caveat or "").strip():
            raise ValueError(
                f"market {slug!r} basis is arbitrage={arbitrage!r} and must carry a caveat "
                "saying why the spread cannot be worked (M19 #222)"
            )
    elif arbitrage is not None:
        raise ValueError(f"market {slug!r} {block} source declares arbitrage — basis-only")
    return Source(
        layer=raw["layer"],
        table=raw["table"],
        date_column=raw["date_column"],
        key_column=raw["key_column"],
        keys=keys,
        value_column=raw["value_column"],
        unit=raw["unit"],
        cadence=cadence,
        quote_kind=quote_kind,
        headline_key=headline,
        reference=raw.get("reference"),
        label=raw.get("label"),
        arbitrage=arbitrage,
        caveat=caveat,
    )


def _crush(raw: dict, *, slug: str) -> Crush:
    missing = {
        "kind", "yield_set", "table", "date_column", "key_column", "legs",
        "value_column", "unit",
    } - raw.keys()
    if missing:
        raise ValueError(f"market {slug!r} crush missing key(s): {sorted(missing)}")
    if raw["unit"] not in PRICE_UNITS:
        raise ValueError(
            f"market {slug!r} crush unit {raw['unit']!r} is not a price unit "
            f"({sorted(PRICE_UNITS)}) — a margin cannot be built from a volume"
        )
    if raw["yield_set"] not in CRUSH_YIELD_SETS:
        raise ValueError(
            f"market {slug!r} crush yield_set {raw['yield_set']!r} "
            f"not in {sorted(CRUSH_YIELD_SETS)}"
        )
    legs = dict(raw["legs"])
    if set(legs) != {"bean", "oil", "meal"}:
        raise ValueError(f"market {slug!r} crush legs must be exactly bean/oil/meal, got {sorted(legs)}")
    return Crush(
        kind=raw["kind"],
        yield_set=raw["yield_set"],
        table=raw["table"],
        date_column=raw["date_column"],
        key_column=raw["key_column"],
        legs=legs,
        value_column=raw["value_column"],
        unit=raw["unit"],
        provisional=bool(raw.get("provisional", False)),
    )


def _market(slug: str, raw: dict) -> Market:
    missing = {"name", "venue", "home_currency", "weather_regions"} - raw.keys()
    if missing:
        raise ValueError(f"market {slug!r} missing key(s): {sorted(missing)}")

    reasons: dict[str, str] = {}
    for block in ("price", "crush", "basis", "weather", "flows"):
        reason = raw.get(f"{block}_absent_reason")
        if reason:
            reasons[block] = reason

    # A block that is None must say why, or the page renders a blank where
    # M1 constraint 2 requires a reason. Caught at load, not at render.
    for block in ("price", "crush", "basis"):
        if raw.get(block) is None and block not in reasons:
            raise ValueError(
                f"market {slug!r} has no {block} source and no {block}_absent_reason — "
                "every empty state must name its reason (M1 #143 constraint 2)"
            )
    if not raw["weather_regions"] and "weather" not in reasons:
        raise ValueError(f"market {slug!r} has no weather_regions and no weather_absent_reason")

    unknown = set(raw["weather_regions"]) - set(config.GROWING_REGIONS)
    if unknown:
        raise ValueError(f"market {slug!r} names weather region(s) not in GROWING_REGIONS: {sorted(unknown)}")
    if raw.get("currency_pair") and raw["currency_pair"] not in config.CURRENCY_TICKERS:
        raise ValueError(
            f"market {slug!r} currency_pair {raw['currency_pair']!r} is not in CURRENCY_TICKERS"
        )
    if raw.get("psd_country") and raw["psd_country"] not in config.PSD_TARGET_COUNTRIES:
        raise ValueError(
            f"market {slug!r} psd_country {raw['psd_country']!r} is not in PSD_TARGET_COUNTRIES"
        )

    return Market(
        slug=slug,
        name=raw["name"],
        venue=raw["venue"],
        home_currency=raw["home_currency"],
        currency_pair=raw.get("currency_pair"),
        price=_source(raw["price"], slug=slug, block="price") if raw.get("price") else None,
        crush=_crush(raw["crush"], slug=slug) if raw.get("crush") else None,
        basis=_source(raw["basis"], slug=slug, block="basis") if raw.get("basis") else None,
        flows=_source(raw["flows"], slug=slug, block="flows") if raw.get("flows") else None,
        weather_regions=tuple(raw["weather_regions"]),
        psd_country=raw.get("psd_country"),
        players_country=raw.get("players_country"),
        absent_reasons=reasons,
    )


def _ledger_leg(leg_id: str, markets: dict[str, Market]) -> LedgerLeg:
    """Resolve one leg id against the market registry, or hard-fail.

    This is the load-time gate M12 #161 asked for. A leg id lives in a
    different id space from a market slug, so nothing else in the registry
    would catch a typo: an unresolvable leg would quietly render an empty row,
    which reads as "that market has not printed" — the ledger's most important
    statement, made by accident.
    """
    raw = config.LEDGER_LEGS.get(leg_id)
    if raw is None:
        raise ValueError(
            f"ledger leg {leg_id!r} is not in config.LEDGER_LEGS "
            f"(known: {sorted(config.LEDGER_LEGS)})"
        )
    missing = {"market", "block", "key", "label"} - raw.keys()
    if missing:
        raise ValueError(f"ledger leg {leg_id!r} missing key(s): {sorted(missing)}")

    market = markets.get(raw["market"])
    if market is None:
        raise ValueError(f"ledger leg {leg_id!r} names market {raw['market']!r}, which is not registered")
    if raw["block"] not in LEDGER_LEG_BLOCKS:
        raise ValueError(
            f"ledger leg {leg_id!r} names block {raw['block']!r}, not one of "
            f"{sorted(LEDGER_LEG_BLOCKS)} — a ledger row is a dual-quoted price"
        )
    source = getattr(market, raw["block"])
    if source is None:
        raise ValueError(
            f"ledger leg {leg_id!r} points at {raw['market']}.{raw['block']}, which has no source"
        )
    if raw["key"] not in source.keys:
        raise ValueError(
            f"ledger leg {leg_id!r} names key {raw['key']!r}, which is not one of "
            f"{raw['market']}.{raw['block']}'s keys {list(source.keys)}"
        )
    if source.quote_kind is None:
        raise ValueError(
            f"ledger leg {leg_id!r} reads {raw['market']}.{raw['block']}, which declares no "
            "quote_kind — a board settlement, a physical bid and an administered minimum sit "
            "in one USD/MT column on this block and an unlabelled one reads as whatever its "
            "neighbours are (M3 #145 constraint 4)"
        )
    if source.cadence != "daily":
        raise ValueError(
            f"ledger leg {leg_id!r} reads a {source.cadence} source — the ledger is "
            "daily-only (M10 #151); a slower series belongs in a cadence-stamped "
            "context band, not a row that would read as an outage"
        )
    return LedgerLeg(
        leg_id=leg_id,
        market=market,
        block=raw["block"],
        key=raw["key"],
        label=raw["label"],
        trade_proof_column=raw.get("trade_proof_column"),
        expected_gap_days=int(
            raw.get("expected_gap_days", config.LEDGER_DEFAULT_EXPECTED_GAP_DAYS)
        ),
    )


def _ledger(slug: str, raw: dict, markets: dict[str, Market]) -> Ledger:
    missing = {"rule", "legs", "note"} - raw.keys()
    if missing:
        raise ValueError(f"market {slug!r} ledger missing key(s): {sorted(missing)}")
    if raw["rule"] not in config.LEDGER_RULES:
        raise ValueError(
            f"market {slug!r} ledger rule {raw['rule']!r} not in {sorted(config.LEDGER_RULES)}"
        )
    if raw["rule"] not in LEDGER_RULE_STATEMENTS:
        raise ValueError(f"ledger rule {raw['rule']!r} has no statement in LEDGER_RULE_STATEMENTS")

    leg_ids = list(raw["legs"])
    if len(set(leg_ids)) != len(leg_ids):
        raise ValueError(f"market {slug!r} ledger repeats a leg: {leg_ids}")
    if len(leg_ids) < 2:
        raise ValueError(
            f"market {slug!r} ledger has {len(leg_ids)} leg(s) — a ledger with nothing "
            "to compare against is a price block, not a ledger"
        )

    legs = tuple(_ledger_leg(leg_id, markets) for leg_id in leg_ids)
    if legs[0].market.slug != slug:
        raise ValueError(
            f"market {slug!r} ledger pins {legs[0].leg_id!r}, which belongs to "
            f"{legs[0].market.slug!r} — the first leg is always the page's own (M12 #161)"
        )

    reference = frozenset(raw.get("reference_legs") or ())
    unknown = reference - set(leg_ids)
    if unknown:
        raise ValueError(f"market {slug!r} ledger names reference leg(s) not in its own set: {sorted(unknown)}")
    if legs[0].leg_id in reference:
        raise ValueError(f"market {slug!r} ledger marks its own pinned leg as a reference row")

    # M12 India: a ledger whose counterparts are all the page's own legs has no
    # foreign price in it at all, and under a rule that promises "the origins
    # this market buys from" that reads as a set someone forgot to finish. The
    # note is where the reason lives, so it is required rather than optional.
    if not raw["note"].strip():
        raise ValueError(f"market {slug!r} ledger has an empty note — every ledger states why these legs")
    if all(leg.market.slug == slug for leg in legs[1:]):
        log.info(
            "market %s ledger names no foreign leg — domestic-only by decision (M12 #161)", slug
        )

    return Ledger(rule=raw["rule"], note=raw["note"].strip(), legs=legs, reference_leg_ids=reference)


def _wire_ledgers(markets: dict[str, Market]) -> dict[str, Market]:
    """Second pass: resolve every ledger now that all markets exist.

    Create-then-wire, for the same reason issues get ids before they can
    reference each other — a leg names another market, so no ledger can be
    built during the first pass.
    """
    from dataclasses import replace

    unknown = set(config.LEDGERS) - set(markets)
    if unknown:
        raise ValueError(f"config.LEDGERS names unregistered market(s): {sorted(unknown)}")
    undeclared = set(markets) - set(config.LEDGERS)
    if undeclared:
        raise ValueError(
            f"market(s) {sorted(undeclared)} declare no ledger — every market must state its "
            "counterpart set or an explicit None with a LEDGER_ABSENT_REASONS entry (M12 #161)"
        )

    wired: dict[str, Market] = {}
    for slug, market in markets.items():
        raw = config.LEDGERS[slug]
        if raw is None:
            reason = (config.LEDGER_ABSENT_REASONS.get(slug) or "").strip()
            if not reason:
                raise ValueError(
                    f"market {slug!r} has no ledger and no LEDGER_ABSENT_REASONS entry — "
                    "every empty state must name its reason (M1 #143 constraint 2)"
                )
            wired[slug] = replace(market, absent_reasons={**market.absent_reasons, "ledger": reason})
            continue
        wired[slug] = replace(market, ledger=_ledger(slug, raw, markets))
    return wired


def load_markets() -> dict[str, Market]:
    """Validate and return every registered market, in registry key order.

    Registry order IS nav order IS M2's ledger order — declared once in
    ``config.MARKETS`` and never re-sorted here.
    """
    markets: dict[str, Market] = {}
    for slug, raw in config.MARKETS.items():
        if slug != slug.lower() or not slug.replace("_", "").isalnum():
            raise ValueError(f"market slug {slug!r} must be lowercase alphanumeric with underscores")
        markets[slug] = _market(slug, raw)
    return _wire_ledgers(markets)


# ---------------------------------------------------------------------------
# Tier — computed from the DB at generation time
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class TierResult:
    """A market's tier plus the evidence for it — printed on the page (M8)."""

    slug: str
    tier: str
    has_daily_leg: bool
    present: tuple[str, ...]
    notes: dict[str, str]

    @property
    def supporting_count(self) -> int:
        return len(self.present)


def _latest_observation(conn, source: Source) -> date | None:
    """Newest observation date across ``source``'s keys, or None.

    None covers all three "nothing there" cases — table absent (a schema this
    DB predates), key absent, or the column holding NULLs. The caller cannot
    tell them apart and does not need to: all three mean the block has no data.
    """
    placeholders = ",".join("?" for _ in source.keys)
    sql = (
        f"SELECT MAX({source.date_column}) FROM {source.table} "  # noqa: S608 — identifiers come from the registry
        f"WHERE {source.key_column} IN ({placeholders})"
    )
    try:
        row = conn.execute(sql, tuple(source.keys)).fetchone()
    except (sqlite3.OperationalError, sqlite3.DatabaseError) as exc:
        log.debug("tier probe: %s unavailable (%s)", source.table, exc)
        return None
    if not row or row[0] is None:
        return None
    try:
        return datetime.fromisoformat(str(row[0])[:19]).date()
    except ValueError:
        log.warning("tier probe: %s.%s holds unparseable %r", source.table, source.date_column, row[0])
        return None


def _is_current(conn, source: Source | None, today: date) -> tuple[bool, str]:
    if source is None:
        return False, "no source configured"
    latest = _latest_observation(conn, source)
    if latest is None:
        return False, "no rows"
    age = (today - latest).days
    budget = source.max_age_days
    if age > budget:
        return False, f"newest observation {latest.isoformat()} is {age}d old (budget {budget}d)"
    return True, f"current to {latest.isoformat()}"


def _weather_source(market: Market) -> Source | None:
    if not market.weather_regions:
        return None
    return Source(
        layer="weather",
        table="weather",
        date_column="Date",
        key_column="region",
        keys=market.weather_regions,
        value_column="temp_max",
        unit="observation",
    )


def compute_tier(market: Market, conn, *, today: date | None = None) -> TierResult:
    """Grade one market against M1 #143's rule using live DB state."""
    today = today or datetime.now(timezone.utc).date()
    notes: dict[str, str] = {}

    has_daily_leg = False
    if market.price is not None and market.price.cadence == "daily":
        has_daily_leg, notes["price"] = _is_current(conn, market.price, today)
    elif market.price is not None:
        notes["price"] = f"{market.price.cadence} cadence — not a daily leg"
    else:
        notes["price"] = market.absent_reason("price")

    present: list[str] = []

    # The ledger is daily-only (M10 #151): present exactly when a daily leg is
    # AND the registry gives this market a counterpart set at all (M12 #161 —
    # Europe and Nigeria have none, and a block that cannot render is not a
    # block this market has).
    if has_daily_leg and market.ledger is not None:
        present.append("ledger")
        notes["ledger"] = f"daily leg present · {len(market.ledger.legs)} rows"
    elif market.ledger is None:
        notes["ledger"] = market.absent_reason("ledger")
    else:
        notes["ledger"] = "no daily leg — the ledger is daily-only (M10 #151)"

    for block, source in (
        ("crush", market.crush.as_source() if market.crush else None),
        ("basis", market.basis),
        ("weather", _weather_source(market)),
    ):
        if source is None:
            notes[block] = market.absent_reason(block)
            continue
        ok, notes[block] = _is_current(conn, source, today)
        if ok:
            present.append(block)

    count = len(present)
    if has_daily_leg and count >= 3:
        tier = TIER_PAGE
    elif has_daily_leg or count >= 2:
        tier = TIER_BRIEF
    else:
        tier = TIER_STUB

    return TierResult(
        slug=market.slug,
        tier=tier,
        has_daily_leg=has_daily_leg,
        present=tuple(present),
        notes=notes,
    )


def compute_tiers(
    markets: dict[str, Market] | None = None,
    *,
    today: date | None = None,
) -> dict[str, TierResult]:
    """Tier every market in one DB session, logging each verdict (M8)."""
    markets = markets if markets is not None else load_markets()

    if not is_cloud() and not os.path.exists(config.DB_PATH):
        # No DB at all (a fresh clone, or a docs-only CI job). Everything is a
        # stub with that stated as the reason — never a crash, and never a
        # silently full page.
        log.warning("No database at %s — every market tiers as a stub", config.DB_PATH)
        return {
            slug: TierResult(slug, TIER_STUB, False, (), {"db": "no database available"})
            for slug in markets
        }

    results: dict[str, TierResult] = {}
    with managed_connection(get_connection()) as conn:
        for slug, market in markets.items():
            results[slug] = compute_tier(market, conn, today=today)
            log.info(
                "market %-13s tier=%-5s daily_leg=%-5s supporting=%d %s",
                slug,
                results[slug].tier,
                results[slug].has_daily_leg,
                results[slug].supporting_count,
                list(results[slug].present),
            )
    return results


# ---------------------------------------------------------------------------
# Nav
# ---------------------------------------------------------------------------
def nav_items(
    tiers: dict[str, TierResult] | None = None,
    *,
    markets: dict[str, Market] | None = None,
    root: str = "",
) -> list[dict]:
    """The masthead market nav, in registry order.

    Every market appears in every tier — a brief and a stub still show, or the
    reader cannot tell the market exists (M8). Stubs are dimmed; pages and
    briefs render identically. The nav does not teach the three-tier system;
    the tier is printed on the page itself.
    """
    markets = markets if markets is not None else load_markets()
    return [
        {
            "slug": slug,
            "name": market.name,
            "href": f"{root}{market.url}",
            "tier": (tiers or {}).get(slug).tier if (tiers or {}).get(slug) else TIER_STUB,
        }
        for slug, market in markets.items()
    ]


def relative_root(output_relpath: str) -> str:
    """Prefix that gets a page at ``output_relpath`` back to ``docs/``.

    ``index.html`` -> ``""``; ``markets/cbot.html`` -> ``"../"``. Pages sit at
    two depths and the nav is identical on all of them, so the prefix has to be
    derived rather than hard-coded per template.
    """
    depth = output_relpath.replace("\\", "/").count("/")
    return "../" * depth


__all__ = [
    "ARBITRAGE_KINDS",
    "CRUSH_YIELD_SETS",
    "LEDGER_LEG_BLOCKS",
    "LEDGER_RULE_STATEMENTS",
    "MARKET_URL_TEMPLATE",
    "SUPPORTING_BLOCKS",
    "TIER_BRIEF",
    "TIER_PAGE",
    "TIER_STUB",
    "Crush",
    "Ledger",
    "LedgerLeg",
    "Market",
    "Source",
    "TierResult",
    "compute_tier",
    "compute_tiers",
    "load_markets",
    "nav_items",
    "relative_root",
]