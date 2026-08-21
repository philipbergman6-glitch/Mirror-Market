"""The nine block builders (M18 #214) — data only, never markup.

M17 #213 shipped the envelope and the page skeleton; every block rendered a
"not built yet" empty state. This module fills them.

Two rules run through all of it:

**The market is a parameter, never a code path.** There is no ``if market ==
"india"`` anywhere below, and adding one is the drift the whole contract exists
to prevent (M8 #150). Everything that varies between markets — which table,
which column, which unit, which FX pair, why a block is absent — is read from
the registry descriptor. That is why these builders are generic SQL over
``Source`` rather than nine hand-written per-market queries: a tenth market is
a ``config.MARKETS`` entry and no code.

**Builders return data; templates own markup.** Before M18 five headline
sections assembled ~600 lines of HTML inside f-strings, which cannot enforce
"same block, same treatment, eight markets" — a per-market tweak was one
f-string away and invisible in review. Every function here returns a ``Block``
whose ``data`` is numbers and labels; ``app/templates/blocks/NN_<id>.html.j2``
decides how it looks.

Failure isolation (M8): ``build_blocks`` catches per block. A builder that
raises becomes an ``empty`` block carrying ``GENERATION_ERROR`` — the same
*shape* as a missing source, a deliberately different *reason* — so one bad
market block cannot tombstone a page.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

import config
from app.blocks import (
    BLOCK_IDS,
    GENERATION_ERROR,
    STATE_ABSENT,
    STATE_EMPTY,
    STATE_OK,
    Block,
    absent_reason,
    make_block,
)
from app.markets import Market, Source, TierResult
from pipeline.units import native_label
from pricing.semantics import quote_kind_label

log = logging.getLogger(__name__)

# How much history each cadence reads. This is M8's chart budget applied at the
# source: a block that renders one number and a change has no business pulling
# 15 years of rows, and `docs/index.html` is 7 MB today precisely because
# nothing bounded the series a surface reads.
LOOKBACK_DAYS_BY_CADENCE = {"daily": 400, "weekly": 1100, "monthly": 1900}

# Sessions back for the "change" figure on a price leg. One observation, not a
# calendar day: a market that did not print yesterday must compare against the
# last time it *did*, or a holiday reads as a flat market.
CHANGE_LOOKBACK_OBS = (1, 5, 21)

# PSD attributes the supply & demand block renders, in balance-sheet order.
PSD_ATTRIBUTES = ("Production", "Imports", "Exports", "Ending Stocks")

# The market whose board every basis is struck against. Named once here rather
# than read from each `Source.reference`, which is checked against it.
REFERENCE_MARKET = "cbot"

# M3 #145's state pills. Three, not four: a venue that re-dates a carried price
# has not repriced, which `no_print_since` already says — see `leg_prints`.
# `out_of_cadence` exists only on the headline ledger, whose rows are markets
# rather than legs and therefore include Europe's weekly leg.
LEDGER_STATE_REPRICED = "repriced"
LEDGER_STATE_NO_PRINT = "no_print_since"
LEDGER_STATE_DARK = "dark"
LEDGER_STATE_OUT_OF_CADENCE = "out_of_cadence"

# How far the USD and home-currency moves must diverge before the row is tagged
# `FX`. A tenth of a percentage point: below that the rate is rounding, above it
# the currency is doing work the reader would otherwise credit to the market.
FX_DIVERGENCE_PP = 0.1


# ---------------------------------------------------------------------------
# Shared reads
# ---------------------------------------------------------------------------
@dataclass
class SiteContext:
    """Everything the nine builders share, loaded once for the whole site.

    Eight market pages × nine blocks is 72 builder calls; without this the
    CBOT reference series alone would be read eight times.
    """

    conn: Any
    today: date
    _cache: dict = field(default_factory=dict)
    _owned: Any = None

    @classmethod
    def open(cls, *, today: date | None = None) -> SiteContext:
        """One connection for the whole site, or none at all.

        A missing DB is not an error here: ``compute_tiers`` already stubs
        every market in that case, and every read below returns nothing, so
        each block states its own honest empty reason rather than the run
        dying on a fresh clone or a docs-only CI job.
        """
        # Via app.markets rather than pipeline.connection directly: the tier
        # probe already resolves the connection there, and two import sites
        # would mean the tiers and the blocks could read different databases.
        import os

        from app.markets import get_connection, is_cloud

        today = today or datetime.now(timezone.utc).date()

        if not is_cloud() and not os.path.exists(config.DB_PATH):
            log.warning("No database at %s — every block renders an empty state", config.DB_PATH)
            return cls(conn=None, today=today)
        conn = get_connection()
        return cls(conn=conn, today=today, _owned=conn)

    def close(self) -> None:
        if self._owned is not None:
            try:
                self._owned.close()
            except Exception:  # noqa: BLE001 — closing must never fail a render
                log.debug("site context close failed", exc_info=True)
            self._owned = None

    def cached(self, key, produce: Callable[[], Any]):
        if key not in self._cache:
            self._cache[key] = produce()
        return self._cache[key]

    # -- generic source reads ------------------------------------------------
    def series(self, source: Source) -> dict[str, list[tuple[date, float, int]]]:
        """``{key: [(date, value, n_rows), ...]}`` oldest-first, per key.

        ``n_rows`` is how many stored rows one key/date collapsed into. Several
        sources legitimately print more than one row per key per day — AMS
        quotes each barge location and delivery window, MAGyP each shipment
        window — so the value is their mean and the count travels with it, to
        be rendered rather than hidden ("avg of 3 quotes").
        """
        return self.cached(("series", source), lambda: _read_series(self.conn, source, self.today))

    def latest(self, source: Source, key: str) -> tuple[date, float, int] | None:
        rows = self.series(source).get(key) or []
        return rows[-1] if rows else None

    def leg_prints(self, leg) -> list[tuple[date, float, int]]:
        """One ledger leg's *prints*, oldest-first — trade-proved where it can be.

        Deliberately not ``series()``: a leg whose venue re-dates a
        carry-forward row (SAFEX, #157) has rows that are not prints, and the
        ledger's whole claim is about which markets have repriced. Where the
        registry names a ``trade_proof_column`` the read enforces it here too,
        rather than trusting the fetcher to have done it — the invariant is
        cheap to state at both ends and expensive to discover at neither.
        """
        source = leg.source

        def build():
            rows = _read_series(
                self.conn, source, self.today, proof_column=leg.trade_proof_column
            )
            return rows.get(leg.key) or []
        return self.cached(("leg", leg.leg_id), build)

    # -- FX ------------------------------------------------------------------
    def fx_series(self, pair: str | None) -> list[tuple[date, float]]:
        """``<CCY>/USD`` closes, oldest-first. Empty when the pair is unset."""
        if not pair:
            return []
        return self.cached(("fx", pair), lambda: _read_fx(self.conn, pair, self.today))

    def fx(self, pair: str | None) -> tuple[date, float] | None:
        rows = self.fx_series(pair)
        return rows[-1] if rows else None

    def fx_rate(self, pair: str | None) -> float | None:
        latest = self.fx(pair)
        return latest[1] if latest else None

    def fx_on(self, pair: str | None, when: date) -> float | None:
        """The rate on ``when``, else the newest rate at or before it.

        A price dated D converted at today's rate is a different number from
        the same price converted at D's rate; the ledger's dual quote is only
        honest if the two legs are struck on the same day where one exists.
        """
        rows = self.fx_series(pair)
        prior = [rate for day, rate in rows if day <= when]
        return prior[-1] if prior else (rows[0][1] if rows else None)

    # -- the reference board -------------------------------------------------
    def reference_series(self, markets: dict[str, Market]) -> list[tuple[date, float]]:
        """CBOT's headline leg in USD/MT, oldest-first — every basis's other leg."""
        def build() -> list[tuple[date, float]]:
            ref = markets.get(REFERENCE_MARKET)
            if ref is None or ref.price is None:
                return []
            key = ref.price.headline_key or ref.price.keys[0]
            rows = self.series(ref.price).get(key) or []
            out = []
            for day, value, _ in rows:
                usd = ref.price.to_usd_mt(value, key, None)
                if usd is not None:
                    out.append((day, usd))
            return out
        return self.cached("reference_series", build)


def _read_series(
    conn,
    source: Source,
    today: date,
    *,
    proof_column: str | None = None,
) -> dict[str, list[tuple[date, float, int]]]:
    if conn is None:
        return {key: [] for key in source.keys}
    lookback = LOOKBACK_DAYS_BY_CADENCE.get(source.cadence, 400)
    cutoff = (today - _days(lookback)).isoformat()
    placeholders = ",".join("?" for _ in source.keys)
    # M12/#157: where a venue publishes a field that proves a trade happened,
    # a row failing it is not a print. `<= 0` is proof of no trade and the row
    # goes; NULL is proof of nothing and the row stays — dropping it would
    # invent an outage, which is the one thing a "who has repriced" surface
    # must never do.
    proof_clause = (
        f"AND ({proof_column} IS NULL OR {proof_column} > 0) " if proof_column else ""
    )
    sql = (
        f"SELECT {source.key_column}, {source.date_column}, "  # noqa: S608 — identifiers come from the registry
        f"AVG({source.value_column}), COUNT({source.value_column}) "
        f"FROM {source.table} "
        f"WHERE {source.key_column} IN ({placeholders}) "
        f"AND {source.date_column} >= ? AND {source.value_column} IS NOT NULL "
        f"{proof_clause}"
        f"GROUP BY {source.key_column}, {source.date_column} "
        f"ORDER BY {source.date_column}"
    )
    out: dict[str, list[tuple[date, float, int]]] = {key: [] for key in source.keys}
    try:
        rows = conn.execute(sql, (*source.keys, cutoff)).fetchall()
    except Exception as exc:  # noqa: BLE001 — a missing table is data, not a crash
        log.debug("block read: %s unavailable (%s)", source.table, exc)
        return out
    for key, raw_date, value, count in rows:
        parsed = _parse_date(raw_date)
        if parsed is None or value is None:
            continue
        out.setdefault(str(key), []).append((parsed, float(value), int(count or 1)))
    return out


def _read_fx(conn, pair: str, today: date) -> list[tuple[date, float]]:
    if conn is None:
        return []
    cutoff = (today - _days(LOOKBACK_DAYS_BY_CADENCE["daily"])).isoformat()
    try:
        rows = conn.execute(
            "SELECT Date, Close FROM currencies WHERE pair = ? AND Date >= ? "
            "AND Close IS NOT NULL ORDER BY Date",
            (pair, cutoff),
        ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug("block read: currencies unavailable (%s)", exc)
        return []
    out = []
    for raw_date, close in rows:
        parsed = _parse_date(raw_date)
        if parsed is not None:
            out.append((parsed, float(close)))
    return out


def _days(n: int):
    from datetime import timedelta
    return timedelta(days=n)


def _parse_date(raw) -> date | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    try:
        return datetime.fromisoformat(str(raw)[:19]).date()
    except ValueError:
        return None


def _pct_change(current: float | None, prior: float | None) -> float | None:
    if current is None or not prior:
        return None
    return (current - prior) / abs(prior) * 100.0


def _age_days(today: date, when: date | None) -> int | None:
    return None if when is None else (today - when).days


# ---------------------------------------------------------------------------
# 01 Price
# ---------------------------------------------------------------------------
def price_block(market: Market, ctx: SiteContext, **_) -> tuple[str, str, dict]:
    source = market.price
    rows_by_key = ctx.series(source)
    legs: list[dict] = []
    ordered = _headline_first(source)

    for key in ordered:
        rows = rows_by_key.get(key) or []
        if not rows:
            continue
        when, value, n_rows = rows[-1]
        fx = ctx.fx_on(market.currency_pair, when)
        changes = {}
        for back in CHANGE_LOOKBACK_OBS:
            prior = rows[-1 - back] if len(rows) > back else None
            changes[f"chg_{back}_pct"] = _pct_change(value, prior[1] if prior else None)
        legs.append({
            "key": key,
            "label": source.key_label(key),
            "is_headline": key == (source.headline_key or ordered[0]),
            "home_value": value,
            "home_currency": market.home_currency,
            "usd_mt": source.to_usd_mt(value, key, fx),
            "as_of": when.isoformat(),
            "age_days": _age_days(ctx.today, when),
            # Several quotes on one date are averaged (see SiteContext.series);
            # say so rather than presenting a mean as a single print.
            "quotes": n_rows,
            **changes,
        })

    if not legs:
        return STATE_EMPTY, (
            f"{source.table} holds no {source.cadence} observation inside its "
            f"{source.max_age_days}-day budget"
        ), {}

    headline = next(leg for leg in legs if leg["is_headline"]) if any(
        leg["is_headline"] for leg in legs) else legs[0]
    return STATE_OK, "", {
        "legs": legs,
        "headline": headline,
        "quote_kind": source.quote_kind,
        "cadence": source.cadence,
        "venue": market.venue,
        "home_currency": market.home_currency,
        "signals": _signal_chips(rows_by_key.get(headline["key"]) or [], headline["key"]),
        # M3 #145: the stack stores no time of day, so a page can honestly show
        # the date a venue printed and never a timestamp.
        "no_timestamp_note": "dates only — this stack stores no time of day",
    }


def _headline_first(source: Source) -> list[str]:
    keys = list(source.keys)
    if source.headline_key and source.headline_key in keys:
        keys.remove(source.headline_key)
        keys.insert(0, source.headline_key)
    return keys


def _signal_chips(rows: list[tuple[date, float, int]], key: str) -> list[dict]:
    """M2 #144: signals ride inside block 01 as severity chips, not a tenth block.

    Computed from the market's own close series, so every market gets the same
    treatment — the price-only detectors, since a generic source has no volume
    column and a volume signal we cannot compute must be absent, not faked.
    """
    if len(rows) < 60:
        return []
    try:
        import pandas as pd

        from analysis.signals import (
            demote_near_roll_signals,
            detect_ma_crossovers,
            detect_macd_crossover,
            detect_rsi_extremes,
        )
        from analysis.technical import compute_all_technicals

        frame = pd.DataFrame(
            {"Close": [value for _, value, _ in rows]},
            index=pd.to_datetime([day for day, _, _ in rows]),
        )
        frame = compute_all_technicals(frame)
        signals: list[dict] = []
        for detect in (detect_ma_crossovers, detect_rsi_extremes, detect_macd_crossover):
            signals.extend(detect(frame, key))
        signals = demote_near_roll_signals(signals)
    except Exception:  # noqa: BLE001 — chips are decoration; the price is the block
        log.warning("signal chips failed for %s", key, exc_info=True)
        return []
    return [
        {
            "severity": sig.get("severity", "info"),
            "message": sig.get("description") or sig.get("message", ""),
        }
        for sig in signals
    ]


# ---------------------------------------------------------------------------
# 02 Propagation ledger
# ---------------------------------------------------------------------------
def ledger_block(market: Market, ctx: SiteContext, **_) -> tuple[str, str, dict]:
    """The propagation ledger: who has repriced, and who has not printed.

    Shape from M3 #145 — settlement-ordered rows, each dual-quoting USD/MT over
    its home print, both moves with an ``FX`` tag when they diverge, and a state
    pill so silence can never read as flat. Counterpart set from M12 #161, read
    from the registry (``config.LEDGERS``) rather than chosen here.

    Two things this builder is careful about, both of which would otherwise
    produce a confident wrong number:

    **A row stamp is not a print.** Where a venue publishes a field proving a
    trade, ``leg_prints`` enforces it — Grain SA re-dates a carried SAFEX price
    with Volume 0 (#157), and a ledger that read that as a reprice would be
    lying in exactly the place it claims to be authoritative.

    **A spread is one session's number.** The legs print on different days —
    that is the point of the surface — so the spread against the pinned leg is
    struck on the most recent session *both* printed, and its date is rendered
    whenever that is not the row's own. Subtracting two different days would
    manufacture an arbitrage out of a calendar gap.
    """
    ledger = market.ledger
    own_prints = ctx.leg_prints(ledger.own)
    if not own_prints:
        return STATE_EMPTY, (
            f"{ledger.own.label} has no trade-proved print inside its "
            f"{ledger.own.source.max_age_days}-day budget — with no pinned leg there is "
            "nothing for the counterparts to be a spread against"
        ), {}

    rows = [
        _ledger_row(
            leg,
            ctx,
            is_own=(index == 0),
            is_reference=leg.leg_id in ledger.reference_leg_ids,
            own_prints=own_prints if index else None,
            own_leg=ledger.own,
        )
        for index, leg in enumerate(ledger.legs)
    ]

    # Settlement-ordered (M3): newest print first, so the reader's eye lands on
    # whoever repriced most recently. The pinned own leg keeps its place at the
    # top whatever its date — it is the page's subject, not a competitor — and
    # reference rows ride last by decision (M12: CBOT on South Africa is a
    # yardstick, not a peer).
    own_row, others = rows[0], rows[1:]
    others.sort(key=lambda row: (row["is_reference"], _sort_date(row)))
    ordered = [own_row, *others]

    dated = [row["as_of"] for row in ordered if row["as_of"]]
    leading_edge = max(dated) if dated else None
    for row in ordered:
        row["state"], row["state_detail"], row["overdue"] = _ledger_state(
            row, leading_edge=leading_edge, today=ctx.today
        )

    return STATE_OK, "", {
        "rule": ledger.rule,
        "rule_statement": ledger.rule_statement,
        "note": ledger.note,
        "rows": ordered,
        "leading_edge": leading_edge,
        "has_spread": True,
        "commodity": "Soybean",
        # M12 decision 2 — one commodity per ledger, named on the block. In a
        # single USD/MT column a per-row good label is not strong enough to stop
        # five numbers reading as one price in five places.
        "commodity_note": (
            "Every row is the soybean. Meal and oil are a different good and would "
            "be a second ledger, not more rows here."
        ),
        # M3 #145 / M4 #146: the stack stores no time of day, so the ordering is
        # by settlement *date* and the page must not imply anything finer.
        "no_timestamp_note": (
            "ordered by print date — this stack stores no time of day, so two legs "
            "sharing a date are not ordered against each other"
        ),
    }


def headline_ledger(markets: dict[str, Market], ctx: SiteContext) -> tuple[str, str, dict]:
    """The headline page's eight-row ledger (M2 #144 / M12 #161).

    Rows are **markets** here, not legs, and the market cell is the link — this
    is the headline's sole per-market presence, so a reader scans it to decide
    which page to open. Three consequences, all of them M12's:

    * **No spread column.** There is no pinned own leg on the headline, so
      there is nothing for a spread to be against. Levels and moves only.
    * **Europe carries no value.** Its only leg is the EC's weekly assessment —
      out of the ledger's cadence, not missing — so it renders
      ``out_of_cadence`` rather than a value that would invite comparison
      against seven daily prints, and rather than a ``dark`` pill that would
      report an outage that is not happening.
    * **Nigeria is dark**, with the registry's own reason: no price leg of any
      kind is ingested.
    """
    rows: list[dict] = []
    for market in markets.values():
        if market.ledger is not None:
            leg = market.ledger.own
            row = _ledger_row(
                leg, ctx, is_own=False, is_reference=False, own_prints=None, own_leg=leg
            )
            row["forced_state"] = None
        else:
            row = _headline_placeholder(market)
        # On the headline the row IS the market, so the market name leads and
        # the leg label rides underneath as the thing actually quoted.
        row["market_label"] = market.name
        row["href"] = market.url
        rows.append(row)

    dated = [row["as_of"] for row in rows if row["as_of"]]
    leading_edge = max(dated) if dated else None
    for row in rows:
        forced = row.pop("forced_state", None)
        if forced:
            row["state"], row["state_detail"], row["overdue"] = forced
        else:
            row["state"], row["state_detail"], row["overdue"] = _ledger_state(
                row, leading_edge=leading_edge, today=ctx.today
            )

    if not dated:
        return STATE_EMPTY, "no market on the ledger has a print in its window", {}
    return STATE_OK, "", {
        "rows": rows,
        "leading_edge": leading_edge,
        "has_spread": False,
        "commodity": "Soybean",
        "commodity_note": (
            "Every row is the soybean — Europe's row names its own good, which is "
            "why it carries no value here."
        ),
        "no_timestamp_note": (
            "ordered as declared in the registry — role in the trade. This stack "
            "stores no time of day, so no finer ordering than the date exists."
        ),
    }


def _headline_placeholder(market: Market) -> dict:
    """A row for a market with no ledger — valueless, and it says which kind."""
    source = market.price
    row = {
        "leg_id": None,
        "label": source.label if source is not None else market.venue,
        "market_slug": market.slug,
        "market_name": market.name,
        "href": market.url,
        "kind": source.quote_kind if source is not None else None,
        "kind_label": (
            quote_kind_label(source.quote_kind)
            if source is not None and source.quote_kind else None
        ),
        "is_own": False,
        "is_reference": False,
        "expected_gap_days": None,
        "budget_days": None,
        "trade_proved": False,
        "as_of": None,
        "age_days": None,
        "usd_mt": None,
        "home_value": None,
        "home_unit": "",
        "has_home_quote": False,
        "usd_chg_pct": None,
        "home_chg_pct": None,
        "fx_tag": False,
        "fx_note": None,
        "quotes": None,
        "spread_usd_mt": None,
        "spread_as_of": None,
        "spread_note": None,
    }
    if source is None:
        row["forced_state"] = (LEDGER_STATE_DARK, market.absent_reason("price"), True)
    else:
        row["label"] = source.label or (source.headline_key or source.keys[0])
        row["forced_state"] = (
            LEDGER_STATE_OUT_OF_CADENCE,
            f"{source.cadence} assessment — outside the ledger's daily cadence, "
            "so no value is shown here (M10 #151)",
            False,
        )
    return row


def _sort_date(row: dict):
    """Newest first, undated last — a leg with no print cannot be ordered by one."""
    return (row["as_of"] is None, "" if row["as_of"] is None else _negated(row["as_of"]))


def _negated(iso: str) -> str:
    """Descending-by-date sort key for an ISO date, as a string."""
    return "".join(chr(ord("9") - (ord(ch) - ord("0"))) if ch.isdigit() else ch for ch in iso)


def _ledger_row(
    leg,
    ctx: SiteContext,
    *,
    is_own: bool,
    is_reference: bool,
    own_prints: list[tuple[date, float, int]] | None,
    own_leg,
) -> dict:
    """One ledger row, before its state pill is set (that needs the whole set)."""
    source = leg.source
    owner = leg.market
    prints = ctx.leg_prints(leg)

    row = {
        "leg_id": leg.leg_id,
        "label": leg.label,
        "market_slug": owner.slug,
        "market_name": owner.name,
        "href": leg.href,
        "kind": source.quote_kind,
        # The venue plus the price type. One USD/MT column carrying a delayed
        # board close, a cash assessment and a decreed minimum needs each row to
        # say which it is; "board" alone reads as a settlement this stack does
        # not hold.
        "kind_label": quote_kind_label(source.quote_kind) if source.quote_kind else None,
        "is_own": is_own,
        "is_reference": is_reference,
        "expected_gap_days": leg.expected_gap_days,
        # The same recency answer main.py grades this layer on — one number in
        # the codebase, so the site cannot call a source alive that the pipeline
        # has already failed.
        "budget_days": source.max_age_days,
        "trade_proved": leg.trade_proof_column is not None,
        "as_of": None,
        "age_days": None,
        "usd_mt": None,
        "home_value": None,
        "home_unit": _home_unit(source, leg.key, owner.home_currency),
        "has_home_quote": source.unit != "usd_per_mt",
        "usd_chg_pct": None,
        "home_chg_pct": None,
        "fx_tag": False,
        "fx_note": None,
        "quotes": None,
        "spread_usd_mt": None,
        "spread_as_of": None,
        "spread_note": None,
    }
    if not prints:
        return row

    when, value, quotes = prints[-1]
    fx = ctx.fx_on(owner.currency_pair, when)
    row.update({
        "as_of": when.isoformat(),
        "age_days": _age_days(ctx.today, when),
        "usd_mt": source.to_usd_mt(value, leg.key, fx),
        "home_value": value,
        "quotes": quotes,
    })

    if len(prints) > 1:
        prior_when, prior_value, _ = prints[-2]
        row["home_chg_pct"] = _pct_change(value, prior_value)
        prior_usd = source.to_usd_mt(prior_value, leg.key, ctx.fx_on(owner.currency_pair, prior_when))
        row["usd_chg_pct"] = _pct_change(row["usd_mt"], prior_usd)
        # M3 #145: show both moves, and say so when the currency did the work.
        # A leg quoted in its own currency can be up at home and down in USD;
        # printing one number would attribute an FX move to the market.
        if (
            source.unit == "home_per_mt"
            and row["usd_chg_pct"] is not None
            and row["home_chg_pct"] is not None
            and abs(row["usd_chg_pct"] - row["home_chg_pct"]) >= FX_DIVERGENCE_PP
        ):
            row["fx_tag"] = True
            row["fx_note"] = (
                f"{owner.currency_pair} moved between prints — the USD and "
                f"{owner.home_currency} moves are not the same number"
            )

    if own_prints is not None:
        row["spread_usd_mt"], row["spread_as_of"], row["spread_note"] = _spread(
            leg, prints, own_leg, own_prints, ctx
        )
    return row


def _spread(leg, prints, own_leg, own_prints, ctx: SiteContext):
    """This leg minus the pinned leg, struck on the newest session both printed.

    Never a cross-day subtraction: two legs from different days differ by the
    calendar as much as by the arbitrage, and the whole reason this surface
    exists is that they *do* print on different days.
    """
    own_by_date = {day: (value, day) for day, value, _ in own_prints}
    for day, value, _ in reversed(prints):
        if day not in own_by_date:
            continue
        mine = leg.source.to_usd_mt(value, leg.key, ctx.fx_on(leg.market.currency_pair, day))
        theirs = own_leg.source.to_usd_mt(
            own_by_date[day][0], own_leg.key, ctx.fx_on(own_leg.market.currency_pair, day)
        )
        if mine is None or theirs is None:
            continue
        return mine - theirs, day.isoformat(), None
    return None, None, (
        f"no session in the window where both {leg.label} and {own_leg.label} printed — "
        "a spread across two dates is a calendar artefact, not an arbitrage"
    )


def _ledger_state(row: dict, *, leading_edge: str | None, today: date):
    """M3 #145's three pills, plus whether being behind is *abnormal*.

    ``dark`` is the leg's own recency budget — the same number ``main.py``
    grades the layer on, so the site and the pipeline cannot disagree about
    whether a source is alive. ``overdue`` is M4 section 3.4 trap 5: a daily leg
    six days silent is well inside ``FRESHNESS_WARNING_DAYS = 7`` and still
    plainly wrong, so the ledger carries each leg's own expected gap instead.
    """
    if row["as_of"] is None:
        return LEDGER_STATE_DARK, (
            "no trade-proved print in the window"
            if row["trade_proved"] else "no print in the window"
        ), True
    if row["age_days"] is not None and row["age_days"] > row["budget_days"]:
        return LEDGER_STATE_DARK, (
            f"newest print {row['as_of']} is {row['age_days']}d old, past this layer's "
            f"{row['budget_days']}d budget"
        ), True
    if leading_edge is not None and row["as_of"] == leading_edge:
        return LEDGER_STATE_REPRICED, f"printed {row['as_of']}, the newest on this ledger", False
    overdue = (row["age_days"] or 0) > row["expected_gap_days"]
    detail = f"last printed {row['as_of']}"
    if overdue:
        detail += (
            f" — {row['age_days']}d ago, past the {row['expected_gap_days']}d gap that is "
            "normal for this leg"
        )
    return LEDGER_STATE_NO_PRINT, detail, overdue


def _home_unit(source: Source, key: str, home_currency: str) -> str:
    """What the venue's own number is quoted in — never inferred from the table.

    A price is only a price with its unit, and one table can hold three: CBOT's
    `prices` rows are cents/bu, cents/lb and USD/short ton at once. The registry
    states the unit; this turns it into a label and nothing here guesses.
    """
    if source.unit == "home_per_mt":
        return f"{home_currency}/MT"
    if source.unit == "usd_per_mt":
        return "USD/MT"
    if source.unit == "usd_per_bushel":
        return "USD/bu"
    if source.unit == "native_exchange":
        return native_label(key) or "native"
    return ""


# ---------------------------------------------------------------------------
# 03 Crush
# ---------------------------------------------------------------------------
def crush_block(market: Market, ctx: SiteContext, **_) -> tuple[str, str, dict]:
    crush = market.crush
    if crush.contracts == "named":
        return _named_crush_block(market, ctx)
    source = crush.as_source()
    rows_by_key = ctx.series(source)

    # One engine, every market (M7 #149): the margin is computed on a date all
    # three legs share. Taking each leg's own latest print would silently mix
    # sessions and quietly move the margin on a day one leg did not trade.
    common = None
    for key in crush.legs.values():
        days = {day for day, _, _ in rows_by_key.get(key) or []}
        common = days if common is None else (common & days)
    if not common:
        return STATE_EMPTY, (
            "the bean, oil and meal legs share no session — a margin across "
            "different days would not be a margin"
        ), {}

    when = max(common)
    fx = ctx.fx_on(market.currency_pair, when)
    legs: dict[str, dict] = {}
    for leg_name, key in crush.legs.items():
        value = next(v for day, v, _ in rows_by_key[key] if day == when)
        legs[leg_name] = {
            "key": key,
            "home_value": value,
            "usd_mt": source.to_usd_mt(value, key, fx),
        }

    if any(leg["usd_mt"] is None for leg in legs.values()):
        return STATE_EMPTY, (
            f"no {market.currency_pair or 'FX'} rate for {when.isoformat()} — the legs "
            "quote in " + market.home_currency + " and cannot be stated in USD/MT"
        ), {}

    yields = crush.yields
    margin_usd = (
        legs["oil"]["usd_mt"] * yields["oil"]
        + legs["meal"]["usd_mt"] * yields["meal"]
        - legs["bean"]["usd_mt"]
    )
    # A home-currency margin exists only where all three legs are quoted per MT
    # in that one currency. CBOT's legs are cents/bu, cents/lb and USD/short
    # ton — the yields are MT-per-MT factors, so combining them in native units
    # produces a number with no unit at all (it printed "USD -820/MT" beside a
    # +$66.9 margin before this check). USD/MT is the only honest statement of
    # a board crush, which is what M7 #149's one engine means in practice.
    margin_home = (
        legs["oil"]["home_value"] * yields["oil"]
        + legs["meal"]["home_value"] * yields["meal"]
        - legs["bean"]["home_value"]
    ) if crush.unit == "home_per_mt" else None
    return STATE_OK, "", {
        "kind": crush.kind,
        "legs": legs,
        "yields": yields,
        "margin_usd_mt": margin_usd,
        "margin_home": margin_home,
        "home_currency": market.home_currency,
        "as_of": when.isoformat(),
        "age_days": _age_days(ctx.today, when),
        "profitable": margin_usd > 0,
        # M7 #149 / M5 #147: a margin off an inferred NCM position code is not
        # a number to trade on, and the caveat rides with the data.
        "provisional": crush.provisional,
        "provisional_note": (
            "provisional — one leg's position code is inferred, not cross-checked "
            "against the labelled series (M5 #147)"
        ) if crush.provisional else None,
        # Structural, not decorative: a margin off continuous main-contract
        # series is arithmetically fine and names no instrument an order can be
        # placed in. The block renders the two differently because they are
        # different claims.
        "contract_basis": crush.contracts,
        "hedgeable": False,
        "contract_note": (
            "Continuous main-contract series: the underlying contract changes on the "
            "provider's own schedule and is not published, so this margin names no "
            "contract and is not one an order can be placed against."
            if crush.contracts == "continuous" else None
        ),
        "legs_named": False,
    }


def _named_crush_block(market: Market, ctx: SiteContext) -> tuple[str, str, dict]:
    """The board crush on explicit delivery months (``analysis.futures.crush``).

    The whole of the CBOT block's arithmetic lives there and is shared with
    Origins, the Workstation, the Opportunity board and the briefing — so the
    five surfaces cannot print five different crushes, which is what happened
    while each one reached for the continuous series itself.
    """
    from analysis.futures.crush import CrushWithheld, named_board_crush
    from analysis.futures.providers import open_provider

    if ctx.conn is None:
        return STATE_EMPTY, "no database connection", {}
    outcome = named_board_crush(open_provider(ctx.conn), as_of=ctx.today)
    if isinstance(outcome, CrushWithheld):
        return STATE_EMPTY, outcome.reason, {}

    data = outcome.to_dict()
    data.update({
        "kind": market.crush.kind,
        "home_currency": market.home_currency,
        "as_of": outcome.observation_date.isoformat(),
        "age_days": _age_days(ctx.today, outcome.observation_date),
        # A named-contract board crush is quoted in three different native
        # units (cents/bu, $/short ton, cents/lb), so USD/MT is the only honest
        # statement of it — the same rule the generic engine keeps.
        "margin_home": None,
        "provisional": market.crush.provisional,
        "provisional_note": None,
        "contract_basis": outcome.contract_basis.value,
        "legs_named": True,
        "contract_note": None,
        # `key` and `usd_mt` are the generic engine's names for the same two
        # facts, kept so one template renders both shapes; the contract fields
        # beside them are what only a named leg can answer.
        "legs": {
            leg.role: {**leg.to_dict(), "key": leg.symbol, "usd_mt": leg.usd_per_mt}
            for leg in outcome.legs
        },
    })
    return STATE_OK, "", data


# ---------------------------------------------------------------------------
# 04 Basis
# ---------------------------------------------------------------------------
def basis_block(market: Market, ctx: SiteContext, *, markets: dict[str, Market], **_):
    source = market.basis
    if source.reference != REFERENCE_MARKET:
        return STATE_EMPTY, f"unknown basis reference {source.reference!r}", {}

    board_market = markets.get(REFERENCE_MARKET)
    board_kind = board_market.price.quote_kind if board_market and board_market.price else None
    reference = dict(ctx.reference_series(markets))
    if not reference:
        return STATE_EMPTY, "the CBOT reference leg has no rows to strike a basis against", {}

    key = source.headline_key or source.keys[0]
    rows = ctx.series(source).get(key) or []
    history: list[tuple[date, float, float, float]] = []
    for when, value, _ in rows:
        board = reference.get(when)
        if board is None:
            continue  # M3: no cross-day joins — a basis is one session's number
        local = source.to_usd_mt(value, key, ctx.fx_on(market.currency_pair, when))
        if local is None:
            continue
        history.append((when, local, board, local - board))

    if not history:
        return STATE_EMPTY, (
            f"{source.label or key} and the CBOT board share no session — the basis "
            "is only defined where both printed"
        ), {}

    when, local, board, basis = history[-1]
    spreads = [row[3] for row in history]
    return STATE_OK, "", {
        "label": source.label or key,
        "basis_usd_mt": basis,
        "local_usd_mt": local,
        "board_usd_mt": board,
        # Both legs name their kind, like the ledger's Kind column. This is the
        # other block that prints two USD/MT figures side by side, and they are
        # rarely the same animal — an AMS cash bid or an Argentine decreed
        # minimum against a delayed CBOT close. The block header stamps one kind
        # (block 01's case) and cannot label two, so each card carries its own.
        # The label carries the price type as well as the venue: the CBOT leg is
        # a delayed daily bar, not the exchange's settlement, and the card is
        # where a reader would otherwise assume otherwise.
        "local_quote_kind": source.quote_kind,
        "local_kind_label": quote_kind_label(source.quote_kind) if source.quote_kind else None,
        "board_quote_kind": board_kind,
        "board_kind_label": quote_kind_label(board_kind) if board_kind else None,
        "direction": "premium" if basis > 0 else "discount",
        "as_of": when.isoformat(),
        "age_days": _age_days(ctx.today, when),
        "n_obs": len(history),
        # M19 #222: a spread no cargo can close is not a basis a trader can
        # work, and the difference is invisible in the number. India's mandi
        # bean prints ~+66% over CBOT behind a GM import ban; the caveat rides
        # with the figure rather than living in a comment nobody renders.
        "arbitrage": source.arbitrage,
        "caveat": source.caveat,
        # A level with no range behind it can mislead; where there is too
        # little history to say, the block says that instead of inventing one.
        "avg": sum(spreads) / len(spreads) if len(spreads) >= 20 else None,
        "low": min(spreads) if len(spreads) >= 20 else None,
        "high": max(spreads) if len(spreads) >= 20 else None,
        "history_note": None if len(spreads) >= 20 else (
            f"history building — {len(spreads)} common session(s); range shown at 20"
        ),
    }


# ---------------------------------------------------------------------------
# 05 Currency
# ---------------------------------------------------------------------------
def currency_block(market: Market, ctx: SiteContext, **_) -> tuple[str, str, dict]:
    rows = ctx.fx_series(market.currency_pair)
    if not rows:
        return STATE_EMPTY, f"no {market.currency_pair} rows in the currencies layer", {}

    when, rate = rows[-1]
    changes = {}
    for back in CHANGE_LOOKBACK_OBS:
        prior = rows[-1 - back][1] if len(rows) > back else None
        changes[f"chg_{back}_pct"] = _pct_change(rate, prior)
    return STATE_OK, "", {
        "pair": market.currency_pair,
        "usd_per_home": rate,
        "home_per_usd": (1.0 / rate) if rate else None,
        "home_currency": market.home_currency,
        "as_of": when.isoformat(),
        "age_days": _age_days(ctx.today, when),
        **changes,
        # M3 #145: the USD figure carries an FX timestamp of its own. A Dalian
        # settle converts at a rate struck hours later — same-day comparable,
        # never simultaneous, and the page says so rather than implying it.
        "conversion_note": (
            f"every USD/MT figure on this page is struck at this rate. The rate and the "
            f"{market.name} print are same-day, not simultaneous — this stack stores no "
            "time of day."
        ),
    }


# ---------------------------------------------------------------------------
# 06 Weather
# ---------------------------------------------------------------------------
def weather_block(market: Market, ctx: SiteContext, **_) -> tuple[str, str, dict]:
    regions = []
    for region in market.weather_regions:
        rows = _weather_rows(ctx, region)
        if not rows:
            continue
        when, temp_max, temp_min, precip = rows[-1]
        recent = [row[3] for row in rows[-7:] if row[3] is not None]
        regions.append({
            "region": region,
            # Why this pin is on this page (M14 #207) — "import origin",
            # "rapeseed, not soy". Without it Europe's pins read as soy.
            "role": market.weather_roles[region],
            "as_of": when.isoformat(),
            "age_days": _age_days(ctx.today, when),
            "temp_max": temp_max,
            "temp_min": temp_min,
            "precip_mm": precip,
            "precip_7d_mm": sum(recent) if recent else None,
            "alert": weather_alert(temp_max, precip),
            # None when the crop is in the ground. The card renders either way.
            "season_note": out_of_season_note(region, ctx.today),
        })
    if not regions:
        return STATE_EMPTY, (
            "the weather layer holds no rows for "
            + ", ".join(market.weather_regions)
        ), {}
    return STATE_OK, "", {
        "regions": regions,
        "alerts": [r for r in regions if r["alert"]],
    }


def _weather_rows(ctx: SiteContext, region: str):
    def build():
        if ctx.conn is None:
            return []
        cutoff = (ctx.today - _days(120)).isoformat()
        try:
            rows = ctx.conn.execute(
                "SELECT Date, temp_max, temp_min, precipitation FROM weather "
                "WHERE region = ? AND Date >= ? ORDER BY Date",
                (region, cutoff),
            ).fetchall()
        except Exception as exc:  # noqa: BLE001
            log.debug("block read: weather unavailable (%s)", exc)
            return []
        out = []
        for raw_date, tmax, tmin, precip in rows:
            parsed = _parse_date(raw_date)
            if parsed is not None:
                out.append((parsed, tmax, tmin, precip))
        return out
    return ctx.cached(("weather", region), build)


_MONTH_NAMES = (
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
)


def out_of_season_note(region: str, today: date) -> str | None:
    """"out of season — planting ~Oct", or None when the crop is in the ground.

    M14 #207 chose a tag over hiding the card: September Iowa rain is real and
    prices harvest logistics, so the reading stays and the tag prices it. The
    calendar is declared planting-first, so ``months[0]`` names the month.

    A region with no calendar entry returns None rather than guessing a
    season — but nothing should reach that branch: the calendar's keys are
    pinned to GROWING_REGIONS by test.
    """
    months = config.WEATHER_GROWING_SEASON_MONTHS.get(region)
    if not months or today.month in months:
        return None
    return f"out of season — planting ~{_MONTH_NAMES[months[0] - 1]}"


def weather_alert(temp_max, precip) -> str | None:
    """Same thresholds and same precedence as the briefing's weather section."""
    if temp_max is not None and temp_max > config.WEATHER_EXTREME_HEAT_C:
        return "Extreme heat"
    if precip is not None and precip > config.WEATHER_HEAVY_RAIN_MM:
        return "Heavy rain"
    if precip is not None and precip < config.WEATHER_DRY_THRESHOLD_MM:
        return "Dry"
    return None


# ---------------------------------------------------------------------------
# 07 Supply & demand
# ---------------------------------------------------------------------------
def supply_demand_block(market: Market, ctx: SiteContext, **_) -> tuple[str, str, dict]:
    rows = _psd_rows(ctx, market.psd_country)
    flows = _flows(market, ctx)
    if not rows and not flows:
        return STATE_EMPTY, (
            f"the PSD layer holds no soybean balance sheet for {market.psd_country}"
        ), {}

    lines = []
    if rows:
        years = sorted({year for _, year, _, _ in rows})
        current, prior = years[-1], (years[-2] if len(years) > 1 else None)
        by_attr = {(attr, year): (value, unit) for attr, year, value, unit in rows}
        for attribute in PSD_ATTRIBUTES:
            now = by_attr.get((attribute, current))
            if now is None:
                continue
            was = by_attr.get((attribute, prior)) if prior else None
            lines.append({
                "attribute": attribute,
                "value": now[0],
                "unit": now[1],
                "yoy_pct": _pct_change(now[0], was[0] if was else None),
            })

    return STATE_OK, "", {
        "country": market.psd_country,
        "marketing_year": (sorted({y for _, y, _, _ in rows})[-1] if rows else None),
        "lines": lines,
        # Annual, and it says so: M10 #151 keeps non-daily series in their own
        # cadence-stamped context, never as a row that reads like an outage.
        "cadence_note": "USDA PSD, annual marketing years — this does not move daily",
        "flows": flows,
    }


def _psd_rows(ctx: SiteContext, country: str | None):
    if not country:
        return []

    def build():
        if ctx.conn is None:
            return []
        try:
            return [
                (attr, int(year), float(value), unit)
                for attr, year, value, unit in ctx.conn.execute(
                    "SELECT attribute, year, value, unit FROM psd "
                    "WHERE country = ? AND commodity = 'Soybeans' AND value IS NOT NULL",
                    (country,),
                ).fetchall()
            ]
        except Exception as exc:  # noqa: BLE001
            log.debug("block read: psd unavailable (%s)", exc)
            return []
    return ctx.cached(("psd", country), build)


def _flows(market: Market, ctx: SiteContext) -> dict | None:
    """The market's own physical-flow leg, where the registry names one.

    South Africa is the case this exists for: SAFEX is licence-capped to a
    last-traded print while SAGIS grants reproduction with acknowledgement, so
    the SA story is tonnage, not price (#157 → #202). The attribution is not
    optional and travels with the numbers.
    """
    source = market.flows
    if source is None:
        return None
    series = ctx.series(source)
    lines = []
    for key in source.keys:
        rows = series.get(key) or []
        if not rows:
            continue
        when, value, _ = rows[-1]
        lines.append({
            "key": key,
            "value": value,
            "as_of": when.isoformat(),
            "age_days": _age_days(ctx.today, when),
        })
    if not lines:
        return None
    return {
        "label": source.label or "Producer deliveries",
        "unit": "MT",
        "cadence": source.cadence,
        "lines": lines,
        "attribution": getattr(config, "SAGIS_ATTRIBUTION", None)
        if source.layer == "sagis" else None,
    }


# ---------------------------------------------------------------------------
# 08 News — reserved
# ---------------------------------------------------------------------------
def news_block(market: Market, ctx: SiteContext, **_) -> tuple[str, str, dict]:
    """Never ``ok``: the slot exists so the layout is right when it is filled.

    Choosing sources, licences, parsing and dedupe is a separate map (out of
    scope on #142). ``skeleton_blocks`` already renders this absent from the
    registry's own reason; this builder exists so the nine-builder set is
    complete and the reason has exactly one home.
    """
    del market, ctx
    return STATE_ABSENT, "reserved — no news layer is ingested (out of scope for map #142)", {}


# ---------------------------------------------------------------------------
# 09 Players
# ---------------------------------------------------------------------------
def players_block(market: Market, ctx: SiteContext, **_) -> tuple[str, str, dict]:
    entries = [
        entry for entry in _players(ctx)
        if entry.get("country") == market.players_country
    ]
    if not entries:
        return STATE_EMPTY, (
            f"the players knowledge base holds no entry for {market.players_country}"
        ), {}

    roles: dict[str, int] = {}
    for entry in entries:
        for role in entry.get("roles") or []:
            roles[role] = roles.get(role, 0) + 1
    return STATE_OK, "", {
        "country": market.players_country,
        "count": len(entries),
        "roles": sorted(roles.items(), key=lambda item: (-item[1], item[0])),
        "names": sorted(entry["name"] for entry in entries if entry.get("name")),
        "href": "players.html",
    }


def _players(ctx: SiteContext) -> list[dict]:
    def build():
        try:
            from scripts.generate_players import load_players
            return load_players()
        except Exception:  # noqa: BLE001
            log.warning("players knowledge base unreadable", exc_info=True)
            return []
    return ctx.cached("players", build)


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------
BUILDERS: dict[str, Callable[..., tuple[str, str, dict]]] = {
    "price": price_block,
    "ledger": ledger_block,
    "crush": crush_block,
    "basis": basis_block,
    "currency": currency_block,
    "weather": weather_block,
    "supply_demand": supply_demand_block,
    "news": news_block,
    "players": players_block,
}


def build_blocks(
    market: Market,
    tier: TierResult | None,
    ctx: SiteContext,
    *,
    markets: dict[str, Market],
    block_ids: tuple[str, ...] = BLOCK_IDS,
) -> list[Block]:
    """Every block for one market, in the contract's fixed order.

    Absent is decided before any builder runs, from the registry's own reason:
    "no source exists for this market" is a fact about the descriptor, not
    something to discover by querying an empty table (M1 #143 constraint 2 —
    the distinction between *no source* and *our ingest is broken* is the whole
    point of the two states).
    """
    blocks: list[Block] = []
    for block_id in block_ids:
        absent = absent_reason(market, block_id)
        if absent:
            blocks.append(make_block(block_id, state=STATE_ABSENT, reason=absent))
            continue
        try:
            state, reason, data = BUILDERS[block_id](market, ctx, markets=markets)
        except Exception:  # noqa: BLE001 — M8's block-level isolation
            log.error("block %s failed for %s", block_id, market.slug, exc_info=True)
            state, reason, data = STATE_EMPTY, GENERATION_ERROR, {}
        blocks.append(make_block(
            block_id,
            state=state,
            reason=reason,
            data=data or None,
            kind=_kind(market, block_id),
        ))
    return blocks


def _kind(market: Market, block_id: str) -> str | None:
    if block_id == "price" and market.price is not None:
        return market.price.quote_kind
    if block_id == "crush" and market.crush is not None:
        return market.crush.kind
    return None


__all__ = ["BUILDERS", "SiteContext", "build_blocks", "headline_ledger"]
