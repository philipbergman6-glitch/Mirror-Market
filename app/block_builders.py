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


def _read_series(conn, source: Source, today: date) -> dict[str, list[tuple[date, float, int]]]:
    if conn is None:
        return {key: [] for key in source.keys}
    lookback = LOOKBACK_DAYS_BY_CADENCE.get(source.cadence, 400)
    cutoff = (today - _days(lookback)).isoformat()
    placeholders = ",".join("?" for _ in source.keys)
    sql = (
        f"SELECT {source.key_column}, {source.date_column}, "  # noqa: S608 — identifiers come from the registry
        f"AVG({source.value_column}), COUNT({source.value_column}) "
        f"FROM {source.table} "
        f"WHERE {source.key_column} IN ({placeholders}) "
        f"AND {source.date_column} >= ? AND {source.value_column} IS NOT NULL "
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
    """Deliberately unbuilt: which counterpart legs belong here is undecided.

    M3 #145 settled what a ledger row *is* (a settlement-ordered dual quote
    with a state pill). Which 3–4 counterparts sit under a given market's own
    leg is [M12 #161](https://github.com/philipbergman6-glitch/Mirror-Market/issues/161),
    still open. Picking a set here would answer a decision ticket inside a
    build ticket and bury the answer in a template, so the block states the
    dependency instead — an honest empty state is the contract's own provision
    for exactly this.
    """
    del market, ctx
    return STATE_EMPTY, "counterpart legs are not chosen yet — blocked on M12 #161", {}


# ---------------------------------------------------------------------------
# 03 Crush
# ---------------------------------------------------------------------------
def crush_block(market: Market, ctx: SiteContext, **_) -> tuple[str, str, dict]:
    crush = market.crush
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
    }


# ---------------------------------------------------------------------------
# 04 Basis
# ---------------------------------------------------------------------------
def basis_block(market: Market, ctx: SiteContext, *, markets: dict[str, Market], **_):
    source = market.basis
    if source.reference != REFERENCE_MARKET:
        return STATE_EMPTY, f"unknown basis reference {source.reference!r}", {}

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
            "as_of": when.isoformat(),
            "age_days": _age_days(ctx.today, when),
            "temp_max": temp_max,
            "temp_min": temp_min,
            "precip_mm": precip,
            "precip_7d_mm": sum(recent) if recent else None,
            "alert": _weather_alert(temp_max, precip),
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


def _weather_alert(temp_max, precip) -> str | None:
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
        absent = absent_reason(market, block_id, tier)
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


__all__ = ["BUILDERS", "SiteContext", "build_blocks"]
