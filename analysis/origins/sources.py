"""Reading origin quotes out of the pipeline database (Phase 2).

This is the only module in ``analysis/origins`` that knows SQL exists.
Everything above it works on :class:`~analysis.origins.domain.OriginQuote`
values that are already USD/MT, already window-stamped and already carrying the
FX observation they were converted at.

**Why it resolves ``config.MARKETS`` rather than importing ``app.markets``.**
The site registry's typed view lives in ``app/`` because it computes page tiers
from the database, and ``app`` imports ``analysis``. Reaching back the other
way would close a cycle. So ``config.ORIGIN_LEGS`` names a (market, block, key)
triple and this module reads the *same* descriptor dict the site reads — one
declaration, two consumers, no restatement. ``tests/test_origin_sources.py``
pins the conversion against ``app.markets.Source.to_usd_mt`` so the two readings
of that shared descriptor cannot drift.

**Windows are parsed, not assumed.** Two of the three ingested origins publish
a shipment period and they publish it differently:

* AMS report 3147 uses delivery slots — ``Current``, ``Oct``, ``Nov¹``,
  ``Nov²`` — where the superscript marks a half-month and the year is implied
  by the report date.
* MAGyP publishes explicit ``ship_from``/``ship_to`` month bounds per row, so
  one circular carries a whole forward curve of administered minimums.
* AgRural publishes a port-side level with no period at all.

That third case is the important one. It is tempting to treat an undated FOB
assessment as "prompt" and rank it against October loading; the level is real
and the temptation is strong precisely because the number looks fine. It is
returned with ``shipment_window=None`` and the comparison refuses to rank it.
"""

from __future__ import annotations

import calendar
import logging
import re
import sqlite3
from datetime import date, datetime, timedelta

import config
from analysis.origins.domain import (
    USD,
    Carrier,
    ContractRef,
    FxObservation,
    Grade,
    Incoterm,
    OriginQuote,
    Port,
    QuoteKind,
    ShipmentWindow,
    SourceRef,
    usd_mt,
)
from pipeline.units import to_metric_tons

log = logging.getLogger(__name__)

# How far back a quote read looks. Long enough that a fortnight-dark scraper
# still produces a (stale, labelled) row rather than vanishing — an origin that
# disappears from the board reads as an origin that stopped existing.
LOOKBACK_DAYS = 120

_MONTHS = {
    name.lower(): number
    for number, name in enumerate(calendar.month_abbr)
    if name
}

# AMS half-month markers. The superscripts are what the PDF prints; the ASCII
# digits are what a re-parse or a hand-entered fixture is likely to use, and
# accepting both costs nothing while rejecting one silently drops half the slots.
_HALF_MARKERS = {"¹": 1, "²": 2, "1": 1, "2": 2}
_AMS_SLOT_RE = re.compile(r"^(?P<month>[A-Za-z]{3})(?P<half>[¹²12])?$")


class OriginSourceError(RuntimeError):
    """A registry leg cannot be resolved. Raised, never rendered as an empty row."""


# ---------------------------------------------------------------------------
# Registry resolution
# ---------------------------------------------------------------------------
def _descriptor(leg_id: str) -> tuple[dict, dict, dict]:
    """``(leg, market, source)`` for one origin leg, hard-failing on a typo.

    An unresolvable leg would render as a market that has not printed, which is
    the single most misleading statement this surface can make by accident —
    the same reason ``app.markets._ledger_leg`` fails at load rather than at
    render.
    """
    leg = config.ORIGIN_LEGS.get(leg_id)
    if leg is None:
        raise OriginSourceError(
            f"origin leg {leg_id!r} is not in config.ORIGIN_LEGS "
            f"(known: {sorted(config.ORIGIN_LEGS)})"
        )
    if leg.get("absent_reason"):
        raise OriginSourceError(f"origin leg {leg_id!r} declares no price source")
    market = config.MARKETS.get(leg["market"])
    if market is None:
        raise OriginSourceError(f"origin leg {leg_id!r} names unregistered market {leg['market']!r}")
    source = market.get(leg["block"])
    if source is None:
        raise OriginSourceError(
            f"origin leg {leg_id!r} points at {leg['market']}.{leg['block']}, which has no source"
        )
    if leg["key"] not in source["keys"]:
        raise OriginSourceError(
            f"origin leg {leg_id!r} names key {leg['key']!r}, not one of {list(source['keys'])}"
        )
    return leg, market, source


def port_for(key: str, *, role: str = "origin") -> Port:
    catalog = config.ORIGIN_PORTS if role == "origin" else config.DESTINATION_PORTS
    raw = catalog.get(key)
    if raw is None:
        raise OriginSourceError(f"unknown {role} port {key!r} (known: {sorted(catalog)})")
    return Port(
        key=key,
        name=raw["name"],
        country=raw["country"],
        country_iso=raw["country_iso"],
        role=role,
    )


def to_usd_per_mt(value: float, *, unit: str, key: str, fx: float | None) -> float | None:
    """The site's four conversions, restated for the one consumer that cannot import them.

    Kept byte-for-byte equivalent to ``app.markets.Source.to_usd_mt`` and pinned
    against it by test, rather than approximated: a bushels-per-tonne factor
    that drifts between two copies produces a 2% error that looks like basis.
    """
    if value is None:
        return None
    if unit == "usd_per_mt":
        return float(value)
    if unit == "native_exchange":
        return to_metric_tons(float(value), key)
    if unit == "usd_per_bushel":
        return to_metric_tons(float(value) * 100.0, key)
    if unit == "home_per_mt":
        if not fx:
            return None
        return float(value) * float(fx)
    return None


# ---------------------------------------------------------------------------
# Window parsing
# ---------------------------------------------------------------------------
def _month_bounds(year: int, month: int) -> tuple[date, date]:
    last = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last)


def parse_ams_slot(slot: str, report_date: date) -> ShipmentWindow | None:
    """One AMS 3147 delivery slot as a dated window, or ``None`` if unreadable.

    ``Current`` is the balance of the report's own month — a prompt barge, not
    a forward one. A named month is the *next* occurrence of that month at or
    after the report month, which is how a December slot on an August report
    resolves to this December and a January slot resolves to next January.
    """
    text = (slot or "").strip()
    if not text:
        return None
    if text.lower() in {"current", "spot", "prompt"}:
        _, end = _month_bounds(report_date.year, report_date.month)
        return ShipmentWindow(report_date, end, label="Current (prompt)")
    match = _AMS_SLOT_RE.match(text)
    if match is None:
        log.debug("AMS slot %r is not a recognised delivery period", slot)
        return None
    month = _MONTHS.get(match.group("month").lower())
    if month is None:
        return None
    year = report_date.year if month >= report_date.month else report_date.year + 1
    start, end = _month_bounds(year, month)
    half = _HALF_MARKERS.get(match.group("half") or "")
    if half == 1:
        end = date(year, month, 15)
        label = f"{calendar.month_abbr[month]} {year} 1st half"
    elif half == 2:
        start = date(year, month, 16)
        label = f"{calendar.month_abbr[month]} {year} 2nd half"
    else:
        label = f"{calendar.month_abbr[month]} {year}"
    return ShipmentWindow(start, end, label=label)


def parse_magyp_window(ship_from: str | None, ship_to: str | None) -> ShipmentWindow | None:
    """MAGyP's ``YYYY-MM`` shipment bounds as a closed window.

    ``ship_to`` is inclusive of its whole month — the circular's band runs to
    the end of the named month, not to its first day. Reading it as a day would
    shrink an eleven-month administered band to a single date and would exclude
    every window a trader actually asks about.
    """
    if not ship_from:
        return None
    try:
        start_year, start_month = (int(part) for part in str(ship_from).split("-")[:2])
    except (TypeError, ValueError):
        return None
    start, start_end = _month_bounds(start_year, start_month)
    if not ship_to:
        return ShipmentWindow(start, start_end, label=str(ship_from))
    try:
        end_year, end_month = (int(part) for part in str(ship_to).split("-")[:2])
    except (TypeError, ValueError):
        return ShipmentWindow(start, start_end, label=str(ship_from))
    _, end = _month_bounds(end_year, end_month)
    if end < start:
        return None
    label = str(ship_from) if str(ship_from) == str(ship_to) else f"{ship_from}..{ship_to}"
    return ShipmentWindow(start, end, label=label)


# ---------------------------------------------------------------------------
# Database reads
# ---------------------------------------------------------------------------
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


def fx_on(conn, pair: str | None, when: date) -> FxObservation | None:
    """The ``<CCY>/USD`` rate on ``when``, else the newest one before it.

    A price dated D converted at today's rate is a different number from the
    same price converted at D's rate, and the difference is an FX move being
    reported as a market move. Same rule the ledger keeps.
    """
    if not pair or conn is None:
        return None
    try:
        row = conn.execute(
            "SELECT Date, Close FROM currencies WHERE pair = ? AND Date <= ? "
            "AND Close IS NOT NULL ORDER BY Date DESC LIMIT 1",
            (pair, when.isoformat()),
        ).fetchone()
    except sqlite3.Error as exc:
        log.debug("origin fx read failed for %s: %s", pair, exc)
        return None
    if not row or row[1] is None:
        return None
    observed = _parse_date(row[0])
    if observed is None:
        return None
    return FxObservation(pair=pair, usd_per_unit=float(row[1]), observed_on=observed)


def _latest_observation_date(conn, table: str, date_column: str, key_column: str, key: str) -> date | None:
    try:
        row = conn.execute(
            f"SELECT MAX({date_column}) FROM {table} WHERE {key_column} = ?",  # noqa: S608 - registry identifiers
            (key,),
        ).fetchone()
    except sqlite3.Error as exc:
        log.debug("origin read: %s unavailable (%s)", table, exc)
        return None
    return _parse_date(row[0]) if row else None


def read_origin_quotes(conn, leg_id: str, *, today: date) -> tuple[OriginQuote, ...]:
    """Every quote for one origin leg on its most recent observation date.

    One date, several windows: that is the shape of all three sources. AMS
    prints seven delivery slots on a report date, MAGyP three or four shipment
    bands on a circular, AgRural exactly one undated level. Returning the whole
    latest session lets the comparison pick the row whose window the trader
    asked about, instead of picking "the first one" and hoping.
    """
    leg, market, source = _descriptor(leg_id)
    if conn is None:
        return ()

    table = source["table"]
    date_column = source["date_column"]
    key_column = source["key_column"]
    value_column = source["value_column"]
    unit = source["unit"]
    key = leg["key"]

    latest = _latest_observation_date(conn, table, date_column, key_column, key)
    if latest is None or (today - latest).days > LOOKBACK_DAYS:
        return ()

    scheme = leg.get("window_scheme", "none")
    extra: list[str] = []
    if scheme == "ams_delivery":
        extra.append(leg["window_column"])
    elif scheme == "magyp_months":
        extra.extend(leg["window_columns"])
    if leg.get("contract_column"):
        extra.append(leg["contract_column"])

    columns = ", ".join([value_column, *extra]) if extra else value_column
    sql = (
        f"SELECT {columns} FROM {table} "  # noqa: S608 - identifiers come from the registry
        f"WHERE {key_column} = ? AND {date_column} = ? AND {value_column} IS NOT NULL"
    )
    try:
        rows = conn.execute(sql, (key, latest.isoformat())).fetchall()
    except sqlite3.Error as exc:
        log.warning("origin read failed for %s: %s", leg_id, exc)
        return ()
    if not rows:
        return ()

    fx = fx_on(conn, market.get("currency_pair"), latest)
    port = port_for(leg["port"])
    grade = Grade(product="soybeans", specification=leg.get("grade"))
    max_age = config.LAYER_MAX_DATA_AGE_DAYS.get(source["layer"], 14)

    quotes: list[OriginQuote] = []
    for row in rows:
        native = float(row[0])
        cursor = 1
        window: ShipmentWindow | None = None
        if scheme == "ams_delivery":
            window = parse_ams_slot(row[cursor], latest)
            cursor += 1
        elif scheme == "magyp_months":
            window = parse_magyp_window(row[cursor], row[cursor + 1])
            cursor += 2
        contract = None
        if leg.get("contract_column"):
            contract = _contract_ref(leg, row[cursor], latest)

        usd = to_usd_per_mt(
            native, unit=unit, key=key, fx=fx.usd_per_unit if fx else None
        )
        if usd is None:
            # A home-currency leg with no rate for its own day renders nothing
            # rather than the local number relabelled — the same rule the site's
            # single conversion site keeps.
            log.info(
                "origin %s: %s on %s has no %s rate — quote dropped rather than "
                "published in the wrong currency",
                leg_id, key, latest.isoformat(), market.get("currency_pair"),
            )
            continue

        quotes.append(
            OriginQuote(
                origin=port,
                grade=grade,
                quote_kind=QuoteKind(source["quote_kind"]),
                incoterm=Incoterm(leg["incoterm"]),
                carrier=Carrier(leg["carrier"]),
                price=usd_mt(usd),
                native_price=native,
                native_currency=(
                    USD if unit in ("usd_per_mt", "usd_per_bushel", "native_exchange")
                    else market["home_currency"]
                ),
                native_unit=unit,
                observation_date=latest,
                # No source in this stack publishes a separate publication
                # timestamp; the observation date is what every one of them
                # stamps. Asserting a publication date we do not have would be
                # the fabrication this phase is built to avoid.
                publication_date=None,
                source=SourceRef(
                    layer=source["layer"],
                    table=table,
                    key=key,
                    detail=leg["label"],
                    href=f"markets/{leg['market']}.html",
                ),
                shipment_window=window,
                fx=fx if unit == "home_per_mt" else None,
                board_contract=contract,
                quotes_averaged=1,
                max_age_days=max_age,
                notes=_leg_notes(leg, scheme),
            )
        )
    return tuple(quotes)


def _contract_ref(leg: dict, raw_month, observed: date) -> ContractRef | None:
    """AMS names the CBOT contract its basis is quoted over, as a month number."""
    if raw_month in (None, ""):
        return None
    try:
        month = int(raw_month)
    except (TypeError, ValueError):
        return None
    if not 1 <= month <= 12:
        return None
    year = observed.year if month >= observed.month else observed.year + 1
    return ContractRef(
        exchange=leg.get("hedge_exchange", "cbot"),
        code=leg.get("hedge_code", "ZS"),
        delivery_month=f"{year:04d}-{month:02d}",
    )


def _leg_notes(leg: dict, scheme: str) -> tuple[str, ...]:
    if scheme == "none":
        return (
            f"{leg['label']} publishes no shipment period — the level is a port-side "
            "assessment, so it cannot be said to be quoted for any particular month",
        )
    return ()


def offered_windows(today: date, *, months_ahead: int | None = None) -> tuple[ShipmentWindow, ...]:
    """The shipment windows the page offers, derived from the run date.

    Generated rather than configured so the selector can never offer a month
    that has already sailed. The first entry is the balance of the current
    month (prompt); the rest are whole forward months.
    """
    months = months_ahead if months_ahead is not None else config.ORIGIN_WINDOW_MONTHS_AHEAD
    _, current_end = _month_bounds(today.year, today.month)
    windows = [ShipmentWindow(today, current_end, label="Prompt (balance of month)")]
    cursor = current_end + timedelta(days=1)
    for _ in range(months):
        start, end = _month_bounds(cursor.year, cursor.month)
        windows.append(
            ShipmentWindow(start, end, label=f"{calendar.month_abbr[start.month]} {start.year}")
        )
        cursor = end + timedelta(days=1)
    return tuple(windows)


__all__ = [
    "LOOKBACK_DAYS",
    "OriginSourceError",
    "fx_on",
    "offered_windows",
    "parse_ams_slot",
    "parse_magyp_window",
    "port_for",
    "read_origin_quotes",
    "to_usd_per_mt",
]
