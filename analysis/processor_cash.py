"""The US processor cash leg, priced beside the board crush (Layer 29, #352).

What this answers: what does crude soybean oil and 46.5–48% meal cost off a US
processor this week, and how far over the board is that — the processor's cash
premium. What it does not answer is a crush margin: the only US cash bean in
the stack is a CIF NOLA export bid, a different location on a different
cadence (``config.PHYSICAL_CRUSH["cbot"]``), so nothing here is netted against
a bean.

The premium is cash minus board, and invariant 8 decides when it exists. AMS
publishes a basis, which *is* the premium — but over a futures snapshot it does
not name. The snapshot is recoverable: on 2026-10-01 every row's price − basis
equalled our stored CBOT close to the cent (ZLV26 66.97, ZMZ26 353.3), so the
strike session is the session whose stored closes reproduce AMS's own
arithmetic. That is found, never assumed. It moves: Thursday on a normal week,
Friday on the two weeks AMS published late (2026-09-03), so "the day before
publication" would be a guess. When no session reproduces the rows, the board
leg and the premium are withheld with that reason, and the cash ask still
renders — it is AMS's number and needs no board to be true.

Every conversion is ``pipeline.units``; every board number is a
``ContractQuote`` from the same named-contract store the crush above reads.
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import config
from analysis.futures.domain import ContractQuote, NamedContract, named_contract
from pipeline.units import (
    cents_per_lb_to_usd_mt,
    points_to_cents_per_lb,
    usd_per_short_ton_to_usd_mt,
)
from pricing.semantics import quote_kind_label

logger = logging.getLogger(__name__)

STATE_OK = "ok"
STATE_EMPTY = "empty"

# Native price unit → USD/MT. Closed: a unit the table names that is not here
# is a stored row this module cannot state in USD/MT, and it raises.
_TO_USD_MT = {
    "cents_per_lb": cents_per_lb_to_usd_mt,
    "usd_per_short_ton": usd_per_short_ton_to_usd_mt,
}
# Native basis unit → the price's own unit, so price − basis is one quantity.
_BASIS_TO_PRICE_UNIT = {
    "points_per_lb": points_to_cents_per_lb,
    "usd_per_short_ton": lambda value: value,
}
# Half a CBOT tick — the same tolerance the fetcher reconciles on.
_TOLERANCE = {"Soybean Oil": 0.006, "Soybean Meal": 0.06}
_PRODUCT_LABEL = {"Soybean Oil": "Crude soybean oil", "Soybean Meal": "Soybean meal 46.5–48%"}
_PRODUCT_ORDER = {"Soybean Oil": 0, "Soybean Meal": 1}
_FREIGHT_LABEL = {"FOB": "FOB", "Delivered": "delivered"}
# How far before the collection week a strike can sit: AMS's Monday rows can
# carry the prior Friday's close. Measured strikes are Thursday or Friday of
# the week itself; the window is wide on purpose, because the match decides.
_STRIKE_LOOKBACK_DAYS = 3


@dataclass(frozen=True)
class _Leg:
    """One end of a row's ask range, over the contract AMS named for it."""

    row: int
    end: str                      # "low" | "high"
    commodity: str
    contract: NamedContract
    implied: float                # price − basis, native futures units


def _contract_year(month: int, week_start: date) -> int:
    """The listed year of a contract month quoted in the week of ``week_start``.

    AMS prints "December (Z)" with no year. A month at or after the week's is
    this year's, an earlier one next year's (January quoted in December). The
    rule is a hypothesis the strike match tests: a wrong year finds no stored
    close equal to AMS's arithmetic, and the leg is withheld rather than shown.
    """
    return week_start.year if month >= week_start.month else week_start.year + 1


def _newest_week(conn) -> list[dict[str, Any]]:
    cur = conn.execute(
        "SELECT * FROM us_processor_cash "
        "WHERE week_end = (SELECT MAX(week_end) FROM us_processor_cash)"
    )
    columns = [c[0] for c in cur.description]
    return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]


def _has_table(conn) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='us_processor_cash'"
    ).fetchone() is not None


def _legs(
    rows: list[dict[str, Any]], week_start: date, board_commodities: dict[str, str]
) -> list[_Leg]:
    """Every leg AMS priced and based over a named month — the checkable set."""
    legs: list[_Leg] = []
    for i, row in enumerate(rows):
        to_price_unit = _BASIS_TO_PRICE_UNIT.get(row["basis_unit"]) if row["basis_unit"] else None
        if to_price_unit is None or row["price_low"] is None:
            continue
        for end in ("low", "high"):
            month = row[f"futures_month_{end}"]
            basis = row[f"basis_{end}"]
            if month is None or basis is None:
                continue
            board = board_commodities[row["commodity"]]
            legs.append(_Leg(
                row=i,
                end=end,
                commodity=row["commodity"],
                contract=named_contract(board, _contract_year(int(month), week_start), int(month)),
                implied=float(row[f"price_{end}"]) - to_price_unit(float(basis)),
            ))
    return legs


def _closes(provider, contract: NamedContract, until: date) -> dict[date, ContractQuote]:
    return {
        quote.observation_date: quote
        for quote in provider.close_history(contract, as_of=until, sessions=15)
    }


def _strike_session(
    legs: list[_Leg], closes: dict[NamedContract, dict[date, ContractQuote]],
    window: tuple[date, date],
) -> tuple[date | None, str]:
    """The one session whose stored closes reproduce AMS's price − basis.

    Requires a strict majority of checkable legs and a unique best session;
    anything weaker is "not identified", which withholds every board leg.
    """
    if not legs:
        return None, "no row carries both a price and a basis over a named month"
    hits: Counter[date] = Counter()
    for leg in legs:
        for day, quote in closes[leg.contract].items():
            if window[0] <= day <= window[1] and abs(quote.price - leg.implied) <= _TOLERANCE[leg.commodity]:
                hits[day] += 1
    if not hits:
        return None, (
            "no stored CBOT close in the collection week reproduces AMS's price less "
            "its basis — the session AMS struck against is not identified"
        )
    ranked = hits.most_common()
    best, count = ranked[0]
    if len(ranked) > 1 and ranked[1][1] == count:
        return None, "two sessions reproduce AMS's arithmetic equally — the strike is ambiguous"
    if count * 2 <= len(legs):
        return None, (
            f"only {count} of {len(legs)} legs reproduce a stored CBOT close on "
            f"{best.isoformat()} — too few to name the session AMS struck against"
        )
    return best, f"{count} of {len(legs)} legs reproduce the CBOT closes of {best:%a %d %b %Y} exactly"


def cash_leg_panel(conn, market_slug: str, *, today: date, provider=None) -> dict[str, Any] | None:
    """The cash-leg envelope for one market, or None where none is declared.

    ``{state, reason, data}`` like every block: a non-``ok`` state carries the
    reason the reader is owed.
    """
    descriptor = config.CRUSH_CASH_LEGS.get(market_slug)
    if descriptor is None:
        return None

    def withheld(reason: str) -> dict[str, Any]:
        return {"state": STATE_EMPTY, "reason": reason, "data": {"label": descriptor["label"]}}

    if conn is None:
        return withheld("no database connection")
    if not _has_table(conn):
        return withheld("the US processor cash table does not exist in this database")
    rows = _newest_week(conn)
    if not rows:
        return withheld(
            "no AMS 3511 rows are stored — Layer 29 needs MARS_API_KEY and records as "
            "skipped without it"
        )

    week_end = date.fromisoformat(rows[0]["week_end"])
    week_start = date.fromisoformat(rows[0]["week_start"])
    age = (today - week_end).days
    budget = config.LAYER_MAX_DATA_AGE_DAYS[descriptor["layer"]]
    if age > budget:
        return withheld(
            f"the newest AMS 3511 week ended {week_end.isoformat()}, {age} days ago — past "
            f"the layer's {budget}-day budget, so the feed is treated as stopped, not quiet"
        )

    published = rows[0]["published_at"]
    published_day = (
        datetime.fromisoformat(published).date() if published else week_end
    )
    if provider is None:
        from analysis.futures.providers import open_provider

        provider = open_provider(conn)

    rows.sort(key=lambda r: (
        _PRODUCT_ORDER[r["commodity"]], r["freight"] != "FOB", r["location"], r["trans_mode"],
    ))
    legs = _legs(rows, week_start, descriptor["board_commodities"])
    closes = {
        contract: _closes(provider, contract, published_day)
        for contract in {leg.contract for leg in legs}
    }
    window = (week_start - timedelta(days=_STRIKE_LOOKBACK_DAYS), published_day)
    strike, strike_note = _strike_session(legs, closes, window)

    rendered: list[dict[str, Any]] = []
    legs_by_row: dict[int, dict[str, _Leg]] = {}
    for leg in legs:
        legs_by_row.setdefault(leg.row, {})[leg.end] = leg
    for i, row in enumerate(rows):
        convert = _TO_USD_MT.get(row["price_unit"])
        if convert is None:
            raise ValueError(f"us_processor_cash: no USD/MT conversion for {row['price_unit']!r}")
        cash = {
            end: (convert(float(row[f"price_{end}"])) if row[f"price_{end}"] is not None else None)
            for end in ("low", "high", "avg")
        }
        ends: dict[str, dict[str, Any]] = {}
        for end, leg in legs_by_row.get(i, {}).items():
            quote = closes[leg.contract].get(strike) if strike else None
            reconciles = quote is not None and abs(quote.price - leg.implied) <= _TOLERANCE[leg.commodity]
            board = quote.usd_per_mt if reconciles and quote is not None else None
            ask = cash[end]
            premium = ask - board if board is not None and ask is not None else None
            if premium is not None and abs(premium) < 0.005:
                premium = 0.0  # a zero basis must not print "-0.0" off a float residue
            ends[end] = {"contract": leg.contract.symbol, "board_usd_mt": board, "premium_usd_mt": premium}
        withheld_ends = [end for end, e in ends.items() if e["premium_usd_mt"] is None]
        boards: dict[str, float | None] = {}
        for e in ends.values():
            boards.setdefault(e["contract"], e["board_usd_mt"])
        rendered.append({
            "product": _PRODUCT_LABEL[row["commodity"]],
            "location": row["location"],
            "terms": f"{_FREIGHT_LABEL[row['freight']]} · {row['trans_mode'].lower()}",
            "cash_low": cash["low"],
            "cash_high": cash["high"],
            "cash_avg": cash["avg"],
            "board": [{"contract": k, "usd_mt": v} for k, v in boards.items()],
            "premium_low": ends["low"]["premium_usd_mt"] if "low" in ends else None,
            "premium_high": ends["high"]["premium_usd_mt"] if "high" in ends else None,
            "has_basis": bool(ends),
            "premium_note": (
                "AMS printed no basis over a named month" if not ends
                else None if not withheld_ends or not strike
                else "AMS's price less basis does not reproduce the CBOT close on the "
                     "strike session for this row"
            ),
        })

    return {
        "state": STATE_OK,
        "reason": "",
        "data": {
            "label": descriptor["label"],
            "source": descriptor["source"],
            "kind_label": quote_kind_label(descriptor["quote_kind"]),
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "published_at": published,
            "age_days": age,
            "strike_session": strike.isoformat() if strike else None,
            "strike_note": strike_note,
            "rows": rendered,
        },
    }


__all__ = ["cash_leg_panel"]
