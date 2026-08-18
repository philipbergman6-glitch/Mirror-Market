"""Position and P&L workspace (Phase 3).

Positions are **entered by the user**, never inferred. This project ingests no
account, no broker statement and no clearing feed, so the only honest source of
a position is a file the trader wrote or exported. Two are supported:

* ``data/reference/positions/*.yml`` — hand-maintained, versioned, reviewable.
  Same shape and same reasoning as ``data/reference/assumptions``: a missing
  directory is an empty book, which is a legitimate state.
* a CSV export — :func:`positions_from_csv`, for a broker statement.

What the workspace computes:

* **Average cost and realised P&L from fills**, on a weighted-average-cost
  basis, when the position is entered as a list of fills. A position entered as
  a bare quantity and average price is taken at its word and its realised P&L
  is whatever the file says — with a note, because a number the software did
  not derive should not look like one it did.
* **Mark and unrealised P&L** from the named-contract quote for a futures
  position, and from the flat price (board + basis) for a physical one.
* **Attribution** of a physical position's unrealised P&L into futures, basis
  and FX components — but only where the file recorded the entry-time futures
  level, basis and rate. Without those three the decomposition is arithmetic
  over unknowns, so it is withheld and the reason is given.
* **Limits**, checked and reported, never enforced. This software does not
  stop anyone doing anything; it says which line was crossed.

The mark is a delayed reference price. Every P&L figure here inherits that and
is labelled with it — a mark-to-market on unproven settlements is a management
number, not a margin call.
"""

from __future__ import annotations

import csv
import logging
import os
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Any

from analysis.futures.domain import (
    METHOD_VERSION,
    ContractQuote,
    NamedContract,
    Side,
    UnknownContract,
    parse_symbol,
    spec_for,
)
from analysis.futures.hedge import PhysicalUnit, to_metric_tons

log = logging.getLogger(__name__)


class PositionError(ValueError):
    """A position file said something that cannot be true."""


class BookKind(str, Enum):
    PHYSICAL = "physical"
    FUTURES = "futures"


@dataclass(frozen=True)
class Fill:
    """One executed trade the user recorded."""

    trade_date: date
    side: Side
    quantity: float          # contracts for futures, MT for physical
    price: float             # native units for futures, USD/MT for physical
    reference: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "trade_date": self.trade_date.isoformat(),
            "side": self.side.value,
            "quantity": self.quantity,
            "price": self.price,
            "reference": self.reference,
        }


@dataclass(frozen=True)
class LotResult:
    """The outcome of running fills through weighted-average cost."""

    net_quantity: float      # signed: positive long, negative short
    average_cost: float | None
    realised: float          # in price units x quantity units
    derived: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "net_quantity": round(self.net_quantity, 6),
            "average_cost": None if self.average_cost is None else round(self.average_cost, 6),
            "realised": round(self.realised, 6),
            "derived": self.derived,
        }


def run_weighted_average(fills: Iterable[Fill]) -> LotResult:
    """Weighted-average cost through a sequence of fills, oldest first.

    Chosen over FIFO deliberately and stated on the page: for a futures book
    the two differ only in how realised and unrealised split, never in the
    total, and weighted average is the convention a commodity merchant's own
    books almost always use. A closing fill realises against the running
    average; a fill that crosses through flat closes the old side at the
    average and opens the new side at the fill price, which is the only
    treatment that leaves no phantom cost basis behind.
    """
    net = 0.0
    average: float | None = None
    realised = 0.0

    for fill in sorted(fills, key=lambda f: f.trade_date):
        signed = fill.side.sign * abs(fill.quantity)
        if net == 0 or (net > 0) == (signed > 0):
            total = net + signed
            if total == 0:
                net, average = 0.0, None
                continue
            average = (
                fill.price if average is None
                else (average * abs(net) + fill.price * abs(signed)) / abs(total)
            )
            net = total
            continue

        closing = min(abs(signed), abs(net))
        if average is not None:
            # Closing a long realises fill - average; closing a short realises
            # average - fill. The sign of the position being closed decides.
            realised += closing * (fill.price - average) * (1 if net > 0 else -1)
        remainder = abs(signed) - closing
        net += signed
        if abs(net) < 1e-12:
            net, average = 0.0, None
        elif remainder > 0:
            average = fill.price

    return LotResult(net_quantity=net, average_cost=average, realised=realised)


@dataclass(frozen=True)
class FuturesPosition:
    """A position in one named contract."""

    contract: NamedContract
    fills: tuple[Fill, ...] = ()
    stated_contracts: float | None = None    # signed
    stated_average_price: float | None = None
    stated_realised_usd: float = 0.0
    account: str = ""
    note: str = ""

    @property
    def lot(self) -> LotResult:
        if self.fills:
            return run_weighted_average(self.fills)
        if self.stated_contracts is None:
            raise PositionError(
                f"{self.contract.symbol}: neither fills nor a stated position — nothing to mark"
            )
        return LotResult(
            net_quantity=float(self.stated_contracts),
            average_cost=self.stated_average_price,
            realised=0.0,
            derived=False,
        )

    @property
    def net_contracts(self) -> float:
        return self.lot.net_quantity

    @property
    def net_mt(self) -> float:
        return self.net_contracts * self.contract.spec.mt_per_contract


@dataclass(frozen=True)
class PhysicalPosition:
    """A physical position, in tonnes, priced in USD/MT."""

    commodity: str
    quantity: float
    unit: PhysicalUnit
    side: Side
    average_cost_usd_mt: float | None = None
    currency: str = "USD"
    fx_pair: str | None = None
    #: The three entry-time levels an attribution needs. All or nothing.
    entry_futures_usd_mt: float | None = None
    entry_basis_usd_mt: float | None = None
    entry_fx_rate: float | None = None
    #: Which board leg this physical is marked against, e.g. "ZSX26".
    mark_contract: str | None = None
    current_basis_usd_mt: float | None = None
    location: str = ""
    note: str = ""

    @property
    def quantity_mt(self) -> float:
        return to_metric_tons(self.quantity, self.unit, self.commodity) * (1 if self.side is Side.LONG else -1)

    @property
    def can_attribute(self) -> bool:
        return (
            self.entry_futures_usd_mt is not None
            and self.entry_basis_usd_mt is not None
            and self.current_basis_usd_mt is not None
            and (self.currency == "USD" or self.entry_fx_rate is not None)
        )


@dataclass(frozen=True)
class Limit:
    """One configurable line. Checked and reported; never enforced."""

    key: str                 # net_mt | unhedged_mt | notional_usd | loss_usd
    scope: str               # commodity name or "*"
    maximum: float
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {"key": self.key, "scope": self.scope, "maximum": self.maximum, "note": self.note}


@dataclass(frozen=True)
class LimitBreach:
    limit: Limit
    observed: float
    def to_dict(self) -> dict[str, Any]:
        return {
            **self.limit.to_dict(),
            "observed": round(self.observed, 2),
            "excess": round(abs(self.observed) - self.limit.maximum, 2),
        }


@dataclass(frozen=True)
class MarkedPosition:
    """A position with its mark, its P&L and how much of that is attributable."""

    kind: BookKind
    key: str                       # ZSX26, or "Soybeans @ Paranagua"
    commodity: str
    net_quantity: float            # contracts (futures) or MT signed (physical)
    net_mt: float
    average_cost: float | None
    mark: float | None
    mark_label: str
    mark_date: date | None
    unrealised_usd: float | None
    realised_usd: float
    attribution: dict[str, float | None]
    attribution_note: str
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind.value,
            "key": self.key,
            "commodity": self.commodity,
            "net_quantity": round(self.net_quantity, 4),
            "net_mt": round(self.net_mt, 3),
            "average_cost": None if self.average_cost is None else round(self.average_cost, 4),
            "mark": None if self.mark is None else round(self.mark, 4),
            "mark_label": self.mark_label,
            "mark_date": self.mark_date.isoformat() if self.mark_date else None,
            "unrealised_usd": None if self.unrealised_usd is None else round(self.unrealised_usd, 2),
            "realised_usd": round(self.realised_usd, 2),
            "attribution": {
                k: (None if v is None else round(v, 2)) for k, v in self.attribution.items()
            },
            "attribution_note": self.attribution_note,
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True)
class Book:
    """Everything the user entered, plus the limits they set."""

    physical: tuple[PhysicalPosition, ...] = ()
    futures: tuple[FuturesPosition, ...] = ()
    limits: tuple[Limit, ...] = ()
    loaded_from: tuple[str, ...] = ()

    @property
    def is_empty(self) -> bool:
        return not self.physical and not self.futures


@dataclass(frozen=True)
class BookValuation:
    """The marked book."""

    as_of: date
    positions: tuple[MarkedPosition, ...]
    breaches: tuple[LimitBreach, ...]
    total_unrealised_usd: float
    total_realised_usd: float
    net_mt_by_commodity: dict[str, float]
    mark_note: str
    method_version: str = METHOD_VERSION
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "method_version": self.method_version,
            "positions": [p.to_dict() for p in self.positions],
            "breaches": [b.to_dict() for b in self.breaches],
            "total_unrealised_usd": round(self.total_unrealised_usd, 2),
            "total_realised_usd": round(self.total_realised_usd, 2),
            "net_mt_by_commodity": {k: round(v, 3) for k, v in sorted(self.net_mt_by_commodity.items())},
            "mark_note": self.mark_note,
            "warnings": list(self.warnings),
        }


MARK_NOTE = (
    "Marks are delayed daily closes, not proven exchange settlements. This valuation is a "
    "management figure; it is not a margin calculation and will not match a clearing statement."
)


# ---------------------------------------------------------------------------
# Valuation
# ---------------------------------------------------------------------------


def _mark_futures(position: FuturesPosition, quote: ContractQuote | None) -> MarkedPosition:
    lot = position.lot
    spec = position.contract.spec
    warnings: list[str] = []
    if not lot.derived:
        warnings.append(
            "position entered as a stated quantity and average price — realised P&L is whatever "
            "the file recorded, not something this software derived"
        )
    unrealised: float | None = None
    if quote is None:
        warnings.append(f"no quote for {position.contract.symbol} — cannot mark this position")
    elif lot.average_cost is None:
        warnings.append("no average cost — unrealised P&L cannot be computed")
    else:
        per_contract = spec.value_usd(quote.price) - spec.value_usd(lot.average_cost)
        unrealised = lot.net_quantity * per_contract

    # run_weighted_average returns realised in (native price units x contracts).
    # value_usd is linear in price, so applying it converts the whole quantity
    # in one step and keeps every USD figure on the same conversion route.
    realised_usd = spec.value_usd(lot.realised) if lot.derived else position.stated_realised_usd

    return MarkedPosition(
        kind=BookKind.FUTURES,
        key=position.contract.symbol,
        commodity=spec.name,
        net_quantity=lot.net_quantity,
        net_mt=lot.net_quantity * spec.mt_per_contract,
        average_cost=lot.average_cost,
        mark=quote.price if quote else None,
        mark_label=quote.price_label if quote else "unavailable",
        mark_date=quote.observation_date if quote else None,
        unrealised_usd=unrealised,
        realised_usd=realised_usd,
        attribution={"futures": unrealised, "basis": None, "fx": None},
        attribution_note="a futures position has no basis or FX component by construction",
        warnings=tuple(warnings),
    )


def _mark_physical(
    position: PhysicalPosition,
    quote: ContractQuote | None,
    fx_rate: tuple[date, float] | None,
) -> MarkedPosition:
    warnings: list[str] = []
    quantity_mt = position.quantity_mt
    board_usd_mt = quote.usd_per_mt if quote else None
    basis = position.current_basis_usd_mt

    mark_usd_mt: float | None = None
    if board_usd_mt is None:
        warnings.append(
            f"no quote for {position.mark_contract or position.commodity} — this position is unmarked"
        )
    elif basis is None:
        warnings.append(
            "no current basis recorded — the physical is marked at the board alone, which "
            "understates or overstates it by the whole basis"
        )
        mark_usd_mt = board_usd_mt
    else:
        mark_usd_mt = board_usd_mt + basis

    unrealised: float | None = None
    if mark_usd_mt is not None and position.average_cost_usd_mt is not None:
        unrealised = quantity_mt * (mark_usd_mt - position.average_cost_usd_mt)
    elif position.average_cost_usd_mt is None:
        warnings.append("no average cost recorded — unrealised P&L cannot be computed")

    attribution: dict[str, float | None] = {"futures": None, "basis": None, "fx": None}
    note = (
        "attribution needs the entry-time board level, entry basis and current basis "
        "(plus the entry rate for a non-USD position); one or more is missing"
    )
    if (
        position.can_attribute
        and board_usd_mt is not None
        and basis is not None
        and position.entry_futures_usd_mt is not None
        and position.entry_basis_usd_mt is not None
    ):
        futures_move = board_usd_mt - position.entry_futures_usd_mt
        basis_move = basis - position.entry_basis_usd_mt
        attribution["futures"] = quantity_mt * futures_move
        attribution["basis"] = quantity_mt * basis_move
        if position.currency == "USD":
            attribution["fx"] = 0.0
            note = "decomposed against the recorded entry board level and entry basis"
        elif fx_rate is not None and mark_usd_mt is not None and position.entry_fx_rate is not None:
            _, rate_now = fx_rate
            entry_rate = position.entry_fx_rate
            # The contract value is fixed in the home currency; the USD it
            # converts to moves with the rate. Value it at the mark so the FX
            # component is the exposure that actually exists today.
            home_amount = abs(quantity_mt) * mark_usd_mt / entry_rate if entry_rate else 0.0
            attribution["fx"] = home_amount * (rate_now - entry_rate)
            note = (
                f"decomposed against the recorded entry levels; FX at {entry_rate:.6f} -> "
                f"{rate_now:.6f}"
            )
        else:
            note = (
                f"board and basis decomposed; no current {position.fx_pair} rate, so the FX "
                "component is unquantified"
            )

    key = f"{position.commodity}" + (f" @ {position.location}" if position.location else "")
    return MarkedPosition(
        kind=BookKind.PHYSICAL,
        key=key,
        commodity=position.commodity,
        net_quantity=quantity_mt,
        net_mt=quantity_mt,
        average_cost=position.average_cost_usd_mt,
        mark=mark_usd_mt,
        mark_label=(
            f"{quote.price_label} + basis" if quote and basis is not None
            else (quote.price_label if quote else "unavailable")
        ),
        mark_date=quote.observation_date if quote else None,
        unrealised_usd=unrealised,
        realised_usd=0.0,
        attribution=attribution,
        attribution_note=note,
        warnings=tuple(warnings),
    )


def value_book(
    book: Book,
    *,
    as_of: date,
    quote_for,
    fx_for=None,
) -> BookValuation:
    """Mark every position and check every limit.

    ``quote_for`` is a callable ``(commodity, symbol) -> ContractQuote | None``
    and ``fx_for`` a callable ``pair -> (date, rate) | None``. Callables rather
    than a provider object so this module can be tested without a database, and
    so an authoritative provider substitutes at one call site.
    """
    marked: list[MarkedPosition] = []
    warnings: list[str] = []

    for futures_position in book.futures:
        marked.append(_mark_futures(
            futures_position,
            quote_for(futures_position.contract.spec.name, futures_position.contract.symbol),
        ))

    for physical_position in book.physical:
        if physical_position.mark_contract:
            quote = quote_for(physical_position.commodity, physical_position.mark_contract)
        else:
            warnings.append(
                f"{physical_position.commodity}: no mark contract named, so the physical is "
                "marked against the front month — name one to make the mark reproducible"
            )
            quote = quote_for(physical_position.commodity, None)
        rate = (
            fx_for(physical_position.fx_pair)
            if (fx_for and physical_position.fx_pair) else None
        )
        marked.append(_mark_physical(physical_position, quote, rate))

    net_by_commodity: dict[str, float] = {}
    for entry in marked:
        net_by_commodity[entry.commodity] = net_by_commodity.get(entry.commodity, 0.0) + entry.net_mt

    total_unrealised = sum(p.unrealised_usd or 0.0 for p in marked)
    total_realised = sum(p.realised_usd for p in marked)

    return BookValuation(
        as_of=as_of,
        positions=tuple(marked),
        breaches=check_limits(book.limits, marked, net_by_commodity, total_unrealised),
        total_unrealised_usd=total_unrealised,
        total_realised_usd=total_realised,
        net_mt_by_commodity=net_by_commodity,
        mark_note=MARK_NOTE,
        warnings=tuple(warnings),
    )


def check_limits(
    limits: tuple[Limit, ...],
    marked: list[MarkedPosition],
    net_by_commodity: dict[str, float],
    total_unrealised: float,
) -> tuple[LimitBreach, ...]:
    """Report every crossed line. Reporting only — nothing is prevented."""
    breaches: list[LimitBreach] = []
    for limit in limits:
        if limit.key == "net_mt":
            scopes = net_by_commodity if limit.scope == "*" else {
                limit.scope: net_by_commodity.get(limit.scope, 0.0)
            }
            for _, value in scopes.items():
                if abs(value) > limit.maximum:
                    breaches.append(LimitBreach(limit, value))
        elif limit.key == "unhedged_mt":
            for commodity, value in net_by_commodity.items():
                if limit.scope not in ("*", commodity):
                    continue
                if abs(value) > limit.maximum:
                    breaches.append(LimitBreach(limit, value))
        elif limit.key == "notional_usd":
            exposure = sum(
                abs(p.net_mt) * (p.mark or 0.0) for p in marked if p.kind is BookKind.PHYSICAL
            )
            if exposure > limit.maximum:
                breaches.append(LimitBreach(limit, exposure))
        elif limit.key == "loss_usd":
            if total_unrealised < -abs(limit.maximum):
                breaches.append(LimitBreach(limit, total_unrealised))
        else:
            log.warning("unknown limit key %r — not checked", limit.key)
    return tuple(breaches)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def _side(raw: Any, where: str) -> Side:
    text = str(raw or "").strip().lower()
    if text in ("long", "buy", "b", "l"):
        return Side.LONG
    if text in ("short", "sell", "s"):
        return Side.SHORT
    raise PositionError(f"{where}: side {raw!r} is neither long nor short")


def _date(raw: Any, where: str) -> date:
    try:
        return date.fromisoformat(str(raw)[:10])
    except (TypeError, ValueError) as exc:
        raise PositionError(f"{where}: {raw!r} is not an ISO date") from exc


def _unit(raw: Any, where: str) -> PhysicalUnit:
    try:
        return PhysicalUnit(str(raw or "mt").strip().lower())
    except ValueError as exc:
        raise PositionError(
            f"{where}: unit {raw!r} is not one of {[u.value for u in PhysicalUnit]}"
        ) from exc


def _known_commodity(commodity: str, where: str) -> None:
    """Reject a product we cannot size, as a position error.

    ``spec_for`` raises ``UnknownContract``, which is the right failure but the
    wrong type here: everything that reads a position file catches
    :class:`PositionError` to mean "a file said something that cannot be true",
    and a differently-typed escape from the same cause was being swallowed one
    level up and rendered as an empty book — "nothing entered" standing in for
    "entered wrongly", which is exactly the substitution this module refuses.
    """
    try:
        spec_for(commodity)
    except UnknownContract as exc:
        raise PositionError(f"{where}: {exc}") from exc


def parse_book(payload: dict[str, Any], *, where: str) -> Book:
    """Validate one position document. Every failure is loud."""
    physical: list[PhysicalPosition] = []
    futures: list[FuturesPosition] = []
    limits: list[Limit] = []

    for index, raw in enumerate(payload.get("physical") or ()):
        label = f"{where}: physical[{index}]"
        commodity = str(raw.get("commodity") or "")
        _known_commodity(commodity, label)
        physical.append(PhysicalPosition(
            commodity=commodity,
            quantity=float(raw["quantity"]),
            unit=_unit(raw.get("unit"), label),
            side=_side(raw.get("side"), label),
            average_cost_usd_mt=_optional_float(raw.get("average_cost_usd_mt")),
            currency=str(raw.get("currency") or "USD").upper(),
            fx_pair=raw.get("fx_pair"),
            entry_futures_usd_mt=_optional_float(raw.get("entry_futures_usd_mt")),
            entry_basis_usd_mt=_optional_float(raw.get("entry_basis_usd_mt")),
            entry_fx_rate=_optional_float(raw.get("entry_fx_rate")),
            mark_contract=raw.get("mark_contract"),
            current_basis_usd_mt=_optional_float(raw.get("current_basis_usd_mt")),
            location=str(raw.get("location") or ""),
            note=str(raw.get("note") or ""),
        ))

    for index, raw in enumerate(payload.get("futures") or ()):
        label = f"{where}: futures[{index}]"
        symbol = str(raw.get("contract") or raw.get("symbol") or "")
        try:
            contract = parse_symbol(symbol)
        except Exception as exc:  # noqa: BLE001 — re-raised as a position error
            raise PositionError(f"{label}: {symbol!r} is not a contract symbol ({exc})") from exc
        fills = tuple(
            Fill(
                trade_date=_date(fill.get("date"), f"{label}.fills"),
                side=_side(fill.get("side"), f"{label}.fills"),
                quantity=float(fill["quantity"]),
                price=float(fill["price"]),
                reference=str(fill.get("reference") or ""),
            )
            for fill in (raw.get("fills") or ())
        )
        stated = raw.get("contracts")
        if not fills and stated is None:
            raise PositionError(
                f"{label}: give either `fills` or a stated `contracts` and `average_price`"
            )
        futures.append(FuturesPosition(
            contract=contract,
            fills=fills,
            stated_contracts=None if stated is None else float(stated),
            stated_average_price=_optional_float(raw.get("average_price")),
            stated_realised_usd=float(raw.get("realised_usd") or 0.0),
            account=str(raw.get("account") or ""),
            note=str(raw.get("note") or ""),
        ))

    for index, raw in enumerate(payload.get("limits") or ()):
        label = f"{where}: limits[{index}]"
        key = str(raw.get("key") or "")
        if key not in ("net_mt", "unhedged_mt", "notional_usd", "loss_usd"):
            raise PositionError(f"{label}: unknown limit key {key!r}")
        limits.append(Limit(
            key=key,
            scope=str(raw.get("scope") or "*"),
            maximum=float(raw["maximum"]),
            note=str(raw.get("note") or ""),
        ))

    return Book(
        physical=tuple(physical), futures=tuple(futures), limits=tuple(limits),
        loaded_from=(where,),
    )


def _optional_float(value: Any) -> float | None:
    return None if value in (None, "") else float(value)


def load_book(directory: str | os.PathLike[str] | None = None) -> Book:
    """Read every ``*.yml`` position document in ``directory``.

    A missing directory is an empty book — a fresh clone has entered no
    positions, and the workspace saying so is correct. A *present but
    malformed* file raises: "nothing entered" and "something entered wrongly"
    are different states and only one of them is safe to render as zero.
    """
    import config

    root = Path(str(directory) if directory is not None else getattr(
        config, "POSITIONS_DIR", "data/reference/positions"
    ))
    if not root.is_dir():
        log.info("no positions directory at %s — the book is empty", root)
        return Book()

    import yaml

    physical: list[PhysicalPosition] = []
    futures: list[FuturesPosition] = []
    limits: list[Limit] = []
    sources: list[str] = []
    for path in sorted(root.glob("*.yml")):
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(payload, dict):
            raise PositionError(f"{path}: expected a mapping with physical/futures/limits keys")
        book = parse_book(payload, where=str(path))
        physical.extend(book.physical)
        futures.extend(book.futures)
        limits.extend(book.limits)
        sources.append(str(path))
    return Book(
        physical=tuple(physical), futures=tuple(futures), limits=tuple(limits),
        loaded_from=tuple(sources),
    )


CSV_COLUMNS = (
    "kind", "commodity", "contract", "side", "quantity", "unit", "price",
    "trade_date", "currency", "fx_pair", "basis_usd_mt", "location",
)


def positions_from_csv(path: str | os.PathLike[str]) -> Book:
    """Import a flat CSV of positions — a broker or ERP export.

    Columns: ``kind`` (physical|futures), ``commodity``, ``contract``,
    ``side``, ``quantity``, ``unit``, ``price``, ``trade_date``, ``currency``,
    ``fx_pair``, ``basis_usd_mt``, ``location``. Rows of ``kind=futures``
    become fills against their named contract, so several rows for one contract
    accumulate into one position at weighted-average cost.
    """
    physical: list[PhysicalPosition] = []
    by_contract: dict[str, list[Fill]] = {}

    with open(path, newline="", encoding="utf-8") as handle:
        for number, row in enumerate(csv.DictReader(handle), start=2):
            where = f"{path}:{number}"
            kind = (row.get("kind") or "").strip().lower()
            if kind == "futures":
                symbol = (row.get("contract") or "").strip().upper()
                parse_symbol(symbol)
                by_contract.setdefault(symbol, []).append(Fill(
                    trade_date=_date(row.get("trade_date"), where),
                    side=_side(row.get("side"), where),
                    quantity=float(row["quantity"]),
                    price=float(row["price"]),
                    reference=where,
                ))
            elif kind == "physical":
                commodity = (row.get("commodity") or "").strip()
                _known_commodity(commodity, where)
                physical.append(PhysicalPosition(
                    commodity=commodity,
                    quantity=float(row["quantity"]),
                    unit=_unit(row.get("unit"), where),
                    side=_side(row.get("side"), where),
                    average_cost_usd_mt=_optional_float(row.get("price")),
                    currency=(row.get("currency") or "USD").upper(),
                    fx_pair=row.get("fx_pair") or None,
                    current_basis_usd_mt=_optional_float(row.get("basis_usd_mt")),
                    location=(row.get("location") or "").strip(),
                ))
            else:
                raise PositionError(f"{where}: kind {kind!r} is neither physical nor futures")

    futures = tuple(
        FuturesPosition(contract=parse_symbol(symbol), fills=tuple(fills))
        for symbol, fills in sorted(by_contract.items())
    )
    return Book(physical=tuple(physical), futures=futures, loaded_from=(str(path),))


__all__ = [
    "CSV_COLUMNS",
    "MARK_NOTE",
    "Book",
    "BookKind",
    "BookValuation",
    "Fill",
    "FuturesPosition",
    "Limit",
    "LimitBreach",
    "LotResult",
    "MarkedPosition",
    "PhysicalPosition",
    "PositionError",
    "check_limits",
    "load_book",
    "parse_book",
    "positions_from_csv",
    "run_weighted_average",
    "value_book",
]
