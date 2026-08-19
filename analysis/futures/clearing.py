"""Official clearing P&L, kept beside — never merged into — the management one.

Two numbers describe the same book every evening and they are not the same
number. The clearer marks against the exchange settlement and reports what the
account actually owes; :mod:`analysis.futures.positions` marks against a
delayed daily close and reports what the desk thinks it made. Both are worth
having. Averaging them, or letting the statement quietly overwrite the mark,
throws away the only thing that makes either trustworthy — the difference, and
the reason for it.

So this module does exactly three things:

* **Classify.** A settlement printed on a statement the *user* supplied is
  :data:`~pricing.semantics.PriceType.ATTESTED_SETTLEMENT`: authoritative for
  that account, and still not proven by anything this project ingests. It does
  not make ``PROVEN_SETTLEMENT_SOURCES`` non-empty and it never reaches
  ``Confidence.EXECUTABLE``.
* **Load**, from ``data/reference/clearing/*.yml``, on the same terms as
  positions and options: a missing directory is no statements, a present but
  malformed one raises. Nothing is defaulted — a line with no settlement price
  is refused rather than marked at the board, because marking it at the board
  would silently turn the official number into ours.
* **Reconcile**, read-only. Every row carries both figures side by side, their
  difference, and whether that difference is inside
  ``config.CLEARING_RECONCILIATION_TOLERANCE_USD``. A contract on the statement
  that is not in the book, and a book position absent from the statement, are
  both findings in their own right: the first is a position the desk did not
  know it had.

Physical positions are not reconciled at all and the report says so. A clearer
holds futures, not beans; scoring a bean length against a futures statement
would manufacture a discrepancy out of a category error.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pricing.semantics import PriceType

if TYPE_CHECKING:  # pragma: no cover - typing only
    from analysis.futures.positions import BookValuation

log = logging.getLogger(__name__)


class ClearingError(ValueError):
    """A clearing statement said something that cannot be relied on."""


#: The label every official figure carries in a payload. Its counterpart lives
#: in ``analysis.futures.positions`` as ``MANAGEMENT_BASIS``; the two constants
#: exist so no renderer has to decide which basis it is looking at.
CLEARING_BASIS = "official_clearing"


class PnlBasis(str, Enum):
    """Which of the two P&Ls a number is. There is no third, and no blend."""

    OFFICIAL_CLEARING = CLEARING_BASIS
    MANAGEMENT_ESTIMATE = "management_estimate"

    @property
    def description(self) -> str:
        if self is PnlBasis.OFFICIAL_CLEARING:
            return (
                "as printed on the account's clearing or broker statement, struck on the "
                "exchange settlement — authoritative for the account, and not verified by "
                "any feed this project ingests"
            )
        return (
            "marked against delayed daily closes by this software — reproducible from the "
            "numbers shown, and not a margin figure"
        )


@dataclass(frozen=True)
class ClearingLine:
    """One position as the clearer reported it."""

    symbol: str
    description: str
    quantity: float                       # signed contracts, short negative
    settlement_price: float
    realised_usd: float | None = None
    unrealised_usd: float | None = None
    currency: str = "USD"

    @property
    def price_type(self) -> PriceType:
        return PriceType.ATTESTED_SETTLEMENT

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "description": self.description,
            "quantity": self.quantity,
            "settlement_price": self.settlement_price,
            "realised_usd": self.realised_usd,
            "unrealised_usd": self.unrealised_usd,
            "currency": self.currency,
            "price_type": self.price_type.value,
            "price_caveat": self.price_type.caveat,
        }


@dataclass(frozen=True)
class ClearingStatement:
    """One statement, for one account, struck on one date."""

    account: str
    statement_date: date
    lines: tuple[ClearingLine, ...] = ()
    broker: str = ""
    currency: str = "USD"
    statement_ref: str = ""
    loaded_from: str = ""

    @property
    def total_realised_usd(self) -> float | None:
        return _total(line.realised_usd for line in self.lines)

    @property
    def total_unrealised_usd(self) -> float | None:
        return _total(line.unrealised_usd for line in self.lines)

    def line(self, symbol: str) -> ClearingLine | None:
        return next((line for line in self.lines if line.symbol == symbol.upper()), None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "account": self.account,
            "broker": self.broker,
            "statement_date": self.statement_date.isoformat(),
            "statement_ref": self.statement_ref,
            "currency": self.currency,
            "lines": [line.to_dict() for line in self.lines],
            "total_realised_usd": self.total_realised_usd,
            "total_unrealised_usd": self.total_unrealised_usd,
        }


def _total(values) -> float | None:
    """Sum, or ``None`` if any component was never stated.

    A partial total is worse than no total: it looks complete and is short by
    whatever the statement left blank.
    """
    collected = list(values)
    if not collected or any(value is None for value in collected):
        return None
    return sum(float(value) for value in collected)


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReconciliationRow:
    """One contract, both ways."""

    key: str
    official_quantity: float | None
    management_quantity: float | None
    official_unrealised_usd: float | None
    management_unrealised_usd: float | None
    official_realised_usd: float | None
    management_realised_usd: float | None
    settlement_price: float | None
    mark: float | None
    tolerance_usd: float
    notes: tuple[str, ...] = ()

    @property
    def difference_usd(self) -> float | None:
        if self.official_unrealised_usd is None or self.management_unrealised_usd is None:
            return None
        return self.official_unrealised_usd - self.management_unrealised_usd

    @property
    def quantity_agrees(self) -> bool | None:
        if self.official_quantity is None or self.management_quantity is None:
            return None
        return abs(self.official_quantity - self.management_quantity) < 1e-9

    @property
    def agrees(self) -> bool | None:
        """``None`` where a side did not state a figure — unknown is not agreement."""
        if self.quantity_agrees is False:
            return False
        difference = self.difference_usd
        if difference is None:
            return None
        if self.quantity_agrees is None:
            return None
        return abs(difference) <= self.tolerance_usd

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "official_quantity": self.official_quantity,
            "management_quantity": self.management_quantity,
            "official_unrealised_usd": _round(self.official_unrealised_usd),
            "management_unrealised_usd": _round(self.management_unrealised_usd),
            "official_realised_usd": _round(self.official_realised_usd),
            "management_realised_usd": _round(self.management_realised_usd),
            "settlement_price": self.settlement_price,
            "mark": self.mark,
            "difference_usd": _round(self.difference_usd),
            "tolerance_usd": self.tolerance_usd,
            "quantity_agrees": self.quantity_agrees,
            "agrees": self.agrees,
            "notes": list(self.notes),
        }


def _round(value: float | None) -> float | None:
    return None if value is None else round(value, 2)


@dataclass(frozen=True)
class Reconciliation:
    """The two bases, side by side. Read-only: nothing here changes a book."""

    as_of: date
    account: str
    statement_date: date
    rows: tuple[ReconciliationRow, ...]
    not_in_book: tuple[str, ...] = ()
    not_on_statement: tuple[str, ...] = ()
    official_total_unrealised_usd: float | None = None
    management_total_unrealised_usd: float | None = None
    official_total_realised_usd: float | None = None
    management_total_realised_usd: float | None = None
    tolerance_usd: float = 0.0
    notes: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    statement: ClearingStatement | None = field(default=None, repr=False)

    def row(self, key: str) -> ReconciliationRow:
        for row in self.rows:
            if row.key == key:
                return row
        raise KeyError(key)

    @property
    def agrees(self) -> bool:
        if self.not_in_book or self.not_on_statement:
            return False
        return all(row.agrees is True for row in self.rows)

    @property
    def summary(self) -> str:
        if not self.rows:
            return "nothing to reconcile — the statement and the book share no contract"
        if self.agrees:
            return (
                f"{len(self.rows)} contract(s) agree within "
                f"USD {self.tolerance_usd:,.2f}; the two bases are not merged"
            )
        parts = []
        differing = [r for r in self.rows if r.agrees is False]
        if differing:
            parts.append(f"{len(differing)} contract(s) differ beyond USD {self.tolerance_usd:,.2f}")
        unknown = [r for r in self.rows if r.agrees is None]
        if unknown:
            parts.append(f"{len(unknown)} could not be compared")
        if self.not_in_book:
            parts.append(f"{len(self.not_in_book)} on the statement are not in the book")
        if self.not_on_statement:
            parts.append(f"{len(self.not_on_statement)} in the book are not on the statement")
        return "; ".join(parts)

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "account": self.account,
            "statement_date": self.statement_date.isoformat(),
            "official": {
                "basis": PnlBasis.OFFICIAL_CLEARING.value,
                "description": PnlBasis.OFFICIAL_CLEARING.description,
                "unrealised_usd": _round(self.official_total_unrealised_usd),
                "realised_usd": _round(self.official_total_realised_usd),
            },
            "management": {
                "basis": PnlBasis.MANAGEMENT_ESTIMATE.value,
                "description": PnlBasis.MANAGEMENT_ESTIMATE.description,
                "unrealised_usd": _round(self.management_total_unrealised_usd),
                "realised_usd": _round(self.management_total_realised_usd),
            },
            "rows": [row.to_dict() for row in self.rows],
            "not_in_book": list(self.not_in_book),
            "not_on_statement": list(self.not_on_statement),
            "tolerance_usd": self.tolerance_usd,
            "agrees": self.agrees,
            "summary": self.summary,
            "notes": list(self.notes),
            "warnings": list(self.warnings),
        }


def reconcile(
    valuation: BookValuation,
    statement: ClearingStatement,
    *,
    tolerance_usd: float | None = None,
) -> Reconciliation:
    """Compare the entered book's marks with what the clearer reported.

    Neither input is modified and no figure is adopted from the other. Where
    the two disagree, the disagreement is the output.
    """
    import config
    from analysis.futures.positions import BookKind

    tolerance = float(
        tolerance_usd
        if tolerance_usd is not None
        else getattr(config, "CLEARING_RECONCILIATION_TOLERANCE_USD", 25.0)
    )

    futures = [p for p in valuation.positions if p.kind is BookKind.FUTURES]
    physical = [p for p in valuation.positions if p.kind is BookKind.PHYSICAL]
    by_symbol = {p.key.upper(): p for p in futures}
    statement_symbols = {line.symbol.upper(): line for line in statement.lines}

    rows: list[ReconciliationRow] = []
    for symbol in sorted(set(by_symbol) | set(statement_symbols)):
        marked = by_symbol.get(symbol)
        line = statement_symbols.get(symbol)
        notes: list[str] = []
        if marked is None:
            notes.append(
                "on the clearing statement and not in the entered book — either a position "
                "nobody recorded, or a statement line for another desk"
            )
        if line is None:
            notes.append(
                "in the entered book and not on the clearing statement — the clearer does "
                "not think this position exists"
            )
        if line is not None and line.unrealised_usd is None:
            notes.append("the statement did not state an unrealised figure for this line")
        if marked is not None and marked.unrealised_usd is None:
            notes.append("this position is unmarked, so there is no management figure to compare")

        row = ReconciliationRow(
            key=symbol,
            official_quantity=None if line is None else line.quantity,
            management_quantity=None if marked is None else marked.net_quantity,
            official_unrealised_usd=None if line is None else line.unrealised_usd,
            management_unrealised_usd=None if marked is None else marked.unrealised_usd,
            official_realised_usd=None if line is None else line.realised_usd,
            management_realised_usd=None if marked is None else marked.realised_usd,
            settlement_price=None if line is None else line.settlement_price,
            mark=None if marked is None else marked.mark,
            tolerance_usd=tolerance,
            notes=tuple(notes),
        )
        if row.quantity_agrees is False:
            row = ReconciliationRow(
                **{
                    **row.__dict__,
                    "notes": row.notes + (
                        f"quantity disagrees: the clearer says {row.official_quantity:g} lots, "
                        f"the book says {row.management_quantity:g}",
                    ),
                }
            )
        rows.append(row)

    report_notes: list[str] = []
    if physical:
        report_notes.append(
            f"{len(physical)} physical position(s) are not reconciled: a clearer holds futures, "
            "not beans, and scoring one against the other would invent a discrepancy"
        )
    report_notes.append(
        "the two bases are reported side by side and never combined — there is no single "
        "'reconciled' P&L, because it would belong to neither desk"
    )

    warnings: list[str] = []
    if statement.statement_date != valuation.as_of:
        warnings.append(
            f"the statement is dated {statement.statement_date.isoformat()} and the valuation "
            f"{valuation.as_of.isoformat()} — two dates are two markets, so a difference here "
            "is partly the session that moved in between"
        )

    return Reconciliation(
        as_of=valuation.as_of,
        account=statement.account,
        statement_date=statement.statement_date,
        rows=tuple(rows),
        not_in_book=tuple(s for s in sorted(statement_symbols) if s not in by_symbol),
        not_on_statement=tuple(s for s in sorted(by_symbol) if s not in statement_symbols),
        official_total_unrealised_usd=statement.total_unrealised_usd,
        management_total_unrealised_usd=_total(p.unrealised_usd for p in futures),
        official_total_realised_usd=statement.total_realised_usd,
        management_total_realised_usd=_total(p.realised_usd for p in futures),
        tolerance_usd=tolerance,
        notes=tuple(report_notes),
        warnings=tuple(warnings),
        statement=statement,
    )


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def parse_statement(payload: dict[str, Any], *, where: str) -> ClearingStatement:
    """Validate one statement document. Nothing is defaulted."""
    if not isinstance(payload, dict):
        raise ClearingError(f"{where}: expected a mapping with account/statement_date/lines")
    account = str(payload.get("account") or "").strip()
    if not account:
        raise ClearingError(
            f"{where}: no `account` — an unattributed statement cannot be reconciled "
            "against any book"
        )
    raw_date = payload.get("statement_date")
    if raw_date in (None, ""):
        raise ClearingError(
            f"{where}: no `statement_date` — a statement dated by when it was read is a "
            "statement compared against the wrong session"
        )
    try:
        statement_date = raw_date if isinstance(raw_date, date) else date.fromisoformat(
            str(raw_date)[:10]
        )
    except ValueError as exc:
        raise ClearingError(f"{where}: statement_date {raw_date!r} is not a date") from exc

    lines: list[ClearingLine] = []
    for index, raw in enumerate(payload.get("lines") or ()):
        label = f"{where}: lines[{index}]"
        if not isinstance(raw, dict):
            raise ClearingError(f"{label}: expected a mapping")
        symbol = str(raw.get("symbol") or raw.get("contract") or "").strip().upper()
        if not symbol:
            raise ClearingError(f"{label}: no `symbol`")
        if raw.get("settlement_price") in (None, ""):
            raise ClearingError(
                f"{label}: no `settlement_price` — this line is not marked at the board "
                "instead, because that would turn the official number into ours"
            )
        if raw.get("quantity") in (None, ""):
            raise ClearingError(f"{label}: no `quantity`")
        lines.append(ClearingLine(
            symbol=symbol,
            description=str(raw.get("description") or ""),
            quantity=float(raw["quantity"]),
            settlement_price=float(raw["settlement_price"]),
            realised_usd=_optional_float(raw.get("realised_usd")),
            unrealised_usd=_optional_float(raw.get("unrealised_usd")),
            currency=str(raw.get("currency") or payload.get("currency") or "USD").upper(),
        ))

    return ClearingStatement(
        account=account,
        statement_date=statement_date,
        lines=tuple(lines),
        broker=str(payload.get("broker") or ""),
        currency=str(payload.get("currency") or "USD").upper(),
        statement_ref=str(payload.get("statement_ref") or ""),
        loaded_from=where,
    )


def _optional_float(value: Any) -> float | None:
    return None if value in (None, "") else float(value)


def load_statements(
    directory: str | os.PathLike[str] | None = None,
) -> tuple[ClearingStatement, ...]:
    """Read every statement document in ``directory``, newest first.

    A missing directory is no statements — a desk that has not exported one is
    a legitimate state. A *present but malformed* file raises, on the same
    terms as positions and options: "none supplied" and "one supplied wrongly"
    are different, and only the first is safe to render as nothing.
    """
    import config

    root = Path(str(directory) if directory is not None else getattr(
        config, "CLEARING_DIR", "data/reference/clearing"
    ))
    if not root.is_dir():
        log.info("no clearing directory at %s — no statements to reconcile", root)
        return ()

    import yaml

    statements: list[ClearingStatement] = []
    for path in sorted(list(root.glob("*.yml")) + list(root.glob("*.yaml"))):
        try:
            payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as exc:
            raise ClearingError(f"{path}: not readable as YAML ({exc})") from exc
        statements.append(parse_statement(payload, where=str(path)))
    return tuple(sorted(statements, key=lambda s: s.statement_date, reverse=True))


__all__ = [
    "CLEARING_BASIS",
    "ClearingError",
    "ClearingLine",
    "ClearingStatement",
    "PnlBasis",
    "Reconciliation",
    "ReconciliationRow",
    "load_statements",
    "parse_statement",
    "reconcile",
]
