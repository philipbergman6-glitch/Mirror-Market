"""Importing a client's broker, clearing or ERP export — safely, and in two steps.

An export is the one file this project cannot control the shape of. Its column
names, its sign convention and its product codes all belong to somebody else's
system, and the dangerous failure is not a crash: it is a clean parse of the
wrong thing. A short 68 lots read as a long 68 is a 136-lot error that looks
exactly like a position.

The workflow is therefore split, and **the first half writes nothing**:

1. :func:`read_import` reads the bytes against an :class:`ImportProfile` — a
   named, reviewable mapping the desk writes once for its own broker and keeps
   under ``data/reference/import_profiles/`` — and returns an
   :class:`ImportReport`. The report says what was accepted, what was rejected
   and why, which columns the profile did not claim, and the sha256 of exactly
   the bytes that were read. A desk can read that report before anything
   becomes a position.
2. :func:`apply_import` turns the accepted rows into a
   :class:`~analysis.futures.positions.Book`, and **refuses while anything was
   rejected** unless the caller passes ``allow_partial=True``. A partial book
   is a book that is quietly short a position; asking for one out loud is the
   least this can require.

Every rule below is the same rule in different clothes — *nothing is guessed*:

* A **missing required column** refuses the whole file before any row is read.
  Per-row failures there would read as bad data rather than a bad mapping.
* A **sign convention** is declared, never inferred. ``signed`` takes the
  quantity's own sign; ``side_column`` requires a side column and a stated
  vocabulary for its values.
* A **date format** is declared and tried once. Falling back to a second format
  is how ``03/04`` becomes March in one file and April in the next.
* A **product code** is translated through the profile's ``commodity_map`` or
  the row is rejected. ``SOJA`` is probably soybeans; probably is not a book.
* A **blank number** is a rejection, never a zero.

Re-importing is idempotent by construction: every row's reference is
``<sha256[:8]>:<row number>``, derived from the file's own bytes, so the same
export read twice produces the same references and a caller can dedupe on them.
"""

from __future__ import annotations

import csv
import hashlib
import logging
import os
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


class ImportError_(ValueError):
    """An export, or the profile describing it, cannot be read safely.

    Named with a trailing underscore so it cannot shadow the builtin — the two
    would be confused at exactly the wrong moment.
    """


#: Declared sign conventions. There is no "detect".
QUANTITY_SIGNS = ("signed", "side_column")

#: What each kind of export must map before a single row is read.
REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "futures": ("trade_date", "contract", "quantity", "price"),
    "physical": ("trade_date", "commodity", "quantity", "price"),
}

DEFAULT_SIDE_VALUES: dict[str, str] = {
    "b": "long", "buy": "long", "long": "long", "l": "long",
    "s": "short", "sell": "short", "short": "short",
}


@dataclass(frozen=True)
class ImportProfile:
    """How one counterparty's export maps onto this project's vocabulary."""

    name: str
    source: str                       # who produced the file — broker, clearer, ERP
    kind: str                         # futures | physical
    quantity_sign: str                # signed | side_column
    columns: dict[str, str]           # our field -> their column header
    side_values: dict[str, str] = field(default_factory=lambda: dict(DEFAULT_SIDE_VALUES))
    commodity_map: dict[str, str] = field(default_factory=dict)
    symbol_map: dict[str, str] = field(default_factory=dict)
    date_format: str = "%Y-%m-%d"
    delimiter: str = ","
    default_unit: str | None = None
    note: str = ""

    def __post_init__(self) -> None:
        if self.kind not in REQUIRED_COLUMNS:
            raise ImportError_(
                f"{self.name}: kind {self.kind!r} is neither "
                f"{' nor '.join(sorted(REQUIRED_COLUMNS))}"
            )
        if self.quantity_sign not in QUANTITY_SIGNS:
            raise ImportError_(
                f"{self.name}: quantity_sign {self.quantity_sign!r} is not one of "
                f"{list(QUANTITY_SIGNS)} — a sign this software works out for itself is a "
                "long position read as a short"
            )
        missing = [c for c in REQUIRED_COLUMNS[self.kind] if c not in self.columns]
        if missing:
            raise ImportError_(f"{self.name}: the profile maps no column for {missing}")
        if self.quantity_sign == "side_column" and "side" not in self.columns:
            raise ImportError_(
                f"{self.name}: quantity_sign is 'side_column' but no `side` column is mapped"
            )
        if self.kind == "physical" and "unit" not in self.columns and not self.default_unit:
            raise ImportError_(
                f"{self.name}: a physical export must map a `unit` column or declare a "
                "`default_unit` — a quantity with no unit is a number, not a tonnage"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source": self.source,
            "kind": self.kind,
            "quantity_sign": self.quantity_sign,
            "columns": dict(self.columns),
            "date_format": self.date_format,
            "default_unit": self.default_unit,
            "note": self.note,
        }


@dataclass(frozen=True)
class ImportRow:
    """One accepted row, in this project's vocabulary."""

    reference: str
    row_number: int
    trade_date: date
    side: str
    quantity: float
    price: float
    contract: str = ""
    commodity: str = ""
    unit: str = ""
    currency: str = "USD"
    location: str = ""
    account: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "reference": self.reference,
            "row_number": self.row_number,
            "trade_date": self.trade_date.isoformat(),
            "side": self.side,
            "quantity": self.quantity,
            "price": self.price,
            "contract": self.contract,
            "commodity": self.commodity,
            "unit": self.unit,
            "currency": self.currency,
            "location": self.location,
            "account": self.account,
        }


@dataclass(frozen=True)
class RejectedRow:
    """One row that was not imported, and the reason, in the file's own terms."""

    row_number: int
    reason: str
    raw: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"row_number": self.row_number, "reason": self.reason}


@dataclass(frozen=True)
class ImportReport:
    """The dry run. Nothing has become a position yet."""

    path: str
    profile_name: str
    source: str
    sha256: str
    accepted: tuple[ImportRow, ...]
    rejected: tuple[RejectedRow, ...]
    unmapped_columns: tuple[str, ...]
    notes: tuple[str, ...] = ()
    kind: str = "futures"

    @property
    def accepted_count(self) -> int:
        return len(self.accepted)

    @property
    def rejected_count(self) -> int:
        return len(self.rejected)

    @property
    def is_clean(self) -> bool:
        return not self.rejected

    def to_dict(self) -> dict[str, Any]:
        return {
            "loaded_from": self.path,
            "profile": self.profile_name,
            "source": self.source,
            "sha256": self.sha256,
            "kind": self.kind,
            "accepted_count": self.accepted_count,
            "rejected_count": self.rejected_count,
            "accepted": [row.to_dict() for row in self.accepted],
            "rejected": [row.to_dict() for row in self.rejected],
            "unmapped_columns": list(self.unmapped_columns),
            "notes": list(self.notes),
            "is_clean": self.is_clean,
        }


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


def read_import(
    path: str | os.PathLike[str],
    profile: ImportProfile,
) -> ImportReport:
    """Read an export against a profile. **Writes nothing and builds no book.**"""
    raw_bytes = Path(path).read_bytes()
    digest = hashlib.sha256(raw_bytes).hexdigest()
    text = raw_bytes.decode("utf-8-sig")

    reader = csv.DictReader(text.splitlines(), delimiter=profile.delimiter)
    headers = list(reader.fieldnames or ())
    if not headers:
        raise ImportError_(f"{path}: no header row")

    missing = sorted({
        column for field_name, column in profile.columns.items() if column not in headers
    })
    if missing:
        raise ImportError_(
            f"{path}: the profile {profile.name!r} maps column(s) {missing} that this file "
            f"does not have — its headers are {headers}. The mapping is wrong, not the data, "
            "so no row is read"
        )

    claimed = set(profile.columns.values())
    unmapped = tuple(h for h in headers if h not in claimed)

    accepted: list[ImportRow] = []
    rejected: list[RejectedRow] = []
    for number, raw in enumerate(reader, start=2):
        try:
            accepted.append(_row(raw, profile, digest, number))
        except ImportError_ as exc:
            rejected.append(RejectedRow(row_number=number, reason=str(exc), raw=dict(raw)))

    notes: list[str] = []
    if unmapped:
        notes.append(
            f"{len(unmapped)} column(s) the profile does not claim were left unread: "
            f"{', '.join(unmapped)} — check none of them carries a sign, a fee or a second "
            "account before relying on this import"
        )
    if rejected:
        notes.append(
            f"{len(rejected)} row(s) were rejected; apply_import will refuse this report "
            "unless allow_partial=True"
        )

    return ImportReport(
        path=str(path),
        profile_name=profile.name,
        source=profile.source,
        sha256=digest,
        accepted=tuple(accepted),
        rejected=tuple(rejected),
        unmapped_columns=unmapped,
        notes=tuple(notes),
        kind=profile.kind,
    )


def _row(raw: dict[str, str], profile: ImportProfile, digest: str, number: int) -> ImportRow:
    def cell(field_name: str) -> str:
        column = profile.columns.get(field_name)
        return "" if column is None else (raw.get(column) or "").strip()

    commodity = ""
    if profile.kind == "physical":
        code = cell("commodity")
        if not code:
            raise ImportError_("no commodity")
        commodity = profile.commodity_map.get(code, profile.commodity_map.get(code.upper(), ""))
        if not commodity:
            raise ImportError_(
                f"product code {code!r} is not in the profile's commodity_map "
                f"({sorted(profile.commodity_map)}) — it is not guessed"
            )

    contract = ""
    if profile.kind == "futures":
        symbol = cell("contract")
        if not symbol:
            raise ImportError_("no contract symbol")
        contract = profile.symbol_map.get(symbol, symbol).upper()

    raw_date = cell("trade_date")
    try:
        trade_date = datetime.strptime(raw_date, profile.date_format).date()
    except ValueError as exc:
        raise ImportError_(
            f"date {raw_date!r} does not match the profile's declared format "
            f"{profile.date_format!r} — a second format is not tried, because that is how "
            "one file's March becomes another's April"
        ) from exc

    quantity = _number(cell("quantity"), "quantity")
    price = _number(cell("price"), "price")

    if profile.quantity_sign == "signed":
        side = "short" if quantity < 0 else "long"
        quantity = abs(quantity)
    else:
        token = cell("side")
        side = profile.side_values.get(token.lower(), "")
        if side not in ("long", "short"):
            raise ImportError_(
                f"side {token!r} is not in the profile's side_values "
                f"({sorted(profile.side_values)})"
            )

    unit = cell("unit") or (profile.default_unit or "")

    return ImportRow(
        reference=f"{digest[:8]}:{number}",
        row_number=number,
        trade_date=trade_date,
        side=side,
        quantity=quantity,
        price=price,
        contract=contract,
        commodity=commodity,
        unit=unit,
        currency=(cell("currency") or "USD").upper(),
        location=cell("location"),
        account=cell("account"),
    )


def _number(text: str, what: str) -> float:
    if text in ("", "-"):
        raise ImportError_(f"no {what} — a blank is not a zero")
    try:
        return float(text.replace(",", ""))
    except ValueError as exc:
        raise ImportError_(f"{what} {text!r} is not a number") from exc


# ---------------------------------------------------------------------------
# Applying
# ---------------------------------------------------------------------------


def apply_import(report: ImportReport, *, allow_partial: bool = False):
    """Turn an accepted report into a :class:`~analysis.futures.positions.Book`.

    Refuses while anything was rejected. A partial book is a book that is
    quietly short a position, and the caller has to ask for one out loud.
    """
    from analysis.futures.positions import (
        Book,
        Fill,
        FuturesPosition,
        PhysicalPosition,
        _known_commodity,
        _side,
        _unit,
        parse_symbol,
    )

    if report.rejected and not allow_partial:
        raise ImportError_(
            f"{report.rejected_count} row(s) of {report.path} were rejected; pass "
            "allow_partial=True to import the rest, having read why each was dropped"
        )

    provenance = [f"{report.path} (profile {report.profile_name}, sha256 {report.sha256[:12]})"]
    if report.rejected:
        provenance.append(
            f"partial import: {report.rejected_count} row(s) rejected and not in this book"
        )

    if report.kind == "futures":
        by_symbol: dict[str, list[Fill]] = {}
        for row in report.accepted:
            parse_symbol(row.contract)
            by_symbol.setdefault(row.contract, []).append(Fill(
                trade_date=row.trade_date,
                side=_side(row.side, row.reference),
                quantity=row.quantity,
                price=row.price,
                reference=row.reference,
            ))
        futures = tuple(
            FuturesPosition(
                contract=parse_symbol(symbol),
                fills=tuple(fills),
                account=next(
                    (r.account for r in report.accepted if r.contract == symbol and r.account), ""
                ),
            )
            for symbol, fills in sorted(by_symbol.items())
        )
        return Book(futures=futures, loaded_from=tuple(provenance))

    physical = []
    for row in report.accepted:
        _known_commodity(row.commodity, row.reference)
        physical.append(PhysicalPosition(
            commodity=row.commodity,
            quantity=row.quantity,
            unit=_unit(row.unit, row.reference),
            side=_side(row.side, row.reference),
            average_cost_usd_mt=row.price,
            currency=row.currency,
            location=row.location,
            note=f"imported {row.reference}",
        ))
    return Book(physical=tuple(physical), loaded_from=tuple(provenance))


# ---------------------------------------------------------------------------
# Profiles on disk
# ---------------------------------------------------------------------------


def parse_profile(payload: dict[str, Any], *, where: str) -> ImportProfile:
    if not isinstance(payload, dict):
        raise ImportError_(f"{where}: expected a mapping describing one profile")
    try:
        return ImportProfile(
            name=str(payload.get("name") or Path(where).stem),
            source=str(payload.get("source") or ""),
            kind=str(payload.get("kind") or ""),
            quantity_sign=str(payload.get("quantity_sign") or ""),
            columns={str(k): str(v) for k, v in (payload.get("columns") or {}).items()},
            side_values={
                str(k).lower(): str(v).lower()
                for k, v in (payload.get("side_values") or DEFAULT_SIDE_VALUES).items()
            },
            commodity_map={str(k): str(v) for k, v in (payload.get("commodity_map") or {}).items()},
            symbol_map={str(k): str(v) for k, v in (payload.get("symbol_map") or {}).items()},
            date_format=str(payload.get("date_format") or "%Y-%m-%d"),
            delimiter=str(payload.get("delimiter") or ","),
            default_unit=payload.get("default_unit"),
            note=str(payload.get("note") or ""),
        )
    except ImportError_ as exc:
        raise ImportError_(f"{where}: {exc}") from exc


def load_profiles(
    directory: str | os.PathLike[str] | None = None,
) -> tuple[ImportProfile, ...]:
    """Every profile in ``directory``. A missing directory is no profiles."""
    import config

    root = Path(str(directory) if directory is not None else getattr(
        config, "IMPORT_PROFILE_DIR", "data/reference/import_profiles"
    ))
    if not root.is_dir():
        log.info("no import-profile directory at %s", root)
        return ()

    import yaml

    profiles = []
    for path in sorted(list(root.glob("*.yml")) + list(root.glob("*.yaml"))):
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        profiles.append(parse_profile(payload, where=str(path)))
    return tuple(profiles)


def load_profile(
    name: str,
    *,
    directory: str | os.PathLike[str] | None = None,
) -> ImportProfile:
    """One profile by name, or an error that names the ones that do exist."""
    profiles = load_profiles(directory)
    for profile in profiles:
        if profile.name == name:
            return profile
    raise ImportError_(
        f"no import profile named {name!r}; available: {[p.name for p in profiles]}"
    )


__all__ = [
    "DEFAULT_SIDE_VALUES",
    "QUANTITY_SIGNS",
    "REQUIRED_COLUMNS",
    "ImportError_",
    "ImportProfile",
    "ImportReport",
    "ImportRow",
    "RejectedRow",
    "apply_import",
    "load_profile",
    "load_profiles",
    "parse_profile",
    "read_import",
]
