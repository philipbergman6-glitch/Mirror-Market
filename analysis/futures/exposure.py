"""Exposure views — the same book, cut the seven ways a risk manager asks for.

A net tonnage is not an exposure. Twelve thousand tonnes of beans bought basis
and hedged short sixty-eight lots is *nearly flat on price and long the basis on
every hedged tonne* — one position, two unrelated risks, and a desk reading only
the net number will be surprised by the second one. So each view here answers
one question, in the unit that question is asked in:

``flat_price``      tonnes that still move with the board
``basis``           tonnes that move with the basis — **hedging moves tonnes
                    into this view, it does not remove them from the book**
``crush``           bean-equivalent tonnes exposed to the crush margin
``fx``              USD at risk per currency pair, per one percent
``contract_month``  what is open in each named delivery month
``first_notice``    what is still open as the notice day approaches
``residual``        physical the hedge does not cover

Three rules hold across all of them:

* **Nothing is inferred.** A position with no stated pricing convention is
  counted at its most exposed reading *and* carries a warning saying the
  convention was not stated. A currency with no rate is reported unquantified,
  never as zero.
* **Every line names its unit of move.** An unlabelled sensitivity is how a
  hundred-fold error survives review.
* **The metrics the limits read are the numbers the page shows.** There is one
  definition of "flat price exposure" and both consumers call it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Any

from analysis.futures.domain import SOY_COMPLEX, NamedContract, spec_for
from analysis.futures.hedge import BasisConvention
from analysis.futures.positions import Book, BookValuation, MarkedPosition
from analysis.spreads import CRUSH_MEAL_YIELD_MT, CRUSH_OIL_YIELD_MT


class ExposureView(str, Enum):
    FLAT_PRICE = "flat_price"
    BASIS = "basis"
    CRUSH = "crush"
    FX = "fx"
    CONTRACT_MONTH = "contract_month"
    FIRST_NOTICE = "first_notice"
    RESIDUAL = "residual"


#: Conventions under which the price is **not yet fixed against the board**, so
#: the tonnes still move with it. ``FLAT_PRICE`` is absent on purpose: that
#: price is contracted, and counting it would report a risk that was traded away
#: and invite a hedge that creates a position rather than removing one.
_BOARD_EXPOSED = (
    BasisConvention.UNPRICED,
    BasisConvention.BASIS_OVER_FUTURES,
    BasisConvention.FORMULA_PRICED,
)

#: Conventions under which the differential is not fixed either.
_BASIS_UNFIXED = (BasisConvention.UNPRICED, BasisConvention.FORMULA_PRICED)

#: Which limit key reads which view, and off which field of the line.
_METRICS: dict[str, tuple[ExposureView, str]] = {
    "flat_price_mt": (ExposureView.FLAT_PRICE, "quantity_mt"),
    "basis_mt": (ExposureView.BASIS, "quantity_mt"),
    "crush_mt": (ExposureView.CRUSH, "quantity_mt"),
    "residual_mt": (ExposureView.RESIDUAL, "quantity_mt"),
    "unhedged_mt": (ExposureView.RESIDUAL, "quantity_mt"),
    "net_mt": (ExposureView.RESIDUAL, "quantity_mt"),
    "month_mt": (ExposureView.CONTRACT_MONTH, "quantity_mt"),
    "first_notice_contracts": (ExposureView.FIRST_NOTICE, "contracts"),
    "fx_usd": (ExposureView.FX, "usd_exposure"),
}


@dataclass(frozen=True)
class ExposureLine:
    """One exposure, in one view, for one scope."""

    view: ExposureView
    key: str
    unit_move_label: str
    quantity_mt: float | None = None
    contracts: float | None = None
    #: P&L in USD for one unit of the move named by ``unit_move_label``.
    usd_per_unit_move: float | None = None
    #: The USD value at risk, where the view has one (FX).
    usd_exposure: float | None = None
    note: str = ""
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "view": self.view.value,
            "key": self.key,
            "unit_move_label": self.unit_move_label,
            "quantity_mt": None if self.quantity_mt is None else round(self.quantity_mt, 2),
            "contracts": None if self.contracts is None else round(self.contracts, 2),
            "usd_per_unit_move": (
                None if self.usd_per_unit_move is None else round(self.usd_per_unit_move, 2)
            ),
            "usd_exposure": None if self.usd_exposure is None else round(self.usd_exposure, 2),
            "note": self.note,
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True)
class ExposureReport:
    as_of: date
    lines: tuple[ExposureLine, ...] = ()
    warnings: tuple[str, ...] = ()
    method_note: str = field(default=(
        "Exposure is computed from entered positions and marked at delayed daily closes. "
        "Hedged tonnes are reported as basis exposure, not removed from the book."
    ))

    @property
    def is_empty(self) -> bool:
        return not self.lines

    def by_view(self, view: ExposureView) -> tuple[ExposureLine, ...]:
        return tuple(line for line in self.lines if line.view is view)

    def metric(self, key: str, scope: str) -> float | None:
        """The number a limit with this key and scope is measured against."""
        view, attribute = _METRICS[key]
        for line in self.by_view(view):
            if line.key == scope:
                return getattr(line, attribute)
        return None

    def metric_scopes(self, key: str) -> dict[str, float]:
        """Every scope this metric has a measured value for. Used by ``scope: "*"``."""
        view, attribute = _METRICS[key]
        return {
            line.key: getattr(line, attribute)
            for line in self.by_view(view)
            if getattr(line, attribute) is not None
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "method_note": self.method_note,
            "lines": [line.to_dict() for line in self.lines],
            "warnings": list(self.warnings),
        }


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def build_exposure(book: Book, valuation: BookValuation | None, *, as_of: date) -> ExposureReport:
    """Cut an entered book into every view.

    ``valuation`` supplies the marks and the FX rates it already looked up, so
    this module reads no database and no rate table of its own — the exposure
    and the P&L cannot end up marked at different prices.

    The marked positions are paired with the book positionally, which is the
    order :func:`analysis.futures.positions.value_book` builds them in (futures
    first, then physical, each in book order). ``tests/test_book_exposure.py``
    pins that pairing.
    """
    if book.is_empty:
        return ExposureReport(as_of=as_of)

    futures_marks = list(valuation.positions[: len(book.futures)]) if valuation else []
    physical_marks = list(valuation.positions[len(book.futures):]) if valuation else []
    fx_rates = dict(valuation.fx_rates) if valuation else {}

    lines: list[ExposureLine] = []
    warnings: list[str] = []

    futures_mt = _futures_mt_by_commodity(book)
    physical_mt = _physical_mt_by_commodity(book)

    lines.extend(_flat_price_lines(book, futures_mt))
    lines.extend(_basis_lines(book, futures_mt, physical_mt))
    lines.extend(_crush_lines(futures_mt, physical_mt))
    lines.extend(_fx_lines(book, physical_marks, fx_rates, warnings))
    lines.extend(_contract_month_lines(book, futures_marks, as_of=as_of))
    lines.extend(_first_notice_lines(book, futures_marks, as_of=as_of))
    lines.extend(_residual_lines(futures_mt, physical_mt))

    return ExposureReport(as_of=as_of, lines=tuple(lines), warnings=tuple(warnings))


def _futures_mt_by_commodity(book: Book) -> dict[str, float]:
    out: dict[str, float] = {}
    for position in book.futures:
        name = position.contract.spec.name
        out[name] = out.get(name, 0.0) + position.net_mt
    return out


def _physical_mt_by_commodity(book: Book) -> dict[str, float]:
    out: dict[str, float] = {}
    for position in book.physical:
        out[position.commodity] = out.get(position.commodity, 0.0) + position.quantity_mt
    return out


def _commodities(*tables: dict[str, float]) -> list[str]:
    seen: list[str] = []
    for table in tables:
        for name in table:
            if name not in seen:
                seen.append(name)
    return sorted(seen)


def _flat_price_lines(book: Book, futures_mt: dict[str, float]) -> list[ExposureLine]:
    exposed: dict[str, float] = {}
    unstated: dict[str, int] = {}
    conventions: dict[str, set[str]] = {}
    for position in book.physical:
        name = position.commodity
        conventions.setdefault(name, set()).add(position.basis_convention.value)
        if not position.pricing_stated:
            unstated[name] = unstated.get(name, 0) + 1
        if position.basis_convention in _BOARD_EXPOSED:
            exposed[name] = exposed.get(name, 0.0) + position.quantity_mt
        else:
            exposed.setdefault(name, 0.0)

    lines: list[ExposureLine] = []
    for name in _commodities(exposed, futures_mt):
        total = exposed.get(name, 0.0) + futures_mt.get(name, 0.0)
        note = "physical priced " + ", ".join(sorted(conventions.get(name, {"—"})))
        note += f"; futures {futures_mt.get(name, 0.0):,.0f} MT"
        warnings: tuple[str, ...] = ()
        if unstated.get(name):
            warnings = (
                f"pricing convention not stated on {unstated[name]} position(s) — counted at "
                "the most exposed reading; state `pricing:` in the position file to correct it",
            )
        lines.append(ExposureLine(
            view=ExposureView.FLAT_PRICE,
            key=name,
            unit_move_label="USD per 1 USD/MT on the board",
            quantity_mt=total,
            usd_per_unit_move=total,
            note=note,
            warnings=warnings,
        ))
    return lines


def _basis_lines(
    book: Book, futures_mt: dict[str, float], physical_mt: dict[str, float]
) -> list[ExposureLine]:
    unfixed: dict[str, float] = {}
    for position in book.physical:
        if position.basis_convention in _BASIS_UNFIXED:
            name = position.commodity
            unfixed[name] = unfixed.get(name, 0.0) + abs(position.quantity_mt)

    lines: list[ExposureLine] = []
    for name in sorted(physical_mt):
        physical = physical_mt[name]
        futures = futures_mt.get(name, 0.0)
        hedged = (
            min(abs(physical), abs(futures)) if physical * futures < 0 else 0.0
        )
        # max, not sum: tonnes that are both unpriced and hedged carry basis
        # risk once. Adding them would report twice the position as at risk.
        total = max(unfixed.get(name, 0.0), hedged)
        if total == 0:
            continue
        lines.append(ExposureLine(
            view=ExposureView.BASIS,
            key=name,
            unit_move_label="USD per 1 USD/MT on the basis",
            quantity_mt=total,
            usd_per_unit_move=total,
            note=(
                f"{hedged:,.0f} MT hedged with futures (flat-price risk converted to basis risk), "
                f"{unfixed.get(name, 0.0):,.0f} MT with an unfixed differential"
            ),
        ))
    return lines


def _crush_lines(
    futures_mt: dict[str, float], physical_mt: dict[str, float]
) -> list[ExposureLine]:
    net = {
        name: futures_mt.get(name, 0.0) + physical_mt.get(name, 0.0)
        for name in SOY_COMPLEX
    }
    if not any(abs(value) > 1e-9 for value in net.values()):
        return []

    beans = net["Soybeans"]
    meal_eq = -net["Soybean Meal"] / CRUSH_MEAL_YIELD_MT
    oil_eq = -net["Soybean Oil"] / CRUSH_OIL_YIELD_MT

    legs = (beans, meal_eq, oil_eq)
    if all(value > 0 for value in legs):
        covered = min(legs)                       # long the crush
    elif all(value < 0 for value in legs):
        covered = max(legs)                       # short the crush (reverse crush)
    else:
        covered = 0.0

    return [ExposureLine(
        view=ExposureView.CRUSH,
        key="Soy complex",
        unit_move_label="USD per 1 USD/MT on the crush margin",
        quantity_mt=covered,
        usd_per_unit_move=abs(covered),
        note=(
            f"bean-equivalent legs: beans {beans:,.0f} MT, "
            f"meal {meal_eq:,.0f} MT-equivalent, oil {oil_eq:,.0f} MT-equivalent; "
            f"crush on for {covered:,.0f} MT of beans, the rest is flat price"
        ),
        warnings=(
            () if covered else
            ("the three legs are not on the same side, so no crush position is on — the legs "
             "are separate flat-price positions",)
        ),
    )]


def _fx_lines(
    book: Book,
    physical_marks: list[MarkedPosition],
    fx_rates: dict[str, tuple[date, float] | None],
    report_warnings: list[str],
) -> list[ExposureLine]:
    exposure: dict[str, float] = {}
    unvalued: dict[str, int] = {}
    for index, position in enumerate(book.physical):
        if position.currency == "USD":
            continue
        pair = position.fx_pair
        if not pair:
            report_warnings.append(
                f"{position.commodity}: priced in {position.currency} with no fx_pair named, so "
                "its currency exposure cannot be attributed to a rate"
            )
            continue
        mark = physical_marks[index].mark if index < len(physical_marks) else None
        per_mt = mark if mark is not None else position.average_cost_usd_mt
        if per_mt is None:
            unvalued[pair] = unvalued.get(pair, 0) + 1
            exposure.setdefault(pair, 0.0)
            continue
        exposure[pair] = exposure.get(pair, 0.0) + abs(position.quantity_mt) * per_mt

    lines: list[ExposureLine] = []
    for pair in sorted(exposure):
        rate = fx_rates.get(pair)
        warnings: list[str] = []
        if rate is None:
            warnings.append(
                f"no rate for {pair} on this session, so the move is unquantified — the exposure "
                "is real and its dollar value is not known here"
            )
        if unvalued.get(pair):
            warnings.append(
                f"{unvalued[pair]} position(s) in {pair} carry neither a mark nor an average "
                "cost, so their notional is excluded rather than guessed"
            )
        lines.append(ExposureLine(
            view=ExposureView.FX,
            key=pair,
            unit_move_label="USD per 1% move in the pair",
            usd_exposure=exposure[pair],
            usd_per_unit_move=None if rate is None else exposure[pair] * 0.01,
            note=(
                f"{exposure[pair]:,.0f} USD of position priced in the home currency"
                + (f", rate {rate[1]:.6f} on {rate[0].isoformat()}" if rate else "")
            ),
            warnings=tuple(warnings),
        ))
    return lines


def _contract_month_lines(
    book: Book, futures_marks: list[MarkedPosition], *, as_of: date
) -> list[ExposureLine]:
    net: dict[str, float] = {}
    contracts: dict[str, NamedContract] = {}
    for position in book.futures:
        symbol = position.contract.symbol
        net[symbol] = net.get(symbol, 0.0) + position.net_contracts
        contracts[symbol] = position.contract

    lines: list[ExposureLine] = []
    for symbol in sorted(net):
        lots = net[symbol]
        if abs(lots) < 1e-9:
            continue
        contract = contracts[symbol]
        spec = contract.spec
        days = contract.days_to_expiry(as_of)
        lines.append(ExposureLine(
            view=ExposureView.CONTRACT_MONTH,
            key=symbol,
            unit_move_label=f"USD per one tick ({spec.tick_size:g} {spec.native_unit.value})",
            quantity_mt=lots * spec.mt_per_contract,
            contracts=lots,
            usd_per_unit_move=lots * spec.tick_value_usd,
            note=(
                f"{contract.delivery_month} delivery"
                + (f", last trade in {days} business days" if days is not None
                   else ", expiry rule not encoded")
            ),
        ))
    return lines


def _first_notice_lines(
    book: Book, futures_marks: list[MarkedPosition], *, as_of: date
) -> list[ExposureLine]:
    """Open lots as the notice day approaches.

    First notice, not last trade: a merchant still long past FND is exposed to
    *delivery*, which is a different and worse problem than an expiring hedge.
    """
    import config

    window = int(getattr(config, "FIRST_NOTICE_WARNING_DAYS", 10))
    net: dict[str, float] = {}
    contracts: dict[str, NamedContract] = {}
    for position in book.futures:
        symbol = position.contract.symbol
        net[symbol] = net.get(symbol, 0.0) + position.net_contracts
        contracts[symbol] = position.contract

    lines: list[ExposureLine] = []
    for symbol in sorted(net):
        lots = net[symbol]
        if abs(lots) < 1e-9:
            continue
        contract = contracts[symbol]
        notice = contract.first_notice
        if notice is not None:
            days = (notice - as_of).days
            if days > window:
                continue
            note = (
                f"first notice {notice.isoformat()}"
                + (f", {days} days away" if days >= 0 else f", {abs(days)} days ago")
            )
            warnings: tuple[str, ...] = (
                ("first notice has passed — a long is exposed to delivery",) if days < 0 else ()
            )
        else:
            # No encoded rule. Only worth saying once the delivery month is
            # close, and it must be said: silence here reads as "no notice
            # risk", which is a claim this project cannot make.
            month_start = contract.contract_month_date
            if (month_start.year, month_start.month) > _month_after(as_of):
                continue
            rule = spec_for(contract.spec.name).first_notice_rule
            note = f"{contract.delivery_month} delivery, first notice date unknown to this project"
            warnings = (
                "this contract has no notice-day mechanism — the delivery obligation attaches "
                "at the close of the last trading day"
                if rule else
                "the first notice rule for this contract is not encoded here — check it with "
                "your broker before the delivery month opens",
            )
        lines.append(ExposureLine(
            view=ExposureView.FIRST_NOTICE,
            key=symbol,
            unit_move_label="lots open into the notice window",
            quantity_mt=lots * contract.spec.mt_per_contract,
            contracts=lots,
            note=note,
            warnings=warnings,
        ))
    return lines


def _month_after(as_of: date) -> tuple[int, int]:
    return (as_of.year + 1, 1) if as_of.month == 12 else (as_of.year, as_of.month + 1)


def _residual_lines(
    futures_mt: dict[str, float], physical_mt: dict[str, float]
) -> list[ExposureLine]:
    lines: list[ExposureLine] = []
    for name in sorted(physical_mt):
        residual = physical_mt[name] + futures_mt.get(name, 0.0)
        lines.append(ExposureLine(
            view=ExposureView.RESIDUAL,
            key=name,
            unit_move_label="USD per 1 USD/MT on the board",
            quantity_mt=residual,
            usd_per_unit_move=residual,
            note=(
                f"{physical_mt[name]:,.0f} MT physical against {futures_mt.get(name, 0.0):,.0f} MT "
                "of futures" + (" — over-hedged" if residual * physical_mt[name] < 0 else "")
            ),
        ))
    return lines


__all__ = [
    "ExposureLine",
    "ExposureReport",
    "ExposureView",
    "build_exposure",
]
