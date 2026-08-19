"""Desk limits: configurable lines, checked and reported, never enforced.

This software does not stop anyone doing anything. It says which line was
crossed, by how much, and against which exposure — and it says so on the page
rather than in a log, because a limit nobody sees is not a limit.

Two things make it a *desk* limit rather than a number in a docstring:

* **It is keyed to an exposure view, not to a position count.** ``net_mt`` was
  the whole vocabulary before, and a net tonnage cannot express "no more than
  6,000 MT of flat price" or "nothing open past first notice" — the two lines a
  merchant's mandate actually contains. Every key here resolves through
  :mod:`analysis.futures.exposure`, so the limit and the risk report cannot
  disagree about what the number means.
* **It has a warning level.** A limit that is only ever ``ok`` or ``breached``
  arrives too late to act on. ``warn_at`` is optional and, when set, must sit
  below the maximum — a warning above the breach would never fire, which is the
  kind of misconfiguration that looks like safety.

An unknown key raises. Silently skipping one would leave a desk believing a
mandate was being checked when nothing was checking it.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    from analysis.futures.exposure import ExposureReport


class LimitError(ValueError):
    """A limit was configured in a way that cannot be checked."""


class LimitStatus(str, Enum):
    OK = "ok"
    WARN = "warn"
    BREACH = "breach"


#: Every configurable key, what it measures and in what unit. The description
#: is rendered beside the limit on the page: a reader must be able to reproduce
#: the observed number from the exposure table without reading this module.
LIMIT_KEYS: dict[str, tuple[str, str]] = {
    "flat_price_mt": ("MT", "tonnes still exposed to a move in the board price"),
    "basis_mt": ("MT", "tonnes exposed to a move in the basis, hedged tonnes included"),
    "crush_mt": ("MT", "bean-equivalent tonnes exposed to a move in the crush margin"),
    "residual_mt": ("MT", "physical tonnes the futures hedge does not cover"),
    "month_mt": ("MT", "tonnes in one named delivery month"),
    "first_notice_contracts": ("lots", "contracts open inside the first-notice window"),
    "fx_usd": ("USD", "USD value of the position exposed to one currency pair"),
    "notional_usd": ("USD", "marked value of the physical book"),
    "loss_usd": ("USD", "unrealised mark-to-market loss, management basis"),
    # The two keys that predate the exposure views, kept so an existing file
    # still parses. Both resolve to an exposure metric rather than to their own
    # arithmetic, so there is one definition of each number and not two.
    "net_mt": ("MT", "net position in a commodity, physical and futures together"),
    "unhedged_mt": ("MT", "physical tonnes the futures hedge does not cover"),
}

#: Keys measured over the whole book rather than per commodity or per pair.
PORTFOLIO_KEYS = frozenset({"notional_usd", "loss_usd"})


@dataclass(frozen=True)
class DeskLimit:
    """One configurable line."""

    key: str
    scope: str          # a commodity, a contract symbol, an FX pair, or "*"
    maximum: float
    warn_at: float | None = None
    note: str = ""

    def __post_init__(self) -> None:
        if self.key not in LIMIT_KEYS:
            raise LimitError(
                f"unknown limit key {self.key!r} — known keys: {sorted(LIMIT_KEYS)}"
            )
        if self.maximum < 0:
            raise LimitError(f"{self.key}: a maximum of {self.maximum} cannot be crossed")
        if self.warn_at is not None and self.warn_at >= self.maximum:
            raise LimitError(
                f"{self.key}: warn_at {self.warn_at} is not below the maximum {self.maximum} — "
                "a warning that fires only after the breach is not a warning"
            )

    @property
    def unit(self) -> str:
        return LIMIT_KEYS[self.key][0]

    @property
    def measures(self) -> str:
        return LIMIT_KEYS[self.key][1]

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "scope": self.scope,
            "maximum": self.maximum,
            "warn_at": self.warn_at,
            "unit": self.unit,
            "measures": self.measures,
            "note": self.note,
        }


@dataclass(frozen=True)
class LimitCheck:
    """One limit, measured against one scope."""

    limit: DeskLimit
    scope_key: str
    observed: float
    status: LimitStatus

    @property
    def excess(self) -> float:
        return abs(self.observed) - self.limit.maximum

    @property
    def headroom(self) -> float:
        return self.limit.maximum - abs(self.observed)

    @property
    def is_breach(self) -> bool:
        return self.status is LimitStatus.BREACH

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.limit.to_dict(),
            "scope_key": self.scope_key,
            "observed": round(self.observed, 2),
            "excess": round(self.excess, 2),
            "headroom": round(self.headroom, 2),
            "status": self.status.value,
        }


#: Kept under the old name because ``analysis/futures/alerts.py`` and the page
#: both speak of breaches. A breach *is* a check whose status is ``breach``.
LimitBreach = LimitCheck


def evaluate(
    limits: tuple[DeskLimit, ...],
    *,
    exposure: ExposureReport | None = None,
    total_unrealised_usd: float | None = None,
    notional_usd: float | None = None,
) -> tuple[LimitCheck, ...]:
    """Measure every limit against every scope it covers.

    Returns a check per (limit, scope) pair, including the ones that are fine —
    a desk needs to see headroom, not only breaches. A limit whose metric
    cannot be measured (no exposure report, no mark) produces **no check**
    rather than a passing one: an unmeasured limit that renders green is the
    single most dangerous output this module could produce.
    """
    checks: list[LimitCheck] = []
    for limit in limits:
        for scope_key, observed in _observations(
            limit,
            exposure=exposure,
            total_unrealised_usd=total_unrealised_usd,
            notional_usd=notional_usd,
        ):
            checks.append(LimitCheck(limit, scope_key, observed, _status(limit, observed)))
    return tuple(checks)


def _status(limit: DeskLimit, observed: float) -> LimitStatus:
    magnitude = abs(observed)
    if magnitude > limit.maximum:
        return LimitStatus.BREACH
    if limit.warn_at is not None and magnitude >= limit.warn_at:
        return LimitStatus.WARN
    return LimitStatus.OK


def _observations(
    limit: DeskLimit,
    *,
    exposure: ExposureReport | None,
    total_unrealised_usd: float | None,
    notional_usd: float | None,
) -> list[tuple[str, float]]:
    if limit.key == "loss_usd":
        # Only a loss can cross a loss limit. A profit of the same magnitude
        # crossing it would be an alert nobody could act on.
        if total_unrealised_usd is None or total_unrealised_usd >= 0:
            return []
        return [("book", total_unrealised_usd)]
    if limit.key == "notional_usd":
        return [] if notional_usd is None else [("book", notional_usd)]
    if exposure is None:
        return []
    if limit.scope == "*":
        return list(exposure.metric_scopes(limit.key).items())
    value = exposure.metric(limit.key, limit.scope)
    return [] if value is None else [(limit.scope, value)]


def breaches(checks: tuple[LimitCheck, ...]) -> tuple[LimitCheck, ...]:
    return tuple(check for check in checks if check.is_breach)


def warnings(checks: tuple[LimitCheck, ...]) -> tuple[LimitCheck, ...]:
    return tuple(check for check in checks if check.status is LimitStatus.WARN)


__all__ = [
    "LIMIT_KEYS",
    "PORTFOLIO_KEYS",
    "DeskLimit",
    "LimitBreach",
    "LimitCheck",
    "LimitError",
    "LimitStatus",
    "breaches",
    "evaluate",
    "warnings",
]
