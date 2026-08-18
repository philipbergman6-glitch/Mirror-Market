"""Options architecture: the interface, and an honest account of what is missing.

There is **no options data in this project**. No source it ingests publishes a
chain, a strike ladder, a premium, an implied volatility or an open interest
figure for any option on any of these contracts. That is the finding, and the
whole of this module follows from it:

* The chain/volatility/Greeks *interface* is defined here, so substituting a
  provider that does publish one is a class, not a redesign.
* :func:`fetch_chain` returns :class:`ChainUnavailable` — always, today — with
  the reason and the list of what a provider would have to supply. It does not
  return an empty chain, because an empty chain renders as "no options traded"
  and that is a statement about the market rather than about us.
* A **manual-input workflow** lets a trader type in the premium or the implied
  volatility their broker quoted and get Greeks and scenarios from it. Every
  such input is stamped ``PriceType.MANUAL`` and carries the user's own source
  string, so nothing on the page can be mistaken for something we sourced.
* :func:`black76` prices and differentiates European options on futures under
  explicitly stated assumptions, tested against published reference values.
  It is called only with inputs the user supplied or the board provided; it is
  never used to fill in a missing market price.

Assumptions of the Black-76 model, stated because they are load-bearing
------------------------------------------------------------------------
1. The underlying is a **futures price**, and it is lognormal with constant
   volatility to expiry. Agricultural futures are not: they have volatility
   smiles that steepen sharply into weather markets, so a single vol produces
   a systematically wrong price away from the money.
2. The option is **European**. Most listed grain options are American-style
   and can be exercised early; for options on futures with no dividend the
   early-exercise premium is usually small but is **not zero**, and it is not
   modelled here. Prices from this function are therefore a floor for an
   American option, not a value for one.
3. The discount rate is continuously compounded and constant. It is an input,
   not something this module sources; the ``economic`` layer's Treasury series
   is a reasonable choice and the caller must make it deliberately.
4. Time to expiry is in **years, on a 365-day calendar**, measured to the
   option's own expiration — which is *not* the futures contract's last trade
   date. This project does not encode option expiry rules, so the date is a
   required input and there is no default.

Anything computed here is a model value. It is labelled a model value
everywhere it surfaces.
"""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from analysis.futures.domain import (
    METHOD_VERSION,
    NamedContract,
    PriceType,
    UnknownContract,
    parse_symbol,
)

log = logging.getLogger(__name__)

DAYS_PER_YEAR = 365.0


class OptionRight(str, Enum):
    CALL = "call"
    PUT = "put"


class OptionStyle(str, Enum):
    EUROPEAN = "european"
    AMERICAN = "american"


@dataclass(frozen=True)
class OptionContract:
    """Identity of one option. The underlying is always a *named* future."""

    underlying: NamedContract
    right: OptionRight
    strike: float                 # native units of the underlying
    expiry: date                  # the option's own expiration, not the future's
    style: OptionStyle = OptionStyle.AMERICAN
    exchange_symbol: str = ""

    @property
    def symbol(self) -> str:
        return self.exchange_symbol or (
            f"{self.underlying.symbol} {self.strike:g} {self.right.value[0].upper()}"
        )

    def years_to_expiry(self, as_of: date) -> float:
        return max((self.expiry - as_of).days, 0) / DAYS_PER_YEAR

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "underlying": self.underlying.symbol,
            "right": self.right.value,
            "strike": self.strike,
            "expiry": self.expiry.isoformat(),
            "style": self.style.value,
        }


@dataclass(frozen=True)
class Greeks:
    """Per-contract sensitivities.

    Units, because unlabelled Greeks are the classic source of a 100x error:
    ``delta`` is dimensionless; ``gamma`` is delta per one native price unit;
    ``vega`` is premium per **one volatility point** (1% = 0.01), already
    scaled; ``theta`` is premium per **calendar day**; ``rho`` is premium per
    one percentage point of rate.
    """

    delta: float
    gamma: float
    vega: float
    theta: float
    rho: float

    def to_dict(self) -> dict[str, float]:
        return {
            "delta": round(self.delta, 6),
            "gamma": round(self.gamma, 8),
            "vega_per_vol_point": round(self.vega, 6),
            "theta_per_day": round(self.theta, 6),
            "rho_per_rate_point": round(self.rho, 6),
        }


@dataclass(frozen=True)
class OptionValuation:
    """A model value, with every input it was produced from."""

    contract: OptionContract
    as_of: date
    forward: float
    volatility: float
    rate: float
    years: float
    premium: float
    premium_usd: float
    greeks: Greeks
    price_type: PriceType
    volatility_source: str
    model: str = "black76"
    method_version: str = METHOD_VERSION
    caveats: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract": self.contract.to_dict(),
            "as_of": self.as_of.isoformat(),
            "model": self.model,
            "method_version": self.method_version,
            "forward": self.forward,
            "volatility": self.volatility,
            "volatility_source": self.volatility_source,
            "rate": self.rate,
            "years": round(self.years, 6),
            "premium": round(self.premium, 6),
            "premium_usd": round(self.premium_usd, 2),
            "price_type": self.price_type.value,
            "greeks": self.greeks.to_dict(),
            "caveats": list(self.caveats),
        }


# ---------------------------------------------------------------------------
# Black-76
# ---------------------------------------------------------------------------


def _norm_cdf(x: float) -> float:
    return 0.5 * math.erfc(-x / math.sqrt(2.0))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _d1_d2(forward: float, strike: float, volatility: float, years: float) -> tuple[float, float]:
    vol_sqrt_t = volatility * math.sqrt(years)
    d1 = (math.log(forward / strike) + 0.5 * volatility * volatility * years) / vol_sqrt_t
    return d1, d1 - vol_sqrt_t


def black76_price(
    forward: float, strike: float, volatility: float, years: float, rate: float, right: OptionRight
) -> float:
    """Black-76 premium in the underlying's native price units.

    Degenerate inputs are handled as intrinsic value rather than raising: an
    expired or zero-volatility option has a well-defined value and refusing to
    state it would push the special case into every caller.
    """
    if forward <= 0 or strike <= 0:
        raise ValueError("Black-76 requires a positive forward and strike")
    if years <= 0 or volatility <= 0:
        intrinsic = (
            max(forward - strike, 0.0) if right is OptionRight.CALL else max(strike - forward, 0.0)
        )
        return math.exp(-rate * max(years, 0.0)) * intrinsic

    d1, d2 = _d1_d2(forward, strike, volatility, years)
    discount = math.exp(-rate * years)
    if right is OptionRight.CALL:
        return discount * (forward * _norm_cdf(d1) - strike * _norm_cdf(d2))
    return discount * (strike * _norm_cdf(-d2) - forward * _norm_cdf(-d1))


def black76_greeks(
    forward: float, strike: float, volatility: float, years: float, rate: float, right: OptionRight
) -> Greeks:
    """Analytic Greeks for Black-76, scaled to the units documented on :class:`Greeks`."""
    if years <= 0 or volatility <= 0:
        # At expiry the option is its intrinsic value: delta is 0 or 1 (signed
        # for a put) and every other sensitivity is zero. Reporting a spurious
        # gamma spike here would be a model artefact, not a risk.
        in_money = forward > strike if right is OptionRight.CALL else forward < strike
        delta = (1.0 if right is OptionRight.CALL else -1.0) if in_money else 0.0
        return Greeks(delta=delta, gamma=0.0, vega=0.0, theta=0.0, rho=0.0)

    d1, d2 = _d1_d2(forward, strike, volatility, years)
    discount = math.exp(-rate * years)
    sqrt_t = math.sqrt(years)
    pdf = _norm_pdf(d1)

    if right is OptionRight.CALL:
        delta = discount * _norm_cdf(d1)
        price = discount * (forward * _norm_cdf(d1) - strike * _norm_cdf(d2))
    else:
        delta = -discount * _norm_cdf(-d1)
        price = discount * (strike * _norm_cdf(-d2) - forward * _norm_cdf(-d1))

    gamma = discount * pdf / (forward * volatility * sqrt_t)
    vega = discount * forward * pdf * sqrt_t
    # Theta is -dV/dT; the identity F n(d1) = K n(d2) collapses the two normal
    # terms into one, which is why only d1 appears.
    theta = rate * price - discount * forward * pdf * volatility / (2 * sqrt_t)
    rho = -years * price

    return Greeks(
        delta=delta,
        gamma=gamma,
        vega=vega / 100.0,          # per one volatility *point*
        theta=theta / DAYS_PER_YEAR,  # per calendar day
        rho=rho / 100.0,            # per one rate point
    )


def implied_volatility(
    premium: float,
    forward: float,
    strike: float,
    years: float,
    rate: float,
    right: OptionRight,
    *,
    tolerance: float = 1e-8,
    max_iterations: int = 200,
) -> float | None:
    """Implied vol by bisection, or None when the premium admits no solution.

    Bisection rather than Newton: it cannot diverge, and a hedger reading an
    implied vol needs it to be right rather than fast. None is returned for a
    premium below intrinsic or above the forward — both are arbitrage
    violations, and returning a number for them would be inventing one.
    """
    if years <= 0 or forward <= 0 or strike <= 0 or premium < 0:
        return None
    intrinsic = black76_price(forward, strike, 1e-12, years, rate, right)
    if premium < intrinsic - tolerance:
        return None
    low, high = 1e-6, 5.0
    if premium > black76_price(forward, strike, high, years, rate, right):
        return None
    for _ in range(max_iterations):
        mid = 0.5 * (low + high)
        value = black76_price(forward, strike, mid, years, rate, right)
        if abs(value - premium) < tolerance:
            return mid
        if value > premium:
            high = mid
        else:
            low = mid
    return 0.5 * (low + high)


def value_option(
    contract: OptionContract,
    *,
    as_of: date,
    forward: float,
    volatility: float,
    rate: float,
    volatility_source: str,
    price_type: PriceType = PriceType.MANUAL,
) -> OptionValuation:
    """Model-value one option from explicitly supplied inputs.

    ``volatility_source`` is required, and it is not decoration: no source in
    this stack publishes an implied volatility, so every vol reaching this
    function came from a human. Recording whose number it was is the difference
    between a model output and a fabrication.
    """
    if not volatility_source.strip():
        raise ValueError(
            "volatility_source is required — no ingested source publishes an implied "
            "volatility, so every vol here is somebody's input and must say whose"
        )
    years = contract.years_to_expiry(as_of)
    premium = black76_price(forward, contract.strike, volatility, years, rate, contract.right)
    greeks = black76_greeks(forward, contract.strike, volatility, years, rate, contract.right)
    spec = contract.underlying.spec

    caveats = [
        "Black-76 model value, not a market price — no source here publishes option prices",
        "assumes a European exercise and one constant volatility to expiry",
    ]
    if contract.style is OptionStyle.AMERICAN:
        caveats.append(
            "this is an American-style option; the early-exercise premium is not modelled, "
            "so the value below is a floor rather than a price"
        )

    return OptionValuation(
        contract=contract,
        as_of=as_of,
        forward=forward,
        volatility=volatility,
        rate=rate,
        years=years,
        premium=premium,
        # A premium is quoted in the same native units as the underlying, so it
        # converts to money through exactly the same contract-size arithmetic.
        premium_usd=spec.value_usd(premium),
        greeks=greeks,
        price_type=price_type,
        volatility_source=volatility_source,
        caveats=tuple(caveats),
    )


# ---------------------------------------------------------------------------
# The chain interface, and the reason it has no implementation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ChainQuote:
    """One strike as a provider would give it. Nothing constructs these today."""

    contract: OptionContract
    bid: float | None
    ask: float | None
    settlement: float | None
    implied_volatility: float | None
    volume: float | None
    open_interest: float | None
    observation_date: date


@dataclass(frozen=True)
class OptionChain:
    underlying: NamedContract
    observation_date: date
    quotes: tuple[ChainQuote, ...]
    provider: str


@dataclass(frozen=True)
class ChainUnavailable:
    """Why there is no chain, and what a provider would have to supply."""

    underlying: NamedContract
    reason: str
    required_fields: tuple[str, ...] = (
        "strike ladder", "call and put settlement or bid/ask", "option expiry date",
        "implied volatility or enough to compute it", "volume", "open interest",
    )
    manual_workflow: str = (
        "Enter the premium or the implied volatility your broker quoted, together with the "
        "option's expiry date and a discount rate, and the Black-76 value and Greeks will be "
        "computed from those inputs and labelled as model values."
    )

    @property
    def available(self) -> bool:
        return False

    def to_dict(self) -> dict[str, Any]:
        return {
            "available": False,
            "underlying": self.underlying.symbol,
            "reason": self.reason,
            "required_fields": list(self.required_fields),
            "manual_workflow": self.manual_workflow,
        }


@runtime_checkable
class OptionChainProvider(Protocol):
    """The seam an options feed would plug into."""

    def chain(self, underlying: NamedContract, *, as_of: date) -> OptionChain | ChainUnavailable:
        ...


NO_CHAIN_REASON = (
    "No source ingested by this project publishes an option chain for CBOT, CME or ICE "
    "contracts. yfinance serves equity option chains only, and none of the twenty-five "
    "data layers here carries a strike, a premium or an implied volatility. Rather than "
    "render an empty ladder — which would read as 'no options traded' — the chain is "
    "reported unavailable and the manual-input workflow is offered instead."
)


def fetch_chain(underlying: NamedContract, *, as_of: date) -> ChainUnavailable:
    """Always unavailable today. Kept as the call site a provider replaces."""
    return ChainUnavailable(underlying=underlying, reason=NO_CHAIN_REASON)


def chain_status(underlying: NamedContract | None = None) -> dict[str, Any]:
    """Render-ready statement of the options position, for the page."""
    return {
        "available": False,
        "reason": NO_CHAIN_REASON,
        "underlying": underlying.symbol if underlying else None,
        "manual_workflow": ChainUnavailable(
            underlying=underlying, reason=NO_CHAIN_REASON
        ).manual_workflow if underlying else (
            "Enter a premium or an implied volatility from your broker to get Black-76 values "
            "and Greeks, labelled as model output."
        ),
        "model": "black76",
        "model_assumptions": [
            "underlying is a futures price, lognormal with one constant volatility to expiry",
            "European exercise; listed grain options are American and the early-exercise "
            "premium is not modelled",
            "constant continuously-compounded discount rate, supplied as an input",
            "time to expiry in years on a 365-day calendar, to the option's own expiration",
        ],
    }


# ---------------------------------------------------------------------------
# The manual ladder — the workflow described above, actually wired up
# ---------------------------------------------------------------------------
#
# The chain is unavailable and stays unavailable; this is the other half of that
# sentence. It was described in :class:`ChainUnavailable.manual_workflow` from
# the start and had no entry point, which made the offer unusable — the module
# said "type in your broker's quote" and nothing could be typed in.
#
# Shape and failure mode are copied deliberately from
# ``analysis/futures/positions.py``: a directory of YAML documents, a missing
# directory is an empty ladder, and a *present but malformed* file raises. The
# reason is the same one — "nothing entered" and "something entered wrongly"
# must not render alike — and the reason for copying rather than inventing is
# that a trader should not have to learn two file formats for the two things
# only they can supply.


class OptionEntryError(ValueError):
    """A manual option document could not be read. Never swallowed into empty."""


@dataclass(frozen=True)
class ManualQuote:
    """One option, as a human typed it off their own screen.

    Exactly one of ``premium`` and ``implied_volatility`` is required, and the
    other is derived: given a premium the vol is backed out by bisection, given
    a vol the premium is priced. ``source`` is mandatory and free text — it is
    the name of whoever quoted it, and it is what stops a model number and a
    market number from becoming indistinguishable three screens later.
    """

    contract: OptionContract
    source: str
    quoted_on: date
    premium: float | None = None
    implied_volatility: float | None = None
    note: str = ""

    def __post_init__(self) -> None:
        if not self.source.strip():
            raise OptionEntryError(
                f"{self.contract.symbol}: source is required — a hand-entered option "
                "quote must say who quoted it"
            )
        supplied = [self.premium is not None, self.implied_volatility is not None]
        if sum(supplied) != 1:
            raise OptionEntryError(
                f"{self.contract.symbol}: give exactly one of premium or "
                "implied_volatility — the other is derived from it. Supplying both "
                "would let two inconsistent numbers sit on one row with nothing "
                "saying which was believed"
            )


@dataclass(frozen=True)
class ManualLadder:
    """Every hand-entered option, and where each document came from."""

    quotes: tuple[ManualQuote, ...] = ()
    loaded_from: tuple[str, ...] = ()

    @property
    def is_empty(self) -> bool:
        return not self.quotes


def value_manual_ladder(
    ladder: ManualLadder, *, as_of: date, forwards: dict[str, float], rate: float,
) -> tuple[dict[str, Any], ...]:
    """Value each hand-entered option against the board's own forward.

    ``forwards`` is keyed by underlying symbol (``ZSX26``) in the underlying's
    native units. A quote whose underlying is not on the board is *not* valued
    — it is returned with its reason, because pricing an option against a
    forward we do not have would mean inventing the one input the trader could
    not give us.
    """
    out: list[dict[str, Any]] = []
    for quote in ladder.quotes:
        symbol = quote.contract.underlying.symbol
        forward = forwards.get(symbol)
        if forward is None:
            out.append({
                "contract": quote.contract.to_dict(),
                "source": quote.source,
                "quoted_on": quote.quoted_on.isoformat(),
                "valued": False,
                "reason": (
                    f"no board price for {symbol} on this session, so there is no forward "
                    "to value it against"
                ),
            })
            continue

        volatility = quote.implied_volatility
        derived_from = "entered implied volatility"
        if volatility is None:
            assert quote.premium is not None      # guaranteed by __post_init__
            volatility = implied_volatility(
                quote.premium, forward, quote.contract.strike,
                quote.contract.years_to_expiry(as_of), rate, quote.contract.right,
            )
            derived_from = "backed out of the entered premium by bisection"
        if volatility is None:
            out.append({
                "contract": quote.contract.to_dict(),
                "source": quote.source,
                "quoted_on": quote.quoted_on.isoformat(),
                "valued": False,
                "reason": (
                    "the entered premium is outside the model's arbitrage bounds, so no "
                    "implied volatility exists for it — check the premium's units"
                ),
            })
            continue

        valuation = value_option(
            quote.contract, as_of=as_of, forward=forward, volatility=volatility,
            rate=rate, volatility_source=f"{quote.source} ({derived_from})",
        )
        payload = valuation.to_dict()
        payload.update({
            "valued": True,
            "source": quote.source,
            "quoted_on": quote.quoted_on.isoformat(),
            "entered_premium": quote.premium,
            "volatility_derived_from": derived_from,
            "note": quote.note,
        })
        out.append(payload)
    return tuple(out)


def parse_ladder(payload: dict[str, Any], *, where: str) -> ManualLadder:
    """Parse one manual-options document. Raises on anything it cannot read."""
    if not isinstance(payload, dict):
        raise OptionEntryError(f"{where}: expected a mapping with an 'options' key")
    rows = payload.get("options") or []
    if not isinstance(rows, list):
        raise OptionEntryError(f"{where}: 'options' must be a list")

    quotes: list[ManualQuote] = []
    for index, row in enumerate(rows, start=1):
        at = f"{where}[options][{index}]"
        if not isinstance(row, dict):
            raise OptionEntryError(f"{at}: expected a mapping")
        try:
            underlying = parse_symbol(str(row["underlying"]))
            right = OptionRight(str(row["right"]).strip().lower())
            strike = float(row["strike"])
            expiry = date.fromisoformat(str(row["expiry"]))
            quoted_on = date.fromisoformat(str(row["quoted_on"]))
        except KeyError as exc:
            raise OptionEntryError(f"{at}: missing required field {exc}") from exc
        except (UnknownContract, ValueError) as exc:
            raise OptionEntryError(f"{at}: {exc}") from exc

        style = OptionStyle(str(row.get("style", "american")).strip().lower())
        quotes.append(ManualQuote(
            contract=OptionContract(
                underlying=underlying, right=right, strike=strike,
                expiry=expiry, style=style,
            ),
            source=str(row.get("source", "")),
            quoted_on=quoted_on,
            premium=None if row.get("premium") in (None, "") else float(row["premium"]),
            implied_volatility=(
                None if row.get("implied_volatility") in (None, "")
                else float(row["implied_volatility"])
            ),
            note=str(row.get("note", "")),
        ))
    return ManualLadder(quotes=tuple(quotes), loaded_from=(where,))


def load_ladder(directory: str | os.PathLike[str] | None = None) -> ManualLadder:
    """Read every ``*.yml`` manual-options document in ``directory``.

    A missing directory is an empty ladder. A malformed one raises — see the
    section comment above for why those two must not look alike.
    """
    import config

    root = Path(str(directory) if directory is not None else getattr(
        config, "OPTIONS_DIR", "data/reference/options"
    ))
    if not root.is_dir():
        log.info("no options directory at %s — no options entered", root)
        return ManualLadder()

    import yaml

    quotes: list[ManualQuote] = []
    sources: list[str] = []
    for path in sorted(root.glob("*.yml")):
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        ladder = parse_ladder(payload, where=str(path))
        quotes.extend(ladder.quotes)
        sources.append(str(path))
    return ManualLadder(quotes=tuple(quotes), loaded_from=tuple(sources))


__all__ = [
    "DAYS_PER_YEAR",
    "NO_CHAIN_REASON",
    "ChainQuote",
    "ChainUnavailable",
    "Greeks",
    "ManualLadder",
    "ManualQuote",
    "OptionEntryError",
    "OptionChain",
    "OptionChainProvider",
    "OptionContract",
    "OptionRight",
    "OptionStyle",
    "OptionValuation",
    "black76_greeks",
    "black76_price",
    "chain_status",
    "fetch_chain",
    "implied_volatility",
    "load_ladder",
    "parse_ladder",
    "value_manual_ladder",
    "value_option",
]
