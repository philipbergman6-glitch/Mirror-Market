"""TradingView symbols for named contracts — the one labelled exception.

`CLAUDE.md` says this product is not a real-time market feed, and every number
it renders is measured against that. The embedded TradingView chart on the
workstation's contract rows is a **deliberate, labelled exception** to that
line (owner decision 2026-08-23, [#320]), not a softening of it: the widget is
a third-party iframe showing TradingView's own ~10-minute-delayed exchange
data, framed as foreign territory, and nothing inside it passes through this
project's price vocabulary. It is not a settlement, it is not our observation,
and `pricing.semantics` never sees it.

What lives here is the *only* thing that must be right on our side: the
mapping from a :class:`~analysis.futures.domain.NamedContract` to the string
TradingView keys that contract by. Two rules hold it honest.

**A venue is a registry entry, never a branch** (invariant 5). ``ZMU26`` is
``CBOT:ZMU2026`` because TradingView prefixes with the exchange and spells the
year in full; whether ICE or CME follow the same convention is not something
this project may assume from one checked venue. So the registry maps only what
has been verified against the live symbol page, and the follow-up ticket adds
rows to it rather than cases to a function.

**An unmapped venue yields nothing** (invariant 2). A guessed prefix does not
fail loudly — TradingView renders *something*, and a chart of the wrong
contract beside a correct price is exactly the wrong-number-worse-than-a-gap
trade invariant 11 refuses. The caller renders no expander at all.

[#320]: https://github.com/philipbergman6-glitch/Mirror-Market/issues/320
"""

from __future__ import annotations

from analysis.futures.domain import Exchange, NamedContract

__all__ = [
    "TRADINGVIEW_ATTRIBUTION_URL",
    "TRADINGVIEW_DELAY_NOTE",
    "TRADINGVIEW_EXCHANGES",
    "tradingview_symbol",
    "tradingview_url",
]

#: Venue prefix by exchange. CBOT only, and checked: ``CBOT:ZMU2026`` resolves
#: to "Soybean Meal Futures (Sep 2026)" on TradingView's own symbol page
#: (verified 2026-08-23). CME and ICE US are absent because nobody has checked
#: them yet, and absence is what keeps a wrong chart off the page.
TRADINGVIEW_EXCHANGES: dict[Exchange, str] = {
    Exchange.CBOT: "CBOT",
}

#: What the free widget actually shows. Stated on the frame, in these words,
#: because the surrounding numbers carry an honest-timestamp claim and the
#: reader must be able to tell which is which.
TRADINGVIEW_DELAY_NOTE = "approximately 10-minute delayed exchange data"

#: Attribution is a licence condition, not decoration: TradingView's terms
#: permit free embedding only with the attribution the generated widget ships
#: with, left as designed. The template renders this link visibly and no CSS
#: in this project hides it (invariant 9 — publishing is the gate).
TRADINGVIEW_ATTRIBUTION_URL = "https://www.tradingview.com/symbols/{slug}/"


def tradingview_symbol(contract: NamedContract) -> str | None:
    """``CBOT:ZMU2026`` for a mapped venue, ``None`` for anything else.

    Pure code — no network, no clock, no database. The month code and root are
    the contract's own; the only thing added is the venue prefix and a
    four-digit year, because TradingView keys by the full year while the
    exchange symbol truncates to two digits.
    """
    prefix = TRADINGVIEW_EXCHANGES.get(contract.spec.exchange)
    if prefix is None:
        return None
    return f"{prefix}:{contract.spec.root}{contract.month_code}{contract.year:04d}"


def tradingview_url(symbol: str) -> str:
    """The public symbol page for ``CBOT:ZMU2026`` — the attribution target."""
    return TRADINGVIEW_ATTRIBUTION_URL.format(slug=symbol.replace(":", "-"))
