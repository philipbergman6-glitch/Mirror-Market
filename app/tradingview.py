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
    "TRADINGVIEW_EXCHANGES",
    "TRADINGVIEW_STAMP",
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

#: The frame's stamp line, whole, in Python — the template renders it verbatim
#: and composes none of it, because the surrounding numbers carry an
#: honest-timestamp claim and no wording that separates ours from theirs may
#: be decided in markup. The delay figure is **TradingView's own claim** about
#: their free CBOT feed, and the stamp says so: this project has not measured
#: it, and `LATENCY.md` owns the vocabulary for ages we assert ourselves.
TRADINGVIEW_STAMP = (
    "Third party · TradingView · delayed exchange data (~10 min, their figure)"
)

#: Attribution is a licence condition, not decoration. TradingView's Terms of
#: Use (https://www.tradingview.com/policies/, read 2026-08-23) bar using
#: their widgets off-site without attribution and require it kept "as
#: originally designed and intended" — so the template ships the embed's own
#: ``tradingview-widget-container`` / ``tradingview-widget-copyright`` markup,
#: whose classes their script (s3.tradingview.com/external-embedding/
#: embed-widget-advanced-chart.js, inspected the same day) looks up and whose
#: link it rewrites. No CSS in this project hides or shrinks it (invariant 9 —
#: publishing is the gate). This URL is that copyright link's target, the
#: symbol's public page.
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
