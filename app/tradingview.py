"""Third-party chart links for named contracts — the one labelled exception.

`CLAUDE.md` says this product is not a real-time market feed. The workstation's
contract rows carry a **deliberate, labelled exception** to that line (owner
decision 2026-08-23, [#320], amended by [#328]): each row links out to the same
contract month on a third party's own site. Originally the exception was an
embedded TradingView widget; [#328] established that TradingView's free embed
widget refuses CME symbols on any plan (the widget's symbol universe is not
the site's free tier), so nothing third-party renders on this site any more —
the exception has narrowed to **links**. The reader leaves; the pixels they
then see are the third party's, on the third party's timing, and nothing about
them passes through this project's price vocabulary. `pricing.semantics`
never sees any of it.

What lives here is the *only* thing that must be right on our side: the
mapping from a :class:`~analysis.futures.domain.NamedContract` to the strings
each third party keys that contract by. Two rules hold it honest.

**A venue is a registry entry, never a branch** (invariant 5). ``ZMU26`` is
TradingView's ``CBOT:ZMU2026`` and Barchart's bare ``ZMU26``; whether other
venues follow either convention is not something this project may assume from
one checked venue. So each registry maps only what has been verified against
the live site, and a follow-up ticket adds rows to it rather than cases to a
function.

**An unmapped venue yields nothing** (invariant 2). A guessed symbol does not
fail loudly — both sites render *something*, and a link to the wrong contract
beside a correct price is exactly the wrong-number-worse-than-a-gap trade
invariant 11 refuses. The caller renders no link at all.

[#320]: https://github.com/philipbergman6-glitch/Mirror-Market/issues/320
[#328]: https://github.com/philipbergman6-glitch/Mirror-Market/issues/328
"""

from __future__ import annotations

from analysis.futures.domain import Exchange, NamedContract

__all__ = [
    "BARCHART_EXCHANGES",
    "THIRD_PARTY_STAMP",
    "TRADINGVIEW_EXCHANGES",
    "TRADINGVIEW_SYMBOL_PAGE_URL",
    "barchart_url",
    "tradingview_symbol",
    "tradingview_url",
]

#: Venue prefix by exchange, for TradingView. CBOT only, and checked:
#: ``CBOT:ZMU2026`` resolves to "Soybean Meal Futures (Sep 2026)" on
#: TradingView's own symbol page (verified 2026-08-23). CME and ICE US are
#: absent because nobody has checked them yet, and absence is what keeps a
#: wrong link off the page.
TRADINGVIEW_EXCHANGES: dict[Exchange, str] = {
    Exchange.CBOT: "CBOT",
}

#: Venues whose contracts Barchart keys by the bare exchange symbol
#: (``ZSX26`` → barchart.com/futures/quotes/ZSX26/interactive-chart). CBOT
#: only, verified against the live page 2026-08-23 (#328 research). Same rule
#: as the TradingView registry: membership is a checked fact, never an
#: assumption, and an absent venue gets no link.
BARCHART_EXCHANGES: frozenset[Exchange] = frozenset({Exchange.CBOT})

#: The link strip's stamp line, whole, in Python — the template renders it
#: verbatim and composes none of it, because the surrounding numbers carry an
#: honest-timestamp claim and no wording that separates ours from theirs may
#: be decided in markup. No delay figure any more: nothing third-party renders
#: on this page (#328), so there is no feed here to characterise — what a
#: reader sees after clicking is each site's own claim, made on their page.
THIRD_PARTY_STAMP = "Third party · TradingView / Barchart · opens on their site"

#: The public symbol page for a contract on TradingView — the link target.
#: When the widget was embedded this URL was a licence-required attribution
#: target; a plain outbound hyperlink carries no such condition (their Terms
#: of Use, re-read 2026-08-23, attach the attribution requirement to use of
#: their *widgets and content*, and a link uses neither).
TRADINGVIEW_SYMBOL_PAGE_URL = "https://www.tradingview.com/symbols/{slug}/"

#: Barchart's per-contract interactive chart, keyed by the bare exchange
#: symbol. Fixed URL shape observed stable in the #328 research (2026-08-23).
BARCHART_CHART_URL = "https://www.barchart.com/futures/quotes/{symbol}/interactive-chart"


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
    """The public symbol page for ``CBOT:ZMU2026`` — the exact contract, theirs."""
    return TRADINGVIEW_SYMBOL_PAGE_URL.format(slug=symbol.replace(":", "-"))


def barchart_url(contract: NamedContract) -> str | None:
    """Barchart's interactive chart for this contract, or ``None`` unchecked.

    Barchart keys by the exchange's own two-digit-year symbol (``ZSX26``), so
    nothing is rebuilt here — the gate is only whether that convention has
    been verified for this contract's venue.
    """
    if contract.spec.exchange not in BARCHART_EXCHANGES:
        return None
    return BARCHART_CHART_URL.format(symbol=contract.symbol)
