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
has been verified against the live symbol page, and a new venue is a row added
here, never a case added to a function. The other market venues were checked
under [#321] and every one stays out — the registry comment below records each
verdict, because an absence with no reason is indistinguishable from a venue
nobody looked at.

**An unmapped venue yields nothing** (invariant 2). A guessed prefix does not
fail loudly — TradingView renders *something*, and a chart of the wrong
contract beside a correct price is exactly the wrong-number-worse-than-a-gap
trade invariant 11 refuses. The caller renders no expander at all.

[#320]: https://github.com/philipbergman6-glitch/Mirror-Market/issues/320
[#321]: https://github.com/philipbergman6-glitch/Mirror-Market/issues/321
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
#:
#: The four non-CBOT venues in ``config.MARKETS`` **have** been checked
#: (2026-08-23, #321 — findings with source URLs are a comment on the issue),
#: and each stays out for its own reason, not for want of looking:
#:
#: - **DCE (Dalian)**: not on TradingView at all. Their symbol-search API
#:   returns an empty universe for ``exchange=DCE``, and their data-coverage
#:   catalog lists CFFEX as the only mainland-China futures venue. The only
#:   embeddable cousin is ``MYX:FSOY`` — Bursa Malaysia's contract
#:   cash-settled on DCE soy oil — which is another venue's proxy, and a
#:   proxy chart under a Dalian row is the wrong-number trade invariant 11
#:   refuses.
#: - **SAFEX / JSE**: TradingView carries JSE equities and indices only; no
#:   derivatives row exists in their catalog and the SAFEX agri symbols 404.
#:   The embed-licence question invariant 9 would ask is moot — there is no
#:   symbol to embed.
#: - **NCDEX**: the venue is carried, but its soy pages are spot indices, and
#:   there is no futures month to chart: SEBI's Dec-2021 suspension of the
#:   soy complex was extended on 2026-03-27 through 2027-03-31.
#: - **MATIF (Euronext)**: the near-miss, refused at the last gate. Per-month
#:   symbols exist in exactly this module's grammar (``EURONEXT:ECOG2027``
#:   names Rapeseed Feb 2027 on the live page, 15-min delayed free — note:
#:   *their* delay figure differs from the ~10 min the CBOT stamp quotes, so
#:   a MATIF entry would need its own stamp). But the embed widget — the only
#:   surface this product uses — **refuses the symbol**: a live test of the
#:   shipped embed markup (headless Chrome, 2026-08-23; a NASDAQ control
#:   rendered data in the same harness) got "This symbol is only available on
#:   TradingView" for both ``ECOG2027`` and ``ECO1!``, matching Euronext's
#:   absence from the widget-docs markets list. The site's symbol universe
#:   and the widget's are different things. Re-check trigger: TradingView
#:   adding Euronext to that list — and the europe page growing rows that
#:   name a month, since its series are continuous today.
#:
#: The same embed test refused ``CBOT:ZSX2026`` and ``CBOT:ZS1!`` — CME Group
#: is likewise absent from the widget-docs markets list — which puts the
#: shipped CBOT expander itself in question. That is #328, a #320 defect, not
#: a mapping question: this registry's CBOT symbol grammar is verified right,
#: and what the widget will serve is decided on TradingView's side.
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
