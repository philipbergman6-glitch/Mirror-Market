"""Headline-section builders — data only, never markup (M18 #214).

These five sections used to assemble ~600 lines of HTML inside f-strings in
``scripts/generate_html.py``: `<div class="mc">` fragments concatenated with
`_esc()` calls, one hand-written variant per country and per leg. That form
cannot enforce "same section, same treatment" — a per-market tweak was one
f-string away and invisible in review — and it is the reason M8 #150 made
"markup lives in Jinja" a contract rather than a preference.

Each builder returns the same envelope the nine market blocks use::

    {"state": "ok" | "empty" | "absent", "reason": str, "data": {...}}

so an empty headline section states *why* it is empty on the page, exactly as
a market block does, instead of vanishing or printing a generic warning.

Charts stay as pre-rendered Plotly HTML: a figure is not markup we assemble,
it is an artifact from a library. Everything around it is data.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Callable
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)

# Sessions of history each headline chart is allowed to carry (M8's page
# budget). docs/index.html is ~7 MB today, almost entirely inline Plotly
# series at full 15-year depth behind charts that are read one year at a
# time. Clipping happens where the series meets the figure, not inside the
# analysts, so the numbers above the chart keep their full-history stats.
CHART_WINDOW_SESSIONS = 504          # ~2 years of daily observations
CHART_WINDOW_MONTHLY_ROWS = 180      # ~15 years of monthly seasonal points

SOY_LEGS = ("Soybeans", "Soybean Oil", "Soybean Meal")


def section(state: str, reason: str = "", **data) -> dict:
    """The envelope. A non-``ok`` section must say why, like every block."""
    if state != "ok" and not reason.strip():
        raise ValueError("a non-ok section must name its reason (M1 #143 constraint 2)")
    return {"state": state, "reason": reason, "data": data}


def _empty(reason: str) -> dict:
    return section("empty", reason)


def clip(frame, sessions: int = CHART_WINDOW_SESSIONS):
    """The last ``sessions`` rows of a series — the chart budget, applied once."""
    if frame is None:
        return frame
    try:
        return frame.tail(sessions)
    except AttributeError:
        return frame


def _chart(build: Callable[[], Any], label: str) -> str:
    """Render one Plotly figure to an embeddable div, or nothing.

    A failed chart is not a failed section: the numbers beside it are the
    point, and the page keeps them.
    """
    try:
        figure = build()
    except Exception:  # noqa: BLE001
        log.warning("chart %s failed", label, exc_info=True)
        return ""
    return figure.to_html(full_html=False, include_plotlyjs=False)


def _direction(value: float | None) -> str:
    if value is None:
        return "muted"
    return "up" if value > 0 else "down" if value < 0 else "muted"


def _as_date(value) -> str | None:
    if value is None:
        return None
    return value.strftime("%Y-%m-%d") if hasattr(value, "strftime") else str(value)


# ---------------------------------------------------------------------------
# 05 Relative value (the crush board is its own section — M16 #208)
# ---------------------------------------------------------------------------
def relative_value_section(data: dict | None) -> dict:
    from app.charts import (
        build_basis_chart,
        build_bean_corn_ratio_chart,
        build_oil_meal_ratio_chart,
    )

    if not data:
        return _empty("the relative-value analyst returned nothing — check the prices layer")

    out: dict[str, Any] = {}

    # The CBOT crush spread that used to open this section is gone (M16 #208).
    # Its successor is the crush board one section up, which strikes CBOT's
    # margin on named delivery months and renders it beside Dalian's,
    # Brazil's and Argentina's — each labelled by kind. A continuous
    # front-month crush chart underneath a named-contract margin would be a
    # second CBOT crush on one page, which is the thing M7 #149 named.
    out["basis"] = _basis_panel(data, build_basis_chart)

    omr = data.get("oil_meal_ratio") or {}
    if omr.get("series") is not None:
        out["oil_meal"] = {
            "current": omr.get("current"),
            "avg_60d": omr.get("avg_60d"),
            "as_of": omr.get("as_of"),
            "chart_html": _chart(
                lambda: build_oil_meal_ratio_chart({**omr, "series": clip(omr["series"])}),
                "oil/meal ratio",
            ),
        }

    if data.get("soy_oil_share"):
        out["soy_oil_share"] = {
            "value": data["soy_oil_share"],
            "as_of": data.get("soy_oil_share_as_of"),
        }

    out["oil_vs_palm"] = _two_oil_panel(
        data.get("oil_vs_palm"),
        left=("soy_oil", "Soy Oil"),
        right=("palm_oil", "Palm Oil"),
    )
    rapeseed = _two_oil_panel(
        data.get("oil_vs_rapeseed"),
        left=("soy_oil", "Soy Oil"),
        right=("rapeseed_oil", "CZCE Rapeseed Oil"),
    )
    if rapeseed:
        rapeseed["spread_usd_mt"] = (data.get("oil_vs_rapeseed") or {}).get("spread_usd_mt")
        rapeseed["spread_note"] = (
            "CZCE rapeseed oil premium over CBOT soy oil, USD/MT — ICE canola (RS=F) "
            "has no free daily feed, so CZCE is the rapeseed leg"
        )
    out["oil_vs_rapeseed"] = rapeseed

    bcr = data.get("bean_corn_ratio") or {}
    if bcr.get("series") is not None:
        current, avg = bcr.get("current"), bcr.get("avg_1y")
        out["bean_corn"] = {
            "current": current,
            "avg_1y": avg,
            "as_of": bcr.get("as_of"),
            "reading": (
                "Above avg — soybeans expensive vs corn"
                if current is not None and avg is not None and current > avg
                else "Below avg — corn expensive vs soy"
            ),
            "chart_html": _chart(
                lambda: build_bean_corn_ratio_chart({**bcr, "series": clip(bcr["series"])}),
                "bean/corn ratio",
            ),
        }

    out = {key: value for key, value in out.items() if value}
    if not out:
        return _empty("no basis or ratio series had enough rows to render")
    return section("ok", **out)


def _basis_panel(data: dict, build_basis_chart) -> dict | None:
    basis = data.get("basis") or {}
    sources = basis.get("sources") or {}
    primary_label = basis.get("primary")
    if not sources or not primary_label or primary_label not in sources:
        return None

    primary = sources[primary_label]
    secondary_label = next((label for label in sources if label != primary_label), None)
    n_obs = primary.get("n_obs", 0)
    avg, pct = primary.get("avg_1y"), primary.get("percentile_1y")
    if avg is not None and pct is not None:
        window = "1Y" if n_obs >= 252 else f"{n_obs}-session"
        stats = f"{window} avg ${avg:+,.1f} · {pct:.0f}th pctile"
    else:
        stats = f"history building ({n_obs} obs — stats at 20)"

    def leg(label: str, stats_dict: dict) -> dict:
        current = stats_dict.get("current_usd_mt", 0.0)
        return {
            "label": label,
            "current_usd_mt": current,
            # A Brazilian discount is export-competitive, so a negative basis
            # reads bullish for trade flow — the colour is deliberately the
            # opposite way round from a price change.
            "class": "up" if current < 0 else "down",
            "direction": stats_dict.get("direction", ""),
            "as_of": stats_dict.get("as_of"),
        }

    return {
        "primary": {**leg(primary_label, primary), "stats": stats},
        "secondary": leg(secondary_label, sources[secondary_label]) if secondary_label else None,
        "wedge_usd_mt": basis.get("wedge_usd_mt"),
        "title": (
            f"Brazil Basis ({primary_label} · {secondary_label} vs CBOT)"
            if secondary_label else f"Brazil Basis ({primary_label} vs CBOT)"
        ),
        "chart_html": _chart(lambda: build_basis_chart(basis, primary), "Brazil basis"),
    }


def _two_oil_panel(data: dict | None, *, left: tuple[str, str], right: tuple[str, str]) -> dict | None:
    if not data:
        return None
    legs = []
    for key, label in (left, right):
        value = data.get(key)
        if not value:
            continue
        change = data.get(f"{key}_weekly_chg")
        legs.append({
            "label": f"{label} ({data.get(f'{key}_unit', 'USD/MT')})",
            "value": value,
            "weekly_chg": change,
            "class": _direction(change),
            "as_of": data.get(f"{key}_as_of"),
            "native": data.get(f"{key}_cny") and f"CNY {data[f'{key}_cny']:,.0f}/MT",
        })
    if not legs:
        return None
    # Every oil panel renders through the same template loop, which asks for
    # a spread; only the rapeseed panel has one. Absent keys are a hard
    # UndefinedError under StrictUndefined — that is what tombstoned the
    # headline page and with it the whole deploy (#226). None means "no
    # spread on this pair"; the template skips the block.
    return {"legs": legs, "spread_usd_mt": None, "spread_note": None}


# ---------------------------------------------------------------------------
# 05 Risk monitor
# ---------------------------------------------------------------------------
def risk_monitor_section(data: dict | None) -> dict:
    from app.charts import build_cot_chart

    if not data:
        return _empty("the risk analyst returned nothing — check the currencies and COT layers")

    currencies = [
        {
            "pair": pair,
            "close": info.get("close"),
            "weekly_chg": info.get("weekly_chg"),
            "class": _direction(info.get("weekly_chg")),
            "monthly_chg": info.get("monthly_chg"),
            "as_of": info.get("as_of"),
        }
        for pair, info in (data.get("currencies") or {}).items()
    ]

    cot = data.get("cot") or {}
    cot_panel = None
    if cot:
        cot_panel = {
            "chart_html": _chart(lambda: build_cot_chart(cot), "COT"),
            "rows": [
                {
                    "leg": leg,
                    "spec_net_chg": info.get("spec_net_chg"),
                    "class": _direction(info.get("spec_net_chg")),
                    "as_of": _as_date(info.get("date")),
                }
                for leg, info in cot.items()
                if info.get("spec_net_chg") is not None
            ],
        }

    alerts = [
        {
            "region": alert.get("region", ""),
            "alert": alert.get("alert", ""),
            "temp_max": alert.get("temp_max"),
            "precip": alert.get("precip"),
            "as_of": _as_date(alert.get("date")),
        }
        for alert in (data.get("weather_alerts") or [])
    ]

    panel = section(
        "ok",
        currencies=currencies,
        cot=cot_panel,
        weather_alerts=alerts,
        correlations=_correlations_panel(),
    )
    if not (currencies or cot_panel or alerts or panel["data"]["correlations"]):
        return _empty("no currency, positioning, weather or correlation series had rows")
    return panel


def _correlations_panel() -> dict | None:
    """Rolling correlations. Its own read, and its own failure — the risk
    section keeps its currency and COT panels when this cannot be built."""
    try:
        from analysis.correlations import rolling_correlation
        from analysis.loaders import load_currencies, load_prices
        from app.charts import build_correlations_chart

        prices = load_prices()
        currencies = load_currencies()
        series = {
            name: clip(prices[name])["Close"]
            for name in ("Soybeans", "Soybean Oil", "Corn")
            if name in prices and not prices[name].empty
        }
        brl = currencies.get("BRL/USD", pd.DataFrame())

        pairs = []
        if "Soybeans" in series and not brl.empty:
            pairs.append(("Soybeans vs BRL/USD", series["Soybeans"], clip(brl)["Close"]))
        if "Soybeans" in series and "Soybean Oil" in series:
            pairs.append(("Soybeans vs Soy Oil", series["Soybeans"], series["Soybean Oil"]))
        if "Soybeans" in series and "Corn" in series:
            pairs.append(("Soybeans vs Corn", series["Soybeans"], series["Corn"]))
        if not pairs:
            return None
        return {"chart_html": _chart(
            lambda: build_correlations_chart(pairs, rolling_correlation), "correlations")}
    except Exception:  # noqa: BLE001
        log.warning("correlations panel failed", exc_info=True)
        return None


# ---------------------------------------------------------------------------
# 06 Forward curves
# ---------------------------------------------------------------------------
def forward_curves_section(data: dict | None) -> dict:
    from app.charts import build_forward_curve_chart
    from pipeline.units import to_metric_tons

    if not data:
        return _empty("the forward-curve analyst returned nothing — check the forward_curve layer")

    legs = []
    for name in SOY_LEGS:
        leg = data.get(name)
        if not leg:
            continue
        curve = leg.get("curve_data_mt", leg.get("curve_data"))
        if curve is None or curve.empty:
            continue
        analysis = leg.get("analysis") or {}
        unit = leg.get("unit", "USD/MT")

        front, back = analysis.get("front_price", 0), analysis.get("back_price", 0)
        # An unknown commodity keeps its native number rather than losing it.
        with contextlib.suppress(Exception):
            front, back = to_metric_tons(front, name), to_metric_tons(back, name)

        calendar = leg.get("calendar_spread") or {}
        legs.append({
            "name": name,
            "as_of": leg.get("as_of"),
            "structure": (analysis.get("structure") or "n/a").title(),
            "front_mt": front,
            "back_mt": back,
            "spread_pct": analysis.get("spread_pct"),
            "has_analysis": bool(analysis),
            "calendar": {
                "near": calendar.get("near_label", ""),
                "far": calendar.get("far_label", ""),
                "spread": calendar.get("spread"),
                "spread_pct": calendar.get("spread_pct"),
            } if calendar else None,
            # A curve is one snapshot of a dozen contract months — already
            # small, and clipping it would drop the back of the curve.
            "chart_html": _chart(
                lambda curve=curve, name=name, unit=unit: build_forward_curve_chart(curve, name, unit),
                f"forward curve {name}",
            ),
        })

    if not legs:
        return _empty("no soy leg has a stored curve snapshot")
    return section("ok", legs=legs)


# ---------------------------------------------------------------------------
# 07 Seasonal
# ---------------------------------------------------------------------------
def seasonal_section(data: dict | None) -> dict:
    from app.charts import build_seasonal_chart

    if not data:
        return _empty("the seasonal analyst returned nothing — check the prices layer")

    legs = []
    for name in SOY_LEGS:
        leg = data.get(name)
        if not leg:
            continue
        monthly = leg.get("monthly")
        vs_seasonal = leg.get("vs_seasonal") or {}
        unit = leg.get("unit", "USD/MT")
        detrended = vs_seasonal.get("detrended_delta_pct")
        deviation = vs_seasonal.get("deviation_pct")
        legs.append({
            "name": name,
            "unit": unit,
            "as_of": leg.get("as_of"),
            "current_price": vs_seasonal.get("current_price"),
            "seasonal_avg": vs_seasonal.get("seasonal_avg"),
            # Two different claims, and the label has to say which: a
            # detrended deviation is "unusual for this month", a raw one is
            # "different from a 15-year level" — mostly a trend, not a season.
            "delta_pct": detrended if detrended is not None else deviation,
            "delta_label": "vs seasonal (detrended)" if detrended is not None else "vs 15y avg level",
            "delta_note": (
                ("Above" if (detrended or 0) > 0 else "Below") + " typical for the month"
                if detrended is not None else "trend not removed"
            ),
            "has_stats": bool(vs_seasonal),
            "chart_html": _chart(
                lambda monthly=monthly, vs_seasonal=vs_seasonal, name=name, unit=unit:
                    build_seasonal_chart(
                        clip(monthly, CHART_WINDOW_MONTHLY_ROWS), vs_seasonal, name, unit),
                f"seasonal {name}",
            ) if monthly is not None and not monthly.empty else "",
        })

    if not legs:
        return _empty("no soy leg has enough history for a seasonal profile")
    return section("ok", legs=legs)


# ---------------------------------------------------------------------------
# 04c Emerging markets
# ---------------------------------------------------------------------------
# Every emerging-market panel is the same shape — a titled group of metric
# cards with an optional note — so one renderer serves all of them and a new
# country or a new series is data, not another f-string branch. This is the
# same "market is a parameter" rule the market pages are built on, applied to
# the headline section they will eventually be replaced by (M2 #144).
def _card(label: str, value, *, places: int = 0, prefix: str = "", suffix: str = "",
          delta: str = "", delta_class: str = "muted", caption: str = "",
          value_class: str = "") -> dict:
    return {
        "label": label,
        "value": value,
        "places": places,
        "prefix": prefix,
        "suffix": suffix,
        "delta": delta,
        "delta_class": delta_class,
        "caption": caption,
        "value_class": value_class,
    }


def _group(title: str, cards: list[dict], *, note: str = "") -> dict | None:
    cards = [card for card in cards if card and card["value"] is not None]
    return {"title": title, "cards": cards, "note": note} if cards else None


def emerging_markets_section(data: dict | None) -> dict:
    if not data or not data.get("countries"):
        return _empty("the emerging-markets analyst returned no countries")

    countries = []
    for name, info in data["countries"].items():
        groups = [
            _psd_group(info),
            _currency_group(info),
            _india_group(info),
            *_brazil_groups(info),
            *_south_africa_groups(info),
        ]
        weather = info.get("weather") or []
        countries.append({
            "name": name,
            "groups": [group for group in groups if group],
            "weather_alerts": [w for w in weather if w.get("alert")],
            "weather_checked": bool(weather),
            "notes": _country_notes(name, info),
        })
    return section("ok", countries=countries)


def _country_notes(name: str, info: dict) -> list[str]:
    """Notes that must appear even when the numbers do not.

    An empty card reads as a bug. India's mandi feed goes quiet on a closed
    market day, and saying so beats omitting the series silently (#155).
    """
    if name == "India" and not info.get("india_domestic"):
        return ["Mandi domestic price (Agmarknet via data.gov.in): no session data — "
                "feed throttled or mandis closed"]
    return []


def _psd_group(info: dict) -> dict | None:
    psd = info.get("psd") or {}
    year = info.get("psd_year", "")
    cards = []
    for attribute in ("Production", "Imports", "Exports", "Ending Stocks"):
        if attribute not in psd:
            continue
        values = psd[attribute]
        yoy = values.get("yoy_pct")
        cards.append(_card(
            f"{attribute} ({year})", values.get("value"),
            delta=f"{yoy:+.1f}% YoY" if yoy is not None else "",
            delta_class=_direction(yoy),
            caption=values.get("unit", ""),
        ))
    return _group("Supply & demand (PSD)", cards)


def _currency_group(info: dict) -> dict | None:
    currency = info.get("currency") or {}
    if not currency:
        return None
    weekly = currency.get("weekly_chg")
    return _group("Currency", [_card(
        currency.get("pair", ""), currency.get("close"), places=4,
        delta=f"{weekly:+.2f}%" if weekly is not None else "",
        delta_class=_direction(weekly),
    )])


def _india_group(info: dict) -> dict | None:
    india = info.get("india_domestic") or {}
    if not india:
        return None
    mandi_date = india.get("soybean_mandi_date")
    basis_date = india.get("basis_date")
    premium = india.get("bean_premium_usd")
    return _group("Mandi domestic price (Agmarknet, MP median)", [
        _card("Soybean", india.get("soybean_mandi_inr"), prefix="₹",
              caption=f"INR/MT{f' · {mandi_date}' if mandi_date else ''}"),
        _card("Soybean (USD)", india.get("soybean_mandi_usd"), places=1, prefix="$",
              caption=f"USD/MT{f' · {basis_date or mandi_date}' if (basis_date or mandi_date) else ''}"),
        _card("vs CBOT beans", premium, places=1, prefix="$",
              value_class=_direction(premium),
              caption=("premium" if (premium or 0) > 0 else "discount")
              + (f" · as of {basis_date}" if basis_date else "")),
    ])


def _brazil_groups(info: dict) -> list[dict | None]:
    brazil = info.get("brazil_domestic") or {}
    if not brazil:
        return []
    cepea_date = brazil.get("cepea_soy_date")
    basis_date = brazil.get("basis_date")
    basis = brazil.get("brazil_cbot_basis_usd")
    agrural_date = brazil.get("agrural_basis_date")
    agrural_basis = brazil.get("agrural_cbot_basis_usd")
    return [
        _group("CEPEA farm-gate price", [
            _card("CEPEA soybean", brazil.get("cepea_soy_brl"), places=2, prefix="R$",
                  caption=f"BRL/MT{f' · {cepea_date}' if cepea_date else ''}"),
            _card("CEPEA (USD)", brazil.get("cepea_soy_usd"), places=1, prefix="$",
                  caption=f"USD/MT{f' · {basis_date or cepea_date}' if (basis_date or cepea_date) else ''}"),
            _card("Brazil−CBOT basis", basis, places=1, prefix="$",
                  value_class=_direction(basis),
                  caption=("premium" if (basis or 0) > 0 else "discount")
                  + (f" · as of {basis_date}" if basis_date else "")),
        ]),
        # AgRural: USD only. The raw BRL/saca quote is not redistributable
        # under AgRural's terms; the derived USD basis is the publishable one.
        _group("AgRural Paranaguá FOB", [
            _card("AgRural (USD)", brazil.get("agrural_soy_usd"), places=1, prefix="$",
                  caption=f"USD/MT{f' · {agrural_date}' if agrural_date else ''}"),
            _card("AgRural−CBOT basis", agrural_basis, places=1, prefix="$",
                  value_class=_direction(agrural_basis),
                  caption=("premium" if (agrural_basis or 0) > 0 else "discount")
                  + (f" · as of {agrural_date}" if agrural_date else "")),
        ]),
    ]


def _south_africa_groups(info: dict) -> list[dict | None]:
    groups: list[dict | None] = []
    safex = info.get("south_africa_domestic") or {}
    if safex:
        safex_date = safex.get("soybean_safex_date")
        basis_date = safex.get("basis_date")
        basis = safex.get("safex_cbot_basis_usd")
        groups.append(_group(
            # "Last traded", never "settlement": the free Grain SA table has
            # no settlement column and the JSE's own MTM file is licensed
            # (#157). The dashboard published the wrong word for months.
            "SAFEX last-traded prices",
            [
                _card("SAFEX soybean", safex.get("soybean_safex_zar"), prefix="R",
                      caption=f"ZAR/MT{f' · {safex_date}' if safex_date else ''}"),
                _card("SAFEX (USD)", safex.get("soybean_safex_usd"), places=1, prefix="$",
                      caption=f"USD/MT{f' · {basis_date or safex_date}' if (basis_date or safex_date) else ''}"),
                _card("SAFEX−CBOT basis", basis, places=1, prefix="$",
                      value_class=_direction(basis),
                      caption=("premium" if (basis or 0) > 0 else "parity")
                      + (f" · as of {basis_date}" if basis_date else "")),
            ],
        ))

    flows = info.get("south_africa_deliveries") or {}
    attribution = flows.get("attribution")
    for key, label in (("soybeans", "Soybeans"), ("sunflower", "Sunflower seed")):
        pace = flows.get(key)
        if not pace:
            continue
        week = pace.get("week_number")
        week_end = pace.get("week_end")
        yoy = pace.get("yoy_pct")
        vs_avg = pace.get("vs_avg3_pct")
        groups.append(_group(
            f"SAGIS weekly producer deliveries — {label}",
            [
                _card("Latest week", pace.get("week_total_mt"),
                      caption=f"MT · wk {week}" + (f" ending {week_end}" if week_end else "")),
                _card(f"Season {pace.get('season_label', '')}", pace.get("progressive_mt"),
                      delta=f"{yoy:+.1f}% YoY" if yoy is not None else "season to date",
                      delta_class=_direction(yoy), caption="MT"),
                _card("vs 3y average", vs_avg, places=1, suffix="%",
                      value_class=_direction(vs_avg), caption="same week number"),
            ],
            # SAGIS grants reproduction with acknowledgement — the string is
            # not optional and travels with the numbers (#202).
            note=attribution or "",
        ))

    smd = info.get("south_africa_supply_demand") or {}
    if smd:
        month = smd.get("month_label", "")
        season = smd.get("season_label", "")
        months = smd.get("month_number")
        cards = []
        for key, label, note in (
            ("crush", "Crush (beans processed)", "oil & oilcake — volume, not margin"),
            ("imports", "Imports", "beans destined for RSA"),
            ("exports_whole", "Bean exports", "whole beans, all routes"),
        ):
            yoy = smd.get(f"{key}_yoy_pct")
            cards.append(_card(
                f"{label} — {season}", smd.get(f"{key}_season_to_date_mt"),
                delta=(f"{yoy:+.1f}% YoY · same {months} months" if yoy is not None
                       else "season to date"),
                delta_class=_direction(yoy), caption=f"MT · {note}",
            ))
        share = smd.get("stock_processors_share_pct")
        cards.append(_card(
            f"Closing stock — {month}", smd.get("closing_stock_mt"),
            caption="MT · " + (f"{share:.0f}% held by processors" if share is not None
                               else "closing stock"),
        ))
        groups.append(_group(
            f"SAGIS monthly supply & demand — {month}", cards,
            note=(smd.get("attribution") or "") if not flows else "",
        ))

    # CEC official crop estimates (Layer 25, #204). A *revision* series: the
    # headline is the standing forecast, and the two deltas are the in-season
    # revision and the year-on-year crop. The USDA line is labelled a lag, not
    # a divergence — PSD has carried the CEC's own final number exactly for
    # three seasons, so calling the gap disagreement would be wrong.
    cec = info.get("south_africa_estimates") or {}
    for key, label in (("soybeans", "Soybeans"), ("sunflower", "Sunflower seed")):
        view = cec.get(key)
        if not view:
            continue
        revision = view.get("revision_pct")
        yoy = view.get("yoy_pct")
        stage = view.get("forecast_label", "")
        released = view.get("release_date", "")
        groups.append(_group(
            f"CEC official crop estimates — {label}",
            [
                _card(f"{view.get('season_year', '')} crop", view.get("production_t"),
                      caption=f"MT · {stage}" + (f" · {released}" if released else "")),
                _card("Revision", revision, places=2, suffix="%",
                      value_class="" if revision == 0 else _direction(revision),
                      caption=f"vs {view.get('prev_release_date', '')}"),
                _card("YoY", yoy, places=1, suffix="%", value_class=_direction(yoy),
                      caption=f"vs {view.get('prior_season_year', '')} final"),
            ],
            note=" · ".join(_cec_notes(view) + ([cec["attribution"]] if cec.get("attribution") else [])),
        ))
    return groups


def _cec_notes(view: dict) -> list[str]:
    notes = []
    if view.get("area_ha"):
        notes.append(f"Area {view['area_ha']:,.0f} ha")
    if view.get("yield_t_ha"):
        notes.append(f"implied yield {view['yield_t_ha']:.2f} t/ha")
    usda = view.get("usda_psd_t")
    if usda is not None:
        gap = view.get("vs_usda_pct")
        year = view.get("usda_psd_year")
        notes.append(
            f"USDA PSD carries {usda:,.0f} MT for {year}/{str(year + 1)[-2:]}"
            + (f" (CEC {gap:+.1f}%)" if gap is not None else "")
            + " — PSD has matched the CEC's final crop exactly for three seasons, "
              "so this is lag, not disagreement"
        )
    return notes


__all__ = [
    "CHART_WINDOW_SESSIONS",
    "clip",
    "emerging_markets_section",
    "forward_curves_section",
    "relative_value_section",
    "risk_monitor_section",
    "seasonal_section",
    "section",
]
