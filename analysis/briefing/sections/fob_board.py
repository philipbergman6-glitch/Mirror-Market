"""CROSS-ORIGIN PORT LEVELS — soybean export-side levels from the three origins.

US Gulf CIF NOLA barge bid (Layer 20, AMS), Brazil Paranaguá on-truck port bid
(Layer 19, AgRural — DAP truck, "sobre rodas", B21 #483) and Argentina
up-river official FOB vessel (Layer 21, MAGyP), each in USD/MT and each on its
own delivery term, printed inline on its row.

**No ranking.** The three numbers are three delivery terms, and the step from
each to FOB vessel (barge elevation at NOLA, truck unload + terminal margin at
Paranaguá) is a real, unpublished cost. The origins page ranks only legs with
a costed path to FOB vessel (``analysis.origins.comparison.fob_board``); this
section applies no bridge, so it withholds the ranking with that reason rather
than print "cheapest" over a footer disclaimer (B27 #501).

The Brazil leg is computed as CBOT + date-aligned AgRural basis
(`compute_brazil_basis`), not by converting the raw quote directly:
that keeps date alignment honest and publishes no more than the basis
line already does.
"""

import logging

import pandas as pd

import config
from analysis.origins.domain import ShipmentWindow
from analysis.origins.sources import nearest_window_key, parse_ams_slot, parse_magyp_window
from analysis.spreads import compute_brazil_basis
from pipeline.query import read_argentina_fob, read_brazil_spot, read_gulf_bids
from pipeline.units import to_metric_tons

logger = logging.getLogger(__name__)

_AGRURAL_SERIES = "Soybean (AgRural Paranaguá FOB)"


def _nearest(day: pd.DataFrame, parse, session) -> tuple[ShipmentWindow, pd.DataFrame] | None:
    """The session's nearest shipment window and its rows (B30 #504) — the
    origins domain's one rule (``nearest_window_key``), shared with the site.
    An unreadable window raises: it may be the nearest one."""
    if day.empty:
        return None
    windows: list[ShipmentWindow] = []
    for _, row in day.iterrows():
        window = parse(row)
        if window is None:
            raise ValueError(f"unreadable shipment window on {session}: {row.to_dict()!r}")
        windows.append(window)
    chosen = min(windows, key=lambda w: nearest_window_key(w, session))
    return chosen, day[[w == chosen for w in windows]]


def _gulf_leg() -> tuple[float, str] | None:
    """Latest CIF NOLA soybean bid for the nearest delivery slot, USD/MT."""
    bids = read_gulf_bids("Soybeans")
    if bids.empty:
        return None
    latest_date = bids["report_date"].max()
    day = bids[bids["report_date"] == latest_date]
    # B30 #504 / B39 #540: the site's own nearest-window rule and loading-month
    # label, so the briefing and the CBOT page name the same slot
    # ("Oct loading (Current (prompt))").
    session = latest_date.date()
    nearest = _nearest(day, lambda r: parse_ams_slot(str(r["delivery"]), session), session)
    if nearest is None:
        return None
    window, rows = nearest
    avg_bu = float(rows["average"].mean())  # places quoting one slot, as the site
    usd_mt = to_metric_tons(avg_bu * 100.0, "Soybeans")  # $/bu → ¢/bu → USD/MT
    if usd_mt is None:
        return None
    return usd_mt, f"{session.isoformat()} · {window.loading_label()}"


def _brazil_leg(
    price_data: dict[str, pd.DataFrame],
    currency_data: dict[str, pd.DataFrame],
) -> tuple[float, str] | None:
    """CBOT + date-aligned AgRural Paranaguá basis in USD/MT."""
    soybeans = price_data.get("Soybeans", pd.DataFrame())
    brl_usd = currency_data.get("BRL/USD", pd.DataFrame())
    if soybeans.empty or brl_usd.empty:
        return None
    spot = read_brazil_spot(_AGRURAL_SERIES)
    if spot.empty:
        return None
    basis = compute_brazil_basis(soybeans, spot, brl_usd)
    if basis.empty:
        return None
    latest = basis.iloc[-1]
    level = float(latest["cepea_usd_mt"])  # spot leg × FX, date-aligned
    when = pd.Timestamp(latest["Date"]).date().isoformat()
    return level, when


def _argentina_leg() -> tuple[float, str] | None:
    """Latest MAGyP official bean FOB, nearest shipment slot, USD/MT."""
    fob = read_argentina_fob("Soybeans")
    if fob.empty:
        return None
    latest_date = fob["date"].max()
    session = latest_date.date()
    nearest = _nearest(
        fob[fob["date"] == latest_date],
        lambda r: parse_magyp_window(r["ship_from"], r["ship_to"] if pd.notna(r["ship_to"]) else None),
        session,
    )
    if nearest is None:
        return None
    # B30 #504 / B39 #540: one band of the circular's curve, labelled as the
    # site labels it — "Sep loading (band Sep–Oct 2026)".
    window, rows = nearest
    return float(rows["price_usd_mt"].iloc[0]), f"{session.isoformat()} · {window.loading_label()}"


# ``config.ORIGIN_LEGS`` keys, in print order. Label and delivery term come
# from that one registry — the strings the origins page prints — so the
# briefing never keeps a second vocabulary. The term is part of the price, so
# it is printed on the row, never left to a footer.
_LEGS: tuple[str, ...] = ("us_gulf", "br_paranagua", "ar_up_river")


def _term(leg_id: str) -> str:
    """``"DAP truck"`` — incoterm + carrier, as ``app/origins_page.py`` prints it."""
    leg = config.ORIGIN_LEGS[leg_id]
    return f"{leg['incoterm']} {leg['carrier']}"


def format(  # noqa: A001
    price_data: dict[str, pd.DataFrame],
    currency_data: dict[str, pd.DataFrame],
) -> str:
    loaders = {
        "us_gulf": _gulf_leg,
        "br_paranagua": lambda: _brazil_leg(price_data, currency_data),
        "ar_up_river": _argentina_leg,
    }
    legs: list[tuple[str, str, float, str]] = []
    for leg_id in _LEGS:
        try:
            leg = loaders[leg_id]()
        except Exception as exc:
            logger.debug("Cross-origin leg %s failed: %s", leg_id, exc)
            leg = None
        if leg is not None:
            legs.append((config.ORIGIN_LEGS[leg_id]["label"], _term(leg_id), *leg))

    if len(legs) < 2:
        return "CROSS-ORIGIN PORT LEVELS: Insufficient data"

    width = max(len(label) for label, _, _, _ in legs) + 1
    lines = ["CROSS-ORIGIN PORT LEVELS (Soybeans, USD/MT, each on its own delivery term):"]
    for label, term, price, when in legs:
        lines.append(f"  {label + ':':<{width}} ${price:,.1f}/MT  {term:<10} ({when})")
    terms = "; ".join(term for _, term, _, _ in legs)
    lines.append(
        f"  Ranking withheld: {len(legs)} delivery terms ({terms}), and no costed path "
        "from each to FOB vessel is applied here, so the gap between them is barge "
        "elevation and port margin as much as market. The origins page ranks only "
        "legs bridged to FOB vessel."
    )
    return "\n".join(lines)
