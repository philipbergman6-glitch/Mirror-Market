"""CROSS-ORIGIN FOB BOARD — soybean export offers from the three origins.

US Gulf CIF (Layer 20, AMS) vs Brazil Paranaguá FOB (Layer 19, AgRural)
vs Argentina up-river FOB (Layer 21, MAGyP official) in USD/MT — the
"which origin is cheap" view for a physical buyer.

The Brazil leg is computed as CBOT + date-aligned AgRural basis
(`compute_brazil_basis`), not by converting the raw quote directly:
that keeps date alignment honest and publishes no more than the basis
line already does. The Gulf leg is a CIF barge bid (pre-elevation) —
comparable direction, not an identical Incoterm; the footer says so.
"""

import logging

import pandas as pd

from analysis.spreads import compute_brazil_basis
from pipeline.query import read_argentina_fob, read_brazil_spot, read_gulf_bids
from pipeline.units import to_metric_tons

logger = logging.getLogger(__name__)

_AGRURAL_SERIES = "Soybean (AgRural Paranaguá FOB)"


def _gulf_leg() -> tuple[float, str] | None:
    """Latest Current-delivery CIF NOLA soybean bid in USD/MT."""
    bids = read_gulf_bids("Soybeans")
    if bids.empty:
        return None
    latest_date = bids["report_date"].max()
    day = bids[bids["report_date"] == latest_date]
    # Prefer the "Current" delivery slot; some sessions quote only named
    # forward months, so fall back to the first (nearest) listed slot.
    current = day[day["delivery"] == "Current"]
    if current.empty:
        current = day.head(1)
    if current.empty:
        return None
    avg_bu = float(current["average"].iloc[0])
    usd_mt = to_metric_tons(avg_bu * 100.0, "Soybeans")  # $/bu → ¢/bu → USD/MT
    if usd_mt is None:
        return None
    return usd_mt, latest_date.date().isoformat()


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
    rows = fob[fob["date"] == latest_date].sort_values("ship_from")
    if rows.empty:
        return None
    return float(rows["price_usd_mt"].iloc[0]), latest_date.date().isoformat()


def format(  # noqa: A001
    price_data: dict[str, pd.DataFrame],
    currency_data: dict[str, pd.DataFrame],
) -> str:
    legs: dict[str, tuple[float, str]] = {}
    for label, loader in (
        ("US Gulf (CIF NOLA barge)", _gulf_leg),
        ("Brazil Paranaguá (FOB)", lambda: _brazil_leg(price_data, currency_data)),
        ("Argentina up-river (FOB)", _argentina_leg),
    ):
        try:
            leg = loader()
        except Exception as exc:
            logger.debug("FOB board leg %s failed: %s", label, exc)
            leg = None
        if leg is not None:
            legs[label] = leg

    if len(legs) < 2:
        return "CROSS-ORIGIN FOB BOARD: Insufficient data"

    lines = ["CROSS-ORIGIN FOB BOARD (Soybeans, USD/MT):"]
    for label, (price, when) in legs.items():
        lines.append(f"  {label + ':':<27} ${price:,.1f}/MT  ({when})")

    cheapest = min(legs.items(), key=lambda kv: kv[1][0])
    others = [v[0] for k, v in legs.items() if k != cheapest[0]]
    edge = min(others) - cheapest[1][0]
    lines.append(
        f"  Cheapest origin: {cheapest[0].split(' (')[0]} — ${edge:,.1f}/MT under next"
    )
    lines.append(
        "  (Gulf leg is a CIF barge bid pre-elevation; Brazil/Argentina are port FOB "
        "— directional comparison, not an Incoterm-identical quote)"
    )
    return "\n".join(lines)
