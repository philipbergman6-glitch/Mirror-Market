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


# (origin, delivery term) per leg, in print order. The term is part of the
# price, so it is printed on the row, never left to a footer.
_LEGS: tuple[tuple[str, str], ...] = (
    ("US Gulf", "CIF NOLA barge bid"),
    ("Brazil Paranaguá", "port bid (DAP truck)"),
    ("Argentina up-river", "official FOB vessel"),
)


def format(  # noqa: A001
    price_data: dict[str, pd.DataFrame],
    currency_data: dict[str, pd.DataFrame],
) -> str:
    loaders = {
        "US Gulf": _gulf_leg,
        "Brazil Paranaguá": lambda: _brazil_leg(price_data, currency_data),
        "Argentina up-river": _argentina_leg,
    }
    legs: list[tuple[str, str, float, str]] = []
    for origin, term in _LEGS:
        try:
            leg = loaders[origin]()
        except Exception as exc:
            logger.debug("Cross-origin leg %s failed: %s", origin, exc)
            leg = None
        if leg is not None:
            legs.append((origin, term, *leg))

    if len(legs) < 2:
        return "CROSS-ORIGIN PORT LEVELS: Insufficient data"

    lines = ["CROSS-ORIGIN PORT LEVELS (Soybeans, USD/MT, each on its own delivery term):"]
    for origin, term, price, when in legs:
        lines.append(f"  {origin + ':':<20} ${price:,.1f}/MT  {term:<20} ({when})")
    terms = "; ".join(term for _, term, _, _ in legs)
    lines.append(
        f"  Ranking withheld: {len(legs)} delivery terms ({terms}), and no costed path "
        "from each to FOB vessel is applied here, so the gap between them is barge "
        "elevation and port margin as much as market. The origins page ranks only "
        "legs bridged to FOB vessel."
    )
    return "\n".join(lines)
