"""CRUSH SPREAD section — Soybeans crush margin + NASS actual crush volume."""

import logging

import pandas as pd

from analysis.nass_crush import latest_crush
from analysis.spreads import compute_crush_spread
from pipeline.units import to_metric_tons

logger = logging.getLogger(__name__)


def _nass_crush_line() -> str:
    """One line with the latest NASS monthly crush volume + YoY, '' if absent."""
    crush = latest_crush()
    if not crush:
        return ""
    line = (
        f"  US crush (NASS): {crush['value']:,.0f} {crush['unit']}".rstrip()
        + f" in {crush['period']} {crush['year']}"
    )
    if crush["yoy_pct"] is not None:
        sign = "+" if crush["yoy_pct"] >= 0 else ""
        line += f" ({sign}{crush['yoy_pct']:.1f}% YoY)"
    return line


def format(price_data: dict[str, pd.DataFrame]) -> str:  # noqa: A001
    spread_line = _spread_line(price_data)
    try:
        nass_line = _nass_crush_line()
    except Exception as exc:
        logger.debug("NASS crush line error: %s", exc)
        nass_line = ""
    return "\n".join(part for part in (spread_line, nass_line) if part)


def _spread_line(price_data: dict[str, pd.DataFrame]) -> str:
    soybeans = price_data.get("Soybeans", pd.DataFrame())
    oil = price_data.get("Soybean Oil", pd.DataFrame())
    meal = price_data.get("Soybean Meal", pd.DataFrame())

    if soybeans.empty or oil.empty or meal.empty:
        return "CRUSH SPREAD: Insufficient data"

    try:
        spread = compute_crush_spread(soybeans, oil, meal)
        if spread.empty:
            return "CRUSH SPREAD: No overlapping dates"

        latest_cents = spread.iloc[-1]["crush_spread"]
        oil_share = spread.iloc[-1]["oil_value_share"]
        crush_mt = to_metric_tons(latest_cents, "Soybeans")
        if len(spread) >= 6:
            prev = spread.iloc[-6]["crush_spread"]
            trend = "widening" if latest_cents > prev else "narrowing"
            profitability = "processors profitable" if latest_cents > 0 else "margin squeeze"
            return f"CRUSH SPREAD: ${crush_mt:,.1f}/MT ({trend} — {profitability}, oil share {oil_share:.0%})"
        return f"CRUSH SPREAD: ${crush_mt:,.1f}/MT (oil share {oil_share:.0%})"
    except Exception as exc:
        logger.debug("Crush spread error: %s", exc)
        return "CRUSH SPREAD: Calculation error"
