"""CRUSH SPREAD section — Soybeans crush margin."""

import logging

import pandas as pd

from analysis.spreads import compute_crush_spread
from pipeline.units import to_metric_tons

logger = logging.getLogger(__name__)


def format(price_data: dict[str, pd.DataFrame]) -> str:  # noqa: A001
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
