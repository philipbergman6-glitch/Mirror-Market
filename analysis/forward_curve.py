"""
Forward curve analysis — contango, backwardation, and calendar spreads.

The forward curve is the term structure of futures prices — plotting
price vs delivery month. It reveals what the market expects:

    - Contango (upward-sloping): future months cost more than nearby.
      This happens when supply is abundant and storage costs dominate.
      Traders pay more for later delivery to cover storage, insurance,
      and financing costs.

    - Backwardation (downward-sloping): nearby months cost more than
      deferred.  This signals tight supply or strong immediate demand.
      Buyers are willing to pay a premium to get the commodity NOW.

    - Calendar spread: the price difference between two contract months.
      Widening spreads signal changing supply/demand expectations.

Key concepts for learning:
    - "Carry" = storage + insurance + financing costs
    - Full carry contango = futures are at the maximum cost-of-carry premium
    - Inverted market = backwardation — historically rare and bullish for nearby
    - The curve slope is a quick summary: positive = contango, negative = backwardation
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def analyze_curve(df: pd.DataFrame) -> dict:
    """
    Analyze a forward curve for a single commodity.

    Parameters
    ----------
    df : pd.DataFrame
        Forward curve data with columns: contract_month, close, label.
        Must be sorted by contract_month (nearest first).

    Returns
    -------
    dict with keys:
        - structure: "contango", "backwardation", or "mixed"
        - front_price: float — nearest contract price
        - back_price: float — furthest contract price
        - spread: float — back minus front
        - spread_pct: float — spread as % of front price
        - num_contracts: int
        - summary: str — human-readable summary
    """
    if df.empty or len(df) < 2:
        return {}

    df = df.sort_values("contract_month").reset_index(drop=True)
    front = df.iloc[0]["close"]
    back = df.iloc[-1]["close"]
    spread = back - front
    spread_pct = (spread / front) * 100 if front != 0 else 0

    # Determine structure by looking at sequential price changes
    increases = 0
    decreases = 0
    for i in range(1, len(df)):
        if df.iloc[i]["close"] > df.iloc[i - 1]["close"]:
            increases += 1
        elif df.iloc[i]["close"] < df.iloc[i - 1]["close"]:
            decreases += 1

    total_moves = increases + decreases
    if total_moves == 0:
        structure = "flat"
    elif increases > decreases * 2:
        structure = "contango"
    elif decreases > increases * 2:
        structure = "backwardation"
    elif increases > decreases:
        structure = "mild contango"
    elif decreases > increases:
        structure = "mild backwardation"
    else:
        structure = "mixed"

    # Build summary
    if "backwardation" in structure:
        implication = "tight supply / strong nearby demand"
    elif "contango" in structure:
        implication = "adequate supply / carrying costs priced in"
    else:
        implication = "mixed signals across the curve"

    front_label = df.iloc[0].get("label", "front")
    back_label = df.iloc[-1].get("label", "back")

    summary = (
        f"{structure.title()}: {front_label} {front:.2f} → {back_label} {back:.2f} "
        f"({spread:+.2f}, {spread_pct:+.1f}%) — {implication}"
    )

    return {
        "structure": structure,
        "front_price": front,
        "back_price": back,
        "spread": spread,
        "spread_pct": spread_pct,
        "num_contracts": len(df),
        "summary": summary,
    }


def curve_slope(df: pd.DataFrame) -> float | None:
    """
    Compute the average slope of the forward curve (price change per month).

    A positive slope means contango; negative means backwardation.

    Parameters
    ----------
    df : pd.DataFrame
        Forward curve data with columns: contract_month, close.

    Returns
    -------
    float or None
        Average price change per month gap, or None if insufficient data.
    """
    if df.empty or len(df) < 2:
        return None

    df = df.sort_values("contract_month").reset_index(drop=True)

    # Convert contract_month to datetime for month gap calculation
    months = pd.to_datetime(df["contract_month"])
    total_months = (months.iloc[-1].year - months.iloc[0].year) * 12 + \
                   (months.iloc[-1].month - months.iloc[0].month)

    if total_months == 0:
        return None

    price_diff = df.iloc[-1]["close"] - df.iloc[0]["close"]
    return price_diff / total_months


def calendar_spread(df: pd.DataFrame, near_idx: int = 0, far_idx: int = 1) -> dict:
    """
    Compute the calendar spread between two contract months.

    Parameters
    ----------
    df : pd.DataFrame
        Forward curve data sorted by contract_month.
    near_idx : int
        Index of the near-month contract (default 0 = front month).
    far_idx : int
        Index of the far-month contract (default 1 = second month).

    Returns
    -------
    dict with keys:
        - near_label, near_price, far_label, far_price
        - spread: float — far minus near
        - spread_pct: float — as % of near price
    """
    if df.empty or len(df) <= max(near_idx, far_idx):
        return {}

    df = df.sort_values("contract_month").reset_index(drop=True)
    near = df.iloc[near_idx]
    far = df.iloc[far_idx]

    spread = far["close"] - near["close"]
    spread_pct = (spread / near["close"]) * 100 if near["close"] != 0 else 0

    return {
        "near_label": near.get("label", ""),
        "near_price": near["close"],
        "far_label": far.get("label", ""),
        "far_price": far["close"],
        "spread": spread,
        "spread_pct": spread_pct,
    }
