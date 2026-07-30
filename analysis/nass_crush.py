"""Latest NASS monthly soybean crush summary.

Layer 14 stores NASS "SOYBEANS - CRUSHED" rows in the `usda` table
(stat_category="CRUSHED", monthly reference periods like "JAN".."DEC",
Value in the row's native unit_desc — typically TONS). This module
distills those rows into a single latest-month dict shared by the
briefing crush section, the snapshot, and soy_analytics so the three
consumers don't drift.
"""

from __future__ import annotations

import pandas as pd

from pipeline.query import read_usda

# NASS reference_period_desc month names → calendar order.
_MONTH_ORDER = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def _numeric_value(raw: object) -> float | None:
    """Parse a NASS Value string ('6,143,000' or '(D)') to float, None if not a number."""
    try:
        return float(str(raw).replace(",", ""))
    except (TypeError, ValueError):
        return None


def latest_crush() -> dict | None:
    """Return the latest monthly NASS crush volume with YoY comparison.

    Returns
    -------
    dict | None
        {value, unit, year, month, period, yoy_pct} where `period` is the
        NASS month string (e.g. "JUN") and yoy_pct is None when the same
        month a year earlier is absent. None when no usable rows exist.
    """
    usda = read_usda("CRUSHED")
    if usda.empty:
        return None

    df = usda.copy()
    for col in ("year", "reference_period_desc", "Value"):
        if col not in df.columns:
            return None

    df["month_num"] = (
        df["reference_period_desc"].astype(str).str.strip().str.upper().map(_MONTH_ORDER)
    )
    df["year_int"] = pd.to_numeric(df["year"], errors="coerce")
    df["value_num"] = df["Value"].map(_numeric_value)
    df = df.dropna(subset=["month_num", "year_int", "value_num"])
    if df.empty:
        return None

    df = df.sort_values(["year_int", "month_num"])
    latest = df.iloc[-1]

    yoy_pct = None
    prior = df[
        (df["year_int"] == latest["year_int"] - 1)
        & (df["month_num"] == latest["month_num"])
        & (df["short_desc"] == latest["short_desc"])
    ]
    if not prior.empty:
        prev_val = float(prior.iloc[-1]["value_num"])
        if prev_val != 0:
            yoy_pct = (float(latest["value_num"]) - prev_val) / prev_val * 100

    return {
        "value": float(latest["value_num"]),
        "unit": str(latest.get("unit_desc", "") or ""),
        "year": int(latest["year_int"]),
        "month": int(latest["month_num"]),
        "period": str(latest["reference_period_desc"]).strip().upper(),
        "yoy_pct": yoy_pct,
    }
