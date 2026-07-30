"""EXPORT SALES section — weekly USDA FAS demand data."""

import logging

import pandas as pd

from pipeline.query import read_export_sales, read_wasde

logger = logging.getLogger(__name__)

# Soybean bushel → metric ton. Mirrors pipeline/units.py (36.7437 bu/MT);
# WASDE stores US soybean exports in million bushels.
MT_PER_MILLION_BUSHELS_SOY = 1_000_000 / 36.7437

# The % of forecast is only computed when the WASDE row carries this unit —
# on a mismatch we show both raw numbers with their units instead.
_WASDE_SOY_EXPORT_UNIT = "million bushels"

# Year-ago accumulated comparison: prior-year week must land within this
# many days of (latest week − 364d) to count as "the same week".
_YR_AGO_TOLERANCE_DAYS = 10


def soy_marketing_year(week: pd.Timestamp) -> str:
    """US soybean marketing year (Sep 1 start) containing `week`, e.g. '2025/26'."""
    start = week.year if week.month >= 9 else week.year - 1
    return f"{start}/{str(start + 1)[-2:]}"


def wasde_soy_export_forecast(marketing_year: str) -> dict:
    """Latest WASDE US soybean export forecast for `marketing_year`.

    Returns {value_mt, raw_value, unit}; value_mt is None when the stored
    unit is not million bushels (no silent bogus conversion).
    """
    out: dict = {"value_mt": None, "raw_value": None, "unit": None}
    wasde = read_wasde("SOYBEANS")
    if wasde.empty:
        return out
    rows = wasde[
        (wasde["attribute"].astype(str).str.strip().str.lower() == "exports")
        & (wasde["year"].astype(str) == marketing_year)
    ].sort_values("reference_period")
    if rows.empty:
        return out
    latest = rows.iloc[-1]
    if pd.isna(latest["value"]):
        return out
    out["raw_value"] = float(latest["value"])
    out["unit"] = str(latest.get("unit", "") or "") or None
    if (out["unit"] or "").strip().lower() == _WASDE_SOY_EXPORT_UNIT:
        out["value_mt"] = out["raw_value"] * MT_PER_MILLION_BUSHELS_SOY
    return out


def year_ago_accumulated(subset: pd.DataFrame, latest_week: pd.Timestamp) -> dict:
    """Accumulated exports for the same week one year earlier, if present.

    Returns {week_ending, accumulated_mt}; both None when no prior-year
    week lands within the tolerance window (degrade silently).
    """
    out: dict = {"week_ending": None, "accumulated_mt": None}
    target = latest_week - pd.Timedelta(days=364)
    weeks = subset["week_ending"].dropna().unique()
    candidates = [
        w for w in weeks
        if abs((pd.Timestamp(w) - target).days) <= _YR_AGO_TOLERANCE_DAYS
    ]
    if not candidates:
        return out
    week = min(candidates, key=lambda w: abs((pd.Timestamp(w) - target).days))
    rows = subset[subset["week_ending"] == week]
    accumulated = rows["accumulated_exports"].sum(skipna=True)
    if pd.isna(accumulated) or accumulated <= 0:
        return out
    out["week_ending"] = pd.Timestamp(week)
    out["accumulated_mt"] = float(accumulated)
    return out


def _soybean_pace_lines(subset: pd.DataFrame, latest_week: pd.Timestamp) -> list[str]:
    """Commitments vs WASDE export forecast + year-ago accumulated comparison."""
    week_data = subset[subset["week_ending"] == latest_week]
    accumulated = week_data["accumulated_exports"].sum(skipna=True)
    outstanding = week_data["outstanding_sales"].sum(skipna=True)
    if (pd.isna(accumulated) or accumulated <= 0) and (pd.isna(outstanding) or outstanding <= 0):
        return []
    accumulated = 0.0 if pd.isna(accumulated) else float(accumulated)
    outstanding = 0.0 if pd.isna(outstanding) else float(outstanding)
    committed = accumulated + outstanding

    my = soy_marketing_year(pd.Timestamp(latest_week))
    pace = (
        f"    Pace (MY {my}): accumulated {accumulated / 1e6:,.2f} MMT + "
        f"outstanding {outstanding / 1e6:,.2f} MMT = {committed / 1e6:,.2f} MMT committed"
    )

    forecast = wasde_soy_export_forecast(my)
    if forecast["value_mt"]:
        pct = committed / forecast["value_mt"] * 100
        pace += f" ({pct:.0f}% of WASDE {forecast['value_mt'] / 1e6:,.2f} MMT export forecast)"
    elif forecast["raw_value"] is not None:
        # Unit mismatch — show both raw numbers, never a bogus %.
        pace += (
            f" | WASDE export forecast: {forecast['raw_value']:,.0f} "
            f"{forecast['unit'] or 'unknown unit'} (unit mismatch — no % computed)"
        )

    lines = [pace]
    yr_ago = year_ago_accumulated(subset, pd.Timestamp(latest_week))
    if yr_ago["accumulated_mt"]:
        prev = yr_ago["accumulated_mt"]
        chg = (accumulated - prev) / prev * 100
        sign = "+" if chg >= 0 else ""
        week_str = yr_ago["week_ending"].strftime("%m/%d/%y")
        lines.append(
            f"    vs year-ago accumulated (w/e {week_str}): "
            f"{prev / 1e6:,.2f} MMT ({sign}{chg:.1f}%)"
        )
    return lines


def format() -> str:  # noqa: A001
    lines = ["EXPORT SALES (USDA Weekly):"]
    es_data = read_export_sales()

    if es_data.empty:
        return "EXPORT SALES (USDA Weekly): No data (set FAS_API_KEY to enable)"

    for commodity in es_data["commodity"].unique():
        subset = es_data[es_data["commodity"] == commodity]
        if subset.empty:
            continue

        latest_week = subset["week_ending"].max()
        week_data = subset[subset["week_ending"] == latest_week]

        total_net_sales = week_data["net_sales"].sum() if "net_sales" in week_data.columns else 0
        total_exports = week_data["weekly_exports"].sum() if "weekly_exports" in week_data.columns else 0

        top_buyers = week_data.nlargest(3, "net_sales") if "net_sales" in week_data.columns else pd.DataFrame()
        buyer_parts = []
        for _, row in top_buyers.iterrows():
            country = row.get("country", "Unknown")
            sales = row.get("net_sales", 0)
            if pd.notna(sales) and sales != 0:
                buyer_parts.append(f"{country} ({sales:,.0f} MT)")

        parts = [f"Net sales: {total_net_sales:,.0f} MT"]
        if total_exports:
            parts.append(f"Exports: {total_exports:,.0f} MT")
        if buyer_parts:
            parts.append(f"Top buyers: {', '.join(buyer_parts)}")

        week_str = latest_week.strftime("%m/%d") if hasattr(latest_week, "strftime") else str(latest_week)
        lines.append(f"  {commodity} (w/e {week_str}): {' | '.join(parts)}")

        if commodity == "Soybeans":
            try:
                lines.extend(_soybean_pace_lines(subset, latest_week))
            except Exception as exc:
                logger.debug("Soybean export pace failed: %s", exc)

    if len(lines) == 1:
        lines.append("  Data available but no sales data found")

    return "\n".join(lines)
