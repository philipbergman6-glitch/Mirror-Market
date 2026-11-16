"""WASDE ESTIMATES section — monthly USDA supply/demand forecasts."""

import pandas as pd

from pipeline.query import read_wasde


def _fmt(val: float) -> str:
    """Format a WASDE value keeping decimals that carry signal.

    Yields move in tenths of a bushel and farm prices in cents — rounding
    $10.50/bu to "11" is materially misleading. Small magnitudes keep two
    decimals, mid magnitudes one, big balance-sheet numbers none.
    """
    mag = abs(val)
    if mag < 100:
        return f"{val:,.2f}"
    if mag < 1000:
        return f"{val:,.1f}"
    return f"{val:,.0f}"


#: Decimals WASDE itself prints for each unit the fetcher stores
#: (``data/history/wasde.csv`` → ``unit``). A farm price moves in cents, a
#: yield in tenths of a bushel, cotton in hundredths of a million bales; the
#: balance-sheet quantities are whole numbers. A unit not listed here falls
#: back to :func:`_fmt`'s magnitude rule, which never cuts a small value to an
#: integer.
WASDE_UNIT_DECIMALS: dict[str, int] = {
    "$/bu": 2,
    "c/lb": 2,
    "$/s.t.": 2,
    "Bushels": 1,                    # yield per harvested acre
    "Pounds": 0,                     # cotton yield, lb/acre
    "Million Acres": 1,
    "Million Bushels": 0,
    "Million Pounds": 0,
    "Thousand Short Tons": 0,
    "Million 480 Pound Bales": 2,
}


def wasde_value_display(val: float, unit: str) -> str:
    """One WASDE value at the precision WASDE publishes it in."""
    decimals = WASDE_UNIT_DECIMALS.get(str(unit).strip())
    if decimals is None:
        return _fmt(val)
    return f"{val:,.{decimals}f}"


def wasde_commodity_display(commodity: str) -> str:
    """``SOYBEAN_MEAL`` → ``Soybean Meal`` — the stored key is not a heading."""
    return str(commodity).replace("_", " ").strip().title()


def format() -> str:  # noqa: A001
    lines = ["WASDE ESTIMATES (USDA Monthly Forecasts):"]
    wasde = read_wasde()

    if wasde.empty:
        return "WASDE ESTIMATES: No data"

    for commodity in wasde["commodity"].unique():
        subset = wasde[wasde["commodity"] == commodity]
        if subset.empty:
            continue

        commodity_lines = []
        for attribute in subset["attribute"].unique():
            attr_data = subset[subset["attribute"] == attribute].copy()
            if attr_data.empty:
                continue

            # WASDE rows include multiple marketing years per release (e.g. 2024/25
            # Est. and 2025/26 Proj. in the same April report). Pin to the latest
            # MY so the MoM revision math compares like with like.
            latest_my = attr_data["year"].max()
            attr_data = attr_data[attr_data["year"] == latest_my]
            attr_data = attr_data.sort_values("reference_period")
            latest = attr_data.iloc[-1]
            val = latest.get("value")
            unit = latest.get("unit", "")
            ref = latest.get("reference_period", "")

            if pd.isna(val):
                continue

            if len(attr_data) >= 2:
                prev = attr_data.iloc[-2]
                prev_val = prev.get("value")
                if pd.notna(prev_val) and prev_val != 0:
                    revision = val - prev_val
                    if revision == 0:
                        commodity_lines.append(
                            f"    {attribute}: {_fmt(val)} {unit} "
                            "(unchanged vs prior month)"
                        )
                    else:
                        sign = "+" if revision > 0 else ""
                        direction = "UP" if revision > 0 else "DOWN"
                        commodity_lines.append(
                            f"    {attribute}: {_fmt(val)} {unit} "
                            f"(revised {direction} {sign}{_fmt(revision)} vs prior month)"
                        )
                    continue

            commodity_lines.append(f"    {attribute}: {_fmt(val)} {unit} ({ref})")

        if commodity_lines:
            lines.append(f"  {commodity}:")
            lines.extend(commodity_lines)

    if len(lines) == 1:
        lines.append("  Data available but no forecast data found")

    return "\n".join(lines)
