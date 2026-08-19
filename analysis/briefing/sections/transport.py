"""TRANSPORT (USDA AMS Grain Transportation Report) section — Layers 26/26b.

Two questions the rest of the briefing could not answer: what it costs to
move a cargo, and whether the boats at the US export ports are moving.

Both legs are rendered with their cadence named. The freight rate is
monthly and assessed by a broker; the vessel counts are weekly and lag the
report by a week. Neither is a daily print, and a reader coming off the
price sections must not read a flat line as a flat market.
"""

import pandas as pd

from config import GTR_OCEAN_ROUTES
from pipeline.query import read_ocean_freight_rates, read_port_vessel_activity

_HEADING = "TRANSPORT (USDA AMS Grain Transportation Report):"

# The two routes, in the order the spread is struck: Gulf minus PNW.
_GULF_ROUTE = GTR_OCEAN_ROUTES[1]
_PNW_ROUTE = GTR_OCEAN_ROUTES[3]


def _latest_two(frame: pd.DataFrame, date_column: str) -> tuple[pd.Series, pd.Series | None]:
    ordered = frame.sort_values(date_column)
    latest = ordered.iloc[-1]
    previous = ordered.iloc[-2] if len(ordered) >= 2 else None
    return latest, previous


def _freight_lines() -> list[str]:
    rates = read_ocean_freight_rates()
    if rates.empty:
        return ["  Ocean freight: No data"]

    lines: list[str] = []
    latest_by_route: dict[str, float] = {}
    as_of = None

    for route in (_GULF_ROUTE, _PNW_ROUTE):
        subset = rates[rates["route"] == route].dropna(subset=["rate_usd_mt"])
        if subset.empty:
            lines.append(f"  {route}: No data")
            continue

        latest, previous = _latest_two(subset, "Date")
        rate = float(latest["rate_usd_mt"])
        latest_by_route[route] = rate
        as_of = max(as_of, latest["Date"]) if as_of is not None else latest["Date"]

        if previous is not None and pd.notna(previous["rate_usd_mt"]):
            change = rate - float(previous["rate_usd_mt"])
            sign = "+" if change >= 0 else ""
            lines.append(
                f"  {route}: ${rate:,.2f}/mt ({sign}{change:,.2f} vs prior month)"
            )
        else:
            lines.append(f"  {route}: ${rate:,.2f}/mt")

    # The spread is the decision: which US coast is the cheaper way out.
    # Derived here rather than stored — the workbook publishes it, and we
    # use that published column only to prove the parse (fetchers/gtr.py).
    if _GULF_ROUTE in latest_by_route and _PNW_ROUTE in latest_by_route:
        spread = latest_by_route[_GULF_ROUTE] - latest_by_route[_PNW_ROUTE]
        lines.append(f"  Gulf over PNW: ${spread:,.2f}/mt")

    if as_of is not None:
        lines.append(
            f"  (monthly freight assessment, {as_of:%b %Y} — benchmark route to "
            "Japan, not a route-specific quote)"
        )
    return lines


def _vessel_lines() -> list[str]:
    activity = read_port_vessel_activity()
    if activity.empty:
        return ["  Vessel lineups: No data"]

    lines: list[str] = []
    as_of = None

    for region in sorted(activity["port_region"].unique()):
        subset = activity[activity["port_region"] == region]
        if subset.empty:
            continue

        latest, previous = _latest_two(subset, "week_ending")
        as_of = (
            max(as_of, latest["week_ending"]) if as_of is not None
            else latest["week_ending"]
        )

        parts: list[str] = []
        for column, label in (
            ("in_port", "in port"),
            ("loaded_7day", "loaded 7d"),
            ("due_10day", "due 10d"),
        ):
            value = latest.get(column)
            if pd.isna(value):
                continue
            if previous is not None and pd.notna(previous.get(column)):
                change = float(value) - float(previous[column])
                sign = "+" if change >= 0 else ""
                parts.append(f"{label} {value:,.0f} ({sign}{change:,.0f})")
            else:
                parts.append(f"{label} {value:,.0f}")

        if parts:
            lines.append(f"  {region}: " + ", ".join(parts))

    if as_of is not None:
        lines.append(f"  (vessel counts, week ending {as_of:%Y-%m-%d})")
    return lines or ["  Vessel lineups: No data"]


def format() -> str:  # noqa: A001
    """Render the transport block, or "" when neither layer has data.

    Empty rather than a "No data" heading: an unrun layer should leave no
    trace in the briefing, and the freshness block above already names any
    layer that failed.
    """
    freight = _freight_lines()
    vessels = _vessel_lines()

    if freight == ["  Ocean freight: No data"] and vessels == ["  Vessel lineups: No data"]:
        return ""

    return "\n".join([_HEADING, *freight, *vessels])
