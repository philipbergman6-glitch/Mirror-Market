"""US GULF BASIS section — AMS CIF Gulf (NOLA barge) soybean export bids."""

import calendar
import logging

import pandas as pd

from pipeline.query import read_gulf_bids
from pipeline.units import to_metric_tons

logger = logging.getLogger(__name__)

_MAX_DELIVERIES = 3  # Current + two forward slots keeps the section tight


def _format_row(row: pd.Series) -> str:
    month_abbr = calendar.month_abbr[int(row["futures_month"])]
    basis = (
        f"{row['basis_low']:+.0f}¢"
        if row["basis_low"] == row["basis_high"]
        else f"{row['basis_low']:+.0f}/{row['basis_high']:+.0f}¢"
    )
    # AMS leaves the change column blank on some deliveries (#190).
    change = (
        f" ({row['basis_change']})" if pd.notna(row.get("basis_change")) else ""
    )
    line = (
        f"  {row['delivery']}: {basis}/bu over {month_abbr}{change} | "
        f"${row['average']:.2f}/bu"
    )
    usd_mt = to_metric_tons(row["average"] * 100.0, "Soybeans")
    if usd_mt is not None:
        line += f" ≈ ${usd_mt:,.1f}/MT"
    if pd.notna(row.get("year_ago")):
        line += f" | yr-ago ${row['year_ago']:.2f}/bu"
    return line


def format() -> str:  # noqa: A001
    try:
        df = read_gulf_bids("Soybeans")
    except Exception as exc:
        logger.debug("Gulf bids read failed: %s", exc)
        return "US GULF BASIS: Insufficient data"

    if df.empty:
        return "US GULF BASIS: Insufficient data"

    latest_date = df["report_date"].max()
    latest = df[df["report_date"] == latest_date].head(_MAX_DELIVERIES)

    lines = [
        "US GULF BASIS (CIF NOLA barge — AMS export bids, "
        f"{latest_date.date().isoformat()}):"
    ]
    lines.extend(_format_row(row) for _, row in latest.iterrows())
    return "\n".join(lines)
