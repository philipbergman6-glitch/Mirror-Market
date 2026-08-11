"""US GULF BASIS section — AMS CIF Gulf (NOLA barge) soybean export bids."""

import calendar
import logging

import pandas as pd

from pipeline.query import read_gulf_bids
from pipeline.units import to_metric_tons

logger = logging.getLogger(__name__)

_MAX_DELIVERIES = 3  # Current + two forward slots keeps the section tight


def _basis_phrase(row: pd.Series) -> str:
    """Basis quote with its contract month(s).

    A ranged quote can span two CBOT contracts ("95.00Q to 100.00X"), in
    which case each leg is labelled with its own month — pricing the high
    leg against the low leg's contract is simply the wrong futures (#196).
    Rows stored before the column existed carry NULL and read as one month.
    """
    month_low = calendar.month_abbr[int(row["futures_month"])]
    high = row.get("futures_month_high")
    month_high = calendar.month_abbr[int(high)] if pd.notna(high) else month_low

    if row["basis_low"] == row["basis_high"]:
        return f"{row['basis_low']:+.0f}¢/bu over {month_low}"
    if month_high != month_low:
        return (
            f"{row['basis_low']:+.0f}¢/bu over {month_low} / "
            f"{row['basis_high']:+.0f}¢/bu over {month_high}"
        )
    return f"{row['basis_low']:+.0f}/{row['basis_high']:+.0f}¢/bu over {month_low}"


def _format_row(row: pd.Series) -> str:
    # AMS leaves the change column blank on some deliveries (#190).
    change = (
        f" ({row['basis_change']})" if pd.notna(row.get("basis_change")) else ""
    )
    line = (
        f"  {row['delivery']}: {_basis_phrase(row)}{change} | "
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
