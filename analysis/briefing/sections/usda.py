"""USDA FUNDAMENTALS section — one line per statistic, latest report only.

NASS QuickStats publishes the same crop-year statistic several times — the
March and June acreage reports, the August–November crop-production forecasts,
then the January final (``reference_period_desc`` "YEAR"). The usda table keys
on that descriptor, so a crop year carries every vintage. Printing them all put
five different 2026 production numbers in a row; this section prints the
latest report for the latest crop year, labels which report it is, and names
the prior report's figure as the revision.
"""

import re

import pandas as pd

from pipeline.query import read_usda

_MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
_VINTAGE = re.compile(r"YEAR - ([A-Z]{3}) (FORECAST|ACREAGE)")
#: "YEAR" is the annual final and supersedes every in-season vintage.
_FINAL_RANK = 13


def report_vintage(reference_period_desc: object) -> tuple[int, str] | None:
    """(rank, label) of one NASS annual-statistic vintage; None if unrecognised.

    Rank orders reports within one crop year: every in-season vintage by its
    release month, the annual final above them all.
    """
    desc = str(reference_period_desc).strip().upper()
    if desc == "YEAR":
        return _FINAL_RANK, "final"
    match = _VINTAGE.fullmatch(desc)
    if match is None or match.group(1) not in _MONTHS:
        return None
    month = match.group(1)
    return _MONTHS[month], f"{month.title()} {match.group(2).lower()}"


def _num(val: float) -> str:
    """NASS values at the precision they carry: yields in tenths, prices in cents."""
    if float(val).is_integer():
        return f"{val:,.0f}"
    if round(val * 10, 6).is_integer():
        return f"{val:,.1f}"
    return f"{val:,.2f}"


def _value(raw: object) -> float | None:
    try:
        val = float(str(raw).replace(",", ""))
    except (ValueError, TypeError):
        return None
    return None if pd.isna(val) else val


def _ranked(df: pd.DataFrame) -> pd.DataFrame:
    """Rows with a parsed value and a recognised vintage, latest report last."""
    out = df.copy()
    out["_val"] = out["Value"].map(_value)
    vintages = out.get("reference_period_desc", pd.Series([""] * len(out), index=out.index))
    parsed = vintages.map(report_vintage)
    out["_rank"] = parsed.map(lambda v: None if v is None else v[0])
    out["_label"] = parsed.map(lambda v: None if v is None else v[1])
    return out


def format() -> str:  # noqa: A001
    lines = ["USDA FUNDAMENTALS:"]
    usda_data = read_usda()

    if usda_data.empty:
        return "USDA FUNDAMENTALS: No data"

    # Monthly CRUSHED rows would spam the annual YoY table — they get
    # their own line in the crush section instead.
    if "stat_category" in usda_data.columns:
        usda_data = usda_data[usda_data["stat_category"] != "CRUSHED"].copy()
    if usda_data.empty:
        return "USDA FUNDAMENTALS: No data"

    usda_data["year_int"] = pd.to_numeric(usda_data["year"], errors="coerce")
    usda_data = usda_data.dropna(subset=["year_int"])

    if usda_data.empty:
        return "USDA FUNDAMENTALS: No valid year data"

    ranked = _ranked(usda_data)
    unrecognised = ranked[ranked["_rank"].isna()]
    ranked = ranked.dropna(subset=["_rank", "_val"])
    if ranked.empty:
        return "USDA FUNDAMENTALS: No rows with a recognised NASS report period"

    latest_year = int(ranked["year_int"].max())
    current = ranked[ranked["year_int"] == latest_year]

    for desc in sorted(current["short_desc"].astype(str).unique()):
        vintages = current[current["short_desc"] == desc].sort_values("_rank")
        latest = vintages.iloc[-1]
        val = float(latest["_val"])
        unit = str(latest.get("unit_desc", "") or "")
        parts = [f"{latest_year} {latest['_label']}"]

        if len(vintages) >= 2:
            prior = vintages.iloc[-2]
            change = val - float(prior["_val"])
            if _num(abs(change)) == _num(0.0):
                parts.append(f"unchanged vs {prior['_label']}")
            else:
                sign = "+" if change > 0 else "-"
                parts.append(
                    f"{sign}{_num(abs(change))} vs {prior['_label']} {_num(float(prior['_val']))}"
                )

        prev_year = ranked[
            (ranked["year_int"] == latest_year - 1) & (ranked["short_desc"] == desc)
        ].sort_values("_rank")
        if not prev_year.empty:
            prev = prev_year.iloc[-1]
            prev_val = float(prev["_val"])
            if prev_val != 0:
                yoy_pct = (val - prev_val) / prev_val * 100
                sign = "+" if yoy_pct >= 0 else ""
                parts.append(f"{sign}{yoy_pct:.1f}% YoY vs {latest_year - 1} {prev['_label']}")

        lines.append(f"  {desc}: {_num(val)} {unit} ({'; '.join(parts)})")

    # Only the year being printed: a vintage this parser cannot rank could be
    # the latest report, so the reader is told it was left out.
    unrecognised = unrecognised[unrecognised["year_int"] == latest_year]
    if not unrecognised.empty:
        periods = sorted(
            unrecognised.get("reference_period_desc", pd.Series(dtype=str)).astype(str).unique()
        )
        lines.append(
            f"  ({len(unrecognised)} row(s) withheld — unrecognised NASS report period: "
            f"{', '.join(repr(p) for p in periods)})"
        )

    if len(lines) == 1:
        lines.append("  Data available but no production/yield data found")

    return "\n".join(lines)
