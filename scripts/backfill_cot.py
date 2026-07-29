"""
One-time COT history backfill (issue #13).

Downloads CFTC legacy futures-and-options yearly files back to 2010 and
upserts them into the `cot` table so COT z-scores rest on a full trailing
3-year baseline from day one.

Older CFTC files differ from current ones in two ways the daily fetcher
does not handle:
  - every market name carries trailing whitespace (e.g. 2010 files)
  - CBOT wheat was "WHEAT - CHICAGO BOARD OF TRADE" before the SRW rename

So this script matches on stripped names and carries explicit aliases.
INSERT OR REPLACE keyed on (commodity, Date) makes re-runs safe.

Usage:
    python scripts/backfill_cot.py            # 2010..2024
    python scripts/backfill_cot.py 2012 2014  # explicit start/end year
"""

import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import COT_COMMODITIES, COT_REPORT_TYPE, setup_logging
from fetchers.cot import _COL_MAP
from pipeline.clean import clean_cot
from pipeline.store import save_cot_data

logger = logging.getLogger(__name__)

# Historical CFTC market names → our commodity names (matched after .strip()).
_ALIASES = {cftc: ours for ours, cftc in COT_COMMODITIES.items()}
_ALIASES["WHEAT - CHICAGO BOARD OF TRADE"] = "Wheat"  # pre-SRW-rename name


def backfill_year(year: int) -> dict[str, int]:
    """Fetch one CFTC year and upsert all tracked commodities. Returns rows saved per commodity."""
    import cot_reports as cot

    logger.info("Downloading COT year %d ...", year)
    df = cot.cot_year(year, cot_report_type=COT_REPORT_TYPE)
    if df.empty:
        raise RuntimeError(f"CFTC returned no data for {year}")

    missing_cols = [c for c in _COL_MAP if c not in df.columns]
    if missing_cols:
        raise RuntimeError(f"{year}: expected columns missing: {missing_cols}")

    df = df.copy()
    df["_name"] = df["Market and Exchange Names"].str.strip()
    df["_commodity"] = df["_name"].map(_ALIASES)
    df = df[df["_commodity"].notna()]

    counts: dict[str, int] = {}
    for our_name in COT_COMMODITIES:
        sub = df[df["_commodity"] == our_name]
        if sub.empty:
            counts[our_name] = 0
            continue
        sub = sub[list(_COL_MAP.keys())].rename(columns=_COL_MAP)
        sub = sub.drop(columns=["market_name"])
        sub["commercial_net"] = sub["commercial_long"] - sub["commercial_short"]
        sub["noncommercial_net"] = sub["noncommercial_long"] - sub["noncommercial_short"]
        sub = clean_cot(sub)
        save_cot_data(our_name, sub)
        counts[our_name] = len(sub)

    empty = [name for name, n in counts.items() if n == 0]
    if empty:
        raise RuntimeError(f"{year}: no rows matched for {empty} — name alias missing?")
    return counts


def main() -> None:
    setup_logging()
    start = int(sys.argv[1]) if len(sys.argv) > 1 else 2010
    end = int(sys.argv[2]) if len(sys.argv) > 2 else 2024

    for year in range(start, end + 1):
        counts = backfill_year(year)
        logger.info("%d: %s", year, ", ".join(f"{k}={v}" for k, v in counts.items()))

    # cot_reports drops its download in the working directory
    if os.path.exists("annualof.txt"):
        os.remove("annualof.txt")
    logger.info("Backfill complete (%d–%d).", start, end)


if __name__ == "__main__":
    main()
