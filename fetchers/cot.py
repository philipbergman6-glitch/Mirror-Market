"""
Layer 4 — CFTC Commitment of Traders (COT) data.

The COT report shows what different types of traders are doing:
    - Commercial traders (hedgers): farmers, processors who actually USE
      the commodity — they hedge to lock in prices.
    - Non-commercial (speculators): hedge funds, managed money betting
      on price moves.
    - Nonreportable: small traders below CFTC reporting thresholds.

When speculators are extremely long or short, it can signal a potential
price reversal.  Published every Friday (data from the previous Tuesday).

Key concepts for learning:
    - Third-party libraries (cot_reports) can save hours of web scraping
    - Filtering a large DataFrame: df[df["col"].isin(values)]
    - Renaming columns with a dict: df.rename(columns={...})
    - Calculating derived columns: net = long - short
"""

import logging
import os
import time

import pandas as pd

from config import (
    COT_REPORT_TYPE, COT_COMMODITIES,
    MAX_RETRIES, RETRY_DELAY,
)

logger = logging.getLogger(__name__)

# Column mapping: CFTC's verbose names → our clean names
_COL_MAP = {
    "As of Date in Form YYYY-MM-DD": "Date",
    "Market and Exchange Names":     "market_name",
    "Open Interest (All)":           "total_open_interest",
    "Commercial Positions-Long (All)":      "commercial_long",
    "Commercial Positions-Short (All)":     "commercial_short",
    "Noncommercial Positions-Long (All)":   "noncommercial_long",
    "Noncommercial Positions-Short (All)":  "noncommercial_short",
}


def fetch_cot_year(year: int) -> pd.DataFrame:
    """
    Fetch COT data for a single year, filtered to our commodities.

    Parameters
    ----------
    year : int
        Calendar year to fetch (e.g. 2025).

    Returns
    -------
    pd.DataFrame
        Filtered COT data with clean column names.
        Empty DataFrame on failure.
    """
    # Import here so the library is only loaded when actually needed.
    # This avoids startup slowdowns if COT data isn't required.
    import cot_reports as cot

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Downloading COT data for %d (attempt %d) ...", year, attempt)

            # cot_reports downloads a text file and returns a DataFrame
            df = cot.cot_year(year, cot_report_type=COT_REPORT_TYPE)

            if df.empty:
                logger.warning("No COT data returned for %d", year)
                return pd.DataFrame()

            # Filter to only our commodities
            target_names = list(COT_COMMODITIES.values())
            df = df[df["Market and Exchange Names"].isin(target_names)]

            if df.empty:
                logger.warning("No matching commodities in COT data for %d", year)
                return pd.DataFrame()

            # Keep and rename only the columns we need
            df = df[list(_COL_MAP.keys())].rename(columns=_COL_MAP)

            # Calculate net positions (long - short)
            df["commercial_net"] = df["commercial_long"] - df["commercial_short"]
            df["noncommercial_net"] = df["noncommercial_long"] - df["noncommercial_short"]

            logger.info("Got %d COT rows for %d", len(df), year)
            return df

        except Exception as exc:
            logger.warning(
                "Attempt %d/%d failed for COT %d: %s",
                attempt, MAX_RETRIES, year, exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    logger.error("All %d attempts failed for COT %d — returning empty DataFrame",
                 MAX_RETRIES, year)
    return pd.DataFrame()


def fetch_cot_recent(years_back: int = 2) -> dict[str, pd.DataFrame]:
    """
    Fetch recent COT data for all tracked commodities.

    Parameters
    ----------
    years_back : int
        How many years of history to fetch (default 2).

    Returns
    -------
    dict[str, pd.DataFrame]
        {commodity_name: DataFrame} with columns:
            Date, commercial_long, commercial_short, commercial_net,
            noncommercial_long, noncommercial_short, noncommercial_net,
            total_open_interest
    """
    from datetime import datetime
    current_year = datetime.now().year

    # Fetch each year and combine
    all_frames = []
    for year in range(current_year - years_back + 1, current_year + 1):
        df = fetch_cot_year(year)
        if not df.empty:
            all_frames.append(df)

    if not all_frames:
        logger.warning("No COT data fetched for any year")
        return {name: pd.DataFrame() for name in COT_COMMODITIES}

    combined = pd.concat(all_frames, ignore_index=True)

    # Split into per-commodity DataFrames
    # Reverse the COT_COMMODITIES dict: CFTC name → our name
    cftc_to_name = {v: k for k, v in COT_COMMODITIES.items()}

    results = {}
    for cftc_name, our_name in cftc_to_name.items():
        commodity_df = combined[combined["market_name"] == cftc_name].copy()
        commodity_df = commodity_df.drop(columns=["market_name"])
        commodity_df = commodity_df.sort_values("Date").reset_index(drop=True)
        results[our_name] = commodity_df
        if not commodity_df.empty:
            logger.info("  %s: %d COT reports", our_name, len(commodity_df))

    # Clean up any temp files the library may have downloaded
    for fname in ("annualof.txt",):
        if os.path.exists(fname):
            os.remove(fname)
    return results


# ── Quick self-test ─────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_cot_recent(years_back=1)
    logger.info("=== COT Summary ===")
    for name, df in data.items():
        if df.empty:
            logger.info("  %s: no data", name)
        else:
            latest = df.iloc[-1]
            logger.info(
                "  %s: %d reports, latest commercial net = %+d, speculator net = %+d",
                name, len(df),
                int(latest["commercial_net"]),
                int(latest["noncommercial_net"]),
            )
