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
import urllib.error
import zipfile

import pandas as pd
import requests

from config import (
    COT_COMMODITIES,
    COT_REPORT_TYPE,
    MAX_RETRIES,
)
from fetchers._backoff import retry_sleep
from pipeline.results import ScraperShapeError

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

_MARKET_COL = "Market and Exchange Names"


def _download_cot_year(year: int) -> pd.DataFrame:
    """Download one calendar year of the configured COT report.

    Isolated as the single network seam so the parsing rules above it can be
    exercised against captured CFTC artifacts.
    """
    # Import here so the library is only loaded when actually needed.
    # This avoids startup slowdowns if COT data isn't required.
    import cot_reports as cot

    return cot.cot_year(year, cot_report_type=COT_REPORT_TYPE)


def _filter_to_tracked(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Filter the raw CFTC frame to the markets in ``COT_COMMODITIES``.

    Raises ``ScraperShapeError`` when any configured market name is absent
    from the file. CFTC renames contracts (the SRW wheat market was
    ``WHEAT - CHICAGO BOARD OF TRADE`` until it became ``WHEAT-SRW - CHICAGO
    BOARD OF TRADE``), and a rename used to slip through as an empty
    commodity that looked exactly like "this contract had no open interest".
    A rename is upstream drift, not a transient fault: it must be loud.
    """
    if _MARKET_COL not in df.columns:
        raise ScraperShapeError(
            f"COT {year}: column {_MARKET_COL!r} missing from CFTC file; "
            f"got {list(df.columns)[:5]}"
        )

    df = df.copy()
    # Older CFTC vintages pad market names with a trailing space; that is
    # formatting drift, not a rename, so normalise before matching.
    df[_MARKET_COL] = df[_MARKET_COL].astype(str).str.strip()

    target_names = list(COT_COMMODITIES.values())
    present = set(df[_MARKET_COL].unique())
    missing = [name for name in target_names if name not in present]
    if missing:
        raise ScraperShapeError(
            f"COT {year}: CFTC file has no rows for {len(missing)} configured "
            f"market name(s) — {missing}. Either the contract was renamed or "
            f"COT_COMMODITIES in config.py is stale; refusing to publish an "
            f"empty commodity."
        )

    return df[df[_MARKET_COL].isin(target_names)]


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
        Empty DataFrame when the download failed on every attempt.

    Raises
    ------
    ScraperShapeError
        When the CFTC file no longer carries every configured market name.
        Not retried — a rename does not heal on the next attempt.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Downloading COT data for %d (attempt %d) ...", year, attempt)

            # cot_reports downloads a text file and returns a DataFrame
            df = _download_cot_year(year)

            if df.empty:
                logger.warning("No COT data returned for %d", year)
                return pd.DataFrame()

            # Filter to only our commodities — hard-fails on a market rename
            df = _filter_to_tracked(df, year)

            # Keep and rename only the columns we need
            df = df[list(_COL_MAP.keys())].rename(columns=_COL_MAP)

            # Calculate net positions (long - short)
            df["commercial_net"] = df["commercial_long"] - df["commercial_short"]
            df["noncommercial_net"] = df["noncommercial_long"] - df["noncommercial_short"]

            logger.info("Got %d COT rows for %d", len(df), year)
            return df

        except ScraperShapeError:
            # Upstream drift, not a transient fault. Retrying it only delays
            # the failure, and swallowing it (ScraperShapeError subclasses
            # ValueError) would silently blank the whole layer.
            raise

        except (
            requests.RequestException,
            urllib.error.URLError,
            zipfile.BadZipFile,
            OSError,
            KeyError,
            ValueError,
            IndexError,
        ) as exc:
            # cot_reports wraps the CFTC text download; failure modes are
            # transport errors (requests/urllib), a non-zip body served with
            # HTTP 200 (BadZipFile — cftc.gov answers redirects and outages
            # with HTML), local extraction errors (OSError), and schema drift
            # (KeyError/IndexError) when the report layout changes.
            logger.warning(
                "Attempt %d/%d failed for COT %d: %s",
                attempt, MAX_RETRIES, year, exc,
            )
            if attempt < MAX_RETRIES:
                retry_sleep(attempt)

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
    blank: list[str] = []
    for cftc_name, our_name in cftc_to_name.items():
        commodity_df = combined[combined["market_name"] == cftc_name].copy()
        commodity_df = commodity_df.drop(columns=["market_name"])
        commodity_df = commodity_df.sort_values("Date").reset_index(drop=True)
        results[our_name] = commodity_df
        if commodity_df.empty:
            blank.append(f"{our_name} ({cftc_name})")
        else:
            logger.info("  %s: %d COT reports", our_name, len(commodity_df))

    # Clean up any temp files the library may have downloaded
    for fname in ("annualof.txt",):
        if os.path.exists(fname):
            os.remove(fname)

    # Data arrived, but a tracked commodity came out empty: the market name
    # no longer matches. Fail loudly rather than publish a blank commodity.
    if blank:
        raise ScraperShapeError(
            f"COT: {len(blank)} tracked commodit(ies) have no rows after the "
            f"market-name split — {blank}. CFTC likely renamed the contract."
        )

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
