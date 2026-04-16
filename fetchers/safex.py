"""
Layer 18 — SAFEX South Africa domestic soy prices via Grain SA.

Source: Grain SA SAFEX Feeds page (https://www.grainsa.co.za/pages/industry-reports/safex-feeds)
Data provider: BVG (credited on the page)
Prices: ZAR/MT — daily settlement prices for JSE Agricultural futures contracts

Why it matters:
    South Africa is the regional soy hub for sub-Saharan Africa.  The SAFEX
    soybean price in ZAR/MT signals domestic crush margins and regional demand
    from neighboring countries (Zimbabwe, Zambia, Mozambique, etc.).
    The SAFEX-CBOT basis (after FX conversion) reveals whether SA is a
    premium or discount market — a key signal for regional trade flows.

Contracts we track:
    SOYB — Soybean (ZAR/MT, multiple contract months)
    SUNS — Sunflower Seed (ZAR/MT)

We take the nearest active contract (highest volume) as the spot price.
Prices are stored in ZAR/MT; USD conversion happens at the analysis layer.

Key concepts for learning:
    - pd.read_html() extracts HTML tables in one line — very powerful for
      pages that display data in <table> tags (no JavaScript needed)
    - Multi-level column headers need special handling (iloc[0] to flatten)
    - "Nearest contract" = the contract with the highest volume traded today
"""

import logging
import time
from datetime import date

import pandas as pd
import requests

from config import (
    SAFEX_STATS_URL,
    SAFEX_COMMODITIES,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    RETRY_DELAY,
)

logger = logging.getLogger(__name__)

# Map Grain SA instrument codes → our commodity names
_INSTRUMENT_MAP = {
    "SOYB": "Soybean (SAFEX)",
    "SUNS": "Sunflower (SAFEX)",
}


def _fetch_page() -> str:
    """Download the Grain SA SAFEX feeds page HTML."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                SAFEX_STATS_URL,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.text
            logger.warning("Grain SA SAFEX: HTTP %d (attempt %d)", resp.status_code, attempt)
        except requests.RequestException as exc:
            logger.warning("Grain SA SAFEX: Request failed (attempt %d): %s", attempt, exc)

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)

    return ""


def _parse_safex_table(html: str) -> dict[str, pd.DataFrame]:
    """
    Parse the Grain SA SAFEX table into per-commodity DataFrames.

    The table has columns:
        Instrument | Contract | Last Traded Time | Last Traded Price |
        Difference | High Price | Low Price | Volume | Open Interest

    For each instrument (SOYB, SUNS) we pick the contract with the
    highest volume — that's the most actively traded (nearest) contract.

    Returns
    -------
    dict
        {commodity_name: DataFrame} with columns: Date, Close, Volume, Unit
    """
    import io

    try:
        tables = pd.read_html(io.StringIO(html))
    except Exception as exc:
        logger.warning("Grain SA SAFEX: pd.read_html() failed: %s", exc)
        return {}

    if not tables:
        logger.warning("Grain SA SAFEX: No tables found on page")
        return {}

    # The first (and only) table is the settlement prices table
    tbl = tables[0].copy()

    # Flatten multi-level column headers if present
    if isinstance(tbl.columns, pd.MultiIndex):
        tbl.columns = [" ".join(str(c) for c in col).strip() for col in tbl.columns]

    # Normalise column names to lowercase
    tbl.columns = [str(c).strip().lower() for c in tbl.columns]

    # Find the key columns we need
    instrument_col = next((c for c in tbl.columns if "instrument" in c), None)
    price_col      = next((c for c in tbl.columns if "last traded price" in c or "price" in c), None)
    volume_col     = next((c for c in tbl.columns if "volume" in c), None)
    date_col       = next((c for c in tbl.columns if "time" in c or "date" in c), None)

    if instrument_col is None or price_col is None:
        logger.warning(
            "Grain SA SAFEX: Could not identify columns. Found: %s", list(tbl.columns)
        )
        return {}

    logger.info("Grain SA SAFEX: Parsing table with %d rows", len(tbl))

    results = {}
    today_str = str(date.today())

    for instrument_code, commodity_name in _INSTRUMENT_MAP.items():
        # Filter to rows for this instrument
        mask = tbl[instrument_col].astype(str).str.upper() == instrument_code
        subset = tbl[mask].copy()

        if subset.empty:
            logger.debug("Grain SA SAFEX: No rows for %s (%s)", commodity_name, instrument_code)
            continue

        # Convert price and volume to numeric
        subset[price_col] = pd.to_numeric(subset[price_col], errors="coerce")
        if volume_col:
            subset[volume_col] = pd.to_numeric(subset[volume_col], errors="coerce")

        # Drop rows with no price
        subset = subset.dropna(subset=[price_col])
        subset = subset[subset[price_col] > 0]

        if subset.empty:
            continue

        # Pick the contract with the highest volume (most active = nearest contract)
        if volume_col and subset[volume_col].notna().any():
            best_row = subset.loc[subset[volume_col].idxmax()]
        else:
            best_row = subset.iloc[0]

        close  = float(best_row[price_col])
        volume = float(best_row[volume_col]) if volume_col and pd.notna(best_row.get(volume_col)) else None

        # Use date from table if available, otherwise today
        trade_date = today_str
        if date_col and pd.notna(best_row.get(date_col)):
            try:
                trade_date = str(pd.to_datetime(best_row[date_col]).date())
            except Exception:
                pass

        record = pd.DataFrame([{
            "Date":   trade_date,
            "Close":  close,
            "Volume": volume,
            "Unit":   "ZAR/MT",
        }])

        results[commodity_name] = record
        logger.info(
            "Grain SA SAFEX %s: Close = %.1f ZAR/MT (vol=%s, date=%s)",
            commodity_name, close, volume, trade_date,
        )

    return results


def fetch_safex() -> dict[str, pd.DataFrame]:
    """
    Fetch SAFEX South Africa soy prices from Grain SA.

    Returns
    -------
    dict
        {commodity_name: DataFrame}
        e.g. {"Soybean (SAFEX)": df, "Sunflower (SAFEX)": df}
        DataFrame columns: Date, Close, Volume, Unit (ZAR/MT)
        Returns {} if fetch/parse fails.
    """
    logger.info("Fetching SAFEX prices from Grain SA ...")
    html = _fetch_page()

    if not html:
        logger.warning(
            "Grain SA SAFEX: Could not download page. "
            "Returning empty — pipeline continues without SAFEX data."
        )
        return {}

    return _parse_safex_table(html)


# ── Quick self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_safex()
    if not data:
        logger.info("SAFEX: No data returned.")
    else:
        for name, df in data.items():
            logger.info(
                "%s: Close = %.1f ZAR/MT, Volume = %s, Date = %s",
                name,
                df["Close"].iloc[0],
                df["Volume"].iloc[0],
                df["Date"].iloc[0],
            )
