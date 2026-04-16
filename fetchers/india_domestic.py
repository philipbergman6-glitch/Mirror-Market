"""
Layer 16 — NCDEX India domestic soy prices.

NCDEX (National Commodity and Derivatives Exchange) publishes daily
settlement prices (the "Bhav Copy") for agricultural contracts traded in
India.  Soybean, Soybean Oil, and Soybean Meal are the three we care about.

Why it matters:
    India is the world's #4 soybean consumer.  When Indian domestic meal
    prices are cheap relative to CBOT, buyers in the Middle East and Africa
    switch suppliers — reducing demand for US exports.

Prices are in INR/MT (we store them native; USD conversion happens in the
analysis layer using the INR/USD rate from the currencies table).

Source: NCDEX Bhav Copy CSV — no API key required.

Key concepts for learning:
    - Trying multiple URL patterns when the exact URL isn't known (defensive)
    - Iterating over several recent trading days (NCDEX skips weekends/holidays)
    - pd.to_numeric(..., errors="coerce") safely converts strings → numbers
"""

import logging
import time
from datetime import date, timedelta

import pandas as pd
import requests

from config import (
    NCDEX_BHAVCOPY_URL_TEMPLATES,
    NCDEX_SOY_SYMBOLS,
    NCDEX_UNIT_MULTIPLIER,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    RETRY_DELAY,
)

logger = logging.getLogger(__name__)

# How many calendar days back to look when today's file isn't available yet
_LOOKBACK_DAYS = 5


def _date_str(d: date) -> str:
    """Format a date as YYYYMMDD — the pattern NCDEX uses in filenames."""
    return d.strftime("%Y%m%d")


def _try_fetch_bhavcopy(target_date: date) -> pd.DataFrame | None:
    """
    Try downloading the NCDEX Bhav Copy CSV for a given date.

    Returns a raw DataFrame if successful, or None if all URL patterns fail.

    We try multiple URL templates because NCDEX has changed its URL format
    in the past and may do so again.  Failing gracefully is better than
    crashing the whole pipeline.
    """
    date_str = _date_str(target_date)

    for template in NCDEX_BHAVCOPY_URL_TEMPLATES:
        url = template.format(date=date_str)
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200 and len(resp.content) > 200:
                # Try parsing as CSV
                try:
                    df = pd.read_csv(
                        pd.io.common.StringIO(resp.text),
                        on_bad_lines="skip",
                    )
                    if not df.empty:
                        logger.info("NCDEX Bhav Copy fetched for %s via %s", target_date, url)
                        return df
                except Exception as parse_err:
                    logger.debug("CSV parse failed for %s: %s", url, parse_err)
        except requests.RequestException as exc:
            logger.debug("Request failed for %s: %s", url, exc)

    return None


def _extract_soy_prices(raw_df: pd.DataFrame, fetch_date: date) -> dict[str, pd.DataFrame]:
    """
    Extract soybean, soy oil, and soy meal rows from a raw NCDEX Bhav Copy.

    The Bhav Copy CSV has a SYMBOL column (or similar).  We search for each
    commodity's known symbol aliases.

    Returns
    -------
    dict
        {commodity_name: DataFrame} with columns: Date, Open, High, Low, Close, Volume, Unit
    """
    # Normalise column names to lowercase for easier matching
    raw_df.columns = [str(c).strip().lower() for c in raw_df.columns]

    # Find the symbol column — NCDEX uses various names
    symbol_col = None
    for candidate in ("symbol", "instrument", "tradingsymbol", "instrument symbol"):
        if candidate in raw_df.columns:
            symbol_col = candidate
            break

    if symbol_col is None:
        logger.warning("NCDEX Bhav Copy: no symbol column found — columns: %s", list(raw_df.columns))
        return {}

    results = {}

    for commodity_name, symbol_aliases in NCDEX_SOY_SYMBOLS.items():
        # Filter rows where the symbol matches any of our aliases (case-insensitive)
        mask = raw_df[symbol_col].str.upper().isin(
            [alias.upper() for alias in symbol_aliases]
        )
        subset = raw_df[mask].copy()

        if subset.empty:
            logger.debug("NCDEX: No rows for %s (tried: %s)", commodity_name, symbol_aliases)
            continue

        # Map common column name variants to our standard names
        col_map = {}
        for col in subset.columns:
            if col in ("open", "openprice", "open price", "prevclose"):
                col_map[col] = "open"
            elif col in ("high", "highprice", "high price"):
                col_map[col] = "high"
            elif col in ("low", "lowprice", "low price"):
                col_map[col] = "low"
            elif col in ("close", "closeprice", "close price", "settle", "settlprice"):
                col_map[col] = "close"
            elif col in ("volume", "ttlqty", "totalvolume", "tottrdqty"):
                col_map[col] = "volume"

        subset = subset.rename(columns=col_map)

        # Build a clean output row
        multiplier = NCDEX_UNIT_MULTIPLIER.get(commodity_name, 1.0)

        # Take the first matching row (different contract months — use nearest)
        row = subset.iloc[0]

        record = {
            "Date": str(fetch_date),
            "Open":   _safe_float(row.get("open")) * multiplier if _safe_float(row.get("open")) else None,
            "High":   _safe_float(row.get("high")) * multiplier if _safe_float(row.get("high")) else None,
            "Low":    _safe_float(row.get("low")) * multiplier if _safe_float(row.get("low")) else None,
            "Close":  _safe_float(row.get("close")) * multiplier if _safe_float(row.get("close")) else None,
            "Volume": _safe_float(row.get("volume")),
            "Unit":   "INR/MT",
        }

        if record["Close"] is None:
            logger.debug("NCDEX: No Close price for %s", commodity_name)
            continue

        results[commodity_name] = pd.DataFrame([record])
        logger.info(
            "NCDEX %s: Close = %.2f INR/MT (%s)",
            commodity_name, record["Close"], fetch_date,
        )

    return results


def _safe_float(val) -> float | None:
    """Convert a value to float, returning None on failure."""
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return None


def fetch_india_domestic() -> dict[str, pd.DataFrame]:
    """
    Fetch NCDEX India domestic soy prices for the most recent available trading day.

    Tries today first, then walks back up to _LOOKBACK_DAYS if today's file
    isn't published yet (e.g. on weekends or before market close).

    Returns
    -------
    dict
        {commodity_name: DataFrame}
        e.g. {"Soybean (NCDEX)": df, "Soybean Oil (NCDEX)": df, ...}
        Each DataFrame has columns: Date, Open, High, Low, Close, Volume, Unit
        Returns {} if all attempts fail.
    """
    today = date.today()

    for days_back in range(_LOOKBACK_DAYS):
        target_date = today - timedelta(days=days_back)

        # Skip weekends — NCDEX doesn't publish on Saturday (6) or Sunday (0)
        if target_date.weekday() >= 5:
            continue

        for attempt in range(1, MAX_RETRIES + 1):
            raw_df = _try_fetch_bhavcopy(target_date)

            if raw_df is not None:
                results = _extract_soy_prices(raw_df, target_date)
                if results:
                    return results
                # File fetched but our symbols not found — try next day
                logger.info(
                    "NCDEX: Bhav Copy for %s fetched but soy symbols not found — "
                    "may need to update NCDEX_SOY_SYMBOLS in config.py",
                    target_date,
                )
                break

            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    logger.warning(
        "NCDEX: Could not fetch Bhav Copy for last %d trading days. "
        "Check NCDEX_BHAVCOPY_URL_TEMPLATES in config.py. "
        "Returning empty — pipeline continues without NCDEX data.",
        _LOOKBACK_DAYS,
    )
    return {}


# ── Quick self-test ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_india_domestic()
    if not data:
        logger.info("NCDEX: No data returned. URL may need verification — see README.")
    else:
        for name, df in data.items():
            logger.info(
                "%s: %d rows, latest Close = %.2f INR/MT",
                name, len(df), df["Close"].iloc[-1],
            )
