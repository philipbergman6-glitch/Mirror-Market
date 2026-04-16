"""
Layer 9 — DCE (Dalian Commodity Exchange) futures via AKShare.

AKShare is an open-source financial data library focused on Chinese
markets.  We use it to pull daily OHLCV + settlement + open interest
for soybean, soybean meal, soybean oil, and palm oil contracts traded
on the Dalian Commodity Exchange (DCE).

Key concepts for learning:
    - AKShare wraps Sina Finance's free futures API
    - ak.futures_zh_daily_sina(symbol="A0") returns a DataFrame with
      columns: date, open, high, low, close, volume, hold, settle
    - "hold" = open interest, "settle" = settlement price
    - Prices are in CNY (Chinese Yuan), NOT USD
    - No API key needed
"""

import logging
import time

import akshare as ak
import pandas as pd

from config import DCE_CONTRACTS, MAX_RETRIES, RETRY_DELAY

logger = logging.getLogger(__name__)


def fetch_one(symbol: str) -> pd.DataFrame:
    """
    Download daily futures data for a single DCE contract.

    Parameters
    ----------
    symbol : str
        AKShare symbol, e.g. "A0" for Soybean No.1 continuous.

    Returns
    -------
    pd.DataFrame
        Columns: date, open, high, low, close, volume, hold, settle
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            data = ak.futures_zh_daily_sina(symbol=symbol)

            if data is None or data.empty:
                logger.warning("No data returned for DCE symbol %s", symbol)
                return pd.DataFrame()

            return data

        except Exception as exc:
            logger.warning(
                "Attempt %d/%d failed for DCE %s: %s",
                attempt, MAX_RETRIES, symbol, exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    logger.error(
        "All %d attempts failed for DCE %s — returning empty DataFrame",
        MAX_RETRIES, symbol,
    )
    return pd.DataFrame()


def fetch_dce_futures() -> dict[str, pd.DataFrame]:
    """
    Download daily data for every contract in config.DCE_CONTRACTS.

    Returns
    -------
    dict
        {contract_name: DataFrame} — e.g. {"DCE Soybean": DataFrame}
    """
    results = {}

    for name, symbol in DCE_CONTRACTS.items():
        logger.info("Fetching %s (DCE %s) ...", name, symbol)
        df = fetch_one(symbol)
        results[name] = df
        if not df.empty:
            logger.info(
                "  Got %d rows, date range: %s → %s",
                len(df), df["date"].min(), df["date"].max(),
            )

    return results


# ── Quick self-test ─────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_dce_futures()
    logger.info("=== DCE Summary ===")
    for name, df in data.items():
        if df.empty:
            logger.info("%s: NO DATA", name)
        else:
            latest = df.iloc[-1]
            logger.info(
                "%s: last close = %.2f, rows = %d",
                name, float(latest["close"]), len(df),
            )
