"""
Layer 1 — Commodity futures prices via yfinance.

yfinance pulls delayed data that originates from CME/ICE/CBOT,
so the prices are the real exchange prices, just not real-time.

Key concepts for learning:
    - yfinance returns a pandas DataFrame with columns:
      Open, High, Low, Close, Volume (OHLCV)
    - The index of the DataFrame is a DatetimeIndex (the trading dates)
    - try/except lets us handle errors gracefully instead of crashing
    - Retry logic handles transient network glitches automatically
"""

import logging

import pandas as pd
import requests
import yfinance as yf

from config import (
    COMMODITY_TICKERS,
    CURRENCY_TICKERS,
    DEFAULT_HISTORY_PERIOD,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
)
from fetchers._backoff import retry_sleep
from fetchers._settlement import drop_unsettled_session

logger = logging.getLogger(__name__)


def fetch_one(ticker: str, period: str = DEFAULT_HISTORY_PERIOD) -> pd.DataFrame:
    """
    Download historical OHLCV data for a single ticker.

    Parameters
    ----------
    ticker : str
        Yahoo Finance ticker symbol, e.g. "ZS=F"
    period : str
        How far back to look — "1y", "2y", "5y", "max", etc.

    Returns
    -------
    pd.DataFrame
        Columns: Open, High, Low, Close, Volume
        Index: Date
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            data = yf.download(ticker, period=period, progress=False, timeout=REQUEST_TIMEOUT)

            if data.empty:
                # An empty frame is usually Yahoo throttling, not a real
                # "no such ticker" — burn a retry instead of giving up.
                logger.warning(
                    "No data returned for %s (attempt %d/%d)", ticker, attempt, MAX_RETRIES
                )
                if attempt < MAX_RETRIES:
                    retry_sleep(attempt)
                    continue
                return data

            # yfinance sometimes returns multi-level columns when downloading
            # a single ticker — flatten them if that happens
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)

            # Drop any completely empty rows (holidays / missing days)
            data = data.dropna(how="all")

            # Yahoo returns a row for the session in progress. Before the
            # venue settles that row is an unfinished bar, and storing it
            # publishes a partial print as the day's close — see
            # fetchers/_settlement.py.
            data = drop_unsettled_session(data, label=ticker)

            return data

        except (requests.RequestException, ValueError, KeyError, AttributeError) as exc:
            # yfinance can raise transport errors, KeyError on schema drift,
            # and AttributeError when the upstream API returns an unexpected
            # response shape. Caught explicitly so unrelated bugs surface.
            logger.warning(
                "Attempt %d/%d failed for %s: %s",
                attempt, MAX_RETRIES, ticker, exc,
            )
            if attempt < MAX_RETRIES:
                retry_sleep(attempt)

    logger.error("All %d attempts failed for %s — returning empty DataFrame", MAX_RETRIES, ticker)
    return pd.DataFrame()


def fetch_all(period: str = DEFAULT_HISTORY_PERIOD) -> dict[str, pd.DataFrame]:
    """
    Download data for every commodity in config.COMMODITY_TICKERS.

    Returns
    -------
    dict
        {commodity_name: DataFrame} — one entry per commodity.
    """
    results = {}

    for name, ticker in COMMODITY_TICKERS.items():
        logger.info("Fetching %s (%s) ...", name, ticker)
        df = fetch_one(ticker, period=period)
        results[name] = df
        if not df.empty:
            logger.info(
                "  Got %d rows, date range: %s → %s",
                len(df), df.index.min().date(), df.index.max().date(),
            )

    return results


def fetch_currencies(period: str = DEFAULT_HISTORY_PERIOD) -> dict[str, pd.DataFrame]:
    """
    Download currency pairs from config.CURRENCY_TICKERS.

    Reuses the existing fetch_one() function — same retry logic and error
    handling as commodity prices.  Currency pairs like BRL/USD tell us how
    export-competitive each country is (a weaker Real makes Brazil's
    soybeans cheaper in dollar terms).

    Returns
    -------
    dict
        {pair_name: DataFrame} — e.g. {"BRL/USD": DataFrame}
    """
    results = {}

    for name, ticker in CURRENCY_TICKERS.items():
        logger.info("Fetching %s (%s) ...", name, ticker)
        df = fetch_one(ticker, period=period)
        results[name] = df
        if not df.empty:
            logger.info(
                "  Got %d rows, date range: %s → %s",
                len(df), df.index.min().date(), df.index.max().date(),
            )

    return results


# ── Quick self-test ─────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_all()
    logger.info("=== Summary ===")
    for name, df in data.items():
        if df.empty:
            logger.info("%s: NO DATA", name)
        else:
            latest = df.iloc[-1]
            logger.info("%s: last close = %.2f, rows = %d", name, latest['Close'], len(df))
