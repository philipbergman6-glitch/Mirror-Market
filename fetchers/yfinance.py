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
from fetchers._settlement import (
    EXCHANGE_SESSION,
    FX_SESSION,
    SessionRule,
    drop_unsettled_session,
)

logger = logging.getLogger(__name__)


def download_bars(ticker: str, period: str = DEFAULT_HISTORY_PERIOD) -> pd.DataFrame:
    """
    Download historical OHLCV data for a single ticker, with no session policy.

    The provider frame as it arrives: retried, flattened and stripped of empty
    rows, but *including* the session in progress. Nothing in the pipeline
    calls this directly — ``fetch_one`` is the choke point that applies the
    settlement guard, and every layer goes through it.

    It is separated out for the trusted path (``trust/fx_ingestion.py``,
    ``trust/cbot_benchmarks.py``), which must see the unfinished bar in order
    to apply — and record — its own policy on it. An adapter handed a frame
    v1's guard has already trimmed can never fire its own rule, and would pass
    while proving nothing.

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


def fetch_one(
    ticker: str,
    period: str = DEFAULT_HISTORY_PERIOD,
    rule: SessionRule = EXCHANGE_SESSION,
) -> pd.DataFrame:
    """
    Download historical OHLCV data for a single ticker, guarded.

    The single choke point every v1 yfinance frame flows through (Layer 1
    prices, Layer 7 currencies, Layer 11 forward curve contracts). Yahoo
    returns a row for the session in progress; before the venue closes that
    row is an unfinished bar, and storing it publishes a partial print as the
    day's close — see ``fetchers/_settlement.py``.

    Parameters
    ----------
    ticker : str
        Yahoo Finance ticker symbol, e.g. "ZS=F"
    period : str
        How far back to look — "1y", "2y", "5y", "max", etc.
    rule : SessionRule
        Which venue clock decides whether the newest bar has finished.
        Defaults to exchange settlement; FX pairs must pass
        ``fetchers._settlement.FX_SESSION`` — spot FX has no settlement and
        judging it by the Chicago cutoff stores an open bar as a close.

    Returns
    -------
    pd.DataFrame
        Columns: Open, High, Low, Close, Volume
        Index: Date
    """
    return drop_unsettled_session(download_bars(ticker, period=period), label=ticker, rule=rule)


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

    The one thing it does NOT share with commodity prices is the session
    rule. Spot FX has no settlement; its bar closes at 17:00 New York.
    Judged by the exchange cutoff instead, an FX bar opened at 17:00 was
    stored as that day's close from 14:30 Chicago onwards — verified live
    on 2026-08-19 (see fetchers/_settlement.py). Every ``home_per_mt`` leg
    on the site converts at that row's own date, so this is a landed-cost
    error on every physical origin, not just a wrong FX cell.

    Returns
    -------
    dict
        {pair_name: DataFrame} — e.g. {"BRL/USD": DataFrame}
    """
    results = {}

    for name, ticker in CURRENCY_TICKERS.items():
        logger.info("Fetching %s (%s) ...", name, ticker)
        df = fetch_one(ticker, period=period, rule=FX_SESSION)
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
