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
import threading
import time
from collections.abc import Callable
from datetime import date

import akshare as ak
import pandas as pd
import requests

from config import (
    AKSHARE_CALL_TIMEOUT,
    DCE_CONTRACTS,
    DCE_CRUSH_MONTH_ROOTS,
    DCE_CRUSH_MONTHS,
    DCE_CRUSH_MONTHS_AHEAD,
    DCE_FETCH_BUDGET_S,
    DCE_NAMED_MONTH_KEYS,
    MAX_RETRIES,
)
from fetchers._backoff import retry_sleep

logger = logging.getLogger(__name__)


class _CallTimedOut(TimeoutError):
    """An AKShare call gave no answer within its deadline."""


def _call_with_deadline(fn: Callable[[], pd.DataFrame], seconds: float) -> pd.DataFrame:
    """Run ``fn`` on a daemon thread; raise ``_CallTimedOut`` past ``seconds``.

    AKShare offers no timeout parameter, so the deadline is enforced from
    outside. A timed-out thread is abandoned, not killed: it is a daemon, so
    it can never hold the process open, and its late answer is discarded.
    """
    outcome: dict[str, object] = {}

    def run() -> None:
        try:
            outcome["value"] = fn()
        except BaseException as exc:  # re-raised on the caller's thread below
            outcome["error"] = exc

    worker = threading.Thread(target=run, name="akshare-call", daemon=True)
    worker.start()
    worker.join(seconds)
    if worker.is_alive():
        raise _CallTimedOut(f"no answer within {seconds:g}s")
    if "error" in outcome:
        raise outcome["error"]  # type: ignore[misc]
    return outcome["value"]  # type: ignore[return-value]


def fetch_one(symbol: str, *, deadline: float | None = None) -> pd.DataFrame:
    """
    Download daily futures data for a single DCE contract.

    Parameters
    ----------
    symbol : str
        AKShare symbol, e.g. "B0" for Soybean No.2 continuous (the
        imported/GMO bean Chinese crushers actually crush).
    deadline : float, optional
        A ``time.monotonic()`` instant past which no attempt starts and no
        attempt runs: the enclosing fetch's budget (#689).

    Returns
    -------
    pd.DataFrame
        Columns: date, open, high, low, close, volume, hold, settle
    """
    for attempt in range(1, MAX_RETRIES + 1):
        allowed: float = AKSHARE_CALL_TIMEOUT
        if deadline is not None:
            allowed = min(allowed, deadline - time.monotonic())
            if allowed <= 0:
                logger.error(
                    "DCE %s not attempted — the fetch's %ss budget is spent "
                    "(attempt %d/%d); returning empty DataFrame",
                    symbol, DCE_FETCH_BUDGET_S, attempt, MAX_RETRIES,
                )
                return pd.DataFrame()
        try:
            data = _call_with_deadline(
                lambda: ak.futures_zh_daily_sina(symbol=symbol), allowed
            )

            if data is None or data.empty:
                logger.warning("No data returned for DCE symbol %s", symbol)
                return pd.DataFrame()

            return data

        except (
            _CallTimedOut, requests.RequestException, ValueError, KeyError, AttributeError,
        ) as exc:
            # AKShare wraps the upstream Sina API; observed failure modes are
            # transport errors, schema drift (KeyError), intermittent
            # AttributeError when the response payload is malformed, and a
            # request that never answers (#689).
            logger.warning(
                "Attempt %d/%d failed for DCE %s: %s",
                attempt, MAX_RETRIES, symbol, exc,
            )
            if attempt < MAX_RETRIES:
                retry_sleep(attempt)

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
        {contract_name: DataFrame} — e.g. {"DCE Soybean No.2": DataFrame}
    """
    results = {}
    deadline = time.monotonic() + DCE_FETCH_BUDGET_S

    for name, symbol in DCE_CONTRACTS.items():
        logger.info("Fetching %s (DCE %s) ...", name, symbol)
        df = fetch_one(symbol, deadline=deadline)
        results[name] = df
        if not df.empty:
            logger.info(
                "  Got %d rows, date range: %s → %s",
                len(df), df["date"].min(), df["date"].max(),
            )

    return results


def crush_delivery_months(today: date, count: int = DCE_CRUSH_MONTHS_AHEAD) -> list[str]:
    """The next ``count`` crush months (YYYY-MM) whose delivery has not begun.

    Strictly after today's month: the crush never strikes a month in delivery
    (analysis.futures.crush.strike_month_crush), so fetching one buys nothing.
    """
    out: list[str] = []
    year, month = today.year, today.month
    while len(out) < count:
        month += 1
        if month > 12:
            year, month = year + 1, 1
        if month in DCE_CRUSH_MONTHS:
            out.append(f"{year:04d}-{month:02d}")
    return out


def fetch_dce_crush_months(today: date | None = None) -> dict[str, pd.DataFrame]:
    """Named delivery months for the three Dalian crush legs (B29 #503).

    One frame per product, keyed by ``config.DCE_NAMED_MONTH_KEYS``, every
    row carrying its ``contract`` ("B2611") and ``delivery_month``
    ("2026-11"). A contract Sina does not answer is logged and left out — the
    crush then falls to a month that is present, or is withheld naming the
    gap; it is never filled from the continuous series.
    """
    today = today or date.today()
    months = crush_delivery_months(today)
    names = {name: key for key, name in DCE_NAMED_MONTH_KEYS.items()}
    results: dict[str, pd.DataFrame] = {}
    deadline = time.monotonic() + DCE_FETCH_BUDGET_S
    for name, (root, _) in DCE_CRUSH_MONTH_ROOTS.items():
        frames = []
        for delivery_month in months:
            symbol = f"{root}{delivery_month[2:4]}{delivery_month[5:7]}"
            logger.info("Fetching %s (DCE %s) ...", name, symbol)
            df = fetch_one(symbol, deadline=deadline)
            if df.empty:
                logger.warning("DCE %s returned no bars — %s %s is absent", symbol, name, delivery_month)
                continue
            frames.append(df.assign(contract=symbol, delivery_month=delivery_month))
        results[names[name]] = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
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
