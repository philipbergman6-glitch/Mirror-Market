"""
Layer 16 — NCDEX India domestic soy prices.

NCDEX (National Commodity and Derivatives Exchange) publishes daily
settlement prices (the "Bhav Copy") for agricultural contracts traded
in India.  We track Soybean, Soybean Oil, and Soybean Meal.

Why it matters:
    India is the world's #4 soybean consumer.  When Indian domestic meal
    prices are cheap relative to CBOT, Middle East / African buyers switch
    suppliers, reducing demand for US exports.

Prices are stored in INR/MT (USD conversion happens at the analysis layer
using the INR/USD rate from the currencies table).

Parser strategy:
    The Bhav Copy is a CSV. We validate that it contains both a SYMBOL-like
    column and a CLOSE-like column; if either is missing the CSV schema
    has changed and we raise ScraperShapeError rather than silently
    returning empty. Filtering down to our tracked symbols may still
    yield an empty dict — that's a normal "no matching contract today"
    outcome, not a shape error.
"""

from __future__ import annotations

import io
import logging
from collections.abc import Sequence
from datetime import date, timedelta

import pandas as pd
import requests

from config import (
    MAX_RETRIES,
    NCDEX_BHAVCOPY_URL_TEMPLATES,
    NCDEX_SOY_SYMBOLS,
    NCDEX_UNIT_MULTIPLIER,
    REQUEST_TIMEOUT,
)
from fetchers._backoff import retry_sleep
from pipeline.results import FetchResult, ScraperShapeError

logger = logging.getLogger(__name__)

# How many calendar days back to look when today's file isn't available yet
_LOOKBACK_DAYS = 5

_SYMBOL_ALIASES: tuple[str, ...] = (
    "symbol", "instrument", "tradingsymbol", "instrument symbol",
)
_CLOSE_ALIASES: tuple[str, ...] = (
    "close", "closeprice", "close price", "settle", "settleprice",
)
_OPEN_ALIASES: tuple[str, ...] = ("open", "openprice", "open price", "prevclose")
_HIGH_ALIASES: tuple[str, ...] = ("high", "highprice", "high price")
_LOW_ALIASES: tuple[str, ...] = ("low", "lowprice", "low price")
_VOLUME_ALIASES: tuple[str, ...] = ("volume", "ttlqty", "totalvolume", "tottrdqty")


def _date_str(d: date) -> str:
    """Format a date as YYYYMMDD — the pattern NCDEX uses in filenames."""
    return d.strftime("%Y%m%d")


def _try_fetch_bhavcopy(target_date: date) -> pd.DataFrame | None:
    """Try downloading the NCDEX Bhav Copy CSV for a given date.

    Returns a raw DataFrame on first success, or None when every URL
    template fails. Multiple templates exist because NCDEX has changed
    its URL format and may do so again.
    """
    date_str = _date_str(target_date)

    for template in NCDEX_BHAVCOPY_URL_TEMPLATES:
        url = template.format(date=date_str)
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            logger.warning("NCDEX request failed for %s: %s", url, exc)
            continue

        content_type = resp.headers.get("Content-Type", "?")
        size = len(resp.content)

        if resp.status_code != 200:
            logger.warning(
                "NCDEX %s → HTTP %d (Content-Type=%s, %d bytes)",
                url, resp.status_code, content_type, size,
            )
            continue

        if "csv" not in content_type.lower() and "text/plain" not in content_type.lower():
            # Live evidence (2026-05): ncdex.com serves a JS fingerprint
            # interstitial on every URL with Content-Type=text/html. Surface
            # that so logs explain why the layer is failing.
            logger.warning(
                "NCDEX %s → HTTP 200 but Content-Type=%s (%d bytes) — "
                "likely anti-bot interstitial, not a CSV",
                url, content_type, size,
            )
            continue

        if size <= 200:
            logger.warning(
                "NCDEX %s → HTTP 200 but only %d bytes — empty or stub",
                url, size,
            )
            continue

        try:
            df = pd.read_csv(io.StringIO(resp.text), on_bad_lines="skip")
        except (ValueError, pd.errors.ParserError) as parse_err:
            logger.warning("NCDEX CSV parse failed for %s: %s", url, parse_err)
            continue

        if not df.empty:
            logger.info("NCDEX Bhav Copy fetched for %s via %s", target_date, url)
            return df

    return None


def _first_match(columns: Sequence[str], candidates: Sequence[str]) -> str | None:
    """Return the first column name that matches any candidate alias."""
    for col in columns:
        if col in candidates:
            return col
    return None


def _safe_float(val: object) -> float | None:
    """Convert a value to float, returning None on failure or NaN."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return None


def _extract_soy_prices(raw_df: pd.DataFrame, fetch_date: date) -> dict[str, pd.DataFrame]:
    """Extract soybean, soy oil, and soy meal rows from a raw NCDEX Bhav Copy.

    Returns ``{commodity_name: DataFrame}`` (single-row DataFrames with
    Date/Open/High/Low/Close/Volume/Unit). Raises ScraperShapeError if
    the CSV is missing the columns we need to identify symbols or extract
    a close price.

    Returns an empty dict when no row matches the configured symbol
    aliases — that's a "no matching contract today" outcome, not a
    structural change.
    """
    if raw_df.empty:
        raise ScraperShapeError("NCDEX Bhav Copy: empty DataFrame (no rows or columns)")

    raw_df = raw_df.copy()
    raw_df.columns = [str(c).strip().lower() for c in raw_df.columns]

    symbol_col = _first_match(raw_df.columns, _SYMBOL_ALIASES)
    if symbol_col is None:
        raise ScraperShapeError(
            f"NCDEX Bhav Copy: no symbol column (looked for {_SYMBOL_ALIASES}) — "
            f"got {list(raw_df.columns)}"
        )

    close_col = _first_match(raw_df.columns, _CLOSE_ALIASES)
    if close_col is None:
        raise ScraperShapeError(
            f"NCDEX Bhav Copy: no close column (looked for {_CLOSE_ALIASES}) — "
            f"got {list(raw_df.columns)}"
        )

    open_col = _first_match(raw_df.columns, _OPEN_ALIASES)
    high_col = _first_match(raw_df.columns, _HIGH_ALIASES)
    low_col = _first_match(raw_df.columns, _LOW_ALIASES)
    volume_col = _first_match(raw_df.columns, _VOLUME_ALIASES)

    results: dict[str, pd.DataFrame] = {}

    for commodity_name, symbol_aliases in NCDEX_SOY_SYMBOLS.items():
        wanted = {alias.upper() for alias in symbol_aliases}
        mask = raw_df[symbol_col].astype(str).str.upper().isin(wanted)
        subset = raw_df[mask]

        if subset.empty:
            logger.debug("NCDEX: No rows for %s (tried: %s)", commodity_name, symbol_aliases)
            continue

        row = subset.iloc[0]
        multiplier = NCDEX_UNIT_MULTIPLIER.get(commodity_name, 1.0)

        def scaled(col_name: str | None, row: pd.Series = row, multiplier: float = multiplier) -> float | None:
            if col_name is None:
                return None
            v = _safe_float(row.get(col_name))
            return v * multiplier if v is not None else None

        close = scaled(close_col)
        if close is None:
            logger.debug("NCDEX: No Close price for %s", commodity_name)
            continue

        record = {
            "Date": str(fetch_date),
            "Open": scaled(open_col),
            "High": scaled(high_col),
            "Low": scaled(low_col),
            "Close": close,
            "Volume": _safe_float(row.get(volume_col)) if volume_col else None,
            "Unit": "INR/MT",
        }

        results[commodity_name] = pd.DataFrame([record])
        logger.info(
            "NCDEX %s: Close = %.2f INR/MT (%s)",
            commodity_name, close, fetch_date,
        )

    return results


def fetch_india_domestic() -> FetchResult:
    """Fetch NCDEX India domestic soy prices for the most recent trading day.

    Walks back up to ``_LOOKBACK_DAYS`` calendar days, skipping weekends.
    Returns ``FetchResult.ok`` with ``{commodity_name: DataFrame}``
    (Date/Open/High/Low/Close/Volume/Unit). NCDEX publishes every trading
    day, so exhausting the lookback window or a schema change is
    ``FetchResult.failed``.
    """
    today = date.today()

    for days_back in range(_LOOKBACK_DAYS):
        target_date = today - timedelta(days=days_back)

        # Skip weekends — NCDEX doesn't publish on Saturday (5) or Sunday (6)
        if target_date.weekday() >= 5:
            continue

        for attempt in range(1, MAX_RETRIES + 1):
            raw_df = _try_fetch_bhavcopy(target_date)

            if raw_df is not None:
                try:
                    results = _extract_soy_prices(raw_df, target_date)
                except ScraperShapeError as exc:
                    logger.error("NCDEX: Bhav Copy schema changed — %s", exc)
                    return FetchResult.failed(str(exc))
                if results:
                    return FetchResult.ok(results)
                logger.info(
                    "NCDEX: Bhav Copy for %s fetched but soy symbols not found — "
                    "may need to update NCDEX_SOY_SYMBOLS in config.py",
                    target_date,
                )
                break

            if attempt < MAX_RETRIES:
                retry_sleep(attempt)

    logger.warning(
        "NCDEX: Could not fetch Bhav Copy for last %d trading days. "
        "Check NCDEX_BHAVCOPY_URL_TEMPLATES in config.py.",
        _LOOKBACK_DAYS,
    )
    return FetchResult.failed(
        f"NCDEX: no Bhav Copy reachable in the last {_LOOKBACK_DAYS} days"
    )


__all__: Sequence[str] = ("_extract_soy_prices", "fetch_india_domestic")


# ── Quick self-test ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    result = fetch_india_domestic()
    if not result.has_rows:
        logger.info("NCDEX: %s — %s", result.status, result.error)
    else:
        for name, df in result.data.items():
            logger.info(
                "%s: %d rows, latest Close = %.2f INR/MT",
                name, len(df), df["Close"].iloc[-1],
            )
