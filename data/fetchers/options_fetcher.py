"""
Layer 16 — Options sentiment via yfinance (EXPERIMENTAL).

Put/call ratios, total open interest, and implied volatility for the
soy complex. Options flow reveals what big money expects to happen.

This layer is experimental because yfinance's option_chain() may or
may not work for agricultural futures. The entire layer is wrapped in
extra-defensive error handling — if it fails, the pipeline continues.

No API key required — uses yfinance.

Key concepts for learning:
    - Options: contracts that give the right (not obligation) to buy (call)
      or sell (put) at a set price.
    - Put/call ratio > 1 = more puts than calls = bearish sentiment.
    - Implied volatility (IV) = the market's forecast of future price swings.
    - Open interest (OI) = total number of outstanding contracts.
"""

import logging
from datetime import datetime

import pandas as pd

from config import OPTIONS_COMMODITIES

logger = logging.getLogger(__name__)


def fetch_options_sentiment() -> dict[str, pd.DataFrame]:
    """
    Fetch options sentiment data for soy complex commodities.

    For each commodity, tries to get the option chain from yfinance,
    aggregates put/call open interest and average implied volatility
    across all strikes for the nearest expiration.

    Returns dict keyed by commodity name. Each value is a single-row
    DataFrame with: Date, total_call_oi, total_put_oi, put_call_ratio,
    avg_call_iv, avg_put_iv.

    Returns empty dict if yfinance doesn't support options for these tickers.
    """
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance not installed — skipping options fetch.")
        return {}

    results = {}
    today = datetime.utcnow().strftime("%Y-%m-%d")

    for name, ticker in OPTIONS_COMMODITIES.items():
        try:
            logger.info("Fetching options for %s (%s) ...", name, ticker)
            t = yf.Ticker(ticker)

            # Get available expiration dates
            expirations = t.options
            if not expirations:
                logger.info("No options expirations available for %s.", name)
                continue

            # Use the nearest expiration
            nearest_exp = expirations[0]
            chain = t.option_chain(nearest_exp)

            calls = chain.calls
            puts = chain.puts

            if calls.empty and puts.empty:
                logger.info("Empty option chain for %s.", name)
                continue

            # Aggregate open interest and IV
            total_call_oi = calls["openInterest"].sum() if "openInterest" in calls.columns else 0
            total_put_oi = puts["openInterest"].sum() if "openInterest" in puts.columns else 0

            put_call_ratio = (total_put_oi / total_call_oi) if total_call_oi > 0 else None

            avg_call_iv = (
                calls["impliedVolatility"].mean()
                if "impliedVolatility" in calls.columns and not calls["impliedVolatility"].isna().all()
                else None
            )
            avg_put_iv = (
                puts["impliedVolatility"].mean()
                if "impliedVolatility" in puts.columns and not puts["impliedVolatility"].isna().all()
                else None
            )

            row = {
                "Date": today,
                "total_call_oi": total_call_oi,
                "total_put_oi": total_put_oi,
                "put_call_ratio": put_call_ratio,
                "avg_call_iv": avg_call_iv,
                "avg_put_iv": avg_put_iv,
            }

            results[name] = pd.DataFrame([row])
            logger.info(
                "Options for %s: P/C ratio=%.2f, Call OI=%d, Put OI=%d",
                name,
                put_call_ratio if put_call_ratio else 0,
                total_call_oi,
                total_put_oi,
            )

        except Exception as exc:
            # Extra-defensive: any error at all, just skip this commodity
            logger.info("Options not available for %s: %s", name, exc)
            continue

    return results


# ── Quick self-test ─────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_options_sentiment()
    logger.info("=== Options Sentiment Summary ===")
    if not data:
        logger.info("  No options data available (expected for ag futures)")
    for name, df in data.items():
        logger.info("  %s: %s", name, df.to_dict("records"))
