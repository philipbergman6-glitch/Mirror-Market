"""
Layer 22 — AFEX Nigeria soybean reference price.

Source: AFEX Commodities Exchange market-data endpoint
        https://api-md.afexnigeria.com/AFEXMD/api/v1/securities/price
Prices: NGN/kg in the feed, stored as NGN/MT.

Why it matters:
    Nigeria is West Africa's largest soybean producer and, until now, had no
    price series in this repo at all — only weather, NGN/USD and PSD
    supply/demand.  AFEX is the only daily, machine-readable Nigerian soy
    price we could find and corroborate (see
    research/2026-08-10-nigeria-price-source.md; level agrees within ~10%
    with NCX Kaduna, NCX Nasarawa and LCFE on a matched date).

What this series is NOT:
    Not an exchange settlement.  Prices change on Saturdays and Sundays and
    74% of changes are day-over-day, so this is a daily *reference* price.
    Anything downstream must label it that way.  No volume or open-interest
    field is exposed, so liquidity cannot be assessed from the feed.

Fetch strategy:
    One unauthenticated GET returns the entire history (~800 KB, back to
    2019) in a single JSON array.  That means the layer self-heals on an
    ephemeral CI database and needs no data/history/ CSV round-trip — which
    also keeps AFEX's data out of the public repo while the licence question
    is open (see AFEX_PUBLISH_RAW in config.py).

    The endpoint requires an Origin header; without it the API returns 401.

Shape guarantees:
    The feed changed schema once already — rows before 2022-01-18 use bare
    'SBS' keys, rows after use the 'SSBS'/'DSBS'/'OSBS' triplet.  We read the
    configured key and hard-fail with ScraperShapeError if it disappears
    entirely, rather than silently storing nothing.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import pandas as pd
import requests

from config import (
    AFEX_MAX_FLAT_DAYS,
    AFEX_ORIGIN,
    AFEX_PRICE_URL,
    AFEX_SERIES,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
)
from fetchers._backoff import retry_sleep
from pipeline.results import FetchResult, ScraperShapeError

logger = logging.getLogger(__name__)

# The feed publishes NGN/kg under the 'S' key. 1 MT = 1000 kg.
_KG_PER_MT = 1000.0

# Sanity band for a Nigerian soybean price in NGN/MT. The observed 2021-2026
# range is roughly 200,000-1,000,000 NGN/MT; anything outside this band means
# the unit changed under us (e.g. the feed switching to NGN/kg in the D key)
# and must not be stored as if nothing happened.
_MIN_NGN_MT = 50_000.0
_MAX_NGN_MT = 5_000_000.0


def _fetch_payload() -> list[dict] | None:
    """Download the AFEX price history JSON.

    Returns the decoded list of daily rows, or None on transport failure.
    Uses exponential backoff with jitter so retries don't all hit the
    upstream at the same instant.
    """
    headers = {
        "Origin": AFEX_ORIGIN,
        "Accept": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(AFEX_PRICE_URL, headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                payload = resp.json()
                if isinstance(payload, dict):
                    # Tolerate an envelope: take the first list-valued field.
                    payload = next(
                        (v for v in payload.values() if isinstance(v, list)), None
                    )
                if isinstance(payload, list):
                    return payload
                logger.warning("AFEX: unexpected JSON shape (attempt %d)", attempt)
            elif resp.status_code == 401:
                # The Origin gate is the single most likely thing to change.
                logger.error(
                    "AFEX: HTTP 401 — the Origin gate rejected us. "
                    "Access may have been revoked; see AFEX_ORIGIN in config.py."
                )
            else:
                logger.warning("AFEX: HTTP %d (attempt %d)", resp.status_code, attempt)
        except (requests.RequestException, ValueError) as exc:
            logger.warning("AFEX: request failed (attempt %d): %s", attempt, exc)

        if attempt < MAX_RETRIES:
            retry_sleep(attempt)

    return None


def _trailing_flat_days(series: pd.Series) -> int:
    """Length of the constant run at the end of a price series.

    A long plateau in this feed is normal — the observed history contains
    24 runs of >= 7 days and a 26-day maximum — so this is reported, not
    treated as an error. It exists to distinguish "quiet" from "stalled".
    """
    if series.empty:
        return 0
    last = series.iloc[-1]
    run = 0
    for value in reversed(series.tolist()):
        if value != last:
            break
        run += 1
    return run


def _parse_series(payload: list[dict], key: str, name: str) -> pd.DataFrame:
    """Extract one commodity's daily NGN/MT series from the raw feed.

    Raises
    ------
    ScraperShapeError
        If ``key`` appears in no row at all (the feed's schema changed), or
        if every parsed price falls outside the plausible NGN/MT band (the
        unit changed under us).
    """
    rows: list[dict] = []
    key_seen = False

    for row in payload:
        if not isinstance(row, dict):
            continue
        date = row.get("date")
        if key not in row:
            continue
        key_seen = True
        raw = row.get(key)
        if date is None or raw is None:
            continue
        try:
            price_kg = float(raw)
        except (TypeError, ValueError):
            continue
        if price_kg <= 0:
            continue
        rows.append({
            "Date": date,
            "price_ngn_mt": price_kg * _KG_PER_MT,
            "Unit": "NGN/MT",
        })

    if not key_seen:
        raise ScraperShapeError(
            f"AFEX: key {key!r} ({name}) absent from every row — feed schema changed"
        )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
    # Dedupe defensively: the feed is one row per calendar day, but a repeat
    # would silently violate the (Date, commodity) primary key downstream.
    df = df.drop_duplicates(subset=["Date"], keep="last").reset_index(drop=True)

    in_band = df["price_ngn_mt"].between(_MIN_NGN_MT, _MAX_NGN_MT)
    if not in_band.any():
        raise ScraperShapeError(
            f"AFEX: no {name} price within {_MIN_NGN_MT:,.0f}-{_MAX_NGN_MT:,.0f} NGN/MT "
            f"(got {df['price_ngn_mt'].min():,.0f}-{df['price_ngn_mt'].max():,.0f}) — "
            "the feed's unit appears to have changed"
        )
    if not in_band.all():
        dropped = int((~in_band).sum())
        logger.warning("AFEX %s: dropped %d row(s) outside the NGN/MT sanity band", name, dropped)
        df = df[in_band].reset_index(drop=True)

    flat = _trailing_flat_days(df["price_ngn_mt"])
    if flat > AFEX_MAX_FLAT_DAYS:
        logger.error(
            "AFEX %s: price unchanged for %d days (last %s) — exceeds the %d-day "
            "historical maximum; the feed may be stalled rather than quiet.",
            name, flat, df["Date"].iloc[-1].date(), AFEX_MAX_FLAT_DAYS,
        )
    elif flat >= 7:
        logger.info(
            "AFEX %s: %d-day flat run (within the normal range, max observed %d)",
            name, flat, AFEX_MAX_FLAT_DAYS,
        )

    logger.info(
        "AFEX %s: %d rows %s → %s, last = %.0f NGN/MT",
        name, len(df), df["Date"].iloc[0].date(), df["Date"].iloc[-1].date(),
        df["price_ngn_mt"].iloc[-1],
    )
    return df


def fetch_afex() -> FetchResult:
    """Fetch AFEX Nigeria soybean reference prices.

    Returns
    -------
    FetchResult
        ``ok`` with ``{commodity_name: DataFrame}`` (Date, price_ngn_mt,
        Unit) carrying the full available history; ``failed`` when the
        endpoint can't be reached or its schema changed; ``empty`` when the
        feed parsed cleanly but carried no usable rows.
    """
    logger.info("Fetching AFEX Nigeria soybean prices ...")
    payload = _fetch_payload()

    if payload is None:
        return FetchResult.failed("AFEX: endpoint unreachable or non-JSON")
    if not payload:
        return FetchResult.empty("AFEX: endpoint returned an empty array")

    data: dict[str, pd.DataFrame] = {}
    try:
        for key, name in AFEX_SERIES.items():
            df = _parse_series(payload, key, name)
            if not df.empty:
                data[name] = df
    except ScraperShapeError as exc:
        logger.error("AFEX: feed structure changed — %s", exc)
        return FetchResult.failed(str(exc))

    if not data:
        return FetchResult.empty("AFEX: feed parsed but carried no usable rows")
    return FetchResult.ok(data)


# Re-export for tests
__all__: Sequence[str] = ("_parse_series", "_trailing_flat_days", "fetch_afex")


# ── Quick self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    result = fetch_afex()
    if not result.has_rows:
        logger.info("AFEX: %s — %s", result.status, result.error)
    else:
        for name, df in result.data.items():
            logger.info(
                "%s: %d rows, last = %.0f NGN/MT on %s",
                name, len(df), df["price_ngn_mt"].iloc[-1], df["Date"].iloc[-1].date(),
            )
