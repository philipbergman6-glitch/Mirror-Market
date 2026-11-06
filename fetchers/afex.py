"""
Layer 36 — AFEX Nigeria soybean reference price.

Source: AFEX (Africa Exchange) market-data endpoint
        https://api-md.afexnigeria.com/AFEXMD/api/v1/securities/price
Prices: NGN/kg in the feed, stored as NGN/MT.

Why it matters:
    Nigeria is West Africa's largest soybean producer and, until this layer,
    had no price series in this repo at all — only weather, NGN/USD and PSD
    supply/demand.  AFEX is the only daily, machine-readable Nigerian soy
    price found and corroborated (see
    research/2026-08-10-nigeria-price-source.md; level agrees within ~10%
    with NCX Kaduna, NCX Nasarawa and LCFE on a matched date).

What this series is NOT:
    Not an exchange settlement.  Prices change on Saturdays and Sundays and
    74% of changes are day-over-day, so this is a daily *reference* price
    (quote_kind ``exchange_reference``).  Anything downstream must label it
    that way.  No volume or open-interest field is exposed, so liquidity
    cannot be assessed from the feed.

Fetch strategy (re-measured 2026-11-05):
    The endpoint changed between 2026-08 and 2026-11.  A bare GET now answers
    only the last 30 days of maize; the full history comes back only with the
    parameters AFEX's own front end sends (``security_code``, ``date_from``,
    ``date_to`` — read from africaexchange.com's ``_app`` bundle, where
    ``market_data`` is built from exactly those three).  One request per
    configured series then returns the whole history (1,747 rows from
    2021-01-23 for SSBS), so the layer self-heals on an ephemeral CI database.
    An unknown or bare ``query`` parameter answers HTTP 500; an unknown
    ``security_code`` answers ``200`` with an empty ``data`` list, so an empty
    list for a series this layer asks for is a broken feed, never a quiet day.

    The endpoint still requires an Origin header; without it the API returns
    401.

Completed days only:
    AFEX stamps a row for the current Lagos calendar day as soon as it
    publishes one, and nothing in the feed says whether that number is final
    for the day.  Invariant 11 (a wrong number is worse than a gap): rows
    dated today in Africa/Lagos are dropped, so what is stored is always a
    finished day, and the full re-read on every run repairs any day that was
    revised after first publication (``AFEX_MIN_AGE_DAYS``).

Shape guarantees:
    Every response must be the ``{"responseCode": "100", "data": [...]}``
    envelope.  The feed changed schema once already — rows before 2022-01-18
    used bare 'SBS' keys, rows after the 'SSBS'/'DSBS'/'OSBS' triplet.  We read
    the configured key and hard-fail with ScraperShapeError if it disappears,
    rather than silently storing nothing.  A ``0.0`` price is "not quoted that
    day" in this feed (the early-2021 rows) and is skipped, never stored.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from config import (
    AFEX_HISTORY_START,
    AFEX_MAX_FLAT_DAYS,
    AFEX_MIN_AGE_DAYS,
    AFEX_ORIGIN,
    AFEX_PRICE_URL,
    AFEX_SERIES,
    AFEX_TIMEZONE,
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


def _lagos_today(now: datetime | None = None) -> date:
    """The AFEX calendar day: the feed stamps rows in Nigeria's own date."""
    now = now or datetime.now(ZoneInfo(AFEX_TIMEZONE))
    return now.astimezone(ZoneInfo(AFEX_TIMEZONE)).date()


def _fetch_payload(security_code: str, *, today: date) -> list[dict] | None:
    """Download one security's full AFEX price history.

    Sends the three parameters the AFEX front end sends (``security_code``,
    ``date_from``, ``date_to``); the bare URL no longer carries the soybean
    series (measured 2026-11-05).  Returns the decoded ``data`` list, or None
    on transport failure or a response that is not the AFEX envelope.  Uses
    exponential backoff with jitter so retries don't all hit the upstream at
    the same instant.
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
    params = {
        "security_code": security_code,
        "date_from": AFEX_HISTORY_START,
        "date_to": today.isoformat(),
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                AFEX_PRICE_URL, headers=headers, params=params, timeout=REQUEST_TIMEOUT
            )
            if resp.status_code == 200:
                payload = resp.json()
                rows = _unwrap_envelope(payload)
                if rows is not None:
                    return rows
                logger.warning(
                    "AFEX %s: unexpected JSON shape (attempt %d): %.200r",
                    security_code, attempt, payload,
                )
            elif resp.status_code == 401:
                # The Origin gate is the single most likely thing to change.
                logger.error(
                    "AFEX: HTTP 401 — the Origin gate rejected us. "
                    "Access may have been revoked; see AFEX_ORIGIN in config.py."
                )
            else:
                logger.warning(
                    "AFEX %s: HTTP %d (attempt %d)", security_code, resp.status_code, attempt
                )
        except (requests.RequestException, ValueError) as exc:
            logger.warning("AFEX %s: request failed (attempt %d): %s", security_code, attempt, exc)

        if attempt < MAX_RETRIES:
            retry_sleep(attempt)

    return None


def _unwrap_envelope(payload: object) -> list[dict] | None:
    """Return the ``data`` list of an AFEX envelope, or None if it is not one.

    The envelope is ``{"responseCode": "100", "data": [...], "message": ...}``
    (observed 2026-08-10 and 2026-11-05).  Anything else — a bare list, a
    missing ``data``, a non-"100" code — is a shape change and must not be
    read as if it were the feed.
    """
    if not isinstance(payload, dict):
        return None
    if str(payload.get("responseCode")) != "100":
        return None
    data = payload.get("data")
    if not isinstance(data, list):
        return None
    return data


def _completed_days_only(df: pd.DataFrame, today: date) -> pd.DataFrame:
    """Drop rows dated within ``AFEX_MIN_AGE_DAYS`` of the AFEX calendar day.

    The current Lagos day is published while it may still change; only a
    finished day is stored (invariant 11).  The feed is re-read in full on
    every run, so yesterday's row arrives tomorrow regardless.
    """
    if df.empty:
        return df
    cutoff = pd.Timestamp(today - timedelta(days=AFEX_MIN_AGE_DAYS - 1))
    kept = df[df["Date"] < cutoff].reset_index(drop=True)
    dropped = len(df) - len(kept)
    if dropped:
        logger.info(
            "AFEX: dropped %d row(s) dated %s or later — not a completed day yet",
            dropped, cutoff.date(),
        )
    return kept


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


def fetch_afex(today: date | None = None) -> FetchResult:
    """Fetch AFEX Nigeria soybean reference prices, one request per series.

    Parameters
    ----------
    today:
        The AFEX calendar day (Africa/Lagos); defaults to now.  Rows dated
        on or after it are not stored (see ``_completed_days_only``).

    Returns
    -------
    FetchResult
        ``ok`` with ``{commodity_name: DataFrame}`` (Date, price_ngn_mt,
        Unit) carrying the full available history of completed days;
        ``failed`` when the endpoint can't be reached, is not the envelope,
        or its schema changed (an empty ``data`` list for a series this layer
        asks for is a schema change — the feed answers unknown codes that
        way); ``empty`` only when every row parsed cleanly and none was a
        completed day.
    """
    logger.info("Fetching AFEX Nigeria soybean prices ...")
    today = today or _lagos_today()

    data: dict[str, pd.DataFrame] = {}
    for key, name in AFEX_SERIES.items():
        payload = _fetch_payload(key, today=today)
        if payload is None:
            return FetchResult.failed(f"AFEX {key}: endpoint unreachable or not the AFEX envelope")
        try:
            df = _parse_series(payload, key, name)
        except ScraperShapeError as exc:
            logger.error("AFEX: feed structure changed — %s", exc)
            return FetchResult.failed(str(exc))
        df = _completed_days_only(df, today)
        if not df.empty:
            data[name] = df

    if not data:
        return FetchResult.empty("AFEX: feed parsed but carried no completed-day rows")
    return FetchResult.ok(data)


# Re-export for tests
__all__: Sequence[str] = (
    "_completed_days_only", "_parse_series", "_trailing_flat_days", "_unwrap_envelope", "fetch_afex",
)


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
