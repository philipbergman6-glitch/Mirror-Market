"""
Layer 16 — India domestic soybean spot via the data.gov.in Mandi Price API.

Official Agmarknet feed (Ministry of Agriculture) republished on the Open
Government Data platform under the GODL licence. Replaces the NCDEX Bhav
Copy source (``fetchers/india_domestic.py``, kept on disk as a dormant
fallback): NCDEX soy derivatives are SEBI-suspended to at least 2027-03-31
and its spot pages sit behind a fingerprint anti-bot wall.

Why it matters:
    India is the world's #4 soybean consumer. Maharashtra (Latur, Vidarbha)
    is the #1 producing state since 2025-26 (~47% of the crop per SOPA
    Kharif 2025), with Madhya Pradesh (~39%) second — but Indore/MP remains
    the crush-industry pricing hub, so the MP series stays the headline
    benchmark. When Indian beans are cheap vs CBOT, import appetite fades
    and Middle East / African meal buyers switch suppliers.

Series construction:
    One series per configured state (``MANDI_STATES``), one row per
    arrival date — the MEDIAN of ``modal_price`` across all reporting
    mandis in that state (~76/day in MP), robust to single-mandi
    outliers. Prices arrive in INR/quintal (100 kg) and are stored as
    INR/MT (×10). High/Low carry the cross-mandi max/min, Volume the
    reporting-mandi count. USD conversion happens at the analysis layer.
    Series are stored per-state and never pooled — a cross-state median
    would put a level break on the existing MP history.

Key handling:
    ``DATA_GOV_IN_API_KEY`` is used when set; otherwise the published
    sample key (a public testing credential). The sample key caps every
    response at 10 rows and shares a global throttle, so pagination and
    429-retry are load-bearing here, not defensive.

Parser strategy:
    The API returns JSON with ``total`` and ``records``. A response
    missing those, or records missing the price/date fields, raises
    ScraperShapeError — the schema changed and silence would be worse
    than a crash. Zero records for the filter is a normal holiday/Sunday
    outcome (mandis closed), not an error.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence
from datetime import datetime

import pandas as pd
import requests

from config import (
    MANDI_API_URL,
    MANDI_COMMODITY,
    MANDI_MAX_PAGES,
    MANDI_PAGE_LIMIT,
    MANDI_SAMPLE_API_KEY,
    MANDI_STATES,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
)
from fetchers._backoff import retry_sleep
from pipeline.results import FetchResult, ScraperShapeError

logger = logging.getLogger(__name__)

_QUINTAL_TO_MT = 10.0  # INR/quintal (100 kg) → INR/MT


def _api_key() -> str:
    return os.environ.get("DATA_GOV_IN_API_KEY") or MANDI_SAMPLE_API_KEY


def _fetch_page(offset: int, state: str) -> dict:
    """Fetch one page of the mandi resource for one state. Raises on
    exhausted retries.

    429s are expected on the shared-throttle sample key and retried with
    backoff like any transport failure.
    """
    params: dict[str, str | int] = {
        "api-key": _api_key(),
        "format": "json",
        "limit": MANDI_PAGE_LIMIT,
        "offset": offset,
        "filters[commodity]": MANDI_COMMODITY,
        "filters[state]": state,
    }
    last_error = "no attempts made"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(MANDI_API_URL, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            last_error = f"HTTP {resp.status_code}"
            logger.warning(
                "Mandi API: %s at offset %d (attempt %d)",
                last_error, offset, attempt,
            )
        except (requests.RequestException, ValueError) as exc:
            last_error = str(exc)
            logger.warning(
                "Mandi API attempt %d failed at offset %d: %s", attempt, offset, exc
            )
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)
    raise requests.RequestException(
        f"Mandi API: offset {offset} failed after {MAX_RETRIES} attempts ({last_error})"
    )


def _collect_records(state: str) -> list[dict]:
    """Paginate through the state-filtered resource until ``total`` rows are in hand."""
    records: list[dict] = []
    total: int | None = None

    for page in range(MANDI_MAX_PAGES):
        payload = _fetch_page(page * MANDI_PAGE_LIMIT, state)
        if "records" not in payload or "total" not in payload:
            raise ScraperShapeError(
                "Mandi API: response missing 'records'/'total' — schema changed "
                f"(keys: {sorted(payload)[:10]})"
            )
        total = int(payload["total"])
        page_records = payload["records"]
        records.extend(page_records)
        if len(records) >= total or not page_records:
            break
    else:
        logger.warning(
            "Mandi API: %s stopped at page cap %d with %d/%s records",
            state, MANDI_MAX_PAGES, len(records), total,
        )
    return records


def _aggregate(records: list[dict]) -> pd.DataFrame:
    """Distill per-mandi rows into one median-modal row per arrival date.

    Returns the ``clean_india_domestic``/``save_india_domestic`` shape:
    Date (ISO), Open/High/Low/Close (INR/MT), Volume (mandi count), Unit.
    """
    parsed: list[dict[str, object]] = []
    malformed = 0
    for rec in records:
        try:
            arrival = datetime.strptime(str(rec["arrival_date"]), "%d/%m/%Y").date()
            modal = float(rec["modal_price"])
            low = float(rec.get("min_price") or 0)
            high = float(rec.get("max_price") or 0)
        except (KeyError, TypeError, ValueError):
            malformed += 1
            continue
        if modal <= 0:
            malformed += 1
            continue
        # Thin-arrival days report min/max of "0" — fall back to modal so a
        # single mandi can't drag the day's Low/High band to zero.
        if low <= 0:
            low = modal
        if high <= 0:
            high = modal
        parsed.append({"date": arrival, "modal": modal, "min": low, "max": high})

    if records and not parsed:
        raise ScraperShapeError(
            f"Mandi API: {len(records)} records, none with parseable "
            "arrival_date/modal_price — field names or formats changed"
        )
    if malformed:
        logger.warning("Mandi API: skipped %d malformed records", malformed)

    raw = pd.DataFrame(parsed)
    if raw.empty:
        return raw

    agg = raw.groupby("date").agg(
        high=("max", "max"),
        low=("min", "min"),
        close=("modal", "median"),
        volume=("modal", "size"),
    ).reset_index()
    df = pd.DataFrame({
        "Date": agg["date"].map(lambda d: d.isoformat()),
        "Open": float("nan"),
        "High": agg["high"] * _QUINTAL_TO_MT,
        "Low": agg["low"] * _QUINTAL_TO_MT,
        "Close": agg["close"] * _QUINTAL_TO_MT,
        "Volume": agg["volume"].astype(float),
        "Unit": "INR/MT",
    })
    return df.sort_values("Date").reset_index(drop=True)


def fetch_mandi_prices() -> FetchResult:
    """Fetch the soybean mandi set for each configured state.

    Returns one series per state in ``MANDI_STATES``. A schema change
    (ScraperShapeError) in any state is ``failed`` — the resource is
    shared, so a shape break in one state means the source changed for
    all. Transport exhaustion on one state degrades to a partial result
    if another state succeeded. Zero matching records everywhere is
    ``empty`` (mandis closed — Sunday/holiday).
    """
    data: dict[str, pd.DataFrame] = {}
    errors: list[str] = []
    for state, series in MANDI_STATES.items():
        logger.info(
            "Fetching %s mandi prices for %s from data.gov.in ...",
            MANDI_COMMODITY, state,
        )
        try:
            records = _collect_records(state)
            df = _aggregate(records)
        except ScraperShapeError as exc:
            logger.error("Mandi API: %s", exc)
            return FetchResult.failed(str(exc))
        except requests.RequestException as exc:
            errors.append(f"{state}: {exc}")
            continue

        if df.empty:
            logger.info("Mandi API: no %s rows for %s today", MANDI_COMMODITY, state)
            continue

        logger.info(
            "Mandi API: %s — %d session row(s), latest ₹%.0f/MT across %d mandis",
            state, len(df), df["Close"].iloc[-1], int(df["Volume"].iloc[-1]),
        )
        data[series] = df

    if data:
        if errors:
            logger.warning("Mandi API: partial result — %s", "; ".join(errors))
        return FetchResult.ok(data)
    if errors:
        return FetchResult.failed("; ".join(errors))
    return FetchResult.empty(
        f"no {MANDI_COMMODITY} rows for any of {', '.join(MANDI_STATES)} today"
    )


__all__: Sequence[str] = ("_aggregate", "_collect_records", "fetch_mandi_prices")


# ── Quick self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    result = fetch_mandi_prices()
    if not result.has_rows:
        logger.info("Mandi API: %s — %s", result.status, result.error)
    else:
        for name, frame in result.data.items():
            logger.info("%s:\n%s", name, frame.to_string(index=False))
