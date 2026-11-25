"""
Layer 16 (Indore leg) — NCDEX polled spot price for soybean at Indore.

NCDEX polls spot prices for its delivery centres a few times each trading
day and publishes them on its "Historical Spot Prices" page, whether or not
the futures contract trades (soy derivatives are SEBI-suspended to at
least 2027-03-31; the spot poll carries on). ``SYBEANIDR`` is the Indore
centre — the crush-industry hub the India headline has always meant.

**What the number is.** One poll of traders' spot quotes for the exchange's
specified quality at Indore, in ₹/quintal. It is *not* a mandi median: on
2026-11-06 → 11-17 it ran 3–8% above MSAMB's Akola modal and SEA's weekly
Indore quote, the spread a specified-quality poll has over mixed farmer
lots. So it is stored under its own key (``config.NCDEX_SPOT_SERIES``) and
never spliced onto ``Soybean (Mandi MP)``. The unit is not printed on the
spot page; ₹/quintal is the NCDEX soybean contract's price quote, and the
level (₹7,655 on 2026-11-17) sits on the per-quintal scale of every other
Indian quote we hold — the mandi plausibility band enforces it.

**Close = the day's last poll** (polls land ~12:10 and ~15:10–15:40 IST).
Open/High/Low stay NaN: two polls are not a traded range. Volume stays
NaN: a poll has none. Completed IST days only (``MANDI_MIN_AGE_DAYS``), so a
day is stored after its final poll.

**Transport.** The page is a Laravel DataTables view: GET the page for its
``csrf-token`` meta and session cookie, then POST ``/spotprices/get_data``
with the symbol and a date window. NCDEX serves only about a month of polls
(earliest 2026-10-15 on 2026-11-17), so each run re-reads
``NCDEX_SPOT_LOOKBACK_DAYS``. Two traps measured on 2026-11-17: a
DataTables ``length`` of 1000+ returns *zero* records with HTTP 200, and
leaving the ``columns[i][data]`` / ``order`` fields out is HTTP 500 — both
are sent exactly as the page sends them.

Guards (ScraperShapeError): every row must name the requested symbol and
centre; dates and prices must parse; a response that says it holds more
rows than it returned is truncated; an empty window is a failure, never a
closure (Indore polls every trading day).
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from datetime import date, datetime, timedelta

import pandas as pd
import requests

from config import (
    MANDI_MIN_AGE_DAYS,
    MANDI_MODAL_MAX_INR_QUINTAL,
    MANDI_MODAL_MIN_INR_QUINTAL,
    MAX_RETRIES,
    NCDEX_SPOT_DATA_URL,
    NCDEX_SPOT_LOOKBACK_DAYS,
    NCDEX_SPOT_PAGE_URL,
    REQUEST_TIMEOUT,
)
from fetchers._backoff import retry_sleep
from fetchers.mandi import _ist_today
from pipeline.results import ScraperShapeError
from pipeline.units import per_quintal_to_per_metric_ton

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; MirrorMarket/1.0)"}
_TOKEN_RE = re.compile(r'<meta\s+name="csrf-token"\s+content="([^"]+)"')
_SYMBOL_RE = re.compile(r"/products/([A-Z0-9]+)")
_COLUMNS = ("symbol", "center", "price_date", "price_time", "spot_price")
_PAGE_LENGTH = 200  # ~2 polls/day × 31 days; 1000+ silently returns nothing


def _form(token: str, symbol: str, start: date, end: date) -> dict[str, object]:
    form: dict[str, object] = {
        "draw": 1, "start": 0, "length": _PAGE_LENGTH,
        "search[value]": "", "search[regex]": "false",
        "order[0][column]": 2, "order[0][dir]": "asc",
        "_token": token, "symbol": symbol, "ftype": 1,
        "df": start.isoformat(), "dt": end.isoformat(),
    }
    for i, name in enumerate(_COLUMNS):
        form[f"columns[{i}][data]"] = name
        form[f"columns[{i}][searchable]"] = "true"
        form[f"columns[{i}][orderable]"] = "true"
        form[f"columns[{i}][search][value]"] = ""
        form[f"columns[{i}][search][regex]"] = "false"
    return form


def _download(symbol: str, start: date, end: date) -> dict:
    """One symbol's polls in [start, end]. Raises RequestException when exhausted."""
    last: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            session = requests.Session()
            session.headers.update(_HEADERS)
            page = session.get(NCDEX_SPOT_PAGE_URL, timeout=REQUEST_TIMEOUT)
            page.raise_for_status()
            match = _TOKEN_RE.search(page.text)
            if not match:
                raise ScraperShapeError("NCDEX spot: page carries no csrf-token meta — layout changed")
            resp = session.post(
                NCDEX_SPOT_DATA_URL, data=_form(match.group(1), symbol, start, end),
                headers={"X-Requested-With": "XMLHttpRequest", "Referer": NCDEX_SPOT_PAGE_URL},
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, dict) or "data" not in payload:
                raise ScraperShapeError(f"NCDEX spot: response is not a DataTables payload: {str(payload)[:200]}")
            return payload
        except ScraperShapeError:
            raise  # a ValueError subclass, but drift — never retried as transport
        except (requests.RequestException, ValueError) as exc:  # ValueError: non-JSON body
            last = exc
            logger.warning("NCDEX spot: %s attempt %d failed: %s", symbol, attempt, exc)
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)
    raise requests.RequestException(f"NCDEX spot: {symbol} failed after {MAX_RETRIES} attempts: {last}")


def _parse(payload: dict, symbol: str, centre: str) -> pd.DataFrame:
    """DataTables rows → one Close (last poll) per date, INR/MT."""
    rows = payload["data"]
    total = payload.get("recordsFiltered", payload.get("recordsTotal"))
    if isinstance(total, int) and total > len(rows):
        raise ScraperShapeError(
            f"NCDEX spot: {symbol} reports {total} polls but returned {len(rows)} — truncated"
        )
    polls: list[tuple[date, str, float]] = []
    for row in rows:
        sym = _SYMBOL_RE.search(str(row.get("symbol", "")))
        if not sym or sym.group(1) != symbol:
            raise ScraperShapeError(f"NCDEX spot: row for {row.get('symbol')!r}, asked for {symbol!r}")
        if str(row.get("center", "")).strip() != centre:
            raise ScraperShapeError(f"NCDEX spot: {symbol} row names centre {row.get('center')!r}, not {centre!r}")
        try:
            day = datetime.strptime(str(row["price_date"]), "%d-%b-%Y").date()
            price = float(str(row["spot_price"]).replace(",", ""))
            datetime.strptime(str(row["price_time"]), "%H:%M:%S")
        except (KeyError, ValueError) as exc:
            raise ScraperShapeError(f"NCDEX spot: unparseable {symbol} row {row!r}") from exc
        if not MANDI_MODAL_MIN_INR_QUINTAL <= price <= MANDI_MODAL_MAX_INR_QUINTAL:
            raise ScraperShapeError(
                f"NCDEX spot: {symbol} {day} ₹{price:,.2f} is outside the ₹/quintal "
                f"band ₹{MANDI_MODAL_MIN_INR_QUINTAL:,}–₹{MANDI_MODAL_MAX_INR_QUINTAL:,} — unit change?"
            )
        polls.append((day, str(row["price_time"]), price))
    if not polls:
        raise ScraperShapeError(f"NCDEX spot: no {symbol} polls in the window — Indore polls every trading day")
    frame = pd.DataFrame(polls, columns=["day", "time", "price"]).sort_values(["day", "time"])
    last = frame.groupby("day", as_index=False).last()
    return pd.DataFrame({
        "Date": last["day"].map(lambda d: d.isoformat()),
        "Open": float("nan"),
        "High": float("nan"),
        "Low": float("nan"),
        "Close": per_quintal_to_per_metric_ton(last["price"]),
        "Volume": float("nan"),
        "arrivals_mt": float("nan"),
        "Unit": "INR/MT",
    }).reset_index(drop=True)


def fetch_ncdex_spot(series: str, symbol: str, centre: str, today: date | None = None) -> pd.DataFrame:
    """Completed-day spot closes for one NCDEX centre, INR/MT.

    Raises ScraperShapeError on a guard failure and
    requests.RequestException on exhausted transport.
    """
    today = today or _ist_today()
    cutoff = today - timedelta(days=MANDI_MIN_AGE_DAYS)
    payload = _download(symbol, today - timedelta(days=NCDEX_SPOT_LOOKBACK_DAYS), cutoff)
    df = _parse(payload, symbol, centre)
    df = df[df["Date"] <= cutoff.isoformat()].reset_index(drop=True)
    if df.empty:
        raise ScraperShapeError(f"NCDEX spot: no completed {symbol} day on or before {cutoff}")
    logger.info(
        "NCDEX spot: %s — %d day(s), latest %s ₹%.0f/MT", series, len(df),
        df["Date"].iloc[-1], df["Close"].iloc[-1],
    )
    return df


__all__: Sequence[str] = ("_parse", "fetch_ncdex_spot")
