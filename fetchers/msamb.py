"""
Layer 16 (Maharashtra leg) — soybean mandi prices from MSAMB.

The Maharashtra State Agricultural Marketing Board publishes each APMC's
daily arrivals and min / max / modal price per commodity. It is the
Maharashtra source Agmarknet itself republishes: on the 2026-10
validation every one of 202 MSAMB market-days carried the same modal as
Agmarknet (LAYERS.md Layer 16). So the series continues under the same
key, ``Soybean (Mandi MH)``, built by the same median as before
(``fetchers.mandi._aggregate``).

Why it is read now: from 2026-10-09 Agmarknet's report API demands a
captcha (#449). MSAMB answers an unattended request from outside India
(HTTP 200 from IL and check-host US/DE nodes, 2026-11-17) with no key,
token or captcha.

Transport: the page's own XHR, ``GET /ApmcDetail/DataGridBind?
commodityCode=…&apmcCode=null``, returns an HTML ``<tr>`` fragment: a
one-cell date row (``dd/mm/yyyy``) and then seven-cell lot rows (APMC,
variety, unit, arrivals, min, max, modal). It serves a rolling ~8 days.

Guards (each raises ScraperShapeError — the source changed, not the market):

* the commodity code must still label सोयाबिन on the price page, or a
  re-coded list would hand us another crop's prices at HTTP 200;
* every row is either a date row or a seven-cell lot row;
* a lot row carrying any unit other than quintal (नग = pieces) prices a
  different unit and is dropped and counted, never pooled; a fragment with
  no quintal lot at all is a failure;
* arrivals are quintals and are stored as MT (÷10), the column the
  Agmarknet build stored;
* ``_aggregate``'s ₹/quintal band catches a silent unit switch.

Completed IST days only (``MANDI_MIN_AGE_DAYS``): today fills APMC by APMC.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import (
    MANDI_MIN_AGE_DAYS,
    MAX_RETRIES,
    MSAMB_GRID_URL,
    MSAMB_PAGE_URL,
    MSAMB_QUINTAL_UNIT,
    MSAMB_SOYBEAN_CODE,
    REQUEST_TIMEOUT,
)
from fetchers._backoff import retry_sleep
from fetchers.mandi import _aggregate, _ist_today
from pipeline.results import ScraperShapeError
from pipeline.units import quintals_to_metric_tons

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; MirrorMarket/1.0)",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": MSAMB_PAGE_URL,
}
_SOYBEAN_LABEL = "सोयाबिन"
_DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")
_LOT_CELLS = 7


def _get(url: str, params: dict[str, str] | None = None) -> str:
    """GET with retries; raises requests.RequestException when exhausted."""
    last: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, headers=_HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.text
            last = requests.HTTPError(f"HTTP {resp.status_code}")
            logger.warning("MSAMB: HTTP %d for %s (attempt %d)", resp.status_code, url, attempt)
        except requests.RequestException as exc:
            last = exc
            logger.warning("MSAMB: request failed (attempt %d): %s", attempt, exc)
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)
    raise requests.RequestException(f"MSAMB: {url} failed after {MAX_RETRIES} attempts: {last}")


def _assert_code_is_soybean(page_html: str) -> None:
    soup = BeautifulSoup(page_html, "html.parser")
    option = soup.find("option", attrs={"value": MSAMB_SOYBEAN_CODE})
    label = option.get_text(strip=True) if option else None
    if label != _SOYBEAN_LABEL:
        raise ScraperShapeError(
            f"MSAMB: commodity code {MSAMB_SOYBEAN_CODE!r} labels {label!r} on the "
            f"price page, not {_SOYBEAN_LABEL!r} — the commodity list was re-coded"
        )


def _number(text: str, what: str, row: list[str]) -> float:
    try:
        return float(text.replace(",", ""))
    except ValueError as exc:
        raise ScraperShapeError(f"MSAMB: unparseable {what} {text!r} in row {row!r}") from exc


def _parse_grid(fragment: str) -> list[dict]:
    """The grid fragment → per-lot records in ``fetchers.mandi._aggregate``'s shape."""
    if not fragment.strip() or fragment.strip() == "No record found!":
        raise ScraperShapeError("MSAMB: the soybean grid came back empty")
    soup = BeautifulSoup(fragment, "html.parser")
    records: list[dict] = []
    other_units: dict[str, int] = {}
    day: str | None = None
    for tr in soup.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cells) == 1 and _DATE_RE.match(cells[0]):
            day = cells[0]
            continue
        if len(cells) != _LOT_CELLS:
            raise ScraperShapeError(f"MSAMB: row with {len(cells)} cell(s), expected a date or {_LOT_CELLS}: {cells!r}")
        if day is None:
            raise ScraperShapeError(f"MSAMB: lot row before any date row: {cells!r}")
        unit = cells[2]
        if unit != MSAMB_QUINTAL_UNIT:
            other_units[unit] = other_units.get(unit, 0) + 1
            continue
        arrivals_qtl = _number(cells[3], "arrivals", cells)
        records.append({
            "arrival_date": day,
            "modal_price": _number(cells[6], "modal price", cells),
            "arrivals": quintals_to_metric_tons(arrivals_qtl) if arrivals_qtl >= 0 else None,
        })
    if other_units:
        logger.info("MSAMB: %s lot(s) in a non-quintal unit dropped, not pooled", other_units)
    if not records:
        raise ScraperShapeError(
            f"MSAMB: no quintal-priced soybean lot in the grid (other units: {other_units})"
        )
    return records


def _completed(records: list[dict], today: date) -> list[dict]:
    cutoff = today - timedelta(days=MANDI_MIN_AGE_DAYS)
    done = [r for r in records if datetime.strptime(r["arrival_date"], "%d/%m/%Y").date() <= cutoff]
    if not done:
        raise ScraperShapeError(
            f"MSAMB: no completed soybean day on or before {cutoff} — "
            f"{len(records)} lot(s) in total; an 8-day window is never a closure"
        )
    return done


def fetch_msamb_soybean(today: date | None = None) -> pd.DataFrame:
    """Maharashtra soybean: one median-modal row per completed IST day.

    Raises ScraperShapeError on a guard failure and
    requests.RequestException on exhausted transport.
    """
    today = today or _ist_today()
    _assert_code_is_soybean(_get(MSAMB_PAGE_URL))
    fragment = _get(MSAMB_GRID_URL, {"commodityCode": MSAMB_SOYBEAN_CODE, "apmcCode": "null"})
    df = _aggregate(_completed(_parse_grid(fragment), today))
    logger.info(
        "MSAMB: Maharashtra — %d completed day(s), latest %s ₹%.0f/MT across %d lots",
        len(df), df["Date"].iloc[-1], df["Close"].iloc[-1], int(df["Volume"].iloc[-1]),
    )
    return df


__all__: Sequence[str] = ("_parse_grid", "fetch_msamb_soybean")
