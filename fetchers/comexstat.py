"""
Layer 29 — Brazil customs exports from MDIC/SECEX Comex Stat (#351).

Monthly exports of soybeans, soybean meal and soybean oil by destination
country and by state of production — tonnage (net kg) and FOB USD, straight
from Brazil's foreign-trade statistics. Brazil is the #1 soy origin and the
stack otherwise holds only its prices (CEPEA, AgRural FOB); this is where its
cargo actually goes.

What this is NOT:
    - Not a price. FOB USD ÷ tonnes is a *customs unit value*: an average over
      every contract that cleared in the month, struck weeks or months
      earlier. It lags the market and is withheld from the public page anyway
      (config.COMEXSTAT_PUBLISH_UNIT_VALUE — CC BY-ND, #351).
    - Not a line-up. Monthly and ~1 month lagged; ANEC's weekly port line-ups
      (#73, licence-gated) are the forward-looking complement.

Traps this module exists to survive (all probed live 2026-10-05):

1.  **The API says how far its numbers run — believe it.** `GET
    /general/dates/updated` names the last published month (2026-08 on
    2026-10-05, released 2026-09-04), and that month bounds the data: the
    request has to span whole years (trap 2), so later months of the year
    are asked for and come back empty, and a row past the declared month —
    MDIC publishing between our two calls on release day — hard-fails rather
    than being stored ahead of the source's own statement. The unreleased
    month can therefore never read as a collapse to zero. Inside the window,
    every month must carry all three products: Brazil ships beans, meal and
    oil every month (68 of 68 live), so a hole is our fetch, not the market,
    and the layer hard-fails rather than storing it. Duplicate keys hard-fail
    too — destinations and states arrive as names, not codes.

2.  **`period` is years × months-of-year, not a range.** `from 2021-01 to
    2026-08` returns January–August of *every* year and silently drops
    September–December of 2021–2025 — 20 of 68 months gone, HTTP 200, no
    warning (live, 2026-10-05; caught by the coverage check in trap 1, not by
    the Jul/Aug cross-check, which sat inside the surviving months). The
    request always spans whole years: January of the start year to December
    of the declared year. Months after the declared one come back empty.

3.  **The rate limit lies about its window.** HTTP 429 says "tente novamente
    em 10 segundos"; retries at 12 s and 65 s still drew 429, and the dates
    call counts against the same budget. One data request per run, waited
    out on config.COMEXSTAT_RATE_LIMIT_WAITS, and a loud failure when the
    schedule runs out — never a partial window.

4.  **Not the bulk CSV.** balanca.economia.gov.br serves an expired leaf
    certificate (notAfter 2026-10-03) with no intermediate. It is only
    reachable with TLS verification off, and invariant 11 says a wrong
    number is worse than a gap.

5.  **Metrics are strings.** `"metricKG": "3163089014"`. A missing or
    unparsable metric is a shape break, never a zero. A *published* zero is
    real (a refined-oil pack whose net weight rounds to 0 kg) and is kept.

6.  **"Não Declarada" is not a state.** Bulk cargo ships before its invoice
    exists, so MDIC records it under an undeclared state and reallocates
    later (Comex Stat manual §6.3.4): the newest months' state split is
    systematically understated. Rows are stored as published; country
    totals are unaffected.

The current year is provisional — MDIC revises every month of it until the
February re-issue — so the whole window is re-downloaded each run and
replaces what is stored (pipeline.store.save_brazil_exports).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import date
from typing import Any

import pandas as pd
import requests

import config
from config import (
    COMEXSTAT_GENERAL_URL,
    COMEXSTAT_LANGUAGE,
    COMEXSTAT_NCM,
    COMEXSTAT_PRODUCTS,
    COMEXSTAT_START_YEAR,
    COMEXSTAT_UPDATED_URL,
    REQUEST_TIMEOUT,
)
from pipeline.results import ScraperShapeError

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; MirrorMarket/1.0)"}


_METRICS = {"metricKG": "kg", "metricFOB": "fob_usd", "metricStatistic": "qty_stat"}

EXPORT_COLUMNS = ("month_end", "ncm", "product", "country", "state", "kg", "fob_usd", "qty_stat")

# Indirection so tests can record waits instead of sleeping.
_sleep = time.sleep


def _month(year: object, month: object) -> tuple[int, int]:
    try:
        y, m = int(str(year)), int(str(month))
    except (TypeError, ValueError):
        raise ScraperShapeError(f"Comex Stat: unreadable month {year!r}-{month!r}") from None
    if not 1 <= m <= 12:
        raise ScraperShapeError(f"Comex Stat: unreadable month {year!r}-{month!r}")
    return y, m


def _label(ym: tuple[int, int]) -> str:
    return f"{ym[0]}-{ym[1]:02d}"


def _metric(row: dict, field: str) -> int:
    raw = row.get(field)
    try:
        value = int(str(raw))
    except (TypeError, ValueError):
        raise ScraperShapeError(
            f"Comex Stat: {field}={raw!r} is not a whole number "
            f"({row.get('coNcm')} {row.get('year')}-{row.get('monthNumber')})"
        ) from None
    if value < 0:
        raise ScraperShapeError(f"Comex Stat: negative {field}={raw!r}")
    return value


def parse_general(payload: object, *, declared: tuple[int, int], start: str) -> dict[str, pd.DataFrame]:
    """Validate a `POST /general` response and split it into one frame per product.

    ``declared`` is the last month MDIC says it has published; ``start`` the
    first month requested ("YYYY-MM"). Raises ScraperShapeError on anything
    that is not a complete, in-window answer.
    """
    if not isinstance(payload, dict) or payload.get("success") is not True:
        raise ScraperShapeError(f"Comex Stat: unsuccessful response {str(payload)[:200]}")
    data = payload.get("data")
    rows = data.get("list") if isinstance(data, dict) else None
    if not isinstance(rows, list) or not rows:
        raise ScraperShapeError("Comex Stat: response carries no data.list rows")

    first = _month(*start.split("-"))
    records = []
    for row in rows:
        ncm = str(row.get("coNcm"))
        if ncm not in COMEXSTAT_NCM:
            raise ScraperShapeError(f"Comex Stat: unrequested NCM {ncm} in response")
        ym = _month(row.get("year"), row.get("monthNumber"))
        if ym > declared or ym < first:
            raise ScraperShapeError(
                f"Comex Stat: row for {_label(ym)} outside the requested window "
                f"{start}..{_label(declared)}"
            )
        country, state = row.get("country"), row.get("state")
        if not country or not state:
            raise ScraperShapeError(f"Comex Stat: row without country/state: {row}")
        record = {
            "month_end": pd.Period(_label(ym), freq="M").end_time.normalize(),
            "ncm": ncm,
            "product": COMEXSTAT_NCM[ncm],
            "country": str(country),
            "state": str(state),
        }
        for field, column in _METRICS.items():
            record[column] = _metric(row, field)
        records.append(record)

    frame = pd.DataFrame.from_records(records, columns=list(EXPORT_COLUMNS))

    # Names, not codes: two rows on one key would be deduplicated away later
    # and their tonnage lost without a sound.
    key = ["month_end", "ncm", "country", "state"]
    duplicated = frame[frame.duplicated(subset=key, keep=False)]
    if not duplicated.empty:
        first_dup = duplicated.iloc[0]
        raise ScraperShapeError(
            f"Comex Stat: {len(duplicated)} rows share a duplicate key, e.g. "
            f"{first_dup['month_end'].date()} {first_dup['ncm']} "
            f"{first_dup['country']}/{first_dup['state']}"
        )

    # Brazil ships beans, meal and oil every month (68 of 68 months live), so
    # a published month missing any of them is a fetch hole, not the market.
    for product in COMEXSTAT_PRODUCTS:
        covered = {
            (ts.year, ts.month)
            for ts in frame.loc[frame["product"] == product, "month_end"]
        }
        for period in pd.period_range(start, _label(declared), freq="M"):
            if (period.year, period.month) not in covered:
                raise ScraperShapeError(
                    f"Comex Stat: published month {period} has no {product} export rows — "
                    "Brazil ships it every month, so this is a fetch hole, not the market"
                )

    out: dict[str, pd.DataFrame] = {}
    for product in COMEXSTAT_PRODUCTS:
        part = frame[frame["product"] == product].reset_index(drop=True)
        if not part.empty:
            out[product] = part
    return out


def _with_rate_limit(send: Callable[[], Any], label: str) -> Any:
    """Send, waiting out 429s on the configured schedule; any other non-200 raises.

    Both endpoints share one budget (the dates call counts against it), and a
    GitHub runner's IP is shared with strangers, so either call can draw a 429
    on its first attempt.
    """
    waits = list(config.COMEXSTAT_RATE_LIMIT_WAITS)
    while True:
        resp = send()
        if resp.status_code == 200:
            return resp
        if resp.status_code == 429 and waits:
            wait = waits.pop(0)
            logger.warning("Comex Stat %s rate-limited (429); waiting %ss", label, wait)
            _sleep(wait)
            continue
        raise RuntimeError(
            f"Comex Stat {label}: HTTP {resp.status_code} "
            f"(rate-limit retries left: {len(waits)}): {resp.text[:200]}"
        )


def _declared_month(today: date) -> tuple[int, int]:
    """The last month MDIC says it has published, from its own endpoint."""
    resp = _with_rate_limit(
        lambda: requests.get(COMEXSTAT_UPDATED_URL, headers=_HEADERS, timeout=REQUEST_TIMEOUT),
        "dates/updated",
    )
    body = resp.json()
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, dict) or "year" not in data or "monthNumber" not in data:
        raise ScraperShapeError(f"Comex Stat dates/updated: unreadable {str(body)[:200]}")
    declared = _month(data["year"], data["monthNumber"])
    if declared >= (today.year, today.month):
        raise ScraperShapeError(
            f"Comex Stat dates/updated declares {_label(declared)}, which has not ended"
        )
    return declared


def _post_general(body: dict) -> dict:
    """POST the data request, waiting out 429s on the configured schedule."""
    resp = _with_rate_limit(
        lambda: requests.post(
            COMEXSTAT_GENERAL_URL, json=body, params={"language": COMEXSTAT_LANGUAGE},
            headers=_HEADERS, timeout=REQUEST_TIMEOUT * 4,
        ),
        "general",
    )
    return resp.json()


def fetch_brazil_exports(today: date | None = None) -> dict[str, pd.DataFrame]:
    """Fetch the whole window, start year → MDIC's declared month, in one request."""
    today = today or date.today()
    declared = _declared_month(today)
    start = f"{COMEXSTAT_START_YEAR}-01"
    body = {
        "flow": "export",
        "monthDetail": True,
        # Whole years: the API filters months-of-year, so a `to` short of
        # December would drop those months from every earlier year (trap 2).
        "period": {"from": start, "to": f"{declared[0]}-12"},
        "filters": [{"filter": "ncm", "values": sorted(COMEXSTAT_NCM)}],
        "details": ["ncm", "country", "state"],
        "metrics": list(_METRICS),
    }
    logger.info("Requesting Comex Stat exports %s..%s ...", start, _label(declared))
    frames = parse_general(_post_general(body), declared=declared, start=start)
    logger.info(
        "Comex Stat: %s rows through %s",
        {k: len(v) for k, v in frames.items()}, _label(declared),
    )
    return frames
