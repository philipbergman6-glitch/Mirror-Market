"""
Layer 33 — SOPA all-India state-wise soybean crop estimate (private).

The Soybean Processors Association of India publishes one kharif estimate a
year — sowing area, expected yield and estimated production by state, with
MP/MH division and district detail beneath — on a page that is one URL per
crop year (``config.SOPA_URL`` + ``select_year``). It is the only state-wise
Indian soybean crop figure in the stack; Layer 16's mandi price sits on top
of it, and ``analysis/india_crop.py`` derives the public-safe readings
(all-India year-on-year change, state shares, one attributed total).

The page is overwritten in place:
    SOPA revises the estimate after the first release (kharif 2025 opened at
    105.36 lakh t and read 110.267 by 2026-10-07) and the page keeps only the
    current figure. Rows are therefore keyed by the day they were read
    (``fetched_date``) so each run's reading survives; the revision path
    exists only where we kept it. Because the table is private (below) it is
    not in the history export, so that path persists only in a local DB —
    the ephemeral CI database does not keep it.

Units are inferred, so every row must reproduce them:
    Nothing on the page states a unit. The columns are lakh hectares, kg/ha
    and lakh tonnes (44.683 × 1169 / 1000 = 52.23 ≈ 52.229), and the parser
    requires that identity on every state row and on the total, to the
    printed rounding. A column restated in quintals or hectares still parses
    as numbers; the arithmetic is what refuses it. The numbered state rows
    must also sum to the total row.

Not yet published is not a failure:
    The next kharif's estimate is presented at SOPA's Soy Conclave in
    mid-October. Until then the page for that year carries no table and
    SOPA's own ``<!-- Debug: Found 0 posts -->`` comment; that is
    ``no_publication`` (an empty result), never a shape error. A page with
    neither the table nor that comment is a page we no longer understand.

Every response opens with an injected ``<style>``/``<script>`` preamble
before ``<!DOCTYPE html>``; the parser selects the table out of the
document rather than trusting its head. The site's wp-json records for the
page are empty — only the HTML carries the table.

Licence:
    SOPA's terms reserve "all rights not otherwise claimed", so the raw table
    is kept private until SOPA agrees in writing (``config.SOPA_PUBLISH``):
    it is not exported to the public ``data/history/`` and no page renders
    it. What may be shown publicly is derived — a year-on-year change, state
    shares — plus at most one attributed headline total
    (``config.SOPA_ATTRIBUTION``).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import (
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    SOPA_ALL_INDIA,
    SOPA_URL,
    SOPA_YEARS_READ,
)
from fetchers._backoff import retry_sleep
from pipeline.results import FetchResult, ScraperShapeError

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")

_HEADERS = {
    "User-Agent": (
        "Mirror-Market/1.0 "
        "(+https://github.com/philipbergman6-glitch/Mirror-Market)"
    ),
}

_TABLE_SELECTOR = "table.export-table"
_NO_POSTS = re.compile(r"<!--\s*Debug:\s*Found 0 posts\s*-->")
_COLUMN_HEADINGS = ("Sowing Area", "Expected Yield", "Estimated Production")
_TOTAL_LABEL = "Total"
# Yield is printed to the kg (±0.5 kg/ha on the area), area and production
# to three decimals (±0.0005 each); a little float noise on top.
_YIELD_ROUNDING = 0.5
_PRINTED_ROUNDING = 0.0005
_NOISE = 1e-6

COLUMNS = (
    "crop_year", "state", "fetched_date",
    "area_lakh_ha", "yield_kg_ha", "production_lakh_t",
)


@dataclass(frozen=True)
class StateRow:
    """One numbered state row, or the all-India total (``state`` is
    ``config.SOPA_ALL_INDIA``)."""

    state: str
    area_lakh_ha: float
    yield_kg_ha: float
    production_lakh_t: float


@dataclass(frozen=True)
class CropEstimate:
    """One kharif's estimate as the page shows it today. The total is last."""

    crop_year: int
    rows: tuple[StateRow, ...]


def _cells(tr) -> list[str]:
    return [cell.get_text(" ", strip=True) for cell in tr.find_all(["th", "td"])]


def _number(text: str, where: str) -> float:
    """A printed figure. Blank is a shape error — never a zero (invariant 2)."""
    cleaned = text.replace(",", "").strip()
    if not re.fullmatch(r"\d+(?:\.\d+)?", cleaned):
        raise ScraperShapeError(f"SOPA: {where}: expected a number, got {text!r}")
    return float(cleaned)


def _check_arithmetic(row: StateRow) -> None:
    """lakh ha × kg/ha / 1000 = lakh t, to the printed rounding."""
    implied = row.area_lakh_ha * row.yield_kg_ha / 1000
    tolerance = (
        row.area_lakh_ha * _YIELD_ROUNDING / 1000
        + _PRINTED_ROUNDING * row.yield_kg_ha / 1000
        + _PRINTED_ROUNDING
        + _NOISE
    )
    if abs(implied - row.production_lakh_t) > tolerance:
        raise ScraperShapeError(
            f"SOPA: {row.state}: area {row.area_lakh_ha} × yield {row.yield_kg_ha} / 1000 "
            f"= {implied:.3f} does not reproduce production {row.production_lakh_t} "
            f"(tolerance {tolerance:.3f}) — units are not lakh ha / kg/ha / lakh t"
        )


def parse_crop_table(html: str, year: int) -> CropEstimate | None:
    """The numbered state rows and the total of the ``Kharif <year>`` table.

    ``None`` when SOPA has not published that year (no table, and the page
    says it found no posts). Anything else that is not the expected table
    raises ``ScraperShapeError``.
    """
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.select(_TABLE_SELECTOR)
    if not tables:
        if _NO_POSTS.search(html):
            return None
        raise ScraperShapeError(
            f"SOPA: no {_TABLE_SELECTOR} on the page for {year} and no 'found 0 posts' marker"
        )
    if len(tables) != 1:
        raise ScraperShapeError(f"SOPA: {len(tables)} {_TABLE_SELECTOR} tables for {year}, expected 1")

    rows = [_cells(tr) for tr in tables[0].find_all("tr")]
    if len(rows) < 4:
        raise ScraperShapeError(f"SOPA: table for {year} has {len(rows)} rows")
    title, headings = rows[0], rows[1]
    expected_title = f"Kharif {year}"
    if len(title) != 3 or title[2] != expected_title:
        raise ScraperShapeError(f"SOPA: header {title} does not read {expected_title!r}")
    if tuple(headings[2:]) != _COLUMN_HEADINGS or any(headings[:2]):
        raise ScraperShapeError(f"SOPA: columns {headings} are not {list(_COLUMN_HEADINGS)}")

    states: list[StateRow] = []
    totals: list[StateRow] = []
    for cells in rows[2:]:
        if len(cells) != 5:
            raise ScraperShapeError(f"SOPA: {year}: row {cells} has {len(cells)} cells, expected 5")
        serial, name = cells[0], cells[1]
        if serial == _TOTAL_LABEL:
            label = SOPA_ALL_INDIA
        elif serial.isdigit():
            if not name:
                raise ScraperShapeError(f"SOPA: {year}: numbered row {serial} has no state name")
            label = name
        else:
            continue  # division / district detail beneath a state
        row = StateRow(
            label,
            _number(cells[2], f"{label} area"),
            _number(cells[3], f"{label} yield"),
            _number(cells[4], f"{label} production"),
        )
        _check_arithmetic(row)
        (totals if label == SOPA_ALL_INDIA else states).append(row)

    if len(totals) != 1:
        raise ScraperShapeError(f"SOPA: {year}: {len(totals)} total rows, expected 1")
    if len(states) < 3:
        raise ScraperShapeError(f"SOPA: {year}: only {len(states)} state rows")
    if len({row.state for row in states}) != len(states):
        raise ScraperShapeError(f"SOPA: {year}: a state is listed twice")
    total = totals[0]
    rounding = _PRINTED_ROUNDING * (len(states) + 1) + _NOISE
    for field, printed in (
        ("area_lakh_ha", total.area_lakh_ha),
        ("production_lakh_t", total.production_lakh_t),
    ):
        summed = sum(getattr(row, field) for row in states)
        if abs(summed - printed) > rounding:
            raise ScraperShapeError(
                f"SOPA: {year}: state {field} sum {summed:.3f} ≠ total {printed} "
                f"(tolerance {rounding:.3f})"
            )
    return CropEstimate(year, tuple(states) + (total,))


def _get(url: str, params: dict) -> str:
    """GET with retries. Raises requests.RequestException once they run out."""
    last_error = "no attempts made"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, headers=_HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.text
            last_error = f"HTTP {resp.status_code}"
        except requests.RequestException as exc:
            last_error = str(exc)
        logger.warning("SOPA: %s %s attempt %d failed: %s", url, params, attempt, last_error)
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)
    raise requests.RequestException(f"{url} {params} failed after {MAX_RETRIES} attempts ({last_error})")


def _ist_today() -> date:
    return datetime.now(_IST).date()


def _frame(estimate: CropEstimate, fetched: date) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "crop_year": estimate.crop_year,
                "state": row.state,
                "fetched_date": fetched.isoformat(),
                "area_lakh_ha": row.area_lakh_ha,
                "yield_kg_ha": row.yield_kg_ha,
                "production_lakh_t": row.production_lakh_t,
            }
            for row in estimate.rows
        ],
        columns=list(COLUMNS),
    )


def fetch_sopa_estimates(
    today: date | None = None, years: tuple[int, ...] | None = None,
) -> FetchResult:
    """Today's reading of each requested kharif, one frame per crop year.

    ``years`` defaults to the ``SOPA_YEARS_READ`` kharifs ending in the
    current IST year — the one being revised and the one about to be
    estimated. A local run can pass a longer tuple to backfill the archive
    (2007 →).

    ``empty`` when none of the years asked for has been published yet;
    ``failed`` (rows kept, ``partial``) when any year could not be read.
    """
    fetched = today or _ist_today()
    if years is None:
        years = tuple(range(fetched.year - SOPA_YEARS_READ + 1, fetched.year + 1))

    data: dict[str, pd.DataFrame] = {}
    unpublished: list[int] = []
    errors: list[str] = []
    for year in years:
        params = {"search_type": "search_by_year", "select_year": str(year)}
        try:
            estimate = parse_crop_table(_get(SOPA_URL, params), year)
        except (ScraperShapeError, requests.RequestException) as exc:
            errors.append(f"{year}: {exc}")
            continue
        if estimate is None:
            unpublished.append(year)
            continue
        data[f"kharif_{year}"] = _frame(estimate, fetched)
        total = estimate.rows[-1]
        logger.info(
            "SOPA: kharif %d reads %.3f lakh t on %.3f lakh ha (%d states) as of %s",
            year, total.production_lakh_t, total.area_lakh_ha, len(estimate.rows) - 1, fetched,
        )
    if unpublished:
        logger.info("SOPA: kharif %s not yet published", ", ".join(map(str, unpublished)))

    if errors:
        message = "SOPA: " + "; ".join(errors)
        logger.error("%s", message)
        return FetchResult.partial(data, message) if data else FetchResult.failed(message)
    if not data:
        return FetchResult.empty(
            f"SOPA: kharif {', '.join(map(str, unpublished))} not yet published"
        )
    return FetchResult.ok(data)
