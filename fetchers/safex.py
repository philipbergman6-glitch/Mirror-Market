"""
Layer 18 — SAFEX South Africa domestic soy prices via Grain SA.

Source: Grain SA SAFEX Feeds page (https://www.grainsa.co.za/pages/industry-reports/safex-feeds)
Data provider: BVG (credited on the page)
Prices: ZAR/MT — daily settlement prices for JSE Agricultural futures contracts

Why it matters:
    South Africa is the regional soy hub for sub-Saharan Africa.  The SAFEX
    soybean price in ZAR/MT signals domestic crush margins and regional demand
    from neighboring countries.  The SAFEX-CBOT basis (after FX conversion)
    reveals whether SA is a premium or discount market.

Contracts we track:
    SOYB — Soybean (ZAR/MT, multiple contract months)
    SUNS — Sunflower Seed (ZAR/MT)

Parser strategy:
    Find the first <table>, locate the header row that contains "Instrument"
    + "LastTradedPrice", validate every expected column is present, then
    extract rows. Mismatched structure raises ScraperShapeError so the
    pipeline records 'failed' instead of silently empty.

    "Nearest contract" = the contract with the highest volume traded today.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Sequence
from datetime import date

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import (
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    SAFEX_STATS_URL,
)
from fetchers._backoff import retry_sleep
from pipeline.results import ScraperShapeError

logger = logging.getLogger(__name__)

# Map Grain SA instrument codes → our commodity names
_INSTRUMENT_MAP = {
    "SOYB": "Soybean (SAFEX)",
    "SUNS": "Sunflower (SAFEX)",
}

_REQUIRED_COLUMNS: tuple[str, ...] = (
    "instrument",
    "contract",
    "lasttradedtime",
    "lasttradedprice",
    "volume",
)


def _fetch_page() -> str:
    """Download the Grain SA SAFEX feeds page HTML.

    Returns the body as a string on success, empty string on transport
    failure. Uses exponential backoff with jitter so retries don't all
    hit the upstream at the same instant.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                SAFEX_STATS_URL,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.text
            logger.warning("Grain SA SAFEX: HTTP %d (attempt %d)", resp.status_code, attempt)
        except requests.RequestException as exc:
            logger.warning("Grain SA SAFEX: Request failed (attempt %d): %s", attempt, exc)

        if attempt < MAX_RETRIES:
            retry_sleep(attempt)

    return ""


def _normalize_header(text: str) -> str:
    """Strip whitespace and lowercase a header cell."""
    return "".join(text.split()).lower()


def _find_settlement_table(soup: BeautifulSoup) -> tuple[list[str], list[list[str]]]:
    """Locate the settlement table and return (header_cells, body_rows).

    Raises ScraperShapeError if the table can't be found or its header
    lacks the required columns. Skips a preamble "Last Updated" row by
    looking for the first row whose normalized cells include "instrument".
    """
    tables = soup.find_all("table")
    if not tables:
        raise ScraperShapeError("Grain SA SAFEX: no <table> elements on page")

    for table in tables:
        rows = table.find_all("tr")
        header_idx = None
        header_cells: list[str] = []

        # Identify the header by the unambiguous "instrument" anchor; the
        # rest is validated explicitly so a renamed column raises with
        # the missing column's name in the error message.
        for i, row in enumerate(rows):
            cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
            normalized = [_normalize_header(c) for c in cells]
            if "instrument" in normalized and len(normalized) >= len(_REQUIRED_COLUMNS):
                header_idx = i
                header_cells = normalized
                break

        if header_idx is None:
            continue

        missing = [c for c in _REQUIRED_COLUMNS if c not in header_cells]
        if missing:
            raise ScraperShapeError(
                f"Grain SA SAFEX: header missing required columns {missing} — got {header_cells}"
            )

        body: list[list[str]] = []
        for row in rows[header_idx + 1:]:
            body_cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
            if len(body_cells) == len(header_cells):
                body.append(body_cells)

        if not body:
            raise ScraperShapeError("Grain SA SAFEX: header found but no data rows beneath it")

        return header_cells, body

    raise ScraperShapeError(
        "Grain SA SAFEX: no table contained the expected header (Instrument + LastTradedPrice)"
    )


def _to_float(val: str) -> float | None:
    """Parse a numeric cell, returning None for blanks or non-numerics."""
    if not val or val in ("-", "—"):
        return None
    try:
        return float(val.replace(",", ""))
    except ValueError:
        return None


def _parse_safex_table(html: str) -> dict[str, pd.DataFrame]:
    """Parse the Grain SA SAFEX HTML page into per-commodity DataFrames.

    For each instrument code in ``_INSTRUMENT_MAP`` we pick the contract
    with the highest volume — that's the most actively traded (nearest)
    contract — and emit a single-row DataFrame.

    Returns
    -------
    dict
        ``{commodity_name: DataFrame}`` with columns Date, Close, Volume, Unit.
        Empty dict if none of our tracked instruments appear on the page
        (that's a normal "no rows for this filter" outcome, not a shape error).

    Raises
    ------
    ScraperShapeError
        If the page no longer exposes a recognisable settlement table.
    """
    soup = BeautifulSoup(html, "html.parser")
    header, rows = _find_settlement_table(soup)

    col = {name: header.index(name) for name in _REQUIRED_COLUMNS}

    results: dict[str, pd.DataFrame] = {}
    today_str = str(date.today())

    for instrument_code, commodity_name in _INSTRUMENT_MAP.items():
        # Collect all contracts for this instrument with a usable price.
        candidates: list[tuple[float | None, float, float | None, str]] = []
        for cells in rows:
            if cells[col["instrument"]].upper() != instrument_code:
                continue
            price = _to_float(cells[col["lasttradedprice"]])
            if price is None or price <= 0:
                continue
            volume = _to_float(cells[col["volume"]])
            traded_at = cells[col["lasttradedtime"]]
            candidates.append((volume, price, volume, traded_at))

        if not candidates:
            logger.debug("Grain SA SAFEX: no rows for %s (%s)", commodity_name, instrument_code)
            continue

        # Pick the row with the highest volume; ties broken by first occurrence.
        # None-volume sorts as -inf so a row with reported volume always wins.
        candidates.sort(key=lambda t: t[0] if t[0] is not None else float("-inf"), reverse=True)
        _, close, volume, traded_at = candidates[0]

        trade_date = today_str
        if traded_at:
            with contextlib.suppress(ValueError, TypeError):
                trade_date = str(pd.to_datetime(traded_at).date())

        results[commodity_name] = pd.DataFrame([{
            "Date": trade_date,
            "Close": close,
            "Volume": volume,
            "Unit": "ZAR/MT",
        }])
        logger.info(
            "Grain SA SAFEX %s: Close = %.1f ZAR/MT (vol=%s, date=%s)",
            commodity_name, close, volume, trade_date,
        )

    return results


def fetch_safex() -> dict[str, pd.DataFrame]:
    """Fetch SAFEX South Africa soy prices from Grain SA.

    Returns
    -------
    dict
        ``{commodity_name: DataFrame}`` (Date, Close, Volume, Unit).
        Returns ``{}`` if the page can't be downloaded or the upstream
        structure no longer matches the expected schema.
    """
    logger.info("Fetching SAFEX prices from Grain SA ...")
    html = _fetch_page()

    if not html:
        logger.warning(
            "Grain SA SAFEX: Could not download page. "
            "Returning empty — pipeline continues without SAFEX data."
        )
        return {}

    try:
        return _parse_safex_table(html)
    except ScraperShapeError as exc:
        logger.error("Grain SA SAFEX: page structure changed — %s", exc)
        return {}


# Re-export for tests
__all__: Sequence[str] = ("_parse_safex_table", "fetch_safex")


# ── Quick self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_safex()
    if not data:
        logger.info("SAFEX: No data returned.")
    else:
        for name, df in data.items():
            logger.info(
                "%s: Close = %.1f ZAR/MT, Volume = %s, Date = %s",
                name,
                df["Close"].iloc[0],
                df["Volume"].iloc[0],
                df["Date"].iloc[0],
            )
