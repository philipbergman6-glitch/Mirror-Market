"""
Layer 18 — SAFEX South Africa domestic soy prices via Grain SA.

Source: Grain SA SAFEX Feeds page (https://www.grainsa.co.za/pages/industry-reports/safex-feeds)
Data provider: BVG (credited on the page)
Prices: ZAR/MT — the *last traded price* for JSE Agricultural futures contracts.

    NOT the JSE official mark-to-market (MTM) settlement. The Grain SA table
    has no settlement column at all — its columns are Instrument, Contract,
    LastTradedTime, LastTradedPrice, Difference, HighPrice, LowPrice, Volume,
    OpenInterest. The JSE's own MTM file sits behind its Client Portal and its
    terms bar use "for commercial gain" without written permission, so the MTM
    number is not available to this project (#157). Every docstring and label
    downstream must therefore say "last traded", never "settlement".

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

    Contract selection = **the most-liquid contract that session**, i.e. the
    largest Volume, ties broken by nearest expiry. Nearest-expiry alone was
    wrong twice over (#157):

      1. It rides the contract into its own death. On 2026-08-11 the nearest
         contract AUG26 traded 163 lots while DEC26 traded 433 and SEP26 271
         — liquidity had already rolled away, so the SA price leg was reading
         the thinnest board on the page.
      2. It cannot see a carried-forward price. LastTradedTime is a *row*
         stamp, not a trade time: on 2026-08-11 OCT26 carried the date
         2026-08-11 with Volume 0, Difference 0.00 and High/Low 0.00 — it did
         not trade at all, yet its stale price was stamped with today's date.
         Requiring Volume > 0 excludes those rows structurally, which the
         date-honesty guard below cannot do on its own.

    The contract label is kept alongside the price so a roll is visible
    downstream. Rows without a parseable trade *date* hard-fail — a frozen
    page must never be stamped with today's date.

    On a non-trading day the page stale-serves the previous session's rows,
    volumes included (verified across 2026-08-02 and 2026-08-08). Those rows
    carry their own real date, so they land on the date they belong to and
    dedupe against what is already stored — never under today's.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import (
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    SAFEX_STATS_URL,
)
from fetchers._backoff import retry_sleep
from pipeline.results import FetchResult, ScraperShapeError

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

_MONTH_CODES = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# A trade date must carry an actual calendar date — either ISO
# (2026-05-11) or day-first (11/05/2026). A bare time like "14:32"
# must NOT qualify: pandas would silently fill in today's date.
_ISO_DATE_PATTERN = re.compile(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}")
_DAYFIRST_DATE_PATTERN = re.compile(r"\d{1,2}[-/]\d{1,2}[-/]\d{2,4}")


def _contract_sort_key(label: str) -> tuple[int, int] | None:
    """Parse an MMMYY contract code ('AUG26') into (year, month) for
    nearest-expiry ordering. Returns None if the label doesn't match."""
    match = re.fullmatch(r"([A-Z]{3})(\d{2})", label.strip().upper())
    if not match:
        return None
    month = _MONTH_CODES.get(match.group(1))
    if month is None:
        return None
    return 2000 + int(match.group(2)), month


def _parse_trade_date(text: str) -> str | None:
    """Extract an ISO date from a LastTradedTime cell, or None.

    Requires an explicit calendar date in the cell. ISO dates parse
    year-first; ambiguous numeric dates are read day-first (JSE
    convention), so 03/07/2026 is July 3rd. dayfirst=True must not be
    applied to ISO strings — pandas would read 2026-05-11 as Nov 5th.
    """
    if not text:
        return None
    try:
        if _ISO_DATE_PATTERN.search(text):
            return str(pd.to_datetime(text, yearfirst=True).date())
        if _DAYFIRST_DATE_PATTERN.search(text):
            return str(pd.to_datetime(text, dayfirst=True).date())
    except (ValueError, TypeError):
        return None
    return None


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


def _find_price_table(soup: BeautifulSoup) -> tuple[list[str], list[list[str]]]:
    """Locate the price table and return (header_cells, body_rows).

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

    For each instrument code in ``_INSTRUMENT_MAP`` we pick the most-liquid
    contract that actually traded — largest Volume, ties broken by nearest
    expiry — and emit a single-row DataFrame carrying the contract label.

    Returns
    -------
    dict
        ``{commodity_name: DataFrame}`` with columns Date, Close, Volume,
        Contract, Unit. Empty dict if none of our tracked instruments
        appear on the page, or if none of their contracts traded (both are
        normal "nothing to report" outcomes, not shape errors).

    Raises
    ------
    ScraperShapeError
        If the page no longer exposes a recognisable price table,
        if a tracked row's contract code stops parsing as MMMYY (nearest-
        contract selection would be meaningless), or if the selected row
        carries no parseable trade date (a frozen page must not be stored
        under today's date).
    """
    soup = BeautifulSoup(html, "html.parser")
    header, rows = _find_price_table(soup)

    col = {name: header.index(name) for name in _REQUIRED_COLUMNS}

    results: dict[str, pd.DataFrame] = {}

    for instrument_code, commodity_name in _INSTRUMENT_MAP.items():
        # Collect all contracts for this instrument with a usable price.
        candidates: list[tuple[tuple[int, int], str, float, float, str]] = []
        untraded = 0
        for cells in rows:
            if cells[col["instrument"]].upper() != instrument_code:
                continue
            price = _to_float(cells[col["lasttradedprice"]])
            if price is None or price <= 0:
                continue
            contract = cells[col["contract"]].strip().upper()
            sort_key = _contract_sort_key(contract)
            if sort_key is None:
                raise ScraperShapeError(
                    f"Grain SA SAFEX: unparseable contract code {contract!r} for "
                    f"{instrument_code} — cannot order contracts by expiry"
                )
            # A zero/absent volume means this contract did not trade in the
            # session the page is showing, so its price is a carry-forward
            # from some earlier session that the page has re-stamped with the
            # current date. Storing it would fabricate a print.
            volume = _to_float(cells[col["volume"]])
            if volume is None or volume <= 0:
                untraded += 1
                continue
            candidates.append((sort_key, contract, price, volume, cells[col["lasttradedtime"]]))

        if not candidates:
            # Not a shape error: every contract on the board can legitimately
            # go a session without trading. Empty here reaches the pipeline as
            # empty-success (empty_fails=False for this layer), not as an
            # outage — the same grading a JSE holiday gets.
            logger.warning(
                "Grain SA SAFEX: no traded contract for %s (%s) — %d contract(s) "
                "present but all with zero volume; storing nothing rather than a "
                "carried-forward price",
                commodity_name, instrument_code, untraded,
            )
            continue

        # Most liquid first; ties broken by nearest expiry.
        candidates.sort(key=lambda t: (-t[3], t[0]))
        _, contract, close, volume, traded_at = candidates[0]

        trade_date = _parse_trade_date(traded_at)
        if trade_date is None:
            raise ScraperShapeError(
                f"Grain SA SAFEX: no parseable trade date for {instrument_code} "
                f"{contract} (LastTradedTime={traded_at!r}) — refusing to stamp today"
            )

        results[commodity_name] = pd.DataFrame([{
            "Date": trade_date,
            "Close": close,
            "Volume": volume,
            "Contract": contract,
            "Unit": "ZAR/MT",
        }])
        logger.info(
            "Grain SA SAFEX %s: Close = %.1f ZAR/MT (%s, vol=%s, date=%s)",
            commodity_name, close, contract, volume, trade_date,
        )

    return results


def fetch_safex() -> FetchResult:
    """Fetch SAFEX South Africa soy prices from Grain SA.

    Returns
    -------
    FetchResult
        ``ok`` with ``{commodity_name: DataFrame}`` (Date, Close, Volume,
        Contract, Unit) on success; ``failed`` when the page can't be downloaded or
        no longer matches the expected structure; ``empty`` when the page
        parsed cleanly but carried no rows.
    """
    logger.info("Fetching SAFEX prices from Grain SA ...")
    html = _fetch_page()

    if not html:
        logger.warning("Grain SA SAFEX: Could not download page.")
        return FetchResult.failed("SAFEX: page download failed")

    try:
        data = _parse_safex_table(html)
    except ScraperShapeError as exc:
        logger.error("Grain SA SAFEX: page structure changed — %s", exc)
        return FetchResult.failed(str(exc))

    if not any(not df.empty for df in data.values()):
        return FetchResult.empty("SAFEX: page parsed but no rows")
    return FetchResult.ok(data)


# Re-export for tests
__all__: Sequence[str] = ("_parse_safex_table", "fetch_safex")


# ── Quick self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    result = fetch_safex()
    if not result.has_rows:
        logger.info("SAFEX: %s — %s", result.status, result.error)
    else:
        for name, df in result.data.items():
            logger.info(
                "%s: Close = %.1f ZAR/MT, Volume = %s, Date = %s",
                name,
                df["Close"].iloc[0],
                df["Volume"].iloc[0],
                df["Date"].iloc[0],
            )
