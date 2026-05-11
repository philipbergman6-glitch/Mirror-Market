"""
Layer 19 — AgRural Paranaguá FOB soy price.

AgRural publishes a daily Paranaguá port soybean buy price (BRL per 60-kg
bag) on its public ``precossojaemilho`` page. This is the trade-convention
"Brazil basis" benchmark: Paranaguá FOB minus CBOT, in USD/MT after the
display-layer conversion in ``analysis.spreads.compute_brazil_basis``.

Paranaguá vs Paraná farm-gate (CEPEA):
    CEPEA Paraná is the farm-gate indicator (Layer 17). The Paranaguá port
    quote runs ~$10–$30/MT above farm-gate to cover inland freight, port
    logistics, and crusher margin. The wedge between the two is itself a
    tradable signal — both layers are kept live so the briefing/dashboard
    can render both lines on the same chart.

Page shape (verified 2026-05-11):
    Two ``<table>`` elements — ``SOJA`` and ``MILHO``. Each opens with a
    banner row whose first cell names the table (``SOJA``/``MILHO``) and
    whose second cell carries the indicator date in ``%d-%b-%y`` format
    (e.g. ``8-May-26``). Row 2 is the column header
    (``Estado | Praça | Compra | Variação hoje | 1 semana | 1 mês``).
    Row 3 onwards is one location per row; the first row for each state
    carries the state code in column 0 (``PR | Paranaguá | 129,50 | ...``),
    later rows for the same state drop that cell (one fewer column).

    We key on:
        1. Banner row first cell == "SOJA" (case-insensitive) — disambiguates
           the soy table from the corn table.
        2. A data row whose first non-state cell text contains "paranagua"
           (case- and accent-insensitive).
        3. The price column is named "Compra" (header keyword: ``compra``).

Redistribution:
    The page footer reads "Fica vedada, sob qualquer forma, a reprodução
    total ou parcial das tabelas." We honour this by storing raw BRL/MT
    locally (for our own analysis) and exposing only the derived USD/MT
    basis on the public GitHub Pages dashboard. See plan Phase 2 §
    "User decisions" and the display constraint enforced in
    ``scripts/generate_html.py``.

Unit conversion:
    AgRural publishes BRL per 60 kg bag → we store BRL/MT.
    BRL/MT = (BRL per bag) / 60 × 1000

    The fetcher never touches USD. USD/MT conversion happens at the analysis
    layer using the BRL/USD rate from the currencies table.

Parser strategy:
    Identical philosophy to ``fetchers/cepea.py``: hard-fail with
    ``ScraperShapeError`` on any structural mismatch (no soy table, no
    Paranaguá row, unparseable header date). Transport failures return
    an empty dict so the pipeline degrades gracefully on a 502.
"""

from __future__ import annotations

import logging
import unicodedata
from collections.abc import Sequence
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import AGRURAL_URL, MAX_RETRIES, REQUEST_TIMEOUT
from fetchers._backoff import retry_sleep
from fetchers.cepea import _parse_brl_price
from pipeline.results import ScraperShapeError

logger = logging.getLogger(__name__)

_AGRURAL_COMMODITY = "Soybean (AgRural Paranaguá FOB)"

_SOY_BANNER_KEYWORD = "soja"
_LOCATION_KEYWORDS = ("praça", "praca", "local")
_PRICE_HEADER_KEYWORDS = ("compra", "valor", "preço", "preco", "price")
_PARANAGUA_KEYWORDS = ("paranagua",)  # accent-stripped match target
_DATE_FORMATS: tuple[str, ...] = ("%d-%b-%y", "%d-%b-%Y", "%d/%m/%Y", "%Y-%m-%d")


def _strip_accents(text: str) -> str:
    """Return ``text`` lowercased with combining diacritics removed.

    ``"Paranaguá"`` → ``"paranagua"`` so the location match is robust to
    encoding/typography drift on the upstream page.
    """
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch)).lower()


def _fetch_agrural_page() -> str:
    """Download the AgRural soy/corn price page HTML.

    Returns the raw HTML body on success, empty string on full retry
    exhaustion. Exponential backoff with jitter — same policy as every
    other scraper in this package.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9,pt;q=0.8",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                AGRURAL_URL,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.text
            logger.warning("AgRural: HTTP %d (attempt %d)", resp.status_code, attempt)
        except requests.RequestException as exc:
            logger.warning("AgRural: Request failed (attempt %d): %s", attempt, exc)

        if attempt < MAX_RETRIES:
            retry_sleep(attempt)

    return ""


def _row_cells(row) -> list[str]:
    return [c.get_text(" ", strip=True) for c in row.find_all(["th", "td"])]


def _find_soy_table(soup: BeautifulSoup):
    """Return the first ``<table>`` whose banner row reads ``SOJA``.

    Raises ScraperShapeError when no soy table is found — the corn table
    has the same shape, so a missing soy table is a structural change we
    want to alert on rather than silently return empty.
    """
    tables = soup.find_all("table")
    if not tables:
        raise ScraperShapeError("AgRural: no <table> elements on page (JavaScript-rendered?)")

    for table in tables:
        rows = table.find_all("tr")
        if not rows:
            continue
        first_cells = _row_cells(rows[0])
        if first_cells and _strip_accents(first_cells[0]) == _SOY_BANNER_KEYWORD:
            return table

    raise ScraperShapeError(
        "AgRural: no <table> opened with a 'SOJA' banner row — "
        "page may have been restructured (table labels renamed or JS-rendered)"
    )


def _parse_banner_date(banner_cells: list[str]) -> str:
    """Pull the indicator date from the SOJA banner row.

    The banner row is ``['SOJA', '8-May-26']``; we accept any date-like
    cell after the first that matches one of the known formats. Raises
    ScraperShapeError when no cell parses.
    """
    candidates = [c.strip() for c in banner_cells[1:] if c and c.strip()]
    for candidate in candidates:
        for fmt in _DATE_FORMATS:
            try:
                return str(datetime.strptime(candidate, fmt).date())
            except ValueError:
                continue
    raise ScraperShapeError(
        "AgRural: SOJA banner row had no date-like cell matching "
        f"{_DATE_FORMATS} — got {banner_cells!r}"
    )


def _find_header_columns(header_cells: list[str]) -> tuple[int, int]:
    """Return ``(location_idx, price_idx)`` from the column-header row.

    Looks for a ``Praça``-style location header and a ``Compra``-style
    price header. Raises ScraperShapeError when either is missing.
    """
    normalized = [_strip_accents(c) for c in header_cells]

    location_idx = next(
        (i for i, c in enumerate(normalized)
         if any(k in c for k in _LOCATION_KEYWORDS)),
        None,
    )
    price_idx = next(
        (i for i, c in enumerate(normalized)
         if any(k in c for k in _PRICE_HEADER_KEYWORDS)),
        None,
    )

    if location_idx is None or price_idx is None:
        raise ScraperShapeError(
            "AgRural: SOJA header row missing required columns. "
            f"Expected praça/compra-style headers, got {header_cells!r}"
        )

    return location_idx, price_idx


def _find_paranagua_price(
    body_rows: list[list[str]],
    location_idx: int,
    price_idx: int,
) -> str:
    """Return the raw price-cell text for the Paranaguá row.

    Walks the body looking for a row whose ``Praça`` cell matches
    ``paranagua`` (accent-stripped). Body rows for non-first state entries
    drop the first column, so the location cell may sit at either
    ``location_idx`` or ``location_idx - 1`` — we check the row's full
    text first to disambiguate.

    Raises ScraperShapeError when no Paranaguá row is present.
    """
    for cells in body_rows:
        if not cells:
            continue

        joined = " | ".join(_strip_accents(c) for c in cells)
        if not any(k in joined for k in _PARANAGUA_KEYWORDS):
            continue

        # Paranaguá is the first PR row in the live page — it carries the
        # state code in column 0, so all column indices match the header.
        if len(cells) > max(location_idx, price_idx):
            location_cell = _strip_accents(cells[location_idx])
            if any(k in location_cell for k in _PARANAGUA_KEYWORDS):
                return cells[price_idx]

        # Fallback: handle the (unobserved but possible) case where
        # Paranaguá ever appears as a non-first state row — cells shift
        # left by one. Recompute the price column relative to the shift.
        shifted_price = price_idx - 1
        shifted_location = location_idx - 1
        if 0 <= shifted_location < len(cells) and 0 <= shifted_price < len(cells):
            location_cell = _strip_accents(cells[shifted_location])
            if any(k in location_cell for k in _PARANAGUA_KEYWORDS):
                return cells[shifted_price]

    raise ScraperShapeError(
        "AgRural: SOJA table found but no Paranaguá row inside it — "
        "AgRural may have dropped the port quote (alert: re-check page)"
    )


def _parse_agrural_table(html: str) -> pd.DataFrame:
    """Extract the AgRural Paranaguá soy price from the page HTML.

    Returns a single-row DataFrame with columns Date, price_brl_mt, Unit.
    Raises ScraperShapeError on any structural mismatch.
    """
    if not html:
        return pd.DataFrame()

    soup = BeautifulSoup(html, "html.parser")
    table = _find_soy_table(soup)
    rows = table.find_all("tr")

    banner = _row_cells(rows[0])
    indicator_date = _parse_banner_date(banner)

    if len(rows) < 3:
        raise ScraperShapeError(
            "AgRural: SOJA table has fewer than 3 rows (banner + header + "
            f"at least one data row expected), got {len(rows)}"
        )

    header_cells = _row_cells(rows[1])
    location_idx, price_idx = _find_header_columns(header_cells)

    body_rows = [_row_cells(r) for r in rows[2:]]
    body_rows = [c for c in body_rows if c]  # drop genuinely blank rows
    if not body_rows:
        raise ScraperShapeError(
            "AgRural: SOJA table header found but no data rows beneath it"
        )

    price_raw = _find_paranagua_price(body_rows, location_idx, price_idx)
    price_per_bag = _parse_brl_price(price_raw)

    if price_per_bag is None or price_per_bag <= 0:
        raise ScraperShapeError(
            f"AgRural: located Paranaguá row but price cell unparseable: {price_raw!r}"
        )

    price_brl_mt = round((price_per_bag / 60.0) * 1000.0, 2)
    df = pd.DataFrame([{
        "Date": indicator_date,
        "price_brl_mt": price_brl_mt,
        "Unit": "BRL/MT",
    }])

    logger.info(
        "AgRural: Parsed Paranaguá FOB = %.2f BRL/MT (%.2f BRL/saca, %s)",
        price_brl_mt, price_per_bag, indicator_date,
    )
    return df


def fetch_agrural() -> dict[str, pd.DataFrame]:
    """Fetch AgRural Paranaguá FOB soybean price.

    Returns ``{"Soybean (AgRural Paranaguá FOB)": df}`` on success,
    ``{}`` on transport failure or upstream structure change.
    """
    logger.info("Fetching AgRural Paranaguá FOB soy quote ...")
    html = _fetch_agrural_page()

    if not html:
        logger.warning(
            "AgRural: Could not download page. "
            "Returning empty — pipeline continues without AgRural data."
        )
        return {}

    try:
        df = _parse_agrural_table(html)
    except ScraperShapeError as exc:
        logger.error("AgRural: page structure changed — %s", exc)
        return {}

    if df.empty:
        logger.warning("AgRural: Parsed empty DataFrame — check page structure.")
        return {}

    return {_AGRURAL_COMMODITY: df}


__all__: Sequence[str] = ("_parse_agrural_table", "fetch_agrural")


# ── Quick self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_agrural()
    if not data:
        logger.info("AgRural: No data returned. Check URL or page structure.")
    else:
        for name, df in data.items():
            logger.info(
                "%s: %d rows\n%s",
                name, len(df), df.head(5).to_string(index=False),
            )
