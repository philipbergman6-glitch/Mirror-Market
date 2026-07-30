"""
Layer 17 — CEPEA/ESALQ Brazil domestic soy spot price.

CEPEA (Centro de Estudos Avançados em Economia Aplicada) at ESALQ/USP
is Brazil's leading agricultural commodity price research center. The URL
this fetcher hits returns the standard CEPEA/ESALQ Soybean Indicator — a
daily farm-gate reference price for **Paraná state** (the country's
benchmark soy origin), quoted in BRL per 60kg bag.

This is NOT the Paranaguá port FOB/CIF premium. CEPEA publishes a separate
Paranaguá indicator with port-specific pricing; a future fetcher (AgRural
or CEPEA Paranaguá) will add that as a parallel layer.

Why it matters:
    Brazilian farm-gate prices drive planting decisions. When BRL weakens,
    Brazilian farmers receive MORE BRL for their soy even at the same USD
    CBOT price — making them more competitive exporters. The
    farm-gate-vs-CBOT spread is computed at the analysis layer as the
    "Brazil basis" indicator.

Unit conversion:
    CEPEA publishes BRL / 60kg bag → we store BRL/MT.
    BRL/MT = (BRL per bag) / 60 × 1000

USD/MT conversion happens at the analysis layer using the BRL/USD rate from
the currencies table — NOT in this fetcher (display-layer-only conversion rule).

Parser strategy:
    Use BeautifulSoup to locate a <table> whose header contains a date-like
    column ("Date" / "Data") and a price-like column ("Value" / "Valor"
    / "Price" / "Preço"). Validate the shape, then parse rows with explicit
    Brazilian-format number handling. Missing structure raises
    ScraperShapeError so a silent zero-row result can't be confused with
    "the upstream JavaScript page failed to render".
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import CEPEA_SOYBEAN_URL, MAX_RETRIES, REQUEST_TIMEOUT
from fetchers._backoff import retry_sleep
from pipeline.results import ScraperShapeError

logger = logging.getLogger(__name__)

_DATE_HEADER_KEYWORDS = ("date", "data", "dia")
_PRICE_HEADER_KEYWORDS = ("value", "valor", "price", "preco", "preço")
_DATE_FORMATS: tuple[str, ...] = ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y")
_DECIMAL_RE = re.compile(r"-?\d[\d.,]*")


def _parse_brl_price(val: str) -> float | None:
    """Convert a Brazilian-format price string to float.

    Brazilian: period thousands, comma decimal — ``"1.234,56" → 1234.56``.
    Also handles US-style ``"1234.56"`` and bare ``"123,45"``.
    Returns None on any unparseable value (caller filters those out).
    """
    if not val:
        return None
    s = str(val).strip()
    if s in ("", "-", "—", "N/A", "nan"):
        return None
    match = _DECIMAL_RE.search(s)
    if not match:
        return None
    s = match.group(0)
    try:
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            s = s.replace(",", ".")
        return float(s)
    except ValueError:
        return None


def _fetch_cepea_page() -> str:
    """Download the CEPEA soybean indicator page HTML.

    Exponential backoff with jitter on transient failures. Returns the
    raw HTML body on success, empty string on full retry exhaustion.
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
                CEPEA_SOYBEAN_URL,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.text
            logger.warning("CEPEA: HTTP %d (attempt %d)", resp.status_code, attempt)
        except requests.RequestException as exc:
            logger.warning("CEPEA: Request failed (attempt %d): %s", attempt, exc)

        if attempt < MAX_RETRIES:
            retry_sleep(attempt)

    return ""


def _normalize(text: str) -> str:
    return text.strip().lower()


def _find_price_table(soup: BeautifulSoup) -> tuple[int, int, list[list[str]]]:
    """Locate the price table and return (date_idx, price_idx, body_rows).

    Walks all <table> elements; for each, scans rows to find a header
    that contains both a date-like keyword and a price-like keyword.
    Raises ScraperShapeError when nothing matches — that's the alert
    signal that the upstream page has changed shape.
    """
    tables = soup.find_all("table")
    if not tables:
        raise ScraperShapeError("CEPEA: no <table> elements on page (JavaScript-rendered?)")

    for table in tables:
        rows = table.find_all("tr")
        if not rows:
            continue

        for header_idx, row in enumerate(rows):
            cells = [_normalize(c.get_text(" ", strip=True)) for c in row.find_all(["th", "td"])]
            if not cells:
                continue

            date_col = next(
                (i for i, c in enumerate(cells)
                 if any(k in c for k in _DATE_HEADER_KEYWORDS)),
                None,
            )
            price_col = next(
                (i for i, c in enumerate(cells)
                 if any(k in c for k in _PRICE_HEADER_KEYWORDS)),
                None,
            )

            if date_col is None or price_col is None:
                continue

            body: list[list[str]] = []
            for data_row in rows[header_idx + 1:]:
                body_cells = [c.get_text(" ", strip=True) for c in data_row.find_all(["th", "td"])]
                # Skip blank/separator rows but tolerate rows that have an
                # extra trailing column (e.g. variation %).
                if len(body_cells) > max(date_col, price_col):
                    body.append(body_cells)

            if not body:
                raise ScraperShapeError(
                    "CEPEA: header found but no data rows beneath it — page may be empty"
                )

            return date_col, price_col, body

    raise ScraperShapeError(
        "CEPEA: no table contained a recognisable date+price header — "
        "page may use JavaScript rendering or has been restructured"
    )


def _parse_cepea_tables(html: str) -> pd.DataFrame:
    """Extract the price table from CEPEA HTML.

    Returns a DataFrame with columns Date, price_brl_mt, Unit (BRL/MT)
    sorted newest-first. Raises ScraperShapeError if the page no longer
    has a recognisable price table.
    """
    if not html:
        return pd.DataFrame()

    soup = BeautifulSoup(html, "html.parser")
    date_idx, price_idx, body = _find_price_table(soup)

    rows: list[dict[str, object]] = []
    for cells in body:
        date_str = cells[date_idx].strip()
        parsed_date = None
        for fmt in _DATE_FORMATS:
            try:
                parsed_date = datetime.strptime(date_str, fmt).date()
                break
            except ValueError:
                continue
        if parsed_date is None:
            continue

        price_per_bag = _parse_brl_price(cells[price_idx])
        if price_per_bag is None or price_per_bag <= 0:
            continue

        price_brl_mt = (price_per_bag / 60.0) * 1000.0
        rows.append({
            "Date": str(parsed_date),
            "price_brl_mt": round(price_brl_mt, 2),
            "Unit": "BRL/MT",
        })

    if not rows:
        # Header existed, body existed, but every row failed to parse —
        # treat as shape change rather than success-with-empty.
        raise ScraperShapeError(
            "CEPEA: located price table but every row failed to parse (date+price)"
        )

    result = pd.DataFrame(rows).drop_duplicates("Date").sort_values("Date", ascending=False)
    logger.info(
        "CEPEA: Parsed %d price rows, latest = %.2f BRL/MT (%s)",
        len(result), result["price_brl_mt"].iloc[0], result["Date"].iloc[0],
    )
    return result


def fetch_cepea() -> FetchResult:
    """Fetch CEPEA/ESALQ Brazil soybean domestic price.

    Returns ``FetchResult.ok({"Soybean (CEPEA)": df})`` on success. The
    indicator publishes every business day, so a download failure, structure
    change, or empty parse is always ``FetchResult.failed``.
    """
    logger.info("Fetching CEPEA/ESALQ Brazil soybean price ...")
    html = _fetch_cepea_page()

    if not html:
        logger.warning("CEPEA: Could not download page.")
        return FetchResult.failed("CEPEA: page download failed")

    try:
        df = _parse_cepea_tables(html)
    except ScraperShapeError as exc:
        logger.error("CEPEA: page structure changed — %s", exc)
        return FetchResult.failed(str(exc))

    if df.empty:
        logger.warning("CEPEA: Parsed empty DataFrame — check page structure.")
        return FetchResult.failed("CEPEA: parser produced zero rows")

    return FetchResult.ok({"Soybean (CEPEA)": df})


__all__: Sequence[str] = ("_parse_cepea_tables", "fetch_cepea")


# ── Quick self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    result = fetch_cepea()
    if not result.has_rows:
        logger.info("CEPEA: %s — %s", result.status, result.error)
    else:
        for name, df in result.data.items():
            logger.info(
                "%s: %d rows\n%s",
                name, len(df), df.head(5).to_string(index=False),
            )
