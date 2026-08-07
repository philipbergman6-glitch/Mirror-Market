"""
Layer 17 — CEPEA/ESALQ Brazil soy indicators via Notícias Agrícolas.

cepea.org.br put its own site behind a Cloudflare Turnstile challenge in
2026-05, killing the direct Layer 17 scraper. Notícias Agrícolas
(noticiasagricolas.com.br) republishes the same CEPEA/ESALQ indicator
values server-rendered — plain HTML, no JavaScript challenge — including
roughly the ten most recent sessions per page and full history via a
``/YYYY-MM-DD`` URL suffix.

Indicators fetched (see ``NOTICIAS_AGRICOLAS_URLS`` in config.py):
    * CEPEA/ESALQ Paraná      — farm-gate reference ("Soybean (CEPEA)")
    * ESALQ/B3 Paranaguá      — port-side indicator, cross-checks AgRural

Units: the site quotes BRL per 60 kg sack; we store BRL/MT
(``price × 1000 / 60``), matching the historical CEPEA rows already in
``brazil_spot_prices``.

Parser strategy: hard-fail with ``ScraperShapeError`` on structural
mismatch (no ``table.cot-fisicas``, missing CEPEA/ESALQ source marker,
no "soja" in the page title or a table's nearest heading, prices outside
the 60-300 R$/saca plausibility band, unparseable dates). Transport
failures return ``FetchResult.failed``.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import MAX_RETRIES, NOTICIAS_AGRICOLAS_URLS, REQUEST_TIMEOUT
from fetchers._backoff import retry_sleep
from fetchers.cepea import _parse_brl_price
from pipeline.results import FetchResult, ScraperShapeError

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
}

# The quote tables carry this CSS class; each holds one session row.
_TABLE_CLASS = "cot-fisicas"
# Sanity anchor — the page must attribute the data to CEPEA/ESALQ before
# we trust any number on it.
_SOURCE_MARKERS = ("cepea", "esalq")
# Commodity anchor — the corn ESALQ page carries the same CEPEA/ESALQ
# markers and cot-fisicas tables, so a redirect there would parse cleanly
# without a "soja" check on the title/headings.
_COMMODITY_MARKER = "soja"
_HEADING_TAGS = ("h1", "h2", "h3", "h4", "caption")

_SACK_KG = 60.0
# Plausibility band, BRL per 60 kg sack. Soy has traded ~70–180 for a
# decade; corn sits ~50–90 (why the band alone can't catch a corn
# redirect) and a silent switch to R$/MT (~2300) or R$/kg (~2.3) lands
# far outside it.
_SACK_MIN_BRL = 60.0
_SACK_MAX_BRL = 300.0


def _fetch_page(url: str) -> str:
    """Download one indicator page. Returns '' after retries are exhausted."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                url, headers=_HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True
            )
            if resp.status_code == 200:
                return resp.text
            logger.warning("Notícias Agrícolas: HTTP %d for %s", resp.status_code, url)
        except requests.RequestException as exc:
            logger.warning(
                "Notícias Agrícolas attempt %d failed for %s: %s", attempt, url, exc
            )
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)
    return ""


def _parse_indicator_page(html: str) -> pd.DataFrame:
    """Parse every ``table.cot-fisicas`` session row on an indicator page.

    Each table holds one session: date (DD/MM/YYYY), value in BRL per
    60 kg sack (Brazilian decimal comma), percent change. Returns a
    DataFrame with ``Date`` (ISO), ``price_brl_mt``, ``Unit`` — the shape
    ``clean_brazil_spot``/``save_brazil_spot`` expect. Raises
    ScraperShapeError on structural mismatch.
    """
    soup = BeautifulSoup(html, "html.parser")

    page_text = soup.get_text(" ", strip=True).lower()
    if not any(marker in page_text for marker in _SOURCE_MARKERS):
        raise ScraperShapeError(
            "Notícias Agrícolas: page carries no CEPEA/ESALQ attribution — "
            "wrong page or upstream restructure"
        )

    title = soup.find("h1")
    title_text = title.get_text(" ", strip=True).lower() if title else ""
    if _COMMODITY_MARKER not in title_text:
        raise ScraperShapeError(
            f"Notícias Agrícolas: page title {title_text!r} lacks "
            f"'{_COMMODITY_MARKER}' — redirected to another commodity?"
        )

    tables = soup.find_all("table", class_=_TABLE_CLASS)
    if not tables:
        raise ScraperShapeError(
            f"Notícias Agrícolas: no <table class='{_TABLE_CLASS}'> on page "
            "(renamed or JS-rendered?)"
        )

    rows: list[dict[str, object]] = []
    for table in tables:
        heading = table.find_previous(_HEADING_TAGS)
        heading_text = heading.get_text(" ", strip=True).lower() if heading else ""
        if _COMMODITY_MARKER not in heading_text:
            raise ScraperShapeError(
                f"Notícias Agrícolas: quote table sits under heading "
                f"{heading_text!r}, not a '{_COMMODITY_MARKER}' section — "
                "page layout reordered?"
            )
        for tr in table.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cells) < 2:
                continue
            try:
                parsed_date = datetime.strptime(cells[0], "%d/%m/%Y").date()
            except ValueError:
                continue
            price_per_sack = _parse_brl_price(cells[1])
            if price_per_sack is None or price_per_sack <= 0:
                continue
            if not _SACK_MIN_BRL <= price_per_sack <= _SACK_MAX_BRL:
                raise ScraperShapeError(
                    f"Notícias Agrícolas: {price_per_sack} on {parsed_date} "
                    f"is outside the {_SACK_MIN_BRL:g}-{_SACK_MAX_BRL:g} "
                    "R$/saca plausibility band — unit change upstream?"
                )
            rows.append({
                "Date": parsed_date.isoformat(),
                "price_brl_mt": round(price_per_sack * 1000.0 / _SACK_KG, 2),
                "Unit": "BRL/MT",
            })

    if not rows:
        raise ScraperShapeError(
            "Notícias Agrícolas: quote tables found but no parseable "
            "date/price rows — column layout may have changed"
        )

    df = pd.DataFrame(rows).drop_duplicates(subset="Date").sort_values("Date")
    return df.reset_index(drop=True)


def fetch_noticias_agricolas(history_date: str | None = None) -> FetchResult:
    """Fetch CEPEA/ESALQ soy indicators republished by Notícias Agrícolas.

    ``history_date`` (ISO ``YYYY-MM-DD``) appends the archive suffix so a
    backfill can walk historical pages; None fetches the current page
    (~10 most recent sessions). Indicators publish every business day, so
    a download failure or structure change on every URL is
    ``FetchResult.failed``; partial coverage still returns ``ok``.
    """
    data: dict[str, pd.DataFrame] = {}
    errors: list[str] = []

    for name, url in NOTICIAS_AGRICOLAS_URLS.items():
        target = f"{url}/{history_date}" if history_date else url
        logger.info("Fetching %s from Notícias Agrícolas ...", name)
        html = _fetch_page(target)
        if not html:
            errors.append(f"{name}: download failed")
            continue
        try:
            data[name] = _parse_indicator_page(html)
        except ScraperShapeError as exc:
            logger.error("Notícias Agrícolas: %s — %s", name, exc)
            errors.append(f"{name}: {exc}")

    if not data:
        return FetchResult.failed("; ".join(errors) or "no indicators configured")
    if errors:
        logger.warning("Notícias Agrícolas: partial fetch — %s", "; ".join(errors))
    return FetchResult.ok(data)


__all__: Sequence[str] = ("_parse_indicator_page", "fetch_noticias_agricolas")


# ── Quick self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    result = fetch_noticias_agricolas()
    if not result.has_rows:
        logger.info("Notícias Agrícolas: %s — %s", result.status, result.error)
    else:
        for name, df in result.data.items():
            logger.info("%s: %d rows\n%s", name, len(df), df.tail(3).to_string(index=False))
