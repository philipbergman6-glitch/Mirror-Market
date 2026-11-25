"""
Layer 17 — CEPEA/ESALQ Brazil soy indicators, read from CEPEA itself.

CEPEA (Centro de Estudos Avançados em Economia Aplicada, ESALQ/USP) is
the publisher of record for both Brazilian soy indicators this layer
stores. One server-rendered page (``config.CEPEA_SOYBEAN_URL``) carries
both, each as a ~15-session table under its own label:

* ``INDICADOR DA SOJA CEPEA/ESALQ - PARANÁ`` → ``Soybean (CEPEA)`` — the
  classic farm-gate Paraná indicator.
* ``INDICADOR DA SOJA CEPEA/ESALQ - PARANAGUÁ`` → ``Soybean (ESALQ/B3
  Paranaguá)`` — the port-side indicator B3 settles its soy contract on.
  Not the AgRural port bid (Layer 19, B21 #483).

History. cepea.org.br sat behind a Cloudflare Turnstile wall from 2026-05,
so from 2026-07-30 the layer read the same indicators republished by
Notícias Agrícolas (``fetchers/noticias_agricolas.py``). From 2026-11-09
Notícias Agrícolas began answering every request — GitHub runners and a
residential address alike — with a Cloudflare JS challenge (HTTP 403,
``cf-mitigated: challenge``), while CEPEA's own page answered 200 from both.
Values were identical on the overlap (2026-10-29/30: Paraná 2541.17/2537.00,
Paranaguá 2638.00/2648.00 BRL/MT), and the commodity keys are unchanged, so
stored history continues without a seam. Notícias Agrícolas stays on disk
for its dated archive pages (``scripts/backfill_cepea_gap.py``).

Tables are chosen by their label, never by position: the page lists
Paranaguá first, and a positional parse would file the port indicator under
the Paraná key — a wrong number that looks right (~4% apart).

Unit: CEPEA publishes R$ per 60 kg sack ("Reais por saca de 60 kg"); we
store BRL/MT via ``pipeline.units.per_sack_to_per_metric_ton`` (sack × 1000 / 60).
USD conversion happens downstream.

Licence: the page footer reads "CEPEA (CC BY-NC 4.0)" — the same terms that
governed the republication; publishing it is covered by the A18 stance.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from collections.abc import Sequence
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

from config import CEPEA_INDICATORS, CEPEA_SOYBEAN_URL, MAX_RETRIES, REQUEST_TIMEOUT
from fetchers._backoff import retry_sleep
from pipeline.results import FetchResult, ScraperShapeError
from pipeline.units import KG_PER_SACK, per_sack_to_per_metric_ton

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; MirrorMarket/1.0)",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

# Alias of the one sack constant (invariant 7, #665), kept because PR #663's
# verifier tripwire reads ``cepea._SACK_KG``; drop it once that test reads
# ``pipeline.units.KG_PER_SACK``. Not used for arithmetic here.
_SACK_KG = KG_PER_SACK
# Plausibility band, R$ per 60 kg sack. Soy has traded ~70–180 for a
# decade; a silent switch to R$/MT (~2,500) or R$/kg (~2.6) lands far
# outside it.
_SACK_MIN_BRL = 60.0
_SACK_MAX_BRL = 300.0
# The page must declare the sack unit before any number on it is trusted.
_UNIT_MARKER = "reais por saca de 60 kg"
_PRICE_HEADER = "valor r$"
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


def _fold(text: str) -> str:
    """Lower-case, accent-strip and collapse whitespace for label matching."""
    decomposed = unicodedata.normalize("NFKD", text)
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    return " ".join(stripped.lower().split())


def _fetch_cepea_page() -> str:
    """Download the CEPEA soy indicator page. Returns '' after retries."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                CEPEA_SOYBEAN_URL, headers=_HEADERS, timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.text
            logger.warning("CEPEA: HTTP %d (attempt %d)", resp.status_code, attempt)
        except requests.RequestException as exc:
            logger.warning("CEPEA: request failed (attempt %d): %s", attempt, exc)
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)
    return ""


def _label_of(table: Tag) -> str:
    """The nearest preceding 'INDICADOR …' text above ``table``, folded."""
    for text in table.find_all_previous(string=True, limit=60):
        folded = _fold(str(text))
        if folded.startswith("indicador "):
            return folded
    return ""


def _parse_table(table: Tag, label: str) -> pd.DataFrame:
    rows = table.find_all("tr")
    header = [_fold(c.get_text(" ", strip=True)) for c in rows[0].find_all(["th", "td"])] if rows else []
    # "Valor R$*" — the asterisk points at the page's unit note.
    if len(header) < 2 or header[1].rstrip("*") != _PRICE_HEADER:
        raise ScraperShapeError(
            f"CEPEA: table under {label!r} has header {header!r}, expected "
            f"column 2 to be {_PRICE_HEADER!r} — layout changed"
        )
    out: list[dict[str, object]] = []
    for tr in rows[1:]:
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cells) < 2:
            continue
        try:
            day = datetime.strptime(cells[0], "%d/%m/%Y").date()
        except ValueError:
            continue
        sack = _parse_brl_price(cells[1])
        if sack is None or sack <= 0:
            continue
        if not _SACK_MIN_BRL <= sack <= _SACK_MAX_BRL:
            raise ScraperShapeError(
                f"CEPEA: {sack} on {day} under {label!r} is outside the "
                f"{_SACK_MIN_BRL:g}-{_SACK_MAX_BRL:g} R$/saca plausibility "
                "band — unit change upstream?"
            )
        out.append({
            "Date": day.isoformat(),
            "price_brl_mt": round(per_sack_to_per_metric_ton(sack), 2),
            "Unit": "BRL/MT",
        })
    if not out:
        raise ScraperShapeError(
            f"CEPEA: table under {label!r} has no parseable date/price rows"
        )
    df = pd.DataFrame(out).drop_duplicates(subset="Date").sort_values("Date")
    return df.reset_index(drop=True)


def _parse_indicator_tables(html: str) -> tuple[dict[str, pd.DataFrame], list[str]]:
    """Map each configured indicator label to its parsed table.

    Returns ``(data, errors)``: a missing or malformed indicator is an
    error for that key only. Page-level problems (no unit declaration, a
    label matching two tables) raise ScraperShapeError.
    """
    soup = BeautifulSoup(html, "html.parser")
    if _UNIT_MARKER not in _fold(soup.get_text(" ", strip=True)):
        raise ScraperShapeError(
            f"CEPEA: page does not declare {_UNIT_MARKER!r} — unit or page changed"
        )

    by_label: dict[str, list[Tag]] = {}
    for table in soup.find_all("table"):
        by_label.setdefault(_label_of(table), []).append(table)

    data: dict[str, pd.DataFrame] = {}
    errors: list[str] = []
    for key, label in CEPEA_INDICATORS.items():
        wanted = _fold(label)
        tables = by_label.get(wanted, [])
        if len(tables) > 1:
            raise ScraperShapeError(
                f"CEPEA: label {label!r} heads {len(tables)} tables — ambiguous"
            )
        if not tables:
            errors.append(f"{key}: label {label!r} not found on page")
            continue
        try:
            data[key] = _parse_table(tables[0], label)
        except ScraperShapeError as exc:
            errors.append(f"{key}: {exc}")
    return data, errors


def fetch_cepea() -> FetchResult:
    """Fetch both CEPEA/ESALQ soy indicators from cepea.org.br.

    The indicators publish every business day, so a download failure or a
    page that yields neither indicator is ``FetchResult.failed``; one of
    two still returns ``ok`` with the miss logged.
    """
    logger.info("Fetching CEPEA/ESALQ soy indicators ...")
    html = _fetch_cepea_page()
    if not html:
        return FetchResult.failed("CEPEA: page download failed")
    try:
        data, errors = _parse_indicator_tables(html)
    except ScraperShapeError as exc:
        logger.error("CEPEA: %s", exc)
        return FetchResult.failed(str(exc))
    if not data:
        return FetchResult.failed("; ".join(errors) or "CEPEA: no indicators configured")
    if errors:
        logger.warning("CEPEA: partial fetch — %s", "; ".join(errors))
    for key, df in data.items():
        logger.info(
            "CEPEA: %s — %d sessions, latest %.2f BRL/MT (%s)",
            key, len(df), df["price_brl_mt"].iloc[-1], df["Date"].iloc[-1],
        )
    return FetchResult.ok(data)


__all__: Sequence[str] = ("_parse_brl_price", "_parse_indicator_tables", "fetch_cepea")


# ── Quick self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    result = fetch_cepea()
    if not result.has_rows:
        logger.info("CEPEA: %s — %s", result.status, result.error)
    else:
        for name, df in result.data.items():
            logger.info("%s: %d rows\n%s", name, len(df), df.tail(3).to_string(index=False))
