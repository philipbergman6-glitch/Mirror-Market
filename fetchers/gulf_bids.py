"""
Layer 20 — US Gulf export basis bids (USDA AMS report 3147).

The daily "Louisiana and Texas Export Bids" report carries CIF Gulf
(NOLA barge-delivered) export-elevator bids for soybeans, corn, and
wheat — the standard US-origin price basis a physical buyer compares
against Brazil Paranaguá FOB. Published as a PDF (prelim ~midday,
Final later), keyless:

    https://www.ams.usda.gov/mnreports/ams_3147.pdf

Row format after pypdf layout extraction (one logical row wraps onto a
continuation line holding the state code):

    Gulf Coast Ports -  Bid  120.00Q to 122.00Q  UNCH  12.9800-13.0000 ...
            LA

Units: basis is cents/bushel over the CBOT contract identified by the
trailing futures month code (Q=Aug, X=Nov, ... standard CME letters);
prices are $/bu ranges. We store the native units — USD/MT conversion
happens at the display layer via ``pipeline/units.py``.

Parser strategy: hard-fail with ``ScraperShapeError`` when the soybean
section, its bid rows, or a fresh report date can't be found — and when
a line that *is* a bid row doesn't fit the column shape, rather than
skipping it. Transport failures return ``FetchResult.failed``.
"""

from __future__ import annotations

import io
import logging
import re
from collections.abc import Sequence
from datetime import date, datetime, timezone

import pandas as pd
import requests

from config import AMS_GULF_BIDS_URL, MAX_RETRIES, REQUEST_TIMEOUT
from fetchers._backoff import retry_sleep
from pipeline.results import FetchResult, ScraperShapeError

logger = logging.getLogger(__name__)

# CME futures month codes → month number, for labelling which contract a
# basis quote is against.
MONTH_CODES = {
    "F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
    "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12,
}

# Section headers in the report → stored commodity names.
_SECTIONS = {
    "Soybeans": re.compile(r"US\s+#\d\s+Soybeans\s+\(Bulk\)"),
    "Corn": re.compile(r"US\s+#\d\s+Yellow\s+Corn\s+\(Bulk\)"),
    "Wheat": re.compile(r"US\s+#\d\s+Soft\s+Red\s+Winter\s+Wheat\s+\(Bulk\)"),
}

# A bid row is read as typed *fields* split on the 2+-space column gutters
# of the layout extraction, not as one monolithic regex. AMS prints ranged
# change columns ("DN 0.1225-DN 0.1325", "UP 1.00-DN 4.00", "UNCH-DN 2.00")
# and leaves the change and Year Ago columns blank on some deliveries; a
# fixed-token regex matched none of those and dropped the row silently —
# including, on 2026-08-11, the headline soybean Current row (#190).
#
# Column sequence (Protein present in the wheat section only):
#   Location  SaleType  [Protein]  Basis  [BasisChange]  Price
#   [PriceChange]  Average  [YearAgo]  Freight  Delivery
_FIELD_SPLIT_RE = re.compile(r"\s{2,}")

# 120.00Q  |  95.00Q to 100.00X
_BASIS_RE = re.compile(
    r"^(?P<low>-?\d+(?:\.\d+)?)(?P<code_low>[FGHJKMNQUVXZ])"
    r"(?:\s+to\s+(?P<high>-?\d+(?:\.\d+)?)(?P<code_high>[FGHJKMNQUVXZ]))?$"
)

# 12.9800  |  12.4250-12.6875
_PRICE_RE = re.compile(r"^(?P<low>\d+(?:\.\d+)?)(?:-(?P<high>\d+(?:\.\d+)?))?$")

_DECIMAL_RE = re.compile(r"^\d+(?:\.\d+)?$")

_PROTEIN_RE = re.compile(r"^(?:Ordinary|[\d.]+%)$")

# UNCH | UP 1.00 | DN 0.1075, optionally a range of two such tokens.
_CHANGE_TOKEN = r"(?:UNCH|(?:UP|DN)\s+[\d.]+)"
_CHANGE_RE = re.compile(rf"^{_CHANGE_TOKEN}(?:\s*-\s*{_CHANGE_TOKEN})?$")

_SALE_TYPES = {"Bid", "Offer"}

_DATE_RE = re.compile(r"([A-Z][a-z]+)\s+(\d{1,2}),\s+(\d{4})")

_MAX_AGE_DAYS = 7  # daily report; tolerate weekends/holidays


def _extract_text(raw: bytes) -> str:
    """Layout-mode text of every PDF page, concatenated."""
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(raw))
    return "\n".join(
        page.extract_text(extraction_mode="layout") for page in reader.pages
    )


def _parse_report_date(text: str, today: date | None = None) -> str:
    """Pull the report date from the page header; assert freshness."""
    match = _DATE_RE.search(text.replace("  ", " "))
    if not match:
        raise ScraperShapeError("AMS 3147: no report date found in header")
    month_name, day, year = match.groups()
    try:
        parsed = datetime.strptime(f"{month_name} {day} {year}", "%B %d %Y").date()
    except ValueError as exc:
        raise ScraperShapeError(f"AMS 3147: unparseable report date {match.group(0)!r}") from exc

    today = today or datetime.now(timezone.utc).date()
    age = (today - parsed).days
    if age > _MAX_AGE_DAYS:
        raise ScraperShapeError(
            f"AMS 3147: report dated {parsed} is {age} days old — upstream stale"
        )
    return parsed.isoformat()


def _is_bid_row(line: str) -> bool:
    """A data row is any line whose second column is Bid/Offer.

    Deliberately loose: identification and parsing are separate so that a
    line that *is* a bid row but no longer fits the column shape raises
    instead of falling through as unrecognised text.
    """
    fields = _FIELD_SPLIT_RE.split(line.strip())
    return len(fields) > 1 and fields[1] in _SALE_TYPES


def _parse_bid_row(line: str) -> dict[str, object]:
    """Parse one bid row into typed fields, or raise ``ScraperShapeError``.

    Every column that AMS may leave blank (Basis Change, Price Change,
    Year Ago) is optional and resolves to ``None``; everything else is
    required. A row that does not fit this shape is drift, not noise —
    it raises rather than being skipped, so the layer fails loudly
    instead of stamping a fresh ``last_success`` over a short table.
    """
    fields = _FIELD_SPLIT_RE.split(line.strip())
    pos = 0

    def peek() -> str | None:
        return fields[pos] if pos < len(fields) else None

    def take(what: str) -> str:
        nonlocal pos
        value = peek()
        if value is None:
            raise ScraperShapeError(
                f"AMS 3147: unparseable bid row — {what} missing: {line.strip()!r}"
            )
        pos += 1
        return value

    location = take("location")
    sale_type = take("sale type")
    if sale_type not in _SALE_TYPES:
        raise ScraperShapeError(
            f"AMS 3147: unparseable bid row — sale type {sale_type!r}: {line.strip()!r}"
        )

    if (nxt := peek()) is not None and _PROTEIN_RE.match(nxt):
        take("protein")  # wheat-only column, not stored

    basis = _BASIS_RE.match(take("basis"))
    if not basis:
        raise ScraperShapeError(
            f"AMS 3147: unparseable bid row — basis column: {line.strip()!r}"
        )

    basis_change = None
    if (nxt := peek()) is not None and _CHANGE_RE.match(nxt):
        basis_change = take("basis change")

    price = _PRICE_RE.match(take("price"))
    if not price:
        raise ScraperShapeError(
            f"AMS 3147: unparseable bid row — price column: {line.strip()!r}"
        )

    price_change = None
    if (nxt := peek()) is not None and _CHANGE_RE.match(nxt):
        price_change = take("price change")

    average = take("average")
    if not _DECIMAL_RE.match(average):
        raise ScraperShapeError(
            f"AMS 3147: unparseable bid row — average {average!r}: {line.strip()!r}"
        )

    year_ago = None
    if (nxt := peek()) is not None and _DECIMAL_RE.match(nxt):
        year_ago = take("year ago")

    freight = take("freight")
    delivery = take("delivery")
    if pos != len(fields):
        raise ScraperShapeError(
            f"AMS 3147: unparseable bid row — {len(fields) - pos} trailing "
            f"column(s): {line.strip()!r}"
        )

    return {
        "location": re.sub(r"\s*-\s*$", "", location).strip(),
        "delivery": delivery,
        "sale_type": sale_type,
        "basis_low": float(basis["low"]),
        "basis_high": float(basis["high"] or basis["low"]),
        "futures_month": MONTH_CODES[basis["code_low"]],
        "basis_change": _normalise_change(basis_change),
        "price_low": float(price["low"]),
        "price_high": float(price["high"] or price["low"]),
        "price_change": _normalise_change(price_change),
        "average": float(average),
        "year_ago": float(year_ago) if year_ago else None,
        "freight": freight,
    }


def _normalise_change(value: str | None) -> str | None:
    """Collapse internal whitespace; keep both endpoints of a ranged change."""
    return re.sub(r"\s*-\s*", "-", re.sub(r"\s+", " ", value)) if value else None


def _parse_gulf_bids(text: str, today: date | None = None) -> pd.DataFrame:
    """Parse every commodity section's bid rows out of the report text.

    Returns columns: report_date, commodity, location, delivery,
    sale_type, basis_low, basis_high (cents/bu), futures_month (1-12),
    basis_change, price_low, price_high ($/bu), average, year_ago,
    freight. Raises ScraperShapeError when the soybean section or its
    rows are missing — corn/wheat sections are allowed to be absent.
    """
    report_date = _parse_report_date(text, today=today)

    # Slice the text into commodity sections by header position.
    positions: list[tuple[int, str]] = []
    for commodity, pattern in _SECTIONS.items():
        m = pattern.search(text)
        if m:
            positions.append((m.start(), commodity))
    if not any(c == "Soybeans" for _, c in positions):
        raise ScraperShapeError("AMS 3147: soybean section header not found")
    positions.sort()

    rows: list[dict[str, object]] = []
    for idx, (start, commodity) in enumerate(positions):
        end = positions[idx + 1][0] if idx + 1 < len(positions) else len(text)
        section = text[start:end]
        for line in section.splitlines():
            if not _is_bid_row(line):
                continue
            rows.append({
                "report_date": report_date,
                "commodity": commodity,
                **_parse_bid_row(line),
            })

    if not any(r["commodity"] == "Soybeans" for r in rows):
        raise ScraperShapeError(
            "AMS 3147: soybean section present but no bid rows matched — "
            "row layout may have changed"
        )
    return pd.DataFrame(rows)


def fetch_gulf_bids() -> FetchResult:
    """Fetch the AMS Louisiana/Texas export bids report (CIF Gulf basis).

    Returns ``FetchResult.ok({"gulf_bids": df})``. The report publishes
    every business day, so download failure, staleness, or a shape change
    is ``FetchResult.failed``.
    """
    raw = b""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Fetching AMS Gulf export bids (attempt %d) ...", attempt)
            resp = requests.get(
                AMS_GULF_BIDS_URL, timeout=REQUEST_TIMEOUT, allow_redirects=True
            )
            if resp.status_code == 200 and resp.content[:4] == b"%PDF":
                raw = resp.content
                break
            logger.warning(
                "AMS 3147: HTTP %d / non-PDF response", resp.status_code
            )
        except requests.RequestException as exc:
            logger.warning("AMS 3147 attempt %d failed: %s", attempt, exc)
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)

    if not raw:
        return FetchResult.failed("AMS 3147: PDF download failed")

    try:
        text = _extract_text(raw)
        df = _parse_gulf_bids(text)
    except ScraperShapeError as exc:
        logger.error("AMS 3147: shape changed — %s", exc)
        return FetchResult.failed(str(exc))
    except Exception as exc:  # pypdf can raise on corrupt files
        logger.error("AMS 3147: PDF extraction failed — %s", exc)
        return FetchResult.failed(f"PDF extraction failed: {exc}")

    logger.info("Parsed %d Gulf bid rows for %s.", len(df), df["report_date"].iloc[0])
    return FetchResult.ok({"gulf_bids": df})


__all__: Sequence[str] = ("_parse_gulf_bids", "fetch_gulf_bids")


# ── Quick self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    result = fetch_gulf_bids()
    if not result.has_rows:
        logger.info("Gulf bids: %s — %s", result.status, result.error)
    else:
        df = result.data["gulf_bids"]
        logger.info("%d rows\n%s", len(df), df.to_string(index=False))
