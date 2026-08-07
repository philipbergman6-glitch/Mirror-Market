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
section, its bid rows, or a fresh report date can't be found. Transport
failures return ``FetchResult.failed``.
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

# One bid row (location wraps to a continuation line handled separately).
# Wheat rows carry an extra Protein column ("Ordinary") and sometimes quote
# a single basis/price value instead of a range — both optional here.
_BID_ROW_RE = re.compile(
    r"^\s*(?P<location>[A-Za-z][A-Za-z .]*?(?:Ports|Elevators)?\s*-?)\s+"
    r"(?P<sale_type>Bid|Offer)\s+"
    r"(?:(?P<protein>Ordinary|[\d.]+%)\s+)?"
    r"(?P<basis_low>-?\d+\.\d+)(?P<code_low>[FGHJKMNQUVXZ])"
    r"(?:\s+to\s+(?P<basis_high>-?\d+\.\d+)(?P<code_high>[FGHJKMNQUVXZ]))?\s+"
    r"(?P<basis_change>UNCH|UP\s+[\d.]+|DN\s+[\d.]+)\s+"
    r"(?P<price_low>\d+\.\d+)(?:-(?P<price_high>\d+\.\d+))?\s+"
    r"(?P<price_change>UNCH|UP\s+[\d.]+|DN\s+[\d.]+)\s+"
    r"(?P<average>\d+\.\d+)"
    r"(?:\s+(?P<year_ago>\d+\.\d+))?\s+"
    r"(?P<freight>\S+)\s+"
    r"(?P<delivery>\S+)\s*$",
    re.MULTILINE,
)

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
        for m in _BID_ROW_RE.finditer(section):
            d = m.groupdict()
            rows.append({
                "report_date": report_date,
                "commodity": commodity,
                "location": re.sub(r"\s*-\s*$", "", d["location"]).strip(),
                "delivery": d["delivery"],
                "sale_type": d["sale_type"],
                "basis_low": float(d["basis_low"]),
                "basis_high": float(d["basis_high"] or d["basis_low"]),
                "futures_month": MONTH_CODES[d["code_low"]],
                "basis_change": re.sub(r"\s+", " ", d["basis_change"]),
                "price_low": float(d["price_low"]),
                "price_high": float(d["price_high"] or d["price_low"]),
                "average": float(d["average"]),
                "year_ago": float(d["year_ago"]) if d["year_ago"] else None,
                "freight": d["freight"],
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
