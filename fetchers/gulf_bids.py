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

from config import (
    AMS_GULF_BIDS_URL,
    MARS_API_KEY,
    MARS_ARCHIVE_TIMEOUT,
    MARS_BASE_URL,
    MARS_GULF_BIDS_SLUG,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
)
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
#
# The two endpoints of a ranged basis quote may reference *different* CBOT
# contracts (Q=Aug, X=Nov above) — ~3% of cells across 2021-06→2026-08. Both
# codes are stored; labelling the high leg with the low leg's month prices a
# spread against the wrong futures contract (#196).
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
        "futures_month_high": MONTH_CODES[basis["code_high"] or basis["code_low"]],
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
    sale_type, basis_low, basis_high (cents/bu), futures_month and
    futures_month_high (1-12; equal unless the quote spans two contracts),
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


# ── The same report over the MARS API (#283) ─────────────────────────────────
#
# USDA's MARS API serves report 3147 as structured rows back to 2020-02-24,
# where the PDF above is only ever *today's* report. That is the whole reason
# this path exists: it is the only way to give Layer 20 a history, and it does
# it in one authenticated pull rather than 1,600 PDF downloads that no longer
# exist. It is deliberately **not** the live path — see LAYERS.md Layer 20:
# the PDF is keyless, so an absent or rotated `MARS_API_KEY` degrades the
# backfill, never the daily leg.
#
# Every field below is mapped to the shape the PDF parser already produces —
# not to a shape of its own. Two transports for one report must yield one
# table, or the archive and the daily rows become different data wearing the
# same column names; tests/test_gulf_bids_api.py pins that cell for cell
# against the same report in both forms. Where the API cannot be mapped into
# that shape — a port, a freight term, a delivery window we have no label for —
# it raises. An unmappable row is drift, and drift stored is a wrong number.

# Report Detail's stable field vocabulary. A value outside these sets is not
# a row to guess at.
_API_QUOTE_TYPE = "Basis"
_API_BASIS_UNIT = "¢/Bu"
_API_PRICE_UNIT = "$ Per Bushel"

# The three commodities the PDF prints sections for (_SECTIONS). The report
# also carries sorghum at the Texas ports; Layer 20 is the soy complex's Gulf
# leg and does not store it — a commodity we never rendered is out of scope,
# not a gap.
_API_COMMODITIES = frozenset(_SECTIONS)

# `trade_loc` → the stored `location`. The PDF prints the port state on a
# continuation line that its parser drops, so "Gulf Coast Ports" *is* the
# Louisiana barge market in every row stored to date. Mapping the Texas ports
# onto that same label would merge two markets under one primary key, so an
# unmapped port raises instead: today no stored commodity quotes from TX, and
# the day one does is a day for a schema decision, not a silent collision.
_API_LOCATIONS = {"Gulf Coast Ports - LA": "Gulf Coast Ports"}

# (freight, trans_mode) → the PDF's freight token. "CIF-B" is CIF-Barge.
_API_FREIGHT = {("C.I.F.", "Barge"): "CIF-B"}

# `delivery Start Half` → the PDF's footnote superscript. Verified against the
# 2026-08-11 report in both forms: first-half Aug reads "Aug¹", last-half
# "Aug²" (tests/test_gulf_bids_api.py).
_API_DELIVERY_HALVES = {None: "", "First Half": "¹", "Last Half": "²"}

_MONTH_ABBR = (
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
)

_MONTH_NAMES = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)

# "August (Q)" — the month spelled out *and* its CME code, which is a check
# rather than a redundancy: the two disagreeing means the row does not know
# which contract it is quoted against, and picking a leg would invent one.
_API_MONTH_RE = re.compile(r"^(?P<name>[A-Z][a-z]+)\s+\((?P<code>[FGHJKMNQUVXZ])\)$")

# Directions AMS prints in the change columns.
_API_DIRECTIONS = frozenset({"UNCH", "UP", "DN"})

# Decimal places the PDF prints each change column to — basis in whole cents
# to 2dp ("DN 5.00"), prices to 4dp ("DN 0.1025"). The API carries the same
# numbers as bare floats, so the formatting has to be restored here or the
# archive's change strings would not match the daily rows'.
_BASIS_CHANGE_DP = 2
_PRICE_CHANGE_DP = 4


def _api_futures_month(label: object, which: str) -> int:
    """"August (Q)" → 8, asserting the spelled month and the code agree."""
    match = _API_MONTH_RE.match(str(label).strip()) if label else None
    if not match:
        raise ScraperShapeError(
            f"AMS 3147 API: unparseable {which} futures month {label!r}"
        )
    code_month = MONTH_CODES[match["code"]]
    name = match["name"]
    if name not in _MONTH_NAMES or _MONTH_NAMES.index(name) + 1 != code_month:
        raise ScraperShapeError(
            f"AMS 3147 API: {which} futures month {label!r} names a month its "
            "CME code contradicts"
        )
    return code_month


def _api_delivery(row: dict) -> str:
    """The stored delivery label: "Current", "Oct", or a half-month "Sep¹"."""
    current = row.get("current")
    if current == "Yes":
        return "Current"
    if current != "No":
        raise ScraperShapeError(
            f"AMS 3147 API: row is neither current nor forward (current={current!r})"
        )

    start, end = row.get("delivery_start"), row.get("delivery_end")
    if not start:
        raise ScraperShapeError(
            "AMS 3147 API: forward row carries no delivery window"
        )
    if start != end:
        raise ScraperShapeError(
            f"AMS 3147 API: delivery window {start}→{end} spans two windows — "
            "one label cannot name both"
        )

    half_start, half_end = row.get("delivery Start Half"), row.get("delivery End Half")
    if half_start != half_end:
        raise ScraperShapeError(
            f"AMS 3147 API: delivery half {half_start!r}→{half_end!r} disagrees "
            "across the window"
        )
    if half_start not in _API_DELIVERY_HALVES:
        raise ScraperShapeError(f"AMS 3147 API: unknown delivery half {half_start!r}")

    try:
        month = datetime.strptime(str(start), "%Y-%m-%d").month
    except ValueError as exc:
        raise ScraperShapeError(
            f"AMS 3147 API: unparseable delivery date {start!r}"
        ) from exc
    return f"{_MONTH_ABBR[month - 1]}{_API_DELIVERY_HALVES[half_start]}"


def _api_change(row: dict, column: str, decimals: int) -> str | None:
    """Rebuild the PDF's change string from the API's four typed fields.

    ``None`` where AMS published no change at all (#190 — a blank column is
    never a zero). A row with one leg quoted and the other blank has no
    rendering in the PDF's vocabulary and raises rather than half-print.
    """
    legs = [(row.get(f"{column} {end} Change"), row.get(f"{column} {end} Direction"))
            for end in ("Min", "Max")]
    blank = [value is None and direction is None for value, direction in legs]
    if all(blank):
        return None
    if any(blank):
        raise ScraperShapeError(
            f"AMS 3147 API: {column} change column is blank on one leg only: {legs!r}"
        )

    tokens = []
    for value, direction in legs:
        if direction not in _API_DIRECTIONS:
            raise ScraperShapeError(
                f"AMS 3147 API: unknown {column} change direction {direction!r}"
            )
        if direction == "UNCH":
            tokens.append("UNCH")
            continue
        if value is None:
            raise ScraperShapeError(
                f"AMS 3147 API: {column} change direction {direction!r} with no value"
            )
        tokens.append(f"{direction} {float(value):.{decimals}f}")
    return tokens[0] if tokens[0] == tokens[1] else f"{tokens[0]}-{tokens[1]}"


def _api_report_date(value: object) -> str:
    """MM/DD/YYYY → ISO. Parsed, never sorted or sliced as a string (#283)."""
    try:
        return datetime.strptime(str(value), "%m/%d/%Y").date().isoformat()
    except ValueError as exc:
        raise ScraperShapeError(
            f"AMS 3147 API: unparseable report_date {value!r}"
        ) from exc


def _map_api_rows(rows: Sequence[dict]) -> pd.DataFrame:
    """Map Report Detail rows onto the PDF parser's frame, or raise.

    Rows for commodities Layer 20 does not store are dropped; everything
    that survives that filter must map completely.
    """
    mapped: list[dict[str, object]] = []
    for row in rows:
        commodity = row.get("commodity")
        if commodity not in _API_COMMODITIES:
            continue

        if row.get("quote_type") != _API_QUOTE_TYPE:
            raise ScraperShapeError(
                f"AMS 3147 API: quote_type {row.get('quote_type')!r} is not a "
                f"{_API_QUOTE_TYPE} quote"
            )
        sale_type = row.get("sale Type")
        if sale_type not in _SALE_TYPES:
            raise ScraperShapeError(f"AMS 3147 API: unknown sale type {sale_type!r}")
        if (row.get("basis_unit"), row.get("price_unit")) != (
            _API_BASIS_UNIT, _API_PRICE_UNIT
        ):
            raise ScraperShapeError(
                f"AMS 3147 API: unexpected units "
                f"{row.get('basis_unit')!r}/{row.get('price_unit')!r} — the "
                "cents-per-bushel basis over a dollar price is the mapping's "
                "whole premise"
            )

        trade_loc = row.get("trade_loc")
        if trade_loc not in _API_LOCATIONS:
            raise ScraperShapeError(
                f"AMS 3147 API: unmapped trade_loc {trade_loc!r} for {commodity} — "
                "storing it under the Louisiana label would merge two markets"
            )
        freight_key = (row.get("freight"), row.get("trans_mode"))
        if freight_key not in _API_FREIGHT:
            raise ScraperShapeError(
                f"AMS 3147 API: unmapped freight term {freight_key!r}"
            )

        for field in ("basis Min", "basis Max", "price Min", "price Max", "avg_price"):
            if row.get(field) is None:
                raise ScraperShapeError(
                    f"AMS 3147 API: {field!r} missing on a stored row: {row!r}"
                )

        year_ago = row.get("avg_price_year_ago")
        mapped.append({
            "report_date": _api_report_date(row.get("report_date")),
            "commodity": commodity,
            "location": _API_LOCATIONS[trade_loc],
            "delivery": _api_delivery(row),
            "sale_type": sale_type,
            "basis_low": float(row["basis Min"]),
            "basis_high": float(row["basis Max"]),
            "futures_month": _api_futures_month(
                row.get("basis Min Futures Month"), "low-leg"
            ),
            "futures_month_high": _api_futures_month(
                row.get("basis Max Futures Month"), "high-leg"
            ),
            "basis_change": _api_change(row, "basis", _BASIS_CHANGE_DP),
            "price_low": float(row["price Min"]),
            "price_high": float(row["price Max"]),
            "price_change": _api_change(row, "price", _PRICE_CHANGE_DP),
            "average": float(row["avg_price"]),
            "year_ago": float(year_ago) if year_ago is not None else None,
            "freight": _API_FREIGHT[freight_key],
        })

    return pd.DataFrame(mapped, columns=_API_FRAME_COLUMNS)


_API_FRAME_COLUMNS = [
    "report_date", "commodity", "location", "delivery", "sale_type",
    "basis_low", "basis_high", "futures_month", "futures_month_high",
    "basis_change", "price_low", "price_high", "price_change", "average",
    "year_ago", "freight",
]


def is_api_configured() -> bool:
    """Is the MARS path usable? Read at call time, like every other key check."""
    return bool(MARS_API_KEY)


def fetch_gulf_bids_api(report_date: str | None = None) -> FetchResult:
    """Fetch report 3147's detail rows over the MARS API.

    ``report_date`` is an ISO date; ``None`` asks for the whole archive in
    one pull, which is what the backfill uses. Returns ``FetchResult.empty``
    for a date the report was not published on — asked and answered — and
    ``failed`` for transport, auth, or a shape the mapping cannot take.
    """
    if not is_api_configured():
        return FetchResult.failed(
            "AMS 3147 API: MARS_API_KEY not set — the keyless PDF path is "
            "unaffected; only the archive is unavailable"
        )

    params = {}
    timeout = MARS_ARCHIVE_TIMEOUT
    if report_date is not None:
        day = datetime.strptime(report_date, "%Y-%m-%d").date()
        params["q"] = f"report_date={day:%m/%d/%Y}"
        timeout = REQUEST_TIMEOUT

    url = f"{MARS_BASE_URL}/{MARS_GULF_BIDS_SLUG}/Report Detail"
    payload: dict | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(
                "Fetching AMS 3147 over MARS (%s, attempt %d) ...",
                report_date or "full archive", attempt,
            )
            resp = requests.get(
                url,
                params=params,
                auth=(MARS_API_KEY, ""),
                timeout=timeout,
            )
            if resp.status_code == 200:
                payload = resp.json()
                break
            if resp.status_code in (401, 403):
                return FetchResult.failed(
                    f"AMS 3147 API: HTTP {resp.status_code} — key rejected"
                )
            logger.warning("AMS 3147 API: HTTP %d", resp.status_code)
        except (requests.RequestException, ValueError) as exc:
            logger.warning("AMS 3147 API attempt %d failed: %s", attempt, exc)
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)

    if payload is None:
        return FetchResult.failed("AMS 3147 API: request failed")
    if not isinstance(payload, dict) or "results" not in payload:
        return FetchResult.failed(
            f"AMS 3147 API: unexpected payload shape {type(payload).__name__}"
        )

    rows = payload["results"]
    stats = payload.get("stats") or {}
    # The allowance is a *cap on the answer*, not an error: a pull that hit it
    # is a silently truncated archive, which is the one failure that would
    # look exactly like a complete backfill.
    returned, allowed = stats.get("returnedRows"), stats.get("userAllowedRows")
    if returned and allowed and returned >= allowed:
        return FetchResult.failed(
            f"AMS 3147 API: pull returned {returned} rows against a "
            f"{allowed}-row allowance — the archive is truncated, not complete"
        )

    try:
        df = _map_api_rows(rows)
    except ScraperShapeError as exc:
        logger.error("AMS 3147 API: shape changed — %s", exc)
        return FetchResult.failed(str(exc))

    if df.empty:
        return FetchResult.empty(
            f"AMS 3147 API: no detail rows for {report_date or 'the archive'}"
        )
    logger.info(
        "Mapped %d Gulf bid rows over %d report dates.",
        len(df), df["report_date"].nunique(),
    )
    return FetchResult.ok({"gulf_bids": df})


__all__: Sequence[str] = (
    "_map_api_rows",
    "_parse_gulf_bids",
    "fetch_gulf_bids",
    "fetch_gulf_bids_api",
    "is_api_configured",
)


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
