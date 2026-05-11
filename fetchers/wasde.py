"""
Layer 12 — WASDE Monthly Estimates via USDA OCE.

NASS QuickStats does not serve WASDE forecast rows for the soy complex / grains
/ cotton, so this fetcher pulls the canonical XLS artifact published monthly
by USDA's Office of the Chief Economist at
https://www.usda.gov/oce/commodity/wasde/wasdeMMYY.xls.

On first run we attempt the last ``WASDE_BACKFILL_MONTHS`` files so the
briefing's MoM revision math has history immediately. Subsequent runs still
re-download the latest file every pipeline tick — stored rows are idempotent
under the wasde-table PK (commodity, year, attribute, reference_period).
"""

from __future__ import annotations

import io
import logging
import re
from datetime import date, datetime, timezone

import pandas as pd
import requests

from config import (
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    WASDE_BACKFILL_MONTHS,
    WASDE_LAYOUT,
    WASDE_URL_TEMPLATE,
)
from fetchers._backoff import retry_sleep

logger = logging.getLogger(__name__)

# Marketing-year header pattern, e.g. "2025/26", "2025/26 Proj.", "2024/25 Est."
_MY_PATTERN = re.compile(r"^\s*(\d{4}/\d{2})\b")

# Footnote markers appended to attribute labels: "Avg. Farm Price ($/bu)  2/"
_FOOTNOTE_PATTERN = re.compile(r"\s+\d+/\s*$")

# Price attribute labels carry their unit inside parentheses, e.g.
#   "Avg. Farm Price ($/bu)", "Avg. Price (c/lb)", "Avg. Price ($/s.t.)"
# These rows must NOT inherit the cascaded volume unit ("Million Bushels"
# etc.) — otherwise the briefing renders "5 Million Bushels" for $5/bu.
_PRICE_UNIT_PATTERN = re.compile(r"\(([\$c]/[^)]+)\)")

# Substrings that identify a "units" row (no numeric data, just an announcement
# of which unit the following data rows are denominated in).
_UNIT_KEYWORDS = (
    "Million Acres",
    "Million Bushels",
    "Million Pounds",
    "Million Metric Tons",
    "Thousand Short Tons",
    "Million 480",          # cotton: "Million 480 Pound Bales"
    "Bushels per Acre",
    "Bushels",
    "Pounds",
    "Metric Tons",
)


def fetch_wasde_estimates(
    backfill_months: int = WASDE_BACKFILL_MONTHS,
    today: date | None = None,
) -> dict[str, pd.DataFrame]:
    """Download and parse the last ``backfill_months`` WASDE XLS reports.

    Returns a dict keyed ``f"{commodity}/{attribute}"`` to match the contract
    consumed by ``clean_wasde`` and ``save_wasde``. Each DataFrame carries the
    columns ``commodity_desc, statisticcat_desc, year, Value, unit_desc,
    reference_period_desc`` — identical to the legacy NASS-based shape so the
    downstream pipeline is untouched.

    The function never raises for missing reports (most-recent month may not be
    published yet) — it just logs and continues, returning whatever it could
    parse. An empty dict signals total failure.
    """
    today = today or datetime.now(timezone.utc).date()
    months = _months_to_attempt(backfill_months, today)
    logger.info("[WASDE] Attempting %d monthly XLS files", len(months))

    rows_by_key: dict[str, list[dict]] = {}
    for year, month in months:
        content = _download(year, month)
        if content is None:
            continue
        report_date = f"{year:04d}-{month:02d}-15"
        try:
            parsed = _parse_xls(content, report_date)
        except Exception:
            logger.exception("[WASDE] Failed to parse %04d-%02d", year, month)
            continue
        for row in parsed:
            key = f"{row['commodity_desc']}/{row['statisticcat_desc']}"
            rows_by_key.setdefault(key, []).append(row)

    return {
        key: pd.DataFrame(rows) for key, rows in rows_by_key.items() if rows
    }


def _months_to_attempt(n: int, today: date) -> list[tuple[int, int]]:
    """Generate (year, month) pairs going back `n` months from `today`."""
    out: list[tuple[int, int]] = []
    y, m = today.year, today.month
    for _ in range(n):
        out.append((y, m))
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    return out


def _build_url(year: int, month: int) -> str:
    return WASDE_URL_TEMPLATE.format(mm=month, yy=year % 100)


def _download(year: int, month: int) -> bytes | None:
    """Fetch a single WASDE XLS with retry. None on missing-report or hard failure."""
    url = _build_url(year, month)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=False)
            if resp.status_code == 200 and resp.content:
                logger.debug("[WASDE] Downloaded %s (%d bytes)", url, len(resp.content))
                return resp.content
            # 3xx → unpublished month (Akamai sends a 302 to an apology page).
            # Not retryable — silently skip.
            if 300 <= resp.status_code < 400:
                logger.info("[WASDE] %04d-%02d not published yet (HTTP %d)", year, month, resp.status_code)
                return None
            logger.warning("[WASDE] HTTP %d for %s", resp.status_code, url)
        except requests.RequestException as exc:
            logger.warning("[WASDE] %s attempt %d failed: %s", url, attempt, exc)
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)
    logger.error("[WASDE] All %d attempts failed for %s", MAX_RETRIES, url)
    return None


def _parse_xls(content: bytes, report_date: str) -> list[dict]:
    """Top-level parser: walk WASDE_LAYOUT, yield one row dict per data point."""
    xl = pd.ExcelFile(io.BytesIO(content))
    rows: list[dict] = []
    for commodity, layout in WASDE_LAYOUT.items():
        sheet = layout["sheet"]
        header_text = layout["header_text"]
        if sheet not in xl.sheet_names:
            logger.warning("[WASDE] Sheet %r missing for %s", sheet, commodity)
            continue
        df = pd.read_excel(xl, sheet_name=sheet, header=None)
        stop_labels = _sheet_stop_labels(sheet, header_text)
        rows.extend(_parse_section(df, commodity, header_text, stop_labels, report_date))
    return rows


def _sheet_stop_labels(sheet: str, current_header: str | None) -> set[str]:
    """Other sub-section header_texts that appear on the same sheet.

    Parsing of one section stops when we hit a row whose col-0 label is one
    of these (the start of the *next* sub-table on the same page).
    """
    return {
        layout["header_text"]
        for layout in WASDE_LAYOUT.values()
        if layout["sheet"] == sheet
        and layout["header_text"]
        and layout["header_text"] != current_header
    }


def _parse_section(
    df: pd.DataFrame,
    commodity: str,
    header_text: str | None,
    stop_labels: set[str],
    report_date: str,
) -> list[dict]:
    """Extract all (attribute, marketing_year, value) tuples for one section."""
    start_row = _find_section_start(df, header_text)
    if start_row is None:
        logger.warning("[WASDE] No section start for %s (header=%r)", commodity, header_text)
        return []

    header_idx, my_cols = _find_my_header(df, start_row)
    if header_idx is None:
        logger.warning("[WASDE] No marketing-year header for %s", commodity)
        return []

    # Each WASDE report carries the current month's projection alongside the
    # previous month's value for the same marketing year (sub-headers "Mar" /
    # "Apr" in the April release). We assign distinct reference periods to
    # those columns so a single file already yields a MoM-comparable pair.
    col_records = _assign_reference_periods(my_cols, report_date)

    rows: list[dict] = []
    current_unit = ""
    for r in range(header_idx + 1, len(df)):
        row = df.iloc[r]
        col0 = _cell_str(row.iloc[0])

        # Stop at the start of the next sub-section on this sheet.
        if col0 in stop_labels:
            break
        # Footer note marks end of data.
        if col0.startswith("Note"):
            break

        # Unit announcement row: contains a unit keyword and no numeric data.
        unit = _detect_unit(row)
        if unit and not _has_numeric_in_cols(row, my_cols):
            current_unit = unit
            continue

        # Skip rows that don't look like data (placeholder "Filler", blank col 0,
        # or a second-row MY sub-header like "Mar"/"Apr").
        if not col0 or col0 == "Filler" or col0 in ("Mar", "Apr", "March", "April"):
            continue

        attribute = _clean_label(col0)
        if not attribute:
            continue

        # Price rows ship their unit inside the label; everything else inherits
        # the most-recent unit announcement. When a "Price" label has no
        # parenthetical unit (Cotton's "Avg. Farm Price 3/") we still must not
        # inherit the volume unit — leave it blank rather than mislabel.
        price_match = _PRICE_UNIT_PATTERN.search(attribute)
        if price_match:
            row_unit = price_match.group(1)
        elif "Price" in attribute:
            row_unit = ""
        else:
            row_unit = current_unit

        for col_idx, (my_label, ref_period) in col_records.items():
            val = _coerce_float(row.iloc[col_idx])
            if val is None:
                continue
            rows.append({
                "commodity_desc": commodity,
                "statisticcat_desc": attribute,
                "year": my_label,
                "Value": val,
                "unit_desc": row_unit,
                "reference_period_desc": ref_period,
            })
    return rows


def _assign_reference_periods(
    my_cols: dict[int, str],
    report_date: str,
) -> dict[int, tuple[str, str]]:
    """Map each MY column to (marketing_year, reference_period_iso).

    When the same marketing year occupies two adjacent columns (the previous
    month's projection vs the current month's projection of the same MY), the
    leftmost gets ``report_date - 1 month`` so the two snapshots are distinct
    rows downstream.
    """
    grouped: dict[str, list[int]] = {}
    for col, my in my_cols.items():
        grouped.setdefault(my, []).append(col)
    for cols in grouped.values():
        cols.sort()

    prior = _previous_month_iso(report_date)
    out: dict[int, tuple[str, str]] = {}
    for my, cols in grouped.items():
        if len(cols) >= 2:
            # All but the rightmost are previous-month projections of the same MY.
            for c in cols[:-1]:
                out[c] = (my, prior)
            out[cols[-1]] = (my, report_date)
        else:
            out[cols[0]] = (my, report_date)
    return out


def _previous_month_iso(report_date: str) -> str:
    """Return ``report_date`` shifted back exactly one calendar month.

    Inputs are ISO ``YYYY-MM-DD`` strings produced by the fetcher itself, so
    the parsing is intentionally strict.
    """
    parts = report_date.split("-")
    y, m = int(parts[0]), int(parts[1])
    d = parts[2] if len(parts) > 2 else "15"
    m -= 1
    if m == 0:
        m, y = 12, y - 1
    return f"{y:04d}-{m:02d}-{d}"


def _find_section_start(df: pd.DataFrame, header_text: str | None) -> int | None:
    """Return the row index where the section begins.

    When ``header_text`` is None (whole sheet is one section) we return 0 and
    rely on ``_find_my_header`` to skip past the title/page-number rows.
    """
    if header_text is None:
        return 0
    for r in range(len(df)):
        if _cell_str(df.iloc[r, 0]) == header_text:
            return r
    return None


def _find_my_header(
    df: pd.DataFrame, start_row: int
) -> tuple[int | None, dict[int, str]]:
    """Locate the marketing-year header row and the columns it spans.

    Scans forward from ``start_row`` for the first row containing at least two
    cells matching ``_MY_PATTERN``. Returns *all* MY-bearing columns — when a
    marketing year appears twice (e.g. the previous-month + current-month
    projection of the same crop year) ``_assign_reference_periods`` later
    splits them into distinct snapshots.
    """
    for r in range(start_row, min(start_row + 20, len(df))):
        row = df.iloc[r]
        my_cols: dict[int, str] = {}
        for col_idx in range(len(row)):
            match = _MY_PATTERN.match(_cell_str(row.iloc[col_idx]))
            if match:
                my_cols[col_idx] = match.group(1)
        if len(my_cols) >= 2:
            return r, my_cols
    return None, {}


def _detect_unit(row: pd.Series) -> str:
    """Return joined unit text if the row announces a unit, else empty string.

    The unit announcement is sometimes split across cells (Cotton's "Million 480"
    + "Pound Bales"). We skip col 0 to avoid swallowing sub-section labels like
    "Area" that share a row with the unit.
    """
    cells = [_cell_str(x) for x in row.iloc[1:] if _cell_str(x)]
    joined = " ".join(cells)
    return joined if any(kw in joined for kw in _UNIT_KEYWORDS) else ""


def _has_numeric_in_cols(row: pd.Series, cols: dict[int, str]) -> bool:
    return any(_coerce_float(row.iloc[c]) is not None for c in cols)


def _coerce_float(val: object) -> float | None:
    """Best-effort numeric conversion. Strings with commas are stripped first."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _cell_str(val: object) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return str(val).strip()


def _clean_label(label: str) -> str:
    """Strip footnote markers and collapse internal whitespace.

    Examples:
        "  Supply, Total"                 -> "Supply, Total"
        "Avg. Farm Price ($/bu)  2/"      -> "Avg. Farm Price ($/bu)"
    """
    cleaned = _FOOTNOTE_PATTERN.sub("", label).strip()
    return " ".join(cleaned.split())
