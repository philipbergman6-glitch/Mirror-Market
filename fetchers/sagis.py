"""
Layer 23 — SAGIS weekly producer deliveries (South Africa).

Source: https://www.sagis.org.za/sagis-weekly-data/
Licence: "SAGIS' information may be reproduced with the acknowledgement of
         the source." — an explicit reproduction grant. Every surface that
         renders these numbers must carry ``config.SAGIS_ATTRIBUTION``.

What this is
------------
Tonnage delivered by South African producers into commercial storage each
week — a **physical flow**, not a price. It is the SA analogue of the USDA
export inspections series (Layer 14), and it is what makes a South Africa
page possible at all: the SAFEX price leg is capped by JSE licensing
(#157), while this is licence-clean and goes back to 2018/19.

Which file, and why
-------------------
SAGIS publishes two workbooks per commodity per week:

  ``ProdProgressive-Sojabone_2026-2027_22.xlsx``
      the *presentation* file — a formatted report with a title block, a
      units row, merged comparison columns and one season per file.
  ``DT-SWP-Soybeans_2026_22.xlsx``
      the *machine-readable* export — a flat 9-column table, one sheet, and
      **every season in one file** (2018–2026 as of week 22/2026).

We take the DT export. It needs no header sniffing, and because it carries
all seasons the layer self-heals: an empty CI database re-downloads the full
history on the first run.

The URL rotates every week
--------------------------
``DT-SWP-Soybeans_2026_22.xlsx`` becomes ``..._23.xlsx`` next week, under a
``/wp-content/uploads/<year>/<month>/`` path that also moves. A hardcoded
deep link would keep resolving with HTTP 200 and serve a frozen week
forever — the same failure the World Bank CMO GUID has in Layer 8. So the
listing page is parsed on every run and the highest ``(season, week)`` link
per commodity wins. If no link is found for a commodity, that is a shape
error, not an empty result.

Columns, and what we keep
-------------------------
``Commodity, SubCereal, SeasonYear, SeasonStatus, WeekNumber, WeekEnd,
FirstPublished, Adjustments, AdjustedWeekTotal``

  FirstPublished     tonnage as first reported for that week
  Adjustments        later revisions to it (can be negative)
  AdjustedWeekTotal  FirstPublished + Adjustments

All three are stored. The presentation file's ``Prog. Total`` column is a
running sum of ``AdjustedWeekTotal`` and is *not* stored — it is derived,
and the map's standing rule is to keep components, never derived-only.

Seasons and weeks
-----------------
``SeasonYear`` is the *starting* calendar year of a March–February marketing
season: 2026 means the 2026/27 season, whose week 1 ends 2026-03-06 and
whose week 52 ends in February 2027. ``SeasonStatus`` is ``Active`` for the
season in progress and ``Final`` for closed ones — and a ``Final`` season's
numbers can still differ from what was published while it was active, which
is why the upsert key is (commodity, season, week) and revisions overwrite.

``WeekEnd`` is a range string, ``"28/02 - 06/03/2026"``. Only the right-hand
side carries a year, and it is day-first. It is parsed strictly: a week whose
end date will not parse hard-fails the layer rather than being dropped, since
a silently missing week is indistinguishable from a week of zero deliveries.
"""

from __future__ import annotations

import io
import logging
import re
from collections.abc import Sequence

import pandas as pd
import requests

from config import (
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    SAGIS_COMMODITIES,
    SAGIS_WEEKLY_DATA_URL,
)
from fetchers._backoff import retry_sleep
from pipeline.results import FetchResult, ScraperShapeError

logger = logging.getLogger(__name__)

# SAGIS serves the listing page and the workbooks to a plain project UA
# (verified 2026-08-11, HTTP 200 on both). Named rather than browser-spoofed
# so the operator is identifiable to the source, per the licence's spirit.
_HEADERS = {
    "User-Agent": (
        "Mirror-Market/1.0 (commodity market research; "
        "+https://github.com/philipbergman6-glitch/Mirror-Market)"
    ),
}

# DT-SWP-<Commodity>_<season>_<week>.xlsx anywhere under the uploads tree.
_DT_LINK_RE = re.compile(
    r"https://[^\"'\s]*?/DT-SWP-(?P<commodity>[A-Za-z]+)_(?P<season>\d{4})_(?P<week>\d{1,2})\.xlsx"
)

_REQUIRED_COLUMNS: tuple[str, ...] = (
    "Commodity",
    "SeasonYear",
    "SeasonStatus",
    "WeekNumber",
    "WeekEnd",
    "FirstPublished",
    "Adjustments",
    "AdjustedWeekTotal",
)

# "28/02 - 06/03/2026" → the end half. Start half has no year and is unused.
_WEEK_END_RE = re.compile(r"-\s*(\d{1,2}/\d{1,2}/\d{4})\s*$")


def _get(url: str, what: str) -> bytes:
    """GET ``url`` with retry/backoff. Returns b"" on transport failure."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.content
            logger.warning("SAGIS %s: HTTP %d (attempt %d)", what, resp.status_code, attempt)
        except requests.RequestException as exc:
            logger.warning("SAGIS %s: request failed (attempt %d): %s", what, attempt, exc)
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)
    return b""


def _resolve_dt_links(html: str) -> dict[str, str]:
    """Map each tracked commodity to its newest DT-SWP export URL.

    "Newest" is the highest ``(season, week)`` pair, not document order and
    not the upload path's date — the listing carries several seasons of
    files side by side and their upload folders do not sort chronologically.

    Raises
    ------
    ScraperShapeError
        If any tracked commodity has no DT-SWP link on the page. The export
        is published weekly for every commodity SAGIS covers, so an absent
        one means the page or the filename convention changed.
    """
    newest: dict[str, tuple[tuple[int, int], str]] = {}

    for match in _DT_LINK_RE.finditer(html):
        token = match.group("commodity")
        if token not in SAGIS_COMMODITIES:
            continue  # Maize, Wheat — published, not tracked here
        version = (int(match.group("season")), int(match.group("week")))
        current = newest.get(token)
        if current is None or version > current[0]:
            newest[token] = (version, match.group(0))

    missing = [token for token in SAGIS_COMMODITIES if token not in newest]
    if missing:
        raise ScraperShapeError(
            f"SAGIS: no DT-SWP export link found for {missing} on "
            f"{SAGIS_WEEKLY_DATA_URL} — page or filename convention changed"
        )

    for token, (version, url) in newest.items():
        logger.info("SAGIS %s: resolved season %d week %d → %s", token, *version, url)
    return {token: url for token, (_, url) in newest.items()}


def _parse_week_end(value: str) -> str:
    """ISO date for the last day of a ``"28/02 - 06/03/2026"`` week range.

    Day-first — SAGIS uses the South African convention, so 06/03/2026 is
    6 March. Raises ScraperShapeError rather than returning None: a week we
    cannot date must not be silently dropped, because a missing week reads
    downstream as a week of zero deliveries.
    """
    match = _WEEK_END_RE.search(str(value or ""))
    if not match:
        raise ScraperShapeError(
            f"SAGIS: unparseable WeekEnd range {value!r} — expected "
            "'DD/MM - DD/MM/YYYY'"
        )
    try:
        return str(pd.to_datetime(match.group(1), dayfirst=True).date())
    except (ValueError, TypeError) as exc:
        raise ScraperShapeError(
            f"SAGIS: WeekEnd {value!r} has an unreadable end date"
        ) from exc


def _parse_dt_export(raw: bytes, commodity: str) -> pd.DataFrame:
    """Parse one DT-SWP workbook into the stored frame shape.

    Returns columns: commodity, season_year, season_status, week_number,
    week_end, first_published, adjustments, week_total, unit.
    """
    try:
        df = pd.read_excel(io.BytesIO(raw), engine="openpyxl")
    except Exception as exc:  # openpyxl raises a zoo of types on bad bytes
        raise ScraperShapeError(f"SAGIS {commodity}: workbook unreadable — {exc}") from exc

    missing = [c for c in _REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ScraperShapeError(
            f"SAGIS {commodity}: DT export missing columns {missing} — "
            f"got {list(df.columns)}"
        )

    if df.empty:
        raise ScraperShapeError(
            f"SAGIS {commodity}: DT export parsed but carried no rows — the "
            "file always holds every season, so zero rows is a shape change"
        )

    out = pd.DataFrame({
        "commodity": SAGIS_COMMODITIES[commodity],
        "season_year": pd.to_numeric(df["SeasonYear"], errors="coerce"),
        "season_status": df["SeasonStatus"].astype(str).str.strip(),
        "week_number": pd.to_numeric(df["WeekNumber"], errors="coerce"),
        "week_end": [_parse_week_end(v) for v in df["WeekEnd"]],
        "first_published": pd.to_numeric(df["FirstPublished"], errors="coerce"),
        "adjustments": pd.to_numeric(df["Adjustments"], errors="coerce"),
        "week_total": pd.to_numeric(df["AdjustedWeekTotal"], errors="coerce"),
        "unit": "MT",
    })

    unkeyed = out["season_year"].isna() | out["week_number"].isna()
    if unkeyed.any():
        raise ScraperShapeError(
            f"SAGIS {commodity}: {int(unkeyed.sum())} row(s) have a "
            "non-numeric SeasonYear or WeekNumber — cannot key them"
        )
    out["season_year"] = out["season_year"].astype(int)
    out["week_number"] = out["week_number"].astype(int)

    logger.info(
        "SAGIS %s: %d weeks across seasons %d–%d (newest week ending %s)",
        SAGIS_COMMODITIES[commodity],
        len(out),
        int(out["season_year"].min()),
        int(out["season_year"].max()),
        out["week_end"].max(),
    )
    return out


def fetch_sagis_deliveries() -> FetchResult:
    """Fetch SAGIS weekly producer deliveries for the tracked commodities.

    Returns
    -------
    FetchResult
        ``ok`` with ``{commodity_key: DataFrame}`` on success; ``failed``
        when the listing page or a workbook can't be downloaded, or when
        either no longer matches the expected structure.

        There is no ``empty`` path. The DT export carries every season in
        one file, so it is never legitimately empty — zero rows means the
        source broke, and the layer is wired with ``empty_fails=True``.
    """
    logger.info("Fetching SAGIS weekly producer deliveries ...")

    listing = _get(SAGIS_WEEKLY_DATA_URL, "listing page")
    if not listing:
        return FetchResult.failed("SAGIS: listing page download failed")

    try:
        links = _resolve_dt_links(listing.decode("utf-8", errors="replace"))
    except ScraperShapeError as exc:
        logger.error("SAGIS: %s", exc)
        return FetchResult.failed(str(exc))

    data: dict[str, pd.DataFrame] = {}
    for token, url in links.items():
        raw = _get(url, f"{token} DT export")
        if not raw:
            return FetchResult.failed(f"SAGIS: download failed for {token} ({url})")
        try:
            data[SAGIS_COMMODITIES[token]] = _parse_dt_export(raw, token)
        except ScraperShapeError as exc:
            logger.error("SAGIS: %s", exc)
            return FetchResult.failed(str(exc))

    return FetchResult.ok(data)


# Re-export for tests
__all__: Sequence[str] = (
    "_parse_dt_export",
    "_parse_week_end",
    "_resolve_dt_links",
    "fetch_sagis_deliveries",
)


# ── Quick self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    result = fetch_sagis_deliveries()
    if not result.has_rows:
        logger.info("SAGIS: %s — %s", result.status, result.error)
    else:
        for name, frame in result.data.items():
            active = frame[frame["season_status"].str.lower() == "active"]
            logger.info(
                "%s: %d rows; active season %s progressive %s MT",
                name,
                len(frame),
                active["season_year"].max() if not active.empty else "n/a",
                f"{active['week_total'].sum():,.0f}" if not active.empty else "n/a",
            )
