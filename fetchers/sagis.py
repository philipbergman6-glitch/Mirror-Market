"""
SAGIS South Africa — two layers off one statutory reporter.

Layer 23 — weekly producer deliveries  (``fetch_sagis_deliveries``)
Layer 24 — monthly soybean supply & demand (``fetch_sagis_supply_demand``)

Sources: https://www.sagis.org.za/sagis-weekly-data/   (Layer 23)
         https://www.sagis.org.za/sagis-monthly-data/  (Layer 24)
Licence: "SAGIS' information may be reproduced with the acknowledgement of
         the source." — an explicit reproduction grant. Every surface that
         renders these numbers must carry ``config.SAGIS_ATTRIBUTION``.

The rest of this docstring describes Layer 23; Layer 24's shape, its file
choice and its zero-vs-unreported trap are documented above
``_parse_smd_workbook`` and ``fetch_sagis_supply_demand`` below.

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
from collections.abc import Hashable, Sequence

import pandas as pd
import requests

from config import (
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    SAGIS_COMMODITIES,
    SAGIS_MONTHLY_DATA_URL,
    SAGIS_SMD_COMMODITIES,
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


# ── Layer 24 — monthly soybean supply & demand (SMD) ─────────────────────────
#
# One season per file: ``Sojabone<start><end>_<pubdate>[_F].xlsx`` carries all
# twelve months of a March–February season as columns, and is re-published
# every month under a *new* filename. Only the current season plus the two
# most recent finals are listed, so both the link and the season set rotate —
# resolve from the landing page every run, and keep the table in
# ``data/history/`` so a season that scrolls off is not lost.
#
# The per-month announcement files (``Sojabone<YYYYMMDD>.xlsx``) are the same
# report in a two-month cut-down; building history from them would cost one
# request per month.

# Sojabone20262027_2026-07-24.xlsx / Sojabone20242025_2025-08-26_F-2.xlsx.
# The ``_<date>`` is what separates a season file from a monthly announcement
# file (Sojabone20260724.xlsx), which is also eight digits after the token.
_SMD_LINK_RE = re.compile(
    r"https://[^\"'\s]*?/(?P<commodity>[A-Za-z]+)"
    r"(?P<start>\d{4})(?P<end>\d{4})_(?P<published>\d{4}-\d{2}-\d{2})"
    r"[^\"'\s/]*\.xlsx"
)

# "SMD-072026" — the report's own vintage stamp, and the only in-file
# statement of how far the numbers actually run. See _report_month.
_SMD_TAG_RE = re.compile(r"\bSMD-(?P<month>\d{2})(?P<year>\d{4})\b")

# Column headers are bilingual: "Mar/Mrt 2026", "Oct/Okt 2026", "Jun 2026".
_SMD_MONTH_RE = re.compile(
    r"^(?P<month>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
    r"(?:/[A-Za-z]{3,4})?\s+(?P<year>\d{4})$"
)
_MONTH_NUMBERS = {
    name: number
    for number, name in enumerate(
        ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
         "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"),
        start=1,
    )
}

# (section letter, normalised label prefix) → stored column.
#
# Anchoring on the section letter is load-bearing, not decoration: the
# workbook repeats sub-labels across sections with *different* numbers.
# "Total Processed for commercial use" appears under (c) and again standalone
# above the (i)/(j) split; "Human consumption", "Animal feed" and "Oil and
# oilcake" appear under both (c) (commercial use, i.e. including the export
# equivalent) and (i) (local market only) — for Jun 2026, (c) animal feed is
# 9,987 t against (i)'s 9,247 t. "Opening stock" and "Closing stock" appear
# again under (h), a block about transit tonnage explicitly *excluded* from
# everything above it.
#
# Labels are matched by normalised prefix because they drift between
# vintages: the 2024/25 final says "Total Processed for commercial use (i+j)"
# where 2026/27 says "…(i+j): (iv)", and (h)'s "Opening stock" is spelled
# "Openening stock" in the older file.
#
# Only components are stored. Local-market processing is
# processed_total − products_exported and is derived at read time, as the
# progressive delivery total is in Layer 23.
_SMD_METRICS: dict[tuple[str, str], str] = {
    ("a", "openingstock"): "opening_stock",
    ("b", "deliveriesdirectlyfromfarms"): "deliveries",
    ("b", "importsdestinedforrsa"): "imports",
    ("c", "totalprocessedforcommercialuse"): "processed_total",
    ("c", "humanconsumption"): "processed_human",
    ("c", "animalfeed"): "processed_feed",
    ("c", "oilandoilcake"): "processed_oil_oilcake",
    ("c", "withdrawnbyproducers"): "withdrawn_by_producers",
    ("c", "releasedtoendconsumer"): "released_to_end_consumers",
    ("c", "seedforplantingpurposes"): "seed_for_planting",
    ("d", "wholesoybeans"): "exports_whole",
    ("d", "borderposts"): "exports_border_posts",
    ("d", "harbours"): "exports_harbours",
    ("e", "netdispatches"): "sundries_net_dispatches",
    ("e", "surplusdeficit"): "sundries_surplus_deficit",
    ("f", "unutilisedstock"): "unutilised_stock",
    ("g", "storersandtraders"): "stock_storers_traders",
    ("g", "processors"): "stock_processors",
    ("j", "soybeansequivalentofproductsexported"): "products_exported",
    ("j", "africancountries"): "products_exported_africa",
    ("j", "othercountries"): "products_exported_other",
}

_SMD_VALUE_COLUMNS: tuple[str, ...] = tuple(dict.fromkeys(_SMD_METRICS.values()))

_SECTION_RE = re.compile(r"^\(([a-j])\)\s*")

# The section totals (b) Acquisition, (c) Utilisation and (e) Sundries are
# deliberately not stored: each is the sum of components that are. That makes
# the report's own balance — (a) + (b) − (c) − (d) − (e) = (f) — checkable
# from the stored row alone, which is the check below. It is a shape test
# with teeth: it is what catches a row mapped into the wrong section, the one
# failure mode label-matching cannot see (the numbers would still parse, they
# would just be the wrong numbers).
_SMD_BALANCE_TOLERANCE_MT = 1.0


def _as_float(value: object) -> float:
    """One spreadsheet cell as a float, NaN when it is not a number."""
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return float("nan")


def _normalise_label(value: str) -> str:
    """Lowercase, strip everything but a–z, drop a leading "(x)" marker."""
    text = _SECTION_RE.sub("", str(value).strip())
    return re.sub(r"[^a-z]", "", text.lower())


def _resolve_smd_links(html: str) -> dict[str, list[tuple[int, str]]]:
    """Map each tracked commodity to its season files, newest publication first.

    Returns ``{token: [(season_start_year, url), ...]}``. Where a season is
    published more than once (a season is re-issued monthly while active, and
    again as a final), the newest *filename* date wins; the workbook's own
    ``SMD-MMYYYY`` tag then decides how far its numbers run.

    Raises
    ------
    ScraperShapeError
        If a tracked commodity has no season file at all — the current
        season's is re-published every month, so its absence means the page
        or the filename convention changed.
    """
    newest: dict[str, dict[int, tuple[str, str]]] = {}

    for match in _SMD_LINK_RE.finditer(html):
        commodity_code = match.group("commodity")
        if commodity_code not in SAGIS_SMD_COMMODITIES:
            continue  # Mielies, Koring, Sonneblom, … — published, not tracked
        start, end = int(match.group("start")), int(match.group("end"))
        if end != start + 1:
            continue  # not a <season><season+1> pair — a different file
        published = match.group("published")
        seasons = newest.setdefault(commodity_code, {})
        current = seasons.get(start)
        if current is None or published > current[0]:
            seasons[start] = (published, match.group(0))

    missing = [t for t in SAGIS_SMD_COMMODITIES if t not in newest]
    if missing:
        raise ScraperShapeError(
            f"SAGIS SMD: no season workbook found for {missing} on "
            f"{SAGIS_MONTHLY_DATA_URL} — page or filename convention changed"
        )

    resolved: dict[str, list[tuple[int, str]]] = {}
    for commodity_code, seasons in newest.items():
        ordered = sorted(seasons.items(), reverse=True)
        resolved[commodity_code] = [(season, url) for season, (_, url) in ordered]
        logger.info(
            "SAGIS SMD %s: resolved %d season workbook(s) — %s",
            commodity_code, len(ordered), ", ".join(f"{s}/{s + 1}" for s, _ in ordered),
        )
    return resolved


def _report_month(df: pd.DataFrame, commodity: str) -> pd.Timestamp:
    """The report's own ``SMD-MMYYYY`` vintage stamp, as a first-of-month.

    Everything after ``report_month - 1`` in a season workbook is printed as
    a hard ``0``, not left blank — an unreported month and a month of zero
    trade are the same cell. Storing those zeros would publish a fabricated
    collapse in every series the moment a season opens, so the tag is
    required and the frame is cut at it.
    """
    for value in df.to_numpy().ravel():
        if isinstance(value, str):
            match = _SMD_TAG_RE.search(value)
            if match:
                return pd.Timestamp(
                    year=int(match.group("year")),
                    month=int(match.group("month")),
                    day=1,
                )
    raise ScraperShapeError(
        f"SAGIS SMD {commodity}: no SMD-MMYYYY report tag in the workbook — "
        "without it, unreported months cannot be told from months of zero"
    )


def _smd_month_columns(
    df: pd.DataFrame, commodity: str
) -> tuple[dict[Hashable, pd.Timestamp], int]:
    """Map spreadsheet column → first-of-month, plus the season's start year.

    The header row is found by content, never by position: it is the row
    carrying the most ``"Mmm[/Afr] YYYY"`` cells. The trailing
    "Mar/Mrt - Jun 2026" progressive column does not match that pattern and
    is skipped, which is what we want — it is a running total, and the map's
    rule is to store components.
    """
    best: dict[Hashable, pd.Timestamp] = {}

    for _, row in df.iterrows():
        found: dict[Hashable, pd.Timestamp] = {}
        for col, value in row.items():
            if not isinstance(value, str):
                continue
            match = _SMD_MONTH_RE.match(value.strip())
            if match:
                found[col] = pd.Timestamp(
                    year=int(match.group("year")),
                    month=_MONTH_NUMBERS[match.group("month")],
                    day=1,
                )
        if len(found) > len(best):
            best = found

    if len(best) < 12:
        raise ScraperShapeError(
            f"SAGIS SMD {commodity}: found {len(best)} month columns, expected "
            "12 — the season workbook's header row changed shape"
        )

    months = dict(sorted(best.items(), key=lambda item: item[1]))
    season_year = min(months.values()).year
    if min(months.values()).month != 3:
        raise ScraperShapeError(
            f"SAGIS SMD {commodity}: season starts in month "
            f"{min(months.values()).month}, expected March — the marketing "
            "season definition changed"
        )
    return months, season_year


def _check_smd_balance(row: dict, commodity: str) -> None:
    """Assert the report's own balance on one stored month.

    (a) opening + (b) acquisition − (c) utilisation − (d) exports
    − (e) sundries = (f) unutilised stock, with every term rebuilt from the
    components this layer stores. A row that does not balance means a line
    item was read out of the wrong section — hard-fail rather than store a
    plausible-looking balance sheet that is quietly wrong.
    """
    terms = (
        "opening_stock", "deliveries", "imports", "processed_total",
        "withdrawn_by_producers", "released_to_end_consumers",
        "seed_for_planting", "exports_whole", "sundries_net_dispatches",
        "sundries_surplus_deficit", "unutilised_stock",
    )
    if any(pd.isna(row[term]) for term in terms):
        return  # a blank cell is not evidence of a mis-mapped row

    acquisition = row["deliveries"] + row["imports"]
    utilisation = (
        row["processed_total"] + row["withdrawn_by_producers"]
        + row["released_to_end_consumers"] + row["seed_for_planting"]
    )
    sundries = row["sundries_net_dispatches"] + row["sundries_surplus_deficit"]
    closing = (
        row["opening_stock"] + acquisition - utilisation
        - row["exports_whole"] - sundries
    )
    drift = closing - row["unutilised_stock"]
    if abs(drift) > _SMD_BALANCE_TOLERANCE_MT:
        raise ScraperShapeError(
            f"SAGIS SMD {commodity}: month ending {row['month_end']} does not "
            f"balance — (a)+(b)-(c)-(d)-(e) = {closing:,.0f} MT against a "
            f"published (f) of {row['unutilised_stock']:,.0f} MT "
            f"({drift:+,.0f} MT); a line item was read from the wrong section"
        )


def _parse_smd_workbook(raw: bytes, commodity: str) -> pd.DataFrame:
    """Parse one season SMD workbook into the stored frame shape.

    Returns one row per *reported* month with columns: season_year,
    month_number (1 = March, SAGIS's own season position), month_end,
    report_month, the tonnage components of ``_SMD_METRICS``, and unit.
    """
    try:
        df = pd.read_excel(io.BytesIO(raw), engine="openpyxl", header=None)
    except Exception as exc:  # openpyxl raises a zoo of types on bad bytes
        raise ScraperShapeError(
            f"SAGIS SMD {commodity}: workbook unreadable — {exc}"
        ) from exc

    if df.empty:
        raise ScraperShapeError(f"SAGIS SMD {commodity}: workbook has no rows")

    report_month = _report_month(df, commodity)
    months, season_year = _smd_month_columns(df, commodity)

    # Walk the label columns, tracking which lettered section we are inside.
    values: dict[str, dict[Hashable, float]] = {}
    section = ""
    for _, row in df.iterrows():
        label = next(
            (row[col] for col in (0, 1, 2)
             if col in row.index and isinstance(row[col], str) and row[col].strip()),
            None,
        )
        if label is None:
            continue
        marker = _SECTION_RE.match(str(row[0]).strip()) if isinstance(row[0], str) else None
        if marker:
            section = marker.group(1)

        key = _normalise_label(label)
        for (want_section, want_prefix), column in _SMD_METRICS.items():
            if section != want_section or not key.startswith(want_prefix):
                continue
            if column in values:
                break  # first match wins; the sheet repeats some labels
            values[column] = {
                col: _as_float(row.get(col)) for col in months
            }
            break

    absent = [c for c in _SMD_VALUE_COLUMNS if c not in values]
    if absent:
        raise ScraperShapeError(
            f"SAGIS SMD {commodity}: no row matched {absent} — the report's "
            "line items were renamed or re-sectioned"
        )

    cutoff = report_month - pd.offsets.MonthEnd(1)
    rows: list[dict] = []
    for position, (col, month_start) in enumerate(months.items(), start=1):
        month_end = month_start + pd.offsets.MonthEnd(0)
        if month_end > cutoff:
            continue  # printed as 0 because it has not been reported yet
        record: dict = {
            "season_year": season_year,
            "month_number": position,
            "month_end": str(month_end.date()),
            "report_month": str(report_month.date()),
            "unit": "MT",
        }
        for column in _SMD_VALUE_COLUMNS:
            record[column] = values[column][col]
        _check_smd_balance(record, commodity)
        rows.append(record)

    if not rows:
        raise ScraperShapeError(
            f"SAGIS SMD {commodity}: season {season_year}/{season_year + 1} "
            f"reports nothing through {cutoff.date()} — the workbook and its "
            "own SMD tag disagree"
        )

    out = pd.DataFrame(rows)
    logger.info(
        "SAGIS SMD %s: season %d/%d — %d month(s) through %s (report %s)",
        SAGIS_SMD_COMMODITIES[commodity], season_year, season_year + 1,
        len(out), out["month_end"].max(), report_month.strftime("%Y-%m"),
    )
    return out


def fetch_sagis_supply_demand() -> FetchResult:
    """Fetch SAGIS's monthly soybean supply & demand (Layer 24).

    One row per reported month per season: opening stock, producer
    deliveries, imports, tonnes processed (with the oil-and-oilcake line that
    is South Africa's crush volume), whole-bean exports split border-posts vs
    harbours, the soybean equivalent of product exports, and closing stock
    split storers-vs-processors.

    Returns
    -------
    FetchResult
        ``ok`` with ``{commodity_key: DataFrame}``; ``failed`` when the
        landing page or a workbook cannot be downloaded or no longer matches
        the expected structure. A season that fails to parse fails the whole
        layer rather than silently shrinking the history.

        There is no ``empty`` path: the current season's workbook is
        re-published every month and always carries at least one reported
        month, so zero rows means the source broke.
    """
    logger.info("Fetching SAGIS monthly supply & demand ...")

    listing = _get(SAGIS_MONTHLY_DATA_URL, "monthly landing page")
    if not listing:
        return FetchResult.failed("SAGIS SMD: landing page download failed")

    try:
        links = _resolve_smd_links(listing.decode("utf-8", errors="replace"))
    except ScraperShapeError as exc:
        logger.error("SAGIS SMD: %s", exc)
        return FetchResult.failed(str(exc))

    data: dict[str, pd.DataFrame] = {}
    for commodity_code, seasons in links.items():
        frames: list[pd.DataFrame] = []
        for season, url in seasons:
            raw = _get(url, f"{commodity_code} {season}/{season + 1} SMD")
            if not raw:
                return FetchResult.failed(
                    f"SAGIS SMD: download failed for {commodity_code} "
                    f"{season}/{season + 1} ({url})"
                )
            try:
                frames.append(_parse_smd_workbook(raw, commodity_code))
            except ScraperShapeError as exc:
                logger.error("SAGIS SMD: %s", exc)
                return FetchResult.failed(str(exc))
        data[SAGIS_SMD_COMMODITIES[commodity_code]] = pd.concat(frames, ignore_index=True)

    return FetchResult.ok(data)


# Re-export for tests
__all__: Sequence[str] = (
    "_parse_dt_export",
    "_parse_smd_workbook",
    "_parse_week_end",
    "_resolve_dt_links",
    "_resolve_smd_links",
    "fetch_sagis_deliveries",
    "fetch_sagis_supply_demand",
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
