"""
Layer 2 — USDA crop and supply/demand data via the NASS QuickStats API.

The USDA publishes reports (WASDE, crop progress, export inspections)
that are the #1 driver of soybean price moves.  This fetcher pulls
structured data from their free API.

Sign up for a key at: https://quickstats.nass.usda.gov/api

Set it as an environment variable:
    export USDA_API_KEY="your-key-here"

Key concepts for learning:
    - REST APIs: you send an HTTP GET with query parameters,
      the server sends back JSON data.
    - requests.get() returns a Response object; .json() parses it.
    - try/except catches errors so one bad request doesn't crash everything.
    - Retry logic handles temporary network problems automatically.
"""

import json
import logging
import re
from datetime import date, datetime, timezone

import pandas as pd
import requests

from config import (
    INSPECTIONS_URL,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    USDA_API_KEY,
    USDA_BASE_URL,
    USDA_CROP_PROGRESS_COMMODITIES,
)
from fetchers._backoff import retry_sleep
from pipeline.results import FetchResult, ScraperShapeError

logger = logging.getLogger(__name__)


def _numeric_values(df: pd.DataFrame, context: str) -> pd.DataFrame:
    """Coerce the NASS ``Value`` column to numeric, dropping suppression
    sentinels — "(D)" withheld, "(NA)", "(Z)" — instead of storing them raw."""
    if "Value" not in df.columns:
        return df
    values = pd.to_numeric(
        df["Value"].astype(str).str.replace(",", "", regex=False), errors="coerce"
    )
    n_dropped = int(values.isna().sum())
    if n_dropped:
        logger.info(
            "Dropped %d suppressed/non-numeric Value rows (%s)", n_dropped, context
        )
    return df.assign(Value=values).dropna(subset=["Value"])


def _current_crop_year_end() -> int:
    """USDA reports forecasts up to the next marketing year — current year + 1."""
    return datetime.now(timezone.utc).year + 1


def fetch_usda(
    commodity: str,
    year_start: int = 2020,
    year_end: int | None = None,
    stat_category: str = "PRODUCTION",
) -> pd.DataFrame:
    """
    Pull annual survey data for a commodity from USDA QuickStats.

    Parameters
    ----------
    commodity : str
        e.g. "SOYBEANS", "CORN"
    year_start, year_end : int
        Range of crop years to request.
    stat_category : str
        "PRODUCTION", "AREA HARVESTED", "YIELD", etc.

    Returns
    -------
    pd.DataFrame   (empty if the API key is missing or request fails)
    """
    if not USDA_API_KEY:
        logger.warning("USDA_API_KEY not set — skipping USDA fetch.")
        logger.info("  Get a free key: https://quickstats.nass.usda.gov/api")
        return pd.DataFrame()

    if year_end is None:
        year_end = _current_crop_year_end()

    params = {
        "key":                USDA_API_KEY,
        "commodity_desc":     commodity,
        "statisticcat_desc":  stat_category,
        "agg_level_desc":     "NATIONAL",
        "source_desc":        "SURVEY",
        "year__GE":           str(year_start),
        "year__LE":           str(year_end),
        "format":             "JSON",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Requesting USDA data for %s / %s (attempt %d) ...",
                        commodity, stat_category, attempt)
            resp = requests.get(USDA_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)

            if resp.status_code != 200:
                logger.warning("HTTP %d: %s", resp.status_code, resp.text[:200])
                if attempt < MAX_RETRIES:
                    retry_sleep(attempt)
                    continue
                return pd.DataFrame()

            payload = resp.json()

            # The API wraps rows in a "data" key
            rows = payload.get("data", [])
            if not rows:
                logger.info("No rows returned for %s / %s.", commodity, stat_category)
                return pd.DataFrame()

            df = pd.DataFrame(rows)

            # Keep only the most useful columns
            keep = [
                "year", "short_desc", "Value", "unit_desc",
                "state_name", "reference_period_desc",
            ]
            keep = [c for c in keep if c in df.columns]
            df = df[keep]
            df = _numeric_values(df, f"{commodity}/{stat_category}")

            logger.info("Got %d rows for %s / %s.", len(df), commodity, stat_category)
            return df

        except (requests.RequestException, json.JSONDecodeError) as exc:
            logger.warning(
                "Attempt %d/%d failed for USDA %s/%s: %s",
                attempt, MAX_RETRIES, commodity, stat_category, exc,
            )
            if attempt < MAX_RETRIES:
                retry_sleep(attempt)

    logger.error("All %d attempts failed for USDA %s/%s — returning empty DataFrame",
                 MAX_RETRIES, commodity, stat_category)
    return pd.DataFrame()


def fetch_soybean_overview() -> dict[str, pd.DataFrame]:
    """Convenience: pull production + area harvested + yield for soybeans."""
    results = {}
    for stat in ("PRODUCTION", "AREA HARVESTED", "YIELD"):
        results[stat] = fetch_usda("SOYBEANS", stat_category=stat)
    return results


def fetch_crop_progress(
    commodity: str = "SOYBEANS",
    year_start: int = 2020,
    year_end: int | None = None,
) -> pd.DataFrame:
    """
    Fetch weekly crop condition and progress data from USDA NASS.

    This is the most price-moving weekly report for US crops. It tells you:
        - What % of the crop is planted, emerged, blooming, mature, harvested
        - What % of the crop is rated good/excellent vs poor/very poor

    A drop in good/excellent % = potential yield loss = price rally.

    Parameters
    ----------
    commodity : str
        e.g. "SOYBEANS", "CORN"
    year_start, year_end : int
        Range of years to request.

    Returns
    -------
    pd.DataFrame
        Columns: year, week_ending, short_desc, Value, unit_desc, state_name
        Empty DataFrame if API key is missing or request fails.
    """
    if not USDA_API_KEY:
        logger.warning("USDA_API_KEY not set — skipping crop progress fetch.")
        return pd.DataFrame()

    if year_end is None:
        year_end = _current_crop_year_end()

    all_rows = []
    for stat_cat in ("PROGRESS", "CONDITION"):
        params = {
            "key":                USDA_API_KEY,
            "commodity_desc":     commodity,
            "statisticcat_desc":  stat_cat,
            "agg_level_desc":     "NATIONAL",
            "source_desc":        "SURVEY",
            "freq_desc":          "WEEKLY",
            "year__GE":           str(year_start),
            "year__LE":           str(year_end),
            "format":             "JSON",
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(
                    "Requesting USDA %s %s (attempt %d) ...",
                    commodity, stat_cat, attempt,
                )
                resp = requests.get(USDA_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)

                if resp.status_code != 200:
                    logger.warning("HTTP %d: %s", resp.status_code, resp.text[:200])
                    if attempt < MAX_RETRIES:
                        retry_sleep(attempt)
                        continue
                    break

                payload = resp.json()
                rows = payload.get("data", [])
                if rows:
                    all_rows.extend(rows)
                    logger.info("Got %d rows for %s/%s.", len(rows), commodity, stat_cat)
                else:
                    logger.info("No rows for %s/%s.", commodity, stat_cat)
                break

            except (requests.RequestException, json.JSONDecodeError) as exc:
                logger.warning(
                    "Attempt %d/%d failed for USDA %s/%s: %s",
                    attempt, MAX_RETRIES, commodity, stat_cat, exc,
                )
                if attempt < MAX_RETRIES:
                    retry_sleep(attempt)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # Keep useful columns
    keep = [
        "year", "week_ending", "short_desc", "Value",
        "unit_desc", "state_name", "statisticcat_desc",
    ]
    keep = [c for c in keep if c in df.columns]
    df = df[keep]
    df = _numeric_values(df, f"{commodity} crop progress")

    logger.info("Total crop progress/condition rows for %s: %d", commodity, len(df))
    return df


def fetch_all_crop_progress() -> dict[str, pd.DataFrame]:
    """
    Fetch crop progress/condition for all configured commodities.

    Returns dict keyed by commodity name (e.g. "SOYBEANS", "CORN").
    """
    results = {}
    for commodity in USDA_CROP_PROGRESS_COMMODITIES:
        results[commodity] = fetch_crop_progress(commodity)
    return results


def fetch_crush_data(
    year_start: int = 2020,
    year_end: int | None = None,
) -> pd.DataFrame:
    """
    Fetch monthly soybean crush volumes from USDA NASS.

    Uses the same API with statisticcat_desc="CRUSHED" (NASS retired the
    old "PROCESSING" category — it now 400s with "invalid query") to get
    how many tons of soybeans were actually crushed
    (short_desc "SOYBEANS - CRUSHED, MEASURED IN TONS").
    """
    return fetch_usda("SOYBEANS", year_start, year_end, stat_category="CRUSHED")


# Crops we extract from the AMS summary table.
_INSPECTION_CROPS: tuple[str, ...] = ("SOYBEANS", "CORN", "WHEAT")

# How stale the latest week_ending may be before we treat the report as broken.
_INSPECTION_MAX_AGE_DAYS = 60

_DATE_TOKEN_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")
_INT_TOKEN_RE = re.compile(r"^-?[\d,]+$")


def _parse_inspections(
    text: str,
    today: date | None = None,
) -> pd.DataFrame:
    """Column-aware parser for the AMS WA_GR101 export inspections report.

    The report contains a summary table where each header row exposes
    three MM/DD/YYYY week-ending dates followed by two market-year-to-date
    columns. We parse only the three weekly columns — those are the
    rows we store. The parser asserts (a) at least one row produced and
    (b) the most-recent week_ending falls within the last
    ``_INSPECTION_MAX_AGE_DAYS`` days, otherwise the upstream report
    is stale or broken and we raise ScraperShapeError.

    The ``today`` parameter is for testability — callers in production
    leave it as None and use the current UTC date.

    Returns a DataFrame with columns ``commodity``, ``week_ending``
    (ISO ``YYYY-MM-DD``), and ``inspections_mt`` (float).
    """
    lines = text.splitlines()

    header_idx = None
    date_tokens: list[str] = []

    # The header is the line that begins with the literal "GRAIN" anchor and
    # contains three MM/DD/YYYY tokens in order.
    for i, line in enumerate(lines):
        tokens = line.split()
        if not tokens or tokens[0].upper() != "GRAIN":
            continue
        dates = [t for t in tokens if _DATE_TOKEN_RE.match(t)]
        if len(dates) >= 3:
            header_idx = i
            date_tokens = dates[:3]
            break

    if header_idx is None:
        raise ScraperShapeError(
            "AMS inspections: could not locate header row 'GRAIN  MM/DD/YYYY  MM/DD/YYYY  MM/DD/YYYY'"
        )

    week_endings = [
        datetime.strptime(d, "%m/%d/%Y").date().isoformat()
        for d in date_tokens
    ]

    rows: list[dict[str, object]] = []
    for raw_line in lines[header_idx + 1:]:
        stripped = raw_line.strip()
        if not stripped:
            # Blank line still inside the table is tolerated; the table
            # ends at a "Total" row or a non-grain header further down.
            continue
        tokens = stripped.split()
        crop_token = tokens[0].upper()

        if crop_token in ("TOTAL", "CROP", "INCLUDES"):
            break
        if crop_token not in _INSPECTION_CROPS:
            continue

        # Pull the first three integer-like tokens after the crop name —
        # those are the values for the three week-ending columns.
        numeric_tokens = [t for t in tokens[1:] if _INT_TOKEN_RE.match(t)]
        if len(numeric_tokens) < 3:
            raise ScraperShapeError(
                f"AMS inspections: row '{stripped}' has fewer than 3 numeric columns"
            )

        for week_ending, value_token in zip(week_endings, numeric_tokens[:3], strict=False):
            try:
                value = float(value_token.replace(",", ""))
            except ValueError as exc:
                raise ScraperShapeError(
                    f"AMS inspections: could not parse '{value_token}' as a number"
                ) from exc
            rows.append({
                "commodity": crop_token.title(),
                "week_ending": week_ending,
                "inspections_mt": value,
            })

    if not rows:
        raise ScraperShapeError(
            "AMS inspections: header located but no soybean/corn/wheat rows beneath it"
        )

    df = pd.DataFrame(rows)

    today = today or datetime.now(timezone.utc).date()
    latest = max(datetime.strptime(d, "%Y-%m-%d").date() for d in df["week_ending"])
    age_days = (today - latest).days
    if age_days > _INSPECTION_MAX_AGE_DAYS:
        raise ScraperShapeError(
            f"AMS inspections: latest week_ending {latest} is {age_days} days old "
            f"(threshold {_INSPECTION_MAX_AGE_DAYS}) — upstream report appears stale"
        )

    return df


# Grain columns AMS may print in Table C / the destination table. The set
# of columns is NOT stable — it tracks what actually shipped that season
# (the 2026-08-06 report drops RYE and FLAXSEED and adds CANOLA), so the
# column order is read off the report header each run and mapped through
# this vocabulary. An upstream name that is not in here is drift: we
# hard-fail rather than silently attach the wrong grain to a column.
_HEADER_COMMODITY_NAMES: dict[str, str] = {
    "WHEAT": "Wheat",
    "RYE": "Rye",
    "CORN YELLOW": "Corn Yellow",
    "CORN WHITE": "Corn White",
    "CORN MIXED": "Corn Mixed",
    "SORGHUM": "Sorghum",
    "SOYBEANS": "Soybeans",
    "FLAXSEED": "Flaxseed",
    "CANOLA": "Canola",
    "BARLEY": "Barley",
    "OATS": "Oats",
    "SUNFLOWER": "Sunflower",
}
_TOTALS_COLUMN = "TOTALS"
_PORT_TABLE_TITLE = "BY REGION AND PORT AREA"
_PORT_NUM_RE = re.compile(r"^[\d,]+$")


def _parse_commodity_columns(
    lines: list[str],
    header_idx: int,
    label_tokens: tuple[str, ...],
    table: str,
) -> list[str]:
    """Read the grain column order off a WA_GR101 table header.

    The header is two lines: an optional continuation line carrying the
    first word of a two-word heading ("CORN" above "YELLOW"/"WHITE") and
    the header line itself, e.g.::

                                           CORN      CORN
          REGION    PORT AREA       WHEAT     YELLOW     WHITE   ...  TOTALS

    Continuation words are attached by character-span overlap. The
    leading row-label tokens (``REGION``, ``PORT AREA`` / ``COUNTRY``)
    are dropped and the trailing ``TOTALS`` column is required but not
    returned — callers store one value per returned commodity plus the
    TOTALS column they discard.

    Raises ScraperShapeError if the header is missing its TOTALS column
    or names a grain this parser has no mapping for.
    """
    header = lines[header_idx]
    above = lines[header_idx - 1] if header_idx > 0 else ""
    # A rule line ("------") is not a continuation row: a report whose
    # headings were all single-word would otherwise glue dashes onto every
    # column name and hard-fail as if the layout had drifted.
    if above.strip() and set(above.strip()) <= {"-"}:
        above = ""
    above_tokens = [(m.group(), m.start(), m.end()) for m in re.finditer(r"\S+", above)]

    names: list[str] = []
    for m in re.finditer(r"\S+", header):
        prefix = " ".join(
            tok for tok, start, end in above_tokens
            if start < m.end() and m.start() < end
        )
        names.append(f"{prefix} {m.group()}".strip().upper())

    # Drop the row-label columns ("REGION", "PORT AREA" / "COUNTRY").
    for expected in label_tokens:
        if not names or names[0] != expected:
            raise ScraperShapeError(
                f"AMS inspections: {table} header {header.strip()!r} does not start "
                f"with {' '.join(label_tokens)}"
            )
        names.pop(0)

    if not names or names[-1] != _TOTALS_COLUMN:
        raise ScraperShapeError(
            f"AMS inspections: {table} header {header.strip()!r} does not end "
            f"with a {_TOTALS_COLUMN} column"
        )
    names.pop()

    if not names:
        raise ScraperShapeError(
            f"AMS inspections: {table} header {header.strip()!r} has no grain columns"
        )

    unknown = [n for n in names if n not in _HEADER_COMMODITY_NAMES]
    if unknown:
        raise ScraperShapeError(
            f"AMS inspections: {table} header has unrecognised column(s) "
            f"{unknown} — upstream layout changed"
        )
    return [_HEADER_COMMODITY_NAMES[n] for n in names]


def _parse_port_flows(text: str) -> pd.DataFrame:
    """Parse WA_GR101 Table C — grain export inspections by region and port area.

    The table gives each port area's weekly inspected tonnage per grain
    (METRIC TONS), with a SUBTOTAL row per region (GULF, PACIFIC, ...) —
    the free US export-flow breakdown a physical buyer reads as "how much
    moved through the Gulf vs the PNW this week". SUBTOTAL rows are stored
    with ``port_area='SUBTOTAL'``; the grand TOTAL row is dropped.

    Returns columns: ``week_ending``, ``region``, ``port_area``,
    ``commodity``, ``inspections_mt``. Raises ScraperShapeError on any
    structural mismatch.
    """
    lines = text.splitlines()

    title_idx = None
    for i, line in enumerate(lines):
        if _PORT_TABLE_TITLE in line.upper():
            title_idx = i
            break
    if title_idx is None:
        raise ScraperShapeError(
            f"AMS inspections: no '{_PORT_TABLE_TITLE}' table title found"
        )

    week_ending = None
    for line in lines[title_idx + 1: title_idx + 4]:
        m = re.search(r"WEEK ENDING\s+([A-Z]{3}\s+\d{1,2},\s+\d{4})", line.upper())
        if m:
            week_ending = datetime.strptime(m.group(1), "%b %d, %Y").date().isoformat()
            break
    if week_ending is None:
        raise ScraperShapeError(
            "AMS inspections: port-area table has no 'WEEK ENDING' date line"
        )

    header_idx = None
    for i in range(title_idx + 1, min(title_idx + 10, len(lines))):
        if "REGION" in lines[i] and "PORT AREA" in lines[i]:
            header_idx = i
            break
    if header_idx is None:
        raise ScraperShapeError(
            "AMS inspections: port-area table header 'REGION  PORT AREA' not found"
        )

    commodities = _parse_commodity_columns(
        lines, header_idx, ("REGION", "PORT", "AREA"), "port-area table"
    )

    rows: list[dict[str, object]] = []
    region = ""
    for raw_line in lines[header_idx + 1:]:
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("-"):
            continue

        # Fields are separated by runs of 2+ spaces; single spaces stay
        # inside a field ("MISSISSIPPI R.", "ST LAWR SWY"). The region
        # field only appears on the first row of its group; continuation
        # and SUBTOTAL rows split with a leading empty field.
        fields = re.split(r"\s{2,}", raw_line.rstrip())
        if len(fields) < 3:
            continue
        if fields[0]:
            region = fields[0]
        port_area = fields[1]
        values = fields[2:]
        if port_area == "TOTAL":
            break

        if not all(_PORT_NUM_RE.match(v) for v in values):
            continue
        if len(values) != len(commodities) + 1:  # +1 for TOTALS
            raise ScraperShapeError(
                f"AMS inspections: port row '{stripped}' has {len(values)} numeric "
                f"columns, expected {len(commodities) + 1}"
            )

        for commodity, token in zip(commodities, values, strict=False):
            rows.append({
                "week_ending": week_ending,
                "region": region,
                "port_area": port_area,
                "commodity": commodity,
                "inspections_mt": float(token.replace(",", "")),
            })

    if not rows:
        raise ScraperShapeError(
            "AMS inspections: port-area table header located but no data rows"
        )
    return pd.DataFrame(rows)


# Destination table columns are read off its own header (same drift as
# Table C — the column set differs between the two tables and between
# reports). TOTALS is deliberately not stored.
_DEST_TABLE_TITLE = "BY REGION AND COUNTRY OF DESTINATION"


def _parse_destinations(text: str) -> pd.DataFrame:
    """Parse WA_GR101 — grain export inspections by region and destination country.

    The free "who is actually receiving US grain this week" breakdown
    (METRIC TONS): rows grouped by coast region (LAKES, ATLANTIC, GULF,
    PACIFIC, INTERIOR) with one row per destination country and a
    SUBTOTAL row per region (stored with ``country='SUBTOTAL'``, grand
    TOTAL dropped) — the destination-side sibling of ``_parse_port_flows``.

    Returns columns: ``week_ending``, ``region``, ``country``,
    ``commodity``, ``inspections_mt``. Raises ScraperShapeError on any
    structural mismatch.
    """
    lines = text.splitlines()

    title_idx = None
    for i, line in enumerate(lines):
        if _DEST_TABLE_TITLE in line.upper():
            title_idx = i
            break
    if title_idx is None:
        raise ScraperShapeError(
            f"AMS inspections: no '{_DEST_TABLE_TITLE}' table title found"
        )

    week_ending = None
    for line in lines[title_idx + 1: title_idx + 4]:
        m = re.search(r"WEEK ENDING\s+([A-Z]{3}\s+\d{1,2},\s+\d{4})", line.upper())
        if m:
            week_ending = datetime.strptime(m.group(1), "%b %d, %Y").date().isoformat()
            break
    if week_ending is None:
        raise ScraperShapeError(
            "AMS inspections: destination table has no 'WEEK ENDING' date line"
        )

    header_idx = None
    for i in range(title_idx + 1, min(title_idx + 12, len(lines))):
        if "REGION" in lines[i] and "COUNTRY" in lines[i]:
            header_idx = i
            break
    if header_idx is None:
        raise ScraperShapeError(
            "AMS inspections: destination table header 'REGION  COUNTRY' not found"
        )

    commodities = _parse_commodity_columns(
        lines, header_idx, ("REGION", "COUNTRY"), "destination table"
    )

    rows: list[dict[str, object]] = []
    region = ""
    for raw_line in lines[header_idx + 1:]:
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("-"):
            continue

        # Same field convention as the port-area table: runs of 2+ spaces
        # separate fields, single spaces stay inside a field ("UN KINGDOM",
        # "COSTA RICA"). Region only appears on its group's first row.
        fields = re.split(r"\s{2,}", raw_line.rstrip())
        if len(fields) < 3:
            continue
        if fields[0]:
            region = fields[0]
        country = fields[1]
        values = fields[2:]
        if country == "TOTAL" or region == "TOTAL":
            break

        if not all(_PORT_NUM_RE.match(v) for v in values):
            continue
        if len(values) != len(commodities) + 1:  # +1 for TOTALS
            raise ScraperShapeError(
                f"AMS inspections: destination row '{stripped}' has {len(values)} "
                f"numeric columns, expected {len(commodities) + 1}"
            )

        for commodity, token in zip(commodities, values, strict=False):
            rows.append({
                "week_ending": week_ending,
                "region": region,
                "country": country,
                "commodity": commodity,
                "inspections_mt": float(token.replace(",", "")),
            })

    if not rows:
        raise ScraperShapeError(
            "AMS inspections: destination table header located but no data rows"
        )
    return pd.DataFrame(rows)


def fetch_export_inspections() -> FetchResult:
    """Fetch the weekly USDA AMS grain export inspections report.

    Returns ``FetchResult.ok`` with two frames:
    ``data['inspections']`` — commodity, week_ending, inspections_mt
    (national weekly totals), ``data['port_flows']`` — week_ending,
    region, port_area, commodity, inspections_mt (Table C breakdown),
    and ``data['destinations']`` — week_ending, region, country,
    commodity, inspections_mt (destination-country breakdown). The two
    supplementary tables may be absent if only their layout changed.
    Transport failure or a stale/misshapen main table is
    ``FetchResult.failed``.
    """
    resp = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Fetching AMS export inspections (attempt %d) ...", attempt)
            resp = requests.get(INSPECTIONS_URL, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            logger.warning("Inspections attempt %d failed: %s", attempt, exc)
            resp = None
            if attempt < MAX_RETRIES:
                retry_sleep(attempt)
            continue

        if resp.status_code != 200:
            logger.warning("HTTP %d for inspections", resp.status_code)
            if attempt < MAX_RETRIES:
                retry_sleep(attempt)
                continue
            return FetchResult.failed(f"AMS inspections: HTTP {resp.status_code}")
        break

    if resp is None or resp.status_code != 200:
        return FetchResult.failed("AMS inspections: download failed after retries")

    try:
        df = _parse_inspections(resp.text)
    except ScraperShapeError as exc:
        logger.error("AMS inspections: shape changed — %s", exc)
        return FetchResult.failed(str(exc))

    data = {"inspections": df}
    try:
        data["port_flows"] = _parse_port_flows(resp.text)
    except ScraperShapeError as exc:
        # Port table is supplementary — losing it degrades, not fails.
        logger.error("AMS inspections: port-area table unparseable — %s", exc)
    try:
        data["destinations"] = _parse_destinations(resp.text)
    except ScraperShapeError as exc:
        # Destination table is supplementary too.
        logger.error("AMS inspections: destination table unparseable — %s", exc)

    logger.info(
        "Parsed %d inspection rows, %d port-flow rows, %d destination rows.",
        len(df), len(data.get("port_flows", ())), len(data.get("destinations", ())),
    )
    return FetchResult.ok(data)


# ── Quick self-test ─────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_soybean_overview()
    logger.info("=== USDA Soybean Summary ===")
    for stat, df in data.items():
        if df.empty:
            logger.info("  %s: no data (API key missing?)", stat)
        else:
            logger.info("  %s: %d rows", stat, len(df))
            logger.info("\n%s", df.head(3).to_string(index=False))

    progress = fetch_all_crop_progress()
    logger.info("=== USDA Crop Progress ===")
    for crop, df in progress.items():
        if df.empty:
            logger.info("  %s: no data", crop)
        else:
            logger.info("  %s: %d rows", crop, len(df))
