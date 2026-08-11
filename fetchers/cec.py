"""
Layer 25 — Crop Estimates Committee (CEC), South Africa's official crop estimates.

Source: https://www.sagis.org.za/crop-estimates-committee-2/
Issuer: the Crop Estimates Committee, a statutory committee of the national
        Department of Agriculture (the department that used to publish as
        DALRRD). SAGIS mirrors every release.
Licence: SAGIS grants reproduction with acknowledgement of the source; the
        underlying release is an official government statistic carrying no
        copyright notice of its own. Every surface renders
        ``config.CEC_ATTRIBUTION``, which names both the issuer and the host.

What this is
------------
South Africa's own area / production estimate for the summer crops, revised
monthly through the season — the structural analogue of Layer 15 (CONAB).
Unlike CONAB it is **not** an independent second opinion: USDA's PSD carries
the CEC's final figure verbatim (2,770,000 / 1,848,000 / 2,800,000 tons for
the 2023 / 2024 / 2025 crops are all exact ties, at PSD year = CEC year − 1),
so what this layer buys is the *in-season revision path* and the *lead* on
the PSD number, not a divergence between two agencies. See #204.

Why the SAGIS mirror is the fetch target
----------------------------------------
The issuer's own site is the weaker host: ``dalrrd.gov.za`` no longer
resolves at all (verified 2026-08-12) and the department's replacement site,
nda.gov.za, publishes no CEC listing we could parse. SAGIS carries every
release back to 1999 on one page. The mirror-stops-updating failure mode is
covered by the ``LAYER_MAX_DATA_AGE_DAYS`` recency budget: releases carry
their own date, so a frozen mirror ages the layer out on its own.

Which releases are parsed
-------------------------
The listing mixes four filename conventions and two file formats:

    CEC_2026-07-28.pdf      current
    CEC-28-Aug-2025.pdf     2025
    CEC_2025-09.pdf         2025, no day in the name
    CEC-2024-12.doc         2013–2024, legacy binary .doc

Everything before ``CEC_HISTORY_START`` is ``.doc`` (a format nothing in the
stack can read) or a layout from a different decade, so the window starts at
that date and every PDF inside it must parse or the layer fails. The whole
window is ~23 files and ~18 MB, which re-downloads in under 10 seconds, so
the layer re-reads it in full on every run and self-heals on an empty CI
database exactly as Layer 22 does. Nothing here needs ``data/history/``.

The release date is read from the document body, never from the filename —
``CEC_2025-09.pdf`` has no day in its name, and where a name does carry a full
date it is the upload date, which can run a day ahead of the report itself.

Report families, and why the parse is column-mapped rather than positional
--------------------------------------------------------------------------
One release per month, but the summary table's shape changes with the point
in the season:

    Jan   preliminary area planted     area only, no production number
    Feb   revised area + 1st forecast  no previous-forecast column
    Mar–  2nd … 9th forecast           area, current forecast, previous forecast
    Nov   final production estimate
    Oct   intentions to plant (next season) *and* the 9th forecast (this one)
    Feb   CELC final crop figures      a different table on a different page
    Dec / mid-month releases           winter cereals only, no summer crops

So a fixed column index is wrong on at least four of those, and the header
rows a parser would naturally anchor on are the ones the source gets wrong:

  * **The units row lies.** ``CEC_2026-02-26.pdf`` heads its column (B) —
    "1st forecast", 2 661 425 — ``Ha``, and it is tons.
  * **The change formula lies.** ``CEC-27-Feb-2025.pdf`` declares
    ``(B) ÷ (D)`` over a column that prints ``(A) ÷ (C)``.

The per-column *labels* have been right in every release, so those are what
the columns are matched on — "area planted" / "intentions" for hectares,
"forecast" / "estimate" / "final crop" for tons — paired with the year each
column carries, with the season's leftmost matching column winning (tables
print the current estimate left of the previous one). An unrecognised label
hard-fails rather than being skipped.

Two checks stand behind the parse, both against the source's own arithmetic:
the printed Change % must be reproducible from the parsed row, and the
implied yield must land in an agronomically sane band. Either failing raises
``ScraperShapeError`` — a release that no longer parses stops the layer
rather than storing a plausible wrong number.
"""

from __future__ import annotations

import calendar
import io
import logging
import re
from collections.abc import Sequence
from datetime import date

import pandas as pd
import pypdf
import requests

from config import (
    CEC_CROPS,
    CEC_HISTORY_START,
    CEC_REPORTS_URL,
    CEC_YIELD_BAND_T_HA,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
)
from fetchers._backoff import retry_sleep
from pipeline.results import FetchResult, ScraperShapeError

logger = logging.getLogger(__name__)

# SAGIS serves both the listing page and the PDFs to a plain project UA
# (verified 2026-08-12). Named rather than browser-spoofed, per the licence.
_HEADERS = {
    "User-Agent": (
        "Mirror-Market/1.0 (commodity market research; "
        "+https://github.com/philipbergman6-glitch/Mirror-Market)"
    ),
}

_PDF_LINK_RE = re.compile(r'https://[^"\'\s]*?/CEC[^"\'\s]*?\.pdf', re.IGNORECASE)

# Full dates that appear in a filename, in the two conventions that carry one.
_NAME_ISO_RE = re.compile(r"CEC[-_](\d{4})-(\d{2})-(\d{2})\.pdf$", re.IGNORECASE)
_NAME_DMY_RE = re.compile(r"CEC[-_](\d{1,2})-([A-Za-z]{3})-(\d{4})\.pdf$", re.IGNORECASE)
_NAME_MONTH_RE = re.compile(r"CEC[-_](\d{4})-(\d{2})\.pdf$", re.IGNORECASE)

_MONTHS = {name.lower(): i for i, name in enumerate(calendar.month_name) if name}
_MONTH_ABBR = {name.lower(): i for i, name in enumerate(calendar.month_abbr) if name}

# "28 July / Julie 2026", "28 October/Oktober 2025", "12 February 2026"
_RELEASE_DATE_RE = re.compile(
    r"^\s*(\d{1,2})\s+(" + "|".join(m.capitalize() for m in _MONTHS) + r")"
    r"\s*/?\s*[A-Za-zë]*\s*(\d{4})\s*$",
    re.MULTILINE,
)

# Family A: the summer-crop summary table, one per season per release.
_SUMMARY_TITLE_RE = re.compile(
    r"^SUMMER CROPS\s*[–—-]{0,2}\s*(?P<title>[A-Z][^:]*?):\s*(?P<season>\d{4})\s*$"
)
# Family B: the CELC release that finalises the previous season's crop against
# SAGIS's actual deliveries. Its number supersedes November's "final estimate".
_FINAL_TITLE_RE = re.compile(
    r"^SUMMARY:\s*FINAL AREA PLANTED AND CROP PRODUCTION FIGURES.*?FOR\s*(?P<season>\d{4})",
    re.IGNORECASE,
)

_YEAR_RE = re.compile(r"^(19|20)\d{2}$")
_YEAR_TOKEN_RE = re.compile(r"\b(19|20)\d{2}\b")
_FORMULA_RE = re.compile(r"\((?P<num>[A-Z])\)\s*[÷/]\s*\((?P<den>[A-Z])\)")
_LETTER_RE = re.compile(r"\(([A-Z])\)")
# A number in SA convention: leading group of 1-3 digits, then 3-digit groups.
_NUMBER_RE = re.compile(r"\d{1,3}(?:[  ]\d{3})*")
# The Change cell, told apart from the values by its decimal comma: every
# other figure in these summary tables is a whole number of hectares or tons.
_PERCENT_RE = re.compile(r"([+-]?\s?\d+[.,]\d+)\s*%?\s*$")

_ORDINALS = {
    "FIRST": 1, "SECOND": 2, "THIRD": 3, "FOURTH": 4, "FIFTH": 5,
    "SIXTH": 6, "SEVENTH": 7, "EIGHTH": 8, "NINTH": 9, "TENTH": 10,
}

# Column-label vocabulary. Checked in this order; a label matching neither
# (and not the Change column) is a shape change, not something to skip.
_AREA_LABELS = ("area planted", "opp beplant", "oppervlakte", "intentions", "voorneme")
_PRODUCTION_LABELS = (
    "forecast", "skatting", "estimate", "final crop", "finale oes", "crop",
)
_CHANGE_LABELS = ("change", "verandering")

# Sanity band on the stored levels themselves — a column mix-up that survived
# the label match would land far outside these.
_AREA_BAND_HA = (10_000, 5_000_000)
_PRODUCTION_BAND_T = (5_000, 30_000_000)

# How far (in layout-mode character columns) a header cell may sit from a
# column's centre and still be read as that column's label. Capped at half
# the measured column pitch, so a narrow table cannot bleed labels sideways.
_LABEL_COLUMN_TOLERANCE = 12.0
# Tolerance on the recomputed Change %, in percentage points. The source
# prints two decimals, so this only has to absorb its own rounding.
_CHANGE_TOLERANCE_PCT = 0.06


# ── HTTP ─────────────────────────────────────────────────────────────────────

def _get(session: requests.Session, url: str, what: str) -> bytes:
    """GET ``url`` with retry/backoff. Returns b"" on transport failure."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, headers=_HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.content
            logger.warning("CEC %s: HTTP %d (attempt %d)", what, resp.status_code, attempt)
        except requests.RequestException as exc:
            logger.warning("CEC %s: request failed (attempt %d): %s", what, attempt, exc)
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)
    return b""


# ── Listing page ─────────────────────────────────────────────────────────────

def _filename_date(url: str) -> date | None:
    """Release date from the filename, or None when it doesn't carry one.

    Only ever used to *check* the date read from the document body, never to
    replace it: two of the four filename conventions on the page omit the day.
    """
    name = url.rsplit("/", 1)[-1]
    m = _NAME_ISO_RE.search(name)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    m = _NAME_DMY_RE.search(name)
    if m:
        month = _MONTH_ABBR.get(m.group(2).lower())
        if month:
            return date(int(m.group(3)), month, int(m.group(1)))
    return None


def _in_window(url: str) -> bool:
    """Is this PDF inside the parseable window (``CEC_HISTORY_START`` on)?

    Judged on whatever date the filename exposes — a month is enough to place
    a file relative to the cutoff, and files that expose nothing datable are
    from the pre-2013 era and excluded.
    """
    stamped = _filename_date(url)
    if stamped is not None:
        return stamped >= CEC_HISTORY_START
    m = _NAME_MONTH_RE.search(url.rsplit("/", 1)[-1])
    if m:
        return date(int(m.group(1)), int(m.group(2)), 28) >= CEC_HISTORY_START
    return False


def _resolve_report_links(html: str) -> list[str]:
    """Every CEC release PDF on the listing page inside the parse window.

    Raises
    ------
    ScraperShapeError
        When the page yields no in-window PDF at all — the listing carries a
        decade of releases, so an empty result means the page changed.
    """
    urls = {u for u in _PDF_LINK_RE.findall(html) if _in_window(u)}
    # The release-date calendar ("CEC_Dates_2026.pdf") sits on the same page
    # and carries no estimates.
    urls = {u for u in urls if "dates" not in u.rsplit("/", 1)[-1].lower()}
    if not urls:
        raise ScraperShapeError(
            f"CEC: no report PDF dated {CEC_HISTORY_START} or later found on "
            f"{CEC_REPORTS_URL} — page or filename convention changed"
        )
    return sorted(urls)


# ── Page geometry ────────────────────────────────────────────────────────────

def _layout_lines(page: pypdf.PageObject) -> list[str]:
    """The page's text in layout mode, one string per visual line.

    Layout mode preserves the column structure with runs of spaces, which is
    what makes these tables readable at all: plain extraction concatenates
    cells, and space-separated thousands then make ``"529 000 635 750"``
    indistinguishable from one ten-digit number. Only the full-width summary
    tables are parsed here — the per-crop province tables sit two-up on the
    page and layout mode interleaves them.
    """
    return page.extract_text(extraction_mode="layout").splitlines()


def _cells(line: str) -> list[tuple[int, str]]:
    """``(start column, text)`` for each cell on a layout-mode line.

    Two or more spaces separate columns; a single space is a thousands
    separator inside one number.
    """
    return [(match.start(), match.group(0).strip())
            for match in re.finditer(r"\S(?:\S| (?! ))*", line)]


def _centre(start: int, text: str) -> float:
    return start + len(text) / 2


def _numbers(text: str) -> list[float]:
    """Every whole number in one cell, thousands separators removed."""
    return [float(match.group(0).replace(" ", "").replace(" ", ""))
            for match in _NUMBER_RE.finditer(text)]


# ── Family A: the summer-crop summary table ──────────────────────────────────

def _classify_column(label: str) -> str:
    """"area" / "production" / "change" for one header label."""
    low = label.lower()
    if any(token in low for token in _CHANGE_LABELS):
        return "change"
    if any(token in low for token in _AREA_LABELS):
        return "area"
    if any(token in low for token in _PRODUCTION_LABELS):
        return "production"
    raise ScraperShapeError(f"CEC: unrecognised summary column label {label!r}")


def _estimate_kind(title: str) -> tuple[str, int | None]:
    """``(kind, forecast_seq)`` for a summary-table title."""
    upper = title.upper()
    if "INTENTIONS TO PLANT" in upper:
        return "intentions", None
    if "PRELIMINARY AREA PLANTED" in upper:
        return "preliminary_area", None
    if "FINAL PRODUCTION ESTIMATE" in upper:
        return "final_estimate", None
    # Extraction sometimes splits a word across runs ("F IFTH"), so ordinals
    # are matched with the whitespace taken out.
    squashed = re.sub(r"\s+", "", upper)
    for word, seq in _ORDINALS.items():
        if f"{word}PRODUCTIONFORECAST" in squashed:
            return "forecast", seq
    raise ScraperShapeError(f"CEC: unrecognised summary table title {title!r}")


def _parse_summary_table(
    lines: Sequence[str], start: int, title: str, season: int
) -> list[dict]:
    """Parse one ``SUMMER CROPS – …: YYYY`` table into per-crop records.

    The header is read rather than assumed: the column set changes four times
    a season (no previous-forecast column in February, hectares instead of
    tons in January, a next-season intentions table in October), and the
    units row cannot be trusted — ``CEC_2026-02-26.pdf`` labels its tons
    column "Ha". Columns are anchored on the year row and named by the labels
    sitting above them.
    """
    years: list[tuple[float, int]] = []
    label_lines: list[str] = []
    formula: tuple[str, str] | None = None
    letters: list[str] = []
    body_start: int | None = None

    for index in range(start + 1, len(lines)):
        cells = _cells(lines[index])
        if not cells:
            continue
        # The year row sometimes shares its line with the row stub
        # ("CROP/GEWAS"), so a stray non-numeric cell is allowed — but no
        # other figure may sit on it, or this is a data row.
        # ("CROP/GEWAS", or a "2026 vs 2025" change header), so a stray
        # non-numeric cell is allowed — but no other figure may sit on it, or
        # this is a data row.
        stamped = [(pos, text) for pos, text in cells if _YEAR_RE.match(text)]
        others = [_YEAR_TOKEN_RE.sub("", text)
                  for _, text in cells if not _YEAR_RE.match(text)]
        if len(stamped) >= 2 and not any(any(c.isdigit() for c in t) for t in others):
            years = [(_centre(pos, text), int(text)) for pos, text in stamped]
            continue
        match = _FORMULA_RE.search(lines[index])
        if match and years:
            formula = (match.group("num"), match.group("den"))
            letters = _LETTER_RE.findall(_FORMULA_RE.sub("", lines[index]))
            body_start = index + 1
            break
        if not years:
            label_lines.append(lines[index])

    if not years:
        raise ScraperShapeError(f"CEC: no year row under {title!r} ({season})")
    if body_start is None or formula is None or len(letters) != len(years):
        raise ScraperShapeError(
            f"CEC: {title!r} ({season}) header carries {len(letters)} column "
            f"letters for {len(years)} year columns, or no change formula"
        )

    kinds = [_classify_column(label) for label in _column_labels(label_lines, years)]

    records = []
    for index in range(body_start, len(lines)):
        text = lines[index].strip()
        if _SUMMARY_TITLE_RE.match(text) or text.upper().startswith(("NOTE", "NOTA")):
            break
        crop = _match_crop(text)
        if crop is None:
            continue
        values, change = _row_values(lines[index], len(years))
        records.append(_crop_record(
            crop, season, title, values, change, years, kinds, letters, formula
        ))
    return records


def _column_labels(
    label_lines: Sequence[str], years: Sequence[tuple[float, int]]
) -> list[str]:
    """Header label per column, assembled from the lines above the year row.

    Labels wrap over two or three lines, so each cell is assigned to the
    column it sits over. A cell further than half a column pitch from every
    column belongs to none — which is how the row stub ("CROP/GEWAS") and the
    trailing "Change/Verandering" header stay out of the estimate columns'
    labels.
    """
    pitch = (
        (years[-1][0] - years[0][0]) / (len(years) - 1)
        if len(years) > 1 else 2 * _LABEL_COLUMN_TOLERANCE
    )
    tolerance = min(pitch / 2, _LABEL_COLUMN_TOLERANCE)

    parts: list[list[str]] = [[] for _ in years]
    for line in label_lines:
        for pos, text in _cells(line):
            distance, index = min(
                (abs(_centre(pos, text) - centre), i)
                for i, (centre, _) in enumerate(years)
            )
            if distance <= tolerance:
                parts[index].append(text)

    labels = [" ".join(part).strip() for part in parts]
    missing = [i for i, label in enumerate(labels) if not label]
    if missing:
        raise ScraperShapeError(
            f"CEC: summary columns {missing} carry no header label — the "
            "table's header layout changed"
        )
    return labels


def _match_crop(text: str) -> str | None:
    """The stored commodity key for a summary row, or None if untracked."""
    for source_name, key in CEC_CROPS.items():
        if re.match(rf"^{re.escape(source_name)}\b", text.strip(), re.IGNORECASE):
            return key
    return None


def _row_values(line: str, columns: int) -> tuple[list[float], float | None]:
    """``(column values, change %)`` for one crop row.

    The Change cell is told apart from the values by its decimal comma —
    every other figure in these tables is a whole number of hectares or tons.
    A row that does not yield exactly one value per column is a shape change,
    not something to patch up: the columns would be silently misaligned.
    """
    values: list[float] = []
    change = None
    for _, text in _cells(line):
        match = _PERCENT_RE.search(text)
        if match:
            change = float(match.group(1).replace(",", ".").replace(" ", ""))
            text = text[: match.start()]
        values.extend(_numbers(text))

    if len(values) != columns:
        raise ScraperShapeError(
            f"CEC: row {line.strip()!r} yielded {len(values)} values for "
            f"{columns} columns"
        )
    return values, change


def _crop_record(
    crop: str,
    season: int,
    title: str,
    values: Sequence[float],
    change: float | None,
    years: Sequence[tuple[float, int]],
    kinds: Sequence[str],
    letters: Sequence[str],
    formula: tuple[str, str],
) -> dict:
    """One stored record for a crop row.

    Where a season has two columns of a kind — the current and the previous
    forecast, or the preliminary area and the intentions — the leftmost wins,
    which is the table's own ordering. The superseded figure is not stored:
    it is the previous release's own row, and the map's standing rule is to
    keep components rather than restatements.
    """
    area = production = None
    for (_, year), kind, value in zip(years, kinds, values, strict=True):
        if year != season:
            continue
        if kind == "area" and area is None:
            area = value
        elif kind == "production" and production is None:
            production = value

    if area is None and production is None:
        raise ScraperShapeError(
            f"CEC: {crop} {season} ({title!r}) has neither an area nor a "
            "production column for its own season"
        )

    _check_change(crop, season, title, values, change, letters, formula)
    _check_levels(crop, season, title, area, production)

    kind, seq = _estimate_kind(title)
    return {
        "commodity": crop,
        "season_year": season,
        "estimate_kind": kind,
        "forecast_seq": seq,
        "forecast_label": title.capitalize(),
        "area_ha": area,
        "production_t": production,
    }


def _check_change(
    crop: str,
    season: int,
    title: str,
    values: Sequence[float],
    change: float | None,
    letters: Sequence[str],
    formula: tuple[str, str],
) -> None:
    """Recompute the printed Change % from the formula the table declares.

    The strongest check available, because it is the source's own arithmetic:
    it ties two parsed numbers to a third the report printed itself, so a row
    split into the wrong columns cannot come out consistent. Skipped only
    where the source prints no change at all (a dash, as in September 2025).

    The declared formula is not always the one the report used —
    ``CEC-27-Feb-2025.pdf`` heads its change column ``(B) ÷ (D)`` and prints
    ``(A) ÷ (C)`` for every row, the same kind of header slip as the units
    row a year later. So a mismatch falls back to asking whether *any* pair
    of the parsed columns reproduces the printed figure: that still pins
    three numbers of the row against each other, which is what the check is
    for, and it is logged rather than swallowed. Only a row no pair can
    explain is a bad split.
    """
    if change is None:
        return
    index = {letter: i for i, letter in enumerate(letters)}
    numerator, denominator = formula
    if numerator not in index or denominator not in index:
        raise ScraperShapeError(
            f"CEC: {crop} {season} change formula ({numerator}) ÷ ({denominator}) "
            f"names a column outside {list(index)}"
        )

    def matches(num: int, den: int) -> bool:
        if values[den] == 0:
            return False
        return abs((values[num] / values[den] - 1) * 100 - change) <= _CHANGE_TOLERANCE_PCT

    if matches(index[numerator], index[denominator]):
        return

    alternative = next(
        (
            (letters[num], letters[den])
            for num in range(len(values))
            for den in range(len(values))
            if num != den and matches(num, den)
        ),
        None,
    )
    if alternative is None:
        raise ScraperShapeError(
            f"CEC: {crop} {season} ({title!r}) prints a change of {change:+.2f}% "
            f"that no pair of the parsed row reproduces — declared "
            f"({numerator}) ÷ ({denominator}), parsed {values} — the row was "
            "split into the wrong columns"
        )
    logger.warning(
        "CEC: %s %d (%r) heads its change column (%s) ÷ (%s) but prints "
        "(%s) ÷ (%s) — using the printed figure to check the split",
        crop, season, title, numerator, denominator, *alternative,
    )


def _check_levels(
    crop: str, season: int, title: str, area: float | None, production: float | None
) -> None:
    """Reject levels no South African summer crop could carry.

    A column mix-up that satisfied both the label match and the change
    formula would still have to survive this: hectares read as tons, or a
    maize row read as soybeans, lands outside the bands or the yield window.
    """
    if area is not None and not _AREA_BAND_HA[0] <= area <= _AREA_BAND_HA[1]:
        raise ScraperShapeError(
            f"CEC: {crop} {season} ({title!r}) area {area:,.0f} ha is outside "
            f"{_AREA_BAND_HA} — column mapping is wrong"
        )
    if production is not None and not (
        _PRODUCTION_BAND_T[0] <= production <= _PRODUCTION_BAND_T[1]
    ):
        raise ScraperShapeError(
            f"CEC: {crop} {season} ({title!r}) production {production:,.0f} t is "
            f"outside {_PRODUCTION_BAND_T} — column mapping is wrong"
        )
    if area and production:
        yield_t_ha = production / area
        low, high = CEC_YIELD_BAND_T_HA
        if not low <= yield_t_ha <= high:
            raise ScraperShapeError(
                f"CEC: {crop} {season} ({title!r}) implies {yield_t_ha:.2f} t/ha "
                f"({production:,.0f} t on {area:,.0f} ha), outside {low}-{high}"
            )


# ── Family B: the CELC final-crop release ────────────────────────────────────

def _parse_final_table(lines: Sequence[str], start: int, season: int) -> list[dict]:
    """Parse the CELC "final area planted and crop production figures" table.

    A different table in a different release: once the season closes, the
    Crop Estimates Liaison Committee recomputes the crop from SAGIS's actual
    producer deliveries plus an on-farm usage survey. Its figure supersedes
    November's final estimate (2,771,225 → 2,800,000 t for the 2025 soybean
    crop) and is the one every later report quotes as "final crop", so a
    layer that stopped at November would disagree with the source's own
    prior-year column forever.

    Columns are fixed here — (A) final area ha, (B) final crop tons, (C) the
    CEC's November area, (D) the CEC's November estimate — because the header
    carries no year row to anchor on. The printed (B) ÷ (D) still checks the
    split, and the same level and yield bands still apply.
    """
    records = []
    for index in range(start + 1, len(lines)):
        text = lines[index].strip()
        # The release carries a second table below this one, breaking the
        # same crops down into deliveries and on-farm use.
        if re.sub(r"\s+", " ", text.upper()).startswith("CALCULATION SUMMARY"):
            break
        crop = _match_crop(text)
        if crop is None:
            continue
        values, change = _row_values(lines[index], 4)
        area, production = values[0], values[1]
        _check_change(
            crop, season, "final crop", values, change, ["A", "B", "C", "D"], ("B", "D")
        )
        _check_levels(crop, season, "final crop", area, production)
        records.append({
            "commodity": crop,
            "season_year": season,
            "estimate_kind": "final_crop",
            "forecast_seq": None,
            "forecast_label": "Final crop (CELC, after utilisation survey)",
            "area_ha": area,
            "production_t": production,
        })
    return records


# ── One report ───────────────────────────────────────────────────────────────

def _parse_release_date(text: str, url: str) -> str:
    """ISO release date, read from the document body.

    The body is authoritative: two of the filename conventions on the page
    carry no day at all, and where a name does carry one it is the *upload*
    date, which can run a day ahead of the report (``CEC-28-Aug-2025.pdf`` is
    dated 27 August 2025 on its face). The date the committee put on the
    report is the one that keys the revision series; a wider disagreement is
    logged, since it would mean the mirror filed a release under the wrong
    name, but the body still wins.
    """
    match = _RELEASE_DATE_RE.search(text)
    if not match:
        raise ScraperShapeError(f"CEC: no release date in the body of {url}")
    stamped = date(
        int(match.group(3)), _MONTHS[match.group(2).lower()], int(match.group(1))
    )
    from_name = _filename_date(url)
    if from_name is not None and abs((from_name - stamped).days) > 3:
        logger.warning(
            "CEC: %s is named for %s but its body is dated %s — using the body",
            url, from_name, stamped,
        )
    return stamped.isoformat()


def _parse_report(raw: bytes, url: str) -> list[dict]:
    """Every tracked crop estimate in one release PDF.

    Returns an empty list for a release carrying no summer-crop table — the
    December and mid-month releases cover winter cereals only, which is
    normal and not an error.
    """
    try:
        reader = pypdf.PdfReader(io.BytesIO(raw))
    except Exception as exc:  # pypdf raises a zoo of types on bad bytes
        raise ScraperShapeError(f"CEC: {url} is unreadable — {exc}") from exc

    plain = "\n".join((page.extract_text() or "") for page in reader.pages)
    release_date = _parse_release_date(plain, url)

    records: list[dict] = []
    for page in reader.pages:
        lines = _layout_lines(page)
        for index, line in enumerate(lines):
            summary = _SUMMARY_TITLE_RE.match(line.strip())
            if summary:
                records.extend(_parse_summary_table(
                    lines, index, summary.group("title").strip(),
                    int(summary.group("season")),
                ))
                continue
            final = _match_final_title(lines, index)
            if final is not None:
                records.extend(_parse_final_table(lines, index, final))

    for record in records:
        record["release_date"] = release_date
        record["source_url"] = url
    return records


def _match_final_title(lines: Sequence[str], index: int) -> int | None:
    """The season a CELC final-crop table covers, or None if this isn't one.

    The title wraps across two lines ("… OF COMMERCIAL SUMMER" / "CROPS FOR
    2025:"), so the following line is folded in before matching.
    """
    joined = lines[index].strip()
    if index + 1 < len(lines):
        joined = f"{joined} {lines[index + 1].strip()}"
    match = _FINAL_TITLE_RE.match(re.sub(r"\s+", " ", joined))
    return int(match.group("season")) if match else None


# ── Layer entry point ────────────────────────────────────────────────────────

def fetch_cec_estimates() -> FetchResult:
    """Fetch every in-window CEC release and return per-commodity frames.

    Returns
    -------
    FetchResult
        ``ok`` with ``{commodity_key: DataFrame}``; ``failed`` when the
        listing page or a report can't be downloaded, or when a report inside
        the window no longer parses.

        There is no ``empty`` path: the window always holds a full season of
        releases, so zero rows means the source broke.
    """
    logger.info("Fetching CEC (South Africa) crop estimates ...")
    session = requests.Session()

    listing = _get(session, CEC_REPORTS_URL, "listing page")
    if not listing:
        return FetchResult.failed("CEC: listing page download failed")

    try:
        urls = _resolve_report_links(listing.decode("utf-8", errors="replace"))
    except ScraperShapeError as exc:
        logger.error("CEC: %s", exc)
        return FetchResult.failed(str(exc))

    logger.info("CEC: %d releases in the parse window (from %s)",
                len(urls), CEC_HISTORY_START)

    records: list[dict] = []
    for url in urls:
        raw = _get(session, url, url.rsplit("/", 1)[-1])
        if not raw:
            return FetchResult.failed(f"CEC: download failed for {url}")
        try:
            found = _parse_report(raw, url)
        except ScraperShapeError as exc:
            logger.error("CEC: %s", exc)
            return FetchResult.failed(str(exc))
        if not found:
            logger.debug("CEC: %s carries no summer-crop table", url)
        records.extend(found)

    if not records:
        return FetchResult.failed(
            "CEC: no crop estimates parsed from any in-window release"
        )

    frame = pd.DataFrame(records)
    frame["unit"] = "MT"
    data = {}
    for commodity, group in frame.groupby("commodity", sort=False):
        rows = group.sort_values(["season_year", "release_date"]).reset_index(drop=True)
        data[commodity] = rows
        latest = rows.iloc[-1]
        logger.info(
            "CEC %s: %d estimates, %d seasons; latest %s %s — %s ha, %s t",
            commodity, len(rows), rows["season_year"].nunique(),
            latest["release_date"], latest["forecast_label"],
            f"{latest['area_ha']:,.0f}" if pd.notna(latest["area_ha"]) else "n/a",
            f"{latest['production_t']:,.0f}" if pd.notna(latest["production_t"]) else "n/a",
        )

    missing = [key for key in CEC_CROPS.values() if key not in data]
    if missing:
        return FetchResult.failed(
            f"CEC: no rows parsed for {missing} — the summary table's crop "
            "rows changed"
        )
    return FetchResult.ok(data)


__all__: Sequence[str] = (
    "_parse_report",
    "_parse_release_date",
    "_resolve_report_links",
    "_numbers",
    "fetch_cec_estimates",
)


# ── Quick self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    result = fetch_cec_estimates()
    if not result.has_rows:
        logger.info("CEC: %s — %s", result.status, result.error)
    else:
        for name, df in result.data.items():
            logger.info("%s:\n%s", name, df.tail(8).to_string(index=False))
