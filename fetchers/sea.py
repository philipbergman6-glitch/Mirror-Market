"""
Layer 32 — SEA India weekly comparative rates (#72).

The Solvent Extractors' Association of India's weekly PDF, "Comparative rate
as registered as on <date>": the India legs nothing else in the stack has —
soymeal FAS Kandla and crude degummed soy oil CIF Mumbai in USD, beside the
domestic INR legs (Indore bean, Indore and Kandla meal, Mumbai and Indore
oil). The analysis on top is ``analysis/india_parity.py``: meal export
premium over CBOT and the oil import-parity window.

Why the PDF and not the post:
    SEA also posts the sheet as an HTML table, rewritten in place every week
    under a 2020 slug. On 2026-10-06 that table carried the PDF's *month-ago*
    numbers under a current "as of 1st Oct 2026" title — a stale body behind
    a fresh stamp, the worst kind of wrong number. Only the PDF is read.

Finding the sheets:
    Each week's PDF stays in SEA's WordPress media library, so the media
    listing (``config.SEA_MEDIA_URL``) is both the newest sheet and the
    archive. Membership is decided by the title (``SEA_TITLE_PATTERN``) and
    the MIME type, never by building a URL — the names flipped from
    DRddmmyy to DRyymmdd and the upload folder is not always the sheet's
    month (the rotating-URL trap, invariant 10).

Identity is the printed date:
    A sheet is dated by "AS REGISTERED AS ON <date>" inside it, never by its
    file name (DR250102 holds 2 Jan 2026). The printed date must be on or
    before the upload and within ``SEA_MAX_UPLOAD_LAG_DAYS`` of it.

Re-uploads:
    SEA uploads some weeks two or three times. Duplicates were identical on
    every date validated, except that one 26 Jun 2026 file had a blank
    current column with every change printed as -100.00, and its "-1"
    re-upload was the correction. So per date: every file that parses must
    agree exactly (else the date is withheld), and a file that fails to
    parse is excused only when another file uploaded within the lag window
    after a parsed date covers it.

Guards — every value is checked against the sheet's own arithmetic:
    Each line prints the current value, then % change / value for a week
    ago, a month ago and a year-average. The parser requires exactly that
    seven-column shape and that each printed % reproduces from the values
    beside it (to SEA's 2 decimals). A shifted column, a misread digit group
    or a value from the wrong line breaks the arithmetic and raises
    ScraperShapeError. Section headings carry the unit, and each series is
    found under its own heading only (``config.SEA_SERIES``). ``NQ`` (not
    quoted) is SEA's own "no price this week": the series is simply absent
    for that date — never a zero.

Licence:
    "Copyright © 2018 Solvent Extractors' Association of India. All Rights
    Reserved." No reuse grant, so the table is kept private until SEA agrees
    in writing (``config.SEA_PUBLISH``): it is not exported to the public
    ``data/history/`` and no page reads it. Each run re-reads the trailing
    ``SEA_LOOKBACK_DAYS`` of sheets instead; SEA keeps every past sheet
    online, so nothing is lost by not persisting it.
"""

from __future__ import annotations

import io
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from pypdf import PdfReader

from config import (
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    SEA_LOOKBACK_DAYS,
    SEA_MAX_UPLOAD_LAG_DAYS,
    SEA_MEDIA_SEARCH,
    SEA_MEDIA_URL,
    SEA_SERIES,
    SEA_TITLE_PATTERN,
)
from fetchers._backoff import retry_sleep
from pipeline.results import FetchResult, ScraperShapeError

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")

_HEADERS = {
    "User-Agent": (
        "Mirror-Market/1.0 "
        "(+https://github.com/philipbergman6-glitch/Mirror-Market)"
    ),
}

# SEA rounds every % change to 2 decimals; anything further off than the
# rounding (plus float noise) is a different number from the one printed.
_PCT_TOLERANCE = 0.011

_NUM = r"(NQ|\d[\d,]*(?:\.\d+)?)"
_PCT = r"(--|-?\d+(?:\.\d+)?)"
_AS_ON = re.compile(
    r"AS\s+REGISTERED\s+AS\s+ON\s+(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9})\.?,?\s+(\d{4})",
)
# How many lines below its section heading a series may sit. The longest
# section on the sheet (local oils) runs to ten lines.
_SECTION_DEPTH = 14


@dataclass(frozen=True)
class RateSheet:
    """One weekly sheet: its printed date and the value of every quoted series.

    A series SEA marked ``NQ`` that week is absent from ``values``.
    """

    as_on: date
    values: dict[str, float] = field(default_factory=dict)


def _label_pattern(label: str) -> str:
    """The label as printed, with whitespace anywhere optional."""
    return r"\s*".join(re.escape(ch) for ch in label if not ch.isspace())


def _number(token: str) -> float | None:
    return None if token == "NQ" else float(token.replace(",", ""))


def _parse_as_on(text: str) -> date:
    match = _AS_ON.search(text)
    if match is None:
        raise ScraperShapeError("SEA: sheet carries no 'AS REGISTERED AS ON <date>' line")
    day, month, year = match.groups()
    try:
        return datetime.strptime(f"{day} {month[:3]} {year}", "%d %b %Y").date()
    except ValueError as exc:
        raise ScraperShapeError(f"SEA: unreadable sheet date {match.group(0)!r}") from exc


def _check_arithmetic(key: str, groups: tuple[str, ...]) -> None:
    current = _number(groups[0])
    if current is None:
        return
    for pct, other in ((groups[1], groups[2]), (groups[3], groups[4]), (groups[5], groups[6])):
        comparison = _number(other)
        if pct == "--" or not comparison:
            continue
        computed = (current / comparison - 1) * 100
        if abs(computed - float(pct)) > _PCT_TOLERANCE:
            raise ScraperShapeError(
                f"SEA: {key} prints {pct}% against {other}, but {groups[0]} over it is "
                f"{computed:.2f}% — the line did not parse into the columns it shows"
            )


def parse_rate_sheet(text: str) -> RateSheet:
    """Parse one sheet's extracted text. Raises ScraperShapeError on any miss.

    Every series in ``config.SEA_SERIES`` must be found exactly once under
    its own section heading, in the seven-column shape, with its printed
    % changes reproducing from its own values.
    """
    as_on = _parse_as_on(text)
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.splitlines()]
    values: dict[str, float] = {}
    for key, spec in SEA_SERIES.items():
        section = re.compile(spec["section"])
        headings = [i for i, line in enumerate(lines) if section.search(line)]
        if len(headings) != 1:
            raise ScraperShapeError(
                f"SEA {as_on}: section for {key} found {len(headings)} times, expected once"
            )
        row = re.compile(
            r"^\d+\.\s*" + _label_pattern(spec["label"])
            + r"\s+" + r"\s+".join([_NUM, _PCT, _NUM, _PCT, _NUM, _PCT, _NUM]) + r"$"
        )
        start = headings[0] + 1
        hits = [m for m in (row.match(line) for line in lines[start:start + _SECTION_DEPTH]) if m]
        if len(hits) != 1:
            raise ScraperShapeError(
                f"SEA {as_on}: {key} ({spec['label']!r}) matched {len(hits)} lines under its "
                "section, expected one complete seven-column line"
            )
        groups = hits[0].groups()
        _check_arithmetic(key, groups)
        value = _number(groups[0])
        if value is not None:
            values[key] = value
    return RateSheet(as_on=as_on, values=values)


# --- fetching -----------------------------------------------------------------


@dataclass(frozen=True)
class _Upload:
    """One DR-titled PDF in SEA's media library."""

    title: str
    name: str        # file name without .pdf — what errors are reported by
    uploaded: date
    url: str


def _ist_today() -> date:
    return datetime.now(_IST).date()


def _get(url: str, params: dict | None = None) -> requests.Response:
    """GET with retries. Raises requests.RequestException once they run out."""
    last_error = "no attempts made"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, headers=_HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp
            last_error = f"HTTP {resp.status_code}"
        except requests.RequestException as exc:
            last_error = str(exc)
        logger.warning("SEA: %s attempt %d failed: %s", url, attempt, last_error)
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)
    raise requests.RequestException(f"{url} failed after {MAX_RETRIES} attempts ({last_error})")


def _pdf_text(content: bytes) -> str:
    """Every page's text, in order. Anything pypdf cannot read is a shape error."""
    try:
        reader = PdfReader(io.BytesIO(content))
        return "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception as exc:  # noqa: BLE001 — pypdf raises a family of its own types
        raise ScraperShapeError(f"not a readable PDF ({exc})") from exc


_PER_PAGE = 100


def _list_uploads(since: date) -> list[_Upload]:
    """Every DR-titled PDF uploaded on or after ``since``, newest first.

    Pages through the listing (newest first) until it passes ``since``, so a
    long backfill window reads as far back as it asks.
    """
    title_pattern = re.compile(SEA_TITLE_PATTERN)
    uploads: list[_Upload] = []
    page = 1
    while True:
        resp = _get(SEA_MEDIA_URL, params={
            "search": SEA_MEDIA_SEARCH, "per_page": _PER_PAGE, "page": page,
            "orderby": "date", "order": "desc",
            "_fields": "date,source_url,title,mime_type",
        })
        try:
            payload = resp.json()
        except ValueError as exc:
            raise ScraperShapeError("SEA: media listing is not JSON") from exc
        if not isinstance(payload, list):
            raise ScraperShapeError(f"SEA: media listing is not a list: {str(payload)[:160]}")
        oldest: date | None = None
        for item in payload:
            try:
                title = str(item["title"]["rendered"]).strip()
                uploaded = date.fromisoformat(str(item["date"])[:10])
                mime, url = item["mime_type"], str(item["source_url"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ScraperShapeError(f"SEA: media item without date/title/url ({exc!r})") from exc
            oldest = uploaded if oldest is None else min(oldest, uploaded)
            if mime != "application/pdf" or not title_pattern.fullmatch(title) or uploaded < since:
                continue
            name = url.rsplit("/", 1)[-1].removesuffix(".pdf")
            uploads.append(_Upload(title=title, name=name, uploaded=uploaded, url=url))
        if len(payload) < _PER_PAGE or oldest is None or oldest < since:
            break
        page += 1
    if not uploads:
        raise ScraperShapeError(
            f"SEA: the media listing holds no DR rate sheet uploaded since {since} — SEA has "
            "published every week since 2018, so the listing changed, not the cadence"
        )
    return uploads


def _read(upload: _Upload) -> RateSheet:
    """Download and parse one sheet; check its printed date against its upload."""
    sheet = parse_rate_sheet(_pdf_text(_get(upload.url).content))
    lag = (upload.uploaded - sheet.as_on).days
    if not 0 <= lag <= SEA_MAX_UPLOAD_LAG_DAYS:
        raise ScraperShapeError(
            f"printed date {sheet.as_on} is {lag} day(s) from its upload on {upload.uploaded} "
            f"(allowed 0-{SEA_MAX_UPLOAD_LAG_DAYS}) — not that week's sheet"
        )
    return sheet


def fetch_sea_rates(
    today: date | None = None, lookback_days: int = SEA_LOOKBACK_DAYS,
) -> FetchResult:
    """Every SEA sheet uploaded in the trailing window, one frame per series.

    Frames carry ``Date`` (the sheet's printed date), ``value`` and ``unit``.

    ``failed`` with no rows when the listing is unusable or holds no sheet in
    the window. When some sheets cannot be read, or re-uploads of one date
    disagree, the rest are returned as ``partial`` (graded failed, rows kept)
    and the disagreeing date is withheld. A sheet that cannot be read is
    excused only by a twin — another upload under the same title that parsed.
    """
    today = today or _ist_today()
    since = today - timedelta(days=lookback_days)
    try:
        uploads = _list_uploads(since)
    except ScraperShapeError as exc:
        logger.error("%s", exc)
        return FetchResult.failed(str(exc))
    except requests.RequestException as exc:
        return FetchResult.failed(f"SEA: media listing: {exc}")

    parsed: list[tuple[_Upload, RateSheet]] = []
    unread: list[tuple[_Upload, str]] = []
    for upload in uploads:
        try:
            parsed.append((upload, _read(upload)))
        except (ScraperShapeError, requests.RequestException) as exc:
            unread.append((upload, str(exc)))

    errors: list[str] = []
    parsed_titles = {upload.title for upload, _ in parsed}
    for upload, reason in unread:
        if upload.title in parsed_titles:
            logger.warning("SEA: %s unreadable, covered by its re-upload: %s", upload.name, reason)
        else:
            errors.append(f"{upload.name}: {reason}")

    by_date: dict[date, list[tuple[_Upload, RateSheet]]] = {}
    for upload, sheet in parsed:
        by_date.setdefault(sheet.as_on, []).append((upload, sheet))
    sheets: list[RateSheet] = []
    for as_on, group in sorted(by_date.items()):
        if any(sheet.values != group[0][1].values for _, sheet in group[1:]):
            names = ", ".join(upload.name for upload, _ in group)
            errors.append(f"{as_on}: re-uploads disagree ({names}) — date withheld")
            continue
        sheets.append(group[0][1])

    data: dict[str, pd.DataFrame] = {}
    for key, spec in SEA_SERIES.items():
        rows = [
            {"Date": sheet.as_on.isoformat(), "value": sheet.values[key], "unit": spec["unit"]}
            for sheet in sheets if key in sheet.values
        ]
        if rows:
            data[key] = pd.DataFrame(rows, columns=["Date", "value", "unit"])

    if sheets:
        newest = sheets[-1]
        logger.info(
            "SEA: %d sheet(s) %s → %s; newest meal FAS $%s/MT, degum CIF $%s/MT",
            len(sheets), sheets[0].as_on, newest.as_on,
            newest.values.get("Soybean Meal FAS Kandla", "NQ"),
            newest.values.get("Soybean Oil CIF Mumbai", "NQ"),
        )
    if errors:
        message = "SEA: " + "; ".join(errors)
        logger.error("%s", message)
        return FetchResult.partial(data, message) if data else FetchResult.failed(message)
    if not data:
        return FetchResult.failed("SEA: no sheet in the window could be read")
    return FetchResult.ok(data)
