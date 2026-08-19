"""
Layers 26 / 26b — USDA AMS Grain Transportation Report.

Two legs of the physical trade this stack could price but could not move:

    26   ocean freight, US Gulf and PNW to Japan, USD/MT, monthly to Jan 1996
    26b  grain vessel lineups at the Gulf and the PNW, counts, weekly to 1995

Both are supporting tables of the same weekly AMS report, posted as
standalone workbooks at fixed filenames. Each workbook carries the whole
series on every download, so the layers self-heal on an empty CI database and
neither needs a `data/history/` round-trip.

What these are NOT:

    - The ocean rate is **not USDA's own measurement**. USDA republishes an
      assessment by O'Neil Commodity Consulting, and the attribution stamped
      on every row says so. It is a benchmark route (Japan), not a rate for
      any cargo anyone here is shipping: it must never be substituted for a
      route-specific freight assumption in a landed-cost stack. Gulf-to-Japan
      is a read on the *level* and, against PNW, on which US coast is
      competitive — not a quote.
    - The vessel counts are **not a lineup by ship**. They are how many
      vessels are in port, loaded in the last 7 days, and due in the next 10,
      by port region. No names, no cargoes, no berths.

Four traps this module exists to survive, each of which parses cleanly and
lies quietly if unguarded:

1.  **The period column carries seven layouts.** Six string formats plus real
    datetimes, catalogued at `_YEAR_FIRST_RE` below — thirty years of
    whoever maintained the sheet that decade. The month token itself has
    three spellings ("Sep", "Sept", "September"), and `Sept` is accepted by
    neither `%b` nor `%B` while being how the file writes every September
    from 2002 to 2016. None of this raises: a parser handling a subset
    returns a *shorter series* and logs success. The first cut of this
    module handled two of the seven and stored 128 of 367 months.

    Figure 20 also contains a plain data-entry error — seven 2019 months
    stored as **1919**, sitting between "May '19" and 2020-01. The sequence
    proves the intent, but rewriting a published year is inventing data and
    storing 1919 puts a century-old rate at the front of every chart, so
    the row is dropped and named. 360 of 367 months survive.

2.  **Both sheets end in a summary block that looks like data.** Figure 20
    repeats its header at row ~380 and then prints year-on-year *ratios*
    (0.33, 0.21) under the same columns as the rates; Table 19 prints 2015
    averages, a text range ("25..54") and percent changes below its last
    week. Read as rows, those become a freight market that collapsed to
    thirty cents and a port with 0.2 ships in it. The defence is not a row
    count — the blocks move — it is that a data row must carry a parseable
    period label, and every value must survive its plausibility band.

3.  **A column shift restates one series as another.** Neither sheet has a
    single header row to key on: the labels are split across two or three
    banner rows, so columns are addressed by index. Indices are therefore
    checked against each sheet's own arithmetic on every row — the published
    Gulf-vs-PNW spread must equal gulf - pnw, and vessels in port must equal
    loading + waiting to load. A shifted column fails that check; a shifted
    column with no check reads as a market.

    That check is a detector of *our* drift, not an audit of the publisher,
    so the verdict rides on the failure **rate**: a handful of contradictory
    rows in thirty years is the publisher's own typing (8 of 1,649 vessel
    weeks) and drops only those rows, while a rate above
    `GTR_MAX_ARITHMETIC_FAILURE_RATE` means the mapping moved and the whole
    workbook goes — the rows that happen to reconcile under a shifted
    mapping are no more trustworthy than the ones that do not.

4.  **A truncated download is served as HTTP 200.** Observed live on
    2026-08-19 against these very files: 634,667 bytes arrived, the central
    directory did not, and the file passed `file(1)` as an Excel workbook.
    openpyxl raises `zipfile.BadZipFile` on that, which is *not* an OSError
    subclass and escapes a naive `except OSError` as a crash mid-run.

The recency budget in `config.LAYER_MAX_DATA_AGE_DAYS` is what catches the
fifth and quietest failure — a workbook that stops being refreshed behind a
filename that keeps answering 200.
"""

import io
import logging
import re
import zipfile

import pandas as pd
import requests

from config import (
    GTR_MAX_ARITHMETIC_FAILURE_RATE,
    GTR_MIN_OBSERVATION_YEAR,
    GTR_OCEAN_FREIGHT_URL,
    GTR_OCEAN_MAX_USD_MT,
    GTR_OCEAN_MIN_USD_MT,
    GTR_OCEAN_ROUTES,
    GTR_OCEAN_SPREAD_COLUMN,
    GTR_PORT_REGIONS,
    GTR_VESSEL_ACTIVITY_URL,
    GTR_VESSEL_MAX_COUNT,
    LAYER_MAX_DATA_AGE_DAYS,
    MAX_RETRIES,
)
from fetchers._backoff import retry_sleep

logger = logging.getLogger(__name__)

# ams.usda.gov answers a bare python-requests UA, but every other AMS leg in
# this project sends a browser UA and the Gulf-bids fetcher learned the hard
# way that AMS edge rules change without notice. One UA, stated once.
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; MirrorMarket/1.0)"}

# The `Data` sheet is the full series; the other sheets in each workbook are
# the two most recent rows laid out for the printed figure. Reading the
# printed sheet would give a two-row history that looks complete.
_DATA_SHEET = "Data"

# Figure 20's period column carries **six** string formats plus real
# datetimes — thirty years of whoever was maintaining the sheet that decade,
# counted live on 2026-08-19 across all 367 data rows:
#
#     96-Jan / 99-June    YY-Mon, year first          42 rows
#     July_99 / Dec_01    Mon_YY                      30 rows
#     Jan. 02 / Apr. 17   Mon. YY                    139 rows
#     May  02             Mon  YY, doubled space       2 rows
#     June 02 / July 17   Mon YY                      46 rows
#     Aug '17 / May '19   Mon 'YY                     22 rows
#     datetime                                        86 rows
#
# Two regexes rather than six: the month token is `[A-Za-z]{3,9}` because it
# mixes abbreviations with full names ("Sep", but "June" and "July"), and the
# separator is any run of punctuation or space. Both are anchored at each end
# so the summary labels ("LY", "4-Yr Avg") cannot match.
_YEAR_FIRST_RE = re.compile(r"^(\d{2})[-_.\s']+([A-Za-z]{3,9})$")
_MONTH_FIRST_RE = re.compile(r"^([A-Za-z]{3,9})[-_.\s']+(\d{2})$")

# Matched by prefix rather than by strptime — see _month_number.
_MONTH_NAMES = (
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
)

# Two-digit years: this series starts in 1996 and the workbook has no
# four-digit form for its early rows, so the pivot is placed past any year
# the file can currently contain rather than at an arbitrary 50.
_CENTURY_PIVOT = 80

# The workbook's own sentinel for a week it did not collect. Distinct from an
# empty cell only in that it would coerce to NaN either way — but naming it
# keeps a future numeric coercion from being read as a real zero.
_NOT_AVAILABLE = {"na", "n/a", "-", ""}

# Floating-point slack when checking a sheet's published arithmetic against
# the columns we read. The published figures carry two decimals.
_ARITHMETIC_TOLERANCE = 0.011


def _download(url: str, label: str) -> bytes:
    """Download a GTR workbook, return raw bytes (b"" on failure)."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Downloading GTR %s ...", label)
            resp = requests.get(url, timeout=120, headers=_HEADERS)
            resp.raise_for_status()
            content = resp.content
            logger.info("GTR %s downloaded (%d KB)", label, len(content) // 1024)
            return content
        except requests.RequestException as exc:
            logger.warning(
                "Attempt %d/%d failed for GTR %s: %s", attempt, MAX_RETRIES, label, exc
            )
            if attempt < MAX_RETRIES:
                retry_sleep(attempt)

    logger.error("All %d attempts failed for GTR %s (%s)", MAX_RETRIES, label, url)
    return b""


def _read_data_sheet(raw_bytes: bytes, label: str) -> pd.DataFrame | None:
    """Read the `Data` sheet headerless, or None when the workbook is unusable.

    `zipfile.BadZipFile` is caught explicitly and not as an OSError: it is not
    one, and it is the shape a truncated 200 arrives in (trap 4 above).
    """
    try:
        return pd.read_excel(
            io.BytesIO(raw_bytes),
            sheet_name=_DATA_SHEET,
            header=None,
            engine="openpyxl",
        )
    except zipfile.BadZipFile:
        logger.error(
            "GTR %s is not a readable workbook (%d bytes) — a truncated or "
            "error-page response served as HTTP 200",
            label, len(raw_bytes),
        )
    except (ValueError, KeyError, OSError) as exc:
        # KeyError: the `Data` sheet was renamed or removed. ValueError:
        # openpyxl's malformed-workbook error. Both are outages, not gaps.
        logger.error("Failed to parse GTR %s workbook: %s", label, exc)
    return None


def _numeric(value) -> float | None:
    """Parse a cell to a float, mapping blanks and `na` sentinels to None."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, str):
        text = value.strip()
        if text.lower() in _NOT_AVAILABLE:
            return None
        try:
            return float(text.replace(",", ""))
        except ValueError:
            return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _month_number(token: str) -> int | None:
    """Month index from any unambiguous prefix of a month name, or None.

    Not `%b`/`%B`: the column carries **three** spellings those two formats
    between them cannot cover — "Sep" and "September" are both accepted by
    strptime, but "Sept" is accepted by neither, and it is how this file
    writes every September from 2002 to 2016. Matching on prefix instead
    covers "Sep", "Sept", "September", "Jun", "June" and "Jul"/"July" with
    one rule, and the three-character floor in the enclosing regexes keeps
    every prefix unambiguous ("Mar"/"May", never a bare "Ma").
    """
    candidate = token.strip().lower()
    matches = {
        index
        for index, name in enumerate(_MONTH_NAMES, start=1)
        if name.startswith(candidate)
    }
    return matches.pop() if len(matches) == 1 else None


def _month_start(value) -> pd.Timestamp | None:
    """Parse Figure 20's period label to the first of its month.

    Handles every form the column actually contains — a real datetime and the
    six string layouts catalogued above — and returns None for anything else,
    which is how the trailing summary block is excluded.

    A parsed year before `GTR_MIN_OBSERVATION_YEAR` is also refused. That is
    not paranoia about the format: seven 2019 rows are stored as 1919 in the
    published file, and the choice there is between storing a century-old
    freight rate, silently rewriting a published year, or leaving a named
    gap. This takes the gap.
    """
    if isinstance(value, str):
        text = value.strip()
        match = _YEAR_FIRST_RE.match(text)
        if match:
            year_2, month_token = match.groups()
        else:
            match = _MONTH_FIRST_RE.match(text)
            if not match:
                return None
            month_token, year_2 = match.groups()

        month = _month_number(month_token)
        if month is None:
            return None
        year = int(year_2)
        year += 1900 if year >= _CENTURY_PIVOT else 2000
        stamp = pd.Timestamp(year=year, month=month, day=1)
    else:
        # A Timestamp, or a datetime.date/datetime pandas did not box as one.
        try:
            stamp = pd.Timestamp(value)
        except (TypeError, ValueError):
            return None
        if pd.isna(stamp):
            return None
        stamp = stamp.normalize().replace(day=1)

    if stamp.year < GTR_MIN_OBSERVATION_YEAR:
        logger.warning(
            "GTR Figure 20: dropping row dated %s — before %d, so the "
            "published year is a data-entry error (the file stores seven "
            "2019 months as 1919). A gap is preferable to either storing "
            "that or rewriting it.",
            stamp.strftime("%Y-%m"), GTR_MIN_OBSERVATION_YEAR,
        )
        return None
    return stamp


def _arithmetic_rate_is_tolerable(
    rejected: int, accepted: int, sheet: str, check: str
) -> bool:
    """Judge a sheet's published-arithmetic failures as noise or as drift.

    The check exists to detect a **column shift**, so what matters is the
    rate, not the count. A handful of contradictory rows in thirty years is
    the publisher's own typing (8 of 1,649 vessel weeks on 2026-08-19) and is
    reported at INFO — logging that at ERROR every run would train a reader
    to ignore the line that matters. Above the threshold the mapping has
    moved, and every row is then suspect, so the caller discards the
    workbook rather than storing a plausible-looking wrong series.
    """
    if not rejected:
        return True

    total = rejected + accepted
    rate = rejected / total if total else 1.0
    if rate > GTR_MAX_ARITHMETIC_FAILURE_RATE:
        logger.error(
            "GTR %s: %d of %d rows failed the %s (%.1f%%, threshold %.1f%%) — "
            "the column mapping has shifted; discarding the workbook",
            sheet, rejected, total, check, rate * 100,
            GTR_MAX_ARITHMETIC_FAILURE_RATE * 100,
        )
        return False

    logger.info(
        "GTR %s: %d of %d rows dropped on the %s — upstream inconsistency, "
        "below the %.1f%% column-shift threshold",
        sheet, rejected, total, check, GTR_MAX_ARITHMETIC_FAILURE_RATE * 100,
    )
    return True


def _parse_ocean_freight(raw_bytes: bytes) -> dict[str, pd.DataFrame]:
    """Parse Figure 20 into {route: DataFrame[Date, rate_usd_mt]}.

    Returns {} on any structural failure. Every accepted row has cleared
    three independent checks: a parseable month label, both rates inside the
    plausibility band, and the sheet's own published spread reproduced from
    the two columns we read.
    """
    sheet = _read_data_sheet(raw_bytes, "ocean freight (Figure 20)")
    if sheet is None:
        return {}

    needed = max([*GTR_OCEAN_ROUTES, GTR_OCEAN_SPREAD_COLUMN])
    if sheet.shape[1] <= needed:
        logger.error(
            "GTR Figure 20 has %d columns, need more than %d — the sheet has "
            "been restructured", sheet.shape[1], needed,
        )
        return {}

    rows: dict[str, list[tuple[pd.Timestamp, float]]] = {
        route: [] for route in GTR_OCEAN_ROUTES.values()
    }
    rejected_band = 0
    rejected_arithmetic = 0

    for _, row in sheet.iterrows():
        date = _month_start(row.iloc[0])
        if date is None:
            continue

        rates = {
            route: _numeric(row.iloc[column])
            for column, route in GTR_OCEAN_ROUTES.items()
        }
        if any(rate is None for rate in rates.values()):
            continue

        if any(
            not (GTR_OCEAN_MIN_USD_MT <= rate <= GTR_OCEAN_MAX_USD_MT)
            for rate in rates.values()
        ):
            # The summary block's ratios land here, as would a unit change.
            rejected_band += 1
            continue

        gulf = rates[GTR_OCEAN_ROUTES[1]]
        pnw = rates[GTR_OCEAN_ROUTES[3]]
        published_spread = _numeric(row.iloc[GTR_OCEAN_SPREAD_COLUMN])
        if published_spread is not None and (
            abs((gulf - pnw) - published_spread) > _ARITHMETIC_TOLERANCE
        ):
            # The columns we read are not the columns the sheet spread was
            # struck on. Dropping the row is right: a shifted column is a
            # wrong number, not a missing one.
            rejected_arithmetic += 1
            continue

        for route, rate in rates.items():
            rows[route].append((date, rate))

    if rejected_band:
        logger.info(
            "GTR Figure 20: %d non-rate rows skipped (summary block)", rejected_band
        )
    if not _arithmetic_rate_is_tolerable(
        rejected_arithmetic,
        len(rows[GTR_OCEAN_ROUTES[1]]),
        "Figure 20",
        "published spread check (gulf - pnw != spread)",
    ):
        return {}

    results: dict[str, pd.DataFrame] = {}
    for route, entries in rows.items():
        if not entries:
            logger.error("GTR Figure 20: route %r parsed to zero rows", route)
            continue
        frame = (
            pd.DataFrame(entries, columns=["Date", "rate_usd_mt"])
            .drop_duplicates(subset=["Date"], keep="last")
            .sort_values("Date")
            .reset_index(drop=True)
        )
        results[route] = frame
        logger.info(
            "  %s: %d monthly rates, %s → %s (latest $%.2f/MT)",
            route, len(frame),
            frame["Date"].min().strftime("%Y-%m"),
            frame["Date"].max().strftime("%Y-%m"),
            frame["rate_usd_mt"].iloc[-1],
        )

    if len(results) != len(GTR_OCEAN_ROUTES):
        # Partial is worse than empty here: the two routes are read from one
        # download of one sheet, so one surviving alone means the mapping
        # moved, and the survivor is as suspect as the casualty.
        logger.error(
            "GTR Figure 20: %d of %d routes parsed — discarding the workbook",
            len(results), len(GTR_OCEAN_ROUTES),
        )
        return {}

    return results


def _parse_vessel_activity(raw_bytes: bytes) -> dict[str, pd.DataFrame]:
    """Parse Table 19 into {port_region: DataFrame} of weekly vessel counts.

    `in_port` is stored rather than derived even though it equals
    loading + waiting_to_load wherever all three print: the 1990s rows carry
    only the total, so deriving it would delete a decade of history. Where
    all three *are* present the identity is enforced, which is what pins the
    column mapping.
    """
    sheet = _read_data_sheet(raw_bytes, "vessel activity (Table 19)")
    if sheet is None:
        return {}

    needed = max(
        column for columns in GTR_PORT_REGIONS.values() for column in columns.values()
    )
    if sheet.shape[1] <= needed:
        logger.error(
            "GTR Table 19 has %d columns, need more than %d — the sheet has "
            "been restructured", sheet.shape[1], needed,
        )
        return {}

    rows: dict[str, list[dict]] = {region: [] for region in GTR_PORT_REGIONS}
    rejected_band = 0
    rejected_arithmetic = 0

    for _, row in sheet.iterrows():
        week_ending = _week_ending(row.iloc[0])
        if week_ending is None:
            continue

        for region, columns in GTR_PORT_REGIONS.items():
            values = {
                field: _numeric(row.iloc[column]) for field, column in columns.items()
            }
            if all(value is None for value in values.values()):
                # A region that did not report that week — Vancouver's whole
                # tail, and the PNW block before it was collected. Not an
                # error, and not a zero either.
                continue

            if any(
                value is not None and not (0 <= value <= GTR_VESSEL_MAX_COUNT)
                for value in values.values()
            ):
                rejected_band += 1
                continue

            loading = values["loading"]
            waiting = values["waiting_to_load"]
            in_port = values["in_port"]
            if None not in (loading, waiting, in_port) and (
                abs((loading + waiting) - in_port) > _ARITHMETIC_TOLERANCE
            ):
                rejected_arithmetic += 1
                continue

            rows[region].append({"week_ending": week_ending, **values})

    if rejected_band:
        logger.info(
            "GTR Table 19: %d non-count rows skipped (summary block)", rejected_band
        )
    if not _arithmetic_rate_is_tolerable(
        rejected_arithmetic,
        sum(len(entries) for entries in rows.values()),
        "Table 19",
        "in-port identity (loading + waiting != in port)",
    ):
        return {}

    results: dict[str, pd.DataFrame] = {}
    for region, entries in rows.items():
        if not entries:
            logger.error("GTR Table 19: port region %r parsed to zero rows", region)
            continue
        frame = (
            pd.DataFrame(entries)
            .drop_duplicates(subset=["week_ending"], keep="last")
            .sort_values("week_ending")
            .reset_index(drop=True)
        )
        results[region] = frame
        logger.info(
            "  %s: %d weeks, %s → %s", region, len(frame),
            frame["week_ending"].min().strftime("%Y-%m-%d"),
            frame["week_ending"].max().strftime("%Y-%m-%d"),
        )

    if len(results) != len(GTR_PORT_REGIONS):
        logger.error(
            "GTR Table 19: %d of %d port regions parsed — discarding the workbook",
            len(results), len(GTR_PORT_REGIONS),
        )
        return {}

    return results


def _week_ending(value) -> pd.Timestamp | None:
    """Parse Table 19's date column, rejecting its trailing summary labels."""
    if isinstance(value, str):
        # "% change from last week", "Average 2015" — labels, not dates.
        # to_datetime would coerce some of these, so strings are refused
        # outright: every real row in this column is a datetime.
        return None
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(stamp):
        return None
    return stamp.normalize()


def _within_budget(
    results: dict[str, pd.DataFrame], date_column: str, layer: str, label: str
) -> bool:
    """True when the newest observation is inside the layer's recency budget.

    The workbook is discarded whole when it is not. Storing a frozen file and
    noting it in a log would leave the layer green on dead numbers, which is
    the failure this guard exists for — the filename never rotates, so
    nothing else would ever notice.
    """
    latest = max(frame[date_column].max() for frame in results.values())
    age_days = (pd.Timestamp.today().normalize() - latest.normalize()).days
    budget = LAYER_MAX_DATA_AGE_DAYS[layer]
    if age_days > budget:
        logger.error(
            "GTR %s ends %s (%d days ago, budget %d) — the workbook has "
            "probably stopped being refreshed behind a fixed filename; "
            "discarding it rather than storing it.",
            label, latest.strftime("%Y-%m-%d"), age_days, budget,
        )
        return False
    return True


def fetch_gtr_ocean_freight() -> dict[str, pd.DataFrame]:
    """
    Download and parse GTR Figure 20 — monthly bulk grain ocean freight.

    Returns
    -------
    dict
        {route: DataFrame} with columns Date (month start) and rate_usd_mt.

        Empty when the download failed, the parse failed, only one route
        survived, **or** the workbook is older than the recency budget. Every
        one of those is a layer failure and never an empty-success: the file
        carries 350+ months on every fetch, so "nothing came back" cannot
        mean "nothing was published this month".
    """
    raw_bytes = _download(GTR_OCEAN_FREIGHT_URL, "ocean freight (Figure 20)")
    if not raw_bytes:
        return {}

    results = _parse_ocean_freight(raw_bytes)
    if not results:
        return {}

    if not _within_budget(results, "Date", "gtr_ocean_freight", "ocean freight"):
        return {}

    return results


def fetch_gtr_vessel_activity() -> dict[str, pd.DataFrame]:
    """
    Download and parse GTR Table 19 — weekly grain vessel activity by port region.

    Returns
    -------
    dict
        {port_region: DataFrame} with columns week_ending, loading,
        waiting_to_load, in_port, loaded_7day, due_10day — counts of vessels.

        Empty on the same terms as the freight leg, and for the same reason:
        the workbook carries 1,600+ weeks on every fetch.
    """
    raw_bytes = _download(GTR_VESSEL_ACTIVITY_URL, "vessel activity (Table 19)")
    if not raw_bytes:
        return {}

    results = _parse_vessel_activity(raw_bytes)
    if not results:
        return {}

    if not _within_budget(results, "week_ending", "gtr_vessels", "vessel activity"):
        return {}

    return results
