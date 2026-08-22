"""One-time backfill of US Gulf export bid history over the MARS API (#283).

Layer 20's live path parses *today's* AMS 3147 PDF, and the PDF is only ever
today's — which is why `gulf_bids` holds a fortnight of history and the Gulf
basis leg has no seasonal shape to read against. The MARS API serves the same
report as structured rows back to 2020-02-24, so this walks that archive
through the shared mapping (`fetchers.gulf_bids._map_api_rows`) and the
existing save path. INSERT OR REPLACE makes re-runs safe.

**The overlap is checked before anything is written.** Every report date that
is already in the database was parsed out of the PDF, so the API rows for
those dates must reproduce them cell for cell; a disagreement means the
mapping is wrong and the archive would enter the table as a different kind of
number wearing the same column names. That aborts the run — the whole point
of a backfill is that afterwards nobody can tell which transport a row came
from. Columns the stored rows predate (`futures_month_high` before #196) are
NULL there and are skipped rather than counted as disagreements.

History CSVs are written by CI (`export_history` + the snapshot commit), never
by hand — run this through .github/workflows/backfill-gulf-bids.yml to persist
the result; a local run only fills the local DB.

    python scripts/backfill_gulf_bids_mars.py            # one archive pull
    python scripts/backfill_gulf_bids_mars.py --per-date [START [END]]
"""

import logging
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import MARS_GULF_BIDS_ARCHIVE_START, setup_logging  # noqa: E402
from fetchers.gulf_bids import (  # noqa: E402
    fetch_gulf_bids_api,
    is_api_configured,
)
from pipeline.query import read_gulf_bids  # noqa: E402
from pipeline.store import init_database, save_gulf_bids  # noqa: E402

logger = logging.getLogger(__name__)

_POLITE_DELAY_S = 0.5

# The primary key of one bid quote.
_KEY = ["report_date", "commodity", "location", "delivery"]

# Compared against the stored PDF rows on overlapping dates. Floats compare
# with a tolerance because both sides round-trip through CSV; the string
# columns must match exactly.
_NUMERIC_COLUMNS = [
    "basis_low", "basis_high", "futures_month", "futures_month_high",
    "price_low", "price_high", "average", "year_ago",
]
_TEXT_COLUMNS = ["sale_type", "basis_change", "price_change", "freight"]
_TOLERANCE = 1e-6

# The flat-price columns, which are the basis plus whatever the referenced
# futures contract was worth when AMS struck the report. Everything else
# states the quote itself and must match exactly.
_PRICE_COLUMNS = ["price_low", "price_high", "average"]

# A re-issue moves futures by cents inside one publication window (the 3147
# prelim lands ~12:47 and the final ~13:14). A shift bigger than this is not
# an intraday re-snapshot — it is a merge or a unit error wearing the shape
# of one, and the run stops for a human.
_MAX_REPRICE_USD_BU = 0.25


def _stored_rows() -> pd.DataFrame:
    """Everything already in `gulf_bids` — i.e. every PDF-parsed row."""
    df = read_gulf_bids()
    if df.empty:
        return df
    df = df.copy()
    df["report_date"] = pd.to_datetime(df["report_date"]).dt.strftime("%Y-%m-%d")
    return df


def _differing(merged: pd.DataFrame, column: str) -> pd.Series:
    """Rows where both transports filled `column` and filled it differently."""
    left, right = merged[f"{column}_api"], merged[f"{column}_pdf"]
    both = left.notna() & right.notna()
    if column in _NUMERIC_COLUMNS:
        return both & (
            (pd.to_numeric(left, errors="coerce")
             - pd.to_numeric(right, errors="coerce")).abs() > _TOLERANCE
        )
    return both & (left.astype(str) != right.astype(str))


def _repricing_offsets(day: pd.DataFrame) -> dict[tuple[str, float], float] | None:
    """Is one report date's price gap a single futures re-snapshot?

    Back the futures level out of each leg — a flat price is its basis over a
    named contract, so ``price - basis/100`` is what that contract was worth
    when the report was struck. If every leg of every row on the date moved by
    one offset per (commodity, contract month), the two transports carry the
    same quotes priced at two moments, and this returns those offsets.
    ``None`` means they disagree about something a clock cannot explain.

    Rows that agree are included, contributing an offset of zero: if one row
    moved and another on the same contract did not, that is not a re-pricing.
    """
    offsets: dict[tuple[str, float], float] = {}
    for _, row in day.iterrows():
        legs = [
            (row["futures_month_api"], row["basis_low_api"],
             row["price_low_api"], row["price_low_pdf"]),
            (row["futures_month_high_api"], row["basis_high_api"],
             row["price_high_api"], row["price_high_pdf"]),
        ]
        row_offsets = []
        for month, basis, api_price, pdf_price in legs:
            if pd.isna(month) or pd.isna(basis) or pd.isna(api_price) or pd.isna(pdf_price):
                return None
            offset = float(pdf_price) - float(api_price)
            if abs(offset) > _MAX_REPRICE_USD_BU:
                return None
            row_offsets.append(offset)
            key = (str(row["commodity"]), float(month))
            if key in offsets and abs(offsets[key] - offset) > _TOLERANCE:
                return None
            offsets[key] = offset

        # The average is the mid of the two legs, so it must have moved by
        # the mid of the two leg offsets — no free parameter of its own.
        if not pd.isna(row["average_api"]) and not pd.isna(row["average_pdf"]):
            moved = float(row["average_pdf"]) - float(row["average_api"])
            if abs(moved - sum(row_offsets) / 2) > _TOLERANCE:
                return None
    return offsets


def check_against_stored_rows(api: pd.DataFrame, stored: pd.DataFrame) -> list[str]:
    """Return every disagreement between the two transports on shared dates.

    A row present on one side only is *not* a disagreement: the PDF prints
    what it prints, and a report the API carries more of is more data, not
    contradicted data. Only cells that both sides fill and fill differently
    count — that is the mapping being wrong.
    """
    if api.empty or stored.empty:
        return []
    shared_dates = set(api["report_date"]) & set(stored["report_date"])
    if not shared_dates:
        logger.warning(
            "No overlapping report dates — the mapping enters the table "
            "unchecked against the PDF path."
        )
        return []

    merged = api[api["report_date"].isin(shared_dates)].merge(
        stored[stored["report_date"].isin(shared_dates)],
        on=_KEY, how="inner", suffixes=("_api", "_pdf"),
    )
    # Dates whose only gap is a futures re-snapshot are set aside first, so a
    # prelim-vs-final vintage does not read as 24 broken cells. The API row
    # then supersedes the stored prelim, which is the later, published truth.
    repriced: dict[str, dict[tuple[str, float], float]] = {}
    for report_date, day in merged.groupby("report_date"):
        quote_differs = any(
            _differing(day, column).any()
            for column in (_NUMERIC_COLUMNS + _TEXT_COLUMNS)
            if column not in _PRICE_COLUMNS
        )
        if quote_differs or not any(
            _differing(day, column).any() for column in _PRICE_COLUMNS
        ):
            continue
        offsets = _repricing_offsets(day)
        if offsets is not None:
            repriced[str(report_date)] = offsets

    for report_date, offsets in repriced.items():
        logger.warning(
            "%s: the stored PDF rows are an earlier vintage of the same report "
            "— identical basis, futures re-snapshotted by %s. The archive row "
            "supersedes them.", report_date,
            ", ".join(f"{c} {int(m)}: {o:+.4f}" for (c, m), o in sorted(offsets.items())),
        )

    problems: list[str] = []
    for column in _NUMERIC_COLUMNS + _TEXT_COLUMNS:
        differs = _differing(merged, column)
        if column in _PRICE_COLUMNS and repriced:
            differs &= ~merged["report_date"].isin(repriced)
        for _, row in merged[differs].iterrows():
            problems.append(
                f"{row['report_date']} {row['commodity']} {row['delivery']}: "
                f"{column} API={row[f'{column}_api']!r} PDF={row[f'{column}_pdf']!r}"
            )
    logger.info(
        "Cross-checked %d rows over %d overlapping report dates — %d disagreements.",
        len(merged), len(shared_dates), len(problems),
    )
    return problems


def _store(df: pd.DataFrame, stored: pd.DataFrame) -> int:
    problems = check_against_stored_rows(df, stored)
    if problems:
        raise SystemExit(
            "MARS mapping disagrees with the stored PDF-parsed rows — nothing "
            "written. Fix the mapping, do not widen the check:\n  "
            + "\n  ".join(problems[:20])
        )
    save_gulf_bids(df)
    return len(df)


def backfill_archive() -> int:
    """One pull of the whole archive. Returns rows written."""
    result = fetch_gulf_bids_api()
    if not result.has_rows:
        # One unmappable row anywhere in 6.5 years rejects the whole pull,
        # which is the right default (the mapping is wrong until proven
        # otherwise) and the wrong end state: --per-date isolates the bad
        # dates, stores the rest, and names what it skipped.
        raise SystemExit(
            f"Archive pull returned no rows: {result.error}\n"
            "If that is a mapping rejection rather than transport, re-run with "
            "--per-date to store the dates that do map and name the ones that "
            "do not."
        )
    df = result.data["gulf_bids"]
    logger.info(
        "Archive pull: %d rows over %d report dates (%s → %s).",
        len(df), df["report_date"].nunique(),
        df["report_date"].min(), df["report_date"].max(),
    )
    return _store(df, _stored_rows())


def backfill_per_date(start: str, end: str) -> int:
    """Walk the archive one report date at a time — the fallback path.

    A date the report was not published on comes back empty and is counted,
    never treated as a failure; a date whose rows the mapping rejects is
    skipped and named, exactly as the MAGyP backfill does. A skipped day is
    evidence, not a reason to loosen a guard.
    """
    stored = _stored_rows()
    total = unpublished = 0
    rejected: list[str] = []
    for ts in pd.bdate_range(start=start, end=end):
        day = ts.date().isoformat()
        result = fetch_gulf_bids_api(day)
        time.sleep(_POLITE_DELAY_S)
        if result.status == "empty":
            unpublished += 1
            continue
        if not result.has_rows:
            rejected.append(f"{day}: {result.error}")
            logger.warning("Day skipped — %s: %s", day, result.error)
            continue
        total += _store(result.data["gulf_bids"], stored)
        logger.info("Stored %s — %d rows", day, len(result.data["gulf_bids"]))

    logger.info(
        "Per-date walk done: %d rows stored, %d unpublished dates, %d rejected.",
        total, unpublished, len(rejected),
    )
    if rejected:
        logger.warning("Rejected dates (mapping drift?): %s", rejected)
    return total


if __name__ == "__main__":
    setup_logging()
    if not is_api_configured():
        raise SystemExit(
            "MARS_API_KEY not set. This backfill needs it; the daily Layer 20 "
            "PDF path does not and is unaffected."
        )
    init_database()

    args = sys.argv[1:]
    if args and args[0] == "--per-date":
        args = args[1:]
        start = args[0] if args else MARS_GULF_BIDS_ARCHIVE_START
        end = args[1] if len(args) > 1 else pd.Timestamp.today().date().isoformat()
        rows = backfill_per_date(start, end)
    else:
        rows = backfill_archive()

    print(f"Backfilled {rows} rows into gulf_bids.")
    if rows == 0:
        sys.exit(1)
