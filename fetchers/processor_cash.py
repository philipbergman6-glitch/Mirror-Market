"""
Layer 29 — US processor cash soybean oil and meal (USDA AMS report 3511, #352).

The weekly "National Grain and Oilseed Processor Feedstuff Report" carries
processor ask ranges per trade location for crude soybean oil and 46.5–48%
soybean meal: a flat price range, an average, and a basis range over a named
CBOT month. The stack priced US oil and meal only on the board; this is the
cash leg a physical buyer actually pays.

Transport is USDA's MARS API (``MARS_API_KEY``, HTTP Basic, the key as the
username), ``Report Detail`` section, one request per commodity. The whole
archive (2022-02-07 →) comes back in two ~1.6 s requests, so every run pulls
all of it: the table is self-healing and never round-trips ``data/history/``.

Five things the mapping depends on, each verified against the live archive on
2026-10-05 rather than read off a label:

* **The oil basis is in points, not the cents/lb its unit field says.** The
  API's ``basis_unit`` and the PDF's column head both read "¢/Lb"; the numbers
  are hundredths of a cent (``-50`` beside a 66.47 ¢/lb price over a 66.97
  close). Read as points, 97% of same-contract rows reconcile their own price
  and basis exactly; read as cents, 6% do.
* **"$ Per Ton" is the US short ton.** Neither the report nor its narrative
  says so. The arithmetic does: price − basis reproduces the CBOT meal close,
  and CBOT meal settles in dollars per short ton (ZMZ26 353.3 on 2026-10-01,
  to the cent, from the week of 09/28).
* **The week is the identity.** ``report_date`` is the Monday the asks were
  collected from (== ``report_begin_date``), the report covers Monday–Friday,
  and it is published Friday afternoon. Rows are keyed by the Friday.
* **A row is one series.** (location, F.O.B./Delivered, truck/rail) is unique
  per week across the whole archive; nothing here pools FOB with delivered or
  rail with truck.
* **The two basis legs can name two contracts** (``0.00V to 200.00Z``), so both
  months are stored (the #196 rule from Layer 20) — and AMS sometimes prints
  only one of them. A month AMS did not print stays NULL; it is never borrowed
  from the other leg.

Units are stored native (cents/lb, points, dollars per short ton) and
converted only in ``pipeline/units.py``.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from datetime import datetime

import numpy as np
import pandas as pd
import requests

from config import (
    MARS_API_KEY,
    MARS_BASE_URL,
    MARS_PROCESSOR_CASH_SLUG,
    MAX_RETRIES,
    PROCESSOR_CASH_COMMODITIES,
    PROCESSOR_CASH_MAX_RECONCILE_FAILURE_RATE,
    REQUEST_TIMEOUT,
)
from fetchers._backoff import retry_sleep
from pipeline.results import ScraperShapeError
from pipeline.units import points_to_cents_per_lb

logger = logging.getLogger(__name__)

TABLE = "us_processor_cash"

# Per commodity: the unit strings the API prints, what they mean, and the one
# grade the layer stores. A second meal protein would be a different good under
# the same commodity name — the DCE No.1/No.2 trap — so it raises.
_SPECS: dict[str, dict[str, str | None]] = {
    "Soybean Oil": {
        "api_unit": "Cents Per Lb",
        "price_unit": "cents_per_lb",
        "basis_unit": "points_per_lb",   # labelled "Cents Per Lb" — see module docstring
        "protein": None,
    },
    "Soybean Meal": {
        "api_unit": "$ Per Ton",
        "price_unit": "usd_per_short_ton",
        "basis_unit": "usd_per_short_ton",
        "protein": "46.5-48%",
    },
}

_QUOTE_TYPES = frozenset({"Basis", "Price"})
# Only asks have ever been printed for these two commodities. A bid is the
# other side of the market, and would be a different claim under one column.
_SALE_TYPES = frozenset({"Ask"})
_FREIGHT = {"F.O.B.": "FOB", "Delivered": "Delivered"}
# The report's own freight-code legend: T, R, RB, T/R, R/B, T/R/B.
_TRANS_MODES = frozenset({
    "Truck", "Rail", "River Barge", "Truck/Rail", "Rail/Barge", "Truck/Rail/Barge",
})

_MONTH_NAMES = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)
_CODE_MONTHS = {c: i + 1 for i, c in enumerate("FGHJKMNQUVXZ")}
_MONTH_RE = re.compile(r"^(?P<name>[A-Z][a-z]+)\s+\((?P<code>[FGHJKMNQUVXZ])\)$")

class _PullFailed(RuntimeError):
    """Transport or auth — the archive never arrived. Not a shape finding."""


_PRICE_FIELDS = ("price_min", "price_max", "avg_price")
_BASIS_FIELDS = ("basis_min", "basis_max")

# Half a tick: CBOT oil ticks 0.01 ¢/lb, meal 10¢/short ton. Two numbers AMS
# derived from one close cannot honestly differ by more.
_RECONCILE_TOLERANCE = {"Soybean Oil": 0.006, "Soybean Meal": 0.06}
# A rate over a handful of rows is noise: one week alone has ~7 checkable rows,
# and one of AMS's own misses is 14% of that. The live pull is the whole
# archive (~2,900 checkable rows), so the gate always has its sample there.
_MIN_RATE_SAMPLE = 100

FRAME_COLUMNS = [
    "week_end", "commodity", "location", "freight", "trans_mode",
    "week_start", "published_at", "sale_type", "protein",
    "price_low", "price_high", "price_avg", "price_unit",
    "basis_low", "basis_high", "basis_unit",
    "futures_month_low", "futures_month_high",
]


def _date(value: object, fmt: str, field: str) -> datetime:
    """Parse an API date. Parsed, never sliced or sorted as a string (#283)."""
    try:
        return datetime.strptime(str(value), fmt)
    except ValueError as exc:
        raise ScraperShapeError(f"AMS 3511: unparseable {field} {value!r}") from exc


def _futures_month(label: object, which: str) -> int | None:
    """"October (V)" → 10. None where AMS printed no month for this leg.

    The spelled month is checked against its own CME code: the two disagreeing
    means the row does not know which contract it is quoted over.
    """
    if label is None or (isinstance(label, float) and np.isnan(label)) or label == "":
        return None
    match = _MONTH_RE.match(str(label).strip())
    if not match:
        raise ScraperShapeError(f"AMS 3511: unparseable {which} futures month {label!r}")
    month = _CODE_MONTHS[match["code"]]
    if _MONTH_NAMES[month - 1] != match["name"]:
        raise ScraperShapeError(
            f"AMS 3511: {which} futures month {label!r} names a month its CME code contradicts"
        )
    return month


def _filled(row: dict, fields: Sequence[str]) -> list[bool]:
    return [row.get(f) not in (None, "") for f in fields]


def _map_row(row: dict) -> dict[str, object] | None:
    """One Report Detail row → one stored row, or None for a slot with no quote."""
    commodity = row.get("commodity")
    spec = _SPECS.get(commodity)  # type: ignore[arg-type]
    if spec is None:
        raise ScraperShapeError(
            f"AMS 3511: commodity {commodity!r} answered a request for "
            f"{list(_SPECS)} — the commodity filter no longer filters"
        )

    has_price, has_basis = _filled(row, _PRICE_FIELDS), _filled(row, _BASIS_FIELDS)
    if not any(has_price) and not any(has_basis):
        return None  # a listed slot with no number in it — nothing was observed
    # Partly filled is neither a quote nor a non-quote. Never observed in the
    # archive; if it arrives, the mapping has met a shape it cannot read.
    if any(has_price) and not all(has_price):
        raise ScraperShapeError(f"AMS 3511: partly filled price {row!r}")
    if any(has_basis) and not all(has_basis):
        raise ScraperShapeError(f"AMS 3511: partly filled basis {row!r}")

    quote_type = row.get("quote_type")
    if quote_type not in _QUOTE_TYPES:
        raise ScraperShapeError(f"AMS 3511: unknown quote_type {quote_type!r}")
    if quote_type == "Price" and any(has_basis):
        raise ScraperShapeError(f"AMS 3511: a Price quote carries a basis {row!r}")
    if quote_type == "Basis" and not any(has_basis):
        raise ScraperShapeError(f"AMS 3511: a Basis quote carries no basis {row!r}")

    if row.get("price_unit") != spec["api_unit"]:
        raise ScraperShapeError(
            f"AMS 3511: {commodity} price unit {row.get('price_unit')!r}, expected "
            f"{spec['api_unit']!r} — every conversion downstream rests on it"
        )
    if any(has_basis) and row.get("basis_unit") != spec["api_unit"]:
        raise ScraperShapeError(
            f"AMS 3511: {commodity} basis unit {row.get('basis_unit')!r}, expected "
            f"{spec['api_unit']!r}"
        )

    sale_type = row.get("sale_type")
    if sale_type not in _SALE_TYPES:
        raise ScraperShapeError(f"AMS 3511: unknown sale_type {sale_type!r}")
    protein = row.get("protein")
    if isinstance(protein, float) and np.isnan(protein):
        protein = None
    if protein != spec["protein"]:
        raise ScraperShapeError(
            f"AMS 3511: {commodity} protein {protein!r}, expected {spec['protein']!r} "
            "— a different grade is a different good"
        )
    freight = row.get("freight")
    if freight not in _FREIGHT:
        raise ScraperShapeError(f"AMS 3511: unknown freight term {freight!r}")
    trans_mode = row.get("trans_mode")
    if trans_mode not in _TRANS_MODES:
        raise ScraperShapeError(f"AMS 3511: unknown trans_mode {trans_mode!r}")
    location = row.get("trade Loc")
    if not location or not str(location).strip():
        raise ScraperShapeError(f"AMS 3511: row has no trade location {row!r}")

    begin = _date(row.get("report_begin_date"), "%m/%d/%Y", "report_begin_date")
    end = _date(row.get("report_end_date"), "%m/%d/%Y", "report_end_date")
    if _date(row.get("report_date"), "%m/%d/%Y", "report_date") != begin:
        raise ScraperShapeError(
            f"AMS 3511: report_date {row.get('report_date')!r} is not the week's "
            f"first day {row.get('report_begin_date')!r}"
        )
    # Monday to Friday on all 233 archived weeks. A different span is a
    # different report shape, and the week is this table's identity.
    if (end - begin).days != 4:
        raise ScraperShapeError(
            f"AMS 3511: week {begin:%Y-%m-%d}→{end:%Y-%m-%d} is not Monday-Friday"
        )
    published = _date(row.get("published_date"), "%m/%d/%Y %H:%M:%S", "published_date")

    def num(field: str) -> float | None:
        value = row.get(field)
        return None if value in (None, "") else float(value)

    return {
        "week_end": end.date().isoformat(),
        "commodity": commodity,
        "location": str(location).strip(),
        "freight": _FREIGHT[freight],
        "trans_mode": trans_mode,
        "week_start": begin.date().isoformat(),
        "published_at": published.isoformat(sep=" "),
        "sale_type": sale_type,
        "protein": protein,
        "price_low": num("price_min"),
        "price_high": num("price_max"),
        "price_avg": num("avg_price"),
        "price_unit": spec["price_unit"],
        "basis_low": num("basis_min"),
        "basis_high": num("basis_max"),
        "basis_unit": spec["basis_unit"] if any(has_basis) else None,
        "futures_month_low": _futures_month(row.get("min_basis_futures_month"), "low-leg"),
        "futures_month_high": _futures_month(row.get("max_basis_futures_month"), "high-leg"),
    }


def map_rows(rows: Sequence[dict]) -> pd.DataFrame:
    """Map Report Detail rows onto the stored frame, or raise."""
    mapped = [m for m in (_map_row(row) for row in rows) if m is not None]
    df = pd.DataFrame(mapped, columns=FRAME_COLUMNS)
    for col in ("futures_month_low", "futures_month_high"):
        df[col] = df[col].astype("Int64")  # NULL where AMS printed no month
    key = ["week_end", "commodity", "location", "freight", "trans_mode"]
    dupes = df[df.duplicated(key, keep=False)]
    if not dupes.empty:
        raise ScraperShapeError(
            f"AMS 3511: {len(dupes)} rows share one (week, location, freight, mode) — "
            "two quotes for one series would be averaged into one we never saw"
        )
    return df


def _implied_futures_gap(df: pd.DataFrame) -> pd.Series:
    """Per same-contract row: |(price_low − basis_low) − (price_high − basis_high)|.

    Both legs over one contract imply one futures level; the gap is how far
    AMS's own arithmetic misses it. NaN where the row cannot be checked.
    """
    checkable = (
        df["futures_month_low"].notna()
        & (df["futures_month_low"] == df["futures_month_high"])
        & df["price_low"].notna() & df["basis_low"].notna()
    )
    sub = df[checkable]
    is_oil = sub["basis_unit"] == "points_per_lb"
    basis_low = sub["basis_low"].where(~is_oil, sub["basis_low"].map(points_to_cents_per_lb))
    basis_high = sub["basis_high"].where(~is_oil, sub["basis_high"].map(points_to_cents_per_lb))
    gap = ((sub["price_low"] - basis_low) - (sub["price_high"] - basis_high)).abs()
    return gap.reindex(df.index)


def check_reconciliation(df: pd.DataFrame) -> None:
    """Raise when the pull's price/basis arithmetic says the mapping moved.

    Two gates, because one pull holds 4+ years of archive and a drift that
    starts this week is ~12 rows inside ~3,000:

    * archive-wide failure rate above PROCESSOR_CASH_MAX_RECONCILE_FAILURE_RATE
      (2.6% measured);
    * in the newest week, a commodity whose every checkable row fails (with at
      least three to check). The archive's worst commodity-week is 4 of 6 —
      a basis unit that changed fails all of them.
    """
    gap = _implied_futures_gap(df)
    tolerance = df["commodity"].map(_RECONCILE_TOLERANCE)
    checked = gap.notna()
    if not checked.any():
        return
    failed = checked & (gap > tolerance)
    rate = failed.sum() / checked.sum()
    if checked.sum() >= _MIN_RATE_SAMPLE and rate > PROCESSOR_CASH_MAX_RECONCILE_FAILURE_RATE:
        raise ScraperShapeError(
            f"AMS 3511: {failed.sum()} of {checked.sum()} same-contract rows "
            f"({rate:.1%}) do not reconcile price against basis — the basis unit "
            "or the column mapping has moved"
        )
    newest = df["week_end"] == df["week_end"].max()
    for commodity in PROCESSOR_CASH_COMMODITIES:
        mask = newest & checked & (df["commodity"] == commodity)
        if mask.sum() >= 3 and failed[mask].all():
            raise ScraperShapeError(
                f"AMS 3511: every checkable {commodity} row of the week ending "
                f"{df['week_end'].max()} fails to reconcile price against basis — "
                "the basis unit has moved"
            )


def is_configured() -> bool:
    """Is the layer runnable? Read at call time, like every other key check."""
    return bool(MARS_API_KEY)


def _pull(commodity: str) -> list[dict]:
    """One commodity's whole Report Detail archive, or raise."""
    url = f"{MARS_BASE_URL}/{MARS_PROCESSOR_CASH_SLUG}/Report Detail"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Fetching AMS 3511 %s over MARS (attempt %d) ...", commodity, attempt)
            resp = requests.get(
                url,
                params={"q": f"commodity={commodity}"},
                auth=(MARS_API_KEY, ""),
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code in (401, 403):
                raise _PullFailed(f"HTTP {resp.status_code} — key rejected")
            if resp.status_code == 200:
                payload = resp.json()
                break
            logger.warning("AMS 3511 %s: HTTP %d", commodity, resp.status_code)
        except (requests.RequestException, ValueError) as exc:
            logger.warning("AMS 3511 %s attempt %d failed: %s", commodity, attempt, exc)
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)
    else:
        raise _PullFailed(f"{commodity} request failed after {MAX_RETRIES} attempts")

    if not isinstance(payload, dict) or "results" not in payload:
        raise ScraperShapeError(
            f"AMS 3511: unexpected payload shape {type(payload).__name__}"
        )
    stats = payload.get("stats") or {}
    # A pull that reaches the allowance is a silently truncated archive — the
    # one failure that looks exactly like a complete one (Layer 20b's rule).
    returned, allowed = stats.get("returnedRows"), stats.get("userAllowedRows")
    if returned and allowed and returned >= allowed:
        raise ScraperShapeError(
            f"AMS 3511: {commodity} pull returned {returned} rows against a "
            f"{allowed}-row allowance — truncated, not complete"
        )
    return payload["results"]


def fetch_processor_cash() -> dict[str, pd.DataFrame]:
    """Both commodities' archives, mapped and reconciled — or ``{}``.

    Empty on any failure: the archive always carries hundreds of weeks, so an
    empty return can only mean the pull, the mapping or the reconciliation
    broke, and the layer grades it as a failure (``empty_fails``).
    """
    try:
        rows = [row for commodity in PROCESSOR_CASH_COMMODITIES for row in _pull(commodity)]
        df = map_rows(rows)
        if df.empty:
            logger.error("AMS 3511: the archive answered with no quoted rows")
            return {}
        for commodity in PROCESSOR_CASH_COMMODITIES:
            if not (df["commodity"] == commodity).any():
                raise ScraperShapeError(f"AMS 3511: no {commodity} rows in the archive")
        check_reconciliation(df)
    except (ScraperShapeError, _PullFailed) as exc:
        logger.error("AMS 3511: %s", exc)
        return {}
    logger.info(
        "AMS 3511: %d processor cash rows over %d weeks (newest week ending %s).",
        len(df), df["week_end"].nunique(), df["week_end"].max(),
    )
    return {TABLE: df}


__all__: Sequence[str] = (
    "FRAME_COLUMNS",
    "check_reconciliation",
    "fetch_processor_cash",
    "is_configured",
    "map_rows",
)
