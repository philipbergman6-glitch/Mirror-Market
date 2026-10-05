"""
Layer 16 — India domestic soybean spot from the Agmarknet 2.0 report API.

The official Agmarknet mandi feed (Directorate of Marketing & Inspection,
Ministry of Agriculture), read from the keyless JSON backend behind
agmarknet.gov.in. Replaces the NCDEX Bhav Copy source
(``fetchers/india_domestic.py``, kept on disk as a dormant fallback): NCDEX
soy derivatives are SEBI-suspended to at least 2027-03-31.

Source history:
    2026-08 → 2026-09-24 this layer read the same Agmarknet feed as
    republished on data.gov.in. From ~2026-09-24 api.data.gov.in refuses
    TCP from every non-Indian address, so the layer went dark. Re-sourced
    2026-10-05 to ``api.agmarknet.gov.in``, which answers from anywhere.
    Validated that day before the switch: on complete days the MP median
    matched our stored data.gov.in values to ≤0.23% (6/6 days) and MH to
    ≤1% (16/17; the miss a 6-mandi Sunday); every one of 202 MSAMB (the
    Maharashtra board) market-days carried the same modal price here.
    **MH ``Volume`` steps up ~20–35% at the switch**: Agmarknet carries MH
    rows the data.gov.in snapshot never held. Prices join cleanly; the
    mandi count does not, so never compare MH Volume across the switch.

Licence:
    Agmarknet's website policy: "Information featured on this website may
    be reproduced free of charge … reproduced accurately and not … in a
    misleading context … the source must be prominently acknowledged."
    The state median is this project's calculation, not a DMI figure.

Why it matters:
    India is the world's #4 soybean consumer. Maharashtra (Latur, Vidarbha)
    is the #1 producing state since 2025-26 (~47% of the crop per SOPA
    Kharif 2025), with Madhya Pradesh (~39%) second — but Indore/MP remains
    the crush-industry pricing hub, so the MP series stays the headline
    benchmark. When Indian beans are cheap vs CBOT, import appetite fades
    and Middle East / African meal buyers switch suppliers.

Series construction:
    One series per configured state (``MANDI_STATES``), one row per
    *completed* Indian arrival date — the MEDIAN of the modal price across
    every per-variety row the report carries for that state and day (~115
    rows/day in MP), robust to single-mandi outliers. Prices arrive in
    INR/quintal (100 kg) and are stored as INR/MT (×10). Volume is the
    row count. USD conversion happens at the analysis layer. Series are
    stored per-state and never pooled — a cross-state median would put a
    level break on the existing MP history.

Completed days and the lookback (#243):
    The current IST day fills mandi by mandi until late evening, so a run
    during it would store a plausible, unfinished median. Only dates at
    least ``MANDI_MIN_AGE_DAYS`` old are kept. Every run re-reads the
    trailing ``MANDI_LOOKBACK_DAYS`` (as whole months — the report is
    month-granular), so a mandi uploading after a run is picked up by the
    next one and a missed run backfills itself. The upsert on
    (Date, commodity) makes the re-read idempotent.

Level validation (#206, 2026-08-12):
    The mandi level is *correct* and its ~+66% premium over CBOT is
    real, not a units error. On 2026-08-11 the MP median across all 115
    reporting mandis was ₹6,725/qtl (₹67,250/MT, $705/MT) against CBOT
    $425/MT — a +$280/MT, +66% premium. Three checks agree:
    commodityonline's national mandi average ₹6,706/qtl (09 Aug),
    Agriwatch's ₹6,700–6,900/qtl band, and — the one that is *not*
    Agmarknet-derived — SOPA's own Indore complex quotes, soy oil
    ₹1,400/10kg and soymeal ex-factory ₹57,000–57,500/MT, which imply a
    bean value of ~₹70,400/MT at an 18%/79% yield, i.e. a ~+4.7% gross
    crush margin on our number. India's GM-import ban plus its tariff
    wall means there is no arbitrage pulling the domestic bean toward
    CBOT; a large premium is the market's normal state, and it reached
    ~2× in 2021. Variety/grade mixing was measured and is immaterial
    (MP Yellow ₹6,765 vs Soyabeen ₹6,725, FAQ-only median 0.36% from
    the all-rows median), so no variety filter is applied.

Why there is no High/Low:
    Dropped in #206. Agmarknet's ``min_price``/``max_price`` are the
    extremes of individual *lots* at one mandi, including distress and
    refuse lots: on 2026-08-11 Indore APMC reported min ₹1,475 against a
    modal ₹6,750, and Tarana APMC ₹800 — so the cross-mandi min of those
    minima stored a ₹1,010/MT "low" on a ₹67,250/MT day. There is no
    intraday range here to record: the series is one cross-sectional
    median per day, and Open/High/Low are all left NaN rather than
    filled with a number that reads like a trading range and is not one.

Guards — why the report is checked against the request:
    Agmarknet answers a request it cannot honour with ``success: true``.
    Probed 2026-10-05: an unknown state id returns a report titled
    "State/UT : N/A" with no markets, and a future month or a state with no
    soybean returns an empty report — indistinguishable in shape from a
    closed day. So every response must carry ``success: true``, a title
    naming exactly the month, commodity and state asked for, the
    ``arrivalDate``/``modalPrice`` columns with the modal column declaring
    ``MANDI_PRICE_UNIT``, and only arrival dates inside the month asked
    for. Any miss raises ScraperShapeError. A trailing month-sized window
    with no completed rows at all is a failure too, never "mandis closed":
    MP and MH trade soybean every week of the year.
"""

from __future__ import annotations

import calendar
import logging
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from config import (
    MANDI_API_URL,
    MANDI_COMMODITY,
    MANDI_COMMODITY_ID,
    MANDI_LOOKBACK_DAYS,
    MANDI_MIN_AGE_DAYS,
    MANDI_MODAL_MAX_INR_QUINTAL,
    MANDI_MODAL_MIN_INR_QUINTAL,
    MANDI_PRICE_UNIT,
    MANDI_STATE_IDS,
    MANDI_STATES,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
)
from fetchers._backoff import retry_sleep
from pipeline.results import FetchResult, ScraperShapeError

logger = logging.getLogger(__name__)

_QUINTAL_TO_MT = 10.0  # INR/quintal (100 kg) → INR/MT
_IST = ZoneInfo("Asia/Kolkata")

_HEADERS = {
    "User-Agent": (
        "Mirror-Market/1.0 "
        "(+https://github.com/philipbergman6-glitch/Mirror-Market)"
    ),
    "Accept": "application/json",
}

# Column keys this module parses. Extra columns appearing is not a break.
_REQUIRED_COLUMN_KEYS = frozenset({"arrivalDate", "modalPrice"})


def _ist_today() -> date:
    return datetime.now(_IST).date()


def _window_months(today: date) -> list[tuple[int, int]]:
    """Every (year, month) the trailing lookback window touches, oldest first."""
    start = today - timedelta(days=MANDI_LOOKBACK_DAYS)
    months: list[tuple[int, int]] = []
    year, month = start.year, start.month
    while (year, month) <= (today.year, today.month):
        months.append((year, month))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return months


def _fetch_month(state: str, year: int, month: int) -> dict:
    """Fetch one state's date-wise report for one month. Raises on
    exhausted retries."""
    params: dict[str, str | int] = {
        "year": year,
        "month": month,
        "stateId": MANDI_STATE_IDS[state],
        "commodityId": MANDI_COMMODITY_ID,
        "includeExcel": "false",
    }
    last_error = "no attempts made"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                MANDI_API_URL,
                params=params,
                headers=_HEADERS,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.json()
            last_error = f"HTTP {resp.status_code}"
        except (requests.RequestException, ValueError) as exc:
            last_error = str(exc)
        logger.warning(
            "Agmarknet: %s %04d-%02d attempt %d failed: %s",
            state, year, month, attempt, last_error,
        )
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)
    raise requests.RequestException(
        f"Agmarknet: {state} {year:04d}-{month:02d} failed after "
        f"{MAX_RETRIES} attempts ({last_error})"
    )


def _assert_report_is_the_one_requested(
    payload: dict, state: str, year: int, month: int
) -> None:
    """Raise unless the report is for exactly the month, commodity and state
    asked for, in the expected unit. See "Guards" in the module docstring."""
    if payload.get("success") is not True:
        raise ScraperShapeError(
            f"Agmarknet: {state} {year:04d}-{month:02d} answered without "
            f"success:true (message: {payload.get('message')!r})"
        )
    title = str(payload.get("title", ""))
    expected = (
        f"on {calendar.month_name[month]}, {year} for Commodity : "
        f"{MANDI_COMMODITY}, State/UT : {state}"
    )
    if not title.endswith(expected):
        raise ScraperShapeError(
            f"Agmarknet: asked for {expected!r}, report is titled {title!r} — "
            "the server answered a different request"
        )
    columns = payload.get("columns")
    if not isinstance(columns, list):
        raise ScraperShapeError(
            f"Agmarknet: report carries no 'columns' list (keys: {sorted(payload)[:12]})"
        )
    titles = {c.get("key"): str(c.get("title", "")) for c in columns if isinstance(c, dict)}
    missing = sorted(_REQUIRED_COLUMN_KEYS - titles.keys())
    if missing:
        raise ScraperShapeError(
            f"Agmarknet: report no longer carries column(s) {missing} "
            f"(carries: {sorted(k for k in titles if k)})"
        )
    if MANDI_PRICE_UNIT not in titles["modalPrice"]:
        raise ScraperShapeError(
            f"Agmarknet: modal price column is {titles['modalPrice']!r}, not "
            f"{MANDI_PRICE_UNIT} — the unit changed"
        )
    if not isinstance(payload.get("markets"), list):
        raise ScraperShapeError("Agmarknet: report carries no 'markets' list")


def _report_records(payload: dict, year: int, month: int) -> list[dict]:
    """Flatten markets → dates → variety rows into ``_aggregate``'s record shape."""
    records: list[dict] = []
    try:
        for market in payload["markets"]:
            for day in market["dates"]:
                arrival = datetime.strptime(str(day["arrivalDate"]), "%d/%m/%Y").date()
                if (arrival.year, arrival.month) != (year, month):
                    raise ScraperShapeError(
                        f"Agmarknet: {year:04d}-{month:02d} report carries "
                        f"arrival date {arrival} — the month was not applied"
                    )
                for row in day["data"]:
                    records.append({
                        "market": market["marketName"],
                        "variety": row.get("variety"),
                        "arrival_date": day["arrivalDate"],
                        "modal_price": row["modalPrice"],
                    })
    except (KeyError, TypeError, ValueError) as exc:
        raise ScraperShapeError(
            f"Agmarknet: report rows no longer parse ({type(exc).__name__}: {exc})"
        ) from exc
    return records


def _collect_records(state: str, today: date) -> list[dict]:
    """Every completed-day variety row for ``state`` across the lookback window.

    Raises ScraperShapeError when the window holds no completed rows at all:
    see "Guards" in the module docstring.
    """
    records: list[dict] = []
    for year, month in _window_months(today):
        payload = _fetch_month(state, year, month)
        _assert_report_is_the_one_requested(payload, state, year, month)
        records.extend(_report_records(payload, year, month))

    cutoff = today - timedelta(days=MANDI_MIN_AGE_DAYS)
    completed = [
        rec for rec in records
        if datetime.strptime(rec["arrival_date"], "%d/%m/%Y").date() <= cutoff
    ]
    if not completed:
        raise ScraperShapeError(
            f"Agmarknet: no completed {MANDI_COMMODITY} rows for {state} in the "
            f"{MANDI_LOOKBACK_DAYS} days to {cutoff} — {len(records)} row(s) "
            "in total; a month-sized window is never a closure"
        )
    return completed


def _aggregate(records: list[dict]) -> pd.DataFrame:
    """Distill per-mandi rows into one median-modal row per arrival date.

    Returns the ``clean_india_domestic``/``save_india_domestic`` shape:
    Date (ISO), Open/High/Low/Close (INR/MT), Volume (mandi count), Unit.
    Open/High/Low are NaN by design — see the module docstring; only the
    median modal is a defensible daily number.

    Raises ScraperShapeError if a day's median lands outside the
    ₹/quintal plausibility band, which is the only way a silent change of
    the source's price unit becomes visible: every unit reads as a valid
    float and 100× wrong is still a number.
    """
    parsed: list[dict[str, object]] = []
    malformed = 0
    for rec in records:
        try:
            arrival = datetime.strptime(str(rec["arrival_date"]), "%d/%m/%Y").date()
            modal = float(rec["modal_price"])
        except (KeyError, TypeError, ValueError):
            malformed += 1
            continue
        if modal <= 0:
            malformed += 1
            continue
        parsed.append({"date": arrival, "modal": modal})

    if records and not parsed:
        raise ScraperShapeError(
            f"Mandi API: {len(records)} records, none with parseable "
            "arrival_date/modal_price — field names or formats changed"
        )
    if malformed:
        logger.warning("Mandi API: skipped %d malformed records", malformed)

    raw = pd.DataFrame(parsed)
    if raw.empty:
        return raw

    agg = raw.groupby("date").agg(
        close=("modal", "median"),
        volume=("modal", "size"),
    ).reset_index()

    for day, median in zip(agg["date"], agg["close"], strict=True):
        if not MANDI_MODAL_MIN_INR_QUINTAL <= median <= MANDI_MODAL_MAX_INR_QUINTAL:
            raise ScraperShapeError(
                f"Mandi API: {day} median modal_price ₹{median:,.0f}/quintal is "
                f"outside the plausible band ₹{MANDI_MODAL_MIN_INR_QUINTAL:,}–"
                f"₹{MANDI_MODAL_MAX_INR_QUINTAL:,} — the source's price unit "
                "likely changed (see #206)"
            )

    df = pd.DataFrame({
        "Date": agg["date"].map(lambda d: d.isoformat()),
        # Open/High/Low: a cross-sectional median has no range. Agmarknet's
        # per-mandi min/max are lot extremes (₹800/qtl against a ₹6,750
        # modal) and stored a ₹1,010/MT "low" on a ₹67,250/MT day (#206).
        "Open": float("nan"),
        "High": float("nan"),
        "Low": float("nan"),
        "Close": agg["close"] * _QUINTAL_TO_MT,
        "Volume": agg["volume"].astype(float),
        "Unit": "INR/MT",
    })
    return df.sort_values("Date").reset_index(drop=True)


def fetch_mandi_prices(today: date | None = None) -> FetchResult:
    """Fetch the soybean mandi series for each configured state.

    ``today`` is the Indian calendar date (default: now in IST); dates on or
    after ``today - MANDI_MIN_AGE_DAYS + 1`` are not stored.

    A guard failure (ScraperShapeError) in any state is ``failed`` with no
    rows: the report format is shared, so a break in one state means the
    source changed for all.

    Transport exhaustion on **any** state is ``failed``, even when another
    state returned a full set — but the surviving state's rows are still
    returned (``FetchResult.partial``). States are never pooled, so a
    missing state does not corrupt the other's number, and ``india_domestic``
    has no ``LAYER_MIN_KEYS`` floor: a plain ``ok`` would stamp a fresh
    ``last_success`` with half the layer dark (#212).
    """
    today = today or _ist_today()
    data: dict[str, pd.DataFrame] = {}
    errors: list[str] = []
    for state, series in MANDI_STATES.items():
        logger.info(
            "Fetching %s mandi prices for %s from Agmarknet ...", MANDI_COMMODITY, state,
        )
        try:
            df = _aggregate(_collect_records(state, today))
        except ScraperShapeError as exc:
            logger.error("Agmarknet: %s", exc)
            return FetchResult.failed(str(exc))
        except requests.RequestException as exc:
            errors.append(f"{state}: {exc}")
            continue

        logger.info(
            "Agmarknet: %s — %d completed day(s), latest %s ₹%.0f/MT across %d rows",
            state, len(df), df["Date"].iloc[-1], df["Close"].iloc[-1],
            int(df["Volume"].iloc[-1]),
        )
        data[series] = df

    if errors:
        reason = "; ".join(errors)
        if data:
            logger.error(
                "Agmarknet: partial result — %d of %d state(s) failed: %s",
                len(errors), len(MANDI_STATES), reason,
            )
            return FetchResult.partial(data, reason)
        return FetchResult.failed(reason)
    return FetchResult.ok(data)


__all__: Sequence[str] = ("_aggregate", "_collect_records", "fetch_mandi_prices")


# ── Quick self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    result = fetch_mandi_prices()
    if not result.has_rows:
        logger.info("Mandi API: %s — %s", result.status, result.error)
    else:
        for name, frame in result.data.items():
            logger.info("%s:\n%s", name, frame.to_string(index=False))
