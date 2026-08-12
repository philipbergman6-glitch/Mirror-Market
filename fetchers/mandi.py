"""
Layer 16 — India domestic soybean spot via the data.gov.in Mandi Price API.

Official Agmarknet feed (Ministry of Agriculture) republished on the Open
Government Data platform under the GODL licence. Replaces the NCDEX Bhav
Copy source (``fetchers/india_domestic.py``, kept on disk as a dormant
fallback): NCDEX soy derivatives are SEBI-suspended to at least 2027-03-31
and its spot pages sit behind a fingerprint anti-bot wall.

Why it matters:
    India is the world's #4 soybean consumer. Maharashtra (Latur, Vidarbha)
    is the #1 producing state since 2025-26 (~47% of the crop per SOPA
    Kharif 2025), with Madhya Pradesh (~39%) second — but Indore/MP remains
    the crush-industry pricing hub, so the MP series stays the headline
    benchmark. When Indian beans are cheap vs CBOT, import appetite fades
    and Middle East / African meal buyers switch suppliers.

Series construction:
    One series per configured state (``MANDI_STATES``), one row per
    arrival date — the MEDIAN of ``modal_price`` across all reporting
    mandis in that state (~115/day in MP), robust to single-mandi
    outliers. Prices arrive in INR/quintal (100 kg) and are stored as
    INR/MT (×10). Volume is the distinct reporting-mandi row count.
    USD conversion happens at the analysis layer. Series are stored
    per-state and never pooled — a cross-state median would put a level
    break on the existing MP history.

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

Key handling:
    ``DATA_GOV_IN_API_KEY`` is used when set; otherwise the published
    sample key (a public testing credential). The sample key caps every
    response at 10 rows and shares a global throttle, so pagination and
    429-retry are load-bearing here, not defensive.

Parser strategy:
    The API returns JSON with ``total`` and ``records``. A response
    missing those, or records missing the price/date fields, raises
    ScraperShapeError — the schema changed and silence would be worse
    than a crash. Zero records for the filter is a normal holiday/Sunday
    outcome (mandis closed), not an error.

    That last sentence is what makes a *field* rename dangerous rather
    than loud: filtering on a field this resource no longer exposes
    returns HTTP 200 with zero rows, which is indistinguishable from a
    closed mandi day. Every response also carries the resource's own
    field catalog, empty ones included, so ``_assert_fields_exist``
    checks the catalog on every page — the only check that can see a
    rename on a day with no records. ``_assert_filters_honoured``
    covers the opposite failure, rows arriving that we never asked for.

User-Agent:
    api.data.gov.in **blackholes** any request whose User-Agent names
    Python — the connection is accepted and then never answered, so it
    surfaces as a read timeout rather than a 403 (verified 2026-08-10:
    ``python-requests/2.32.3`` and ``Python/3.11 aiohttp/3.9`` both hang
    until the client gives up; ``Mirror-Market/…`` and curl's default
    both return 200 in ~1.2s from the same IP, same second). requests'
    default UA is exactly that string, so the layer had been dark on
    every run since it shipped. The honest project UA below is not a
    spoof and is load-bearing — do not drop it.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence
from datetime import datetime

import pandas as pd
import requests

from config import (
    MANDI_API_URL,
    MANDI_COMMODITY,
    MANDI_MAX_PAGES,
    MANDI_MODAL_MAX_INR_QUINTAL,
    MANDI_MODAL_MIN_INR_QUINTAL,
    MANDI_PAGE_LIMIT,
    MANDI_PAGE_LIMIT_PERSONAL,
    MANDI_SAMPLE_API_KEY,
    MANDI_SORT_FIELD,
    MANDI_STATES,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
)
from fetchers._backoff import retry_sleep
from pipeline.results import FetchResult, ScraperShapeError

logger = logging.getLogger(__name__)

_QUINTAL_TO_MT = 10.0  # INR/quintal (100 kg) → INR/MT

# See the User-Agent note in the module docstring: a Python-identifying UA
# is silently blackholed by api.data.gov.in. Identifies the project
# honestly rather than impersonating a browser — the endpoint only rejects
# Python, not non-browsers.
_HEADERS = {
    "User-Agent": (
        "Mirror-Market/1.0 "
        "(+https://github.com/philipbergman6-glitch/Mirror-Market)"
    ),
}


def _api_key() -> str:
    return os.environ.get("DATA_GOV_IN_API_KEY") or MANDI_SAMPLE_API_KEY


def _page_limit() -> int:
    """Sample key is hard-capped at 10 rows/page; a personal key supports
    larger pages, cutting request count (and 429 exposure) ~10×."""
    if os.environ.get("DATA_GOV_IN_API_KEY"):
        return MANDI_PAGE_LIMIT_PERSONAL
    return MANDI_PAGE_LIMIT


def _fetch_page(offset: int, state: str) -> dict:
    """Fetch one page of the mandi resource for one state. Raises on
    exhausted retries.

    429s are expected on the shared-throttle sample key and retried with
    backoff like any transport failure. So is the throttle's *other*
    shape: an HTTP 200 carrying ``{"error": "Rate limit exceeded"}`` and
    no records, which is a transport condition wearing a payload's
    clothes — retried here rather than being handed on to
    ``_collect_records``, where a missing ``records`` key means "the
    schema changed" and hard-fails the whole layer (observed twice
    against the shared sample key, 2026-08-12).

    Pagination is sorted (``MANDI_SORT_FIELD``): unsorted offset paging
    on this resource repeats rows across pages and drops others entirely.
    """
    params: dict[str, str | int] = {
        "api-key": _api_key(),
        "format": "json",
        "limit": _page_limit(),
        "offset": offset,
        "filters[commodity]": MANDI_COMMODITY,
        "filters[state]": state,
        f"sort[{MANDI_SORT_FIELD}]": "asc",
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
                payload = resp.json()
                if "records" in payload or not payload.get("error"):
                    return payload
                last_error = f"API error: {payload['error']}"
            else:
                last_error = f"HTTP {resp.status_code}"
            logger.warning(
                "Mandi API: %s at offset %d (attempt %d)",
                last_error, offset, attempt,
            )
        except (requests.RequestException, ValueError) as exc:
            last_error = str(exc)
            logger.warning(
                "Mandi API attempt %d failed at offset %d: %s", attempt, offset, exc
            )
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)
    raise requests.RequestException(
        f"Mandi API: offset {offset} failed after {MAX_RETRIES} attempts ({last_error})"
    )


def _dedupe(records: list[dict]) -> list[dict]:
    """Drop rows that are identical in every field, preserving order.

    Belt-and-braces behind ``MANDI_SORT_FIELD``: an unsorted page walk
    served the same mandi twice and skipped another, which inflated
    Volume (the reporting-mandi count) without moving the median. A
    market legitimately appears more than once per day under different
    variety/grade rows — those differ in a field and are kept.
    """
    seen: set[tuple] = set()
    unique: list[dict] = []
    for rec in records:
        key = tuple(sorted((k, str(v)) for k, v in rec.items()))
        if key in seen:
            continue
        seen.add(key)
        unique.append(rec)
    if len(unique) < len(records):
        logger.warning(
            "Mandi API: dropped %d duplicate row(s) of %d — paging overlap",
            len(records) - len(unique), len(records),
        )
    return unique


# The field ids this module filters on and parses. Deliberately narrower
# than the resource's catalog — see _assert_fields_exist.
_REQUIRED_FIELD_IDS = frozenset({"state", "commodity", "arrival_date", "modal_price"})


def _assert_fields_exist(payload: dict) -> None:
    """Check the resource still exposes the field ids we filter and parse on.

    The failure mode this exists for is **silent emptiness**, and it is
    only visible here. Verified live 2026-08-12: an unrecognised filter
    field is not rejected and does not degrade to unfiltered — the API
    answers HTTP 200, ``message: "Resource lists ok"``, ``total: 0``,
    ``records: []``. ``filters[commodity_name]``, ``filters[Commodity]``
    and ``filters[state_name]`` each returned 0 against a resource
    holding 6,421 rows that second. So the day data.gov.in renames
    ``state`` or ``commodity``, this layer does not break — it goes
    quiet, and a quiet mandi day is an ordinary one (Sunday, holiday,
    pre-arrival hours), which is why ``india_domestic`` grades empty as
    a success. The 7-day ``LAYER_MAX_DATA_AGE_DAYS`` budget would
    eventually notice, a week of dark data later, and would blame
    staleness rather than a rename.

    Every response carries the resource's own field catalog — including
    the zero-record ones (verified on the same probe) — so the rename is
    detectable on an empty day at no extra request. That is the whole
    point: a check that only fires when rows come back cannot see this.

    Only the ids the code actually depends on are required. Extra fields
    appearing is not a break, and ``variety``/``grade``/``district`` are
    carried by the records but never read.
    """
    catalog = payload.get("field")
    if not isinstance(catalog, list):
        raise ScraperShapeError(
            "Mandi API: response carries no 'field' catalog — envelope changed "
            f"(keys: {sorted(payload)[:12]})"
        )
    ids = {f.get("id") for f in catalog if isinstance(f, dict)}
    missing = sorted(_REQUIRED_FIELD_IDS - ids)
    if missing:
        raise ScraperShapeError(
            f"Mandi API: resource no longer exposes field(s) {missing} — renamed "
            f"or dropped (exposed: {sorted(i for i in ids if i)}). Filters on a "
            "renamed field return 0 rows at HTTP 200, so this would otherwise "
            "read as a closed-mandi day"
        )


def _assert_filters_honoured(records: list[dict], state: str) -> None:
    """Check the rows we were handed are the rows we asked for.

    Complements ``_assert_fields_exist`` from the other side. The
    probe above found this API answers an unknown filter with nothing
    rather than with everything, so today a filter that stops being
    applied cannot reach ``_aggregate``. That is a behaviour of the
    upstream, not a guarantee from it, and the consequence if it ever
    changes is not an outage but a **wrong number**: a median over every
    commodity in every state, stored under ``Soybean (Mandi MP)``,
    indistinguishable in shape from a real one. Cheap to pin, so pinned.

    Raises rather than filtering the offending rows out. A response that
    mixes states is not a response with some bad rows in it — it means
    the request no longer means what the code thinks it means, and the
    rows that *did* match are then a partial set of unknown size.
    """
    for rec in records:
        got_state = str(rec.get("state", "")).strip()
        got_commodity = str(rec.get("commodity", "")).strip()
        if got_state.casefold() != state.casefold():
            raise ScraperShapeError(
                f"Mandi API: asked for state {state!r}, got a row for "
                f"{got_state!r} — the state filter is no longer applied"
            )
        if got_commodity.casefold() != MANDI_COMMODITY.casefold():
            raise ScraperShapeError(
                f"Mandi API: asked for commodity {MANDI_COMMODITY!r}, got a row "
                f"for {got_commodity!r} — the commodity filter is no longer applied"
            )


def _collect_records(state: str) -> list[dict]:
    """Paginate through the state-filtered resource until ``total`` rows are in hand.

    A walk that ends short of the resource's own ``total`` raises. The
    daily number this feeds is a **median across the reporting mandis**,
    so a truncated walk does not produce a missing number — it produces a
    *plausible wrong one*, computed over whichever pages happened to
    survive, with nothing in its shape to mark it as partial. That is the
    failure mode #206 traced: three different closes for MP on the same
    date (₹67,430 / ₹67,250 / ₹67,360) were three different surviving
    subsets, not three different markets.

    Two ways the walk can end short, both previously silent:
      * the page cap (``MANDI_MAX_PAGES``) — logged a warning and returned
        the truncated set;
      * a page answering zero records before ``total`` is reached, which
        broke the loop as if the set were complete.
    Both now raise. A truncated median is worse than no row at all.
    """
    records: list[dict] = []
    total: int | None = None

    for page in range(MANDI_MAX_PAGES):
        payload = _fetch_page(page * _page_limit(), state)
        if "records" not in payload or "total" not in payload:
            raise ScraperShapeError(
                "Mandi API: response missing 'records'/'total' — schema changed "
                f"(keys: {sorted(payload)[:10]})"
            )
        _assert_fields_exist(payload)
        total = int(payload["total"])
        page_records = payload["records"]
        records.extend(page_records)
        if len(records) >= total or not page_records:
            break

    # Measured before _dedupe: a shortfall here means pages we never
    # received, which is the truncation that corrupts the median. Paging
    # *overlap* (raw ≥ total, distinct < total) is a different and much
    # milder defect — it inflates the mandi count without moving the
    # median — and _dedupe keeps warning about it rather than raising,
    # because a source that legitimately repeats an identical row would
    # otherwise hard-fail the layer every day.
    if total is not None and len(records) < total:
        raise requests.RequestException(
            f"Mandi API: {state} walk truncated at {len(records)}/{total} records "
            f"after {page + 1} page(s) of {_page_limit()} — a median over a "
            "partial mandi set is a wrong number, not a missing one (#206)"
        )
    _assert_filters_honoured(records, state)
    return _dedupe(records)


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

    for date, median in zip(agg["date"], agg["close"], strict=True):
        if not MANDI_MODAL_MIN_INR_QUINTAL <= median <= MANDI_MODAL_MAX_INR_QUINTAL:
            raise ScraperShapeError(
                f"Mandi API: {date} median modal_price ₹{median:,.0f}/quintal is "
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


def fetch_mandi_prices() -> FetchResult:
    """Fetch the soybean mandi set for each configured state.

    Returns one series per state in ``MANDI_STATES``. A schema change
    (ScraperShapeError) in any state is ``failed`` — the resource is
    shared, so a shape break in one state means the source changed for
    all. Zero matching records everywhere is ``empty`` (mandis closed —
    Sunday/holiday).

    Transport exhaustion on **any** state is ``failed``, even when another
    state returned a full set. Why the whole layer and not just that
    state:

      * States are never pooled (see ``MANDI_STATES``), so a missing state
        does not corrupt the surviving state's number. The rows that did
        arrive are therefore still worth storing — hence
        ``FetchResult.partial``, which saves them and grades the run
        failed, rather than ``FetchResult.failed``, which would discard
        them. The resource serves the current day only, so a discarded
        day is a permanent hole.
      * But the *verdict* has to be the failure. ``india_domestic`` has no
        ``LAYER_MIN_KEYS`` floor, so nothing downstream would notice half
        the layer going dark; it would stamp a fresh ``last_success``
        against a state that was never asked. That is precisely the
        empty-success inversion CLAUDE.md's "Success requires rows"
        section exists to prevent.
      * An empty state and a failed state are not the same thing and are
        not treated the same here. Empty means asked and answered with
        nothing (a state holiday — MP and MH keep different local
        calendars, so one-state-empty is an ordinary day) and does not
        contribute an error. Failed means never answered, so its absence
        carries no information about whether data existed.
    """
    data: dict[str, pd.DataFrame] = {}
    errors: list[str] = []
    for state, series in MANDI_STATES.items():
        logger.info(
            "Fetching %s mandi prices for %s from data.gov.in ...",
            MANDI_COMMODITY, state,
        )
        try:
            records = _collect_records(state)
            df = _aggregate(records)
        except ScraperShapeError as exc:
            logger.error("Mandi API: %s", exc)
            return FetchResult.failed(str(exc))
        except requests.RequestException as exc:
            errors.append(f"{state}: {exc}")
            continue

        if df.empty:
            logger.info("Mandi API: no %s rows for %s today", MANDI_COMMODITY, state)
            continue

        logger.info(
            "Mandi API: %s — %d session row(s), latest ₹%.0f/MT across %d mandis",
            state, len(df), df["Close"].iloc[-1], int(df["Volume"].iloc[-1]),
        )
        data[series] = df

    if errors:
        reason = "; ".join(errors)
        if data:
            logger.error(
                "Mandi API: partial result — %s of %d state(s) failed: %s",
                len(errors), len(MANDI_STATES), reason,
            )
            return FetchResult.partial(data, reason)
        return FetchResult.failed(reason)
    if data:
        return FetchResult.ok(data)
    return FetchResult.empty(
        f"no {MANDI_COMMODITY} rows for any of {', '.join(MANDI_STATES)} today"
    )


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
