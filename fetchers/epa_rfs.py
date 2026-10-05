"""
Layer 29 — EPA Renewable Fuel Standard: the soy-oil demand lever.

    rin_generation  EMTS RINs generated per production month, by D-code
    rin_prices      weekly volume-weighted average price of separated RINs, USD/RIN
    rvo             final-rule volume requirements, cross-checked against EPA

WHY THIS IS HERE. US biofuel policy is the largest single driver of soybean
oil's share of crush value. Layer 13 (EIA) measures the biodiesel that gets
made; this layer carries what makes it worth making — how many D4 credits the
obligation requires, how many are being generated against it, and what a D4
credit is worth next to the conventional D6 one.

WHAT THIS IS NOT. A RIN price is not a commodity price. It is stored in USD
per RIN and never enters `to_usd_mt`: turning it into USD/MT of oil needs a
RINs-per-gallon × gallons-per-tonne bridge that this layer does not invent
(#353). Nor are the RVOs a forecast — only rules EPA has published as final
are carried, from `config.EPA_RFS_RVO_REFERENCE`, with their citation.

Traps this module exists to survive:

1.  **The generation CSV's URL rotates every month.** EPA files it as
    `/system/files/other-files/<upload YYYY-MM>/rindata_<mon><yyyy>.csv`, so
    the link is resolved from the landing page on every run (invariant 10) and
    the newest *data month in the filename* wins — not the upload folder,
    which can hold an older month re-posted. Month tokens are not uniform
    (`sept2015`, `june2016`, `aug2026`): only the first three letters are read.

2.  **Revisions are normal.** EPA: "the data in these reports may change due
    to reasons such as remedial actions or updated/resubmitted reports." Every
    file carries the whole history from July 2010, every month is upserted on
    every run, and the newest month is stamped ``preliminary`` — it is the one
    most likely to move.

3.  **RIN prices exist only inside a Qlik Sense app.** There is no file. The
    app answers anonymous JSON-RPC over a websocket once the page has issued a
    session cookie. We ask the engine for the *same measure EPA's own chart
    draws* (`config.EPA_RFS_PRICE_MEASURE`) rather than re-averaging raw trade
    rows, so the number stored is EPA's number with EPA's outlier filters.

4.  **BBD changed units in 2026.** Up to 2025 the biomass-based diesel
    requirement was physical gallons (3.35 billion for 2025); from 2026 it is
    RINs (9.07 billion). A D4-generation-vs-BBD comparison is only like-for-
    like from 2026, which is why no earlier year is entered.

5.  **A rule can move under a stored number.** EPA has said it will propose
    reallocating more 2025 exempted volume into 2026-2027. The rvo key
    compares the reference totals with the obligation EPA's app reports for
    the same year and fails — loudly, and only that key — when they disagree,
    so a superseded obligation is never shown as the current one.

6.  **A frozen key can hide behind a fresh one.** main.py dates a layer by its
    newest row across every key, so each dated key is aged against its own
    budget (`config.EPA_RFS_KEY_MAX_AGE_DAYS`) here and dropped when stale;
    the LAYER_MIN_KEYS floor turns the gap into a failed run.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import date
from io import BytesIO
from typing import Any

import pandas as pd
import requests

from config import (
    EPA_RFS_ATTRIBUTION,
    EPA_RFS_D_CODES,
    EPA_RFS_GENERATION_LANDING_URL,
    EPA_RFS_KEY_MAX_AGE_DAYS,
    EPA_RFS_PRICE_BOUNDS_USD_PER_RIN,
    EPA_RFS_PRICE_MEASURE,
    EPA_RFS_QLIK_APP_ID,
    EPA_RFS_QLIK_BASE,
    EPA_RFS_QLIK_RVO_COLUMNS,
    EPA_RFS_RVO_REFERENCE,
    EPA_RFS_RVO_RULE,
    EPA_RFS_RVO_TOLERANCE_RINS,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
)
from fetchers._backoff import retry_sleep

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; MirrorMarket/1.0)"}

GENERATION_COLUMNS = ("Date", "rins", "batch_volume_gal", "preliminary")
PRICE_COLUMNS = ("Date", "price_usd_per_rin", "rins_in_average")
RVO_COLUMNS = (
    "compliance_year", "category", "base_rins", "sre_reallocation_rins",
    "total_rins", "epa_reported_rins", "unit", "rule_status", "rule_citation",
    "rule_published", "rule_effective",
)

# The header row of rindata_*.csv, exactly as EPA writes it (2015-2026).
_GENERATION_HEADER = (
    "FUEL_CODE", "RIN_YEAR", "Production Month", "RIN_QUANTITY", "BATCH_VOLUME",
)
_RINDATA_LINK = re.compile(
    r'href="(?P<url>[^"]*/rindata_(?P<mon>[a-z]+)(?P<year>\d{4})\.csv)"',
    re.IGNORECASE,
)
_MONTHS = {
    m: i for i, m in enumerate(
        ("jan", "feb", "mar", "apr", "may", "jun",
         "jul", "aug", "sep", "oct", "nov", "dec"), start=1,
    )
}

# Qlik's dual-valued dates are day serials from this epoch.
_QLIK_EPOCH = pd.Timestamp("1899-12-30")
_PRICE_WEEK_FIELD = "Price_Transfer Date by week"
_PRICE_CODE_FIELD = "Price_FUEL_CD"
_RVO_TABLE = "RVO_T2"
_RVO_ROW = "Projected Volume Obligation"
# The engine caps one data page at 10,000 cells.
_QLIK_PAGE_CELLS = 10_000


class EpaRfsError(RuntimeError):
    """The upstream answered, but not with something we can trust."""


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
def _download(url: str, label: str) -> bytes | None:
    """GET bytes with the project retry policy. None = never answered."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Requesting %s (attempt %d) ...", label, attempt)
            resp = requests.get(url, headers=_HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.content
            logger.warning("HTTP %d for %s", resp.status_code, label)
        except requests.RequestException as exc:
            logger.warning(
                "Attempt %d/%d failed for %s: %s", attempt, MAX_RETRIES, label, exc
            )
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)
    logger.error("All %d attempts failed for %s", MAX_RETRIES, label)
    return None


# ---------------------------------------------------------------------------
# rin_generation
# ---------------------------------------------------------------------------
def _resolve_generation_url(landing_html: str) -> tuple[str, pd.Timestamp]:
    """The newest `rindata_<mon><yyyy>.csv` linked from the landing page.

    Returns the URL and the data month its filename names. Raises when no link
    parses — a landing page with no file is a changed page, never a quiet
    month.
    """
    best: tuple[pd.Timestamp, str] | None = None
    for match in _RINDATA_LINK.finditer(landing_html):
        month = _MONTHS.get(match["mon"][:3].lower())
        if month is None:
            raise EpaRfsError(f"unreadable month token in {match['url']!r}")
        stamp = pd.Timestamp(year=int(match["year"]), month=month, day=1)
        url = match["url"]
        if url.startswith("/"):
            url = "https://www.epa.gov" + url
        if best is None or stamp > best[0]:
            best = (stamp, url)
    if best is None:
        raise EpaRfsError("no rindata_*.csv link on the EPA landing page")
    return best[1], best[0]


def _parse_generation(raw: bytes, file_month: pd.Timestamp) -> dict[str, pd.DataFrame]:
    """rindata CSV → one frame per D-code, the newest month flagged preliminary."""
    df = pd.read_csv(BytesIO(raw))
    if tuple(df.columns) != _GENERATION_HEADER:
        raise EpaRfsError(f"rindata header changed: {list(df.columns)}")

    for col in _GENERATION_HEADER:
        df[col] = pd.to_numeric(df[col], errors="raise")
    if df[["RIN_QUANTITY", "BATCH_VOLUME"]].lt(0).any().any():
        raise EpaRfsError("negative RIN or volume quantity in rindata")

    df["Date"] = pd.to_datetime(
        {"year": df["RIN_YEAR"], "month": df["Production Month"], "day": 1},
        errors="raise",
    )
    newest = df["Date"].max()
    if newest != file_month:
        # The filename is how EPA tells us which month this file closes. A
        # file whose rows end elsewhere is either the wrong file or a changed
        # layout; either way "preliminary" would land on the wrong month.
        raise EpaRfsError(
            f"rindata file names {file_month:%Y-%m} but its rows end {newest:%Y-%m}"
        )
    if df.duplicated(["FUEL_CODE", "Date"]).any():
        raise EpaRfsError("rindata carries duplicate (D-code, month) rows")

    out: dict[str, pd.DataFrame] = {}
    for code, part in df.groupby("FUEL_CODE"):
        d_code = f"D{int(str(code))}"
        if d_code not in EPA_RFS_D_CODES:
            raise EpaRfsError(f"unknown D-code {d_code} in rindata")
        frame = pd.DataFrame({
            "Date": part["Date"],
            "rins": part["RIN_QUANTITY"].astype(float),
            "batch_volume_gal": part["BATCH_VOLUME"].astype(float),
            "preliminary": (part["Date"] == newest).astype(int),
        })
        out[d_code] = frame.sort_values("Date").reset_index(drop=True)
    return out


def fetch_rin_generation() -> pd.DataFrame:
    """Every D-code's monthly generation as one long frame (`d_code` column).

    Empty means the page, the link or the file could not be trusted — the
    reason is logged at ERROR. Never a partial file.
    """
    page = _download(EPA_RFS_GENERATION_LANDING_URL, "EPA RIN generation landing page")
    if page is None:
        return pd.DataFrame()
    try:
        url, file_month = _resolve_generation_url(page.decode("utf-8", "replace"))
        raw = _download(url, f"EPA rindata {file_month:%Y-%m}")
        if raw is None:
            return pd.DataFrame()
        per_code = _parse_generation(raw, file_month)
    except (EpaRfsError, ValueError) as exc:
        logger.error("EPA RIN generation rejected: %s", exc)
        return pd.DataFrame()

    frames = [f.assign(d_code=code) for code, f in per_code.items()]
    out = pd.concat(frames, ignore_index=True)
    logger.info(
        "EPA RIN generation: %d rows, %d D-codes, %s → %s (latest preliminary)",
        len(out), len(per_code), out["Date"].min().strftime("%Y-%m"),
        file_month.strftime("%Y-%m"),
    )
    return out


# ---------------------------------------------------------------------------
# Qlik engine (rin_prices + rvo)
# ---------------------------------------------------------------------------
QlikCall = Callable[[int, str, Any], dict]


@contextmanager
def _qlik_session() -> Iterator[tuple[QlikCall, int]]:
    """Open EPA's public RFS app and yield ``(call, doc_handle)``.

    The page GET issues the `X-Qlik-Session-public` cookie the websocket
    needs; without it the engine refuses the upgrade.
    """
    from websockets.sync.client import connect

    session = requests.Session()
    session.headers.update(_HEADERS)
    resp = session.get(
        f"{EPA_RFS_QLIK_BASE}/single/?appid={EPA_RFS_QLIK_APP_ID}",
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    cookie = "; ".join(f"{k}={v}" for k, v in session.cookies.items())
    if not cookie:
        raise EpaRfsError("EPA Qlik page issued no session cookie")

    ws_url = EPA_RFS_QLIK_BASE.replace("https://", "wss://") + f"/app/{EPA_RFS_QLIK_APP_ID}"
    with connect(
        ws_url,
        additional_headers={"Cookie": cookie, **_HEADERS},
        open_timeout=REQUEST_TIMEOUT,
        max_size=2**26,
    ) as ws:
        counter = 0

        def call(handle: int, method: str, params: Any) -> dict:
            nonlocal counter
            counter += 1
            ws.send(json.dumps({
                "jsonrpc": "2.0", "id": counter, "handle": handle,
                "method": method, "params": params,
            }))
            while True:
                msg = json.loads(ws.recv(timeout=REQUEST_TIMEOUT))
                if msg.get("id") != counter:
                    continue  # engine notifications (OnConnected etc.)
                if "error" in msg:
                    raise EpaRfsError(f"Qlik {method} failed: {msg['error']}")
                return msg["result"]

        doc = call(-1, "OpenDoc", [EPA_RFS_QLIK_APP_ID])["qReturn"]["qHandle"]
        yield call, doc


def _price_cube_rows(call: QlikCall, doc: int) -> list[list[dict]]:
    """Every (transfer week × D-code) cell of EPA's price measure."""
    cube = {
        "qInfo": {"qType": "mirror-market-rin-price"},
        "qHyperCubeDef": {
            "qDimensions": [
                {"qDef": {"qFieldDefs": [_PRICE_WEEK_FIELD]}},
                {"qDef": {"qFieldDefs": [_PRICE_CODE_FIELD]}},
            ],
            "qMeasures": [
                {"qDef": {"qDef": EPA_RFS_PRICE_MEASURE}},
                {"qDef": {"qDef": 'Sum({$<[Price_FUEL_CD]={"3","4","5","6"}>}Price_TOTAL_RINS)'}},
            ],
            "qInitialDataFetch": [],
        },
    }
    handle = call(doc, "CreateSessionObject", [cube])["qReturn"]["qHandle"]
    layout = call(handle, "GetLayout", [])["qLayout"]
    total = layout["qHyperCube"]["qSize"]["qcy"]
    width = 4
    page = _QLIK_PAGE_CELLS // width
    rows: list[list[dict]] = []
    for top in range(0, total, page):
        result = call(handle, "GetHyperCubeData", [
            "/qHyperCubeDef",
            [{"qTop": top, "qLeft": 0, "qWidth": width, "qHeight": min(page, total - top)}],
        ])
        rows.extend(result["qDataPages"][0]["qMatrix"])
    if len(rows) != total:
        raise EpaRfsError(f"Qlik price cube returned {len(rows)} of {total} rows")
    return rows


def _parse_price_rows(rows: list[list[dict]]) -> dict[str, pd.DataFrame]:
    """Cube cells → one frame per D-code. A week with no trade has no row."""
    records = []
    lo, hi = EPA_RFS_PRICE_BOUNDS_USD_PER_RIN
    for week, code, price, volume in rows:
        serial = week.get("qNum")
        if not isinstance(serial, (int, float)) or serial != serial:  # NaN check
            raise EpaRfsError(f"undatable price week {week.get('qText')!r}")
        week_start = _QLIK_EPOCH + pd.Timedelta(days=int(serial))
        d_code = f"D{code.get('qText')}"
        if d_code not in EPA_RFS_D_CODES:
            raise EpaRfsError(f"unknown D-code {d_code} in Qlik price cube")
        value = price.get("qNum")
        if not isinstance(value, (int, float)) or value != value:
            continue  # no qualifying trade that week for that code
        if not lo < value <= hi:
            raise EpaRfsError(
                f"{d_code} price {value} USD/RIN for week {week_start:%Y-%m-%d} "
                f"is outside {lo}-{hi} — a changed field or unit, not a trade"
            )
        rins = volume.get("qNum")
        # The volume is context, not the price: an unreadable one is NULL,
        # never a zero (invariant 2).
        rins_in_average = float(rins) if isinstance(rins, (int, float)) else None
        records.append((d_code, week_start, float(value), rins_in_average))

    if not records:
        raise EpaRfsError("Qlik price cube held no priced week")
    df = pd.DataFrame(records, columns=["d_code", *PRICE_COLUMNS])
    if df.duplicated(["d_code", "Date"]).any():
        raise EpaRfsError("Qlik price cube repeated a (D-code, week) cell")
    return {
        str(code): part.drop(columns="d_code").sort_values("Date").reset_index(drop=True)
        for code, part in df.groupby("d_code")
    }


def _rvo_table_rows(call: QlikCall, doc: int) -> pd.DataFrame:
    """EPA's RVO_T2 table as a frame keyed by its own field names."""
    tables = call(doc, "GetTablesAndKeys", [
        {"qcx": 1000, "qcy": 1000}, {"qcx": 0, "qcy": 0}, 30, True, False,
    ])["qtr"]
    spec = next((t for t in tables if t["qName"] == _RVO_TABLE), None)
    if spec is None:
        raise EpaRfsError(f"Qlik app no longer has table {_RVO_TABLE}")
    fields = [f["qName"] for f in spec["qFields"]]
    data = call(doc, "GetTableData", [0, spec["qNoOfRows"], False, _RVO_TABLE])["qData"]
    rows = [[cell.get("qText") for cell in row["qValue"]] for row in data]
    return pd.DataFrame(rows, columns=fields)


def _build_rvo(epa_rows: pd.DataFrame) -> pd.DataFrame:
    """The reference RVOs, each confirmed against EPA's reported obligation.

    Raises when EPA's app disagrees with a reference total or lacks a
    reference year: the stored obligation would be a superseded one.
    """
    needed = {"RVO_T2_parameter", "RVO_T2_year", *EPA_RFS_QLIK_RVO_COLUMNS}
    if not needed <= set(epa_rows.columns):
        raise EpaRfsError(f"{_RVO_TABLE} fields changed: {list(epa_rows.columns)}")
    projected = epa_rows[epa_rows["RVO_T2_parameter"] == _RVO_ROW]

    records: list[dict[str, Any]] = []
    for year, categories in EPA_RFS_RVO_REFERENCE.items():
        match = projected[projected["RVO_T2_year"] == str(year)]
        if len(match) != 1:
            raise EpaRfsError(f"EPA reports no single {_RVO_ROW!r} row for {year}")
        row = match.iloc[0]
        for column, category in EPA_RFS_QLIK_RVO_COLUMNS.items():
            base, realloc, total = categories[category]
            reported = pd.to_numeric(row[column], errors="coerce")
            if pd.isna(reported) or abs(reported - total) > EPA_RFS_RVO_TOLERANCE_RINS:
                raise EpaRfsError(
                    f"{year} {category}: EPA now reports {row[column]!r} RINs, "
                    f"reference ({EPA_RFS_RVO_RULE['citation']}) says {total:,.0f} — "
                    "a later rule may be final; update EPA_RFS_RVO_REFERENCE "
                    "with its citation"
                )
            records.append({
                "compliance_year": year,
                "category": category,
                "base_rins": base,
                "sre_reallocation_rins": realloc,
                "total_rins": total,
                "epa_reported_rins": float(reported),
                "unit": "RINs",
                "rule_status": EPA_RFS_RVO_RULE["status"],
                "rule_citation": EPA_RFS_RVO_RULE["citation"],
                "rule_published": EPA_RFS_RVO_RULE["published"],
                "rule_effective": EPA_RFS_RVO_RULE["effective"],
            })

    later = sorted(
        int(y) for y in projected["RVO_T2_year"]
        if str(y).isdigit() and int(y) > max(EPA_RFS_RVO_REFERENCE)
    )
    if later:
        logger.warning(
            "EPA reports obligations for %s, beyond the reference table — enter "
            "them only once their rule is final", later,
        )
    return pd.DataFrame(records, columns=list(RVO_COLUMNS))


def fetch_qlik_keys() -> dict[str, pd.DataFrame]:
    """rin_prices + rvo from one engine session, retried as a unit.

    Each key fails on its own: a changed RVO table must not take the prices
    down with it, and the reverse.
    """
    out: dict[str, pd.DataFrame] = {}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with _qlik_session() as (call, doc):
                try:
                    per_code = _parse_price_rows(_price_cube_rows(call, doc))
                    out["rin_prices"] = pd.concat(
                        [f.assign(d_code=c) for c, f in per_code.items()],
                        ignore_index=True,
                    )
                except EpaRfsError as exc:
                    logger.error("EPA RIN prices rejected: %s", exc)
                try:
                    out["rvo"] = _build_rvo(_rvo_table_rows(call, doc))
                except EpaRfsError as exc:
                    logger.error("EPA RVO cross-check failed: %s", exc)
            return out
        except Exception as exc:  # transport: requests, websockets, timeouts
            logger.warning(
                "Attempt %d/%d failed for EPA Qlik app: %s", attempt, MAX_RETRIES, exc
            )
            out = {}
            if attempt < MAX_RETRIES:
                retry_sleep(attempt)
    logger.error("All %d attempts failed for EPA Qlik app", MAX_RETRIES)
    return out


# ---------------------------------------------------------------------------
# Layer entry point
# ---------------------------------------------------------------------------
def _within_budget(key: str, frame: pd.DataFrame, today: date | None = None) -> bool:
    """True when a dated key's newest row is inside its own age budget."""
    latest = pd.Timestamp(frame["Date"].max()).normalize()
    now = pd.Timestamp(today or date.today())
    age = (now - latest).days
    budget = EPA_RFS_KEY_MAX_AGE_DAYS[key]
    if age > budget:
        logger.error(
            "EPA %s ends %s (%d days ago, budget %d) — EPA has stopped "
            "refreshing it or the link resolved to an old file; dropping it",
            key, latest.strftime("%Y-%m-%d"), age, budget,
        )
        return False
    return True


def fetch_epa_rfs() -> dict[str, pd.DataFrame]:
    """Layer 29 — every key that fetched, parsed and is inside its budget.

    A key that failed is absent rather than empty, so `LAYER_MIN_KEYS`
    sees the outage. Long frames: generation and prices carry `d_code`.
    """
    results: dict[str, pd.DataFrame] = {}
    generation = fetch_rin_generation()
    if not generation.empty:
        results["rin_generation"] = generation
    results.update(fetch_qlik_keys())

    for key in list(EPA_RFS_KEY_MAX_AGE_DAYS):
        if key in results and not _within_budget(key, results[key]):
            del results[key]
    return {k: v.assign(attribution=EPA_RFS_ATTRIBUTION) for k, v in results.items()}
