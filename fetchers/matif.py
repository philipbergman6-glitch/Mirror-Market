"""
Layer 37 — Euronext MATIF rapeseed (ECO) delayed trades, per contract month.
PRIVATE DESK LAYER: nothing here is published (config.MATIF_PUBLISH is False
and enforced by tests/test_matif_private_gate.py).

Source: Euronext's MiFIR Article 13 delayed post-trade file for Paris
commodity derivatives —
    https://marketdata.euronext.com/data-reporting-service/trades-file/
        download/COMMODITY_DERIVATIVES/{PREVIOUS_TRADING_DAY|CURRENT_TRADING_DAY}/PAR
a zip holding one CSV of every XMAT trade of that day, ~15 minutes behind the
clock, kept "at least 24 hours". No key, no login. Euronext's own "Terms and
Conditions for Delayed Trade Data" (clause 1): the right to Use the Data is
free of charge and unrestricted except in the event of distribution — which
is exactly why the rows never reach docs/, data/history/ or the MCP image.
Research: research/2026-11-05-matif-rapeseed-source.md.

What this is NOT — and the number must never be called it:
    - Not a settlement. The daily settlement price (DSP) renders only on
      live.euronext.com through an XHR route the Euronext Terms of Use close
      to robots and systematic retrieval. It was located and not called.
      Every row here is `board_last_traded` (pricing.semantics), the SAFEX
      precedent (Layer 18).
    - Not the whole market. Off-book prints (MmtMarketMechanism != 1) are
      counted but never priced; strategy/spread fills reported under a bare
      numeric id (no ISIN) are left unresolved.

Three traps this module exists to survive:

1.  **The file names instruments by ISIN only.** No product code, no month.
    Each ISIN is resolved through ESMA's public FIRDS register
    (`gnr_full_name` "Rapeseed / Colza FUTR PHYS JAN 2027",
    `drv_expiry_date`, `drvcmd_further_sub_product` RPSD, CFI FCAPSX), in
    batches of MATIF_FIRDS_BATCH — a broad "every RPSD on XMAT" query 504s
    at the gateway. Resolved ISINs are cached in the stored rows, so a
    FIRDS outage blocks only never-seen ISINs. FIRDS names the *expiry*
    month; the delivery month is the one after (contract spec: last trading
    day is the last business day of the month preceding delivery), and an
    expiry outside Jan/Apr/Jul/Oct is a shape change and raises.

2.  **TAS and options share the product.** "Milling Wheat TAS FUTR PHYS" is
    a Trading-At-Settlement instrument priced as a differential; options are
    CFI O*. Both are excluded by name/CFI, never by price level.

3.  **A run before 20:15 Paris holds an unfinished session.** The session
    labelled D closes at 20:15 Europe/Paris on D (contract spec; the 11-04
    file's last trade is 19:14:59Z = 20:14:59 CET). The same SessionRule the
    yfinance layers use drops it until it is finished — a gap over a wrong
    number (invariant 11).
"""

from __future__ import annotations

import io
import logging
import os
import re
import zipfile
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime

import pandas as pd
import requests

from config import (
    MATIF_CONTRACT,
    MATIF_DELIVERY_MONTHS,
    MATIF_FETCH_ENV,
    MATIF_FIRDS_BATCH,
    MATIF_FIRDS_URL,
    MATIF_PRODUCT,
    MATIF_QUOTE_KIND,
    MATIF_SESSION_CLOSE_LOCAL,
    MATIF_SESSION_TIMEZONE,
    MATIF_SESSION_WEEKDAYS,
    MATIF_TRADES_SELECTIONS,
    MATIF_TRADES_URL,
    MATIF_VENUE_MIC,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
)
from fetchers._backoff import retry_sleep
from fetchers._settlement import SessionRule, drop_unsettled_session
from pipeline.results import FetchResult, ScraperShapeError

logger = logging.getLogger(__name__)

# The session labelled D is finished at 20:15 Paris on D, Monday-Friday.
MATIF_SESSION = SessionRule(
    name="Euronext Paris commodities session end",
    timezone=MATIF_SESSION_TIMEZONE,
    close_local=MATIF_SESSION_CLOSE_LOCAL,
    session_weekdays=frozenset(MATIF_SESSION_WEEKDAYS),
)

_REQUIRED_COLUMNS: tuple[str, ...] = (
    "TradingDateTime", "MifidInstrumentID", "MifidPrice", "MifidQuantity",
    "MifidPriceNotation", "MifidCurrency", "MmtMarketMechanism",
    "MmtModificationIndicator", "Venue", "TradeUniqueIdentifier",
)
_ON_BOOK = "1"          # MMT market mechanism: central limit order book
_UNMODIFIED = "-"       # MMT modification indicator: neither CANC nor AMND
_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
_TAS_RE = re.compile(r"\bTAS\b")
_HEADERS = {"User-Agent": "Mozilla/5.0 (Mirror-Market desk; private delayed-data use)"}

OUTPUT_COLUMNS: tuple[str, ...] = (
    "Date", "contract_month", "isin", "firds_name", "expiry_date",
    "open", "high", "low", "last", "vwap", "lots", "trade_count", "last_trade_at",
    "off_book_trades", "off_book_lots", "cancelled_trades",
)


class MatifTransportError(RuntimeError):
    """Euronext or ESMA did not answer — the layer never learned anything."""


@dataclass(frozen=True)
class Instrument:
    """One FIRDS record, reduced to what the layer needs."""

    isin: str
    name: str
    cfi: str
    product: str
    expiry_date: date | None

    @property
    def is_eco_future(self) -> bool:
        return (
            self.product == MATIF_PRODUCT
            and self.cfi.startswith("F")
            and not _TAS_RE.search(self.name)
        )

    @property
    def contract_month(self) -> str:
        if self.expiry_date is None:
            raise ScraperShapeError(f"{self.isin} ({self.name}): FIRDS record carries no expiry date")
        return delivery_month(self.expiry_date, isin=self.isin)


def is_configured() -> bool:
    """The desk opts in by setting MATIF_FETCH_ENV; unset = never ran, no freshness row."""
    return bool(os.getenv(MATIF_FETCH_ENV))


def delivery_month(expiry: date, *, isin: str = "") -> str:
    """ECO delivery month (``YYYY-MM``) from the FIRDS expiry date: the month after.

    The spec's last trading day is the last business day of the month
    preceding delivery, so expiry 2027-01-29 is the Feb 2027 contract. An
    expiry month outside Jan/Apr/Jul/Oct means the listing changed shape.
    """
    year, month = (expiry.year + 1, 1) if expiry.month == 12 else (expiry.year, expiry.month + 1)
    if month not in MATIF_DELIVERY_MONTHS:
        raise ScraperShapeError(
            f"{isin or MATIF_CONTRACT}: expiry {expiry.isoformat()} implies delivery month {month}, "
            f"not one of {MATIF_DELIVERY_MONTHS} — the ECO listing changed shape"
        )
    return f"{year:04d}-{month:02d}"


# --------------------------------------------------------------------------
# Transport
# --------------------------------------------------------------------------


def download_trades(selection: str) -> bytes | None:
    """The zip for one time selection; ``None`` when Euronext answers 404 (no file)."""
    url = MATIF_TRADES_URL.format(selection=selection)
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 404:
                # The page's own JS treats 404 as "empty file": asked, nothing
                # to give (a weekend, a holiday, pre-open). Not a failure.
                logger.info("MATIF %s: no delayed trade file (404)", selection)
                return None
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as exc:
            last_error = exc
            logger.warning("Attempt %d/%d failed for MATIF %s: %s", attempt, MAX_RETRIES, selection, exc)
            if attempt < MAX_RETRIES:
                retry_sleep(attempt)
    raise MatifTransportError(f"{selection}: {last_error}")


def parse_trades(raw: bytes) -> pd.DataFrame:
    """The zip's one CSV as a frame of typed trades; a changed shape raises."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile as exc:
        raise ScraperShapeError(f"MATIF trade file is not a zip: {exc}") from exc
    members = archive.namelist()
    if len(members) != 1:
        raise ScraperShapeError(f"MATIF trade zip holds {len(members)} members, expected 1: {members}")
    text = archive.read(members[0]).decode("utf-8", errors="replace")
    lines = text.splitlines()
    if not lines:
        raise ScraperShapeError("MATIF trade CSV is empty")
    # Line 1 is Euronext's copyright/terms preamble; the header follows it.
    # Anchor on the header text itself, never on a line number.
    if "TradingDateTime" not in lines[0]:
        if len(lines) < 2 or "TradingDateTime" not in lines[1]:
            raise ScraperShapeError(f"MATIF trade CSV header not found in the first two lines: {lines[0][:80]!r}")
        text = "\n".join(lines[1:])
    try:
        # index_col=False: a ragged row must raise, not make pandas promote
        # column 1 to the index and shift every price into the next column.
        df = pd.read_csv(io.StringIO(text), dtype=str, keep_default_na=False, index_col=False)
    except pd.errors.ParserError as exc:
        raise ScraperShapeError(f"MATIF trade CSV does not parse: {exc}") from exc
    missing = [c for c in _REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ScraperShapeError(f"MATIF trade CSV lacks column(s) {missing}; has {list(df.columns)}")
    if df.empty:
        return df
    out = df.copy()
    out["ts"] = pd.to_datetime(out["TradingDateTime"], utc=True, errors="coerce")
    out["px"] = pd.to_numeric(out["MifidPrice"], errors="coerce")
    out["qty"] = pd.to_numeric(out["MifidQuantity"], errors="coerce")
    bad = out["ts"].isna() | out["px"].isna() | out["qty"].isna()
    if bad.any():
        raise ScraperShapeError(f"MATIF trade CSV: {int(bad.sum())} row(s) with an unparseable time, price or quantity")
    off_venue = out["Venue"] != MATIF_VENUE_MIC
    if off_venue.any():
        raise ScraperShapeError(f"MATIF trade CSV carries {int(off_venue.sum())} row(s) not on {MATIF_VENUE_MIC}")
    not_eur = (out["MifidCurrency"] != "EUR") | (out["MifidPriceNotation"] != "MONE")
    if not_eur.any():
        raise ScraperShapeError(f"MATIF trade CSV: {int(not_eur.sum())} row(s) not priced as EUR money")
    return out


# --------------------------------------------------------------------------
# Instrument resolution (ESMA FIRDS)
# --------------------------------------------------------------------------


def _firds_query(isins: list[str]) -> list[dict]:
    query = " OR ".join(f"isin:{isin}" for isin in isins)
    params = {
        "q": query, "fq": f"mic:{MATIF_VENUE_MIC}", "wt": "json", "rows": str(len(isins) * 4),
        "fl": "isin,gnr_full_name,gnr_cfi_code,drv_expiry_date,drvcmd_further_sub_product,latest_received_flag",
    }
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(MATIF_FIRDS_URL, params=params, headers=_HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            body = resp.json()
            docs = body["response"]["docs"]
            if not isinstance(docs, list):
                raise ScraperShapeError("FIRDS response.docs is not a list")
            return docs
        except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
            last_error = exc
            logger.warning("Attempt %d/%d failed for FIRDS lookup: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                retry_sleep(attempt)
    raise MatifTransportError(f"FIRDS: {last_error}")


def _instrument_from_doc(doc: Mapping[str, object]) -> Instrument:
    try:
        isin = str(doc["isin"])
        name = str(doc["gnr_full_name"])
        cfi = str(doc["gnr_cfi_code"])
    except KeyError as exc:
        raise ScraperShapeError(f"FIRDS record lacks {exc}: {dict(doc)}") from exc
    raw_expiry = doc.get("drv_expiry_date")
    expiry = date.fromisoformat(str(raw_expiry)[:10]) if raw_expiry else None
    product = str(doc.get("drvcmd_further_sub_product") or "")
    return Instrument(isin=isin, name=name, cfi=cfi, product=product, expiry_date=expiry)


def resolve_instruments(
    ids: Iterable[str], known: Mapping[str, Instrument] | None = None,
) -> dict[str, Instrument]:
    """ISIN → Instrument for every ISIN-shaped id, FIRDS for the ones ``known`` lacks.

    Non-ISIN ids (numeric strategy ids) are skipped and counted. An ISIN
    FIRDS does not know stays unresolved — and is never priced.
    """
    resolved: dict[str, Instrument] = dict(known or {})
    wanted = sorted({i for i in ids if _ISIN_RE.match(i)} - set(resolved))
    skipped = sum(1 for i in set(ids) if not _ISIN_RE.match(i))
    if skipped:
        logger.info("MATIF: %d non-ISIN instrument id(s) left unresolved (strategies/spreads)", skipped)
    for start in range(0, len(wanted), MATIF_FIRDS_BATCH):
        batch = wanted[start:start + MATIF_FIRDS_BATCH]
        by_isin: dict[str, list[Mapping[str, object]]] = {}
        for doc in _firds_query(batch):
            by_isin.setdefault(str(doc.get("isin")), []).append(doc)
        for isin, docs in by_isin.items():
            current = [d for d in docs if str(d.get("latest_received_flag")) == "1"] or docs
            resolved[isin] = _instrument_from_doc(current[0])
        unknown = [i for i in batch if i not in by_isin]
        if unknown:
            logger.warning("MATIF: FIRDS has no record for %d ISIN(s): %s", len(unknown), unknown)
    return resolved


# --------------------------------------------------------------------------
# Session rows
# --------------------------------------------------------------------------


def session_rows(
    trades: pd.DataFrame,
    instruments: Mapping[str, Instrument],
    *,
    now: datetime | None = None,
) -> pd.DataFrame:
    """One row per finished (session date, delivery month) of ECO futures."""
    if trades.empty:
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))
    eco = {isin: inst for isin, inst in instruments.items() if inst.is_eco_future}
    df = trades[trades["MifidInstrumentID"].isin(eco)].copy()
    if df.empty:
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))
    df["Date"] = df["ts"].dt.tz_convert(MATIF_SESSION_TIMEZONE).dt.date
    df["contract_month"] = df["MifidInstrumentID"].map(lambda i: eco[i].contract_month)

    # A cancellation or amendment names the trade id; drop every row under
    # that id rather than guess which print survived, and count it.
    modified_ids = set(df.loc[df["MmtModificationIndicator"] != _UNMODIFIED, "TradeUniqueIdentifier"])
    modified = df["TradeUniqueIdentifier"].isin(modified_ids)
    cancelled = (
        df[modified].groupby(["Date", "contract_month"])["TradeUniqueIdentifier"].nunique()
        if modified.any() else pd.Series(dtype="int64")
    )
    df = df[~modified]
    on_book = df[df["MmtMarketMechanism"] == _ON_BOOK].sort_values("ts")
    off_book = df[df["MmtMarketMechanism"] != _ON_BOOK]

    rows: list[dict] = []
    for (session, month), grp in on_book.groupby(["Date", "contract_month"], sort=True):
        inst = eco[str(grp["MifidInstrumentID"].iloc[0])]
        off = off_book[(off_book["Date"] == session) & (off_book["contract_month"] == month)]
        lots = float(grp["qty"].sum())
        rows.append({
            "Date": pd.Timestamp(str(session)),
            "contract_month": month,
            "isin": inst.isin,
            "firds_name": inst.name,
            "expiry_date": inst.expiry_date.isoformat() if inst.expiry_date else None,
            "open": float(grp["px"].iloc[0]),
            "high": float(grp["px"].max()),
            "low": float(grp["px"].min()),
            "last": float(grp["px"].iloc[-1]),
            "vwap": float((grp["px"] * grp["qty"]).sum() / lots) if lots else None,
            "lots": lots,
            "trade_count": int(len(grp)),
            "last_trade_at": grp["ts"].iloc[-1].strftime("%Y-%m-%dT%H:%M:%SZ"),
            "off_book_trades": int(len(off)),
            "off_book_lots": float(off["qty"].sum()) if len(off) else 0.0,
            "cancelled_trades": int(cancelled.get((session, month), 0)) if len(cancelled) else 0,
        })
    out = pd.DataFrame(rows, columns=list(OUTPUT_COLUMNS))
    if out.empty:
        return out
    guarded = drop_unsettled_session(out.set_index("Date"), label=MATIF_CONTRACT, now=now, rule=MATIF_SESSION)
    return guarded.reset_index().sort_values(["Date", "contract_month"]).reset_index(drop=True)


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------


def fetch_matif_trades(
    *,
    known: Mapping[str, Instrument] | None = None,
    now: datetime | None = None,
) -> FetchResult:
    """Both delayed files → ECO session rows, graded by what actually answered.

    ``known`` is the ISIN map already stored (pipeline.query.read_matif_instruments),
    so FIRDS is asked only about new ISINs. Returns ``{MATIF_CONTRACT: frame}``.
    """
    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    for selection in MATIF_TRADES_SELECTIONS:
        try:
            raw = download_trades(selection)
        except MatifTransportError as exc:
            errors.append(str(exc))
            continue
        if raw is None:
            continue
        try:
            frames.append(parse_trades(raw))
        except ScraperShapeError as exc:
            errors.append(f"{selection}: {exc}")
    if not frames:
        if errors:
            return FetchResult.failed("; ".join(errors))
        return FetchResult.empty("no delayed trade file for either selection")

    trades = pd.concat(frames, ignore_index=True).drop_duplicates(
        subset=["TradeUniqueIdentifier", "TradingDateTime", "MifidInstrumentID"]
    )
    try:
        instruments = resolve_instruments(trades["MifidInstrumentID"].unique().tolist(), known)
        rows = session_rows(trades, instruments, now=now)
    except MatifTransportError as exc:
        # Unlabelled trades cannot be stored under a contract month, and
        # storing them under a guess would be invention. Nothing is saved.
        return FetchResult.failed(f"instrument map unavailable: {exc}")

    logger.info(
        "MATIF %s: %d trades in %d file(s) → %d session row(s)%s",
        MATIF_CONTRACT, len(trades), len(frames), len(rows), f" ({'; '.join(errors)})" if errors else "",
    )
    if rows.empty:
        if errors:
            return FetchResult.failed("; ".join(errors))
        return FetchResult.empty(f"no finished {MATIF_CONTRACT} on-book session in the delayed files")
    rows = rows.assign(currency="EUR", unit="EUR/t", quote_kind=MATIF_QUOTE_KIND)
    if errors:
        return FetchResult.partial({MATIF_CONTRACT: rows}, "; ".join(errors))
    return FetchResult.ok({MATIF_CONTRACT: rows})
