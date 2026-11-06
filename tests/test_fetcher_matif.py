"""Layer 37 — Euronext MATIF rapeseed (ECO) delayed trades, private.

The trade CSV and FIRDS documents below are synthetic in Euronext's and
ESMA's own shapes (preamble line, header, MMT codes, FIRDS field names) with
invented prints. No Euronext data is committed to this public repository.
"""

from __future__ import annotations

import io
import json
import zipfile
from datetime import date, datetime, timezone

import pandas as pd
import pytest

import fetchers.matif as matif
from fetchers.matif import (
    Instrument,
    MatifTransportError,
    delivery_month,
    fetch_matif_trades,
    parse_trades,
    resolve_instruments,
    session_rows,
)
from pipeline.results import ScraperShapeError

PREAMBLE = (
    "(c) 2025 Euronext N.V. All Rights Reserved. Euronext is required to make trade data available "
    "pursuant to article 13 ... Terms and Conditions for Delayed Trade Data available at: "
    "https://www.euronext.com/delayed-data-terms-conditions"
)
HEADER = (
    "TradingDateTime,PublicationDateTime,MifidInstrumentID,MifidPrice,MifidQuantity,MifidPriceNotation,"
    "MifidCurrency,MifidQtyInMeasurementUnitNotation,MifidQuantityMeasurementUnit,MifidNotionalAmount,"
    "NotionalCurrency,MifidClearingFlag,MmtPostTradeDeferral,NumberOfTransactions,MmtMarketMechanism,"
    "MmtNegotiationIndicator,MmtModificationIndicator,MmtBenchMarkIndicator,MmtContributionToPrice,"
    "MmtAlgorithmicIndicator,MmtPublicationMode,Venue,ThirdCountryTradingVenueExecution,"
    "TradeUniqueIdentifier,MissingPrice,MmtContingentTransactionIndicator,VenueOfPublication"
)

FEB27 = "FREN27722858"   # Rapeseed FUTR expiry JAN 2027 → Feb 2027 delivery
MAY27 = "FREX00164092"   # expiry APR 2027 → May 2027
WHEAT = "FREN12499710"   # Milling Wheat FUTR — not ours
OPTION = "FREX00054111"  # Rapeseed OPTN
TAS = "FREX09999990"     # Rapeseed TAS FUTR — a differential, excluded
STRATEGY = "4224099884"  # bare numeric id, never resolved

FIRDS = {
    FEB27: {"isin": FEB27, "gnr_full_name": "Rapeseed / Colza FUTR PHYS JAN 2027", "gnr_cfi_code": "FCAPSX",
            "drv_expiry_date": "2027-01-29T00:00:00Z", "drvcmd_further_sub_product": "RPSD", "latest_received_flag": "1"},
    MAY27: {"isin": MAY27, "gnr_full_name": "Rapeseed / Colza FUTR PHYS APR 2027", "gnr_cfi_code": "FCAPSX",
            "drv_expiry_date": "2027-04-30T00:00:00Z", "drvcmd_further_sub_product": "RPSD", "latest_received_flag": "1"},
    WHEAT: {"isin": WHEAT, "gnr_full_name": "Milling Wheat / Ble FUTR PHYS DEC 2026", "gnr_cfi_code": "FCAPSX",
            "drv_expiry_date": "2026-12-10T00:00:00Z", "drvcmd_further_sub_product": "MWHT", "latest_received_flag": "1"},
    OPTION: {"isin": OPTION, "gnr_full_name": "Rapeseed / Colza OPTN PHYS AMER JAN 2027 570 CALL",
             "gnr_cfi_code": "OCAFPS", "drv_expiry_date": "2026-12-15T00:00:00Z",
             "drvcmd_further_sub_product": "RPSD", "latest_received_flag": "1"},
    TAS: {"isin": TAS, "gnr_full_name": "Rapeseed TAS FUTR PHYS JAN 2027", "gnr_cfi_code": "FCAPSX",
          "drv_expiry_date": "2027-01-29T00:00:00Z", "drvcmd_further_sub_product": "RPSD", "latest_received_flag": "1"},
}


def _trade(ts: str, isin: str, px: str, qty: str, *, mech: str = "1", mod: str = "-", tid: str | None = None,
           venue: str = "XMAT", ccy: str = "EUR", notation: str = "MONE") -> str:
    tid = tid or f"T{abs(hash((ts, isin, px, qty))) % 10**9}"
    return (f'"{ts}","{ts}","{isin}","{px}","{qty}","{notation}","{ccy}","","50.00000","0","EUR","true","-","",'
            f'"{mech}","-","{mod}","-","-","-","-","{venue}","","{tid}","","-","{venue}"')


def test_a_ragged_row_is_a_shape_change_not_a_silent_shift():
    # One field too many used to make pandas take column 1 as the index and
    # shift every value left — price became quantity. index_col=False stops it.
    ragged = _trade("2026-11-04T10:00:00Z", FEB27, "470", "1").replace('"true",', '"true","extra",')
    with pytest.raises(ScraperShapeError):
        parse_trades(_zip([ragged]))


def _zip(rows: list[str], *, preamble: bool = True, header: str = HEADER, members: int = 1) -> bytes:
    text = "\n".join(([PREAMBLE] if preamble else []) + [header] + rows) + "\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for i in range(members):
            z.writestr(f"Trades_CommodityDerivatives{i or ''}.csv", text)
    return buf.getvalue()


# Session 2026-11-04 (Wednesday), finished; trades in UTC (CET = UTC+1).
DAY = [
    _trade("2026-11-04T09:45:23.418973Z", FEB27, "470.00", "4"),
    _trade("2026-11-04T12:00:00.000000Z", FEB27, "474.50", "10"),
    _trade("2026-11-04T17:29:10.000000Z", FEB27, "472.25", "6"),
    _trade("2026-11-04T18:10:00.000000Z", FEB27, "490.00", "100", mech="4"),      # off-book block, never priced
    _trade("2026-11-04T19:14:59.000000Z", FEB27, "473.00", "2"),                 # last on-book print
    _trade("2026-11-04T10:00:00.000000Z", MAY27, "478.00", "1"),
    _trade("2026-11-04T10:05:00.000000Z", MAY27, "999.00", "1", tid="CANCME"),  # cancelled below
    _trade("2026-11-04T10:06:00.000000Z", MAY27, "999.00", "1", mod="CANC", tid="CANCME"),
    _trade("2026-11-04T11:00:00.000000Z", WHEAT, "223.00", "5"),
    _trade("2026-11-04T11:00:00.000000Z", OPTION, "12.50", "5"),
    _trade("2026-11-04T11:00:00.000000Z", TAS, "0.25", "5"),
    _trade("2026-11-04T11:00:00.000000Z", STRATEGY, "5.25", "5"),
]
AFTER_CLOSE = datetime(2026, 11, 4, 20, 0, tzinfo=timezone.utc)   # 21:00 Paris — session over
MID_SESSION = datetime(2026, 11, 4, 15, 0, tzinfo=timezone.utc)   # 16:00 Paris — still trading

INSTRUMENTS = {isin: matif._instrument_from_doc(doc) for isin, doc in FIRDS.items()}


# ── parsing ───────────────────────────────────────────────────────────────


def test_parse_skips_the_preamble_and_types_the_columns():
    df = parse_trades(_zip(DAY))
    assert len(df) == len(DAY)
    assert str(df["ts"].dt.tz) == "UTC"
    assert df["px"].iloc[0] == 470.0 and df["qty"].iloc[0] == 4.0


def test_parse_accepts_a_file_without_the_preamble():
    assert len(parse_trades(_zip(DAY, preamble=False))) == len(DAY)


@pytest.mark.parametrize("bad", [
    b"not a zip at all",
    _zip(DAY, members=2),
    _zip(DAY, header=HEADER.replace("MifidPrice,", "Price,")),
    _zip([_trade("2026-11-04T10:00:00Z", FEB27, "abc", "1")]),
    _zip([_trade("2026-11-04T10:00:00Z", FEB27, "470", "1", venue="XPAR")]),
    _zip([_trade("2026-11-04T10:00:00Z", FEB27, "470", "1", ccy="USD")]),
])
def test_parse_hard_fails_on_a_changed_shape(bad: bytes):
    with pytest.raises(ScraperShapeError):
        parse_trades(bad)


def test_parse_of_a_header_only_file_is_empty_not_an_error():
    assert parse_trades(_zip([])).empty


# ── instruments ───────────────────────────────────────────────────────────


@pytest.mark.parametrize("expiry, month", [
    (date(2027, 1, 29), "2027-02"), (date(2027, 4, 30), "2027-05"),
    (date(2027, 7, 30), "2027-08"), (date(2027, 10, 29), "2027-11"), (date(2026, 10, 30), "2026-11"),
])
def test_delivery_month_is_the_month_after_expiry(expiry: date, month: str):
    assert delivery_month(expiry) == month


def test_an_expiry_outside_the_eco_cycle_is_a_shape_change():
    with pytest.raises(ScraperShapeError, match="delivery month 12"):
        delivery_month(date(2026, 11, 30), isin="X")


def test_only_rapeseed_futures_that_are_not_tas_count():
    assert INSTRUMENTS[FEB27].is_eco_future and INSTRUMENTS[MAY27].is_eco_future
    assert not INSTRUMENTS[WHEAT].is_eco_future
    assert not INSTRUMENTS[OPTION].is_eco_future
    assert not INSTRUMENTS[TAS].is_eco_future


def _firds_stub(monkeypatch, docs_by_isin: dict, calls: list):
    class _Resp:
        status_code = 200

        def __init__(self, isins):
            self._docs = [d for i in isins for d in docs_by_isin.get(i, [])]

        def raise_for_status(self):
            pass

        def json(self):
            return {"response": {"docs": self._docs}}

    def fake_get(url, params=None, headers=None, timeout=None):
        assert url == matif.MATIF_FIRDS_URL
        isins = [part.split(":", 1)[1] for part in params["q"].split(" OR ")]
        calls.append(isins)
        return _Resp(isins)

    monkeypatch.setattr(matif.requests, "get", fake_get)


def test_resolve_asks_firds_only_for_unknown_isins_and_skips_numeric_ids(monkeypatch):
    calls: list = []
    _firds_stub(monkeypatch, {i: [d] for i, d in FIRDS.items()}, calls)
    known = {FEB27: INSTRUMENTS[FEB27]}
    out = resolve_instruments([FEB27, MAY27, WHEAT, STRATEGY], known)
    assert set(out) == {FEB27, MAY27, WHEAT}
    assert len(calls) == 1 and set(calls[0]) == {MAY27, WHEAT}


def test_resolve_prefers_the_latest_received_firds_record(monkeypatch):
    stale = dict(FIRDS[FEB27], gnr_full_name="Rapeseed / Colza FUTR PHYS JAN 2027 OLD", latest_received_flag="0")
    _firds_stub(monkeypatch, {FEB27: [stale, FIRDS[FEB27]]}, [])
    assert resolve_instruments([FEB27]) [FEB27].name == "Rapeseed / Colza FUTR PHYS JAN 2027"


def test_resolve_batches_at_the_configured_size(monkeypatch):
    calls: list = []
    _firds_stub(monkeypatch, {}, calls)
    monkeypatch.setattr(matif, "MATIF_FIRDS_BATCH", 2)
    resolve_instruments(["FR0000000001", "FR0000000002", "FR0000000003"])
    assert [len(c) for c in calls] == [2, 1]


def test_resolve_hard_fails_on_a_record_without_a_name(monkeypatch):
    _firds_stub(monkeypatch, {FEB27: [{"isin": FEB27, "gnr_cfi_code": "FCAPSX"}]}, [])
    with pytest.raises(ScraperShapeError, match="gnr_full_name"):
        resolve_instruments([FEB27])


# ── session rows ──────────────────────────────────────────────────────────


def test_session_rows_price_on_book_only_and_count_the_rest():
    rows = session_rows(parse_trades(_zip(DAY)), INSTRUMENTS, now=AFTER_CLOSE)
    assert list(rows["contract_month"]) == ["2027-02", "2027-05"]
    feb = rows.iloc[0]
    assert (feb["open"], feb["high"], feb["low"], feb["last"]) == (470.0, 474.5, 470.0, 473.0)
    assert feb["lots"] == 22 and feb["trade_count"] == 4
    assert feb["vwap"] == pytest.approx((470 * 4 + 474.5 * 10 + 472.25 * 6 + 473 * 2) / 22)
    assert feb["off_book_trades"] == 1 and feb["off_book_lots"] == 100
    assert feb["last_trade_at"] == "2026-11-04T19:14:59Z"
    assert feb["isin"] == FEB27 and feb["expiry_date"] == "2027-01-29"
    may = rows.iloc[1]
    assert may["last"] == 478.0 and may["cancelled_trades"] == 1 and may["trade_count"] == 1


def test_session_rows_drop_the_unfinished_session():
    rows = session_rows(parse_trades(_zip(DAY)), INSTRUMENTS, now=MID_SESSION)
    assert rows.empty


def test_session_rows_drop_a_weekend_label():
    sat = [_trade("2026-11-07T10:00:00Z", FEB27, "470", "1")]
    assert session_rows(parse_trades(_zip(sat)), INSTRUMENTS, now=datetime(2026, 11, 9, tzinfo=timezone.utc)).empty


def test_session_date_is_the_paris_calendar_day():
    late = [_trade("2026-11-04T23:30:00Z", FEB27, "470", "1")]  # 00:30 Paris on the 5th
    rows = session_rows(parse_trades(_zip(late)), INSTRUMENTS, now=datetime(2026, 11, 6, tzinfo=timezone.utc))
    assert rows["Date"].iloc[0] == pd.Timestamp("2026-11-05")


# ── fetch ─────────────────────────────────────────────────────────────────


def _download_stub(monkeypatch, answers: dict):
    def fake(selection):
        answer = answers[selection]
        if isinstance(answer, Exception):
            raise answer
        return answer
    monkeypatch.setattr(matif, "download_trades", fake)


def test_fetch_is_ok_with_rows_labelled_as_last_traded(monkeypatch):
    _download_stub(monkeypatch, {"PREVIOUS_TRADING_DAY": _zip(DAY), "CURRENT_TRADING_DAY": None})
    result = fetch_matif_trades(known=INSTRUMENTS, now=AFTER_CLOSE)
    assert result.status == "ok"
    frame = result.data["ECO"]
    assert set(frame["quote_kind"]) == {"board_last_traded"}
    assert set(frame["unit"]) == {"EUR/t"} and set(frame["currency"]) == {"EUR"}
    assert "settle" not in " ".join(frame.columns)


def test_fetch_dedupes_a_session_served_in_both_files(monkeypatch):
    _download_stub(monkeypatch, {"PREVIOUS_TRADING_DAY": _zip(DAY), "CURRENT_TRADING_DAY": _zip(DAY)})
    result = fetch_matif_trades(known=INSTRUMENTS, now=AFTER_CLOSE)
    assert result.data["ECO"].iloc[0]["trade_count"] == 4


def test_fetch_is_empty_when_neither_file_exists(monkeypatch):
    _download_stub(monkeypatch, {"PREVIOUS_TRADING_DAY": None, "CURRENT_TRADING_DAY": None})
    assert fetch_matif_trades(known=INSTRUMENTS).status == "empty"


def test_fetch_is_failed_when_both_downloads_die(monkeypatch):
    _download_stub(monkeypatch, {"PREVIOUS_TRADING_DAY": MatifTransportError("x"), "CURRENT_TRADING_DAY": MatifTransportError("y")})
    result = fetch_matif_trades(known=INSTRUMENTS)
    assert result.status == "failed" and not result.data


def test_fetch_is_partial_when_one_download_dies(monkeypatch):
    _download_stub(monkeypatch, {"PREVIOUS_TRADING_DAY": _zip(DAY), "CURRENT_TRADING_DAY": MatifTransportError("y")})
    result = fetch_matif_trades(known=INSTRUMENTS, now=AFTER_CLOSE)
    assert result.status == "failed" and result.has_rows and result.error == "y"


def test_fetch_is_failed_not_stored_when_the_instrument_map_is_unavailable(monkeypatch):
    _download_stub(monkeypatch, {"PREVIOUS_TRADING_DAY": _zip(DAY), "CURRENT_TRADING_DAY": None})

    def dead(*a, **k):
        raise MatifTransportError("FIRDS: 504")
    monkeypatch.setattr(matif, "resolve_instruments", dead)
    result = fetch_matif_trades(now=AFTER_CLOSE)
    assert result.status == "failed" and not result.data


def test_download_treats_404_as_no_file(monkeypatch):
    class _R:
        status_code = 404
        content = b""

        def raise_for_status(self):
            raise AssertionError("must not be called on 404")
    monkeypatch.setattr(matif.requests, "get", lambda *a, **k: _R())
    assert matif.download_trades("CURRENT_TRADING_DAY") is None


def test_is_configured_reads_the_desk_flag(monkeypatch):
    monkeypatch.delenv(matif.MATIF_FETCH_ENV, raising=False)
    assert not matif.is_configured()
    monkeypatch.setenv(matif.MATIF_FETCH_ENV, "1")
    assert matif.is_configured()


def test_instrument_from_doc_reads_the_firds_fields():
    inst = matif._instrument_from_doc(json.loads(json.dumps(FIRDS[FEB27])))
    assert inst == Instrument(isin=FEB27, name="Rapeseed / Colza FUTR PHYS JAN 2027", cfi="FCAPSX",
                              product="RPSD", expiry_date=date(2027, 1, 29))
    assert inst.contract_month == "2027-02"
