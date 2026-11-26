"""Layer 16 since 2026-11-17: MSAMB (Maharashtra mandis) + NCDEX polled Indore spot."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
import requests

import fetchers.india_prices as india_prices
import fetchers.msamb as msamb
from config import MANDI_SERIES_MH, NCDEX_SPOT_SERIES
from fetchers.mandi import _aggregate
from fetchers.msamb import _assert_code_is_soybean, _completed, _parse_grid
from fetchers.ncdex_spot import _parse
from pipeline.results import ScraperShapeError

_FIXTURES = Path(__file__).parent / "fixtures"
_TODAY = date(2026, 11, 17)


def _grid() -> str:
    return (_FIXTURES / "msamb_soybean_2026-11-17.html").read_text()


def _ncdex() -> dict:
    return json.loads((_FIXTURES / "ncdex_spot_sybeanidr_2026-11-17.json").read_text())


def _lot(apmc: str, unit: str, qty: str, modal: str) -> str:
    return (f"<tr><td>{apmc}</td><td>---</td><td>{unit}</td><td>{qty}</td>"
            f"<td>1</td><td>2</td><td>{modal}</td></tr>")


# ── MSAMB ────────────────────────────────────────────────────────────────────

def test_msamb_live_grid_gives_one_median_per_completed_day() -> None:
    df = _aggregate(_completed(_parse_grid(_grid()), _TODAY))
    assert list(df["Date"]) == [f"2026-11-{d}" for d in range(10, 17)]  # 11-17 still filling
    last = df.iloc[-1]
    assert last["Close"] == 72_500.0          # ₹7,250/quintal median modal × 10
    assert last["Volume"] == 126.0
    assert last["arrivals_mt"] == pytest.approx(9_335.8)  # quintals ÷ 10
    assert (df["Unit"] == "INR/MT").all()


def test_msamb_drops_lots_priced_in_another_unit_rather_than_pooling_them() -> None:
    fragment = ("<tr><td>16/11/2026</td></tr>"
                + _lot("A", "क्विंटल", "100", "7000")
                + _lot("B", "नग", "5", "90"))
    records = _parse_grid(fragment)
    assert [r["modal_price"] for r in records] == [7000.0]
    assert records[0]["arrivals"] == 10.0


def test_msamb_grid_of_only_other_units_fails() -> None:
    with pytest.raises(ScraperShapeError, match="no quintal-priced"):
        _parse_grid("<tr><td>16/11/2026</td></tr>" + _lot("B", "नग", "5", "90"))


def test_msamb_row_of_unexpected_width_fails() -> None:
    with pytest.raises(ScraperShapeError, match="cell"):
        _parse_grid("<tr><td>16/11/2026</td></tr><tr><td>A</td><td>7000</td></tr>")


def test_msamb_lot_before_any_date_fails() -> None:
    with pytest.raises(ScraperShapeError, match="before any date"):
        _parse_grid(_lot("A", "क्विंटल", "100", "7000"))


def test_msamb_empty_grid_fails() -> None:
    with pytest.raises(ScraperShapeError, match="empty"):
        _parse_grid("No record found!")


def test_msamb_only_todays_lots_is_not_a_completed_day() -> None:
    records = _parse_grid("<tr><td>17/11/2026</td></tr>" + _lot("A", "क्विंटल", "1", "7000"))
    with pytest.raises(ScraperShapeError, match="no completed"):
        _completed(records, _TODAY)


def _stub_msamb(monkeypatch, grid: str) -> None:
    page = '<select><option value="04017">सोयाबिन</option></select>'
    monkeypatch.setattr(msamb, "_get", lambda url, params=None: grid if params else page)


def test_msamb_yesterday_is_stored_but_flagged_as_still_filling(monkeypatch) -> None:
    """#621: an MSAMB day gains APMC lots through all of D+1 (11-24 read on
    11-25 at 08:39 IST: 22 lots @ ₹77,025; by 16:32 IST: 37 @ ₹76,500). The
    row is stored — save first, grade second — but says it is still filling;
    a day read on D+2 or later is complete."""
    _stub_msamb(monkeypatch, _grid())
    df = msamb.fetch_msamb_soybean(_TODAY)
    by_day = dict(zip(df["Date"], df["filling"], strict=True))
    assert by_day["2026-11-16"] == 1                      # read on D+1: filling
    assert {by_day[f"2026-11-{d}"] for d in range(10, 16)} == {0}  # read on D+2+
    assert "2026-11-17" not in by_day                     # today is never stored


def test_msamb_commodity_code_must_still_label_soybean() -> None:
    _assert_code_is_soybean('<select><option value="04017">सोयाबिन</option></select>')
    with pytest.raises(ScraperShapeError, match="re-coded"):
        _assert_code_is_soybean('<select><option value="04017">कापूस</option></select>')


# ── NCDEX polled spot ────────────────────────────────────────────────────────

def test_ncdex_live_polls_keep_the_last_poll_of_each_day() -> None:
    df = _parse(_ncdex(), "SYBEANIDR", "Indore")
    assert len(df) == 10
    by_day = dict(zip(df["Date"], df["Close"], strict=True))
    assert by_day["2026-11-13"] == pytest.approx(73_781.5)   # 15:xx poll ₹7,378.15/qtl
    assert by_day["2026-11-17"] == pytest.approx(76_549.0)
    assert df[["Open", "High", "Low", "Volume"]].isna().all().all()  # a poll has no range


def test_ncdex_row_for_another_centre_fails() -> None:
    with pytest.raises(ScraperShapeError, match="centre"):
        _parse(_ncdex(), "SYBEANIDR", "Akola")


def test_ncdex_row_for_another_symbol_fails() -> None:
    with pytest.raises(ScraperShapeError, match="asked for"):
        _parse(_ncdex(), "SYBEANAKL", "Indore")


def test_ncdex_truncated_page_fails() -> None:
    payload = _ncdex()
    payload["recordsFiltered"] = len(payload["data"]) + 5
    with pytest.raises(ScraperShapeError, match="truncated"):
        _parse(payload, "SYBEANIDR", "Indore")


def test_ncdex_price_outside_the_quintal_band_fails() -> None:
    payload = _ncdex()
    payload["data"][0]["spot_price"] = "76,549.00"   # read as ₹/MT, not ₹/quintal
    with pytest.raises(ScraperShapeError, match="band"):
        _parse(payload, "SYBEANIDR", "Indore")


def test_ncdex_empty_window_fails() -> None:
    with pytest.raises(ScraperShapeError, match="no SYBEANIDR polls"):
        _parse({"recordsTotal": 0, "recordsFiltered": 0, "data": []}, "SYBEANIDR", "Indore")


# ── Composite grading (#212 rule) ────────────────────────────────────────────

def _frame(newest: str) -> pd.DataFrame:
    return pd.DataFrame({"Date": ["2026-11-10", newest], "Close": [70_000.0, 71_000.0]})


def _wire(monkeypatch, spot, msamb) -> None:
    monkeypatch.setattr(india_prices, "fetch_ncdex_spot", lambda *a, **k: spot())
    monkeypatch.setattr(india_prices, "fetch_msamb_soybean", lambda *a, **k: msamb())


def _raise(exc: Exception):
    def go():
        raise exc
    return go


def test_both_series_ok_is_ok(monkeypatch) -> None:
    _wire(monkeypatch, lambda: _frame("2026-11-16"), lambda: _frame("2026-11-16"))
    result = india_prices.fetch_india_domestic(today=_TODAY)
    assert result.status == "ok"
    assert set(result.data) == {NCDEX_SPOT_SERIES, MANDI_SERIES_MH}


def test_one_series_dark_keeps_the_other_but_fails_the_layer(monkeypatch) -> None:
    _wire(monkeypatch, _raise(requests.ConnectionError("refused")), lambda: _frame("2026-11-16"))
    result = india_prices.fetch_india_domestic(today=_TODAY)
    assert result.status != "ok"
    assert set(result.data) == {MANDI_SERIES_MH}
    assert NCDEX_SPOT_SERIES in (result.error or "")


def test_a_shape_break_on_one_series_fails_the_layer(monkeypatch) -> None:
    _wire(monkeypatch, lambda: _frame("2026-11-16"), _raise(ScraperShapeError("drift")))
    result = india_prices.fetch_india_domestic(today=_TODAY)
    assert result.status != "ok" and set(result.data) == {NCDEX_SPOT_SERIES}


def test_both_dark_is_failed_with_no_rows(monkeypatch) -> None:
    _wire(monkeypatch, _raise(ScraperShapeError("a")), _raise(ScraperShapeError("b")))
    result = india_prices.fetch_india_domestic(today=_TODAY)
    assert result.status == "failed" and not result.data


def test_a_series_past_its_age_budget_fails_the_layer_with_rows_kept(monkeypatch) -> None:
    _wire(monkeypatch, lambda: _frame("2026-11-16"), lambda: _frame("2026-10-20"))
    result = india_prices.fetch_india_domestic(today=_TODAY)
    assert result.status != "ok"
    assert set(result.data) == {NCDEX_SPOT_SERIES, MANDI_SERIES_MH}
    assert "older than" in (result.error or "")
