"""Parser-level tests for HTML/text scrapers.

These tests run against committed fixtures under ``tests/fixtures/``. They
do NOT hit the network — they exercise the parse logic that turns raw
upstream payloads into typed DataFrames. The transport layer (HTTP retry,
backoff) is tested separately.

When a live site changes shape, the fixtures stay stable, the parser
still raises ScraperShapeError against the new shape, and these tests
continue to pass against the snapshot — that's the alert signal.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from bs4 import BeautifulSoup

from fetchers.agrural import _parse_agrural_table, fetch_agrural
from fetchers.cepea import _parse_cepea_tables
from fetchers.conab_precos import _parse_farmgate, _week_end_date
from fetchers.india_domestic import _extract_soy_prices
from fetchers.mandi import _aggregate as _mandi_aggregate
from fetchers.mandi import _collect_records as _mandi_collect
from fetchers.noticias_agricolas import _parse_indicator_page
from fetchers.safex import _parse_safex_table
from fetchers.usda import _parse_inspections
from pipeline.results import ScraperShapeError

FIXTURES = Path(__file__).parent / "fixtures"


# ── SAFEX (Grain SA) ────────────────────────────────────────────────────────

def _load_safex_html() -> str:
    return (FIXTURES / "safex_grainsa.html").read_text(encoding="utf-8")


def test_safex_parse_extracts_soybean_and_sunflower() -> None:
    """Live fixture must yield the two instruments we track in ZAR/MT."""
    result = _parse_safex_table(_load_safex_html())

    assert set(result.keys()) == {"Soybean (SAFEX)", "Sunflower (SAFEX)"}
    soy = result["Soybean (SAFEX)"]

    assert list(soy.columns) == ["Date", "Close", "Volume", "Contract", "Unit"]
    assert soy["Unit"].iloc[0] == "ZAR/MT"
    assert soy["Close"].iloc[0] > 0
    assert soy["Date"].iloc[0]  # non-empty


def test_safex_parse_picks_nearest_contract_by_month() -> None:
    """Fixture lists MAY26–DEC27; MAY26 is nearest regardless of volume."""
    result = _parse_safex_table(_load_safex_html())
    soy = result["Soybean (SAFEX)"]
    assert len(soy) == 1
    assert soy["Contract"].iloc[0] == "MAY26"
    assert soy["Close"].iloc[0] == 6939.00
    assert soy["Date"].iloc[0] == "2026-05-11"
    sun = result["Sunflower (SAFEX)"]
    assert sun["Contract"].iloc[0] == "MAY26"
    assert sun["Close"].iloc[0] == 8780.00


def _safex_page(rows: str) -> str:
    return f"""
    <html><body><table>
      <tr><th>Instrument</th><th>Contract</th><th>LastTradedTime</th>
          <th>LastTradedPrice</th><th>Difference</th><th>HighPrice</th>
          <th>LowPrice</th><th>Volume</th><th>OpenInterest</th></tr>
      {rows}
    </table></body></html>
    """


def _safex_row(contract: str, traded: str, price: str, volume: str) -> str:
    return (
        f"<tr><td>SOYB</td><td>{contract}</td><td>{traded}</td><td>{price}</td>"
        f"<td>0.00</td><td>0.00</td><td>0.00</td><td>{volume}</td><td>1.00</td></tr>"
    )


def test_safex_nearest_contract_beats_higher_volume() -> None:
    """Audit F2 fixture: Jul-31 board must pick AUG26 (8100), not the
    higher-volume DEC26 (8250)."""
    html = _safex_page(
        _safex_row("DEC26", "2026-07-31", "8250.00", "1901")
        + _safex_row("AUG26", "2026-07-31", "8100.00", "12")
    )
    soy = _parse_safex_table(html)["Soybean (SAFEX)"]
    assert soy["Contract"].iloc[0] == "AUG26"
    assert soy["Close"].iloc[0] == 8100.00


def test_safex_undated_row_hard_fails() -> None:
    """A time-only LastTradedTime must raise, never stamp today's date."""
    html = _safex_page(_safex_row("AUG26", "14:32", "8100.00", "12"))
    with pytest.raises(ScraperShapeError, match="no parseable trade date"):
        _parse_safex_table(html)


def test_safex_blank_date_hard_fails() -> None:
    html = _safex_page(_safex_row("AUG26", "", "8100.00", "12"))
    with pytest.raises(ScraperShapeError, match="no parseable trade date"):
        _parse_safex_table(html)


def test_safex_ambiguous_date_parses_dayfirst() -> None:
    """03/07/2026 is July 3rd (day-first), not March 7th."""
    html = _safex_page(_safex_row("AUG26", "03/07/2026", "8100.00", "12"))
    soy = _parse_safex_table(html)["Soybean (SAFEX)"]
    assert soy["Date"].iloc[0] == "2026-07-03"


def test_safex_unparseable_contract_code_hard_fails() -> None:
    """Nearest-by-month is meaningless if contract codes stop parsing."""
    html = _safex_page(_safex_row("202608", "2026-07-31", "8100.00", "12"))
    with pytest.raises(ScraperShapeError, match="unparseable contract code"):
        _parse_safex_table(html)


def test_safex_parse_raises_on_missing_required_column() -> None:
    """Renaming LastTradedPrice must trip ScraperShapeError.

    The live page splits the header as ``Last<br/>Traded<br/>Price`` — we
    mutate the final fragment so the rendered cell becomes ``LastTradedMystery``.
    """
    html = _load_safex_html().replace("Traded<br/>Price", "Traded<br/>Mystery")
    assert html != _load_safex_html(), "fixture pattern must match — header rename had no effect"

    with pytest.raises(ScraperShapeError, match="lasttradedprice"):
        _parse_safex_table(html)


def test_safex_parse_raises_when_no_table_present() -> None:
    with pytest.raises(ScraperShapeError, match="no <table>"):
        _parse_safex_table("<html><body><p>maintenance</p></body></html>")


def test_safex_parse_raises_when_header_present_but_no_data_rows() -> None:
    """Header row alone (no data) is a shape change worth alerting on."""
    minimal = """
    <html><body><table>
      <tr><th>Instrument</th><th>Contract</th><th>LastTradedTime</th>
          <th>LastTradedPrice</th><th>Difference</th><th>HighPrice</th>
          <th>LowPrice</th><th>Volume</th><th>OpenInterest</th></tr>
    </table></body></html>
    """
    with pytest.raises(ScraperShapeError, match="no data rows"):
        _parse_safex_table(minimal)


def test_safex_parse_returns_empty_when_our_instruments_absent() -> None:
    """A valid table that doesn't list SOYB/SUNS is an empty result, NOT a shape error."""
    foreign = """
    <html><body><table>
      <tr><th>Instrument</th><th>Contract</th><th>LastTradedTime</th>
          <th>LastTradedPrice</th><th>Difference</th><th>HighPrice</th>
          <th>LowPrice</th><th>Volume</th><th>OpenInterest</th></tr>
      <tr><td>WMAZ</td><td>DEC27</td><td>2026-05-11</td><td>3849.00</td>
          <td>0.00</td><td>0.00</td><td>0.00</td><td>9</td><td>18.00</td></tr>
    </table></body></html>
    """
    assert _parse_safex_table(foreign) == {}


# ── CEPEA (ESALQ Brazil) ────────────────────────────────────────────────────

def _load_cepea_html() -> str:
    return (FIXTURES / "cepea_soybean.html").read_text(encoding="utf-8")


def test_cepea_parse_converts_brl_per_bag_to_brl_per_mt() -> None:
    """First fixture row is 142,35 BRL/60kg → 2372.50 BRL/MT."""
    df = _parse_cepea_tables(_load_cepea_html())

    assert not df.empty
    assert list(df.columns) == ["Date", "price_brl_mt", "Unit"]
    assert df["Unit"].iloc[0] == "BRL/MT"
    # Newest-first ordering — 09/05/2026 is the most recent row
    assert df["Date"].iloc[0] == "2026-05-09"
    expected = round((142.35 / 60.0) * 1000.0, 2)
    assert df["price_brl_mt"].iloc[0] == expected


def test_cepea_parse_drops_duplicate_dates_and_sorts_descending() -> None:
    df = _parse_cepea_tables(_load_cepea_html())
    dates = df["Date"].tolist()
    assert dates == sorted(dates, reverse=True)
    assert len(dates) == len(set(dates))


def test_cepea_parse_raises_on_missing_value_column() -> None:
    """Renaming Value→Mystery so neither value/price keyword is present."""
    html = _load_cepea_html().replace("Value R$", "Mystery R$")
    with pytest.raises(ScraperShapeError, match="recognisable date\\+price header"):
        _parse_cepea_tables(html)


def test_cepea_parse_raises_on_no_table() -> None:
    with pytest.raises(ScraperShapeError, match="no <table>"):
        _parse_cepea_tables("<html><body><p>JavaScript only</p></body></html>")


def test_cepea_parse_raises_when_every_row_unparseable() -> None:
    """Header survives but every data row has garbage values."""
    html = """
    <html><body><table>
      <tr><th>Date</th><th>Value</th></tr>
      <tr><td>not-a-date</td><td>n/a</td></tr>
      <tr><td>nope</td><td>—</td></tr>
    </table></body></html>
    """
    with pytest.raises(ScraperShapeError, match="every row failed to parse"):
        _parse_cepea_tables(html)


def test_cepea_parse_brazilian_number_format() -> None:
    """Period thousands + comma decimal: 1.234,56 → 1234.56 BRL/bag."""
    from fetchers.cepea import _parse_brl_price
    assert _parse_brl_price("1.234,56") == 1234.56
    assert _parse_brl_price("142,35") == 142.35
    assert _parse_brl_price("1234.56") == 1234.56
    assert _parse_brl_price("-") is None
    assert _parse_brl_price("") is None


# ── AgRural (Paranaguá FOB) ─────────────────────────────────────────────────

_AGRURAL_FIXTURE = FIXTURES / "agrural_paranagua.html"


def _load_agrural_html() -> str:
    return _AGRURAL_FIXTURE.read_text(encoding="utf-8")


def test_agrural_parse_extracts_paranagua_price_in_brl_per_mt() -> None:
    """Live fixture row is 129,50 BRL/60kg → 2158.33 BRL/MT, dated 2026-05-08."""
    df = _parse_agrural_table(_load_agrural_html())

    assert list(df.columns) == ["Date", "price_brl_mt", "Unit"]
    assert len(df) == 1
    assert df["Unit"].iloc[0] == "BRL/MT"
    assert df["Date"].iloc[0] == "2026-05-08"
    expected = round((129.50 / 60.0) * 1000.0, 2)
    assert df["price_brl_mt"].iloc[0] == expected


def test_agrural_parse_raises_when_no_table_present() -> None:
    with pytest.raises(ScraperShapeError, match="no <table>"):
        _parse_agrural_table("<html><body><p>maintenance</p></body></html>")


def test_agrural_parse_raises_when_soy_banner_missing() -> None:
    """Only a MILHO table present → soy banner detection must fail loudly."""
    html = """
    <html><body><table>
      <tr><td>MILHO</td><td>8-May-26</td></tr>
      <tr><td>Estado</td><td>Praça</td><td>Compra</td><td>Variação hoje</td></tr>
      <tr><td>PR</td><td>Paranaguá</td><td>66,00</td><td>0,00</td></tr>
    </table></body></html>
    """
    with pytest.raises(ScraperShapeError, match="SOJA"):
        _parse_agrural_table(html)


def test_agrural_parse_raises_when_paranagua_row_absent() -> None:
    """Soy table present but Paranaguá location row missing — alert signal."""
    html = """
    <html><body><table>
      <tr><td>SOJA</td><td>8-May-26</td></tr>
      <tr><td>Estado</td><td>Praça</td><td>Compra</td><td>Variação hoje</td></tr>
      <tr><td>PR</td><td>Ponta Grossa</td><td>122,00</td><td>1,00</td></tr>
      <tr><td>MT</td><td>Sorriso</td><td>110,00</td><td>0,00</td></tr>
    </table></body></html>
    """
    with pytest.raises(ScraperShapeError, match="Paranaguá"):
        _parse_agrural_table(html)


def test_agrural_parse_raises_when_banner_date_unparseable() -> None:
    """SOJA banner present but date cell doesn't match known formats."""
    html = """
    <html><body><table>
      <tr><td>SOJA</td><td>not-a-date</td></tr>
      <tr><td>Estado</td><td>Praça</td><td>Compra</td><td>Variação hoje</td></tr>
      <tr><td>PR</td><td>Paranaguá</td><td>129,50</td><td>1,00</td></tr>
    </table></body></html>
    """
    with pytest.raises(ScraperShapeError, match="date-like cell"):
        _parse_agrural_table(html)


def test_agrural_parse_raises_when_compra_column_missing() -> None:
    """Header row without a Compra/price keyword must trip a shape error."""
    html = """
    <html><body><table>
      <tr><td>SOJA</td><td>8-May-26</td></tr>
      <tr><td>Estado</td><td>Praça</td><td>Mystery</td><td>Variação hoje</td></tr>
      <tr><td>PR</td><td>Paranaguá</td><td>129,50</td><td>1,00</td></tr>
    </table></body></html>
    """
    with pytest.raises(ScraperShapeError, match="required columns"):
        _parse_agrural_table(html)


def test_agrural_parse_raises_when_paranagua_price_unparseable() -> None:
    """Row found but its Compra cell is garbage — fail loudly."""
    html = """
    <html><body><table>
      <tr><td>SOJA</td><td>8-May-26</td></tr>
      <tr><td>Estado</td><td>Praça</td><td>Compra</td><td>Variação hoje</td></tr>
      <tr><td>PR</td><td>Paranaguá</td><td>—</td><td>0,00</td></tr>
    </table></body></html>
    """
    with pytest.raises(ScraperShapeError, match="unparseable"):
        _parse_agrural_table(html)


def test_agrural_parse_accepts_accent_stripped_paranagua() -> None:
    """The live page uses Paranaguá; tolerate the de-accented form too."""
    html = """
    <html><body><table>
      <tr><td>SOJA</td><td>8-May-26</td></tr>
      <tr><td>Estado</td><td>Praça</td><td>Compra</td><td>Variação hoje</td></tr>
      <tr><td>PR</td><td>Paranagua</td><td>129,50</td><td>1,00</td></tr>
    </table></body></html>
    """
    df = _parse_agrural_table(html)
    assert df["price_brl_mt"].iloc[0] == round((129.50 / 60.0) * 1000.0, 2)


def test_agrural_fetch_returns_failed_on_transport_failure(monkeypatch) -> None:
    """fetch_agrural() must report failed when the page can't be downloaded."""
    monkeypatch.setattr("fetchers.agrural._fetch_agrural_page", lambda: "")
    result = fetch_agrural()
    assert result.status == "failed"
    assert result.data == {}


def test_agrural_fetch_returns_failed_on_shape_error(monkeypatch) -> None:
    """Structural change → FetchResult.failed (logged at ERROR), not crash."""
    monkeypatch.setattr(
        "fetchers.agrural._fetch_agrural_page",
        lambda: "<html><body><p>maintenance</p></body></html>",
    )
    result = fetch_agrural()
    assert result.status == "failed"
    assert result.data == {}


# ── NCDEX (India Bhav Copy) ─────────────────────────────────────────────────

def _load_ncdex_df() -> pd.DataFrame:
    return pd.read_csv(FIXTURES / "ncdex_bhavcopy.csv")


def test_ncdex_extract_pulls_soybean_and_soy_oil() -> None:
    """Synthetic fixture has SYBEANIDR + SYOIL — both should appear in result."""
    result = _extract_soy_prices(_load_ncdex_df(), date(2026, 5, 8))

    assert "Soybean (NCDEX)" in result
    assert "Soybean Oil (NCDEX)" in result

    soy = result["Soybean (NCDEX)"]
    assert soy["Unit"].iloc[0] == "INR/MT"
    # 4621.00 INR/quintal * 10 multiplier = 46_210 INR/MT
    assert soy["Close"].iloc[0] == 46_210.0
    assert soy["Date"].iloc[0] == "2026-05-08"

    oil = result["Soybean Oil (NCDEX)"]
    # 950.20 INR/10kg * 100 multiplier = 95_020 INR/MT
    assert oil["Close"].iloc[0] == 95_020.0


def test_ncdex_extract_raises_on_missing_symbol_column() -> None:
    df = _load_ncdex_df().rename(columns={"SYMBOL": "MYSTERY"})
    with pytest.raises(ScraperShapeError, match="symbol column"):
        _extract_soy_prices(df, date(2026, 5, 8))


def test_ncdex_extract_raises_on_missing_close_column() -> None:
    df = _load_ncdex_df().drop(columns=["CLOSE", "SETTLEPRICE"])
    with pytest.raises(ScraperShapeError, match="close column"):
        _extract_soy_prices(df, date(2026, 5, 8))


def test_ncdex_extract_returns_empty_when_no_soy_symbols_present() -> None:
    """A valid CSV with only non-soy symbols is empty, not a shape error."""
    df = _load_ncdex_df()
    df = df[~df["SYMBOL"].str.upper().isin(
        {"SYBEANIDR", "SYOIL", "SOYBEAN", "SOYBEANIDR", "REFSOLOIL",
         "SOYOIL", "SOYMEAL", "SYBEANMEAL"}
    )]
    assert _extract_soy_prices(df, date(2026, 5, 8)) == {}


def test_ncdex_extract_raises_on_empty_dataframe() -> None:
    with pytest.raises(ScraperShapeError, match="empty DataFrame"):
        _extract_soy_prices(pd.DataFrame(), date(2026, 5, 8))


# ── AMS Export Inspections (WA_GR101) ───────────────────────────────────────

def _load_ams_text() -> str:
    return (FIXTURES / "ams_inspections.txt").read_text(encoding="utf-8")


# The fixture is for the report week ending 2026-04-30; pin "today" for
# deterministic freshness checks regardless of when the test runs.
_AMS_FIXTURE_TODAY = date(2026, 5, 11)


def test_ams_inspections_extracts_three_crops_each_with_three_weeks() -> None:
    """Summary table yields Soybeans/Corn/Wheat × 3 week-ending columns each."""
    df = _parse_inspections(_load_ams_text(), today=_AMS_FIXTURE_TODAY)

    assert set(df["commodity"]) == {"Soybeans", "Corn", "Wheat"}
    # Three weekly columns × three crops = 9 rows
    assert len(df) == 9


def test_ams_inspections_parses_known_soybean_value() -> None:
    """The 04/30/2026 SOYBEANS row in the fixture is 450,145 MT."""
    df = _parse_inspections(_load_ams_text(), today=_AMS_FIXTURE_TODAY)
    soy = df[(df["commodity"] == "Soybeans") & (df["week_ending"] == "2026-04-30")]
    assert len(soy) == 1
    assert soy["inspections_mt"].iloc[0] == 450_145.0


def test_ams_inspections_column_alignment_preserved() -> None:
    """Earliest column (year-ago) must align with the right value, not a current-week one."""
    df = _parse_inspections(_load_ams_text(), today=_AMS_FIXTURE_TODAY)
    corn_yearago = df[
        (df["commodity"] == "Corn") & (df["week_ending"] == "2025-05-01")
    ]
    assert corn_yearago["inspections_mt"].iloc[0] == 1_616_806.0


def test_ams_inspections_raises_when_header_missing() -> None:
    text = "WA_GR101\n\nReport unavailable — system maintenance\n"
    with pytest.raises(ScraperShapeError, match="locate header"):
        _parse_inspections(text, today=_AMS_FIXTURE_TODAY)


def test_ams_inspections_raises_when_report_is_stale() -> None:
    """If the report's latest week ending is >60 days old, that's a shape change."""
    far_future = date(2026, 12, 31)
    with pytest.raises(ScraperShapeError, match="stale"):
        _parse_inspections(_load_ams_text(), today=far_future)


def test_ams_inspections_raises_when_grain_row_has_too_few_numbers() -> None:
    """A grain row missing one of its three weekly columns is a shape change."""
    text = (
        "  GRAIN      04/30/2026  04/23/2026  05/01/2025    TO DATE     TO DATE\n"
        "\n"
        "SOYBEANS      450,145     638,303\n"
    )
    with pytest.raises(ScraperShapeError, match="fewer than 3 numeric columns"):
        _parse_inspections(text, today=_AMS_FIXTURE_TODAY)


# ── data.gov.in Mandi Price API (India, Layer 16 rebuild) ───────────────────

def _mandi_record(**overrides) -> dict:
    rec = {
        "state": "Madhya Pradesh",
        "district": "Ujjain",
        "market": "Nagda",
        "commodity": "Soyabean",
        "arrival_date": "02/08/2026",
        "min_price": "6800",
        "max_price": "7200",
        "modal_price": "7000",
    }
    rec.update(overrides)
    return rec


def test_mandi_aggregate_takes_median_modal_in_inr_mt() -> None:
    """Median of modal prices across mandis, converted INR/quintal → INR/MT."""
    records = [
        _mandi_record(market="A", modal_price="7000", min_price="6900", max_price="7100"),
        _mandi_record(market="B", modal_price="7100", min_price="7000", max_price="7300"),
        _mandi_record(market="C", modal_price="6900", min_price="6500", max_price="7000"),
    ]
    df = _mandi_aggregate(records)

    assert len(df) == 1
    row = df.iloc[0]
    assert row["Date"] == "2026-08-02"
    assert row["Close"] == 70_000.0   # median 7000 × 10
    assert row["Low"] == 65_000.0     # min of min_price × 10
    assert row["High"] == 73_000.0    # max of max_price × 10
    assert row["Volume"] == 3.0       # reporting mandis
    assert row["Unit"] == "INR/MT"


def test_mandi_aggregate_groups_by_arrival_date() -> None:
    records = [
        _mandi_record(arrival_date="01/08/2026", modal_price="6900"),
        _mandi_record(arrival_date="02/08/2026", modal_price="7000"),
    ]
    df = _mandi_aggregate(records)
    assert list(df["Date"]) == ["2026-08-01", "2026-08-02"]
    assert list(df["Close"]) == [69_000.0, 70_000.0]


def test_mandi_aggregate_skips_malformed_rows_but_keeps_good_ones() -> None:
    records = [
        _mandi_record(modal_price="not-a-number"),
        _mandi_record(modal_price="0"),
        _mandi_record(modal_price="7000"),
    ]
    df = _mandi_aggregate(records)
    assert len(df) == 1
    assert df.iloc[0]["Volume"] == 1.0


def test_mandi_aggregate_zero_min_max_falls_back_to_modal() -> None:
    """Thin-arrival mandis report min/max of "0" — must not drag Low/High to 0."""
    records = [
        _mandi_record(market="A", modal_price="7000", min_price="0", max_price="0"),
        _mandi_record(market="B", modal_price="7100", min_price="6900", max_price="7200"),
    ]
    df = _mandi_aggregate(records)
    assert df.iloc[0]["Low"] == 69_000.0   # min(7000, 6900) × 10 — not 0
    assert df.iloc[0]["High"] == 72_000.0  # max(7000, 7200) × 10


def test_mandi_aggregate_raises_when_no_record_is_parseable() -> None:
    """All-malformed records mean the API's field names/formats changed."""
    records = [_mandi_record(arrival_date="2026-08-02")]  # wrong date format
    with pytest.raises(ScraperShapeError, match="none with parseable"):
        _mandi_aggregate(records)


def test_mandi_aggregate_empty_records_returns_empty_frame() -> None:
    """Zero records is a normal closed-mandi day, not a shape error."""
    assert _mandi_aggregate([]).empty


def test_mandi_collect_paginates_until_total(monkeypatch) -> None:
    pages = [
        {"total": 25, "records": [_mandi_record()] * 10},
        {"total": 25, "records": [_mandi_record()] * 10},
        {"total": 25, "records": [_mandi_record()] * 5},
    ]
    calls: list[int] = []

    def fake_fetch(offset: int, state: str) -> dict:
        calls.append(offset)
        return pages[len(calls) - 1]

    monkeypatch.setattr("fetchers.mandi._fetch_page", fake_fetch)
    records = _mandi_collect("Madhya Pradesh")
    assert len(records) == 25
    assert calls == [0, 10, 20]


def test_mandi_collect_raises_on_missing_records_key(monkeypatch) -> None:
    monkeypatch.setattr(
        "fetchers.mandi._fetch_page",
        lambda offset, state: {"message": "invalid key"},
    )
    with pytest.raises(ScraperShapeError, match="records"):
        _mandi_collect("Madhya Pradesh")


# ── CONAB weekly farmgate prices (Layer 15b) ────────────────────────────────

_CONAB_HEADER = (
    "produto;classificao_produto;id_produto;uf;regiao;ano;mes;"
    "data_inicial_final_semana;semana;dsc_nivel_comercializacao;valor_produto_kg"
)


def _conab_text(rows: list[str]) -> str:
    return "\n".join([_CONAB_HEADER, *rows])


def test_conab_farmgate_extracts_pr_soybean_rows_in_brl_mt() -> None:
    text = _conab_text([
        "SOJA                 ;EM GRAOS;123;PR        ;SUL;2026;7;"
        "20-07-2026 - 24-07-2026  ;4;PREÇO RECEBIDO P/ PR;2,05",
        "SOJA                 ;EM GRAOS;123;MT        ;CENTRO-OESTE;2026;7;"
        "20-07-2026 - 24-07-2026  ;4;PREÇO RECEBIDO P/ PR;2,10",
        "SOJA                 ;EM GRAOS;123;PR        ;SUL;2026;7;"
        "13-07-2026 - 17-07-2026  ;3;PREÇO RECEBIDO P/ PR;2,03",
        "00-18-18             ;NÃO INFORMADO;10224;PR        ;SUL;2026;7;"
        "20-07-2026 - 24-07-2026  ;4;PREÇO PAGO PELO PROD;2,27",
    ])
    df = _parse_farmgate(text)

    assert list(df["Date"]) == ["2026-07-17", "2026-07-24"]
    assert list(df["price_brl_mt"]) == [2030.0, 2050.0]  # R$/kg × 1000
    assert set(df["Unit"]) == {"BRL/MT"}


def test_conab_farmgate_raises_on_missing_columns() -> None:
    with pytest.raises(ScraperShapeError, match="columns"):
        _parse_farmgate("foo;bar\n1;2")


def test_conab_farmgate_raises_when_filter_matches_nothing() -> None:
    """PR soybeans are always quoted — an empty filter is a vocabulary change."""
    text = _conab_text([
        "MILHO                ;EM GRAOS;456;PR        ;SUL;2026;7;"
        "20-07-2026 - 24-07-2026  ;4;PREÇO RECEBIDO P/ PR;1,20",
    ])
    with pytest.raises(ScraperShapeError, match="vocabulary changed"):
        _parse_farmgate(text)


def test_conab_week_end_date_parses_range() -> None:
    assert _week_end_date("21-07-2025 - 25-07-2025  ") == "2025-07-25"
    assert _week_end_date("garbage") is None


# ── Notícias Agrícolas CEPEA/ESALQ republication (Layer 17) ─────────────────

def _load_noticias_html(name: str = "noticias_agricolas_parana.html") -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def test_noticias_parse_extracts_sessions_in_brl_mt() -> None:
    """Live fixture: one row per session, sack price converted to BRL/MT."""
    df = _parse_indicator_page(_load_noticias_html())

    assert list(df.columns) == ["Date", "price_brl_mt", "Unit"]
    assert len(df) == 10
    assert (df["Unit"] == "BRL/MT").all()
    latest = df.iloc[-1]
    assert latest["Date"] == "2026-08-06"
    # 137,50 R$/saca × 1000/60
    assert latest["price_brl_mt"] == pytest.approx(2291.67, abs=0.01)


def test_noticias_corn_redirect_raises() -> None:
    """The corn ESALQ page carries the same markers and table class —
    only the 'soja' title anchor stops it (64,84 R$/saca is in-band)."""
    with pytest.raises(ScraperShapeError, match="another commodity"):
        _parse_indicator_page(_load_noticias_html("noticias_agricolas_milho.html"))


def test_noticias_section_reorder_raises() -> None:
    """A quote table filed under a non-soja heading must hard-fail, not be
    positionally swept into the soy series."""
    corn_soup = BeautifulSoup(
        _load_noticias_html("noticias_agricolas_milho.html"), "html.parser"
    )
    corn_table = str(corn_soup.find("table", class_="cot-fisicas"))
    html = _load_noticias_html().replace(
        '<table class="cot-fisicas"',
        f"<h2>Indicador do Milho Esalq/B3</h2>{corn_table}"
        '<table class="cot-fisicas"',
        1,
    )
    with pytest.raises(ScraperShapeError, match="reordered"):
        _parse_indicator_page(html)


def test_noticias_unit_switch_raises() -> None:
    """Same column silently requoted in R$/MT lands outside the
    60-300 R$/saca band and must raise, not ship a 16x wrong number."""
    html = _load_noticias_html().replace("137,50", "2.291,67")
    with pytest.raises(ScraperShapeError, match="plausibility band"):
        _parse_indicator_page(html)


def test_noticias_kg_quote_raises() -> None:
    """Low-side violation (R$/kg-style ~2,29) must also raise."""
    html = _load_noticias_html().replace("137,50", "2,29")
    with pytest.raises(ScraperShapeError, match="plausibility band"):
        _parse_indicator_page(html)
