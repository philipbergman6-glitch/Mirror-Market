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

import contextlib
import gzip
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from bs4 import BeautifulSoup

from config import MANDI_SORT_FIELD
from fetchers.agrural import _parse_agrural_table, fetch_agrural
from fetchers.cepea import _parse_cepea_tables
from fetchers.conab_precos import _parse_farmgate, _validate_download, _week_end_date
from fetchers.india_domestic import _extract_soy_prices
from fetchers.mandi import _aggregate as _mandi_aggregate
from fetchers.mandi import _collect_records as _mandi_collect
from fetchers.mandi import _fetch_page as _mandi_fetch_page
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


def test_safex_parse_picks_most_liquid_contract() -> None:
    """Fixture lists MAY26–DEC27; the most-traded contract wins (#157).

    Reverses the audit-F2 nearest-expiry rule from #81: on the live board
    of 2026-08-11 nearest-expiry was reading AUG26 at 163 lots while DEC26
    traded 433, i.e. it followed the contract into expiry as liquidity
    rolled away from it.

    On this fixture the soybean margin is thin — JUL26 419 lots against
    MAY26 417 — so it also documents the rule's known weak spot: near a
    roll, two contracts can swap the lead on a couple of lots and move the
    stored series by ~1%. Sunflower is unambiguous (MAY26 174 vs JUL26 119).
    """
    result = _parse_safex_table(_load_safex_html())
    soy = result["Soybean (SAFEX)"]
    assert len(soy) == 1
    assert soy["Contract"].iloc[0] == "JUL26"
    assert soy["Close"].iloc[0] == 7015.80
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


def test_safex_higher_volume_beats_nearest_contract() -> None:
    """Deliberate reversal of the audit-F2 rule (#81 → #157).

    Same Jul-31 board as the original test, opposite expectation: DEC26 on
    1901 lots is where the market is, and AUG26's 12 lots is a near-dead
    contract whose print is the noisier of the two.
    """
    html = _safex_page(
        _safex_row("DEC26", "2026-07-31", "8250.00", "1901")
        + _safex_row("AUG26", "2026-07-31", "8100.00", "12")
    )
    soy = _parse_safex_table(html)["Soybean (SAFEX)"]
    assert soy["Contract"].iloc[0] == "DEC26"
    assert soy["Close"].iloc[0] == 8250.00


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


# The live envelope ships the resource's own field catalog on every
# response, empty ones included (probed 2026-08-12) — that catalog is what
# _assert_fields_exist reads, so a payload fixture without it is not a
# payload the API ever returns.
_MANDI_FIELD_CATALOG = [
    {"name": "State", "id": "state", "type": "keyword"},
    {"name": "District", "id": "district", "type": "keyword"},
    {"name": "Market", "id": "market", "type": "keyword"},
    {"name": "Commodity", "id": "commodity", "type": "keyword"},
    {"name": "Variety", "id": "variety", "type": "keyword"},
    {"name": "Grade", "id": "grade", "type": "keyword"},
    {"name": "Arrival_Date", "id": "arrival_date", "type": "date"},
    {"name": "Min_x0020_Price", "id": "min_price", "type": "double"},
    {"name": "Max_x0020_Price", "id": "max_price", "type": "double"},
    {"name": "Modal_x0020_Price", "id": "modal_price", "type": "double"},
]


def _mandi_payload(records: list[dict], total: int | None = None, **overrides) -> dict:
    payload = {
        "total": len(records) if total is None else total,
        "count": len(records),
        "message": "Resource lists ok",
        "records": records,
        "field": [dict(f) for f in _MANDI_FIELD_CATALOG],
    }
    payload.update(overrides)
    return payload


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
    assert row["Volume"] == 3.0       # reporting mandis
    assert row["Unit"] == "INR/MT"


def test_mandi_aggregate_stores_no_high_low_band(monkeypatch) -> None:
    """Agmarknet min/max are lot extremes, not a trading range (#206).

    Indore APMC reported min ₹1,475/qtl against a ₹6,750 modal on
    2026-08-11, and the cross-mandi min of those minima was stored as a
    ₹1,010/MT "low" on a ₹67,250/MT day. There is no daily range in a
    cross-sectional median, so none is invented.
    """
    records = [
        _mandi_record(market="A", modal_price="6750", min_price="1475", max_price="6940"),
        _mandi_record(market="B", modal_price="6800", min_price="800", max_price="6860"),
    ]
    row = _mandi_aggregate(records).iloc[0]
    assert pd.isna(row["Open"])
    assert pd.isna(row["High"])
    assert pd.isna(row["Low"])
    assert row["Close"] == 67_750.0


@pytest.mark.parametrize(
    "modal, unit",
    [("67", "₹/kg"), ("670000", "₹/MT")],
)
def test_mandi_aggregate_hard_fails_when_the_price_unit_changes(
    modal: str, unit: str
) -> None:
    """A unit switch parses cleanly and is silently 10–100× wrong (#206).

    The band is the only thing standing between a re-denominated feed and
    a published India basis line that is off by an order of magnitude.
    """
    records = [_mandi_record(modal_price=modal)]
    with pytest.raises(ScraperShapeError, match="outside the plausible band"):
        _mandi_aggregate(records)


def test_mandi_aggregate_accepts_the_validated_live_level() -> None:
    """₹6,736/qtl — the MP median on 2026-08-11, cross-checked in #206."""
    row = _mandi_aggregate([_mandi_record(modal_price="6736")]).iloc[0]
    assert row["Close"] == 67_360.0


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


def test_mandi_aggregate_raises_when_no_record_is_parseable() -> None:
    """All-malformed records mean the API's field names/formats changed."""
    records = [_mandi_record(arrival_date="2026-08-02")]  # wrong date format
    with pytest.raises(ScraperShapeError, match="none with parseable"):
        _mandi_aggregate(records)


def test_mandi_aggregate_empty_records_returns_empty_frame() -> None:
    """Zero records is a normal closed-mandi day, not a shape error."""
    assert _mandi_aggregate([]).empty


def test_mandi_collect_paginates_until_total(monkeypatch) -> None:
    markets = iter(f"market-{i}" for i in range(25))
    pages = [
        _mandi_payload([_mandi_record(market=next(markets)) for _ in range(10)], total=25),
        _mandi_payload([_mandi_record(market=next(markets)) for _ in range(10)], total=25),
        _mandi_payload([_mandi_record(market=next(markets)) for _ in range(5)], total=25),
    ]
    calls: list[int] = []

    def fake_fetch(offset: int, state: str) -> dict:
        calls.append(offset)
        return pages[len(calls) - 1]

    monkeypatch.setattr("fetchers.mandi._fetch_page", fake_fetch)
    records = _mandi_collect("Madhya Pradesh")
    assert len(records) == 25
    assert calls == [0, 10, 20]


def test_mandi_collect_drops_rows_repeated_across_pages(monkeypatch) -> None:
    """Unsorted offset paging served ~20 of 115 MP rows twice (#206).

    The duplicates left the median alone but inflated Volume — the
    reporting-mandi count — by 21%, and each repeat stands for a real
    mandi the walk never served at all.
    """
    dupe = _mandi_record(market="Mandsaur", modal_price="6600")
    pages = [
        _mandi_payload([dupe, _mandi_record(market="Indore")], total=3),
        _mandi_payload([dupe], total=3),
    ]
    monkeypatch.setattr(
        "fetchers.mandi._fetch_page", lambda offset, state: pages[offset // 10]
    )
    records = _mandi_collect("Madhya Pradesh")
    assert len(records) == 2
    assert _mandi_aggregate(records).iloc[0]["Volume"] == 2.0


def test_mandi_page_request_is_sorted(monkeypatch) -> None:
    """A stable sort is what makes offset paging total-ordered here."""
    seen: dict = {}

    class _Resp:
        status_code = 200

        @staticmethod
        def json() -> dict:
            return {"total": 0, "records": []}

    def fake_get(url, params=None, headers=None, timeout=None):
        seen.update(params or {})
        return _Resp()

    monkeypatch.setattr("fetchers.mandi.requests.get", fake_get)
    _mandi_fetch_page(0, "Madhya Pradesh")
    assert seen[f"sort[{MANDI_SORT_FIELD}]"] == "asc"


def test_mandi_rate_limit_envelope_is_retried_not_a_schema_error(monkeypatch) -> None:
    """The shared sample key answers HTTP 200 ``{"error": "Rate limit exceeded"}``.

    Handed on to ``_collect_records`` that reads as a missing ``records``
    key — "the schema changed" — and hard-fails the whole layer over a
    throttle that clears in seconds (observed twice live, 2026-08-12).
    """
    payloads = [
        {"error": "Rate limit exceeded"},
        {"total": 1, "records": [_mandi_record()]},
    ]

    class _Resp:
        status_code = 200

        def __init__(self, payload: dict) -> None:
            self._payload = payload

        def json(self) -> dict:
            return self._payload

    calls: list[int] = []

    def fake_get(url, params=None, headers=None, timeout=None):
        calls.append(1)
        return _Resp(payloads[len(calls) - 1])

    monkeypatch.setattr("fetchers.mandi.requests.get", fake_get)
    monkeypatch.setattr("fetchers.mandi.retry_sleep", lambda attempt: None)
    payload = _mandi_fetch_page(0, "Madhya Pradesh")
    assert payload["total"] == 1
    assert len(calls) == 2


def test_mandi_collect_raises_on_missing_records_key(monkeypatch) -> None:
    monkeypatch.setattr(
        "fetchers.mandi._fetch_page",
        lambda offset, state: {"message": "invalid key"},
    )
    with pytest.raises(ScraperShapeError, match="records"):
        _mandi_collect("Madhya Pradesh")


@pytest.mark.parametrize("renamed", ["state", "commodity", "arrival_date", "modal_price"])
def test_mandi_collect_rejects_a_renamed_field_on_an_empty_day(
    monkeypatch, renamed: str
) -> None:
    """A renamed filter field is answered with 0 rows, not with an error.

    Probed live 2026-08-12: ``filters[commodity_name]=Soyabean``,
    ``filters[Commodity]=Soyabean`` and ``filters[state_name]=Tamil Nadu``
    each returned HTTP 200, ``message: "Resource lists ok"``, ``total: 0``
    against a resource holding 6,421 rows that second — the unknown field
    is neither rejected nor ignored-and-unfiltered. So a rename reads as a
    closed-mandi day, which ``india_domestic`` grades as a success.

    The empty payload is the whole point of the fixture: this is the case
    a record-level check cannot see, because there are no records.
    """
    catalog = [f for f in _MANDI_FIELD_CATALOG if f["id"] != renamed]
    catalog.append({"name": renamed, "id": f"{renamed}_name", "type": "keyword"})
    monkeypatch.setattr(
        "fetchers.mandi._fetch_page",
        lambda offset, state: _mandi_payload([], total=0, field=catalog),
    )
    with pytest.raises(ScraperShapeError, match=f"no longer exposes field.*{renamed}"):
        _mandi_collect("Madhya Pradesh")


def test_mandi_collect_accepts_an_empty_day_with_the_catalog_intact() -> None:
    """Sunday, holiday, or pre-arrival hours — empty is not a shape error.

    Guards the check above from over-firing: the whole reason the layer
    grades empty as a success is that most of a mandi week legitimately
    looks like this.
    """
    import fetchers.mandi as mandi_mod

    mandi_mod._assert_fields_exist(_mandi_payload([], total=0))


def test_mandi_collect_ignores_extra_fields_appearing_in_the_catalog() -> None:
    """A field the resource adds is not a break; only a missing one is."""
    import fetchers.mandi as mandi_mod

    extra = [*_MANDI_FIELD_CATALOG, {"name": "Tehsil", "id": "tehsil", "type": "keyword"}]
    mandi_mod._assert_fields_exist(_mandi_payload([], total=0, field=extra))


def test_mandi_collect_raises_when_the_envelope_drops_the_field_catalog(
    monkeypatch,
) -> None:
    """No catalog means the rename check is blind — fail rather than trust."""
    monkeypatch.setattr(
        "fetchers.mandi._fetch_page",
        lambda offset, state: {"total": 0, "count": 0, "records": []},
    )
    with pytest.raises(ScraperShapeError, match="no 'field' catalog"):
        _mandi_collect("Madhya Pradesh")


@pytest.mark.parametrize(
    "stray, match",
    [
        ({"state": "Tamil Nadu"}, "state filter is no longer applied"),
        ({"commodity": "Paddy(Common)"}, "commodity filter is no longer applied"),
    ],
)
def test_mandi_collect_rejects_rows_outside_the_requested_filters(
    monkeypatch, stray: dict, match: str
) -> None:
    """The other half of the defence: rows we did not ask for.

    Today this API answers an unknown filter with nothing rather than with
    everything (see the rename test), so this path is unreachable live —
    which is exactly why it is pinned. If the filter ever stops being
    applied, the result is not an outage but a median over every commodity
    in every state, stored under ``Soybean (Mandi MP)`` and shaped like a
    real number.
    """
    pages = [_mandi_payload([_mandi_record(), _mandi_record(**stray)], total=2)]
    monkeypatch.setattr("fetchers.mandi._fetch_page", lambda offset, state: pages[0])
    with pytest.raises(ScraperShapeError, match=match):
        _mandi_collect("Madhya Pradesh")


def test_mandi_collect_accepts_filter_values_differing_only_in_case() -> None:
    """The API matches filter values case-insensitively (``soyabean`` → 9 rows,
    probed 2026-08-12), so the returned casing is not a contract."""
    import fetchers.mandi as mandi_mod

    mandi_mod._assert_filters_honoured(
        [_mandi_record(state="madhya pradesh", commodity="SOYABEAN")],
        "Madhya Pradesh",
    )


def test_mandi_never_sends_a_python_user_agent(monkeypatch) -> None:
    """api.data.gov.in blackholes Python-identifying User-Agents.

    It does not 403 — it accepts the connection and never answers, so the
    failure surfaces as a read timeout and the layer goes silently dark
    (that is exactly what happened between 2026-08-07 and 2026-08-10).
    requests' default UA is ``python-requests/x.y``, so an explicit header
    is the whole fix; this test fails if it is ever dropped.
    """
    seen: dict[str, object] = {}

    class _Resp:
        status_code = 200

        @staticmethod
        def json() -> dict:
            return {"total": 0, "records": []}

    def fake_get(url, params=None, headers=None, timeout=None):
        seen["headers"] = headers or {}
        return _Resp()

    monkeypatch.setattr("fetchers.mandi.requests.get", fake_get)
    _mandi_fetch_page(0, "Madhya Pradesh")

    ua = str(seen["headers"].get("User-Agent", ""))
    assert ua, "mandi requests must carry an explicit User-Agent"
    assert "python" not in ua.lower()


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


# ── CONAB download sanity gate (live fixture) ───────────────────────────────
#
# tests/fixtures/conab_precos_semanal_uf.txt.gz is the real PrecosSemanalUF.txt
# as served on 2026-08-11 (13,608,731 bytes, 89,700 lines), gzipped for the
# repo. The failure cases below are derived from those genuine bytes by
# truncation/mutilation — nothing here is hand-authored.

def _load_conab_live() -> str:
    with gzip.open(FIXTURES / "conab_precos_semanal_uf.txt.gz", "rb") as fh:
        return fh.read().decode("latin-1")


def test_conab_gate_passes_on_the_genuine_download() -> None:
    text = _load_conab_live()
    _validate_download(text)  # must not raise
    df = _parse_farmgate(text)
    assert len(df) > 50
    assert df["price_brl_mt"].between(500, 5000).all()


def test_conab_gate_rejects_truncated_download() -> None:
    """A 200 OK that delivers only the first few KB must not be parsed."""
    truncated = _load_conab_live()[:5000]
    with pytest.raises(ScraperShapeError, match="undersized"):
        _validate_download(truncated)


def test_conab_gate_rejects_full_size_but_row_starved_download() -> None:
    """Big payload, almost no rows — padded/garbage body, not a price file."""
    text = _load_conab_live()
    head = "\n".join(text.split("\n")[:200])
    with pytest.raises(ScraperShapeError, match="undersized"):
        _validate_download(head + "\n" + "x" * 2_000_000)


def test_conab_gate_rejects_headerless_download() -> None:
    """Header row dropped upstream — the first data row would be eaten as
    column names and the shape check must catch it before parse."""
    body = _load_conab_live().split("\n", 1)[1]
    with pytest.raises(ScraperShapeError, match="header"):
        _validate_download(body)


def test_conab_gate_rejects_delimiter_switch() -> None:
    """Semicolons swapped for tabs: same bytes, unparseable shape."""
    text = _load_conab_live().replace(";", "\t")
    with pytest.raises(ScraperShapeError, match="header"):
        _validate_download(text)


def test_conab_gate_rejects_implausible_magnitudes() -> None:
    """Prices requoted R$/tonne (x1000) keep every column name and row
    count — only the magnitude band catches it."""
    lines = _load_conab_live().split("\n")
    out = [lines[0]]
    for line in lines[1:]:
        if not line.strip():
            continue
        fields = line.split(";")
        value = fields[-1].strip().replace(",", ".")
        with contextlib.suppress(ValueError):
            fields[-1] = f"{float(value) * 1000:.2f}".replace(".", ",")
        out.append(";".join(fields))
    with pytest.raises(ScraperShapeError, match="implausible"):
        _validate_download("\n".join(out))


def test_conab_gate_rejects_corruption_below_the_head_of_the_file() -> None:
    """The file is sorted by product name, so a head-only sample would only
    ever see the NPK fertilisers. Corrupt the *second half* of the real
    rows and the strided sample must still catch it."""
    lines = [ln for ln in _load_conab_live().split("\n") if ln.strip()]
    half = len(lines) // 2
    mangled = lines[:half] + [ln + ";extra" for ln in lines[half:]]
    with pytest.raises(ScraperShapeError, match="field-count mismatch"):
        _validate_download("\n".join(mangled))


def test_conab_gate_rejects_empty_price_column() -> None:
    """Every price blanked: names, row count and field widths all intact."""
    lines = [ln for ln in _load_conab_live().split("\n") if ln.strip()]
    out = [lines[0]]
    for line in lines[1:]:
        fields = line.split(";")
        fields[-1] = ""
        out.append(";".join(fields))
    with pytest.raises(ScraperShapeError, match="implausible"):
        _validate_download("\n".join(out))


def test_conab_gate_tolerates_the_files_normal_blank_prices() -> None:
    """~7% of real rows carry no price; blanks are missing data, not a
    format break, so a heavier-than-usual blank week must still pass."""
    lines = [ln for ln in _load_conab_live().split("\n") if ln.strip()]
    out = [lines[0]]
    for i, line in enumerate(lines[1:]):
        fields = line.split(";")
        if i % 3 == 0:
            fields[-1] = ""
        out.append(";".join(fields))
    _validate_download("\n".join(out))  # must not raise


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
