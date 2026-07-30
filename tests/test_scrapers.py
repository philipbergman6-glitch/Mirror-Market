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

from fetchers.agrural import _parse_agrural_table, fetch_agrural
from fetchers.cepea import _parse_cepea_tables
from fetchers.india_domestic import _extract_soy_prices
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

    assert list(soy.columns) == ["Date", "Close", "Volume", "Unit"]
    assert soy["Unit"].iloc[0] == "ZAR/MT"
    assert soy["Close"].iloc[0] > 0
    assert soy["Date"].iloc[0]  # non-empty


def test_safex_parse_picks_highest_volume_contract() -> None:
    """With ties at 0 volume, the parser must still produce one row per code."""
    result = _parse_safex_table(_load_safex_html())
    soy = result["Soybean (SAFEX)"]
    assert len(soy) == 1


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
