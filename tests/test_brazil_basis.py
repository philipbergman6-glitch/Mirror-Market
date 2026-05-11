"""Smoke tests for analysis.spreads.compute_brazil_basis and
analysis.soy_analytics.relative_value_analysis (basis orchestration)."""

from __future__ import annotations

import pandas as pd
import pandas.testing as pdt
import pytest

from analysis import soy_analytics
from analysis.spreads import compute_brazil_basis
from pipeline.units import to_metric_tons

EXPECTED_COLUMNS = [
    "Date",
    "cbot_usd_mt",
    "cepea_usd_mt",
    "brl_usd",
    "basis_usd_mt",
    "basis_pct",
]


def _close_df(dates: list[str], closes: list[float]) -> pd.DataFrame:
    idx = pd.to_datetime(dates)
    idx.name = "Date"
    return pd.DataFrame({"Close": closes}, index=idx)


def _cepea_df(dates: list[str], prices_brl_mt: list[float]) -> pd.DataFrame:
    return pd.DataFrame({
        "Date": dates,
        "price_brl_mt": prices_brl_mt,
    })


def test_compute_brazil_basis_formula():
    # CBOT 1100 c/bu × 36.7437/100 = 404.18 USD/MT
    # CEPEA 2000 BRL/MT × 0.20 USD/BRL = 400 USD/MT
    # basis = 400 - 404.18 = -4.18 USD/MT (Brazilian discount)
    dates = ["2026-01-02", "2026-01-03"]
    beans = _close_df(dates, [1100.0, 1150.0])
    cepea = _cepea_df(dates, [2000.0, 2050.0])
    brl_usd = _close_df(dates, [0.20, 0.19])

    out = compute_brazil_basis(beans, cepea, brl_usd)

    assert list(out.columns) == EXPECTED_COLUMNS
    assert len(out) == 2

    expected_cbot_0 = to_metric_tons(1100.0, "Soybeans")
    expected_cepea_0 = 2000.0 * 0.20
    assert out.iloc[0]["cbot_usd_mt"] == pytest.approx(expected_cbot_0)
    assert out.iloc[0]["cepea_usd_mt"] == pytest.approx(expected_cepea_0)
    assert out.iloc[0]["basis_usd_mt"] == pytest.approx(expected_cepea_0 - expected_cbot_0)
    assert out.iloc[0]["basis_pct"] == pytest.approx(
        (expected_cepea_0 - expected_cbot_0) / expected_cbot_0
    )


def test_compute_brazil_basis_aligns_on_intersection():
    # Only 2026-01-02 is in all three series.
    beans = _close_df(["2026-01-01", "2026-01-02"], [1100.0, 1100.0])
    cepea = _cepea_df(["2026-01-02", "2026-01-03"], [2000.0, 2050.0])
    brl_usd = _close_df(["2026-01-02", "2026-01-04"], [0.20, 0.19])

    out = compute_brazil_basis(beans, cepea, brl_usd)

    assert len(out) == 1
    assert out.iloc[0]["Date"] == pd.Timestamp("2026-01-02")


def test_compute_brazil_basis_disjoint_dates_returns_empty():
    beans = _close_df(["2026-01-01"], [1100.0])
    cepea = _cepea_df(["2026-02-01"], [2000.0])
    brl_usd = _close_df(["2026-03-01"], [0.20])

    out = compute_brazil_basis(beans, cepea, brl_usd)

    assert out.empty
    assert list(out.columns) == EXPECTED_COLUMNS


def test_compute_brazil_basis_empty_inputs_return_empty():
    empty = pd.DataFrame()

    out = compute_brazil_basis(empty, _cepea_df(["2026-01-01"], [2000.0]),
                               _close_df(["2026-01-01"], [0.20]))

    assert out.empty
    assert list(out.columns) == EXPECTED_COLUMNS


def test_compute_brazil_basis_drops_zero_fx_rate():
    # A BRL/USD close of zero would explode basis_pct — must be filtered out.
    dates = ["2026-01-02", "2026-01-03"]
    beans = _close_df(dates, [1100.0, 1150.0])
    cepea = _cepea_df(dates, [2000.0, 2050.0])
    brl_usd = _close_df(dates, [0.0, 0.20])

    out = compute_brazil_basis(beans, cepea, brl_usd)

    assert len(out) == 1
    assert out.iloc[0]["Date"] == pd.Timestamp("2026-01-03")
    assert out.iloc[0]["brl_usd"] == pytest.approx(0.20)


def test_compute_brazil_basis_does_not_mutate_inputs():
    dates = ["2026-01-02", "2026-01-03"]
    beans = _close_df(dates, [1100.0, 1150.0])
    cepea = _cepea_df(dates, [2000.0, 2050.0])
    brl_usd = _close_df(dates, [0.20, 0.19])

    before_beans = beans.copy(deep=True)
    before_cepea = cepea.copy(deep=True)
    before_brl = brl_usd.copy(deep=True)

    compute_brazil_basis(beans, cepea, brl_usd)

    pdt.assert_frame_equal(beans, before_beans)
    pdt.assert_frame_equal(cepea, before_cepea)
    pdt.assert_frame_equal(brl_usd, before_brl)


def test_compute_brazil_basis_accepts_db_column_name():
    # `read_brazil_spot` returns columns Date, commodity, price_brl, unit —
    # the storage layer renames price_brl_mt → price_brl. Both are BRL/MT.
    dates = ["2026-01-02", "2026-01-03"]
    beans = _close_df(dates, [1100.0, 1150.0])
    cepea_db = pd.DataFrame({
        "Date": dates,
        "commodity": ["Soybean (CEPEA)"] * 2,
        "price_brl": [2000.0, 2050.0],
        "unit": ["BRL/MT"] * 2,
    })
    brl_usd = _close_df(dates, [0.20, 0.19])

    out = compute_brazil_basis(beans, cepea_db, brl_usd)

    assert len(out) == 2
    expected_cepea_0 = 2000.0 * 0.20
    assert out.iloc[0]["cepea_usd_mt"] == pytest.approx(expected_cepea_0)


def test_compute_brazil_basis_missing_price_column_raises():
    dates = ["2026-01-02"]
    beans = _close_df(dates, [1100.0])
    bad = pd.DataFrame({"Date": dates, "commodity": ["x"], "unit": ["BRL/MT"]})
    brl_usd = _close_df(dates, [0.20])

    with pytest.raises(KeyError, match="price_brl_mt"):
        compute_brazil_basis(beans, bad, brl_usd)


def test_compute_brazil_basis_accepts_date_indexed_cepea():
    # The cached loader path delivers CEPEA as a DatetimeIndex, not a Date column.
    dates = pd.to_datetime(["2026-01-02", "2026-01-03"])
    dates.name = "Date"
    beans = _close_df(["2026-01-02", "2026-01-03"], [1100.0, 1150.0])
    cepea = pd.DataFrame({"price_brl_mt": [2000.0, 2050.0]}, index=dates)
    brl_usd = _close_df(["2026-01-02", "2026-01-03"], [0.20, 0.19])

    out = compute_brazil_basis(beans, cepea, brl_usd)

    assert len(out) == 2
    assert out.iloc[0]["Date"] == pd.Timestamp("2026-01-02")


# ── relative_value_analysis basis orchestration (multi-source) ──────────

def _db_spot_df(dates: list[str], prices_brl_mt: list[float], commodity: str) -> pd.DataFrame:
    return pd.DataFrame({
        "Date": dates,
        "commodity": [commodity] * len(dates),
        "price_brl": prices_brl_mt,
        "unit": ["BRL/MT"] * len(dates),
    })


def _stub_basis_inputs(monkeypatch, agrural_df, cepea_df):
    """Wire monkeypatches so relative_value_analysis sees stubbed price/spot/fx."""
    dates = ["2026-01-02", "2026-01-03"]
    beans = _close_df(dates, [1100.0, 1150.0])
    brl_usd = _close_df(dates, [0.20, 0.20])

    monkeypatch.setattr(
        soy_analytics, "_load_soy_prices", lambda: {"Soybeans": beans}
    )
    monkeypatch.setattr(
        soy_analytics, "_load_currency_data", lambda: {"BRL/USD": brl_usd}
    )

    def fake_read_brazil_spot(commodity: str) -> pd.DataFrame:
        if commodity == "Soybean (AgRural Paranaguá FOB)":
            return agrural_df
        if commodity == "Soybean (CEPEA)":
            return cepea_df
        return pd.DataFrame()

    monkeypatch.setattr(soy_analytics, "read_brazil_spot", fake_read_brazil_spot)


def test_relative_value_analysis_promotes_agrural_to_primary_when_present(monkeypatch):
    dates = ["2026-01-02", "2026-01-03"]
    agrural = _db_spot_df(dates, [2100.0, 2150.0], "Soybean (AgRural Paranaguá FOB)")
    cepea = _db_spot_df(dates, [2000.0, 2050.0], "Soybean (CEPEA)")
    _stub_basis_inputs(monkeypatch, agrural, cepea)

    result = soy_analytics.relative_value_analysis()

    assert "basis" in result
    basis = result["basis"]
    assert basis["primary"] == "Paranaguá FOB"
    assert set(basis["sources"].keys()) == {"Paranaguá FOB", "CEPEA Paraná"}

    # Hand-check the wedge: both use the same BRL/USD = 0.20 and same CBOT,
    # so the wedge is purely the BRL/MT spread × FX.
    ag_curr = basis["sources"]["Paranaguá FOB"]["current_usd_mt"]
    cepea_curr = basis["sources"]["CEPEA Paraná"]["current_usd_mt"]
    expected_wedge = ag_curr - cepea_curr
    assert basis["wedge_usd_mt"] == pytest.approx(expected_wedge)
    # And the BRL-side delta is 2150 − 2050 = 100 BRL/MT × 0.20 = 20 USD/MT.
    assert basis["wedge_usd_mt"] == pytest.approx(20.0)


def test_relative_value_analysis_falls_back_to_cepea_when_agrural_empty(monkeypatch):
    dates = ["2026-01-02", "2026-01-03"]
    cepea = _db_spot_df(dates, [2000.0, 2050.0], "Soybean (CEPEA)")
    _stub_basis_inputs(monkeypatch, pd.DataFrame(), cepea)

    result = soy_analytics.relative_value_analysis()

    basis = result["basis"]
    assert basis["primary"] == "CEPEA Paraná"
    assert "Paranaguá FOB" not in basis["sources"]
    assert basis["wedge_usd_mt"] is None


def test_relative_value_analysis_returns_no_basis_when_both_empty(monkeypatch):
    _stub_basis_inputs(monkeypatch, pd.DataFrame(), pd.DataFrame())

    result = soy_analytics.relative_value_analysis()

    assert "basis" not in result
