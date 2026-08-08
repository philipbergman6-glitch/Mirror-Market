"""Tests for the X1 cross-oilseed relative-value lines.

CBOT soy oil vs CZCE rapeseed oil (USD/MT):
- analysis.soy_analytics.relative_value_analysis -> oil_vs_rapeseed
- analysis.briefing.sections.market_drivers cross-oilseed driver

ICE canola (RS=F) is dead on yfinance, so the CZCE Rapeseed Oil
continuous (CNY/MT, converted at CNY/USD spot) is the daily rapeseed leg.
"""

from __future__ import annotations

import pandas as pd
import pytest

from analysis import soy_analytics
from analysis.briefing.sections import market_drivers
from pipeline.units import to_metric_tons


def _close_df(dates: list[str], closes: list[float]) -> pd.DataFrame:
    idx = pd.to_datetime(dates)
    idx.name = "Date"
    return pd.DataFrame({"Close": closes}, index=idx)


def _rapeseed_df(dates: list[str], closes: list[float]) -> pd.DataFrame:
    return pd.DataFrame({
        "Date": pd.to_datetime(dates),
        "commodity": ["CZCE Rapeseed Oil"] * len(dates),
        "Close": closes,
    })


_DATES = [f"2026-01-{d:02d}" for d in (5, 6, 7, 8, 9, 12, 13)]


# ── relative_value_analysis: oil_vs_rapeseed ────────────────────────────

def _stub_rv_inputs(monkeypatch, rapeseed_df, with_cny=True):
    oil = _close_df(_DATES, [45.0, 45.5, 46.0, 46.5, 47.0, 47.5, 48.0])
    currencies = {}
    if with_cny:
        currencies["CNY/USD"] = _close_df(_DATES, [0.14] * len(_DATES))
    monkeypatch.setattr(
        soy_analytics, "_load_soy_prices", lambda: {"Soybean Oil": oil}
    )
    monkeypatch.setattr(
        soy_analytics, "_load_currency_data", lambda: currencies
    )
    monkeypatch.setattr(
        soy_analytics, "read_dce_futures", lambda commodity=None: rapeseed_df
    )
    return oil


def test_oil_vs_rapeseed_conversion_and_spread(monkeypatch):
    rapeseed = _rapeseed_df(_DATES, [9000.0, 9100.0, 9200.0, 9300.0, 9400.0, 9500.0, 9600.0])
    oil = _stub_rv_inputs(monkeypatch, rapeseed)

    result = soy_analytics.relative_value_analysis()

    assert "oil_vs_rapeseed" in result
    ovr = result["oil_vs_rapeseed"]

    expected_soy = to_metric_tons(oil["Close"].iloc[-1], "Soybean Oil")
    expected_rape = 9600.0 * 0.14
    assert ovr["soy_oil"] == pytest.approx(expected_soy)
    assert ovr["rapeseed_oil"] == pytest.approx(expected_rape)
    assert ovr["rapeseed_oil_cny"] == pytest.approx(9600.0)
    assert ovr["cny_usd"] == pytest.approx(0.14)
    assert ovr["spread_usd_mt"] == pytest.approx(expected_rape - expected_soy)
    assert ovr["soy_oil_as_of"] == "2026-01-13"
    assert ovr["rapeseed_oil_as_of"] == "2026-01-13"
    # Weekly changes: iloc[-1] vs iloc[-6]
    assert ovr["soy_oil_weekly_chg"] == pytest.approx((48.0 - 45.5) / 45.5 * 100)
    assert ovr["rapeseed_oil_weekly_chg"] == pytest.approx((9600.0 - 9100.0) / 9100.0 * 100)


def test_oil_vs_rapeseed_skipped_without_cny(monkeypatch):
    rapeseed = _rapeseed_df(_DATES, [9000.0] * len(_DATES))
    _stub_rv_inputs(monkeypatch, rapeseed, with_cny=False)

    result = soy_analytics.relative_value_analysis()

    assert "oil_vs_rapeseed" not in result


def test_oil_vs_rapeseed_skipped_when_czce_empty(monkeypatch):
    _stub_rv_inputs(monkeypatch, pd.DataFrame())

    result = soy_analytics.relative_value_analysis()

    assert "oil_vs_rapeseed" not in result


# ── market_drivers: cross-oilseed driver ────────────────────────────────

def _stub_market_driver_reads(monkeypatch, rapeseed_df):
    empty = pd.DataFrame()
    for name in (
        "read_cot", "read_weather", "read_export_sales", "read_forward_curve",
        "read_eia_data", "read_brazil_estimates", "read_psd", "read_economic",
    ):
        monkeypatch.setattr(market_drivers, name, lambda *a, **k: empty)
    monkeypatch.setattr(
        market_drivers, "read_dce_futures", lambda commodity=None: rapeseed_df
    )


def _enriched_oil(weekly_chg_pct: float) -> pd.DataFrame:
    df = _close_df(_DATES, [45.0] * len(_DATES))
    df["weekly_pct_change"] = weekly_chg_pct
    return df


def test_market_drivers_rapeseed_outperformance_fires(monkeypatch):
    # CZCE up ~6.7% on the week vs soy oil +1% -> divergence > 3 -> driver
    rapeseed = _rapeseed_df(_DATES, [9000.0, 9000.0, 9000.0, 9200.0, 9400.0, 9500.0, 9600.0])
    _stub_market_driver_reads(monkeypatch, rapeseed)
    enriched = {"Soybean Oil": _enriched_oil(1.0)}
    currency_data = {"CNY/USD": _close_df(_DATES, [0.14] * len(_DATES))}

    text = market_drivers.format({}, enriched, currency_data)

    assert "CZCE rapeseed oil outperforming soybean oil" in text
    assert "USD/MT" in text


def test_market_drivers_rapeseed_silent_when_aligned(monkeypatch):
    # Flat CZCE vs +1% soy oil -> divergence < 3 -> no driver line
    rapeseed = _rapeseed_df(_DATES, [9000.0] * len(_DATES))
    _stub_market_driver_reads(monkeypatch, rapeseed)
    enriched = {"Soybean Oil": _enriched_oil(1.0)}
    currency_data = {"CNY/USD": _close_df(_DATES, [0.14] * len(_DATES))}

    text = market_drivers.format({}, enriched, currency_data)

    assert "rapeseed" not in text.lower()


def test_market_drivers_rapeseed_skipped_without_cny(monkeypatch):
    rapeseed = _rapeseed_df(_DATES, [9000.0, 9000.0, 9000.0, 9200.0, 9400.0, 9500.0, 9600.0])
    _stub_market_driver_reads(monkeypatch, rapeseed)
    enriched = {"Soybean Oil": _enriched_oil(1.0)}

    text = market_drivers.format({}, enriched, {})

    assert "rapeseed" not in text.lower()


# ── config wiring ───────────────────────────────────────────────────────

def test_config_cross_oilseed_entries():
    from config import COT_COMMODITIES, PSD_TARGET_COMMODITIES, PSD_TARGET_COUNTRIES

    assert COT_COMMODITIES["Canola"] == "CANOLA - ICE FUTURES U.S."
    # Codes verified against the 2026 oilseeds bulk CSV (leading zeros
    # stripped by pandas int inference — Meal, Rapeseed is 0813600).
    assert PSD_TARGET_COMMODITIES["Rapeseed"] == "2226000"
    assert PSD_TARGET_COMMODITIES["Rapeseed Oil"] == "4239100"
    assert PSD_TARGET_COMMODITIES["Rapeseed Meal"] == "813600"
    assert "Canada" in PSD_TARGET_COUNTRIES
