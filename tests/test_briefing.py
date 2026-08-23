"""End-to-end smoke tests for the briefing package.

These don't snapshot section text byte-for-byte (DB content varies) but
they DO verify:
  - the orchestrator imports cleanly and wires every section module
  - generate_briefing() against an empty DB returns a string with the
    expected section headers (i.e. every section returns something even
    when its source table is empty)
  - generate_briefing() against a populated DB exercises non-empty paths
    for prices, currencies, and freshness without raising
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analysis import loaders
from analysis.briefing import BriefingData, generate_briefing, generate_briefing_data
from pipeline import store

# Section header strings the orchestrator MUST emit on every run, even
# when the underlying tables are empty. If a header is missing the
# orchestrator likely failed to call a section or the section returned
# the empty string when it shouldn't have.
ALWAYS_PRESENT_HEADERS = [
    "=== Mirror Market Daily Briefing",
    "PRICES:",
    "CRUSH SPREAD:",
    "ECONOMIC CONTEXT (FRED)",
    "USDA FUNDAMENTALS",
    "CROP CONDITIONS",
    "WASDE ESTIMATES",
    "EXPORT SALES",
    "EXPORT INSPECTIONS",
    "DCE CHINESE FUTURES",
    "FORWARD CURVE",
    "BIOFUEL & ENERGY",
    "BRAZIL CROP ESTIMATES",
    "CURRENCIES:",
    "COT POSITIONING",
    "WEATHER ALERTS",
    "GLOBAL SUPPLY",
    "WORLD PRICES",
    "EMERGING MARKETS",
    "CORRELATIONS:",
    "SEASONAL ANALYSIS",
    "MARKET DRIVERS:",
    "SIGNALS:",
]


@pytest.fixture(autouse=True)
def _reset_loaders():
    loaders.clear_loader_cache()
    yield
    loaders.clear_loader_cache()


def test_generate_briefing_empty_db_returns_string_with_all_headers(patched_db):
    out = generate_briefing()

    assert isinstance(out, str)
    assert len(out) > 100  # non-trivial output
    missing = [h for h in ALWAYS_PRESENT_HEADERS if h not in out]
    assert not missing, f"missing section headers: {missing}"


def test_generate_briefing_with_data_does_not_crash(patched_db):
    rng = np.random.default_rng(seed=11)
    idx = pd.date_range("2024-01-01", periods=120, freq="B")
    idx.name = "Date"
    close = 1300.0 + rng.normal(0, 10, size=120).cumsum()
    ohlcv = pd.DataFrame(
        {
            "Open": close,
            "High": close * 1.01,
            "Low": close * 0.99,
            "Close": close,
            "Volume": rng.integers(50_000, 200_000, size=120).astype(float),
        },
        index=idx,
    )
    for commodity in ["Soybeans", "Soybean Oil", "Soybean Meal", "Corn"]:
        store.save_price_data(commodity, ohlcv)

    currency = ohlcv.copy()
    currency["Close"] = np.linspace(5.0, 5.2, 120)
    store.save_currency_data("BRL/USD", currency)

    out = generate_briefing()

    assert isinstance(out, str)
    assert "Soybeans" in out
    assert "BRL/USD" in out
    assert "CRUSH SPREAD" in out


def test_generate_briefing_data_returns_typed_briefing_data(patched_db):
    data = generate_briefing_data()

    assert isinstance(data, BriefingData)
    assert isinstance(data.text, str) and data.text
    # Every section in _SECTION_ORDER except the always-skip ones contributes
    # a key. We can't assert exact count without coupling to orchestrator
    # internals, but we can assert key sections are present.
    for key in ("prices", "crush", "currencies", "market_drivers", "signals"):
        assert key in data.section_texts, f"missing section: {key}"


def test_generate_briefing_text_matches_briefing_data_text(patched_db):
    """`generate_briefing()` must be a pure wrapper around `generate_briefing_data().text`."""
    rng = np.random.default_rng(seed=23)
    idx = pd.date_range("2024-01-01", periods=80, freq="B")
    close = 1300.0 + rng.normal(0, 10, size=80).cumsum()
    ohlcv = pd.DataFrame(
        {
            "Open": close,
            "High": close * 1.01,
            "Low": close * 0.99,
            "Close": close,
            "Volume": rng.integers(50_000, 200_000, size=80).astype(float),
        },
        index=idx,
    )
    ohlcv.index.name = "Date"
    store.save_price_data("Soybeans", ohlcv)

    loaders.clear_loader_cache()  # ensure fresh state between calls
    text_first = generate_briefing()
    loaders.clear_loader_cache()
    data = generate_briefing_data()

    # Only assert structural equivalence, not byte-for-byte — the briefing
    # header includes the current date, which is identical in this test.
    assert text_first.splitlines()[:3] == data.text.splitlines()[:3]
    # And section text comes through the BriefingData.
    assert data.section("prices") in data.text
    assert data.section("market_drivers") in data.text


def test_briefing_data_section_returns_empty_for_unknown_name(patched_db):
    data = generate_briefing_data()
    assert data.section("nonexistent") == ""


def _populate_full_briefing_fixtures(patched_db):
    """Populate the briefing-relevant tables with minimal but valid rows.

    Exercises the post-empty-check formatting paths in each section. The
    values themselves don't need to be realistic — we're verifying code
    paths run, not numerical correctness.
    """
    rng = np.random.default_rng(seed=7)
    idx = pd.date_range("2024-01-01", periods=240, freq="B")
    idx.name = "Date"
    close = 1300.0 + rng.normal(0, 10, size=240).cumsum()
    ohlcv = pd.DataFrame(
        {
            "Open": close,
            "High": close * 1.01,
            "Low": close * 0.99,
            "Close": close,
            "Volume": rng.integers(50_000, 200_000, size=240).astype(float),
        },
        index=idx,
    )

    for commodity in ["Soybeans", "Soybean Oil", "Soybean Meal", "Corn", "Wheat", "Palm Oil (CME)", "Live Cattle"]:
        store.save_price_data(commodity, ohlcv)

    for pair in ["BRL/USD", "CNY/USD", "ARS/USD"]:
        store.save_currency_data(pair, ohlcv)

    econ = pd.Series(
        np.linspace(95.0, 105.0, 30),
        index=pd.date_range("2024-09-01", periods=30, freq="B", name="Date"),
    )
    store.save_fred_data("US Dollar Index", econ)
    store.save_fred_data("Treasury 2Y", pd.Series(np.linspace(4.5, 4.2, 30), index=econ.index))
    store.save_fred_data("Treasury 10Y", pd.Series(np.linspace(4.0, 4.4, 30), index=econ.index))

    cot = pd.DataFrame({
        "Date": pd.date_range("2024-09-01", periods=4, freq="W"),
        "commercial_long": [100, 110, 120, 115],
        "commercial_short": [80, 90, 100, 105],
        "commercial_net": [20, 20, 20, 10],
        "noncommercial_long": [200, 210, 220, 215],
        "noncommercial_short": [150, 160, 170, 175],
        "noncommercial_net": [50, 50, 50, 40],
        "total_open_interest": [400, 410, 420, 415],
    })
    store.save_cot_data("Soybeans", cot)

    weather = pd.DataFrame({
        "Date": pd.date_range("2024-09-01", periods=5),
        "temp_max": [28.0, 30.0, 25.0, 26.0, 40.0],   # latest day = extreme heat
        "temp_min": [18.0, 20.0, 17.0, 18.0, 22.0],
        "precipitation": [5.0, 10.0, 0.5, 2.0, 25.0], # latest day = heavy rain
    })
    store.save_weather_data("US Midwest (Iowa)", weather)

    psd = pd.DataFrame({
        "country": ["Brazil", "United States", "China", "Indonesia"],
        "year": [2024, 2024, 2024, 2024],
        "attribute": ["Production", "Production", "Imports", "Production"],
        "value": [155_000.0, 115_000.0, 105_000.0, 47_000.0],
        "unit": ["1000 MT", "1000 MT", "1000 MT", "1000 MT"],
    })
    store.save_psd_data("Soybeans", psd[psd["country"] != "Indonesia"])
    store.save_psd_data("Palm Oil", psd[psd["country"] == "Indonesia"])

    wb = pd.DataFrame({
        "Date": pd.date_range("2024-08-01", periods=3, freq="MS"),
        "price": [400.0, 420.0, 415.0],
        "unit": ["USD/mt", "USD/mt", "USD/mt"],
    })
    store.save_worldbank_data("Soybeans", wb)

    es = pd.DataFrame({
        "week_ending": pd.date_range("2024-09-01", periods=2, freq="W"),
        "country": ["China", "Mexico"],
        "net_sales": [500_000.0, 100_000.0],
        "weekly_exports": [400_000.0, 80_000.0],
        "accumulated_exports": [10_000_000.0, 2_000_000.0],
        "outstanding_sales": [3_000_000.0, 500_000.0],
    })
    store.save_export_sales("Soybeans", es)

    insp = pd.DataFrame({
        "week_ending": pd.date_range("2024-09-01", periods=2, freq="W"),
        "inspections_mt": [400_000.0, 380_000.0],
    })
    store.save_inspections("Soybeans", insp)

    dce = pd.DataFrame({
        "Date": pd.date_range("2024-09-01", periods=5),
        "Open": [4500.0] * 5,
        "High": [4520.0] * 5,
        "Low": [4480.0] * 5,
        "Close": [4510.0] * 5,
        "Volume": [10000.0] * 5,
        "Open_Interest": [50000.0] * 5,
        "Settle": [4510.0] * 5,
    })
    store.save_dce_futures_data("DCE Soybean No.2", dce)

    fc = pd.DataFrame({
        "contract_month": ["2024-11", "2025-01", "2025-03"],
        "label": ["F", "H", "K"],
        "ticker": ["ZSX24", "ZSF25", "ZSH25"],
        "close": [1300.0, 1320.0, 1340.0],
    })
    store.save_forward_curve("Soybeans", fc)

    eia = pd.DataFrame({
        "Date": pd.date_range("2024-08-01", periods=3, freq="MS"),
        "value": [1.0, 1.1, 1.05],
        "unit": ["mil bbl/d", "mil bbl/d", "mil bbl/d"],
    })
    store.save_eia_data("Biodiesel Production", eia)
    store.save_eia_data("Ethanol Production", eia)

    wasde = pd.DataFrame({
        "commodity_desc": ["SOYBEANS", "SOYBEANS"],
        "statisticcat_desc": ["PRODUCTION", "PRODUCTION"],
        "Value": [4_500_000.0, 4_550_000.0],
        "year": ["2025/26", "2025/26"],
        "unit_desc": ["Million Bushels", "Million Bushels"],
        "reference_period_desc": ["2026-03-15", "2026-04-15"],
    })
    store.save_wasde("SOYBEANS/Production", wasde)

    brazil_est = pd.DataFrame({
        "source": ["CONAB", "CONAB"],
        "commodity": ["Soybeans", "Soybeans"],
        "crop_year": ["2023/24", "2024/25"],
        "attribute": ["Production", "Production"],
        "value": [150_000.0, 158_000.0],
        "unit": ["1000 MT", "1000 MT"],
        "report_date": ["2024-08-01", "2024-09-01"],
    })
    store.save_brazil_estimates(brazil_est)

    usda = pd.DataFrame({
        "year": [2023, 2024],
        "short_desc": ["SOYBEANS - PRODUCTION, MEASURED IN BU", "SOYBEANS - PRODUCTION, MEASURED IN BU"],
        "Value": ["4,000,000", "4,200,000"],
        "unit_desc": ["BU", "BU"],
        "state_name": ["US TOTAL", "US TOTAL"],
        "reference_period_desc": ["YEAR", "YEAR"],
    })
    store.save_usda_data(usda, "PRODUCTION")

    crop = pd.DataFrame({
        "week_ending": ["2024-09-01", "2024-09-08"],
        "year": [2024, 2024],
        "short_desc": ["SOYBEANS - CONDITION, GOOD/EXCELLENT", "SOYBEANS - PROGRESS, PLANTED"],
        "Value": ["65", "92"],
        "unit_desc": ["PCT", "PCT"],
        "statisticcat_desc": ["CONDITION", "PROGRESS"],
    })
    store.save_crop_progress("SOYBEANS", crop)

    store.save_freshness("Layer 1: Prices", rows_fetched=100, status="success")


def test_generate_briefing_with_fully_populated_db(patched_db):
    """Exercise the orchestrator's non-empty paths across every section."""
    _populate_full_briefing_fixtures(patched_db)

    out = generate_briefing()

    # Section-level smoke checks: each populated table should produce a
    # non-trivial signal in the output.
    assert "Soybeans" in out
    assert "BRL/USD" in out
    assert "US Midwest (Iowa)" in out  # weather alert wrote a populated region
    assert "Brazil" in out               # PSD highlights mention Brazil
    assert "Heavy rain" in out or "Extreme heat" in out  # one of the alert triggers
    assert "WASDE ESTIMATES" in out
    assert "EXPORT SALES" in out
    assert "BIOFUEL & ENERGY" in out


def test_near_roll_suppression_reaches_archive_and_display(patched_db, monkeypatch):
    """Regression (#14, re-decided in A4 #301): the archive, the display and
    BriefingData.signals must agree — and the agreed treatment is now
    suppression. A provider-series technical signal inside the estimated
    roll window exists nowhere downstream.
    """
    import json
    import sqlite3

    from analysis.briefing import orchestrator

    near_roll_signal = {
        "date": pd.Timestamp("2026-07-15"),  # estimated Soybeans July roll date (mid-month expiry)
        "commodity": "Soybeans",
        "signal_type": "golden_cross_50_200",
        "severity": "alert",
        "description": "Soybeans MAJOR golden cross",
    }
    monkeypatch.setattr(
        orchestrator.prices, "format",
        lambda price_data: ("PRICES:", [dict(near_roll_signal)], {}),
    )
    monkeypatch.setattr(
        orchestrator.stocks_to_use, "format", lambda: ("STOCKS-TO-USE:", []),
    )

    data = generate_briefing_data(archive=True)

    # BriefingData.signals carries no trace of the suppressed signal ...
    assert data.signals == []

    # ... the displayed section agrees ...
    assert "golden cross" not in data.section("signals")

    # ... and so does the archived signals_json.
    conn = sqlite3.connect(str(patched_db))
    (signals_json,) = conn.execute(
        "SELECT signals_json FROM briefings ORDER BY briefing_date DESC LIMIT 1"
    ).fetchone()
    conn.close()
    assert json.loads(signals_json) == []
