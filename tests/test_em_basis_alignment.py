"""F1 regression tests — EM basis must come from a date-aligned join.

The 2026-08 audit found `emerging_markets_analysis()` computing each
country's basis from the *latest close of three series on potentially
different dates* (spot, FX, CBOT), while the briefing's BRAZIL BASIS
section date-aligns via `compute_brazil_basis`. Result: two different
Brazil basis numbers on the same dashboard page, and a stalled spot leg
silently mixed week-old prices with today's futures/FX.

Scenario used throughout: the spot legs (CEPEA, SAFEX, mandi) stall on
2026-07-31 while CBOT and FX keep printing through 2026-08-07 with
materially different values. The correct basis uses the 07-31 close of
*all three* legs.
"""

from __future__ import annotations

import pandas as pd
import pytest

import analysis.soy_analytics as soy_analytics
from analysis.spreads import compute_brazil_basis
from pipeline.units import to_metric_tons

STALL_DATE = "2026-07-31"
FRESH_DATES = ["2026-07-30", "2026-07-31", "2026-08-06", "2026-08-07"]

# CBOT (cents/bu): 1000 on the stall date, 1100 by the fresh date.
CBOT_CLOSES = [990.0, 1000.0, 1080.0, 1100.0]
# FX rates (USD per local unit): change between stall and fresh dates so
# mixed-date math produces a visibly different number.
BRL_USD = [0.21, 0.20, 0.19, 0.18]
ZAR_USD = [0.054, 0.055, 0.051, 0.050]
INR_USD = [0.0121, 0.0120, 0.0116, 0.0115]

CEPEA_BRL_MT = 2100.0
SAFEX_ZAR_MT = 8000.0
MANDI_INR_MT = 42000.0

CBOT_USD_MT_STALL = to_metric_tons(1000.0, "Soybeans")


def _indexed(dates: list[str], closes: list[float]) -> pd.DataFrame:
    idx = pd.to_datetime(dates)
    idx.name = "Date"
    return pd.DataFrame({"Close": closes}, index=idx)


def _cepea_df() -> pd.DataFrame:
    return pd.DataFrame({
        "Date": ["2026-07-30", STALL_DATE],
        "commodity": ["Soybean (CEPEA)"] * 2,
        "price_brl": [2050.0, CEPEA_BRL_MT],
        "unit": ["BRL/MT"] * 2,
    })


def _safex_df() -> pd.DataFrame:
    return pd.DataFrame({
        "Date": ["2026-07-30", STALL_DATE],
        "commodity": ["Soybean (SAFEX)"] * 2,
        "Close": [7900.0, SAFEX_ZAR_MT],
    })


def _mandi_df() -> pd.DataFrame:
    return pd.DataFrame({
        "Date": ["2026-07-30", STALL_DATE],
        "Close": [41000.0, MANDI_INR_MT],
    })


@pytest.fixture
def stalled_spot_em(monkeypatch: pytest.MonkeyPatch):
    """Wire emerging_markets_analysis to the stalled-spot scenario."""
    prices = {"Soybeans": _indexed(FRESH_DATES, CBOT_CLOSES)}
    currencies = {
        "BRL/USD": _indexed(FRESH_DATES, BRL_USD),
        "ZAR/USD": _indexed(FRESH_DATES, ZAR_USD),
        "INR/USD": _indexed(FRESH_DATES, INR_USD),
    }

    monkeypatch.setattr(soy_analytics, "load_prices", lambda: prices)
    monkeypatch.setattr(soy_analytics, "load_currencies", lambda: currencies)
    monkeypatch.setattr(soy_analytics, "read_psd", lambda: pd.DataFrame())
    monkeypatch.setattr(soy_analytics, "read_weather", lambda: pd.DataFrame())
    monkeypatch.setattr(
        soy_analytics, "read_brazil_spot", lambda *args: _cepea_df()
    )
    monkeypatch.setattr(soy_analytics, "read_safex", lambda: _safex_df())
    monkeypatch.setattr(
        soy_analytics, "read_india_domestic", lambda *args: _mandi_df()
    )
    return prices, currencies


def test_brazil_basis_matches_briefing_path(stalled_spot_em):
    """Acceptance: EM card and briefing print the same Brazil basis."""
    prices, currencies = stalled_spot_em

    dom = soy_analytics.emerging_markets_analysis()["countries"]["Brazil"][
        "brazil_domestic"
    ]

    # The briefing's BRAZIL BASIS section computes exactly this:
    briefing_basis = compute_brazil_basis(
        prices["Soybeans"], _cepea_df(), currencies["BRL/USD"]
    )
    briefing_latest = round(float(briefing_basis.iloc[-1]["basis_usd_mt"]), 2)

    assert dom["brazil_cbot_basis_usd"] == briefing_latest


def test_brazil_basis_uses_common_date_not_latest_closes(stalled_spot_em):
    dom = soy_analytics.emerging_markets_analysis()["countries"]["Brazil"][
        "brazil_domestic"
    ]

    expected = round(CEPEA_BRL_MT * 0.20 - CBOT_USD_MT_STALL, 2)
    buggy_mixed = round(CEPEA_BRL_MT * 0.18 - to_metric_tons(1100.0, "Soybeans"), 2)

    assert dom["brazil_cbot_basis_usd"] == expected
    assert dom["brazil_cbot_basis_usd"] != buggy_mixed
    assert dom["basis_date"] == STALL_DATE
    # The CBOT reference shown next to the basis is the aligned-date one.
    assert dom["cbot_usd"] == round(CBOT_USD_MT_STALL, 2)
    assert dom["cepea_soy_usd"] == round(CEPEA_BRL_MT * 0.20, 2)
    # The raw BRL print stays the genuine latest quote.
    assert dom["cepea_soy_brl"] == CEPEA_BRL_MT


def test_safex_basis_uses_common_date(stalled_spot_em):
    dom = soy_analytics.emerging_markets_analysis()["countries"]["South Africa"][
        "south_africa_domestic"
    ]

    expected = round(SAFEX_ZAR_MT * 0.055 - CBOT_USD_MT_STALL, 2)

    assert dom["safex_cbot_basis_usd"] == expected
    assert dom["basis_date"] == STALL_DATE
    assert dom["cbot_usd"] == round(CBOT_USD_MT_STALL, 2)
    assert dom["soybean_safex_usd"] == round(SAFEX_ZAR_MT * 0.055, 2)


def test_india_premium_uses_common_date(stalled_spot_em):
    dom = soy_analytics.emerging_markets_analysis()["countries"]["India"][
        "india_domestic"
    ]

    expected = round(MANDI_INR_MT * 0.0120 - CBOT_USD_MT_STALL, 2)

    assert dom["bean_premium_usd"] == expected
    assert dom["basis_date"] == STALL_DATE
    assert dom["cbot_bean_usd"] == round(CBOT_USD_MT_STALL, 2)
    assert dom["soybean_mandi_usd"] == round(MANDI_INR_MT * 0.0120, 2)


def test_usd_display_degrades_to_aligned_fx_when_cbot_missing(
    stalled_spot_em, monkeypatch: pytest.MonkeyPatch
):
    """No CBOT → no basis, but the USD conversion still date-aligns FX."""
    _, currencies = stalled_spot_em
    monkeypatch.setattr(soy_analytics, "load_prices", lambda: {})

    dom = soy_analytics.emerging_markets_analysis()["countries"]["India"][
        "india_domestic"
    ]

    assert "bean_premium_usd" not in dom
    assert "cbot_bean_usd" not in dom
    # Mandi INR (07-31) × INR/USD of the same date — not the fresh 0.0115.
    assert dom["soybean_mandi_usd"] == round(MANDI_INR_MT * 0.0120, 2)
