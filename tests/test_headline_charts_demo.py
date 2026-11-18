"""Headline §10 technicals and §09 seasonal — the two defects a QA pass found.

1. Volume (contracts, ~10⁵) shared the USD/MT price axis (~450), so price
   drew as a flat line at zero. Volume now rides its own overlaid axis.
   The named series' adjustment note sat at paper y=1.08 inside a 40px top
   margin — clipped — and is now the figure title with automargin.
2. The seasonal analyst read the technicals frame, which since the named
   ratio-adjusted series switched on is ~4 years of *adjusted* levels: under
   SEASONAL_MIN_YEARS_PER_MONTH in every month, and not prices anyway. Every
   leg rendered a bare header. It now reads the raw provider closes (the
   series the briefing's seasonal line already uses), and a leg that still
   has no norm says why instead of rendering nothing.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from app.charts import build_technical_chart
from app.sections import seasonal_section
from app.templating import site_environment
from config import SEASONAL_MIN_YEARS_PER_MONTH


def _frame(n: int = 300, *, ohlc: bool = True) -> pd.DataFrame:
    idx = pd.bdate_range("2025-09-01", periods=n)
    close = pd.Series(np.linspace(430, 470, n), index=idx)
    cols = {
        "Close": close, "Volume": np.linspace(150_000, 312_000, n),
        "MA_20": close, "MA_50": close, "MA_200": close,
        "BB_Upper": close + 5, "BB_Lower": close - 5,
        "RSI": 50.0, "MACD": 0.0, "MACD_Signal": 0.0, "MACD_Histogram": 0.0,
    }
    if ohlc:
        cols |= {"Open": close - 1, "High": close + 2, "Low": close - 2}
    return pd.DataFrame(cols, index=idx)


def _volume(fig):
    return next(t for t in fig.data if t.name == "Volume")


# ---------------------------------------------------------------------------
# 1. Technicals: volume off the price axis; note inside the figure
# ---------------------------------------------------------------------------
def test_volume_never_shares_the_usd_mt_price_axis():
    for ohlc in (True, False):
        fig = build_technical_chart(_frame(ohlc=ohlc), "Soybeans")
        price_axes = {t.yaxis for t in fig.data if t.name in ("Price", "Close", "MA_20")}
        assert price_axes == {"y"}, price_axes
        vol = _volume(fig)
        assert vol.yaxis != "y"
        axis = fig.layout[vol.yaxis.replace("y", "yaxis")]
        assert axis.overlaying == "y"          # same row as price, own scale
        assert axis.showticklabels is False    # contracts are not USD/MT
        # Tucked under the bottom ~fifth of the price pane, never above it.
        assert axis.range[0] == 0 and axis.range[1] >= 312_000 * 4


def test_named_series_with_gaps_in_volume_still_draws_its_volume():
    df = _frame(ohlc=False)
    df["Volume"] = df["Volume"].astype(object)
    df.iloc[:10, df.columns.get_loc("Volume")] = None
    vol = _volume(build_technical_chart(df, "Soybeans"))
    assert vol.yaxis != "y"


def test_a_series_with_no_volume_at_all_draws_no_volume_trace():
    df = _frame(ohlc=False)
    df["Volume"] = None
    fig = build_technical_chart(df, "Soybeans")
    assert not [t for t in fig.data if t.name == "Volume"]


def test_adjustment_note_is_laid_out_in_an_auto_sized_margin():
    df = _frame(ohlc=False)
    note = (
        "rolled 5 business days before each front contract's last trade date and "
        "ratio-adjusted at each roll — returns and volatility are continuous, but "
        "levels before the last roll are NOT tradeable prices"
    )
    df.attrs["adjustment_note"] = note
    fig = build_technical_chart(df, "Soybeans")
    # Not a paper-coordinate annotation floating above a fixed 40px margin.
    assert not [a for a in fig.layout.annotations if "rolled" in (a.text or "")]
    title = fig.layout.title
    assert title.automargin is True
    flat = title.text.replace("<br>", " ")
    assert "NOT tradeable prices" in flat and "ratio-adjusted" in flat
    # Wrapped, so a long method sentence is not one 1,000-px line.
    assert all(len(line) <= 120 for line in title.text.split("<br>"))


# ---------------------------------------------------------------------------
# 2. Seasonal: the raw provider series, and a reason where there is no norm
# ---------------------------------------------------------------------------
def _daily(years: float) -> pd.DataFrame:
    idx = pd.bdate_range(end="2026-11-13", periods=int(years * 252))
    day = np.arange(len(idx))
    close = 10.0 + np.sin(day / 40.0)  # cents/bu-ish scale, any positive level
    return pd.DataFrame({
        "Open": close, "High": close, "Low": close, "Close": close, "Volume": 1.0,
    }, index=idx)


def test_seasonal_analyst_reads_the_raw_provider_closes(monkeypatch):
    from analysis import soy_analytics

    raw = {leg: _daily(15) for leg in ("Soybeans", "Soybean Oil", "Soybean Meal")}
    # The technicals frame: the short ratio-adjusted named series.
    adjusted = {leg: _daily(4) for leg in raw}

    def fake_load_prices(*, with_technicals: bool = False):
        return adjusted if with_technicals else raw

    monkeypatch.setattr(soy_analytics, "load_prices", fake_load_prices)
    out = soy_analytics.seasonal_analysis()
    assert set(out) == set(raw)
    for leg in out.values():
        assert leg["vs_seasonal"], "15y of raw closes must yield a seasonal norm"
        assert not leg["monthly"].empty


def _short_leg() -> dict:
    return {"monthly": pd.DataFrame(), "vs_seasonal": {}, "unit": "USD/MT", "as_of": "2026-11-13"}


def test_a_leg_with_no_seasonal_norm_carries_a_withheld_reason():
    result = seasonal_section({"Soybeans": _short_leg()})
    assert result["state"] == "ok"
    leg = result["data"]["legs"][0]
    assert leg["chart_html"] == "" and not leg["has_stats"]
    assert str(SEASONAL_MIN_YEARS_PER_MONTH) in leg["withheld_reason"]


def test_a_leg_with_a_profile_but_no_current_month_norm_says_so():
    monthly = pd.DataFrame({
        "month": [1, 2], "avg_close": [400.0, 410.0], "min_close": [380.0, 390.0],
        "max_close": [420.0, 430.0], "n_years": [5, 5], "avg_dev_pct": [0.1, -0.1],
    })
    leg_in = {**_short_leg(), "monthly": monthly}
    leg = seasonal_section({"Soybeans": leg_in})["data"]["legs"][0]
    assert leg["chart_html"]
    assert "this calendar month" in leg["withheld_reason"].lower()


def test_a_full_leg_carries_no_withheld_reason():
    monthly = pd.DataFrame({
        "month": [11], "avg_close": [400.0], "min_close": [380.0],
        "max_close": [420.0], "n_years": [15], "avg_dev_pct": [0.1],
    })
    vs = {"current_price": 450.0, "seasonal_avg": 400.0, "deviation_pct": 12.5,
          "detrended_delta_pct": 2.0}
    leg = seasonal_section({"Soybeans": {**_short_leg(), "monthly": monthly, "vs_seasonal": vs}})
    assert leg["data"]["legs"][0]["withheld_reason"] == ""


def test_seasonal_template_prints_the_reason_under_the_leg_header():
    s = seasonal_section({"Soybeans": _short_leg()})["data"]
    html = site_environment().get_template("sections/seasonal.html.j2").render(s=s)
    assert "Soybeans" in html
    assert s["legs"][0]["withheld_reason"] in html
    assert "alert-warn" in html
