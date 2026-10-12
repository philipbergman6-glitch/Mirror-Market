"""Market-drivers narrative rules (#404): interpretation never printed as observation.

Economic counterexamples, not arithmetic — each test is a market state the
old rules mis-described:

- 1 t China + 1 t other was "China buying pace strong" (share ≠ pace)
- a hog price move was "expanding herds" (liquidation lifts hog prices too)
- biodiesel production growth was a renewable-diesel claim (distinct EIA fuels)
- price up + weather alert was "weather premium building" (correlation as cause)
- net long + high RSI was "crowded" with no historical-extreme requirement
"""

from __future__ import annotations

import re

import numpy as np
import pandas as pd
import pytest

from analysis.briefing.sections import market_drivers

_SESSIONS = pd.bdate_range("2026-09-01", periods=10)
_WEEK = pd.Timestamp("2026-10-01")


def _enriched(weekly_chg: float, rsi: float | None = None) -> pd.DataFrame:
    df = pd.DataFrame({"Close": [1000.0] * len(_SESSIONS)}, index=_SESSIONS)
    df["weekly_pct_change"] = weekly_chg
    if rsi is not None:
        df["RSI"] = rsi
    return df


@pytest.fixture
def quiet(monkeypatch):
    """Every DB read empty; tests opt individual reads back in."""
    empty = pd.DataFrame()
    for name in (
        "read_cot", "read_weather", "read_export_sales", "read_forward_curve",
        "read_eia_data", "read_brazil_estimates", "read_psd", "read_economic",
        "read_wasde",
    ):
        monkeypatch.setattr(market_drivers, name, lambda *a, **k: empty)
    monkeypatch.setattr(market_drivers, "read_dce_futures", lambda commodity=None: empty)
    return monkeypatch


def _driver_blocks(text: str) -> list[list[str]]:
    """Split the section into numbered drivers, each a list of its lines."""
    blocks: list[list[str]] = []
    for line in text.splitlines()[1:]:
        if re.match(r"^  \d+\. ", line):
            blocks.append([line])
        elif blocks:
            blocks[-1].append(line)
    return blocks


# ── export sales: share ≠ pace ───────────────────────────────────────────

def _es_rows(weeks: list[tuple[str, float, float]], *, committed=None) -> pd.DataFrame:
    """(week, china_net, other_net) rows; committed = (accumulated, outstanding) per row."""
    rows = []
    for week, china, other in weeks:
        acc, out = committed or (np.nan, np.nan)
        rows.append({"commodity": "Soybeans", "week_ending": pd.Timestamp(week),
                     "country": "China, Peoples Republic of", "net_sales": china,
                     "accumulated_exports": acc, "outstanding_sales": out})
        rows.append({"commodity": "Soybeans", "week_ending": pd.Timestamp(week),
                     "country": "Mexico", "net_sales": other,
                     "accumulated_exports": acc, "outstanding_sales": out})
    return pd.DataFrame(rows)


def test_one_tonne_each_is_not_a_strong_pace(quiet):
    quiet.setattr(market_drivers, "read_export_sales",
                  lambda *a, **k: _es_rows([("2026-10-01", 1.0, 1.0)]))

    text = market_drivers.format({}, {}, {})

    assert "China" in text  # the observation is still printed
    assert "strong" not in text.lower()
    assert "bullish" not in text.lower()


def _record_china_week(prior_china: float) -> pd.DataFrame:
    """Latest week: China 1.5 MMT but only 25 % of a 6 MMT total."""
    weeks = [(f"2026-{m:02d}-{d:02d}", prior_china, 1_000_000.0)
             for m, d in ((8, 6), (8, 13), (8, 20), (8, 27), (9, 3), (9, 10), (9, 17), (9, 24))]
    weeks.append(("2026-10-01", 1_500_000.0, 4_500_000.0))
    return _es_rows(weeks, committed=(10_000_000.0, 20_000_000.0))


def _wasde_with_year_ago(year_ago_pct_of_forecast: float) -> pd.DataFrame:
    # 2026/27 forecast 1,800 mbu ≈ 48.99 MMT; committed 30 MMT ≈ 61 %.
    return pd.DataFrame([
        {"commodity": "SOYBEANS", "year": "2026/27", "attribute": "Exports",
         "value": 1800.0, "unit": "million bushels", "reference_period": "2026-09"},
        {"commodity": "SOYBEANS", "year": "2025/26", "attribute": "Exports",
         "value": 1800.0, "unit": "million bushels", "reference_period": "2026-09"},
    ])


def _with_year_ago_committed(es: pd.DataFrame, committed_mt: float) -> pd.DataFrame:
    yr_ago = pd.DataFrame([{
        "commodity": "Soybeans", "week_ending": pd.Timestamp("2025-10-02"),
        "country": "China, Peoples Republic of", "net_sales": 1.0,
        "accumulated_exports": committed_mt / 2, "outstanding_sales": committed_mt / 2,
    }])
    return pd.concat([es, yr_ago], ignore_index=True)


def test_record_absolute_china_with_revised_forecasts_withholds_pace(quiet):
    # High absolute sales and a revised year-ago denominator cannot prove
    # the share of the forecast actually available at the historical week.
    es = _with_year_ago_committed(_record_china_week(prior_china=500_000.0), 20_000_000.0)
    quiet.setattr(market_drivers, "read_export_sales", lambda *a, **k: es)
    quiet.setattr(market_drivers, "read_wasde", lambda *a, **k: _wasde_with_year_ago(41.0))

    text = market_drivers.format({}, {}, {})

    assert "25%" in text
    assert "strong" not in text.lower()
    assert "availability" in text
    block = next(b for b in _driver_blocks(text) if "China" in b[0])
    assert any(line.lstrip().startswith("Not interpreted:") for line in block)


def test_record_absolute_china_at_25pct_share_prints_observation_when_pace_fails(quiet):
    # Same week, but the trailing eight weeks were just as large → not a pace.
    es = _with_year_ago_committed(_record_china_week(prior_china=1_500_000.0), 20_000_000.0)
    quiet.setattr(market_drivers, "read_export_sales", lambda *a, **k: es)
    quiet.setattr(market_drivers, "read_wasde", lambda *a, **k: _wasde_with_year_ago(41.0))

    text = market_drivers.format({}, {}, {})

    assert "1,500,000" in text and "25%" in text
    assert "strong" not in text.lower()


def test_china_pace_withheld_with_reason_when_forecast_missing(quiet):
    es = _record_china_week(prior_china=500_000.0)  # no WASDE, no year-ago rows
    quiet.setattr(market_drivers, "read_export_sales", lambda *a, **k: es)

    text = market_drivers.format({}, {}, {})

    assert "strong" not in text.lower()
    assert "not assessed" in text.lower()


# ── livestock: a price move is not a herd ────────────────────────────────

def test_hog_price_up_four_percent_says_nothing_about_herds(quiet):
    text = market_drivers.format({}, {"Lean Hogs": _enriched(4.0)}, {})

    assert "Lean Hogs" in text
    assert "herd" not in text.lower()
    assert "meal demand" not in text.lower()


# ── positioning: crowded needs a historical extreme ──────────────────────

def _cot(net_history: list[float], latest_net: float) -> pd.DataFrame:
    dates = pd.date_range(end="2026-09-29", periods=len(net_history) + 1, freq="7D")
    return pd.DataFrame({
        "commodity": "Soybeans", "Date": dates,
        "noncommercial_net": net_history + [latest_net],
    })


def test_net_long_at_40th_percentile_with_rsi_75_is_not_crowded(quiet):
    # 100 weeks spread 0..99k; latest 40k sits at the 40th percentile.
    quiet.setattr(market_drivers, "read_cot",
                  lambda *a, **k: _cot([float(i * 1000) for i in range(100)], 40_000.0))

    text = market_drivers.format({}, {"Soybeans": _enriched(0.0, rsi=75.0)}, {})

    assert "crowded" not in text.lower()


def test_net_long_at_historical_extreme_with_rsi_75_is_crowded(quiet):
    quiet.setattr(market_drivers, "read_cot",
                  lambda *a, **k: _cot([float(i * 1000) for i in range(100)], 150_000.0))

    text = market_drivers.format({}, {"Soybeans": _enriched(0.0, rsi=75.0)}, {})

    assert "crowded long" in text.lower()
    assert "percentile" in text.lower()


def test_short_history_never_calls_crowded(quiet):
    quiet.setattr(market_drivers, "read_cot",
                  lambda *a, **k: _cot([1000.0, 2000.0, 3000.0], 150_000.0))

    text = market_drivers.format({}, {"Soybeans": _enriched(0.0, rsi=75.0)}, {})

    assert "crowded" not in text.lower()


# ── biodiesel ≠ renewable diesel ─────────────────────────────────────────

def test_biodiesel_growth_makes_no_renewable_diesel_claim(quiet):
    eia = pd.DataFrame({
        "series_name": ["Biodiesel Production"] * 2,
        "Date": pd.to_datetime(["2026-07-01", "2026-08-01"]),
        "value": [100.0, 110.0],
    })
    quiet.setattr(market_drivers, "read_eia_data", lambda *a, **k: eia)

    text = market_drivers.format({}, {}, {})

    assert "Biodiesel production" in text
    assert "renewable diesel pulling" not in text.lower()
    assert "bullish" not in text.lower()


# ── weather: coincidence is not a cause ──────────────────────────────────

def test_price_up_with_weather_alert_is_not_a_premium(quiet, monkeypatch):
    weather = pd.DataFrame({"region": ["Iowa"], "Date": [pd.Timestamp("2026-09-30")]})
    quiet.setattr(market_drivers, "read_weather", lambda *a, **k: weather)

    class _Alert:
        rule = market_drivers.RULE_HEAT
    class _Assessed:
        alerts = [_Alert()]
    monkeypatch.setattr(market_drivers, "assess_region", lambda region, subset: _Assessed())

    text = market_drivers.format({}, {"Soybeans": _enriched(2.5)}, {})

    assert "Iowa" in text
    assert "premium building" not in text.lower()


# ── every driver carries the three parts, distinguishably ────────────────

def test_every_driver_separates_observation_interpretation_falsifier(quiet):
    es = _with_year_ago_committed(_record_china_week(prior_china=500_000.0), 20_000_000.0)
    quiet.setattr(market_drivers, "read_export_sales", lambda *a, **k: es)
    quiet.setattr(market_drivers, "read_wasde", lambda *a, **k: _wasde_with_year_ago(41.0))
    quiet.setattr(market_drivers, "read_cot",
                  lambda *a, **k: _cot([float(i * 1000) for i in range(100)], 150_000.0))
    enriched = {
        "Soybeans": _enriched(5.0, rsi=75.0), "Corn": _enriched(0.0),
        "Lean Hogs": _enriched(4.0),
    }
    brl = pd.DataFrame({"Close": [5.0] * 5 + [5.2]}, index=pd.bdate_range("2026-09-22", periods=6))

    text = market_drivers.format({}, enriched, {"BRL/USD": brl})

    blocks = _driver_blocks(text)
    assert len(blocks) >= 5
    for block in blocks:
        tails = [line.lstrip() for line in block[1:]]
        assert any(t.startswith(("Reads as:", "Not interpreted:")) for t in tails), block
        assert any(t.startswith("Confirm / refute:") for t in tails), block


@pytest.mark.parametrize("values", [[None, None], [1.0, None]])
def test_commitments_require_every_destination_component(values):
    rows = pd.DataFrame({"accumulated_exports": values, "outstanding_sales": [100.0, 200.0]})
    assert market_drivers._committed_mt(rows) is None


def test_known_zero_commitments_are_zero():
    assert market_drivers._committed_mt(pd.DataFrame({
        "accumulated_exports": [0.0], "outstanding_sales": [0.0],
    })) == 0.0


def test_reference_period_is_not_publication_evidence(quiet):
    es = _with_year_ago_committed(_record_china_week(500_000.0), 20_000_000.0)
    quiet.setattr(market_drivers, "read_wasde", lambda *a: _wasde_with_year_ago(41.0))
    passed, reason = market_drivers.assess_china_pace("Soybeans", es, _WEEK, 1_500_000.0)
    assert passed is None
    assert "availability" in reason


@pytest.mark.parametrize("value", [0.0, -1.0, 1e308, float("inf"), float("nan")])
def test_forecast_denominator_must_be_finite_positive(value):
    wasde = _wasde_with_year_ago(41.0)
    wasde.loc[wasde.year == "2026/27", "value"] = value
    assert market_drivers._wasde_export_forecast_mt(wasde, "2026/27") is None


@pytest.mark.parametrize("historical", [False, True])
@pytest.mark.parametrize("column", ["accumulated_exports", "outstanding_sales"])
def test_missing_component_withholds_current_and_historical_pace(quiet, historical, column):
    es = _with_year_ago_committed(_record_china_week(500_000.0), 20_000_000.0)
    target = pd.Timestamp("2025-10-02") if historical else _WEEK
    es.loc[es.index[es.week_ending == target][0], column] = np.nan
    quiet.setattr(market_drivers, "read_wasde", lambda *a: _wasde_with_year_ago(41.0))
    passed, reason = market_drivers.assess_china_pace("Soybeans", es, _WEEK, 1_500_000.0)
    assert passed is None
    assert "commitments" in reason


def test_later_wasde_revision_cannot_change_asof_interpretation(quiet):
    es = _with_year_ago_committed(_record_china_week(500_000.0), 20_000_000.0)
    outcomes = []
    for revised in (900.0, 1800.0):
        wasde = _wasde_with_year_ago(41.0)
        wasde.loc[wasde.year == "2025/26", "value"] = revised
        quiet.setattr(market_drivers, "read_wasde", lambda *a, frame=wasde: frame)
        outcomes.append(market_drivers.assess_china_pace("Soybeans", es, _WEEK, 1_500_000.0))
    assert outcomes[0] == outcomes[1]
    assert outcomes[0][0] is None
