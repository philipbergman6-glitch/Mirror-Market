"""Section-level integration tests for the COT and weather z-score annotations.

Verifies that the σ notation appears in the briefing output when there's enough
history to compute it, and is silently omitted when there isn't. Uses the
patched_db fixture so the section format() functions read from a real (temp)
SQLite database.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from analysis.briefing.sections import cot, weather
from pipeline import store


def _make_cot_history(n_weeks: int, *, latest_spec_net: float, seed: int = 1) -> pd.DataFrame:
    """COT history ending today. The first n-1 weeks form the baseline,
    the last row carries `latest_spec_net` so the z-score is controllable."""
    rng = np.random.default_rng(seed=seed)
    dates = pd.date_range(end=pd.Timestamp.today().normalize(), periods=n_weeks, freq="W")
    baseline_spec_net = rng.normal(loc=50_000, scale=10_000, size=n_weeks - 1)
    spec_net = np.append(baseline_spec_net, latest_spec_net)
    return pd.DataFrame({
        "Date": dates,
        "commercial_long": np.full(n_weeks, 100.0),
        "commercial_short": np.full(n_weeks, 80.0),
        "commercial_net": np.full(n_weeks, 20.0),  # constant => no z-score for commercials
        "noncommercial_long": spec_net + 150_000,
        "noncommercial_short": np.full(n_weeks, 150_000.0),
        "noncommercial_net": spec_net,
        "total_open_interest": np.full(n_weeks, 400_000.0),
    })


def test_cot_section_renders_zscore_when_history_is_sufficient(patched_db):
    df = _make_cot_history(n_weeks=156, latest_spec_net=100_000.0)
    store.save_cot_data("Soybeans", df)

    out = cot.format()

    assert "Soybeans" in out
    assert "Specs net long" in out
    assert "σ vs 3yr" in out  # σ notation should appear for specs (varying net)


def test_cot_section_omits_zscore_when_history_is_short(patched_db):
    df = _make_cot_history(n_weeks=4, latest_spec_net=100_000.0)
    store.save_cot_data("Soybeans", df)

    out = cot.format()

    assert "Soybeans" in out
    assert "Specs net long" in out
    assert "σ" not in out  # not enough baseline observations


def test_cot_section_omits_zscore_when_baseline_has_no_variance(patched_db):
    # 156 weeks but every spec_net is identical => std=0 => no z-score
    n = 156
    df = pd.DataFrame({
        "Date": pd.date_range(end=pd.Timestamp.today().normalize(), periods=n, freq="W"),
        "commercial_long": np.full(n, 100.0),
        "commercial_short": np.full(n, 80.0),
        "commercial_net": np.full(n, 20.0),
        "noncommercial_long": np.full(n, 200_000.0),
        "noncommercial_short": np.full(n, 150_000.0),
        "noncommercial_net": np.full(n, 50_000.0),
        "total_open_interest": np.full(n, 400_000.0),
    })
    store.save_cot_data("Corn", df)

    out = cot.format()

    assert "Corn" in out
    assert "σ" not in out


def test_weather_section_annotates_alerts_with_zscore(patched_db):
    today = pd.Timestamp.today().normalize()
    dates = pd.date_range(end=today, periods=90, freq="D")
    rng = np.random.default_rng(seed=2)
    # Background: mild temps and modest rain
    temp = rng.normal(loc=25.0, scale=2.0, size=90)
    precip = rng.normal(loc=2.0, scale=1.0, size=90).clip(min=0)
    # Last day: extreme heat AND heavy rain (triggers both alerts)
    temp[-1] = 42.0
    precip[-1] = 30.0
    df = pd.DataFrame({"Date": dates, "temp_max": temp, "temp_min": temp - 8, "precipitation": precip})
    store.save_weather_data("US Midwest (Iowa)", df)

    out = weather.format()

    assert "Extreme heat" in out
    assert "Heavy rain" in out
    assert "σ vs 90d" in out  # both alerts should carry the z-score annotation


def test_weather_section_omits_zscore_with_insufficient_history(patched_db):
    today = pd.Timestamp.today().normalize()
    df = pd.DataFrame({
        "Date": pd.date_range(end=today, periods=5, freq="D"),
        "temp_max": [28.0, 30.0, 25.0, 26.0, 40.0],
        "temp_min": [18.0, 20.0, 17.0, 18.0, 22.0],
        "precipitation": [5.0, 10.0, 0.5, 2.0, 25.0],
    })
    store.save_weather_data("US Midwest (Iowa)", df)

    out = weather.format()

    # Both raw alerts must still fire — the threshold logic is unchanged.
    assert "Heavy rain" in out
    assert "Extreme heat" in out
    # …but with only 5 days of history we can't compute z-scores.
    assert "σ" not in out
