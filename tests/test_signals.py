"""Smoke tests for analysis.signals.

Each detector gets positive, negative, and guard-clause cases. Inputs
are hand-built DataFrames with the indicator columns set to exactly
the values needed to trigger (or not trigger) a signal — no reliance
on technical.py to compute them.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analysis.signals import (
    demote_near_roll_signals,
    detect_all_signals,
    detect_bollinger_squeeze,
    detect_ma_crossovers,
    detect_macd_crossover,
    detect_rsi_divergence,
    detect_rsi_extremes,
    detect_volume_spikes,
    is_near_roll,
)

REQUIRED_SIGNAL_KEYS = {"date", "commodity", "signal_type", "severity", "description"}
VALID_SEVERITIES = {"info", "warning", "alert"}


def _assert_signal_shape(sig: dict, commodity: str) -> None:
    assert REQUIRED_SIGNAL_KEYS.issubset(sig.keys())
    assert sig["severity"] in VALID_SEVERITIES
    assert sig["commodity"] == commodity


def _bday_index(n: int) -> pd.DatetimeIndex:
    idx = pd.date_range("2026-01-01", periods=n, freq="B")
    idx.name = "Date"
    return idx


# ---------------------------------------------------------------------------
# detect_ma_crossovers
# ---------------------------------------------------------------------------


def test_detect_ma_crossovers_golden_cross_20_50():
    df = pd.DataFrame(
        {"MA_20": [98.0, 99.0, 101.0], "MA_50": [100.0, 100.0, 100.0]},
        index=_bday_index(3),
    )
    signals = detect_ma_crossovers(df, "Soybeans")

    assert len(signals) == 1
    assert signals[0]["signal_type"] == "golden_cross_20_50"
    assert signals[0]["severity"] == "warning"
    _assert_signal_shape(signals[0], "Soybeans")


def test_detect_ma_crossovers_death_cross_20_50():
    df = pd.DataFrame(
        {"MA_20": [102.0, 101.0, 99.0], "MA_50": [100.0, 100.0, 100.0]},
        index=_bday_index(3),
    )
    signals = detect_ma_crossovers(df, "Corn")

    assert len(signals) == 1
    assert signals[0]["signal_type"] == "death_cross_20_50"
    assert signals[0]["severity"] == "warning"


def test_detect_ma_crossovers_50_200_alert_severity():
    df = pd.DataFrame(
        {
            "MA_20": [50.0, 50.0, 50.0],   # below MA_50 the whole time, no 20/50 cross
            "MA_50": [98.0, 99.0, 101.0],
            "MA_200": [100.0, 100.0, 100.0],
        },
        index=_bday_index(3),
    )
    signals = detect_ma_crossovers(df, "Wheat")

    types = [s["signal_type"] for s in signals]
    assert "golden_cross_50_200" in types
    for sig in signals:
        if sig["signal_type"] == "golden_cross_50_200":
            assert sig["severity"] == "alert"


def test_detect_ma_crossovers_no_signal_when_trend_continues():
    df = pd.DataFrame(
        {"MA_20": [101.0, 102.0, 103.0], "MA_50": [100.0, 100.0, 100.0]},
        index=_bday_index(3),
    )
    assert detect_ma_crossovers(df, "Soybeans") == []


def test_detect_ma_crossovers_short_df_returns_empty():
    df = pd.DataFrame(
        {"MA_20": [101.0], "MA_50": [100.0]},
        index=_bday_index(1),
    )
    assert detect_ma_crossovers(df, "Soybeans") == []


# ---------------------------------------------------------------------------
# detect_volume_spikes
# ---------------------------------------------------------------------------


def test_detect_volume_spikes_fires_on_3x_volume():
    df = pd.DataFrame(
        {"Volume": [100_000.0] * 20 + [300_000.0]},
        index=_bday_index(21),
    )
    signals = detect_volume_spikes(df, "Soybeans")

    assert len(signals) == 1
    assert signals[0]["signal_type"] == "volume_spike"
    assert signals[0]["severity"] == "info"
    _assert_signal_shape(signals[0], "Soybeans")


def test_detect_volume_spikes_no_signal_on_average_volume():
    df = pd.DataFrame(
        {"Volume": [100_000.0] * 21},
        index=_bday_index(21),
    )
    assert detect_volume_spikes(df, "Soybeans") == []


def test_detect_volume_spikes_missing_column_returns_empty():
    df = pd.DataFrame({"Close": [1.0] * 21}, index=_bday_index(21))
    assert detect_volume_spikes(df, "Soybeans") == []


def test_detect_volume_spikes_short_df_returns_empty():
    df = pd.DataFrame({"Volume": [100_000.0] * 20}, index=_bday_index(20))
    assert detect_volume_spikes(df, "Soybeans") == []


def test_detect_volume_spikes_zero_average_volume_guard():
    df = pd.DataFrame({"Volume": [0.0] * 20 + [10_000.0]}, index=_bday_index(21))
    # avg is 0 ⇒ guarded against division and returns empty.
    assert detect_volume_spikes(df, "Soybeans") == []


# ---------------------------------------------------------------------------
# detect_rsi_extremes
# ---------------------------------------------------------------------------


def test_detect_rsi_extremes_overbought():
    df = pd.DataFrame({"RSI": [50.0, 60.0, 75.0]}, index=_bday_index(3))
    signals = detect_rsi_extremes(df, "Soybeans")
    assert len(signals) == 1
    assert signals[0]["signal_type"] == "rsi_overbought"
    assert signals[0]["severity"] == "warning"


def test_detect_rsi_extremes_oversold():
    df = pd.DataFrame({"RSI": [50.0, 40.0, 25.0]}, index=_bday_index(3))
    signals = detect_rsi_extremes(df, "Soybeans")
    assert len(signals) == 1
    assert signals[0]["signal_type"] == "rsi_oversold"


def test_detect_rsi_extremes_neutral_returns_empty():
    df = pd.DataFrame({"RSI": [50.0, 50.0, 50.0]}, index=_bday_index(3))
    assert detect_rsi_extremes(df, "Soybeans") == []


def test_detect_rsi_extremes_missing_column_returns_empty():
    df = pd.DataFrame({"Close": [100.0]}, index=_bday_index(1))
    assert detect_rsi_extremes(df, "Soybeans") == []


def test_detect_rsi_extremes_nan_returns_empty():
    df = pd.DataFrame({"RSI": [50.0, 60.0, np.nan]}, index=_bday_index(3))
    assert detect_rsi_extremes(df, "Soybeans") == []


# ---------------------------------------------------------------------------
# detect_rsi_divergence
# ---------------------------------------------------------------------------


def test_detect_rsi_divergence_bearish():
    # 21 rows; price peaks at row 5 with high RSI; row 20 (current) is near
    # the price peak but with lower RSI ⇒ bearish divergence.
    n = 21
    closes = [100.0] + [105.0] * 4 + [110.0] + [104.0] * 14 + [109.0]
    rsi = [50.0] + [70.0] * 4 + [80.0] + [60.0] * 14 + [65.0]
    df = pd.DataFrame({"Close": closes, "RSI": rsi}, index=_bday_index(n))

    signals = detect_rsi_divergence(df, "Soybeans")
    types = [s["signal_type"] for s in signals]
    assert "bearish_divergence" in types


def test_detect_rsi_divergence_bullish():
    # 21 rows; price bottoms at row 5 with low RSI; row 20 (current) revisits
    # the price low but with higher RSI ⇒ bullish divergence.
    n = 21
    closes = [110.0] + [105.0] * 4 + [90.0] + [96.0] * 14 + [90.5]
    rsi = [50.0] + [30.0] * 4 + [20.0] + [40.0] * 14 + [35.0]
    df = pd.DataFrame({"Close": closes, "RSI": rsi}, index=_bday_index(n))

    signals = detect_rsi_divergence(df, "Soybeans")
    types = [s["signal_type"] for s in signals]
    assert "bullish_divergence" in types


def test_detect_rsi_divergence_no_signal_when_aligned():
    # Price and RSI both rise together — no divergence.
    n = 21
    closes = list(np.linspace(100, 120, n))
    rsi = list(np.linspace(40, 70, n))
    df = pd.DataFrame({"Close": closes, "RSI": rsi}, index=_bday_index(n))

    signals = detect_rsi_divergence(df, "Soybeans")
    assert signals == []


def test_detect_rsi_divergence_short_df_returns_empty():
    df = pd.DataFrame(
        {"Close": [100.0] * 20, "RSI": [50.0] * 20},
        index=_bday_index(20),
    )
    assert detect_rsi_divergence(df, "Soybeans") == []


# ---------------------------------------------------------------------------
# detect_macd_crossover
# ---------------------------------------------------------------------------


def test_detect_macd_crossover_bullish():
    df = pd.DataFrame(
        {"MACD": [-0.5, -0.2, 0.3], "MACD_Signal": [0.0, 0.0, 0.0]},
        index=_bday_index(3),
    )
    signals = detect_macd_crossover(df, "Soybeans")
    assert len(signals) == 1
    assert signals[0]["signal_type"] == "macd_bullish"


def test_detect_macd_crossover_bearish():
    df = pd.DataFrame(
        {"MACD": [0.5, 0.2, -0.3], "MACD_Signal": [0.0, 0.0, 0.0]},
        index=_bday_index(3),
    )
    signals = detect_macd_crossover(df, "Soybeans")
    assert len(signals) == 1
    assert signals[0]["signal_type"] == "macd_bearish"


def test_detect_macd_crossover_no_signal_when_continuing():
    df = pd.DataFrame(
        {"MACD": [0.5, 0.6, 0.7], "MACD_Signal": [0.0, 0.0, 0.0]},
        index=_bday_index(3),
    )
    assert detect_macd_crossover(df, "Soybeans") == []


def test_detect_macd_crossover_nan_returns_empty():
    df = pd.DataFrame(
        {"MACD": [np.nan, 0.2, 0.3], "MACD_Signal": [0.0, 0.0, 0.0]},
        index=_bday_index(3),
    )
    assert detect_macd_crossover(df, "Soybeans") == []


def test_detect_macd_crossover_missing_column_returns_empty():
    df = pd.DataFrame({"MACD": [0.1, 0.2]}, index=_bday_index(2))
    assert detect_macd_crossover(df, "Soybeans") == []


# ---------------------------------------------------------------------------
# detect_bollinger_squeeze
# ---------------------------------------------------------------------------


def test_detect_bollinger_squeeze_fires_at_120_day_min():
    # 120 rows; current BB_Width is the minimum of the lookback window.
    widths = [10.0] * 119 + [5.0]
    df = pd.DataFrame({"BB_Width": widths}, index=_bday_index(120))
    signals = detect_bollinger_squeeze(df, "Soybeans")

    assert len(signals) == 1
    assert signals[0]["signal_type"] == "bollinger_squeeze"
    assert signals[0]["severity"] == "info"


def test_detect_bollinger_squeeze_no_signal_when_width_high():
    # min sits at row 50; current sits well above it.
    widths = [10.0] * 50 + [5.0] + [10.0] * 68 + [15.0]
    df = pd.DataFrame({"BB_Width": widths}, index=_bday_index(120))
    assert detect_bollinger_squeeze(df, "Soybeans") == []


def test_detect_bollinger_squeeze_short_df_returns_empty():
    df = pd.DataFrame({"BB_Width": [5.0] * 119}, index=_bday_index(119))
    assert detect_bollinger_squeeze(df, "Soybeans") == []


def test_detect_bollinger_squeeze_missing_column_returns_empty():
    df = pd.DataFrame({"Close": [100.0] * 120}, index=_bday_index(120))
    assert detect_bollinger_squeeze(df, "Soybeans") == []


# ---------------------------------------------------------------------------
# detect_all_signals
# ---------------------------------------------------------------------------


def test_detect_all_signals_returns_list_for_empty_df():
    result = detect_all_signals(pd.DataFrame(), "Soybeans")
    assert result == []


def test_detect_all_signals_aggregates_multiple_detectors():
    # 21 rows: trigger 20/50 golden cross + RSI overbought + volume spike.
    # Not enough rows to trigger the Bollinger squeeze (needs 120).
    n = 21
    ma_20 = [98.0] * 20 + [101.0]
    ma_50 = [100.0] * 21
    rsi = [50.0] * 20 + [75.0]
    macd = [0.5] * 21
    macd_signal = [0.0] * 21
    volume = [100_000.0] * 20 + [300_000.0]

    df = pd.DataFrame(
        {
            "MA_20": ma_20,
            "MA_50": ma_50,
            "RSI": rsi,
            "MACD": macd,
            "MACD_Signal": macd_signal,
            "Volume": volume,
        },
        index=_bday_index(n),
    )

    signals = detect_all_signals(df, "Soybeans")
    types = {s["signal_type"] for s in signals}

    assert isinstance(signals, list)
    assert "golden_cross_20_50" in types
    assert "rsi_overbought" in types
    assert "volume_spike" in types
    for sig in signals:
        _assert_signal_shape(sig, "Soybeans")


@pytest.mark.parametrize(
    "detector",
    [
        detect_ma_crossovers,
        detect_volume_spikes,
        detect_rsi_extremes,
        detect_rsi_divergence,
        detect_macd_crossover,
        detect_bollinger_squeeze,
    ],
    ids=lambda d: d.__name__,
)
def test_each_detector_handles_empty_dataframe(detector):
    assert detector(pd.DataFrame(), "Soybeans") == []


# ---------------------------------------------------------------------------
# is_near_roll / demote_near_roll_signals
# ---------------------------------------------------------------------------
# Soybeans contract months are [1, 3, 5, 7, 8, 9, 11]. The estimated roll
# date is the first business day of each delivery month; for July 2026 that
# is Wednesday 2026-07-01.


def test_is_near_roll_on_estimated_roll_date():
    assert is_near_roll("2026-07-01", "Soybeans") is True


def test_is_near_roll_within_window_after_roll():
    # 2026-07-06 (Mon) is +3 business days from 2026-07-01 (Wed)
    assert is_near_roll("2026-07-06", "Soybeans") is True


def test_is_near_roll_within_window_before_roll():
    # 2026-06-26 (Fri) is -3 business days from 2026-07-01 (Wed)
    assert is_near_roll("2026-06-26", "Soybeans") is True


def test_is_near_roll_outside_window():
    # 2026-07-07 (Tue) is +4 business days from 2026-07-01
    assert is_near_roll("2026-07-07", "Soybeans") is False
    # Mid-April: nearest soy rolls are early March / early May (~9+ bdays away)
    assert is_near_roll("2026-04-15", "Soybeans") is False


def test_is_near_roll_unknown_commodity_returns_false():
    assert is_near_roll("2026-07-01", "Bitcoin") is False


def test_is_near_roll_accepts_pd_timestamp():
    assert is_near_roll(pd.Timestamp("2026-07-01"), "Soybeans") is True


def test_demote_near_roll_signals_demotes_warning_to_info():
    signals = [
        {
            "date": pd.Timestamp("2026-07-01"),
            "commodity": "Soybeans",
            "signal_type": "golden_cross_20_50",
            "severity": "warning",
            "description": "Soybeans golden cross",
        },
    ]
    out = demote_near_roll_signals(signals)
    assert out[0]["severity"] == "info"
    assert "(near-roll)" in out[0]["description"]


def test_demote_near_roll_signals_demotes_alert_to_info():
    signals = [
        {
            "date": pd.Timestamp("2026-07-01"),
            "commodity": "Soybeans",
            "signal_type": "golden_cross_50_200",
            "severity": "alert",
            "description": "Soybeans MAJOR golden cross",
        },
    ]
    out = demote_near_roll_signals(signals)
    assert out[0]["severity"] == "info"
    assert "(near-roll)" in out[0]["description"]


def test_demote_near_roll_signals_leaves_far_signals_unchanged():
    signals = [
        {
            "date": pd.Timestamp("2026-04-15"),
            "commodity": "Soybeans",
            "signal_type": "golden_cross_20_50",
            "severity": "warning",
            "description": "Soybeans golden cross",
        },
    ]
    out = demote_near_roll_signals(signals)
    assert out[0]["severity"] == "warning"
    assert "(near-roll)" not in out[0]["description"]


def test_demote_near_roll_signals_does_not_mutate_input():
    original = {
        "date": pd.Timestamp("2026-07-01"),
        "commodity": "Soybeans",
        "signal_type": "macd_bullish",
        "severity": "info",
        "description": "Soybeans MACD bullish crossover",
    }
    out = demote_near_roll_signals([original])
    assert original["severity"] == "info"
    assert "(near-roll)" not in original["description"]
    assert "(near-roll)" in out[0]["description"]


def test_demote_near_roll_signals_handles_missing_keys():
    # Defensive: signals missing date or commodity are passed through unchanged.
    signals = [
        {"severity": "warning", "description": "stray"},
        {"date": pd.Timestamp("2026-07-01"), "severity": "warning", "description": "no commodity"},
    ]
    out = demote_near_roll_signals(signals)
    assert out == signals
