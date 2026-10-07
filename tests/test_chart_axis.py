"""Daily bars serialise as dates, not microsecond timestamps (#400)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from app.charts import axis_dates, build_technical_chart


def _daily_frame(n: int = 504) -> pd.DataFrame:
    idx = pd.bdate_range("2024-10-03", periods=n)
    close = pd.Series(np.linspace(400, 480, n), index=idx)
    return pd.DataFrame({
        "Open": close - 1, "High": close + 2, "Low": close - 2, "Close": close,
        "Volume": 1000, "MA_20": close, "MA_50": close, "MA_200": close,
        "BB_Upper": close + 5, "BB_Lower": close - 5,
        "RSI": 50.0, "MACD": 0.0, "MACD_Signal": 0.0, "MACD_Histogram": 0.0,
    })


def test_daily_index_becomes_plain_date_strings():
    out = axis_dates(pd.bdate_range("2026-01-05", periods=3))
    assert out == ["2026-01-05", "2026-01-06", "2026-01-07"]


def test_an_index_with_a_time_component_is_passed_through():
    idx = pd.DatetimeIndex(["2026-01-05 09:30", "2026-01-05 10:30"])
    assert axis_dates(idx) is idx


def test_a_timezone_aware_index_is_passed_through():
    idx = pd.date_range("2026-01-05", periods=2, tz="America/Chicago")
    assert axis_dates(idx) is idx


def test_a_non_datetime_index_is_passed_through():
    idx = pd.Index(["a", "b"])
    assert axis_dates(idx) is idx
    empty = pd.DatetimeIndex([])
    assert axis_dates(empty) is empty


def test_every_technical_trace_rides_the_compact_axis():
    fig = build_technical_chart(_daily_frame(), "Soybeans")
    xs = [list(trace.x) for trace in fig.data]
    assert len(xs) >= 11
    assert all(x[0] == "2024-10-03" and len(x[0]) == 10 for x in xs)
    assert all(isinstance(v, str) for x in xs for v in x[:3])


def test_compact_axis_cuts_the_serialised_figure_by_at_least_a_third():
    """The measured live figure was 255 KB, 156 KB of it timestamps."""
    df = _daily_frame()
    fig = build_technical_chart(df, "Soybeans")
    compact = len(fig.to_json())
    # The same figure with the encoding Plotly used before: a DatetimeIndex
    # on every trace, serialised as microsecond timestamps.
    for trace in fig.data:
        trace.x = df.index
    verbose = len(fig.to_json())
    assert compact < verbose * 2 / 3, (compact, verbose)
