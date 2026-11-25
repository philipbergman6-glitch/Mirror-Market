"""The #622 boundary sampler: samples, schedule and verdict, offline."""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from scripts.measure_cbot_bar_finality import (
    DEFAULT_TIMES,
    boundary,
    sample,
    schedule,
    summarise,
)

UTC = timezone.utc
NY = ZoneInfo("America/New_York")


def _bars(closes: dict[str, float]) -> pd.DataFrame:
    values = list(closes.values())
    return pd.DataFrame(
        {"Open": values, "High": values, "Low": values, "Close": values, "Volume": [100.0] * len(values)},
        index=pd.DatetimeIndex(pd.to_datetime(list(closes)), name="Date"),
    )


FRAME = _bars({"2026-11-23": 1000.0, "2026-11-24": 1001.0, "2026-11-25": 1002.0})


def test_the_boundary_is_the_guards_own_new_york_midnight():
    assert boundary(date(2026, 11, 24)) == datetime(2026, 11, 25, 0, 0, tzinfo=NY)
    assert boundary(date(2026, 7, 14)).astimezone(UTC) == datetime(2026, 7, 15, 4, 0, tzinfo=UTC)


def test_a_sample_before_midnight_marks_d_unfinished_and_after_it_finished():
    before = datetime(2026, 11, 24, 23, 50, tzinfo=NY)
    after = datetime(2026, 11, 25, 0, 5, tzinfo=NY)
    rows_before = {r["label"]: r for r in sample(before, lambda t: FRAME, tickers=("ZS=F",))}
    rows_after = {r["label"]: r for r in sample(after, lambda t: FRAME, tickers=("ZS=F",))}
    assert rows_before["2026-11-24"]["final_by_rule"] == "False"
    assert rows_before["2026-11-24"]["offset_minutes"] == "-10.0"
    assert rows_after["2026-11-24"]["final_by_rule"] == "True"
    assert rows_after["2026-11-24"]["offset_minutes"] == "5.0"
    assert rows_after["2026-11-24"]["volume"] == "100.0"


def test_an_empty_ticker_raises_rather_than_reading_as_stable():
    with pytest.raises(RuntimeError):
        sample(datetime(2026, 11, 25, 5, 5, tzinfo=UTC), lambda t: FRAME.iloc[0:0], tickers=("ZS=F",))


def test_a_naive_clock_is_refused():
    with pytest.raises(ValueError):
        sample(datetime(2026, 11, 25, 5, 5), lambda t: FRAME, tickers=("ZS=F",))


def _row(offset: float, close: str, ticker: str = "ZS=F", label: str = "2026-11-24") -> dict[str, str]:
    return {"ticker": ticker, "label": label, "offset_minutes": str(offset), "close": close}


def test_a_bar_final_at_midnight_supports_the_earlier_slot():
    rows = [_row(-10, "1001.25"), _row(0, "1001.5"), _row(5, "1001.5"), _row(15, "1001.5"),
            _row(60, "1001.5")]
    lines = summarise(rows)
    assert "final by +00:00" in lines[0]
    assert lines[-2].startswith("VERDICT: no Close changed after +00:00 over 1")
    assert "would have read the final Close on every" in lines[-1]


def test_a_change_after_midnight_is_named_and_blocks_the_earlier_slot():
    rows = [_row(-10, "1001.25"), _row(0, "1001.5"), _row(5, "1001.5"), _row(15, "1001.5"),
            _row(30, "1001.75"), _row(60, "1001.75")]
    lines = summarise(rows)
    assert "1001.5 -> 1001.75 between +00:15 and +00:30" in lines[0]
    assert "final by +00:30" in lines[0]
    assert "NOT proven final" in lines[-2]
    assert lines[-1] == "A slot at +00:15 would have read a non-final Close for: ZS=F 2026-11-24."


def test_bars_never_sampled_across_their_boundary_are_not_evidence():
    lines = summarise([_row(30, "1"), _row(60, "1"), _row(-30, "2", label="2026-11-25")])
    assert lines == ["VERDICT: no bar was sampled on both sides of its boundary yet."]


def test_the_week_schedule_keeps_each_23_50_with_the_midnight_after_it():
    times = [time.fromisoformat(t) for t in DEFAULT_TIMES.split(",")]
    start = datetime(2026, 11, 24, 18, 0, tzinfo=NY)
    targets = schedule(times, 2, start)
    assert len(targets) == 12
    assert targets[0] == datetime(2026, 11, 24, 23, 50, tzinfo=NY)
    assert targets[1] == datetime(2026, 11, 25, 0, 0, tzinfo=NY)
    assert targets[5] == datetime(2026, 11, 25, 1, 0, tzinfo=NY)
    assert targets[6] == datetime(2026, 11, 25, 23, 50, tzinfo=NY)
    assert targets == sorted(targets)


def test_a_start_after_midnight_samples_the_rest_of_that_night_first():
    times = [time.fromisoformat(t) for t in DEFAULT_TIMES.split(",")]
    start = datetime(2026, 11, 25, 0, 10, tzinfo=NY)
    targets = schedule(times, 1, start)
    assert targets == [datetime(2026, 11, 25, h, m, tzinfo=NY) for h, m in ((0, 15), (0, 30), (1, 0))]


def test_zero_nights_is_refused():
    with pytest.raises(ValueError):
        schedule([time(0, 0)], 0, datetime(2026, 11, 25, tzinfo=UTC))
