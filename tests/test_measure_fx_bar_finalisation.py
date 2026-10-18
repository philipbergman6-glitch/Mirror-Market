"""The B28 (#502) measurement script: samples and the change summary, offline."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from scripts.measure_fx_bar_finalisation import PAIRS, sample, summarise


def _bars(closes: dict[str, float]) -> pd.DataFrame:
    return pd.DataFrame(
        {"Open": closes.values(), "High": closes.values(), "Low": closes.values(), "Close": closes.values()},
        index=pd.DatetimeIndex(pd.to_datetime(list(closes)), name="Date"),
    )


def test_sample_marks_the_bar_the_fx_rule_has_just_closed() -> None:
    now = datetime(2026, 10, 14, 23, 5, tzinfo=timezone.utc)  # 00:05 BST on 10-15
    frame = _bars({"2026-10-13": 0.19, "2026-10-14": 0.191, "2026-10-15": 0.192})
    rows = sample(now, lambda ticker: frame, pairs=("BRL/USD",))
    assert [(row["label"], row["newest_settled"]) for row in rows] == [
        ("2026-10-13", "False"),
        ("2026-10-14", "True"),
        ("2026-10-15", "False"),
    ]
    assert rows[0]["sampled_at_london"] == "2026-10-15T00:05:00+01:00"


def test_a_weekend_live_row_is_never_the_measured_bar() -> None:
    now = datetime(2026, 10, 18, 10, 37, tzinfo=timezone.utc)  # Sunday
    frame = _bars({"2026-10-15": 0.198, "2026-10-16": 0.196, "2026-10-17": 0.197})
    rows = sample(now, lambda ticker: frame, pairs=("BRL/USD",))
    assert [row["label"] for row in rows if row["newest_settled"] == "True"] == ["2026-10-16"]


def test_an_empty_pair_raises_rather_than_reading_as_stable() -> None:
    now = datetime(2026, 10, 14, 23, 5, tzinfo=timezone.utc)
    with pytest.raises(RuntimeError, match="no bars"):
        sample(now, lambda ticker: pd.DataFrame(), pairs=PAIRS[:1])


def test_summary_names_a_changed_close_and_a_stable_one() -> None:
    rows = [
        {"pair": "BRL/USD", "label": "2026-10-14", "sampled_at_london": "2026-10-15T00:05:00+01:00", "close": "0.191"},
        {"pair": "BRL/USD", "label": "2026-10-14", "sampled_at_london": "2026-10-15T00:30:00+01:00", "close": "0.1912"},
        {"pair": "ZAR/USD", "label": "2026-10-14", "sampled_at_london": "2026-10-15T00:05:00+01:00", "close": "0.06"},
        {"pair": "ZAR/USD", "label": "2026-10-14", "sampled_at_london": "2026-10-15T00:30:00+01:00", "close": "0.06"},
    ]
    assert summarise(rows) == [
        "BRL/USD 2026-10-14 (2 samples, 2026-10-15T00:05:00+01:00 .. 2026-10-15T00:30:00+01:00): "
        "0.191 -> 0.1912 between 2026-10-15T00:05:00+01:00 and 2026-10-15T00:30:00+01:00",
        "ZAR/USD 2026-10-14 (2 samples, 2026-10-15T00:05:00+01:00 .. 2026-10-15T00:30:00+01:00): stable at 0.06",
    ]
