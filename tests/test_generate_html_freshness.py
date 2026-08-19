"""Regression: ISSUE-002 — freshness sidebar must show non-negative ages.

Found by /qa on 2026-05-20
Report: .gstack/qa-reports/qa-report-philipbergman6-glitch.github.io-2026-05-20.md

Root cause: scripts/generate_html.py compared `datetime.now(tz=None)`
(naive local time) against `pd.to_datetime(last_success)` (naive UTC
string from pipeline.store.save_freshness). On non-UTC machines the
subtraction produced a negative timedelta, rendering as "-5h ago" in
the sidebar — visible on every dashboard page.

Fix: read with `datetime.now(timezone.utc)` + `pd.to_datetime(..., utc=True)`.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from config import PRODUCTION_LAYERS


def test_freshness_age_is_non_negative_for_past_timestamp(monkeypatch):
    """A timestamp 3 hours in the past must render with a positive age string.

    Before the fix, calling `_build_freshness_items()` on a machine where
    the system clock's local tz != UTC produced "-Nh ago" labels even
    when last_success was strictly earlier than now.
    """
    from scripts import generate_html

    past_utc = datetime.now(timezone.utc) - timedelta(hours=3)
    past_str = past_utc.strftime("%Y-%m-%d %H:%M:%S")

    df = pd.DataFrame({
        "layer_name": ["prices", "fred"],
        "last_success": [past_str, past_str],
        "last_attempt": [past_str, past_str],
        "rows_fetched": [100, 50],
        "status": ["success", "success"],
    })
    df["last_success"] = pd.to_datetime(df["last_success"])
    df["last_attempt"] = pd.to_datetime(df["last_attempt"])

    import pipeline.query as query
    monkeypatch.setattr(query, "read_freshness", lambda: df)

    items = generate_html._build_freshness_items()

    assert len(items) == len(PRODUCTION_LAYERS)
    for item in (item for item in items if item["name"] in {"prices", "fred"}):
        age_str = item["age"]
        assert not age_str.startswith("-"), (
            f"Freshness age for {item['name']!r} was {age_str!r}; "
            "a negative age indicates the timezone bug from ISSUE-002 regressed."
        )
        assert age_str.endswith("ago"), f"unexpected age format: {age_str!r}"


def test_freshness_age_handles_aware_and_naive_timestamps(monkeypatch):
    """Whether the stored timestamp parses as tz-aware or naive, age must be non-negative.

    Belt-and-suspenders: pd.to_datetime in pipeline.query may produce
    either tz-aware or tz-naive Timestamps depending on input format.
    The freshness builder must tolerate both without producing a
    negative age.
    """
    from scripts import generate_html

    past_utc = datetime.now(timezone.utc) - timedelta(hours=2)

    df = pd.DataFrame({
        "layer_name": ["prices", "fred"],
        "last_success": [past_utc, past_utc.replace(tzinfo=None)],
        "last_attempt": [past_utc, past_utc.replace(tzinfo=None)],
        "rows_fetched": [1, 1],
        "status": ["success", "success"],
    })

    import pipeline.query as query
    monkeypatch.setattr(query, "read_freshness", lambda: df)

    items = generate_html._build_freshness_items()

    assert len(items) == len(PRODUCTION_LAYERS)
    for item in (item for item in items if item["name"] in {"prices", "fred"}):
        assert not item["age"].startswith("-"), (
            f"{item['name']!r}: age {item['age']!r} is negative — "
            "tz handling regressed for "
            f"{'aware' if item['name'] == 'prices' else 'naive'} input."
        )


def test_freshness_handles_writer_format_against_non_utc_local_clock(monkeypatch):
    """The exact failure mode from ISSUE-002: writer stamps naive UTC, reader runs in non-UTC tz.

    Reproduces the production state on 2026-05-11. `pipeline.store.save_freshness`
    writes `datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")` — a
    naive string in UTC. The old reader used `datetime.now(tz=None)` which
    returns naive *local* time. On a non-UTC dev machine, this produced
    negative ages.

    To make this deterministic on any CI runner, we patch the module-level
    `datetime` to a class whose `now()` returns a fixed value for both
    naive (local) and aware (UTC) calls, simulating a machine where local
    time is 5h behind UTC. The bug-buggy code would compute a -3h age;
    the fixed code computes +2h.
    """
    from scripts import generate_html as gh

    stored_utc = datetime(2026, 5, 20, 9, 0, 0, tzinfo=timezone.utc)
    stored_naive_str = stored_utc.strftime("%Y-%m-%d %H:%M:%S")

    df = pd.DataFrame({
        "layer_name": ["prices"],
        "last_success": pd.to_datetime([stored_naive_str]),
        "last_attempt": pd.to_datetime([stored_naive_str]),
        "rows_fetched": [100],
        "status": ["success"],
    })

    import pipeline.query as query
    monkeypatch.setattr(query, "read_freshness", lambda: df)

    fake_aware_utc = datetime(2026, 5, 20, 11, 0, 0, tzinfo=timezone.utc)
    fake_naive_local = datetime(2026, 5, 20, 6, 0, 0)  # CDT-like: UTC - 5h

    class FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return fake_naive_local
            return fake_aware_utc.astimezone(tz)

    monkeypatch.setattr(gh, "datetime", FakeDatetime)

    items = gh._build_freshness_items()
    assert len(items) == len(PRODUCTION_LAYERS)
    age_str = next(item for item in items if item["name"] == "prices")["age"]
    assert age_str == "2h ago", (
        f"expected '2h ago' (stored=09:00 UTC, now=11:00 UTC), got {age_str!r}. "
        "If this returned '-3h ago', the reader is using naive local time again."
    )
