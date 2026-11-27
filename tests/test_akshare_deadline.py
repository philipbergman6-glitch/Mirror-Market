"""AKShare calls carry a wall-clock deadline (#689).

Run 45802035610 (2026-11-26 10:30Z): ``ak.futures_zh_daily_sina("Y0")`` never
answered — AKShare calls ``requests.get`` with no timeout — and the 15-minute
job timeout cancelled the whole dce refresh. A hung call must count as a
failed attempt, and the layer must give up inside its budget, so the run
grades the gap instead of being killed by it.
"""

from __future__ import annotations

import logging
import threading
import time

import pandas as pd
import pytest

from fetchers import akshare


@pytest.fixture
def hang(monkeypatch):
    """``futures_zh_daily_sina`` blocks until the test ends; no backoff sleeps."""
    release = threading.Event()
    calls: list[str] = []

    def hung(symbol):
        calls.append(symbol)
        release.wait(10)
        return pd.DataFrame({"date": ["2026-11-26"], "close": [1.0]})

    monkeypatch.setattr(akshare.ak, "futures_zh_daily_sina", hung)
    monkeypatch.setattr(akshare, "retry_sleep", lambda attempt: None)
    monkeypatch.setattr(akshare, "AKSHARE_CALL_TIMEOUT", 0.05)
    yield calls
    release.set()


def test_a_hung_call_times_out_retries_and_returns_empty(hang, caplog):
    started = time.monotonic()
    with caplog.at_level(logging.WARNING, logger="fetchers.akshare"):
        df = akshare.fetch_one("Y0")
    assert time.monotonic() - started < 2
    assert df.empty
    assert hang == ["Y0"] * akshare.MAX_RETRIES
    assert "no answer within 0.05s" in caplog.text
    assert any(r.levelno == logging.ERROR and "Y0" in r.getMessage() for r in caplog.records)


def test_a_layer_whose_budget_is_spent_skips_the_rest_loudly(hang, monkeypatch, caplog):
    """Every call hangs: the fetch stops at its budget, naming what it skipped."""
    monkeypatch.setattr(akshare, "DCE_FETCH_BUDGET_S", 0.12)
    started = time.monotonic()
    with caplog.at_level(logging.ERROR, logger="fetchers.akshare"):
        frames = akshare.fetch_dce_futures()
    assert time.monotonic() - started < 2
    assert set(frames) == set(akshare.DCE_CONTRACTS)
    assert all(df.empty for df in frames.values())
    assert len(hang) < len(akshare.DCE_CONTRACTS) * akshare.MAX_RETRIES
    assert "budget" in caplog.text


def test_an_answering_call_is_unchanged(monkeypatch):
    frame = pd.DataFrame({"date": ["2026-11-26"], "close": [7800.0]})
    monkeypatch.setattr(akshare.ak, "futures_zh_daily_sina", lambda symbol: frame)
    pd.testing.assert_frame_equal(akshare.fetch_one("B0"), frame)


def test_an_exception_inside_the_call_still_reaches_the_retry_loop(monkeypatch):
    calls: list[str] = []

    def broken(symbol):
        calls.append(symbol)
        raise KeyError("schema drift")

    monkeypatch.setattr(akshare.ak, "futures_zh_daily_sina", broken)
    monkeypatch.setattr(akshare, "retry_sleep", lambda attempt: None)
    assert akshare.fetch_one("M0").empty
    assert calls == ["M0"] * akshare.MAX_RETRIES
