"""Tests for the required-FX v1-vs-trusted reconciliation runner (#194).

Network-free: every test serves provider frames from a stub downloader, so
the job's own logic is exercised without touching Yahoo.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import pandas as pd
import pytest
import requests

from scripts.reconcile_fx import (
    EXIT_DIVERGED,
    EXIT_OK,
    EXIT_UNAVAILABLE,
    STATUS_DIVERGED,
    STATUS_NO_SESSION,
    STATUS_RECONCILED,
    main,
    reconcile_once,
    write_report,
)
from trust.fx_ingestion import FX_PAIRS, ticker_for

# 17:00 New York ends the FX bar; in August that is 21:00 UTC.
BEFORE_FX_CLOSE = datetime(2026, 8, 13, 19, 0, tzinfo=timezone.utc)
AFTER_FX_CLOSE = datetime(2026, 8, 13, 22, 0, tzinfo=timezone.utc)

RATES = {"BRL/USD": 0.1840, "CNY/USD": 0.1390, "INR/USD": 0.0115, "ZAR/USD": 0.0560}


def frame(rows: list[tuple[date, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Open": close - 0.0005,
                "High": close + 0.0005,
                "Low": close - 0.0010,
                "Close": close,
                "Volume": 0,
            }
            for _, close in rows
        ],
        index=pd.DatetimeIndex([pd.Timestamp(day) for day, _ in rows], name="Date"),
    )


def downloader(by_ticker: dict[str, pd.DataFrame]):
    """Serve a stored frame per ticker; an unknown ticker answers empty."""

    def download(ticker: str, period: str = "5d") -> pd.DataFrame:
        return by_ticker.get(ticker, pd.DataFrame())

    return download


def sessions(days: list[date]):
    """The same shape of history for every required pair."""
    return downloader(
        {
            ticker_for(pair): frame([(day, RATES[pair] + index * 0.0001) for index, day in enumerate(days)])
            for pair in FX_PAIRS
        }
    )


# ---------------------------------------------------------------------------
# The core promise: both paths see the same bars, over one session
# ---------------------------------------------------------------------------


def test_identical_bars_reconcile_across_both_paths() -> None:
    run = reconcile_once(
        download=sessions([date(2026, 8, 11), date(2026, 8, 12)]),
        now=AFTER_FX_CLOSE,
    )

    assert run.status == STATUS_RECONCILED
    assert run.session == date(2026, 8, 12)
    assert [item.pair for item in run.pairs] == list(FX_PAIRS)
    assert all(item.report.matched_rows == 1 for item in run.pairs)
    assert run.exit_code == EXIT_OK


def test_each_ticker_is_downloaded_once_for_both_paths() -> None:
    asked: list[str] = []
    served = sessions([date(2026, 8, 12)])

    def counting(ticker: str, period: str = "5d") -> pd.DataFrame:
        asked.append(ticker)
        return served(ticker, period)

    reconcile_once(pairs=("BRL/USD",), download=counting, now=AFTER_FX_CLOSE)

    assert asked == [ticker_for("BRL/USD")]


def test_a_run_before_the_rollover_reconciles_by_both_paths_dropping_the_bar() -> None:
    """The pilot's own subject: v1's guard and the adapter's cutoff agree."""
    run = reconcile_once(
        pairs=("BRL/USD",),
        download=sessions([date(2026, 8, 12), date(2026, 8, 13)]),
        now=BEFORE_FX_CLOSE,
    )

    assert run.status == STATUS_RECONCILED
    assert run.session == date(2026, 8, 12)
    assert run.unfinished_sessions == (("BRL/USD", date(2026, 8, 13)),)
    assert run.exit_code == EXIT_OK


def test_after_the_rollover_the_same_bar_is_priced_by_both_paths() -> None:
    run = reconcile_once(
        pairs=("BRL/USD",),
        download=sessions([date(2026, 8, 12), date(2026, 8, 13)]),
        now=AFTER_FX_CLOSE,
    )

    assert run.status == STATUS_RECONCILED
    assert run.session == date(2026, 8, 13)
    assert run.unfinished_sessions == ()


def test_no_published_session_is_not_graded_as_a_divergence() -> None:
    run = reconcile_once(download=downloader({}), now=AFTER_FX_CLOSE)

    assert run.status == STATUS_NO_SESSION
    assert run.session is None
    assert run.pairs == ()
    assert run.exit_code == EXIT_OK


# ---------------------------------------------------------------------------
# Divergence
# ---------------------------------------------------------------------------


def test_a_field_difference_is_reported_and_fails(monkeypatch) -> None:
    import trust.fx_ingestion as adapter

    original = adapter.legacy_currency_frame

    def shifted(pair: str, provider_frame, **kwargs):
        result = original(pair, provider_frame, **kwargs)
        if not result.empty:
            result.loc[result.index[-1], "Close"] = float(result.iloc[-1]["Close"]) + 0.001
        return result

    monkeypatch.setattr("scripts.reconcile_fx.legacy_currency_frame", shifted)

    run = reconcile_once(
        pairs=("BRL/USD",),
        download=sessions([date(2026, 8, 12)]),
        now=AFTER_FX_CLOSE,
    )

    assert run.status == STATUS_DIVERGED
    assert run.exit_code == EXIT_DIVERGED
    difference = run.pairs[0].report.field_differences[0]
    assert difference["field"] == "Close"


def test_a_rate_the_ledger_quarantines_is_reported_as_missing_not_hidden() -> None:
    # 5.43 is USD per BRL — the pair upside down, outside the plausibility
    # band. v1 stores it; the ledger holds it back, and the report says so.
    inverted = downloader({ticker_for("BRL/USD"): frame([(date(2026, 8, 12), 5.43)])})

    run = reconcile_once(pairs=("BRL/USD",), download=inverted, now=AFTER_FX_CLOSE)

    assert run.status == STATUS_DIVERGED
    assert run.quarantined_revision_ids
    assert [row["Date"] for row in run.pairs[0].report.missing_in_trusted] == ["2026-08-12"]


def test_a_pair_that_did_not_print_this_session_is_empty_on_both_sides() -> None:
    served = {
        ticker_for(pair): frame([(date(2026, 8, 12), RATES[pair])])
        for pair in ("BRL/USD", "ZAR/USD")
    }
    served[ticker_for("ZAR/USD")] = frame([(date(2026, 8, 11), RATES["ZAR/USD"])])

    run = reconcile_once(
        pairs=("BRL/USD", "ZAR/USD"), download=downloader(served), now=AFTER_FX_CLOSE
    )

    assert run.status == STATUS_RECONCILED
    assert run.session == date(2026, 8, 12)
    lagging = next(item for item in run.pairs if item.pair == "ZAR/USD")
    assert (lagging.legacy_rows, lagging.trusted_rows) == (0, 0)


# ---------------------------------------------------------------------------
# The report, and the job's contract with CI
# ---------------------------------------------------------------------------


def test_report_json_is_machine_readable(tmp_path) -> None:
    run = reconcile_once(
        pairs=("BRL/USD",), download=sessions([date(2026, 8, 12)]), now=AFTER_FX_CLOSE
    )
    path = write_report(run, tmp_path / "fx.json")

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["status"] == STATUS_RECONCILED
    assert payload["session"] == "2026-08-12"
    assert payload["dataset_keys"] == list(run.dataset_keys)
    assert payload["pairs"][0]["reconciliation"]["reconciled"] is True


def test_upstream_outage_exits_unavailable_not_diverged(monkeypatch, tmp_path) -> None:
    def dead(ticker: str, period: str = "5d") -> pd.DataFrame:
        raise requests.RequestException("yahoo is down")

    monkeypatch.setattr("fetchers.yfinance.download_bars", dead)

    assert main(["--output", str(tmp_path / "fx.json")]) == EXIT_UNAVAILABLE


def test_shape_drift_exits_unavailable(monkeypatch, tmp_path) -> None:
    def undated(ticker: str, period: str = "5d") -> pd.DataFrame:
        return pd.DataFrame(
            [{"Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0, "Volume": 0}],
            index=pd.DatetimeIndex([pd.NaT], name="Date"),
        )

    monkeypatch.setattr("fetchers.yfinance.download_bars", undated)

    assert main(["--output", str(tmp_path / "fx.json")]) == EXIT_UNAVAILABLE


@pytest.mark.parametrize("status", [STATUS_RECONCILED, STATUS_NO_SESSION])
def test_non_divergent_statuses_never_fail_the_job(status) -> None:
    run = reconcile_once(
        pairs=("BRL/USD",),
        download=sessions([date(2026, 8, 12)]) if status == STATUS_RECONCILED else downloader({}),
        now=AFTER_FX_CLOSE,
    )

    assert run.status == status
    assert run.exit_code == EXIT_OK


def test_the_v1_downloader_is_restored_after_the_run() -> None:
    import fetchers.yfinance as yfinance_fetcher

    before = yfinance_fetcher.download_bars
    reconcile_once(pairs=("BRL/USD",), download=sessions([date(2026, 8, 12)]), now=AFTER_FX_CLOSE)

    assert yfinance_fetcher.download_bars is before
