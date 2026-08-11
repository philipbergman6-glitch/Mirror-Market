"""Smoke tests for the pipeline's critical-layer exit code logic.

We don't run the full pipeline (it hits the network). Instead we patch the
fetcher imports so layers behave deterministically and verify:

    - run() returns 0 when all critical layers succeed
    - run() returns 1 when a critical layer (prices) fails

The pipeline orchestration is the only thing under test here; layer-internal
behavior is covered elsewhere.
"""

from __future__ import annotations

from unittest import mock

import pandas as pd
import pytest

from pipeline.results import FetchResult


@pytest.fixture
def stub_fetchers(monkeypatch, tmp_path):
    """Replace every fetcher with a no-op that returns the right shape.

    By default everything returns empty/usable data so no layer raises.
    Tests override individual fetchers to force failure.
    """
    import config
    monkeypatch.setattr(config, "STORAGE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "mirror_market.db"))
    # run() ends with export_history(); without this it writes the stub rows
    # into the repo's real data/history/ CSVs.
    monkeypatch.setattr(config, "HISTORY_DIR", str(tmp_path / "history"))
    monkeypatch.setattr("pipeline.history.HISTORY_DIR", str(tmp_path / "history"))

    # Force local SQLite (no Turso)
    monkeypatch.setattr(config, "TURSO_DATABASE_URL", "")
    monkeypatch.setattr(config, "TURSO_AUTH_TOKEN", "")

    # Reload pipeline modules so they pick up the patched STORAGE_DIR/DB_PATH
    import importlib

    import pipeline.connection as connection
    import pipeline.query as query
    import pipeline.store as store
    importlib.reload(connection)
    importlib.reload(store)
    importlib.reload(query)

    # Reload main last so it pulls in the reloaded modules
    import main
    importlib.reload(main)

    empty_df = pd.DataFrame()
    empty_dict: dict[str, pd.DataFrame] = {}

    patches = {
        "fetch_prices": empty_dict,
        "fetch_currencies": empty_dict,
        "fetch_soybean_overview": empty_dict,
        "fetch_all_crop_progress": empty_dict,
        "fetch_all_series": empty_dict,
        "fetch_cot_recent": empty_dict,
        "fetch_all_regions": empty_dict,
        "fetch_psd_all": empty_dict,
        "fetch_worldbank_prices": empty_dict,
        "fetch_dce_futures": empty_dict,
        "fetch_all_export_sales": empty_dict,
        "fetch_all_forward_curves": empty_dict,
        "fetch_wasde_estimates": empty_dict,
        "fetch_all_eia": empty_dict,
        "fetch_crush_data": empty_df,
        "fetch_export_inspections": FetchResult.empty(),
        "fetch_conab_estimates": empty_df,
        "fetch_safex": FetchResult.empty(),
        "fetch_agrural": FetchResult.empty(),
        "fetch_noticias_agricolas": FetchResult.empty(),
        "fetch_gulf_bids": FetchResult.empty(),
        # #127: these three were added to main.py after the fixture was written
        # and were NOT stubbed — the "no network" docstring lied, and the live
        # data.gov.in call (mandi) can hang the whole suite for 25+ minutes.
        "fetch_conab_farmgate": FetchResult.empty(),
        "fetch_mandi_prices": FetchResult.empty(),
        "fetch_magyp_fob": FetchResult.empty(),
    }
    # #127 was a one-time fix; this makes it permanent. Any fetcher added to
    # main.py without a stub here makes a real network call (with retries) on
    # every test in this module, which is exactly what these stubs prevent.
    unpatched = {
        name for name in dir(main) if name.startswith("fetch_") and name not in patches
    }
    assert not unpatched, f"unstubbed fetchers would hit the network: {sorted(unpatched)}"
    for name, retval in patches.items():
        monkeypatch.setattr(main, name, mock.Mock(return_value=retval))

    return main


# Dates must be relative to today, not pinned. A layer's "success" now also
# requires recency (config.LAYER_MAX_DATA_AGE_DAYS, audit F3), so hardcoded
# 2026-01 dates would make every critical layer here read as a frozen
# upstream and fail the run — increasingly so as the calendar moves on.
def _recent_index(periods: int = 2) -> pd.DatetimeIndex:
    return pd.date_range(end=pd.Timestamp.now().normalize(), periods=periods, freq="D")


def _make_ohlcv() -> pd.DataFrame:
    return pd.DataFrame(
        {"Open": [10.0, 11.0], "High": [11.0, 12.0],
         "Low": [9.0, 10.0], "Close": [10.5, 11.5], "Volume": [100, 110]},
        index=_recent_index(),
    )


def _make_prices_at_floor() -> dict[str, pd.DataFrame]:
    """Enough non-empty tickers to clear the LAYER_MIN_KEYS floor for prices."""
    from config import LAYER_MIN_KEYS
    return {f"Commodity{i}": _make_ohlcv() for i in range(LAYER_MIN_KEYS["prices"])}


def _make_fred_at_floor() -> dict[str, pd.Series]:
    from config import LAYER_MIN_KEYS
    series = pd.Series([1.0, 2.0], index=_recent_index())
    return {f"Series{i}": series for i in range(LAYER_MIN_KEYS["fred"])}


def _make_worldbank() -> dict[str, pd.DataFrame]:
    """A recent Pink Sheet vintage — monthly, so dated to the current month."""
    dates = pd.date_range(end=pd.Timestamp.now().normalize(), periods=3, freq="MS")
    return {
        "Palm Oil": pd.DataFrame(
            {"Date": dates, "price": [900.0, 910.0, 920.0], "unit": "$/mt"}
        )
    }


def test_exit_zero_when_critical_layers_succeed(stub_fetchers, monkeypatch):
    main = stub_fetchers

    # Make prices and FRED return non-empty so both critical layers succeed
    monkeypatch.setattr(main, "fetch_prices", mock.Mock(return_value=_make_prices_at_floor()))
    monkeypatch.setattr(main, "fetch_all_series", mock.Mock(return_value=_make_fred_at_floor()))
    # World Bank now counts an empty return as a hard failure (#56): {} means
    # either the download died or the stale-file guard discarded the vintage,
    # neither of which is "nothing to publish this month". With the five
    # scraper layers already hard-failing on empty, leaving it empty here
    # would put this run at 6 and trip the MAX_FAILED_LAYERS backstop — which
    # is not what this test is about.
    monkeypatch.setattr(
        main, "fetch_worldbank_prices", mock.Mock(return_value=_make_worldbank())
    )

    assert main.run() == 0


def test_exit_one_when_prices_below_floor(stub_fetchers, monkeypatch):
    """1 of 11 tickers is an outage, not a success — floor marks prices failed."""
    main = stub_fetchers

    monkeypatch.setattr(main, "fetch_prices", mock.Mock(return_value={"Soybeans": _make_ohlcv()}))
    monkeypatch.setattr(main, "fetch_all_series", mock.Mock(return_value=_make_fred_at_floor()))

    assert main.run() == 1

    from pipeline.query import read_freshness
    df = read_freshness()
    prices_row = df[df["layer_name"] == "prices"]
    assert prices_row.iloc[0]["status"] == "failed"


def test_exit_one_when_many_layers_hard_fail(stub_fetchers, monkeypatch):
    """Critical layers pass but a sweep of hard failures trips the backstop."""
    main = stub_fetchers

    monkeypatch.setattr(main, "fetch_prices", mock.Mock(return_value=_make_prices_at_floor()))
    monkeypatch.setattr(main, "fetch_all_series", mock.Mock(return_value=_make_fred_at_floor()))
    for fetcher in (
        "fetch_cot_recent", "fetch_all_regions", "fetch_psd_all",
        "fetch_worldbank_prices", "fetch_dce_futures", "fetch_all_forward_curves",
        "fetch_conab_estimates",
    ):
        monkeypatch.setattr(main, fetcher, mock.Mock(side_effect=RuntimeError("network down")))

    assert main.run() == 1


def test_exit_one_when_prices_fail(stub_fetchers, monkeypatch):
    main = stub_fetchers

    # Force prices to raise
    monkeypatch.setattr(main, "fetch_prices", mock.Mock(side_effect=RuntimeError("yfinance down")))
    monkeypatch.setattr(main, "fetch_all_series", mock.Mock(return_value=_make_fred_at_floor()))

    assert main.run() == 1


def test_exit_one_when_fred_fails(stub_fetchers, monkeypatch):
    main = stub_fetchers

    monkeypatch.setattr(main, "fetch_prices", mock.Mock(return_value=_make_prices_at_floor()))
    monkeypatch.setattr(main, "fetch_all_series", mock.Mock(side_effect=RuntimeError("FRED down")))

    assert main.run() == 1


def test_run_writes_pipeline_status_file(stub_fetchers, monkeypatch, tmp_path):
    """The CI alerter reads this file — hard failures must be listed in it."""
    import json

    main = stub_fetchers

    monkeypatch.setattr(main, "fetch_prices", mock.Mock(return_value=_make_prices_at_floor()))
    monkeypatch.setattr(main, "fetch_all_series", mock.Mock(return_value=_make_fred_at_floor()))
    monkeypatch.setattr(main, "fetch_psd_all", mock.Mock(side_effect=RuntimeError("timeout")))
    monkeypatch.setattr(
        main, "fetch_agrural",
        mock.Mock(return_value=FetchResult.failed("page download failed")),
    )

    main.run()

    status = json.loads((tmp_path / "pipeline_status.json").read_text())
    assert "psd" in status["hard_failures"]
    assert "agrural" in status["hard_failures"]
    assert status["critical_failures"] == []
    assert "prices" in status["succeeded"]


def test_failed_layer_writes_freshness_row(stub_fetchers, monkeypatch):
    """A failed layer leaves a status='failed' row so the dashboard can render it."""
    main = stub_fetchers

    monkeypatch.setattr(main, "fetch_prices", mock.Mock(side_effect=RuntimeError("boom")))
    monkeypatch.setattr(main, "fetch_all_series", mock.Mock(return_value=_make_fred_at_floor()))

    main.run()

    from pipeline.query import read_freshness
    df = read_freshness()
    prices_row = df[df["layer_name"] == "prices"]
    assert not prices_row.empty
    assert prices_row.iloc[0]["status"] == "failed"
