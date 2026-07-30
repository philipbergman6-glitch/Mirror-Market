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
    }
    for name, retval in patches.items():
        monkeypatch.setattr(main, name, mock.Mock(return_value=retval))

    return main


def _make_ohlcv() -> pd.DataFrame:
    idx = pd.to_datetime(["2026-01-01", "2026-01-02"])
    return pd.DataFrame(
        {"Open": [10.0, 11.0], "High": [11.0, 12.0],
         "Low": [9.0, 10.0], "Close": [10.5, 11.5], "Volume": [100, 110]},
        index=idx,
    )


def _make_prices_at_floor() -> dict[str, pd.DataFrame]:
    """Enough non-empty tickers to clear the LAYER_MIN_KEYS floor for prices."""
    from config import LAYER_MIN_KEYS
    return {f"Commodity{i}": _make_ohlcv() for i in range(LAYER_MIN_KEYS["prices"])}


def _make_fred_at_floor() -> dict[str, pd.Series]:
    from config import LAYER_MIN_KEYS
    series = pd.Series([1.0, 2.0], index=pd.to_datetime(["2026-01-01", "2026-01-02"]))
    return {f"Series{i}": series for i in range(LAYER_MIN_KEYS["fred"])}


def test_exit_zero_when_critical_layers_succeed(stub_fetchers, monkeypatch):
    main = stub_fetchers

    # Make prices and FRED return non-empty so both critical layers succeed
    monkeypatch.setattr(main, "fetch_prices", mock.Mock(return_value=_make_prices_at_floor()))
    monkeypatch.setattr(main, "fetch_all_series", mock.Mock(return_value=_make_fred_at_floor()))

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
