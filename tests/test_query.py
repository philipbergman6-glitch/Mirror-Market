"""Smoke tests for pipeline.query.

Covers the three behaviours every read_* function shares:
  1. DB exists but the table is empty ⇒ empty DataFrame
  2. After a paired save_*, the read returns the rows with the right shape
     (commodity filter applied, Date column parsed to datetime)
  3. DB file is missing and is_cloud() is False ⇒ empty DataFrame

Plus a handful of dedicated tests for the freshness readers and the
table-missing branch (clear_database ⇒ read returns empty via the
try/except wrapper).
"""

from __future__ import annotations

import sqlite3

import pandas as pd
import pytest

from pipeline import query, store

# ---------------------------------------------------------------------------
# Empty-database paths
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "reader",
    [
        query.read_prices,
        query.read_economic,
        query.read_usda,
        query.read_cot,
        query.read_weather,
        query.read_crop_progress,
        query.read_psd,
        query.read_currencies,
        query.read_worldbank_prices,
        query.read_export_sales,
        query.read_forward_curve,
        query.read_dce_futures,
        query.read_wasde,
        query.read_inspections,
        query.read_eia_data,
        query.read_brazil_estimates,
        query.read_india_domestic,
        query.read_brazil_spot,
        query.read_safex,
        query.read_freshness,
        query.read_commodity_freshness,
    ],
    ids=lambda fn: fn.__name__,
)
def test_reader_empty_table_returns_empty_df(patched_db, reader):
    out = reader()
    assert isinstance(out, pd.DataFrame)
    assert out.empty


@pytest.mark.parametrize(
    "reader",
    [
        query.read_prices,
        query.read_economic,
        query.read_usda,
        query.read_cot,
        query.read_weather,
        query.read_crop_progress,
        query.read_psd,
        query.read_currencies,
        query.read_worldbank_prices,
        query.read_export_sales,
        query.read_forward_curve,
        query.read_dce_futures,
        query.read_wasde,
        query.read_inspections,
        query.read_eia_data,
        query.read_brazil_estimates,
        query.read_india_domestic,
        query.read_brazil_spot,
        query.read_safex,
        query.read_freshness,
        query.read_commodity_freshness,
    ],
    ids=lambda fn: fn.__name__,
)
def test_reader_returns_empty_when_db_file_missing(tmp_path, monkeypatch, reader):
    nonexistent = tmp_path / "does_not_exist.db"
    monkeypatch.setattr("pipeline.query.DB_PATH", str(nonexistent))
    monkeypatch.setattr("pipeline.query.is_cloud", lambda: False)
    out = reader()
    assert isinstance(out, pd.DataFrame)
    assert out.empty


# ---------------------------------------------------------------------------
# Filter + Date-conversion checks (sample a few readers)
# ---------------------------------------------------------------------------


def _price_df(dates: list[str], close: float) -> pd.DataFrame:
    idx = pd.to_datetime(dates)
    idx.name = "Date"
    return pd.DataFrame(
        {
            "Open": [close] * len(dates),
            "High": [close + 1] * len(dates),
            "Low": [close - 1] * len(dates),
            "Close": [close] * len(dates),
            "Volume": [1000.0] * len(dates),
        },
        index=idx,
    )


def test_read_prices_filter_selects_only_named_commodity(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-01"], 1200.0))
    store.save_price_data("Corn", _price_df(["2026-01-01"], 500.0))

    soy = query.read_prices("Soybeans")
    all_rows = query.read_prices()

    assert set(soy["commodity"]) == {"Soybeans"}
    assert len(all_rows) == 2
    assert set(all_rows["commodity"]) == {"Soybeans", "Corn"}


def test_read_prices_date_column_is_datetime(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-01", "2026-01-02"], 1200.0))
    out = query.read_prices("Soybeans")
    assert pd.api.types.is_datetime64_any_dtype(out["Date"])


def test_read_currencies_date_column_is_datetime(patched_db):
    idx = pd.to_datetime(["2026-01-01"])
    idx.name = "Date"
    df = pd.DataFrame(
        {"Open": [5.0], "High": [5.1], "Low": [4.9], "Close": [5.05]},
        index=idx,
    )
    store.save_currency_data("USDBRL", df)
    out = query.read_currencies("USDBRL")
    assert pd.api.types.is_datetime64_any_dtype(out["Date"])


def test_read_export_sales_week_ending_column_is_datetime(patched_db):
    df = pd.DataFrame(
        [
            {
                "week_ending": "2026-01-08",
                "country": "China",
                "net_sales": 500000.0,
                "weekly_exports": 400000.0,
                "accumulated_exports": 30000000.0,
                "outstanding_sales": 10000000.0,
            }
        ]
    )
    store.save_export_sales("Soybeans", df)
    out = query.read_export_sales("Soybeans")
    assert pd.api.types.is_datetime64_any_dtype(out["week_ending"])


def test_read_inspections_week_ending_column_is_datetime(patched_db):
    df = pd.DataFrame([{"week_ending": "2026-01-08", "inspections_mt": 1.0}])
    store.save_inspections("Soybeans", df)
    out = query.read_inspections("Soybeans")
    assert pd.api.types.is_datetime64_any_dtype(out["week_ending"])


# ---------------------------------------------------------------------------
# Table-missing branch — exercises the try/except wrappers in query.py
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "reader",
    [
        query.read_crop_progress,
        query.read_export_sales,
        query.read_forward_curve,
        query.read_wasde,
        query.read_inspections,
        query.read_eia_data,
        query.read_brazil_estimates,
        query.read_india_domestic,
        query.read_brazil_spot,
        query.read_safex,
        query.read_freshness,
        query.read_commodity_freshness,
    ],
    ids=lambda fn: fn.__name__,
)
def test_reader_returns_empty_when_table_missing(patched_db, reader):
    # clear_database drops every table; the try/except in each reader
    # should swallow the "no such table" error and return an empty DF.
    store.clear_database()
    out = reader()
    assert isinstance(out, pd.DataFrame)
    assert out.empty


# ---------------------------------------------------------------------------
# read_freshness — datetime parsing + missing-status fallback
# ---------------------------------------------------------------------------


def test_read_freshness_parses_timestamps(patched_db):
    store.save_freshness("yfinance", rows_fetched=10, status="success")
    out = query.read_freshness()

    assert len(out) == 1
    assert pd.api.types.is_datetime64_any_dtype(out["last_success"])
    assert pd.api.types.is_datetime64_any_dtype(out["last_attempt"])
    assert out["status"].iloc[0] == "success"


def test_read_freshness_fills_status_when_column_missing(patched_db):
    # Drop and re-create data_freshness WITHOUT the status column to mimic
    # a legacy DB. read_freshness should synthesise status='success'.
    conn = sqlite3.connect(str(patched_db))
    try:
        conn.execute("DROP TABLE data_freshness")
        conn.execute(
            "CREATE TABLE data_freshness ("
            " layer_name TEXT PRIMARY KEY,"
            " last_success TEXT,"
            " rows_fetched INTEGER)"
        )
        conn.execute(
            "INSERT INTO data_freshness (layer_name, last_success, rows_fetched) "
            "VALUES (?, ?, ?)",
            ("legacy_layer", "2026-01-01 00:00:00", 5),
        )
        conn.commit()
    finally:
        conn.close()

    out = query.read_freshness()
    assert len(out) == 1
    assert out["status"].iloc[0] == "success"
    assert pd.api.types.is_datetime64_any_dtype(out["last_success"])


def test_read_commodity_freshness_round_trip(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-01"], 1200.0))
    store.update_commodity_freshness()

    out = query.read_commodity_freshness()
    assert len(out) >= 1
    assert "Soybeans" in set(out["commodity"])


# ---------------------------------------------------------------------------
# Filterless reads return all rows
# ---------------------------------------------------------------------------


def test_read_cot_without_filter_returns_all_commodities(patched_db):
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2026-01-07"]),
            "commercial_long": [100.0],
            "commercial_short": [50.0],
            "commercial_net": [50.0],
            "noncommercial_long": [200.0],
            "noncommercial_short": [150.0],
            "noncommercial_net": [50.0],
            "total_open_interest": [600.0],
        }
    )
    store.save_cot_data("Soybeans", df)
    store.save_cot_data("Corn", df.copy())

    out = query.read_cot()
    assert set(out["commodity"]) == {"Soybeans", "Corn"}
