"""Tests for fetchers/export_sales.py — ESR response parsing."""

import logging
from datetime import date
from unittest.mock import patch

import pandas as pd

from config import EXPORT_SALES_COMMODITIES
from fetchers import export_sales

# Real field names as returned by the ESR API (verified against the
# OpenData swagger examples and captured API output).
ESR_EXPORT_ROW = {
    "commodityCode": 801,
    "countryCode": 5700,
    "weeklyExports": 13506,
    "accumulatedExports": 13506,
    "outstandingSales": 352846,
    "grossNewSales": 224335,
    "currentMYNetSales": 25376,
    "currentMYTotalCommitment": 366352,
    "nextMYOutstandingSales": 0,
    "nextMYNetSales": 0,
    "unitId": 1,
    "weekEndingDate": "2026-07-23T00:00:00",
}

ESR_COUNTRIES = [
    {"countryCode": 5700, "countryName": "China", "regionId": 7},
    {"countryCode": 2010, "countryName": "Mexico", "regionId": 2},
]

STORAGE_COLUMNS = [
    "week_ending", "country", "net_sales", "weekly_exports",
    "accumulated_exports", "outstanding_sales", "unit",
]


@patch.object(export_sales, "FAS_API_KEY", "test-key")
@patch.object(export_sales, "_fas_get")
def test_parses_real_esr_fields(mock_get):
    mock_get.return_value = [ESR_EXPORT_ROW]

    df = export_sales.fetch_export_sales(
        "801", 2026, {5700: "China"}, unit_map={1: "Metric Tons"}
    )

    assert list(df.columns) == STORAGE_COLUMNS
    row = df.iloc[0]
    assert row["country"] == "China"
    assert row["net_sales"] == 25376
    assert row["weekly_exports"] == 13506
    assert row["accumulated_exports"] == 13506
    assert row["outstanding_sales"] == 352846
    assert row["week_ending"] == "2026-07-23T00:00:00"
    assert row["unit"] == "Metric Tons"


@patch.object(export_sales, "FAS_API_KEY", "test-key")
@patch.object(export_sales, "_fas_get")
def test_unknown_country_code_falls_back_to_raw_code(mock_get):
    mock_get.return_value = [ESR_EXPORT_ROW]

    df = export_sales.fetch_export_sales("801", 2026, {})

    assert df.iloc[0]["country"] == "5700"


@patch.object(export_sales, "FAS_API_KEY", "test-key")
@patch.object(export_sales, "_fas_get")
def test_missing_required_field_returns_empty(mock_get):
    row = {k: v for k, v in ESR_EXPORT_ROW.items() if k != "weekEndingDate"}
    mock_get.return_value = [row]

    df = export_sales.fetch_export_sales("801", 2026, {})

    assert df.empty


@patch.object(export_sales, "FAS_API_KEY", "test-key")
@patch.object(export_sales, "_fas_get")
def test_country_map_built_from_countries_endpoint(mock_get):
    mock_get.return_value = ESR_COUNTRIES

    assert export_sales.fetch_country_map() == {5700: "China", 2010: "Mexico"}


@patch.object(export_sales, "FAS_API_KEY", "test-key")
@patch.object(export_sales, "_fas_get")
def test_api_failure_returns_empty(mock_get):
    mock_get.return_value = None

    df = export_sales.fetch_export_sales("801", 2026, {})

    assert df.empty
    assert isinstance(df, pd.DataFrame)


# ── Marketing-year rollover fallback (#181) ────────────────────────
#
# ESR answers a marketing year that has not started yet with HTTP 200 and
# an empty array.  On 1 September four of the six commodities roll at once,
# which used to drop the layer below its LAYER_MIN_KEYS floor and page CI
# for ~8-10 days.  A confirmed-empty year falls back once to MY-1.


@patch.object(export_sales, "FAS_API_KEY", "test-key")
@patch.object(export_sales, "_fas_get")
def test_empty_market_year_falls_back_to_prior_year(mock_get, caplog):
    mock_get.side_effect = lambda endpoint: (
        [ESR_EXPORT_ROW] if "marketYear/2026" in endpoint else []
    )

    with caplog.at_level(logging.WARNING):
        df = export_sales.fetch_export_sales("801", 2027, {5700: "China"}, {1: "Metric Tons"})

    assert len(df) == 1
    assert df.iloc[0]["country"] == "China"
    assert mock_get.call_count == 2
    assert "marketYear/2027" in mock_get.call_args_list[0].args[0]
    assert "marketYear/2026" in mock_get.call_args_list[1].args[0]
    assert "2027" in caplog.text and "2026" in caplog.text


@patch.object(export_sales, "FAS_API_KEY", "test-key")
@patch.object(export_sales, "_fas_get")
def test_request_failure_does_not_fall_back(mock_get):
    """None means the request failed — re-serving last year's rows would
    hide a genuine FAS outage, so no fallback request is issued."""
    mock_get.return_value = None

    df = export_sales.fetch_export_sales("801", 2027, {})

    assert df.empty
    assert mock_get.call_count == 1


@patch.object(export_sales, "FAS_API_KEY", "test-key")
@patch.object(export_sales, "_fas_get")
def test_two_empty_market_years_return_empty_without_chaining(mock_get):
    mock_get.return_value = []

    df = export_sales.fetch_export_sales("801", 2027, {})

    assert df.empty
    assert mock_get.call_count == 2


@patch.object(export_sales, "FAS_API_KEY", "test-key")
@patch.object(export_sales, "_fas_get")
def test_populated_market_year_issues_one_request(mock_get):
    mock_get.return_value = [ESR_EXPORT_ROW]

    df = export_sales.fetch_export_sales("801", 2027, {5700: "China"}, {1: "Metric Tons"})

    assert len(df) == 1
    assert mock_get.call_count == 1


@patch.object(export_sales, "FAS_API_KEY", "test-key")
@patch.object(export_sales, "_fas_get")
def test_september_rollover_keeps_all_commodities_populated(mock_get):
    """1 September: the four Sep-start commodities have no report weeks in
    the new MY yet.  With the fallback, all six still clear the floor."""
    sep_start_codes = {
        EXPORT_SALES_COMMODITIES[name]
        for name in ("Soybeans", "Soybean Oil", "Soybean Meal", "Corn")
    }

    def fake_get(endpoint):
        if endpoint == "/countries":
            return ESR_COUNTRIES
        if endpoint == "/unitsOfMeasure":
            return [{"unitId": 1, "unitNames": "Metric Tons"}]
        parts = endpoint.split("/")
        code, year = parts[3], int(parts[-1])
        if code in sep_start_codes and year == 2027:
            return []
        return [ESR_EXPORT_ROW]

    mock_get.side_effect = fake_get

    with patch.object(export_sales, "date") as mock_date:
        mock_date.today.return_value = date(2026, 9, 1)
        results = export_sales.fetch_all_export_sales()

    assert set(results) == set(EXPORT_SALES_COMMODITIES)
    assert all(not df.empty for df in results.values())
