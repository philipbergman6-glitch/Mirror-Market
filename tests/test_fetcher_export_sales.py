"""Tests for fetchers/export_sales.py — ESR response parsing."""

from unittest.mock import patch

import pandas as pd

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
