"""Tests for the FetchResult / ScraperShapeError types."""

from __future__ import annotations

import pandas as pd
import pytest

from pipeline.results import FetchResult, ScraperShapeError


def test_ok_constructor_marks_status_and_keeps_data() -> None:
    df = pd.DataFrame({"Close": [1.0, 2.0]})
    result = FetchResult.ok({"Soybean": df})

    assert result.status == "ok"
    assert result.error is None
    assert result.total_rows == 2
    assert result.has_rows is True


def test_empty_constructor_signals_zero_rows_but_no_error() -> None:
    result = FetchResult.empty("no inspections this week")

    assert result.status == "empty"
    assert result.error == "no inspections this week"
    assert result.data == {}
    assert result.total_rows == 0
    assert result.has_rows is False


def test_failed_constructor_carries_error_string() -> None:
    result = FetchResult.failed("HTTP 503 from upstream")

    assert result.status == "failed"
    assert result.error == "HTTP 503 from upstream"
    assert result.data == {}
    assert result.has_rows is False


def test_has_rows_is_false_when_all_frames_are_empty() -> None:
    result = FetchResult(
        data={"Soybean": pd.DataFrame(), "Corn": pd.DataFrame()},
        status="ok",
    )
    assert result.has_rows is False
    assert result.total_rows == 0


def test_scraper_shape_error_is_a_valueerror() -> None:
    with pytest.raises(ValueError):
        raise ScraperShapeError("missing 'price' column")

    with pytest.raises(ScraperShapeError):
        raise ScraperShapeError("row count 0 below floor")
