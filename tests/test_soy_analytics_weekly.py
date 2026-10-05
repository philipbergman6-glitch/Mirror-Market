"""Calendar-week change for series that print on weekends too."""

from __future__ import annotations

import pandas as pd

from analysis.soy_analytics import _pct_chg_calendar_days


def _rows(days: list[str], closes: list[float]) -> pd.DataFrame:
    return pd.DataFrame({"Date": pd.to_datetime(days), "Close": closes})


def test_weekly_change_reaches_back_seven_calendar_days_not_five_rows() -> None:
    """Mandi rows include Saturdays and Sundays, so five rows back is not a week."""
    rows = _rows(
        ["2026-09-27", "2026-09-28", "2026-09-29", "2026-09-30",
         "2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04"],
        [50_000, 51_000, 52_000, 53_000, 54_000, 55_000, 56_000, 55_000],
    )
    # 2026-10-04 vs 2026-09-27: 55,000 / 50,000 − 1 = +10%.
    assert _pct_chg_calendar_days(rows, 7) == 10.0


def test_weekly_change_uses_the_last_print_on_or_before_the_week_ago_date() -> None:
    """A holiday a week ago falls back to the print before it, never after."""
    rows = _rows(["2026-09-26", "2026-09-29", "2026-10-04"], [40_000, 99_000, 44_000])
    assert _pct_chg_calendar_days(rows, 7) == 10.0


def test_weekly_change_is_none_without_a_print_a_week_back() -> None:
    rows = _rows(["2026-10-01", "2026-10-04"], [50_000, 55_000])
    assert _pct_chg_calendar_days(rows, 7) is None
