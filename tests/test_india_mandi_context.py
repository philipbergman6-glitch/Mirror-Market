"""India mandi context: arrivals pace and the MSP reference (Layer 16)."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from analysis.briefing.sections.emerging_markets import _format_india_domestic
from analysis.soy_analytics import _india_msp_on, _mandi_arrivals_pace
from app.sections import _india_group


def _rows(days: int, end: str, arrivals: float | None = 100.0) -> pd.DataFrame:
    dates = pd.date_range(end=end, periods=days, freq="D")
    return pd.DataFrame({
        "Date": dates,
        "Close": 56_000.0,
        "arrivals_mt": [arrivals] * days,
    })


def test_pace_sums_the_seven_days_before_the_newest_and_compares_the_prior_seven() -> None:
    """The newest day is still filling with late uploads (MH 10-05 was first
    stored from 83 lots, 122 a day later), so the window ends the day before."""
    rows = _rows(15, "2026-10-06")
    rows.loc[rows["Date"] >= "2026-09-29", "arrivals_mt"] = 150.0
    rows.loc[rows["Date"] == "2026-10-06", "arrivals_mt"] = 1.0      # still filling

    pace = _mandi_arrivals_pace(rows)

    assert pace == {
        "arrivals_7d_mt": 1050.0,
        "arrivals_7d_end": "2026-10-05",
        "arrivals_7d_chg_pct": 50.0,
    }


def test_pace_is_withheld_when_a_day_in_its_week_has_no_arrivals() -> None:
    """Rows stored before arrivals existed are NULL — summing around them
    would understate the week and read as a real slowdown."""
    rows = _rows(15, "2026-10-06")
    rows.loc[rows["Date"] == "2026-10-01", "arrivals_mt"] = None
    assert _mandi_arrivals_pace(rows) is None


def test_an_unknown_day_in_the_prior_week_withholds_only_the_change() -> None:
    rows = _rows(15, "2026-10-06")
    rows.loc[rows["Date"] == "2026-09-25", "arrivals_mt"] = None
    assert _mandi_arrivals_pace(rows) == {
        "arrivals_7d_mt": 700.0, "arrivals_7d_end": "2026-10-05",
    }


def test_pace_without_a_prior_week_carries_no_change() -> None:
    pace = _mandi_arrivals_pace(_rows(8, "2026-10-06"))
    assert pace == {"arrivals_7d_mt": 700.0, "arrivals_7d_end": "2026-10-05"}


def test_pace_counts_a_day_with_no_rows_as_no_arrivals() -> None:
    """The 31-day re-read stores every date the report carries, so a date
    inside it with no row is a day no mandi reported soybean (Diwali 2025
    never had one, but a holiday could) — zero tonnes, not unknown."""
    rows = _rows(15, "2026-10-06")
    rows = rows[rows["Date"] != "2026-10-02"]
    assert _mandi_arrivals_pace(rows)["arrivals_7d_mt"] == 600.0


def test_pace_without_the_column_is_withheld() -> None:
    assert _mandi_arrivals_pace(_rows(15, "2026-10-06").drop(columns="arrivals_mt")) is None


@pytest.mark.parametrize(
    ("day", "expected"),
    [
        (date(2025, 9, 30), None),             # before the schedule: not modelled
        (date(2025, 10, 1), 53_280.0),          # KMS 2025-26 opens
        (date(2026, 9, 30), 53_280.0),
        (date(2026, 10, 1), 57_080.0),          # KMS 2026-27
        (date(2027, 3, 1), 57_080.0),
    ],
)
def test_msp_is_the_marketing_season_in_force_on_the_mandi_date(day, expected) -> None:
    found = _india_msp_on(day)
    assert (found[0] if found else None) == expected


def test_india_group_renders_msp_and_arrivals_cards() -> None:
    group = _india_group({"india_domestic": {
        "soybean_mandi_inr": 57_750.0,
        "soybean_mandi_date": "2026-10-06",
        "msp_inr": 57_080.0,
        "msp_season": "2026-27",
        "mandi_vs_msp_pct": 1.17,
        "arrivals_7d_mt": 238_000.0,
        "arrivals_7d_end": "2026-10-05",
        "arrivals_7d_chg_pct": 31.2,
    }})
    cards = {c["label"]: c for c in group["cards"]}

    assert cards["vs MSP"]["value"] == 1.17
    assert "₹57,080/MT" in cards["vs MSP"]["caption"]
    assert "2026-27" in cards["vs MSP"]["caption"]
    assert cards["Arrivals, 7 days"]["value"] == 238_000.0
    assert cards["Arrivals, 7 days"]["delta"] == "+31.2% vs prior 7d"
    assert "to 2026-10-05" in cards["Arrivals, 7 days"]["caption"]


def test_india_group_omits_cards_it_has_no_number_for() -> None:
    group = _india_group({"india_domestic": {"soybean_mandi_inr": 57_750.0}})
    labels = {c["label"] for c in group["cards"]}
    assert "vs MSP" not in labels and "Arrivals, 7 days" not in labels


def test_briefing_prints_msp_and_arrivals_lines() -> None:
    text = _format_india_domestic({"India": {"india_domestic": {
        "soybean_mandi_inr": 57_750.0,
        "msp_inr": 57_080.0,
        "msp_season": "2026-27",
        "mandi_vs_msp_pct": 1.17,
        "arrivals_7d_mt": 238_000.0,
        "arrivals_7d_end": "2026-10-05",
        "arrivals_7d_chg_pct": 31.2,
    }}})
    assert "MSP 2026-27: ₹57,080/MT — MP median +1.2% vs MSP" in text
    assert "MP arrivals, 7d to 2026-10-05: 238,000 MT (+31.2% vs prior 7d)" in text
