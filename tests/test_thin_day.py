"""The MH thin-day rule (#370, option 2: label only, never drop or patch).

Worked examples are the real MSAMB counts from `india_domestic_prices`,
2026-11-10 → 11-23 (LAYERS.md Layer 16, count-break markers): weekdays
108–142 lots, Saturdays 83–86, Sundays 17–18, Diwali week 40 / 33.
"""

from __future__ import annotations

from datetime import date

import pytest

from analysis.thin_day import ThinDayRule, thin_day

RULE = ThinDayRule(
    count_column="Volume",
    count_noun="lots",
    since=date(2026, 11, 10),
    ratio=0.5,
    window=20,
    min_days=5,
)

# (date, lots) — the stored complete days, as printed in the history CSV.
MSAMB = [
    (date(2026, 11, 10), 40.0),   # Tue, Diwali week
    (date(2026, 11, 11), 33.0),   # Wed, Diwali week
    (date(2026, 11, 12), 108.0),
    (date(2026, 11, 13), 121.0),
    (date(2026, 11, 14), 86.0),   # Sat
    (date(2026, 11, 15), 17.0),   # Sun
    (date(2026, 11, 16), 132.0),
    (date(2026, 11, 17), 142.0),
    (date(2026, 11, 18), 131.0),
    (date(2026, 11, 19), 132.0),
    (date(2026, 11, 20), 137.0),
    (date(2026, 11, 21), 83.0),   # Sat
    (date(2026, 11, 22), 18.0),   # Sun
    (date(2026, 11, 23), 118.0),
]


def test_a_sunday_under_half_the_weekday_median_is_labelled_with_both_counts():
    # Weekdays 11-10 → 11-20 (9 days): median 131; half is 65.5; 18 is under.
    label = thin_day(MSAMB, date(2026, 11, 22), RULE)
    assert label is not None
    assert label.text == "thin day · 18 lots (typical weekday ~131)"


def test_a_saturday_above_half_the_weekday_median_carries_no_label():
    assert thin_day(MSAMB, date(2026, 11, 21), RULE) is None  # 83 ≥ 65.5


def test_a_normal_weekday_carries_no_label():
    assert thin_day(MSAMB, date(2026, 11, 23), RULE) is None


def test_no_label_until_the_baseline_holds_min_days_weekdays():
    # 11-15 has four MSAMB weekdays behind it (11-10 → 11-13): no baseline,
    # so no claim either way — the rule says nothing it cannot back.
    assert thin_day(MSAMB, date(2026, 11, 15), RULE) is None


def test_the_baseline_never_reads_across_the_count_break():
    # Agmarknet rows before 11-10 counted rows differently (LAYERS.md, #371):
    # 500-lot "weekdays" there must not raise the bar for an MSAMB day.
    before = [(date(2026, 10, d), 500.0) for d in (1, 2, 5, 6, 7, 8)]
    # Read across the break, 11-14 (86 lots) would sit under half of ~500.
    assert thin_day(before + MSAMB, date(2026, 11, 14), RULE) is None
    label = thin_day(before + MSAMB, date(2026, 11, 22), RULE)
    assert label is not None and label.typical == 131.0


def test_the_baseline_is_the_trailing_window_of_weekdays_only():
    rule = ThinDayRule(**{**RULE.__dict__, "window": 3, "min_days": 3})
    # Last three weekdays before 11-22: 11-18/19/20 → 131, 132, 137 → 132.
    label = thin_day(MSAMB, date(2026, 11, 22), rule)
    assert label is not None and label.typical == 132.0


def test_a_day_with_no_stored_count_is_never_labelled():
    rows = [*MSAMB, (date(2026, 11, 24), None)]
    assert thin_day(rows, date(2026, 11, 24), RULE) is None
    assert thin_day(MSAMB, date(2026, 11, 30), RULE) is None  # not stored at all


def test_a_baseline_day_with_no_count_is_left_out_not_read_as_zero():
    rows = [(day, None if day == date(2026, 11, 16) else n) for day, n in MSAMB]
    label = thin_day(rows, date(2026, 11, 22), RULE)
    # Weekdays without 11-16: 40, 33, 108, 121, 142, 131, 132, 137 → 126.
    assert label is not None and label.typical == 126.0


@pytest.mark.parametrize("field, value", [
    ("ratio", 0.0), ("ratio", 1.0), ("window", 0), ("min_days", 0), ("min_days", 21),
    ("count_noun", " "), ("count_column", ""),
])
def test_an_unusable_rule_is_refused_at_construction(field, value):
    with pytest.raises(ValueError):
        ThinDayRule(**{**RULE.__dict__, field: value})
