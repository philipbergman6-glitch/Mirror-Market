"""A1 (#298) / B2 (#308): the one FX-date rule, as a pure function.

A home-currency leg converts at its own date's FX close, else at the newest
*prior* close at most ``FX_ALIGNMENT_MAX_GAP_DAYS`` calendar days older, else
renders blank with a reason. Never at a later-dated rate.
"""
from __future__ import annotations

from datetime import date

import pytest

import config
from pricing.fx_alignment import (
    FX_GAP_EXCEEDED,
    FX_NO_PAIR,
    FX_NO_PRIOR_RATE,
    FxAlignment,
    FxResolution,
    align_fx,
)

D = date(2026, 10, 6)  # a Tuesday
RATES = [
    (date(2026, 9, 29), 0.140),
    (date(2026, 10, 2), 0.141),   # Friday
    (date(2026, 10, 6), 0.142),
    (date(2026, 10, 7), 0.143),   # the price's own future
]


def test_the_cap_is_three_calendar_days_and_lives_in_config():
    assert config.FX_ALIGNMENT_MAX_GAP_DAYS == 3


def test_an_exact_date_rate_is_used_and_is_not_an_alignment():
    res = align_fx("CNY/USD", RATES, D)
    assert isinstance(res, FxResolution)
    assert res.ok and res.reason is None
    assert res.rate == 0.142
    assert res.alignment == FxAlignment(
        pair="CNY/USD", usd_per_unit=0.142, price_date=D, observed_on=D
    )
    assert res.alignment.gap_days == 0
    assert res.alignment.aligned is False
    assert res.alignment.label is None


def test_a_prior_close_inside_the_cap_is_used_and_labelled():
    # Monday 5 Oct: no rate; Friday 2 Oct is 3 calendar days back — the
    # weekend-plus-holiday case the cap exists for.
    res = align_fx("CNY/USD", RATES, date(2026, 10, 5))
    assert res.ok
    assert res.rate == 0.141
    assert res.alignment.observed_on == date(2026, 10, 2)
    assert res.alignment.price_date == date(2026, 10, 5)
    assert res.alignment.gap_days == 3
    assert res.alignment.aligned is True
    assert "2026-10-02" in res.alignment.label
    assert "3d" in res.alignment.label


def test_a_prior_close_older_than_the_cap_withholds_with_fx_gap_exceeded():
    # Thursday 1 Oct: newest prior is 29 Sep, 2 days — fine. Friday 2 Oct
    # removed → 3 Oct's newest prior is 29 Sep, 4 days — withheld.
    rates = [r for r in RATES if r[0] != date(2026, 10, 2)]
    res = align_fx("CNY/USD", rates, date(2026, 10, 3))
    assert not res.ok
    assert res.rate is None
    assert res.alignment is None
    assert res.reason == FX_GAP_EXCEEDED == "fx_gap_exceeded"
    assert res.gap_days == 4
    assert "2026-09-29" in res.note and "4" in res.note


def test_a_later_dated_rate_is_never_used():
    # Only rates after the price date exist: this is the fallback_to_oldest
    # case A1 killed — a price converted at a rate from its own future.
    res = align_fx("BRL/USD", [(date(2026, 10, 7), 0.19)], D)
    assert not res.ok
    assert res.rate is None
    assert res.reason == FX_NO_PRIOR_RATE
    assert res.gap_days is None


def test_no_rates_at_all_is_distinct_from_a_gap():
    res = align_fx("BRL/USD", [], D)
    assert res.reason == FX_NO_PRIOR_RATE
    assert res.gap_days is None


def test_a_market_without_a_pair_resolves_to_no_pair_not_a_crash():
    res = align_fx(None, RATES, D)
    assert res.reason == FX_NO_PAIR
    assert res.rate is None


def test_unsorted_input_is_still_the_newest_prior():
    res = align_fx("CNY/USD", list(reversed(RATES)), date(2026, 10, 5))
    assert res.alignment.observed_on == date(2026, 10, 2)


def test_a_non_positive_rate_is_rejected_not_converted():
    with pytest.raises(ValueError):
        align_fx("CNY/USD", [(D, 0.0)], D)


def test_the_cap_can_be_overridden_but_never_negative():
    assert align_fx("CNY/USD", RATES, date(2026, 10, 5), max_gap_days=2).reason == FX_GAP_EXCEEDED
    with pytest.raises(ValueError):
        align_fx("CNY/USD", RATES, D, max_gap_days=-1)


def test_to_dict_carries_both_dates():
    res = align_fx("CNY/USD", RATES, date(2026, 10, 5))
    payload = res.to_dict()
    assert payload["price_date"] == "2026-10-05"
    assert payload["observed_on"] == "2026-10-02"
    assert payload["gap_days"] == 3
    assert payload["aligned"] is True
    assert payload["reason"] is None
