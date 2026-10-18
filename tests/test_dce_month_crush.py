"""B29 #503 — the Dalian crush is struck on ONE delivery month, or withheld.

The defect, from V1 #474 (PR #499, edition 2026-10-17): the displayed Dalian
crush, CNY 49, was B2611 beans against Y2701 oil and M2701 meal. The continuous
B0/Y0/M0 series roll on their own schedules, so the "margin" embedded a
Nov→Jan calendar spread and said nothing about it. The same-month figures that
day were Nov +9.13 and Jan −0.53.

The closes below are the DCE 2026-10-16 session as Sina served it on
2026-10-18 (and as V1 read it off DCE's own dayQuotes): every expected margin
here is a worked example from those prints, never recomputed the way the code
computes it.
"""

from __future__ import annotations

from datetime import date

import pytest

from analysis.futures.crush import (
    CrushWithheld,
    MonthCrush,
    MonthQuote,
    WithheldReason,
    crush_months,
    strike_month_crush,
)
from analysis.spreads import CRUSH_MEAL_YIELD_MT, CRUSH_OIL_YIELD_MT

SESSION = date(2026, 10, 16)
YIELDS = {"oil": CRUSH_OIL_YIELD_MT, "meal": CRUSH_MEAL_YIELD_MT}
# Months all three DCE legs list (No.2 lists every month; meal/oil 1,3,5,7,8,9,11,12).
DCE_MONTHS = (1, 3, 5, 7, 8, 9, 11, 12)

# (contract, close, volume) on 2026-10-16.
OCT16 = {
    "bean": [("B2611", 3978.0, 154218.0), ("B2701", 4028.0, 35022.0)],
    "oil": [("Y2611", 8776.0, 11128.0), ("Y2701", 8844.0, 312470.0)],
    "meal": [("M2611", 3243.0, 128772.0), ("M2701", 3281.0, 1234349.0)],
}


def _month(contract: str) -> str:
    return f"20{contract[1:3]}-{contract[3:5]}"


def _quotes(table=OCT16, session=SESSION) -> dict[str, list[MonthQuote]]:
    return {
        role: [
            MonthQuote(session=session, contract=c, delivery_month=_month(c), close=close, volume=vol)
            for c, close, vol in rows
        ]
        for role, rows in table.items()
    }


def test_crush_months_are_the_months_every_leg_lists():
    roster = {
        "bean": tuple(range(1, 13)),
        "oil": (1, 3, 5, 7, 8, 9, 11, 12),
        "meal": (1, 3, 5, 7, 8, 9, 11, 12),
    }
    assert crush_months(roster) == DCE_MONTHS


def test_crush_months_refuses_a_roster_with_no_shared_month():
    with pytest.raises(ValueError, match="no delivery month"):
        crush_months({"bean": (1,), "oil": (2,), "meal": (2,)})


def test_the_nearest_month_all_three_legs_traded_is_the_strike():
    result = strike_month_crush(_quotes(), crush_months=DCE_MONTHS)
    assert isinstance(result, MonthCrush)
    assert result.month == "2026-11"
    assert result.session == SESSION
    assert {role: q.contract for role, q in result.legs.items()} == {
        "bean": "B2611", "oil": "Y2611", "meal": "M2611",
    }
    # Worked example (V1 row 11): 11/60 × 8776 + 44/60 × 3243 − 3978 = 9.13.
    assert result.margin(YIELDS) == pytest.approx(9.1333, abs=1e-3)


def test_the_month_is_labelled_inline():
    result = strike_month_crush(_quotes(), crush_months=DCE_MONTHS)
    assert result.label == "Nov-26 crush"


def test_nov_beans_and_jan_products_never_blend():
    """The 2026-10-16 shape exactly: beans held only in Nov, products only in Jan.

    The blended number this used to print was 49.47 CNY/MT. Either a single
    month strikes or the result is withheld — here no month has all three legs,
    so it is withheld and the reason names both months.
    """
    table = {
        "bean": [("B2611", 3978.0, 154218.0)],
        "oil": [("Y2701", 8844.0, 312470.0)],
        "meal": [("M2701", 3281.0, 1234349.0)],
    }
    result = strike_month_crush(_quotes(table), crush_months=DCE_MONTHS)
    assert isinstance(result, CrushWithheld)
    assert result.code is WithheldReason.NO_CRUSH_MONTH
    assert "Nov-26" in result.reason and "Y2611" in result.reason
    assert result.to_dict()["margin_usd_mt"] is None


def test_a_month_one_leg_did_not_trade_falls_to_the_next_month():
    table = {
        "bean": [("B2611", 3978.0, 154218.0), ("B2701", 4028.0, 35022.0)],
        # Y2611 printed a bar with no volume — not a trade on this session.
        "oil": [("Y2611", 8776.0, 0.0), ("Y2701", 8844.0, 312470.0)],
        "meal": [("M2611", 3243.0, 128772.0), ("M2701", 3281.0, 1234349.0)],
    }
    result = strike_month_crush(_quotes(table), crush_months=DCE_MONTHS)
    assert isinstance(result, MonthCrush)
    assert result.label == "Jan-27 crush"
    # 11/60 × 8844 + 44/60 × 3281 − 4028 = −0.53 (V1 row 11, same-month Jan).
    assert result.margin(YIELDS) == pytest.approx(-0.5333, abs=1e-3)
    assert any("Nov-26" in line and "Y2611" in line for line in result.considered)


def test_a_null_volume_is_not_a_trade():
    """NULL is "never learned" — it cannot prove the leg printed."""
    table = {role: [(c, close, None) for c, close, _ in rows] for role, rows in OCT16.items()}
    result = strike_month_crush(_quotes(table), crush_months=DCE_MONTHS)
    assert isinstance(result, CrushWithheld)
    assert result.code is WithheldReason.NO_CRUSH_MONTH


def test_a_month_whose_delivery_has_begun_is_never_the_strike():
    """DCE's last trade falls inside the delivery month and no DCE calendar is
    encoded, so the delivery month's first day is the conservative bound."""
    november = date(2026, 11, 2)
    result = strike_month_crush(_quotes(session=november), crush_months=DCE_MONTHS)
    assert isinstance(result, MonthCrush)
    assert result.month == "2027-01"


def test_a_month_not_every_leg_lists_is_skipped():
    """Feb: No.2 lists it, meal and oil do not — it is never a crush month."""
    table = {
        "bean": [("B2702", 4000.0, 10.0)],
        "oil": [("Y2703", 8800.0, 10.0)],
        "meal": [("M2703", 3200.0, 10.0)],
    }
    result = strike_month_crush(_quotes(table, session=date(2027, 1, 20)), crush_months=DCE_MONTHS)
    assert isinstance(result, CrushWithheld)
    assert "B2703" in result.reason


def test_legs_that_share_no_session_are_withheld():
    quotes = _quotes()
    quotes["oil"] = [
        MonthQuote(session=date(2026, 10, 15), contract=q.contract,
                   delivery_month=q.delivery_month, close=q.close, volume=q.volume)
        for q in quotes["oil"]
    ]
    result = strike_month_crush(quotes, crush_months=DCE_MONTHS)
    assert isinstance(result, CrushWithheld)
    assert result.code is WithheldReason.MIXED_SESSIONS


def test_a_missing_leg_is_withheld_not_substituted():
    quotes = _quotes()
    quotes["meal"] = []
    result = strike_month_crush(quotes, crush_months=DCE_MONTHS)
    assert isinstance(result, CrushWithheld)
    assert result.code is WithheldReason.NO_CURVE
    assert "meal" in result.reason


def test_the_newest_common_session_is_used_never_an_older_complete_one():
    """Falling back to an older session would print a stale margin as today's."""
    older = _quotes(session=date(2026, 10, 15))
    newest = {
        "bean": [("B2611", 3978.0, 154218.0)],
        "oil": [("Y2701", 8844.0, 312470.0)],
        "meal": [("M2701", 3281.0, 1234349.0)],
    }
    quotes = {role: older[role] + _quotes(newest)[role] for role in older}
    result = strike_month_crush(quotes, crush_months=DCE_MONTHS)
    assert isinstance(result, CrushWithheld)
    assert SESSION.isoformat() in result.reason
