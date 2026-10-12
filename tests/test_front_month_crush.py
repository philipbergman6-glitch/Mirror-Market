"""The command-center front-month crush tile names its months (diagnosis 2026-10-12).

ZS=F / ZM=F / ZL=F roll on their own calendars: from mid-September to
mid-December the bean leg is a different delivery month from the products, so
the continuous-series "crush" embeds a calendar spread. The tile keeps its
same-session date (the site promotion probe reads it) but never calls a
mixed-month or near-roll number "profitable".
"""

from __future__ import annotations

import pandas as pd

from analysis.soy_analytics import front_month_crush


def _frame(last: str, close: float, sessions: int = 8) -> pd.DataFrame:
    idx = pd.bdate_range(end=last, periods=sessions)
    return pd.DataFrame({"Close": [close] * sessions}, index=idx)


def _legs(last: str):
    # 1000 c/bu beans, 50 c/lb oil, 300 $/st meal → crush = 550 + 660 − 1000 = +210 c/bu
    return _frame(last, 1000.0), _frame(last, 50.0), _frame(last, 300.0)


def test_one_delivery_month_off_the_roll_reads_a_verdict():
    crush = front_month_crush(*_legs("2026-08-04"))
    assert crush["available"] is True
    assert crush["months_aligned"] is True
    assert crush["contracts"] == {"Soybeans": "ZSQ26", "Soybean Meal": "ZMQ26", "Soybean Oil": "ZLQ26"}
    assert crush["profitable"] is True
    assert crush["as_of"] == "2026-08-04"


def test_november_beans_against_october_products_reads_no_verdict():
    crush = front_month_crush(*_legs("2026-10-09"))
    assert crush["available"] is True
    assert crush["as_of"] == "2026-10-09"  # still one session: the promotion probe holds
    assert crush["months_aligned"] is False
    assert crush["contracts"]["Soybeans"] == "ZSX26"
    assert crush["contracts"]["Soybean Meal"] == "ZMV26"
    assert crush["profitable"] is None
    assert "ZSX26" in crush["verdict_withheld"] and "ZMV26" in crush["verdict_withheld"]


def test_a_session_near_the_roll_reads_no_verdict():
    """Around the 15th the continuous series may or may not have rolled yet."""
    crush = front_month_crush(*_legs("2026-07-14"))
    assert crush["profitable"] is None
    assert "roll" in crush["verdict_withheld"]


def _tile(crush: dict) -> dict:
    from scripts import generate_html

    view = generate_html._build_command_center({"legs": [], "crush": crush, "key_metrics": {}})
    assert view is not None
    return next(m for m in view["key_metrics"] if m.get("derived") == "crush")


def test_the_tile_prints_the_mixed_month_reason_instead_of_a_verdict():
    tile = _tile(front_month_crush(*_legs("2026-10-09")))
    assert "Profitable" not in tile["delta"] and "Negative" not in tile["delta"]
    assert "mixed delivery months" in tile["delta"]
    assert tile["as_of"] == "2026-10-09"  # the promotion probe's aligned date survives


def test_the_tile_keeps_its_verdict_on_one_delivery_month():
    tile = _tile(front_month_crush(*_legs("2026-08-04")))
    assert tile["delta"] == "Profitable"
    assert tile["delta_class"] == "up"
