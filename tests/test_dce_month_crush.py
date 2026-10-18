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

import config
from analysis.futures.crush import (
    CrushWithheld,
    MonthCrush,
    MonthQuote,
    WithheldReason,
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
    """No.2 lists every month; meal and oil 1,3,5,7,8,9,11,12 (Sina, 2026-10-18)."""
    assert config.DCE_CRUSH_MONTHS == DCE_MONTHS
    assert config.MARKETS["dalian"]["crush"]["crush_months"] == DCE_MONTHS


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


# ---------------------------------------------------------------------------
# End to end: stored rows → block 03, the headline card, Origins (B29 #503)
# ---------------------------------------------------------------------------
TODAY = date(2026, 10, 18)


@pytest.fixture
def dalian_db(tmp_path, monkeypatch):
    """A DB holding the 2026-10-16 DCE session as named months, plus its CNY rate."""
    import sqlite3

    from app import markets as markets_mod
    from pipeline import schema

    path = tmp_path / "dalian.db"
    conn = sqlite3.connect(str(path))
    for ddl in (schema._CREATE_DCE_FUTURES, schema._CREATE_DCE_CONTRACT_BARS,
                schema._CREATE_CURRENCIES, schema._CREATE_PRICES):
        conn.execute(ddl)
    conn.execute("INSERT INTO currencies (pair, Date, Close) VALUES (?,?,?)",
                 ("CNY/USD", SESSION.isoformat(), 0.149772))
    conn.commit()
    monkeypatch.setattr(markets_mod, "get_connection", lambda: sqlite3.connect(str(path)))
    monkeypatch.setattr(config, "DB_PATH", str(path))
    try:
        yield conn
    finally:
        conn.close()


_KEYS = {"bean": "DCE Soybean No.2", "oil": "DCE Soybean Oil", "meal": "DCE Soybean Meal"}


def _store(conn, table=OCT16, session=SESSION):
    for role, rows in table.items():
        for contract, close, volume in rows:
            conn.execute(
                "INSERT OR REPLACE INTO dce_contract_bars (commodity, contract, delivery_month, "
                "Date, Close, Volume) VALUES (?,?,?,?,?,?)",
                (_KEYS[role], contract, _month(contract), session.isoformat(), close, volume),
            )
    conn.commit()


def _crush_block(conn):
    from app.block_builders import SiteContext, build_blocks
    from app.markets import load_markets

    registry = load_markets()
    ctx = SiteContext(conn=conn, today=TODAY)
    return next(
        b for b in build_blocks(registry["dalian"], None, ctx, markets=registry) if b.id == "crush"
    )


def _render(block) -> str:
    from app.templating import site_environment

    return site_environment().get_template("blocks/03_crush.html.j2").render(block=block)


def test_block_03_prints_the_nov_crush_and_labels_its_month(dalian_db):
    _store(dalian_db)
    block = _crush_block(dalian_db)
    assert block.state == "ok", block.reason
    assert block.data["period_label"] == "Nov-26 crush"
    assert block.data["margin_home"] == pytest.approx(9.1333, abs=1e-3)
    assert {leg["key"] for leg in block.data["legs"].values()} == {"B2611", "Y2611", "M2611"}
    html = _render(block)
    assert "Nov-26 crush" in html
    assert "B2611" in html and "Y2611" in html and "M2611" in html


def test_block_03_with_nov_beans_and_jan_products_is_withheld_never_blended(dalian_db):
    """The exact 2026-10-16 mix the continuous series produced: no 49 CNY, a reason."""
    _store(dalian_db, {
        "bean": [("B2611", 3978.0, 154218.0)],
        "oil": [("Y2701", 8844.0, 312470.0)],
        "meal": [("M2701", 3281.0, 1234349.0)],
    })
    block = _crush_block(dalian_db)
    assert block.state == "empty"
    assert "calendar spread" in block.reason
    assert "Y2611" in block.reason and "B2701" in block.reason


def test_the_headline_card_labels_the_month_inline(dalian_db):
    from app.block_builders import SiteContext, headline_crush_board
    from app.markets import load_markets

    _store(dalian_db)
    _state, _reason, data = headline_crush_board(load_markets(), SiteContext(conn=dalian_db, today=TODAY))
    card = next(row for row in data["rows"] if row["market_slug"] == "dalian")
    assert card["contract_note"] == "Nov-26 crush — B2611 / Y2611 / M2611, all traded 2026-10-16"


def test_the_range_is_the_struck_months_own_history_never_stitched(dalian_db):
    """Older sessions of OTHER months must not enter the Nov-26 range."""
    from datetime import timedelta

    from app.block_builders import CRUSH_RANGE_MIN_OBS, SiteContext, headline_crush_board
    from app.markets import load_markets

    for back in range(1, CRUSH_RANGE_MIN_OBS + 3):
        day = SESSION - timedelta(days=back)
        dalian_db.execute("INSERT INTO currencies (pair, Date, Close) VALUES (?,?,?)",
                          ("CNY/USD", day.isoformat(), 0.149772))
        # Nov-26 on every older session, and a Jan-27 crush 1,000 CNY richer.
        _store(dalian_db, {
            "bean": [("B2611", 3978.0, 1.0), ("B2701", 3028.0, 1.0)],
            "oil": [("Y2611", 8776.0, 1.0), ("Y2701", 8844.0, 1.0)],
            "meal": [("M2611", 3243.0, 1.0), ("M2701", 3281.0, 1.0)],
        }, session=day)
    _store(dalian_db)
    _state, _reason, data = headline_crush_board(load_markets(), SiteContext(conn=dalian_db, today=TODAY))
    band = next(row for row in data["rows"] if row["market_slug"] == "dalian")["range"]
    assert band["n_obs"] == CRUSH_RANGE_MIN_OBS + 3
    # Every session is the same Nov-26 crush, so the range has no width at all.
    assert band["high"] == pytest.approx(band["low"])
    assert band["high"] == pytest.approx(9.1333 * 0.149772, abs=1e-3)


def test_origins_board_level_names_each_legs_contract_and_month(dalian_db):
    from analysis.origins.crush import board_crush

    _store(dalian_db)
    result = board_crush(dalian_db, "dalian", today=TODAY).to_dict()
    assert result["period_label"] == "Nov-26 crush"
    assert result["contract_basis"] == "named_contract"
    assert {leg["contract_symbol"] for leg in result["legs"]} == {"B2611", "Y2611", "M2611"}
    assert {leg["contract_month"] for leg in result["legs"]} == {"2026-11"}
    assert result["margin_usd_mt"] == pytest.approx(9.1333 * 0.149772, abs=1e-3)


def test_origins_board_level_withholds_a_month_mix(dalian_db):
    from analysis.origins.crush import board_crush

    _store(dalian_db, {
        "bean": [("B2611", 3978.0, 154218.0)],
        "oil": [("Y2701", 8844.0, 312470.0)],
        "meal": [("M2701", 3281.0, 1234349.0)],
    })
    result = board_crush(dalian_db, "dalian", today=TODAY)
    assert result.margin is None
    assert result.withheld_code == WithheldReason.NO_CRUSH_MONTH.value


# ---------------------------------------------------------------------------
# Layer 9: fetch → clean → store (no network: Sina is stubbed)
# ---------------------------------------------------------------------------
def test_crush_delivery_months_are_crush_months_after_todays_month():
    from fetchers.akshare import crush_delivery_months

    assert crush_delivery_months(date(2026, 10, 18)) == ["2026-11", "2026-12", "2027-01", "2027-03"]
    # In November, Nov is in delivery and is not fetched.
    assert crush_delivery_months(date(2026, 11, 2))[0] == "2026-12"


def test_layer9_named_months_round_trip_into_the_crush(monkeypatch, patched_db):
    import sqlite3

    import pandas as pd

    from fetchers import akshare
    from pipeline.clean import clean_dce_futures
    from pipeline.store import save_dce_contract_bars

    closes = {c: (close, vol) for rows in OCT16.values() for c, close, vol in rows}
    asked: list[str] = []

    def fake_fetch_one(symbol: str) -> pd.DataFrame:
        asked.append(symbol)
        if symbol not in closes:
            return pd.DataFrame()
        close, volume = closes[symbol]
        return pd.DataFrame({
            "date": [SESSION.isoformat()], "open": [close], "high": [close], "low": [close],
            "close": [close], "volume": [volume], "hold": [1.0], "settle": [close],
        })

    monkeypatch.setattr(akshare, "fetch_one", fake_fetch_one)
    frames = akshare.fetch_dce_crush_months(date(2026, 10, 18))
    assert set(frames) == set(config.DCE_NAMED_MONTH_KEYS)
    assert {"B2611", "Y2611", "M2611", "B2701", "Y2701", "M2701"} <= set(asked)
    for key, frame in frames.items():
        save_dce_contract_bars(config.DCE_NAMED_MONTH_KEYS[key], clean_dce_futures(frame))

    from analysis.futures.crush import MonthTable, month_board_crush

    spec = MonthTable.from_descriptor(config.MARKETS["dalian"]["crush"])
    result = month_board_crush(sqlite3.connect(str(patched_db)), spec, today=TODAY)
    assert isinstance(result, MonthCrush)
    assert result.label == "Nov-26 crush"


def test_the_named_month_frames_are_layer9_catalog_keys():
    """A product whose named months all failed is a named coverage gap."""
    assert set(config.DCE_NAMED_MONTH_KEYS) <= set(config.LAYER_KEY_CATALOGS["dce"])
    assert config.layer_expected_keys("dce") == len(config.DCE_CONTRACTS) + 3
