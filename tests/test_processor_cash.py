"""Layer 30 — US processor cash soybean oil and meal (AMS 3511 over MARS, #352).

The decisive fixture is the week of 2026-09-28: its prices less its basis
reproduce the CBOT closes of Thu 2026-10-01 to the cent. That one identity is
what proves both unit findings the layer rests on — the oil basis is in points
(not the cents/lb its unit field says) and "$ Per Ton" is the short ton CBOT
meal trades in — and it is what the cash-leg panel uses to find the session a
premium may be struck on.
"""

from __future__ import annotations

import copy
import json
import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from jinja2 import Environment, FileSystemLoader

import config
from analysis.processor_cash import cash_leg_panel
from fetchers import processor_cash
from fetchers.processor_cash import check_reconciliation, map_rows
from pipeline import query, store
from pipeline.results import ScraperShapeError
from pipeline.schema import ALL_SCHEMAS
from pipeline.units import (
    CONVERSION_FACTORS,
    cents_per_lb_to_usd_mt,
    points_to_cents_per_lb,
    usd_per_short_ton_to_usd_mt,
)

_FIXTURES = Path(__file__).parent / "fixtures"
_WEEK = _FIXTURES / "ams_3511_2026-09-28_api.json"
_SHAPES = _FIXTURES / "ams_3511_archive_shapes_api.json"
_REPO = Path(__file__).resolve().parent.parent

# CBOT closes of the strike session, Thu 2026-10-01 (yfinance, 2026-10-05).
_STRIKE_CLOSES = {
    ("Soybean Oil", "2026-10-01", "ZLV26.CBT"): 66.97,
    ("Soybean Oil", "2026-12-01", "ZLZ26.CBT"): 67.38,
    ("Soybean Meal", "2026-10-01", "ZMV26.CBT"): 352.1,
    ("Soybean Meal", "2026-12-01", "ZMZ26.CBT"): 353.3,
}


def _week_rows() -> list[dict]:
    return json.loads(_WEEK.read_text())["results"]


def _row(location: str = "Iowa", commodity: str = "Soybean Oil", **overrides) -> dict:
    row = copy.deepcopy(next(
        r for r in _week_rows() if r["trade Loc"] == location and r["commodity"] == commodity
    ))
    row.update(overrides)
    return row


# ── Units ────────────────────────────────────────────────────────────────────


def test_unit_named_conversions_match_the_commodity_factors() -> None:
    assert cents_per_lb_to_usd_mt(1.0) == pytest.approx(CONVERSION_FACTORS["Soybean Oil"])
    assert usd_per_short_ton_to_usd_mt(1.0) == pytest.approx(CONVERSION_FACTORS["Soybean Meal"])
    assert cents_per_lb_to_usd_mt(66.97) == pytest.approx(1476.43, abs=0.01)
    assert usd_per_short_ton_to_usd_mt(353.3) == pytest.approx(389.45, abs=0.01)
    assert points_to_cents_per_lb(-50.0) == -0.5


def test_price_less_basis_reproduces_the_cbot_closes() -> None:
    """Points for oil, short tons for meal — proved, not read off a label."""
    df = map_rows(_week_rows())
    implied = set()
    for _, r in df.iterrows():
        to_price = points_to_cents_per_lb if r["basis_unit"] == "points_per_lb" else (lambda v: v)
        implied.add(round(r["price_low"] - to_price(r["basis_low"]), 2))
        implied.add(round(r["price_high"] - to_price(r["basis_high"]), 2))
    assert implied == {v for v in _STRIKE_CLOSES.values()}


# ── Mapping ──────────────────────────────────────────────────────────────────


def test_live_week_maps_every_row_as_its_own_series() -> None:
    df = map_rows(_week_rows())
    assert len(df) == 12
    assert set(df["week_end"]) == {"2026-10-02"}
    assert set(df["week_start"]) == {"2026-09-28"}
    assert set(df["published_at"]) == {"2026-10-02 13:47:14"}
    # FOB and delivered, truck and rail: never pooled.
    illinois_meal = df[(df.location == "Illinois") & (df.commodity == "Soybean Meal")]
    assert sorted(illinois_meal["trans_mode"]) == ["Rail", "Truck"]
    assert df.loc[df.location == "CA-South", "freight"].tolist() == ["Delivered"]
    oil = df[df.commodity == "Soybean Oil"]
    assert set(oil["basis_unit"]) == {"points_per_lb"}
    assert set(oil["price_unit"]) == {"cents_per_lb"}
    assert oil["protein"].isna().all()
    meal = df[df.commodity == "Soybean Meal"]
    assert set(meal["basis_unit"]) == {"usd_per_short_ton"}
    assert set(meal["protein"]) == {"46.5-48%"}


def test_a_two_contract_basis_keeps_both_months() -> None:
    """`0.00V to 200.00Z` — the #196 rule from Layer 20."""
    df = map_rows([_row("Iowa", "Soybean Oil")])
    assert (df.loc[0, "futures_month_low"], df.loc[0, "futures_month_high"]) == (10, 12)


def test_archive_shapes_are_stored_as_published() -> None:
    df = map_rows(json.loads(_SHAPES.read_text())["results"]).set_index("location")
    # A basis printed with only its high-leg month: the low month is never
    # learned, and is not borrowed from the high leg.
    io = df.loc["Indiana-Ohio"]
    assert pd.isna(io["futures_month_low"]) and io["futures_month_high"] == 12
    assert io["basis_low"] == 15.0
    # A Price quote: flat price, no basis at all.
    portland = df.loc["Portland, OR"]
    assert portland["price_avg"] == 350.0
    assert pd.isna(portland["basis_low"]) and pd.isna(portland["basis_unit"])
    # A basis with no price.
    kc = df.loc["KC Region"]
    assert pd.isna(kc["price_low"]) and kc["basis_low"] == 40.0


def test_a_slot_with_no_number_is_not_stored() -> None:
    blank = {f: None for f in ("price_min", "price_max", "avg_price", "basis_min", "basis_max")}
    assert map_rows([_row(**blank)]).empty


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"protein": "44%"}, "different grade"),
        ({"commodity": "Canola Meal"}, "no longer filters"),
        ({"price_unit": "$ Per Ton"}, "price unit"),
        ({"basis_unit": "Points"}, "basis unit"),
        ({"min_basis_futures_month": "October (Z)"}, "contradicts"),
        ({"price_max": None}, "partly filled price"),
        ({"basis_max": None}, "partly filled basis"),
        ({"report_end_date": "10/03/2026"}, "Monday-Friday"),
        ({"report_date": "09/29/2026"}, "first day"),
        ({"sale_type": "Bid"}, "sale_type"),
        ({"freight": "C.I.F."}, "freight"),
        ({"trans_mode": "Barge"}, "trans_mode"),
        ({"quote_type": "Price"}, "Price quote carries a basis"),
    ],
)
def test_drift_raises_rather_than_storing(overrides: dict, match: str) -> None:
    row = _row()
    row.update(overrides)
    with pytest.raises(ScraperShapeError, match=match):
        map_rows([row])


def test_two_quotes_for_one_series_raise() -> None:
    with pytest.raises(ScraperShapeError, match="share one"):
        map_rows([_row(), _row()])


# ── Reconciliation ───────────────────────────────────────────────────────────


def test_live_week_reconciles() -> None:
    check_reconciliation(map_rows(_week_rows()))


def test_a_basis_unit_change_fails_the_pull() -> None:
    """AMS printing oil basis in true cents would fail every oil row."""
    rows = _week_rows()
    for r in rows:
        if r["commodity"] == "Soybean Oil":
            r["basis_min"] /= 100
            r["basis_max"] /= 100
    with pytest.raises(ScraperShapeError):
        check_reconciliation(map_rows(rows))


def test_a_few_unreconciled_rows_are_the_publishers_and_pass() -> None:
    rows = _week_rows()
    rows[0]["price_max"] += 1.0   # one row of twelve off, as AMS sometimes is
    check_reconciliation(map_rows(rows))


# ── Transport ────────────────────────────────────────────────────────────────


class _Resp:
    def __init__(self, status: int, payload: dict | None = None) -> None:
        self.status_code = status
        self._payload = payload

    def json(self) -> dict:
        return self._payload or {}


def _serve(monkeypatch: pytest.MonkeyPatch, *, status: int = 200, stats: dict | None = None) -> list:
    calls: list = []

    def fake_get(url, params, auth, timeout):  # noqa: ANN001
        calls.append(params)
        commodity = params["q"].split("=", 1)[1]
        results = [r for r in _week_rows() if r["commodity"] == commodity]
        return _Resp(status, {"stats": stats or {"returnedRows": len(results), "userAllowedRows": 100000},
                              "results": results})

    monkeypatch.setattr(processor_cash, "MARS_API_KEY", "test-key")
    monkeypatch.setattr(processor_cash.requests, "get", fake_get)
    monkeypatch.setattr(processor_cash, "retry_sleep", lambda attempt: None)
    return calls


def test_fetch_pulls_each_commodity_and_returns_one_table(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _serve(monkeypatch)
    out = processor_cash.fetch_processor_cash()
    assert list(out) == ["us_processor_cash"]
    assert len(out["us_processor_cash"]) == 12
    assert [c["q"] for c in calls] == ["commodity=Soybean Oil", "commodity=Soybean Meal"]


def test_rejected_key_fails_the_layer(monkeypatch: pytest.MonkeyPatch) -> None:
    _serve(monkeypatch, status=403)
    assert processor_cash.fetch_processor_cash() == {}


def test_a_truncated_archive_fails_the_layer(monkeypatch: pytest.MonkeyPatch) -> None:
    _serve(monkeypatch, stats={"returnedRows": 100000, "userAllowedRows": 100000})
    assert processor_cash.fetch_processor_cash() == {}


def test_unset_key_is_unconfigured_not_broken(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(processor_cash, "MARS_API_KEY", "")
    assert processor_cash.is_configured() is False
    import main

    layer = next(e for e in main._build_dict_layers() if e.key == "us_processor_cash")
    assert layer.run_if is processor_cash.is_configured
    assert "MARS_API_KEY" in (layer.skip_msg or "")
    assert layer.empty_fails is True


# ── Registry ─────────────────────────────────────────────────────────────────


def test_layer_is_registered_with_a_weekly_budget() -> None:
    assert "us_processor_cash" in config.PRODUCTION_LAYER_KEYS
    assert config.LAYER_MAX_DATA_AGE_DAYS["us_processor_cash"] == 21
    assert "Layer 30" in config.API_KEY_LAYERS["MARS_API_KEY"]


def test_the_cbot_physical_crush_still_withholds() -> None:
    """Cash oil and meal exist now; a US physical crush still does not."""
    cbot = config.PHYSICAL_CRUSH["cbot"]
    assert cbot["missing_legs"] == ("bean",)
    assert "CIF NOLA" in cbot["absent_reason"]


# ── Storage ──────────────────────────────────────────────────────────────────


def test_store_round_trip_keeps_null_months(patched_db) -> None:  # noqa: ANN001
    df = map_rows(json.loads(_SHAPES.read_text())["results"] + _week_rows())
    store.save_processor_cash("us_processor_cash", df)
    back = query.read_processor_cash()
    assert len(back) == 15
    assert set(back["quote_kind"]) == {config.PROCESSOR_CASH_QUOTE_KIND}
    io = back[back.location == "Indiana-Ohio"]
    assert io[io.week_end == "2025-11-21"]["futures_month_low"].isna().all()


def test_not_a_history_table() -> None:
    """The whole archive re-downloads every run (Layers 22/26 precedent)."""
    from pipeline.history import HISTORY_TABLES

    assert "us_processor_cash" not in HISTORY_TABLES


# ── The cash-leg panel ───────────────────────────────────────────────────────


@pytest.fixture
def cash_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    for ddl in ALL_SCHEMAS:
        conn.execute(ddl)
    df = map_rows(_week_rows())
    df["cadence"], df["quote_kind"] = "weekly", "weekly processor ask"
    df.to_sql("us_processor_cash", conn, if_exists="append", index=False)
    return conn


def _bars(conn: sqlite3.Connection, *, shift: float = 0.0) -> None:
    for (commodity, month, ticker), close in _STRIKE_CLOSES.items():
        for day, offset in (("2026-09-30", 0.5), ("2026-10-01", shift), ("2026-10-02", -1.2)):
            conn.execute(
                "INSERT INTO contract_bars (commodity, ticker, contract_month, Date, Close, "
                "Volume, fetched_date) VALUES (?, ?, ?, ?, ?, 1000, '2026-10-05')",
                (commodity, ticker, month, day, close + offset),
            )


def test_panel_strikes_on_the_session_ams_used(cash_db) -> None:  # noqa: ANN001
    _bars(cash_db)
    panel = cash_leg_panel(cash_db, "cbot", today=date(2026, 10, 5))
    assert panel is not None and panel["state"] == "ok"
    data = panel["data"]
    assert data["strike_session"] == "2026-10-01"
    illinois_oil = next(
        r for r in data["rows"] if r["product"].startswith("Crude") and r["location"] == "Illinois"
    )
    # -50 to +600 points over ZLV26 is -0.50 to +6.00 c/lb.
    assert illinois_oil["premium_low"] == pytest.approx(cents_per_lb_to_usd_mt(-0.5))
    assert illinois_oil["premium_high"] == pytest.approx(cents_per_lb_to_usd_mt(6.0))
    assert [b["contract"] for b in illinois_oil["board"]] == ["ZLV26"]
    # Iowa's 0.00V basis is a premium of exactly zero, not a float-residue -0.0.
    iowa_oil = next(
        r for r in data["rows"] if r["product"].startswith("Crude") and r["location"] == "Iowa"
    )
    assert iowa_oil["premium_low"] == 0.0 and str(iowa_oil["premium_low"]) == "0.0"
    assert data["kind_label"] == "weekly processor ask"
    assert "crush" not in data["label"].lower()


def test_no_matching_session_withholds_board_but_keeps_the_ask(cash_db) -> None:  # noqa: ANN001
    _bars(cash_db, shift=3.0)   # no stored close equals AMS's arithmetic
    panel = cash_leg_panel(cash_db, "cbot", today=date(2026, 10, 5))
    assert panel["state"] == "ok"
    data = panel["data"]
    assert data["strike_session"] is None
    assert "not identified" in data["strike_note"]
    assert all(r["premium_low"] is None and r["premium_high"] is None for r in data["rows"])
    assert all(b["usd_mt"] is None for r in data["rows"] for b in r["board"])
    assert all(r["cash_avg"] is not None for r in data["rows"])


def test_a_stale_week_is_withheld(cash_db) -> None:  # noqa: ANN001
    panel = cash_leg_panel(cash_db, "cbot", today=date(2026, 11, 30))
    assert panel["state"] == "empty" and "budget" in panel["reason"]


def test_an_empty_table_names_the_key() -> None:
    conn = sqlite3.connect(":memory:")
    for ddl in ALL_SCHEMAS:
        conn.execute(ddl)
    panel = cash_leg_panel(conn, "cbot", today=date(2026, 10, 5))
    assert panel["state"] == "empty" and "MARS_API_KEY" in panel["reason"]


def test_markets_without_a_cash_leg_get_none(cash_db) -> None:  # noqa: ANN001
    assert cash_leg_panel(cash_db, "brazil", today=date(2026, 10, 5)) is None


def test_crush_template_renders_the_panel(cash_db) -> None:  # noqa: ANN001
    _bars(cash_db)
    panel = cash_leg_panel(cash_db, "cbot", today=date(2026, 10, 5))
    env = Environment(loader=FileSystemLoader(str(_REPO / "app" / "templates")), autoescape=True)
    leg = {"key": "ZSX26", "usd_mt": 400.0}
    block = {"data": {
        "profitable": True, "margin_usd_mt": 50.0, "margin_home": None, "period_label": None,
        "as_of": "2026-10-02", "age_days": 3, "legs": {"bean": leg, "oil": leg, "meal": leg},
        "yields": {"oil": 0.19, "meal": 0.79}, "legs_named": False, "contract_note": None,
        "provisional": False, "cash": panel,
    }}
    html = env.get_template("blocks/03_crush.html.j2").render(block=block)
    assert "US processor cash" in html
    assert "not a crush margin" in html
    assert "CA-South" in html and "delivered · rail" in html
    assert "2026-10-01" in html
