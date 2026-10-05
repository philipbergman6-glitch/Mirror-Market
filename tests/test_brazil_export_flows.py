"""Brazil page, block 07 — the Comex Stat customs export flow read (#351).

The line a buyer reads: where did Brazil's beans, meal and oil go in the
newest month MDIC has published — China against the rest of the world — and
how does that compare with the same month a year earlier.

What these pin:

1. **Tonnes, China vs rest of world, same-month YoY**, from raw kg through
   pipeline/units.py — a per-row sum, never an average of rows.
2. **The newest month is preliminary and says so**; the month MDIC has not
   released is named as not yet published, never shown as a zero.
3. **The unit value is publish-gated.** Comex Stat is CC BY-ND 3.0 and FOB ÷
   tonnes is our arithmetic on MDIC's figures — it does not leave the
   builder while config gates it off.
4. **Attribution travels with the numbers** into the markup.
5. **No rows, no read** — withheld with a reason that blames our ingest, not
   the market.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pandas as pd
import pytest
from jinja2 import Environment, FileSystemLoader

import config
from app.block_builders import SiteContext, build_blocks
from app.markets import load_markets
from pipeline import schema

TODAY = datetime.now(timezone.utc).date()
LATEST = pd.Period(TODAY, freq="M") - 2          # MDIC's usual lag
PRIOR_YEAR = LATEST - 12


def _month_end(period: pd.Period) -> str:
    return period.end_time.date().isoformat()


def _insert(conn, period, ncm, product, country, kg, fob, state="Mato Grosso"):
    conn.execute(
        "INSERT INTO brazil_exports (month_end, ncm, product, country, state, kg, fob_usd, qty_stat) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (_month_end(period), ncm, product, country, state, kg, fob, kg // 1000),
    )


@pytest.fixture
def ctx():
    conn = sqlite3.connect(":memory:")
    conn.execute(schema._CREATE_BRAZIL_EXPORTS)
    conn.execute(schema._CREATE_DATA_FRESHNESS)
    conn.execute(schema._CREATE_PSD)
    yield SiteContext(conn=conn, today=TODAY)
    conn.close()


def _seed(conn):
    # Newest month: 8.0 Mt of beans, 7.0 to China (split across two states,
    # one of them MDIC's undeclared bucket — still China's tonnage).
    _insert(conn, LATEST, "12019000", "Soybeans", "China", 6_000_000_000, 3_000_000_000)
    _insert(conn, LATEST, "12019000", "Soybeans", "China", 1_000_000_000, 500_000_000,
            state="Não Declarada")
    _insert(conn, LATEST, "12019000", "Soybeans", "Spain", 1_000_000_000, 400_000_000)
    # Same month last year: 7.0 Mt, 5.0 to China.
    _insert(conn, PRIOR_YEAR, "12019000", "Soybeans", "China", 5_000_000_000, 2_200_000_000)
    _insert(conn, PRIOR_YEAR, "12019000", "Soybeans", "Spain", 2_000_000_000, 880_000_000)
    # An older month that must not be mistaken for the newest.
    _insert(conn, LATEST - 1, "12019000", "Soybeans", "China", 9_000_000_000, 4_000_000_000)
    # Meal: two NCMs are one product. None to China.
    _insert(conn, LATEST, "23040090", "Soybean Meal", "Netherlands", 1_500_000_000, 500_000_000)
    _insert(conn, LATEST, "23040010", "Soybean Meal", "Indonesia", 500_000_000, 180_000_000)
    # Oil: no same-month-last-year row → YoY withheld, not zero.
    _insert(conn, LATEST, "15071000", "Soybean Oil", "India", 200_000_000, 230_000_000)
    conn.commit()


def _exports(ctx):
    registry = load_markets()
    blocks = build_blocks(registry["brazil"], None, ctx, markets=registry)
    sd = next(b for b in blocks if b.id == "supply_demand")
    return sd, sd.data["exports"]


def test_brazil_names_its_customs_export_feed_in_the_registry():
    assert load_markets()["brazil"].customs_exports == "comexstat"
    # Every other market is untouched — the read is a registry pointer.
    assert all(m.customs_exports is None for slug, m in load_markets().items() if slug != "brazil")


def test_newest_month_china_vs_rest_of_world_in_tonnes(ctx):
    _seed(ctx.conn)
    _, exports = _exports(ctx)

    assert exports["state"] == "ok"
    data = exports["data"]
    assert data["month"] == str(LATEST)
    beans = next(r for r in data["rows"] if r["product"] == "Soybeans")
    assert beans["tonnes"] == pytest.approx(8_000_000)
    assert beans["focus_tonnes"] == pytest.approx(7_000_000)
    assert beans["rest_tonnes"] == pytest.approx(1_000_000)
    assert beans["focus_share_pct"] == pytest.approx(87.5)
    assert data["focus_destination"] == "China"


def test_same_month_last_year_comparison(ctx):
    _seed(ctx.conn)
    _, exports = _exports(ctx)
    rows = {r["product"]: r for r in exports["data"]["rows"]}

    assert rows["Soybeans"]["yoy_pct"] == pytest.approx(100 * (8 / 7 - 1))
    assert rows["Soybeans"]["focus_yoy_pct"] == pytest.approx(40.0)
    # No prior-year row is "never learned", not a 100% rise from zero.
    assert rows["Soybean Oil"]["yoy_pct"] is None
    assert rows["Soybean Oil"]["focus_yoy_pct"] is None


def test_meal_sums_both_ncm_lines_and_a_zero_china_share_is_a_real_zero(ctx):
    _seed(ctx.conn)
    _, exports = _exports(ctx)
    meal = next(r for r in exports["data"]["rows"] if r["product"] == "Soybean Meal")
    assert meal["tonnes"] == pytest.approx(2_000_000)
    assert meal["focus_tonnes"] == 0
    assert meal["focus_share_pct"] == 0


def test_the_newest_month_is_labelled_preliminary_and_the_next_is_named_unreleased(ctx):
    _seed(ctx.conn)
    _, exports = _exports(ctx)
    data = exports["data"]
    assert data["preliminary"] is True
    assert str(LATEST + 1) in data["unreleased_note"]
    assert "not yet published" in data["unreleased_note"]


def test_the_unit_value_does_not_leave_the_builder_while_gated(ctx, monkeypatch):
    monkeypatch.setattr(config, "COMEXSTAT_PUBLISH_UNIT_VALUE", False)
    _seed(ctx.conn)
    _, exports = _exports(ctx)
    for row in exports["data"]["rows"]:
        assert "unit_value_usd_mt" not in row
    assert "unit_value_trend" not in exports["data"]
    assert "unit value" in exports["data"]["withheld_note"]


def test_the_unit_value_is_a_customs_unit_value_when_ungated(ctx, monkeypatch):
    monkeypatch.setattr(config, "COMEXSTAT_PUBLISH_UNIT_VALUE", True)
    _seed(ctx.conn)
    _, exports = _exports(ctx)
    beans = next(r for r in exports["data"]["rows"] if r["product"] == "Soybeans")
    # (3.0e9 + 0.5e9 + 0.4e9) USD over 8.0 Mt — a ratio of sums, not a mean of ratios.
    assert beans["unit_value_usd_mt"] == pytest.approx(487.5)
    trend = exports["data"]["unit_value_trend"]["Soybeans"]
    assert trend[-1] == (str(LATEST), pytest.approx(487.5))
    assert "not a price" in exports["data"]["unit_value_caveat"]


def test_attribution_reaches_the_markup(ctx):
    _seed(ctx.conn)
    sd, _ = _exports(ctx)
    env = Environment(loader=FileSystemLoader("app/templates"), autoescape=True)
    html = env.get_template("blocks/07_supply_demand.html.j2").render(block=sd)
    assert "Comex Stat" in html
    assert "CC BY-ND 3.0" in html
    assert "preliminary" in html
    assert "8,000,000" in html or "8.0" in html


def test_no_rows_withholds_with_a_reason(ctx):
    _, exports = _exports(ctx)
    assert exports["state"] == "empty"
    assert "comexstat" in exports["reason"]


def test_a_failed_ingest_is_blamed_on_us_not_the_market(ctx):
    ctx.conn.execute(
        "INSERT INTO data_freshness (layer_name, last_success, last_attempt, rows_fetched, status) "
        "VALUES (?,?,?,?,?)", ("comexstat", None, "2026-10-05T20:00:00", 0, "failed"))
    ctx.conn.commit()
    _, exports = _exports(ctx)
    assert exports["state"] == "empty"
    assert "our comexstat ingest failed upstream" in exports["reason"]


def test_a_missing_table_is_an_empty_read_not_a_crash():
    conn = sqlite3.connect(":memory:")
    conn.execute(schema._CREATE_PSD)
    _, exports = _exports(SiteContext(conn=conn, today=TODAY))
    assert exports["state"] == "empty"
