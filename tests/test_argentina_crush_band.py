"""#507: Argentina's market-page crush (block 03) is struck on ONE shared MAGyP
band — never on each leg's mean across its own bands.

MAGyP's circular prints every product for several shipment bands at once —
on 2081 (2026-10-16) beans 3, oil 5, meal 6. The generic board engine read
each leg as ``AVG(...) GROUP BY product, date``: three means over three
different horizons, a margin for a cargo nobody can ship (invariants 2 and 8).
The origins page already struck a shared band
(``analysis.origins.crush.shared_crush_band``); the market page now reads the
same rule, and names each leg's band on its card.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader

import config
from analysis.origins.crush import shared_crush_band
from analysis.origins.domain import ShipmentWindow
from app.block_builders import (
    CRUSH_RANGE_MIN_OBS,
    SiteContext,
    build_blocks,
    headline_crush_board,
)
from app.markets import CRUSH_YIELD_SETS, _crush, load_markets
from pipeline import schema

TODAY = datetime.now(timezone.utc).date()
D0 = TODAY - timedelta(days=1)
D1 = TODAY - timedelta(days=2)
TEMPLATES = Path(__file__).resolve().parents[1] / "app" / "templates"

# (first month ahead, last month ahead, USD/MT), months counted from D0's.
# The nearest band of each leg is circular 2081's own print as #507 quotes
# it (493.0 / 1200.0 / 423.0); the later bands are illustrative, chosen so
# each leg's mean is the one the old read published (483.0 / 1209.4 / 404.33).
BEAN_BANDS = ((0, 0, 493.0), (1, 6, 495.0), (7, 11, 461.0))
OIL_BANDS = ((0, 0, 1200.0), (1, 2, 1210.0), (3, 5, 1215.0), (6, 8, 1212.0), (9, 11, 1210.0))
MEAL_BANDS = (
    (0, 1, 423.0), (2, 3, 415.0), (4, 5, 400.0), (6, 7, 395.0), (8, 9, 396.0), (10, 11, 397.0),
)
LEGS = {"Soybeans": BEAN_BANDS, "Soybean Oil": OIL_BANDS, "Soybean Meal": MEAL_BANDS}


def _month(day: date, ahead: int) -> str:
    index = day.year * 12 + day.month - 1 + ahead
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


def _seed(conn, day: date, legs=LEGS, *, anchor: date = D0) -> None:
    for product, bands in legs.items():
        for index, (start, end, price) in enumerate(bands):
            conn.execute(
                "INSERT INTO argentina_fob (date, product, position, ship_from, ship_to, "
                "price_usd_mt) VALUES (?,?,?,?,?,?)",
                (day.isoformat(), product, f"pos-{product}-{index}", _month(anchor, start),
                 _month(anchor, end), price),
            )


@pytest.fixture
def conn(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "ar_crush.db"
    connection = sqlite3.connect(str(db_path))
    for ddl in (schema._CREATE_PRICES, schema._CREATE_CURRENCIES, schema._CREATE_ARGENTINA_FOB):
        connection.execute(ddl)
    _seed(connection, D0)
    connection.commit()
    monkeypatch.setattr(config, "DB_PATH", str(db_path))
    yield connection
    connection.close()


@pytest.fixture
def registry():
    return load_markets()


def _ctx(conn) -> SiteContext:
    return SiteContext(conn=conn, today=TODAY)


def _block03(conn, registry):
    blocks = build_blocks(registry["argentina"], None, _ctx(conn), markets=registry)
    return next(b for b in blocks if b.id == "crush")


def _margin(bean: float, oil: float, meal: float) -> float:
    yields = CRUSH_YIELD_SETS["soy_board"]
    return oil * yields["oil"] + meal * yields["meal"] - bean


# ---------------------------------------------------------------------------
# The registry
# ---------------------------------------------------------------------------
def test_the_crush_source_declares_the_price_descriptors_windows(registry):
    """A source with no ``window_scheme`` is read as each key's mean (#504)."""
    argentina = registry["argentina"]
    source = argentina.crush.as_source()
    assert source.window_scheme == argentina.price.window_scheme == "magyp_months"
    assert source.window_columns == argentina.price.window_columns


def test_the_origins_crush_and_the_market_crush_read_the_same_window_columns():
    assert (
        tuple(config.PHYSICAL_CRUSH["argentina"]["window_columns"])
        == tuple(config.MARKETS["argentina"]["crush"]["window_columns"])
    )


@pytest.mark.parametrize(
    ("patch", "message"),
    [
        ({"window_scheme": "magyp_months"}, "reads 2 column"),
        ({"window_columns": ("ship_from", "ship_to")}, "not in"),
    ],
)
def test_half_a_crush_window_declaration_fails_at_load(patch, message):
    raw = {**config.MARKETS["argentina"]["crush"]}
    raw.pop("window_scheme")
    raw.pop("window_columns")
    with pytest.raises(ValueError, match=message):
        _crush({**raw, **patch}, slug="argentina")


# ---------------------------------------------------------------------------
# The number
# ---------------------------------------------------------------------------
def test_block_03_strikes_one_shared_band_never_three_means(conn, registry):
    block = _block03(conn, registry)
    assert block.state == "ok", block.reason
    legs = block.data["legs"]
    assert [legs[name]["usd_mt"] for name in ("bean", "oil", "meal")] == [493.0, 1200.0, 423.0]
    assert block.data["margin_usd_mt"] == pytest.approx(_margin(493.0, 1200.0, 423.0))
    # The old read: 483.0 / 1209.4 / 404.33, each a mean nobody quoted.
    assert block.data["margin_usd_mt"] != pytest.approx(_margin(483.0, 1209.4, 2426.0 / 6))


def test_every_leg_names_the_band_it_was_quoted_for(conn, registry):
    data = _block03(conn, registry).data
    assert data["shipment_window"] == _month(D0, 0)
    assert data["legs"]["bean"]["window"] == _month(D0, 0)
    assert data["legs"]["oil"]["window"] == _month(D0, 0)
    # Meal prints no Oct-only band: it takes the one overlapping the bean's.
    assert data["legs"]["meal"]["window"] == f"{_month(D0, 0)}..{_month(D0, 1)}"


def test_no_shared_band_withholds_rather_than_falling_back_to_an_older_session(
    conn, registry
):
    """D1 has bands that line up; D0's meal prints none overlapping any bean
    band. The newest session is withheld — never quietly D1's margin."""
    _seed(conn, D1)
    conn.execute("DELETE FROM argentina_fob WHERE date = ? AND product = 'Soybean Meal'",
                 (D0.isoformat(),))
    _seed(conn, D0, {"Soybean Meal": ((13, 14, 410.0),)})
    conn.commit()
    block = _block03(conn, registry)
    assert block.state == "empty"
    assert "band" in block.reason
    assert D0.isoformat() in block.reason


def test_the_headline_card_prints_block_03s_band_margin(conn, registry):
    state, reason, data = headline_crush_board(registry, _ctx(conn))
    assert state == "ok", reason
    card = next(row for row in data["rows"] if row["market_slug"] == "argentina")
    assert card["margin_usd_mt"] == pytest.approx(_margin(493.0, 1200.0, 423.0))


def test_the_range_is_restruck_on_each_sessions_shared_band(conn, registry):
    """A mean-of-bands history would put a range around a margin the card is
    not printing. Every session here strikes the same band, so the range
    collapses to the level."""
    for offset in range(2, CRUSH_RANGE_MIN_OBS + 1):
        _seed(conn, TODAY - timedelta(days=offset))
    conn.commit()
    state, reason, data = headline_crush_board(registry, _ctx(conn))
    assert state == "ok", reason
    card = next(row for row in data["rows"] if row["market_slug"] == "argentina")
    band = card["range"]
    assert band is not None, card["range_note"]
    assert band["n_obs"] == CRUSH_RANGE_MIN_OBS
    assert band["window"] == f"{CRUSH_RANGE_MIN_OBS}-session on the {_month(D0, 0)} band"
    level = _margin(493.0, 1200.0, 423.0)
    assert band["low"] == pytest.approx(level)
    assert band["high"] == pytest.approx(level)


def test_the_range_never_spans_a_band_roll(conn, registry):
    """Sessions struck on last month's band are a different cargo's margin —
    the run B29 #503 refuses to stitch for Dalian, refused here too."""
    month_ago = D0 - timedelta(days=31)
    for offset in range(2, CRUSH_RANGE_MIN_OBS + 1):
        _seed(conn, TODAY - timedelta(days=offset), anchor=month_ago)
    conn.commit()
    _, _, data = headline_crush_board(registry, _ctx(conn))
    card = next(row for row in data["rows"] if row["market_slug"] == "argentina")
    assert card["range"] is None
    assert f"1 session(s) all three legs struck on the {_month(D0, 0)} band" in card["range_note"]


def test_the_headline_card_names_its_band(conn, registry):
    _, _, data = headline_crush_board(registry, _ctx(conn))
    card = next(row for row in data["rows"] if row["market_slug"] == "argentina")
    assert card["shipment_window"] == _month(D0, 0)
    env = Environment(loader=FileSystemLoader(str(TEMPLATES)), autoescape=True)
    html = env.get_template("sections/crush_board.html.j2").render(s=data, root="../")
    assert f"{_month(D0, 0)} band" in html


def test_a_session_with_no_shared_band_is_dropped_from_the_range(conn, registry):
    for offset in range(2, CRUSH_RANGE_MIN_OBS + 2):
        _seed(conn, TODAY - timedelta(days=offset))
    stale = TODAY - timedelta(days=5)
    conn.execute("DELETE FROM argentina_fob WHERE date = ? AND product = 'Soybean Oil'",
                 (stale.isoformat(),))
    _seed(conn, stale, {"Soybean Oil": ((13, 14, 1300.0),)})
    conn.commit()
    _, _, data = headline_crush_board(registry, _ctx(conn))
    card = next(row for row in data["rows"] if row["market_slug"] == "argentina")
    assert card["range"]["n_obs"] == CRUSH_RANGE_MIN_OBS


# ---------------------------------------------------------------------------
# The page
# ---------------------------------------------------------------------------
def test_block_03_prints_each_legs_band(conn, registry):
    block = _block03(conn, registry)
    env = Environment(loader=FileSystemLoader(str(TEMPLATES)), autoescape=True)
    html = env.get_template("blocks/03_crush.html.j2").render(block=block, root="../")
    assert html.count(f"window {_month(D0, 0)}<") == 2          # bean and oil cards
    assert f"window {_month(D0, 0)}..{_month(D0, 1)}" in html   # meal card
    assert f"struck on the {_month(D0, 0)} band" in html
    assert "$483.0" not in html


# ---------------------------------------------------------------------------
# The shared rule
# ---------------------------------------------------------------------------
def _w(start: tuple[int, int], end: tuple[int, int]) -> ShipmentWindow:
    first = date(*start, 1)
    last = date(end[0] + (end[1] // 12), end[1] % 12 + 1, 1) - timedelta(days=1)
    return ShipmentWindow(first, last)


def test_the_rule_never_leads_with_a_sailed_bean_band():
    session = date(2026, 10, 16)
    picked = shared_crush_band(
        {
            "bean": [(_w((2026, 9), (2026, 9)), 480.0), (_w((2026, 10), (2026, 10)), 493.0)],
            "oil": [(_w((2026, 9), (2026, 10)), 1200.0)],
            "meal": [(_w((2026, 9), (2026, 10)), 423.0)],
        },
        session,
    )
    assert not isinstance(picked, str)
    assert picked.values["bean"] == 493.0


def test_equal_overlap_takes_the_nearer_product_band_not_the_dearer():
    session = date(2027, 3, 1)
    bean = _w((2027, 4), (2027, 6))
    # Mar–Apr overlaps the bean's April (30 days), Jun–Jul its June (30 days):
    # a true tie. The dearer band is listed first, so order alone cannot win.
    oil_near, oil_far = _w((2027, 3), (2027, 4)), _w((2027, 6), (2027, 7))
    assert oil_near.overlap_days(bean) == oil_far.overlap_days(bean)
    picked = shared_crush_band(
        {
            "bean": [(bean, 493.0)],
            "oil": [(oil_far, 1300.0), (oil_near, 1200.0)],
            "meal": [(_w((2027, 4), (2027, 6)), 423.0)],
        },
        session,
    )
    assert not isinstance(picked, str)
    assert picked.values["oil"] == 1200.0
    assert picked.windows["oil"] == oil_near


def test_the_rule_names_why_when_no_band_is_shared():
    reason = shared_crush_band(
        {
            "bean": [(_w((2026, 10), (2026, 10)), 493.0)],
            "oil": [(_w((2026, 10), (2026, 10)), 1200.0)],
            "meal": [(_w((2027, 3), (2027, 4)), 423.0)],
        },
        date(2026, 10, 16),
    )
    assert isinstance(reason, str)
    assert "band" in reason
