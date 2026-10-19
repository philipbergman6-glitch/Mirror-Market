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
from analysis.origins.crush import NO_SHARED_BAND_REASON, shared_crush_band
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


# ---------------------------------------------------------------------------
# The three-way intersection (B35 #532)
# ---------------------------------------------------------------------------
# MAGyP circular of 2026-07-22 as stored in data/history/argentina_fob.csv.
JULY_22 = date(2026, 7, 22)
JULY_22_ROWS = {
    "bean": [(_w((2026, 7), (2026, 9)), 469.0), (_w((2026, 10), (2027, 6)), 480.0)],
    "oil": [
        (_w((2026, 7), (2026, 7)), 1217.0),
        (_w((2026, 8), (2026, 8)), 1203.0),
        (_w((2026, 9), (2026, 9)), 1191.0),
        (_w((2026, 10), (2026, 10)), 1189.0),
        (_w((2026, 11), (2027, 6)), 1173.0),
    ],
    "meal": [
        (_w((2026, 7), (2026, 7)), 361.0),
        (_w((2026, 8), (2026, 9)), 363.0),
        (_w((2026, 10), (2026, 10)), 364.0),
        (_w((2026, 11), (2027, 6)), 369.0),
    ],
}


def _shared(picked) -> ShipmentWindow:
    """The period all three legs' own bands cover — what the band must name."""
    windows = list(picked.windows.values())
    start = max(window.start for window in windows)
    end = min(window.end for window in windows)
    assert start <= end, f"no period all three legs ship: {picked.windows}"
    return ShipmentWindow(start, end)


def test_july_22_bean_oil_and_meal_bands_that_only_overlap_the_bean_are_no_band():
    """Oil Jul and meal Aug–Sep each overlap the Jul–Sep bean and not each
    other: there is no cargo all three ship, so there is no margin."""
    reason = shared_crush_band(
        {
            "bean": [JULY_22_ROWS["bean"][0]],
            "oil": [JULY_22_ROWS["oil"][0]],
            "meal": [JULY_22_ROWS["meal"][1]],
        },
        JULY_22,
    )
    assert reason == NO_SHARED_BAND_REASON


def test_july_22_stored_circular_strikes_a_trio_that_ships_together():
    picked = shared_crush_band(JULY_22_ROWS, JULY_22)
    assert not isinstance(picked, str)
    assert (picked.window.start, picked.window.end) == (
        _shared(picked).start, _shared(picked).end
    )
    # Oil Aug with meal Aug–Sep overlaps the bean 31 + 61 days — more than
    # Jul/Jul (62) or Sep/Aug–Sep (91) — and the three ship together in August.
    assert picked.window.describe() == "2026-08"
    assert picked.values == {"bean": 469.0, "oil": 1203.0, "meal": 363.0}


def test_the_band_shrinks_to_the_period_all_three_legs_ship():
    """Bean Jul–Sep, oil Aug–Sep, meal Aug: the margin is for August."""
    bean, oil, meal = _w((2026, 7), (2026, 9)), _w((2026, 8), (2026, 9)), _w((2026, 8), (2026, 8))
    picked = shared_crush_band(
        {"bean": [(bean, 469.0)], "oil": [(oil, 1200.0)], "meal": [(meal, 363.0)]},
        JULY_22,
    )
    assert not isinstance(picked, str)
    assert (picked.window.start, picked.window.end) == (date(2026, 8, 1), date(2026, 8, 31))
    assert picked.window.describe() == "2026-08"
    assert picked.windows == {"bean": bean, "oil": oil, "meal": meal}


def test_a_shrunk_band_that_is_no_legs_own_band_is_labelled_by_its_months():
    """Bean Jul–Sep, oil Aug–Oct, meal Jun–Aug share only August — a period
    none of the three printed as a band, named in the circular's own form."""
    picked = shared_crush_band(
        {
            "bean": [(_w((2026, 7), (2026, 9)), 469.0)],
            "oil": [(_w((2026, 8), (2026, 10)), 1200.0)],
            "meal": [(_w((2026, 6), (2026, 8)), 363.0)],
        },
        JULY_22,
    )
    assert not isinstance(picked, str)
    assert picked.window.describe() == "2026-08"
    picked = shared_crush_band(
        {
            "bean": [(_w((2026, 7), (2026, 12)), 469.0)],
            "oil": [(_w((2026, 8), (2026, 12)), 1200.0)],
            "meal": [(_w((2026, 6), (2026, 10)), 363.0)],
        },
        JULY_22,
    )
    assert not isinstance(picked, str)
    assert picked.window.describe() == "2026-08..2026-10"


def test_a_product_takes_the_band_that_intersects_not_the_one_overlapping_the_bean_most():
    """Oil Jul–Aug overlaps the Jul–Sep bean more than oil Sep, but only oil
    Sep ships with the Sep meal."""
    picked = shared_crush_band(
        {
            "bean": [(_w((2026, 7), (2026, 9)), 469.0)],
            "oil": [(_w((2026, 7), (2026, 8)), 1210.0), (_w((2026, 9), (2026, 9)), 1191.0)],
            "meal": [(_w((2026, 9), (2026, 9)), 363.0)],
        },
        JULY_22,
    )
    assert not isinstance(picked, str)
    assert picked.values["oil"] == 1191.0
    assert picked.window.describe() == "2026-09"


def test_every_band_returned_is_shipped_by_all_three_legs():
    """Exhaustive over a small family of month bands: a band is returned
    exactly when some bean/oil/meal trio shares a day, and it is that trio's
    shared period."""
    months = [(2026, m) for m in range(7, 13)]
    bands = [_w(a, b) for i, a in enumerate(months) for b in months[i:]]
    for bean in bands[::3]:
        for oil in bands[::4]:
            for meal in bands[::5]:
                picked = shared_crush_band(
                    {"bean": [(bean, 1.0)], "oil": [(oil, 2.0)], "meal": [(meal, 3.0)]},
                    JULY_22,
                )
                shares = bean.overlaps(oil) and bean.overlaps(meal) and oil.overlaps(meal)
                if not shares:
                    assert picked == NO_SHARED_BAND_REASON
                    continue
                assert not isinstance(picked, str)
                shared = _shared(picked)
                assert (picked.window.start, picked.window.end) == (shared.start, shared.end)


def test_one_band_printed_twice_at_two_prices_is_refused():
    """Which of two prices for one band is the oil leg? Not a question the
    rule answers by list order."""
    oil = _w((2026, 7), (2026, 7))
    with pytest.raises(ValueError, match="twice"):
        shared_crush_band(
            {
                "bean": [(_w((2026, 7), (2026, 9)), 469.0)],
                "oil": [(oil, 1217.0), (oil, 1220.0)],
                "meal": [(oil, 361.0)],
            },
            JULY_22,
        )


def test_a_pair_whose_own_best_bands_already_ship_together_is_unchanged():
    """Circular of 2024-11-01: oil Dec–Feb and meal Nov–Dec are each the band
    overlapping the Nov–Feb bean most, and they share December. That pair
    stands, even though oil Dec–Feb with meal Jan–Mar shares more months —
    which band to prefer is the window policy (#536), not this fix."""
    picked = shared_crush_band(
        {
            "bean": [(_w((2024, 11), (2025, 2)), 413.0), (_w((2025, 3), (2025, 10)), 389.0)],
            "oil": [
                (_w((2024, 11), (2024, 11)), 1120.0),
                (_w((2024, 12), (2025, 2)), 1109.0),
                (_w((2025, 3), (2025, 3)), 1031.0),
                (_w((2025, 4), (2025, 10)), 997.0),
            ],
            "meal": [
                (_w((2024, 11), (2024, 12)), 337.0),
                (_w((2025, 1), (2025, 3)), 342.0),
                (_w((2025, 4), (2025, 10)), 334.0),
            ],
        },
        date(2024, 11, 1),
    )
    assert not isinstance(picked, str)
    assert picked.values == {"bean": 413.0, "oil": 1109.0, "meal": 337.0}
    assert picked.window.describe() == "2024-12"


def test_equal_overlap_takes_the_nearer_meal_band_not_the_dearer():
    bean = _w((2027, 4), (2027, 6))
    meal_near, meal_far = _w((2027, 3), (2027, 4)), _w((2027, 6), (2027, 7))
    picked = shared_crush_band(
        {
            "bean": [(bean, 493.0)],
            "oil": [(bean, 1200.0)],
            "meal": [(meal_far, 450.0), (meal_near, 423.0)],
        },
        date(2027, 3, 1),
    )
    assert not isinstance(picked, str)
    assert picked.windows["meal"] == meal_near
    assert picked.window.describe() == "2027-04"


def test_a_shared_period_that_has_ended_never_beats_one_still_open():
    """Mid-August, bean Jul–Sep: a July trio overlaps the bean more, but July
    has sailed; the September trio is the cargo still to ship."""
    picked = shared_crush_band(
        {
            "bean": [(_w((2026, 7), (2026, 9)), 469.0)],
            "oil": [(_w((2026, 6), (2026, 7)), 1210.0), (_w((2026, 9), (2026, 9)), 1191.0)],
            "meal": [(_w((2026, 7), (2026, 7)), 361.0), (_w((2026, 9), (2026, 9)), 363.0)],
        },
        date(2026, 8, 10),
    )
    assert not isinstance(picked, str)
    assert picked.window.describe() == "2026-09"
    assert picked.values == {"bean": 469.0, "oil": 1191.0, "meal": 363.0}


def test_a_shared_period_that_is_not_whole_months_is_named_by_its_dates():
    picked = shared_crush_band(
        {
            "bean": [(ShipmentWindow(date(2026, 8, 1), date(2026, 8, 31), "2026-08"), 469.0)],
            "oil": [(ShipmentWindow(date(2026, 8, 10), date(2026, 9, 15)), 1200.0)],
            "meal": [(ShipmentWindow(date(2026, 7, 20), date(2026, 8, 20)), 363.0)],
        },
        JULY_22,
    )
    assert not isinstance(picked, str)
    assert picked.window.describe() == "2026-08-10..2026-08-20"


def test_one_band_printed_twice_under_two_labels_is_refused():
    oil = _w((2026, 7), (2026, 7))
    with pytest.raises(ValueError, match="twice"):
        shared_crush_band(
            {
                "bean": [(_w((2026, 7), (2026, 9)), 469.0)],
                "oil": [
                    (ShipmentWindow(oil.start, oil.end, "2026-07"), 1217.0),
                    (ShipmentWindow(oil.start, oil.end, "July"), 1217.0),
                ],
                "meal": [(oil, 361.0)],
            },
            JULY_22,
        )


def test_the_range_never_mixes_margins_struck_on_different_bean_bands(conn, registry):
    """Older sessions share the level's October period but price a Sep–Nov
    bean, not the October bean: a different bean quote, so not the level's
    history (B35 #532 — the label alone no longer names the bean's band)."""
    wide_bean = {
        "Soybeans": ((-1, 1, 480.0),),
        "Soybean Oil": OIL_BANDS,
        "Soybean Meal": MEAL_BANDS,
    }
    for offset in range(2, CRUSH_RANGE_MIN_OBS + 2):
        _seed(conn, TODAY - timedelta(days=offset), wide_bean)
    conn.commit()
    _, _, data = headline_crush_board(registry, _ctx(conn))
    card = next(row for row in data["rows"] if row["market_slug"] == "argentina")
    assert card["range"] is None
    assert f"1 session(s) all three legs struck on the {_month(D0, 0)} band" in card["range_note"]
