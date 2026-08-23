"""The contract-row chart expander: labelled, lazy, and both editions (#320).

The embedded TradingView chart is an approved exception to the "not a
real-time feed" line, and an exception only stays honest while it is *framed*.
These tests hold the frame, not the chart: that the widget is never requested
until a reader asks for it, that the panel says whose data it is, that the
licence-required attribution is present and unhidden, and that a venue this
project has not checked gets no expander at all.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime

import pytest
from jinja2 import Environment, FileSystemLoader

from analysis.futures.privacy import AUDIENCE_PRIVATE, AUDIENCE_PUBLIC
from app.tradingview import TRADINGVIEW_DELAY_NOTE
from app.workstation_page import build_view
from pipeline import schema

TEMPLATE_DIR = "app/templates"
TODAY = date(2026, 8, 19)
GENERATED_AT = datetime(2026, 8, 19, 21, 30)

EMBED_SRC = "s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js"


@pytest.fixture
def curve_db():
    conn = sqlite3.connect(":memory:")
    for ddl in schema.ALL_SCHEMAS:
        conn.execute(ddl)
    conn.executemany(
        "INSERT INTO forward_curve (commodity, contract_month, label, ticker, close, "
        "observation_date, volume, open_interest, fetched_date) "
        "VALUES (?,?,?,?,?,'2026-08-19',4210,NULL,'2026-08-19')",
        [
            ("Soybeans", "2026-11-01", "Nov 2026", "ZSX26.CBT", 1150.00),
            ("Soybeans", "2027-01-01", "Jan 2027", "ZSF27.CBT", 1162.00),
            ("Soybean Meal", "2026-12-01", "Dec 2026", "ZMZ26.CBT", 305.00),
            ("Soybean Oil", "2026-12-01", "Dec 2026", "ZLZ26.CBT", 52.00),
        ],
    )
    conn.commit()
    yield conn
    conn.close()


def view(conn, *, audience=AUDIENCE_PUBLIC):
    return build_view(conn, today=TODAY, generated_at=GENERATED_AT, audience=audience)


def render(page_view) -> str:
    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=False)
    return env.get_template("workstation.html.j2").render(
        workstation=page_view,
        root="",
        market_nav=[],
        current_page="workstation",
        generated_at="2026-08-19 21:30",
    )


def contracts(page_view) -> dict:
    return next(s for s in page_view["sections"] if s["id"] == "contracts")


# ---------------------------------------------------------------------------
# The symbol reaches the row
# ---------------------------------------------------------------------------
def test_every_cbot_leg_carries_its_own_contract_month_symbol(curve_db):
    section = contracts(view(curve_db))
    legs = {
        leg["symbol"]: leg["tradingview_symbol"]
        for row in section["data"]["commodities"]
        for leg in row["legs"]
    }
    # Per contract month, not per product: ZSX26 and ZSF27 are different charts.
    assert legs["ZSX26"] == "CBOT:ZSX2026"
    assert legs["ZSF27"] == "CBOT:ZSF2027"
    assert legs["ZMZ26"] == "CBOT:ZMZ2026"
    assert legs["ZLZ26"] == "CBOT:ZLZ2026"


def test_the_attribution_target_is_the_symbol_page(curve_db):
    section = contracts(view(curve_db))
    leg = next(
        leg
        for row in section["data"]["commodities"]
        for leg in row["legs"]
        if leg["symbol"] == "ZSX26"
    )
    assert leg["tradingview_url"] == "https://www.tradingview.com/symbols/CBOT-ZSX2026/"


# ---------------------------------------------------------------------------
# Lazy: nothing third-party is fetched until a reader asks
# ---------------------------------------------------------------------------
def test_no_widget_is_loaded_until_a_row_is_expanded(curve_db):
    """The page lists every listed month; N iframes up front is the thing the
    ticket forbids. The embed URL appears exactly once, inside the script that
    injects it on first expand — never in markup that the browser would act on."""
    html = render(view(curve_db))
    assert html.count(EMBED_SRC) == 1
    assert "<iframe" not in html
    # The panel's mount point ships empty.
    assert '<div class="tv-widget" data-tv-symbol="CBOT:ZSX2026"></div>' in html


def test_every_panel_ships_hidden(curve_db):
    html = render(view(curve_db))
    assert html.count('<tr class="tv-panel"') == html.count('class="tv-panel" id="tv-')
    assert 'class="tv-panel" id="tv-CBOT-ZSX2026" hidden' in html


def test_nothing_on_the_page_blocks_the_third_party_script(curve_db):
    """GitHub Pages sends no CSP header for a user site, and `_base.html.j2`
    sets no CSP meta. If one is ever added, the embed silently stops loading —
    so this fails first and names the reason."""
    html = render(view(curve_db))
    assert "Content-Security-Policy" not in html


def test_the_expander_affordance_is_gated_on_script(curve_db):
    """With script off the chevron never appears: `.tv-toggle` is display:none
    until the script adds `.tv-ready`. A control that does nothing is worse
    than no control."""
    html = render(view(curve_db))
    assert ".tv-toggle { display: none;" in html
    assert ".dtable.tv-ready .tv-toggle { display: inline-block; }" in html
    assert "classList.add('tv-ready')" in html


# ---------------------------------------------------------------------------
# Framed as foreign territory
# ---------------------------------------------------------------------------
def test_the_frame_names_the_third_party_and_the_delay(curve_db):
    html = render(view(curve_db))
    assert TRADINGVIEW_DELAY_NOTE in html
    assert "Third party · TradingView" in html
    assert "not our observation, not a settlement, and not routable" in html


def test_the_frame_does_not_borrow_our_brand_accent(curve_db):
    """DESIGN.md 2026-08-23: the panel is slate on --info, deliberately outside
    the Morning Scan paper-and-soy-green palette, so the reader can see where
    our numbers stop."""
    html = render(view(curve_db))
    assert "tr.tv-panel > td { padding: 0; background: #EEF1F4; border-left: 3px solid var(--info); }" in html


def test_the_attribution_link_is_rendered_and_never_hidden(curve_db):
    """A licence condition, not decoration (invariant 9). No rule in the page
    may hide `.tv-credit`."""
    html = render(view(curve_db))
    assert 'href="https://www.tradingview.com/symbols/CBOT-ZSX2026/"' in html
    assert "CBOT:ZSX2026 chart by TradingView" in html
    for hiding in (".tv-credit { display: none", ".tv-credit{display:none"):
        assert hiding not in html


def test_the_widget_cannot_be_switched_to_another_symbol(curve_db):
    """The frame answers "what has this contract done". A reader who changed
    the symbol would be reading a chart that no longer matches its row."""
    html = render(view(curve_db))
    assert "allow_symbol_change: false" in html


def test_a_failed_embed_says_so_rather_than_showing_an_empty_box(curve_db):
    html = render(view(curve_db))
    assert "script.onerror" in html
    assert "did not load" in html


# ---------------------------------------------------------------------------
# Both editions, and no privacy consequence
# ---------------------------------------------------------------------------
def test_both_editions_get_the_expander(curve_db):
    """The widget shows exchange data, not book data — invariant 4 is untouched,
    so there is no reason for the two editions to differ here."""
    public = render(view(curve_db, audience=AUDIENCE_PUBLIC))
    private = render(view(curve_db, audience=AUDIENCE_PRIVATE))
    for html in (public, private):
        assert 'data-tv-symbol="CBOT:ZSX2026"' in html
        assert html.count(EMBED_SRC) == 1


def test_the_symbol_is_the_only_thing_that_leaves_this_page(curve_db):
    """Nothing desk-entered is in the config the widget is handed: the symbol,
    and exchange-public rendering options."""
    html = render(view(curve_db))
    for desk_word in ("basis", "position", "account", "premium"):
        block = html.split("script.text = JSON.stringify(")[1].split("});")[0]
        assert desk_word not in block.lower()
