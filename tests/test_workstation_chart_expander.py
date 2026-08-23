"""The contract-row chart expander: ours on top, links out below (#320, #328).

#328 established that TradingView's free embed widget refuses CME symbols on
any plan, so the embedded chart is gone and nothing third-party renders on
this page any more. What the expander holds now: this project's own stored
close history for the contract (our data, our palette, honest about how thin
it is — invariant 2), and below a visible territory line, plain links out to
the same contract month on TradingView's and Barchart's own sites. These
tests hold that frame: that nothing is ever requested from a third party,
that the two territories carry the two palettes, that a thin history is
withheld with a reason rather than dressed up, and that a venue this project
has not checked gets no expander at all.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime

import pytest
from jinja2 import Environment, FileSystemLoader

from analysis.futures.domain import spec_for
from analysis.futures.privacy import AUDIENCE_PRIVATE, AUDIENCE_PUBLIC
from app.tradingview import THIRD_PARTY_STAMP
from app.workstation_page import build_view
from pipeline import schema

TEMPLATE_DIR = "app/templates"
TODAY = date(2026, 8, 19)
GENERATED_AT = datetime(2026, 8, 19, 21, 30)

#: The sessions ZSX26 has history for — 9 of them, enough for a joined line
#: (the 8-observation floor), while every other leg has only the final one.
ZSX26_SESSIONS = [
    ("2026-08-07", 1141.00),
    ("2026-08-10", 1143.50),
    ("2026-08-11", 1144.00),
    ("2026-08-12", 1146.25),
    ("2026-08-13", 1145.00),
    ("2026-08-14", 1147.75),
    ("2026-08-17", 1149.00),
    ("2026-08-18", 1148.50),
    ("2026-08-19", 1150.00),
]


@pytest.fixture
def curve_db():
    conn = sqlite3.connect(":memory:")
    for ddl in schema.ALL_SCHEMAS:
        conn.execute(ddl)
    conn.executemany(
        "INSERT INTO forward_curve (commodity, contract_month, label, ticker, close, "
        "observation_date, volume, open_interest, fetched_date) "
        "VALUES ('Soybeans','2026-11-01','Nov 2026','ZSX26.CBT',?,?,4210,NULL,?)",
        [(close, session, session) for session, close in ZSX26_SESSIONS],
    )
    # A Saturday re-fetch of Friday's session: same observation date, later
    # fetched date. Must collapse into ONE point, not two.
    conn.execute(
        "INSERT INTO forward_curve (commodity, contract_month, label, ticker, close, "
        "observation_date, volume, open_interest, fetched_date) "
        "VALUES ('Soybeans','2026-11-01','Nov 2026','ZSX26.CBT',1150.00,"
        "'2026-08-19',4210,NULL,'2026-08-20')"
    )
    conn.executemany(
        "INSERT INTO forward_curve (commodity, contract_month, label, ticker, close, "
        "observation_date, volume, open_interest, fetched_date) "
        "VALUES (?,?,?,?,?,'2026-08-19',4210,NULL,'2026-08-19')",
        [
            ("Soybeans", "2027-01-01", "Jan 2027", "ZSF27.CBT", 1162.00),
            ("Soybean Meal", "2026-12-01", "Dec 2026", "ZMZ26.CBT", 305.00),
        ],
    )
    # ZLZ26 sits in the dots regime: 3 sessions — enough to render (the floor
    # is 2), too few for a joined line (the floor is 8).
    conn.executemany(
        "INSERT INTO forward_curve (commodity, contract_month, label, ticker, close, "
        "observation_date, volume, open_interest, fetched_date) "
        "VALUES ('Soybean Oil','2026-12-01','Dec 2026','ZLZ26.CBT',?,?,4210,NULL,?)",
        [
            (51.20, "2026-08-17", "2026-08-17"),
            (51.85, "2026-08-18", "2026-08-18"),
            (52.00, "2026-08-19", "2026-08-19"),
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


def leg_for(page_view, symbol: str) -> dict:
    return next(
        leg
        for row in contracts(page_view)["data"]["commodities"]
        for leg in row["legs"]
        if leg["symbol"] == symbol
    )


def panel_markup(html: str, tv_symbol: str) -> str:
    return html.split(f'id="tv-{tv_symbol.replace(":", "-")}"')[1].split("</tr>")[0]


# ---------------------------------------------------------------------------
# The symbols and links reach the row
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


def test_the_tradingview_link_is_the_exact_contract_page(curve_db):
    leg = leg_for(view(curve_db), "ZSX26")
    assert leg["tradingview_url"] == "https://www.tradingview.com/symbols/CBOT-ZSX2026/"


def test_the_barchart_link_is_the_per_contract_chart(curve_db):
    leg = leg_for(view(curve_db), "ZSX26")
    assert leg["barchart_url"] == (
        "https://www.barchart.com/futures/quotes/ZSX26/interactive-chart"
    )


def test_both_links_render_in_the_panel(curve_db):
    html = render(view(curve_db))
    panel = panel_markup(html, "CBOT:ZSX2026")
    assert 'href="https://www.tradingview.com/symbols/CBOT-ZSX2026/"' in panel
    assert (
        'href="https://www.barchart.com/futures/quotes/ZSX26/interactive-chart"'
        in panel
    )
    # Outbound links open elsewhere and leak no referrer-side opener handle.
    assert panel.count('rel="noopener nofollow" target="_blank"') == 2


# ---------------------------------------------------------------------------
# Nothing third-party is requested, ever (#328: the embed is dead)
# ---------------------------------------------------------------------------
def test_nothing_third_party_is_ever_requested(curve_db):
    """The widget refused CME symbols and fell back to AAPL — the exact
    wrong-chart-beside-a-right-price failure invariant 11 names. What ships
    now is links only: no iframe, no third-party script, no embed markup."""
    html = render(view(curve_db))
    assert "s3.tradingview.com" not in html
    assert "embed-widget" not in html
    assert "<iframe" not in html
    assert "tradingview-widget-container" not in html


def test_every_panel_ships_hidden(curve_db):
    html = render(view(curve_db))
    assert html.count('<tr class="tv-panel"') == html.count('class="tv-panel" id="tv-')
    assert 'class="tv-panel" id="tv-CBOT-ZSX2026" hidden' in html


def test_the_expander_affordance_is_gated_on_script(curve_db):
    """With script off the chevron never appears: `.tv-toggle` is display:none
    until the script adds `.tv-ready`. A control that does nothing is worse
    than no control."""
    html = render(view(curve_db))
    assert ".tv-toggle { display: none;" in html
    assert ".dtable.tv-ready .tv-toggle { display: inline-block; }" in html
    assert "classList.add('tv-ready')" in html


# ---------------------------------------------------------------------------
# Our chart: our data, honest about its thinness (invariant 2)
# ---------------------------------------------------------------------------
def test_a_leg_with_history_carries_its_own_stored_closes(curve_db):
    history = leg_for(view(curve_db), "ZSX26")["close_history"]
    assert history["count"] == len(ZSX26_SESSIONS)
    assert history["since"] == "2026-08-07"
    assert history["withheld_reason"] == ""
    assert history["stamp"].startswith("Our data · delayed daily closes")
    assert "history since 7 Aug 2026" in history["stamp"]


def test_points_are_per_session_not_per_fetch(curve_db):
    """The Saturday re-fetch of Friday's close is the same observation — one
    point. Counting fetches would draw a flat segment nobody traded."""
    history = leg_for(view(curve_db), "ZSX26")["close_history"]
    sessions = [point[0] for point in history["points"]]
    assert sessions == sorted(set(sessions))
    assert sessions.count("2026-08-19") == 1


def test_points_are_usd_per_mt_from_the_one_conversion_site(curve_db):
    """USD/MT via the contract spec (invariant 7) — the same figure the row's
    own USD/MT column prints, never a second conversion."""
    history = leg_for(view(curve_db), "ZSX26")["close_history"]
    expected = round(spec_for("Soybeans").native_to_usd_per_mt(1150.00), 2)
    assert history["points"][-1] == ["2026-08-19", expected]


def test_the_chart_json_ships_in_the_panel_and_is_drawn_locally(curve_db):
    html = render(view(curve_db))
    panel = panel_markup(html, "CBOT:ZSX2026")
    assert 'data-symbol="ZSX26"' in panel
    payload = panel.split('<script type="application/json">')[1].split("</script>")[0]
    assert len(json.loads(payload)) == len(ZSX26_SESSIONS)
    # Drawn by the page's own script on first expand — from this JSON, not a fetch.
    assert "draw(panel.querySelector('.tv-chart'))" in html
    assert "insertAdjacentHTML" in html


def test_a_sparse_series_is_dots_never_a_joined_line(curve_db):
    """Under 8 observations no polyline (the ledger's M21 rule): a line
    through the holes of a thin history asserts a path nobody observed."""
    html = render(view(curve_db))
    assert 'data-line-min="8"' in html
    assert "points.length >= lineMin" in html


def test_a_dots_regime_series_still_ships_its_points(curve_db):
    """ZLZ26 has 3 sessions — between the render floor (2) and the line floor
    (8). It must ship as chart data, not be withheld: how it is drawn (dots,
    no polyline) is the script's job, but whether it exists is the server's."""
    history = leg_for(view(curve_db), "ZLZ26")["close_history"]
    assert history["count"] == 3
    assert history["withheld_reason"] == ""
    assert len(history["points"]) == 3
    html = render(view(curve_db))
    panel = panel_markup(html, "CBOT:ZLZ2026")
    assert 'data-symbol="ZLZ26"' in panel
    assert "application/json" in panel


def test_a_single_session_withholds_the_chart_with_a_reason(curve_db):
    """ZMZ26 has one stored session. One point is a number, not a history —
    the panel says so instead of drawing it (invariant 2)."""
    history = leg_for(view(curve_db), "ZMZ26")["close_history"]
    assert history["points"] == []
    assert "1 stored session" in history["withheld_reason"]
    assert "not a history" in history["withheld_reason"]
    html = render(view(curve_db))
    panel = panel_markup(html, "CBOT:ZMZ2026")
    assert "not a history" in panel
    assert "application/json" not in panel  # no chart data, not empty chart data


# ---------------------------------------------------------------------------
# Two territories, two palettes
# ---------------------------------------------------------------------------
def test_our_zone_wears_our_palette_and_theirs_wears_the_slate(curve_db):
    """DESIGN.md 2026-08-23 (#328): the panel splits — our chart on paper with
    the --green rule, the link strip on slate with the --info rule — so the
    border colour itself marks where our data stops."""
    html = render(view(curve_db))
    assert ".tv-ours { background: var(--paper); border-left: 3px solid var(--green);" in html
    assert ".tv-links { background: #EEF1F4; border-left: 3px solid var(--info);" in html


def test_the_frame_names_the_third_party_without_a_delay_claim(curve_db):
    html = render(view(curve_db))
    # The stamp renders whole, from Python — no half of it is template text.
    assert THIRD_PARTY_STAMP in html
    assert "opens on their site" in THIRD_PARTY_STAMP
    # The ~10-minute figure was TradingView's claim about a feed that rendered
    # here. Nothing renders here now, so the page makes no delay claim at all.
    assert "10 min" not in html
    assert "not our observation, not a settlement, and not routable" in html


def test_one_panel_open_at_a_time_across_the_whole_page(curve_db):
    """DESIGN.md: one open at a time. The section renders one table per
    commodity, so the close-others sweep must be document-wide — a per-table
    sweep would let beans, meal and oil each hold an open panel."""
    html = render(view(curve_db))
    assert "each(document.querySelectorAll('tr.crow.open'), close);" in html
    assert "table.querySelectorAll('tr.crow.open')" not in html


# ---------------------------------------------------------------------------
# Both editions, and no privacy consequence
# ---------------------------------------------------------------------------
def test_both_editions_get_the_expander(curve_db):
    """The panel shows exchange-derived closes and outbound links, not book
    data — invariant 4 is untouched, so the two editions do not differ here."""
    public = render(view(curve_db, audience=AUDIENCE_PUBLIC))
    private = render(view(curve_db, audience=AUDIENCE_PRIVATE))
    for html in (public, private):
        assert 'id="tv-CBOT-ZSX2026"' in html
        assert 'data-symbol="ZSX26"' in html
        assert "s3.tradingview.com" not in html
