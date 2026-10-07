"""The wayfinding pass, as rendered: nav groups, the key, deeper lines, section 11.

`tests/test_wayfinding.py` covers the resolver and the vocabulary; this file
renders the templates through the production environment factory and reads
the markup a trader would.
"""

from __future__ import annotations

from bs4 import BeautifulSoup

from app.markets import TIER_BRIEF, TIER_PAGE, TIER_STUB
from app.templating import site_environment
from app.wayfinding import deeper_links
from scripts import generate_html

NAV = [
    {"slug": "cbot", "name": "CBOT", "href": "markets/cbot.html", "tier": TIER_PAGE},
    {"slug": "dalian", "name": "Dalian", "href": "markets/dalian.html", "tier": TIER_PAGE},
    {"slug": "brazil", "name": "Brazil", "href": "markets/brazil.html", "tier": TIER_PAGE},
    {"slug": "argentina", "name": "Argentina", "href": "markets/argentina.html", "tier": TIER_PAGE},
    {"slug": "india", "name": "India", "href": "markets/india.html", "tier": TIER_BRIEF},
    {"slug": "nigeria", "name": "Nigeria", "href": "markets/nigeria.html", "tier": TIER_STUB},
]


def _render(**context) -> BeautifulSoup:
    base = {
        "sections": generate_html.SECTIONS,
        "generated_at": "2026-10-07 13:11 UTC",
        "masthead": {},
        "freshness_items": [],
        "market_nav": NAV,
        "root": "",
        "current_page": "headline",
        "deeper": {s["id"]: deeper_links(s["id"], NAV) for s in generate_html.SECTIONS},
    }
    html = site_environment().get_template("dashboard.html.j2").render(**{**base, **context})
    return BeautifulSoup(html, "html.parser")


def test_nav_is_two_labelled_groups_plus_utilities():
    soup = _render()
    nav = soup.select_one("nav.market-nav")
    labels = [el.get_text(strip=True) for el in nav.select(".mn-label")]
    assert labels == ["Markets", "Desk"]
    groups = nav.select(".mn-group")
    assert [a.get_text(strip=True) for a in groups[0].select("a")][:2] == ["Headline", "CBOT"]
    assert [a.get_text(strip=True) for a in groups[1].select("a")] == [
        "Origins", "Workstation", "Opportunities", "Players",
    ]
    assert nav.select_one("a[href='briefing.html']").get_text(strip=True) == "Briefing"
    assert nav.select_one("a[data-open-key]")["href"] == "#how-to-read"
    assert nav.select_one("a.stub").get_text(strip=True) == "Nigeria"


def test_key_renders_collapsed_with_live_component_swatches():
    soup = _render()
    key = soup.select_one("details#how-to-read")
    assert key is not None and not key.has_attr("open")
    assert key.select_one("dt span.kind") is not None
    assert key.select_one("dt span.pill.pill-dark") is not None
    assert key.select_one("dt span.tier-pill.stub") is not None
    assert key.select_one("dt span.es-label.state-empty") is not None
    titles = [h.get_text(strip=True) for h in key.select("h3")]
    assert "How old a number is" in titles


def test_deeper_lines_follow_tier_and_scope():
    soup = _render()
    crush = soup.select_one("section#crush-board .deeper")
    assert crush.select_one(".dp-no").get_text(strip=True) == "03"
    assert [a.get_text(strip=True) for a in crush.select("a")] == ["CBOT", "Dalian", "Brazil", "Argentina"]
    assert crush.select_one("a")["href"] == "markets/cbot.html#block-crush"

    ledger = soup.select_one("section#propagation .deeper")
    names = [a.get_text(strip=True) for a in ledger.select("a")]
    assert "India" not in names          # a brief renders no ledger block
    assert "Nigeria" not in names        # a stub renders no blocks at all

    supply = soup.select_one("section#supply-demand .deeper")
    assert "India" in [a.get_text(strip=True) for a in supply.select("a")]

    assert soup.select_one("section#signals .deeper") is None
    assert soup.select_one("section#seasonal .deeper") is None


def test_section_eleven_is_a_pointer_not_the_report():
    soup = _render(briefing_text="line one\nline two", briefing_lines=2, briefing_uri="data:text/plain,x")
    section = soup.select_one("section#briefing")
    assert section.select_one(".sec-no").get_text(strip=True) == "11"
    assert section.select_one("div.briefing") is None
    assert section.select_one("a[href='briefing.html']") is not None
    assert "line one" not in section.get_text(" ")
    assert section.select_one(".deeper a")["href"] == "briefing.html"


def test_section_eleven_without_a_report_shows_the_fallback():
    soup = _render(briefing_text="")
    assert "No briefing data" in soup.select_one("section#briefing").get_text(" ")


def test_briefing_page_carries_the_report_and_marks_itself_current():
    html = site_environment().get_template("briefing.html.j2").render(
        generated_at="2026-10-07 13:11 UTC", masthead={}, market_nav=NAV, root="",
        current_page="briefing", briefing_text="the report", briefing_uri="data:text/plain,x",
    )
    soup = BeautifulSoup(html, "html.parser")
    assert soup.select_one("#briefing .briefing").get_text(strip=True) == "the report"
    assert "current" in soup.select_one("nav.market-nav a[href='briefing.html']")["class"]


def test_a_bare_environment_still_renders_the_page_without_the_key():
    """Focused tests build their own Environment; the guard in _base keeps them
    rendering. Production is pinned to the factory by test_wayfinding."""
    from jinja2 import Environment, FileSystemLoader

    from app.templating import TEMPLATE_DIR

    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=True)
    html = env.get_template("dashboard.html.j2").render(
        sections=generate_html.SECTIONS, generated_at="", masthead={}, freshness_items=[],
        market_nav=[], root="",
    )
    assert 'id="how-to-read"' not in html
