"""Seeded ``<script>`` injection over externally derived text (#313).

The site renders text that arrived from outside — commodity names from APIs,
health messages quoting fetched rows, tracebacks quoting upstream response
bodies. None of it is trusted markup. Since #313 both production Jinja
environments autoescape, and the only raw HTML that reaches a page passes
through an explicit ``| safe`` on a fragment the codebase itself built.

These tests seed a script tag into the externally derived fields and assert it
comes out inert. They render through the *production* environments
(``scripts.generate_site._env`` / ``scripts.generate_html``'s), not a test
double — an escaping test against a differently configured environment proves
nothing about the deployed site.
"""

from __future__ import annotations

from datetime import datetime, timezone

from scripts import generate_html
from scripts.generate_site import _env

PAYLOAD = "<script>alert('pwned')</script>"
ESCAPED = "&lt;script&gt;alert(&#39;pwned&#39;)&lt;/script&gt;"

NOW = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)


def test_production_environment_autoescapes() -> None:
    """The off switch must not quietly come back."""
    assert _env().autoescape


def test_layer_name_and_health_message_render_inert_on_the_dashboard() -> None:
    """A poisoned layer name / health message must not become live markup.

    The layer name is the closest field to the wire: it labels whatever a
    fetcher said it fetched, and the masthead prints it verbatim.
    """
    items = [
        {"name": f"prices{PAYLOAD}", "status": "fresh", "age": "1h ago"},
        {"name": "weather", "status": "stale", "age": "3d ago"},
    ]
    masthead = generate_html._build_masthead(
        items,
        NOW,
        health={"issues": [{
            "severity": "critical",
            "table": "prices",
            "commodity": "Soybeans",
            "message": f"MISSING {PAYLOAD}",
        }]},
    )
    html = _env().get_template("dashboard.html.j2").render(
        sections=[],
        generated_at="2026-08-23 12:00 UTC",
        masthead=masthead,
        freshness_items=items,
    )

    assert PAYLOAD not in html
    assert ESCAPED in html


def test_tombstone_error_text_renders_inert() -> None:
    """A traceback can quote an upstream response body — it renders as text."""
    html = _env().get_template("tombstone.html.j2").render(
        page_name=f"origins{PAYLOAD}",
        error=f"HTTPError: 502 from upstream, body: {PAYLOAD}",
        root="",
        market_nav=[],
        current_page=None,
        current_market=None,
        day_line="SUNDAY 23 AUGUST 2026",
        generated_at="2026-08-23 12:00 UTC",
        generated_at_iso=NOW.isoformat(),
    )

    assert PAYLOAD not in html
    assert ESCAPED in html


def test_trusted_fragments_still_pass_through_safe() -> None:
    """The escape stance must not neuter the charts: `| safe` fragments render raw."""
    fragment = '<div class="alert alert-ok">All data sources healthy</div>'
    html = _env().get_template("dashboard.html.j2").render(
        sections=[],
        generated_at="2026-08-23 12:00 UTC",
        masthead=generate_html._build_masthead(
            [{"name": "prices", "status": "fresh", "age": "1h ago"}],
            NOW,
            health={"issues": []},
        ),
        freshness_items=[],
        # health_html renders inside the briefing block, so one must exist
        briefing_text="the day's briefing",
        health_html=fragment,
    )

    assert fragment in html
