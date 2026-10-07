from datetime import date, datetime, timezone

from config import PRODUCTION_LAYERS
from trust.site_promotion import expected_site_paths, verify_site_candidate


def _pages() -> dict[str, str]:
    generated = '<meta name="mirror-market-generated-at" content="2026-08-18T12:00:00+00:00">'
    key = '<details id="how-to-read"></details>'
    pages = {
        path: f"<!doctype html><html><head>{generated}</head><body>{key}</body></html>"
        for path in expected_site_paths()
    }
    nav = "".join(f'<a href="{path}">{path}</a>' for path in expected_site_paths())
    layers = "".join(f'<tr data-layer="{row[0]}"></tr>' for row in PRODUCTION_LAYERS)
    legs = "".join(
        f'<div data-benchmark="{name}" data-as-of="2026-08-17"></div>'
        for name in ("Soybeans", "Soybean Oil", "Soybean Meal")
    )
    pages["index.html"] = f"""<!doctype html><html><head>{generated}
      <meta name="mirror-market-layer-count" content="{len(PRODUCTION_LAYERS)}">
      </head><body>{key}{nav}<section id="briefing"><a href="briefing.html">Read the full briefing</a></section>
      {layers}{legs}<div data-derived="crush" data-aligned="true" data-as-of="2026-08-17"></div>
      </body></html>"""
    # The briefing text lives on its own page (wayfinding pass, 2026-10-07).
    pages["briefing.html"] = f"""<!doctype html><html><head>{generated}</head><body>{key}{nav}
      <section id="briefing"><div class="briefing">Daily briefing</div></section></body></html>"""
    return pages


def test_complete_candidate_satisfies_promotion_contract():
    verdict = verify_site_candidate(
        _pages(),
        today=date(2026, 8, 18),
        now=datetime(2026, 8, 18, 13, tzinfo=timezone.utc),
    )

    assert verdict.verified is True
    assert verdict.failures == ()


def test_contract_rejects_missing_briefing_tombstone_and_stale_benchmark():
    pages = _pages()
    pages["briefing.html"] = pages["briefing.html"].replace(
        '<div class="briefing">Daily briefing</div>', "No briefing data"
    )
    pages["index.html"] = pages["index.html"].replace(
        'data-as-of="2026-08-17"', 'data-as-of="2026-07-01"', 1
    )
    pages["players.html"] = pages["players.html"].replace(
        "<body>", '<body><div class="tomb">could not be generated today</div>'
    )

    verdict = verify_site_candidate(
        pages,
        today=date(2026, 8, 18),
        now=datetime(2026, 8, 18, 13, tzinfo=timezone.utc),
    )

    assert verdict.verified is False
    assert "daily briefing is absent" in verdict.failures
    assert "daily briefing fallback is visible" in verdict.failures
    assert "unexpected tombstone: players.html" in verdict.failures
    assert any("benchmark outside cadence" in failure for failure in verdict.failures)


def test_contract_rejects_missing_urls_broken_links_and_count_drift():
    pages = _pages()
    del pages["players.html"]
    pages["index.html"] = pages["index.html"].replace(
        f'content="{len(PRODUCTION_LAYERS)}"', 'content="25"'
    )

    verdict = verify_site_candidate(
        pages,
        today=date(2026, 8, 18),
        now=datetime(2026, 8, 18, 13, tzinfo=timezone.utc),
    )

    assert "missing expected URL: players.html" in verdict.failures
    assert "broken internal link: index.html -> players.html" in verdict.failures
    assert any("source/layer count mismatch" in failure for failure in verdict.failures)


def test_every_page_the_masthead_links_to_is_in_the_promotion_contract():
    """A nav link to a page outside the contract ships a site whose nav 404s.

    The masthead is the authority: it is defined once in ``_base.html.j2`` and
    every page extends it, so a page reachable from the nav must be verified
    before upload or a failed build silently publishes a dead link.
    """
    import re
    from pathlib import Path

    base = Path("app/templates/_base.html.j2").read_text(encoding="utf-8")
    linked = {
        href.split("/")[-1]
        for href in re.findall(r'href="\{\{ *nav\.(\w+) *\}\}"', base)
    }
    nav_targets = set(re.findall(r'href="(?:\{\{[^}]*\}\})?([\w./-]*\.html)"', base))
    contract = set(expected_site_paths())
    for target in nav_targets:
        assert target.split("/")[-1] in {p.split("/")[-1] for p in contract}, target
    assert "workstation.html" in contract
    assert "origins.html" in contract
    assert "briefing.html" in contract
    assert linked or nav_targets     # the masthead links to something at all


def test_contract_rejects_a_dead_cross_page_anchor_and_a_missing_key():
    """A "Deeper" link to `markets/x.html#block-ledger` passes a file-exists
    check even when that page renders no such block; the gate must read the
    target. And a page that rendered without the key took the template's
    test-only branch in production."""
    pages = _pages()
    pages["index.html"] = pages["index.html"].replace(
        "<section id=", '<a href="markets/cbot.html#block-ledger">deeper</a>'
        '<a href="briefing.html#briefing">ok</a><section id=', 1
    )
    pages["players.html"] = pages["players.html"].replace('<details id="how-to-read"></details>', "")

    verdict = verify_site_candidate(
        pages,
        today=date(2026, 8, 18),
        now=datetime(2026, 8, 18, 13, tzinfo=timezone.utc),
    )

    assert "dead anchor: index.html -> markets/cbot.html#block-ledger" in verdict.failures
    assert not any("briefing.html#briefing" in f for f in verdict.failures)
    assert "how-to-read key is absent: players.html" in verdict.failures
