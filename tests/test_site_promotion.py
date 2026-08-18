from datetime import date, datetime, timezone

from config import PRODUCTION_LAYERS
from trust.site_promotion import expected_site_paths, verify_site_candidate


def _pages() -> dict[str, str]:
    generated = '<meta name="mirror-market-generated-at" content="2026-08-18T12:00:00+00:00">'
    pages = {
        path: f"<!doctype html><html><head>{generated}</head><body></body></html>"
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
      </head><body>{nav}<section id="briefing"><div class="briefing">Daily briefing</div></section>
      {layers}{legs}<div data-derived="crush" data-aligned="true" data-as-of="2026-08-17"></div>
      </body></html>"""
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
    pages["index.html"] = pages["index.html"].replace(
        '<div class="briefing">Daily briefing</div>', "No briefing data"
    ).replace('data-as-of="2026-08-17"', 'data-as-of="2026-07-01"', 1)
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
