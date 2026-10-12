"""One edition, one stamp (#444).

The post-deploy smoke reads the candidate's ``index.html`` stamp and requires
every served page to be at least that new. The headline renderer used to stamp
its own clock at the end of a ~15-20 s render, so the index was always newer
than the pages and the manifest, and the smoke failed every page after a deploy
that had succeeded. The orchestrator now hands the headline the edition clock.
"""

from datetime import datetime, timezone
from pathlib import Path

import scripts.generate_html as generate_html
from scripts import generate_site


def test_headline_renders_with_the_edition_clock(monkeypatch, tmp_path: Path):
    edition = datetime(2026, 10, 12, 10, 44, 31, tzinfo=timezone.utc)
    seen: dict = {}

    def fake_generate(**kwargs):
        seen.update(kwargs)
        return {"dashboard": tmp_path / "index.html"}

    monkeypatch.setattr(generate_html, "generate", fake_generate)
    generate_site._render_headline(tmp_path, [], now=edition, public_trust_state=None)
    assert seen["generated_at"] == edition


def test_generate_without_a_clock_still_stamps_its_own():
    """A standalone ``generate()`` run keeps stamping its own clock."""
    import inspect

    sig = inspect.signature(generate_html.generate)
    assert sig.parameters["generated_at"].default is None


def test_smoke_accepts_pages_stamped_exactly_at_the_candidate():
    """Equal stamps pass; only an older page is a prior edition."""
    from scripts.smoke_site import _candidate_failures

    stamp = datetime(2026, 10, 12, 10, 44, 31, tzinfo=timezone.utc)
    same = f'<meta name="mirror-market-generated-at" content="{stamp.isoformat()}">'
    older = f'<meta name="mirror-market-generated-at" content="{stamp.replace(second=14).isoformat()}">'
    pages = {"index.html": same, "origins.html": same}
    assert _candidate_failures(pages, stamp) == []
    pages["origins.html"] = older
    assert len(_candidate_failures(pages, stamp)) == 1
