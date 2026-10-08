"""The post-deploy smoke waits for the CDN to serve the edition it just shipped.

Observed 2026-09-17: `deploy-pages` returned, and 37 s later the public index
still carried the previous night's edition, so the promotion-window check
failed a deploy that had succeeded. The wait is bounded and never hides a real
failure — on timeout the smoke grades whatever the site serves.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from scripts import smoke_site

NOW = datetime(2026, 9, 17, 13, 17, tzinfo=timezone.utc)


def _page(stamp: datetime | None) -> str:
    meta = f'<meta name="mirror-market-generated-at" content="{stamp.isoformat()}">' if stamp else ""
    return f"<html><head>{meta}</head><body></body></html>"


class _Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += seconds


def test_waits_until_the_public_index_carries_the_candidate_stamp():
    served = iter([_page(NOW - timedelta(hours=12)), _page(NOW - timedelta(hours=12)), _page(NOW)])
    clock = _Clock()
    ok = smoke_site._wait_for_propagation(
        "https://example.test/site/", NOW, timeout=240, interval=10,
        fetch=lambda url: next(served), sleep=clock.sleep, clock=clock,
    )
    assert ok is True
    assert clock.t == 20.0  # two stale reads, two sleeps, then the fresh one


def test_a_newer_stamp_than_the_candidate_also_counts():
    ok = smoke_site._wait_for_propagation(
        "https://example.test/site", NOW, timeout=240, interval=10,
        fetch=lambda url: _page(NOW + timedelta(minutes=3)), sleep=lambda s: None, clock=lambda: 0.0,
    )
    assert ok is True


def test_gives_up_after_the_timeout_and_reports_false(capsys):
    clock = _Clock()
    ok = smoke_site._wait_for_propagation(
        "https://example.test/site", NOW, timeout=30, interval=10,
        fetch=lambda url: _page(NOW - timedelta(hours=12)), sleep=clock.sleep, clock=clock,
    )
    assert ok is False
    assert clock.t == 30.0
    assert "gave up after 30s" in capsys.readouterr().out


def test_an_unreadable_index_mid_propagation_is_retried_not_fatal():
    calls = {"n": 0}

    def fetch(url: str) -> str:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("HTTP 404")
        return _page(NOW)

    clock = _Clock()
    ok = smoke_site._wait_for_propagation(
        "https://example.test/site", NOW, timeout=240, interval=5,
        fetch=fetch, sleep=clock.sleep, clock=clock,
    )
    assert ok is True and calls["n"] == 2


def test_candidate_stamp_is_read_from_the_candidate_index(tmp_path):
    (tmp_path / "index.html").write_text(_page(NOW), encoding="utf-8")
    assert smoke_site._candidate_stamp(tmp_path) == NOW


def test_candidate_without_a_stamp_is_a_hard_failure(tmp_path):
    (tmp_path / "index.html").write_text(_page(None), encoding="utf-8")
    with pytest.raises(SystemExit):
        smoke_site._candidate_stamp(tmp_path)


def test_propagation_timeout_cannot_pass_a_recent_but_old_edition(tmp_path, monkeypatch, capsys):
    from types import SimpleNamespace

    (tmp_path / "index.html").write_text(_page(NOW))
    monkeypatch.setattr(smoke_site, "_wait_for_propagation", lambda *a, **k: False)
    monkeypatch.setattr(smoke_site, "_load_remote", lambda *a: {"index.html": _page(NOW - timedelta(minutes=5))})
    monkeypatch.setattr(smoke_site, "_available_assets", lambda *a: set())
    monkeypatch.setattr(smoke_site, "verify_site_candidate", lambda *a, **k: SimpleNamespace(failures=()))
    monkeypatch.setattr(smoke_site, "_publication_latency_report", lambda *a: [])
    assert smoke_site.main(["--url", "https://example.test", "--candidate", str(tmp_path)]) == 1
    assert "candidate" in capsys.readouterr().out


def test_each_page_must_reach_candidate_even_if_index_has_propagated():
    failures = smoke_site._candidate_failures({
        "index.html": _page(NOW), "origins.html": _page(NOW - timedelta(minutes=1)),
    }, NOW)
    assert len(failures) == 1
    assert "origins.html" in failures[0]
