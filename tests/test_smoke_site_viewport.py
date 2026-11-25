"""A viewport probe that measured nothing must never read as overflow (#617).

Every `Refresh Prices (dce)` dispatch from 2026-11-20 to 11-23 went red on
"viewport overflow at 390px: players.html (missing)" (and later "workstation.html
(pending)"), though the same pages measured `ok` on a re-run. Headless Chrome
exits 0 when navigation fails or when it is served an error page, so the smoke
read a probe that never ran the page's reporting script as a measured overflow.

The browser outputs below are what Chrome 14x `--dump-dom` actually returned on
2026-11-25 for an unreachable URL and for a GitHub Pages 404.
"""

from __future__ import annotations

import subprocess
from datetime import datetime, timedelta, timezone

import pytest

from scripts import smoke_site

CANDIDATE = datetime(2026, 11, 20, 10, 31, 55, tzinfo=timezone.utc)

LOAD_FAILED_STDERR = (
    "[24660:142915846:1125/130508.106412:ERROR:components/headless/command_handler/"
    "headless_command_handler.cc:403] Page load failed: net::ERR_CONNECTION_RESET\n"
)
GITHUB_404 = (
    "<!DOCTYPE html>\n<html><head>\n"
    '    <meta http-equiv="Content-type" content="text/html; charset=utf-8">\n'
    "    <title>Page not found · GitHub Pages</title>\n</head><body>404</body></html>"
)


def _page(marker: str, stamp: datetime | None = CANDIDATE) -> str:
    generated = f'<meta name="mirror-market-generated-at" content="{stamp.isoformat()}">' if stamp else ""
    return f'<html><head>{generated}<meta name="qa-overflow" content="{marker}"></head><body></body></html>'


@pytest.fixture
def one_page(monkeypatch):
    monkeypatch.setattr(smoke_site, "expected_site_paths", lambda: ["players.html"])


def _browser(monkeypatch, stdout: str, stderr: str = "", returncode: int = 0) -> list[list[str]]:
    calls: list[list[str]] = []

    def run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, returncode, stdout=stdout, stderr=stderr)

    monkeypatch.setattr(smoke_site.subprocess, "run", run)
    return calls


def test_a_failed_page_load_is_an_incomplete_probe_not_overflow(monkeypatch, one_page):
    _browser(monkeypatch, stdout="", stderr=LOAD_FAILED_STDERR)
    failures = smoke_site._viewport_failures("https://example.test/site/", "chrome")
    assert len(failures) == 2  # both widths, still red
    assert not any("viewport overflow" in f for f in failures)
    assert failures[1] == (
        "viewport probe incomplete at 390px: players.html — page load failed "
        "(net::ERR_CONNECTION_RESET); nothing measured"
    )


def test_an_error_page_without_the_marker_is_an_incomplete_probe(monkeypatch, one_page):
    _browser(monkeypatch, stdout=GITHUB_404)
    failures = smoke_site._viewport_failures("https://example.test/site/", "chrome")
    assert not any("viewport overflow" in f for f in failures)
    assert "title 'Page not found · GitHub Pages'" in failures[0]
    assert failures[0].startswith("viewport probe incomplete at 1440px: players.html")


def test_an_empty_dump_names_itself(monkeypatch, one_page):
    _browser(monkeypatch, stdout="<html><head></head><body></body></html>")
    failures = smoke_site._viewport_failures("https://example.test/site/", "chrome")
    assert all("(empty document); nothing measured" in f for f in failures)


def test_a_pending_marker_is_an_unfinished_probe_with_its_budget(monkeypatch, one_page):
    _browser(monkeypatch, stdout=_page("pending"))
    failures = smoke_site._viewport_failures("https://example.test/site/", "chrome")
    assert failures[0] == (
        "viewport probe incomplete at 1440px: players.html — marker still 'pending' "
        "when the 1500 ms virtual-time budget ended; nothing measured"
    )


def test_a_measured_overflow_is_still_overflow(monkeypatch, one_page):
    _browser(monkeypatch, stdout=_page("overflow:37"))
    failures = smoke_site._viewport_failures("https://example.test/site/", "chrome")
    assert failures == [
        "viewport overflow at 1440px: players.html (overflow:37)",
        "viewport overflow at 390px: players.html (overflow:37)",
    ]


def test_ok_passes(monkeypatch, one_page):
    _browser(monkeypatch, stdout=_page("ok"))
    assert smoke_site._viewport_failures("https://example.test/site/", "chrome") == []


def test_ok_on_an_older_edition_than_the_candidate_does_not_count(monkeypatch, one_page):
    _browser(monkeypatch, stdout=_page("ok", stamp=CANDIDATE - timedelta(hours=8)))
    failures = smoke_site._viewport_failures("https://example.test/site/", "chrome", expected=CANDIDATE)
    assert len(failures) == 2
    assert failures[0].startswith("viewport probe measured another edition at 1440px: players.html")


def test_ok_on_the_candidate_edition_passes(monkeypatch, one_page):
    _browser(monkeypatch, stdout=_page("ok"))
    assert smoke_site._viewport_failures(
        "https://example.test/site/", "chrome", expected=CANDIDATE
    ) == []


def test_the_browser_budget_is_unchanged(monkeypatch, one_page):
    """The fix reclassifies; it must not buy green by giving Chrome more time."""
    calls = _browser(monkeypatch, stdout=_page("ok"))
    smoke_site._viewport_failures("https://example.test/site/", "chrome")
    assert all("--virtual-time-budget=1500" in command for command in calls)


def test_an_unexpected_marker_value_is_unreadable_not_ok(monkeypatch, one_page):
    _browser(monkeypatch, stdout=_page(""))
    failures = smoke_site._viewport_failures("https://example.test/site/", "chrome")
    assert failures[0] == "viewport probe unreadable at 1440px: players.html — marker ''; nothing measured"
