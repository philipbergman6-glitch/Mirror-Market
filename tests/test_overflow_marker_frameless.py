"""The qa-overflow marker must be final without any animation frame (#678).

Headless Chrome's `--dump-dom --virtual-time-budget` loads the page, waits for
the `load` event, and only then starts the budget (Chromium
components/headless/command_handler/headless_command.js). Virtual time then
fast-forwards while frames arrive on the real-time compositor clock. A
rAF-deferred report raced that dump. With DOMContentLoaded and `load` close
together, the report's frame landed after `load` in 20/20 instrumented local
runs, 22 ms to 1.65 s after DCL. On the heaviest page (workstation.html) the
deploy smoke sometimes dumped the template's default `pending` (run 45817327395).

The marker is now measured synchronously (reading scrollWidth forces layout)
at DOMContentLoaded and again at `load`. So it is final before the budget
starts, and no frame is needed. This test serves the real base template to the
real browser with `requestAnimationFrame` stubbed to never call back, the
exact "no frame landed before the dump" condition, through the smoke's own probe.
"""

from __future__ import annotations

import re

import pytest

from app.templating import site_environment
from scripts import smoke_site

CHROME = smoke_site._chrome_binary(None)

# Runs before the base template's script: no frame ever arrives.
NO_FRAMES = "<script>window.requestAnimationFrame = () => 0;</script>"


def _probe_frameless(tmp_path, monkeypatch, body_extra: str = "") -> list[str]:
    html = site_environment().get_template("_base.html.j2").render(root="")
    # Keep the browser off the internet: the fonts are irrelevant to the marker.
    html = re.sub(r"<link[^>]*fonts\.g[^>]*>", "", html)
    assert '<meta name="qa-overflow" content="pending">' in html
    html = html.replace("<head>", "<head>" + NO_FRAMES, 1)
    html = html.replace("</body>", body_extra + "</body>", 1)
    (tmp_path / "index.html").write_text(html, encoding="utf-8")

    monkeypatch.setattr(smoke_site, "expected_site_paths", lambda: ["index.html"])
    server, base_url = smoke_site._serve(tmp_path)
    try:
        return smoke_site._viewport_failures(base_url, CHROME)
    finally:
        server.shutdown()


@pytest.mark.skipif(CHROME is None, reason="needs a local Chrome/Chromium")
def test_marker_is_measured_when_no_animation_frame_arrives(tmp_path, monkeypatch):
    assert _probe_frameless(tmp_path, monkeypatch) == []


@pytest.mark.skipif(CHROME is None, reason="needs a local Chrome/Chromium")
def test_a_real_overflow_is_still_measured_without_frames(tmp_path, monkeypatch):
    """Being frame-free must not turn the marker into a constant 'ok'."""
    wide = '<div style="width:3000px;height:4px"></div>'
    failures = _probe_frameless(tmp_path, monkeypatch, body_extra=wide)
    assert len(failures) == 2
    assert all(f.startswith("viewport overflow at ") and "(overflow:" in f for f in failures)
