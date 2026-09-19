"""Smoke a private site candidate or the real public GitHub Pages artifact."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import urlopen

from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from trust.site_promotion import (  # noqa: E402
    PUBLISHED_ASSETS,
    expected_site_paths,
    verify_site_candidate,
)


def _load_local(root: Path) -> dict[str, str]:
    return {
        path: (root / path).read_text(encoding="utf-8")
        for path in expected_site_paths()
        if (root / path).is_file()
    }


def _fetch(url: str) -> str:
    with urlopen(url, timeout=30) as response:  # noqa: S310 - explicit smoke target
        if response.status != 200:
            raise RuntimeError(f"{url}: HTTP {response.status}")
        return response.read().decode("utf-8")


def _load_remote(base_url: str) -> dict[str, str]:
    base = base_url.rstrip("/") + "/"
    return {path: _fetch(base + path) for path in expected_site_paths()}


def _generated_at(html: str) -> datetime | None:
    stamp = BeautifulSoup(html, "html.parser").select_one(
        'meta[name="mirror-market-generated-at"]'
    )
    raw = stamp.get("content", "") if stamp else ""
    try:
        generated = datetime.fromisoformat(str(raw))
    except (TypeError, ValueError):
        return None
    return generated if generated.tzinfo else generated.replace(tzinfo=timezone.utc)


def _candidate_stamp(candidate: Path) -> datetime:
    index = candidate / "index.html"
    if not index.is_file():
        raise SystemExit(f"--candidate {candidate}: no index.html to read a generation stamp from")
    stamp = _generated_at(index.read_text(encoding="utf-8"))
    if stamp is None:
        raise SystemExit(f"--candidate {candidate}: index.html carries no generation stamp")
    return stamp


def _wait_for_propagation(
    base_url: str,
    expected: datetime,
    *,
    timeout: float,
    interval: float,
    fetch=_fetch,
    sleep=time.sleep,
    clock=time.monotonic,
) -> bool:
    """Block until the public index carries the candidate's own stamp, or give up.

    GitHub Pages reports a deployment as done before its CDN serves it
    everywhere: observed 2026-09-17, the public index still carried the
    previous night's edition 37 s after `deploy-pages` returned, and the
    promotion-window check then failed a deploy that had in fact succeeded.
    So the smoke asks the live site for the stamp it just uploaded and only
    grades what it sees once that stamp — or a newer one — is what comes back.

    Returns False on timeout. The caller proceeds regardless: whatever the
    site serves at that point is graded honestly by the promotion contract,
    which is the failure the wait exists to avoid raising falsely, not the
    failure it exists to hide.
    """
    url = base_url.rstrip("/") + "/index.html"
    deadline = clock() + timeout
    while True:
        try:
            served = _generated_at(fetch(url))
        except Exception as exc:  # noqa: BLE001 — a 404 mid-propagation is expected
            served = None
            print(f"propagation: index not readable yet ({exc})")
        if served is not None and served >= expected:
            print(f"propagation: public index carries {served.isoformat()}")
            return True
        if clock() >= deadline:
            print(
                f"propagation: gave up after {int(timeout)}s — public index carries "
                f"{served.isoformat() if served else 'no stamp'}, candidate is "
                f"{expected.isoformat()}"
            )
            return False
        sleep(interval)


def _chrome_binary(explicit: str | None) -> str | None:
    if explicit:
        return explicit
    candidates = (
        "google-chrome", "google-chrome-stable", "chromium", "chromium-browser",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    )
    return next((candidate for candidate in candidates if Path(candidate).exists() or shutil.which(candidate)), None)


def _viewport_failures(base_url: str, chrome: str) -> list[str]:
    failures: list[str] = []
    for width, height in ((1440, 1000), (390, 844)):
        for path in expected_site_paths():
            command = [
                chrome, "--headless=new", "--disable-gpu", "--no-sandbox",
                "--run-all-compositor-stages-before-draw", "--virtual-time-budget=1500",
                f"--window-size={width},{height}", "--dump-dom",
                base_url.rstrip("/") + "/" + path,
            ]
            result = subprocess.run(command, capture_output=True, text=True, timeout=30)
            if result.returncode:
                failures.append(f"browser failed at {width}px: {path}")
                continue
            soup = BeautifulSoup(result.stdout, "html.parser")
            meta = soup.select_one('meta[name="qa-overflow"]')
            state = meta.get("content", "missing") if meta else "missing"
            if state != "ok":
                failures.append(f"viewport overflow at {width}px: {path} ({state})")
    return failures


def _serve(root: Path):
    handler = lambda *args, **kwargs: SimpleHTTPRequestHandler(  # noqa: E731
        *args, directory=str(root), **kwargs
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{server.server_port}/"


def _available_assets(root: Path | None, base_url: str | None) -> set[str]:
    """Which non-page published files are actually there."""
    found: set[str] = set()
    for asset in PUBLISHED_ASSETS:
        if root is not None:
            if (root / asset).is_file():
                found.add(asset)
            continue
        try:
            with urlopen(base_url.rstrip("/") + "/" + asset, timeout=30) as response:  # noqa: S310
                if response.status == 200:
                    found.add(asset)
        except Exception:  # noqa: BLE001 — absence is the answer we want
            pass
    return found


def _publication_latency_report(pages: dict[str, str], base_url: str | None) -> list[str]:
    """Measure generation -> publicly readable, the one leg nothing else can.

    Every other interval in the latency chain is observable from inside the
    build. This one is not: the deploy happens after the process that
    generated the page has exited, so the only way to learn it is to ask the
    live site and compare its own stamp against now. That is exactly what
    this smoke step is already doing, so it measures it here rather than
    inventing a second job to do the same fetch.

    It REPORTS and does not fail. A slow Pages deploy is a real operational
    fact and belongs in the log, but failing the post-deploy smoke over it
    would raise a deploy alert for a site that published correctly — and this
    step's failures block the publication outcome. The objective it is
    measured against is enforced in scripts/latency_report.py, which is where
    a threshold belongs.
    """
    if base_url is None:
        return []
    index = pages.get("index.html")
    if not index:
        return []
    generated = _generated_at(index)
    if generated is None:
        return []
    delay = datetime.now(timezone.utc) - generated
    print(
        f"PUBLICATION LATENCY: generated {generated.isoformat()}, publicly readable "
        f"within {int(delay.total_seconds())}s (upper bound — includes the wait "
        "before this check asked)"
    )
    return []


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--root", type=Path)
    source.add_argument("--url")
    parser.add_argument("--browser", action="store_true")
    parser.add_argument("--chrome")
    parser.add_argument(
        "--candidate", type=Path,
        help=(
            "with --url: the local candidate just deployed. The smoke waits until the "
            "public index carries that candidate's generation stamp before grading, "
            "bounded by --propagation-timeout."
        ),
    )
    parser.add_argument("--propagation-timeout", type=float, default=240.0,
                        help="seconds to wait for the CDN to serve the candidate (default 240)")
    parser.add_argument("--propagation-interval", type=float, default=10.0)
    args = parser.parse_args(argv)
    if args.candidate and not args.url:
        parser.error("--candidate only makes sense with --url")

    if args.url and args.candidate:
        _wait_for_propagation(
            args.url, _candidate_stamp(args.candidate),
            timeout=args.propagation_timeout, interval=args.propagation_interval,
        )
    pages = _load_local(args.root) if args.root else _load_remote(args.url)
    assets = _available_assets(args.root, args.url)
    verdict = verify_site_candidate(pages, assets=assets)
    failures = list(verdict.failures)
    failures.extend(_publication_latency_report(pages, args.url))

    server = None
    if args.browser:
        chrome = _chrome_binary(args.chrome)
        if chrome is None:
            failures.append("Chrome/Chromium is unavailable for viewport validation")
        else:
            if args.root:
                server, base_url = _serve(args.root)
            else:
                base_url = args.url
            try:
                failures.extend(_viewport_failures(base_url, chrome))
            finally:
                if server:
                    server.shutdown()

    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        return 1
    print(f"PROMOTION CONTRACT PASSED: {len(pages)} URLs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
