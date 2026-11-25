"""#637 — the Open-Meteo Worker's contract runs with the Python suite.

The Worker (``infra/openmeteo-worker/src/worker.js``) is JavaScript, tested
with Node's built-in runner against the same recorded Open-Meteo bodies the
fetcher tests replay. This wrapper makes it part of ``pytest`` so the Worker
and the fetcher it serves cannot drift apart unnoticed: the header names
below are the ones ``fetchers/weather.py`` relies on.

Node is preinstalled on GitHub's ubuntu runners; in CI a missing ``node``
fails rather than skips (a skipped contract test is a silent one).
Locally it skips with the reason.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

import fetchers.weather as weather

WORKER = Path(__file__).resolve().parent.parent / "infra" / "openmeteo-worker"


def _node() -> str:
    node = shutil.which("node")
    if node is None:
        if os.getenv("CI"):
            pytest.fail("node is not on PATH in CI; the Worker contract cannot run")
        pytest.skip("node is not installed; the Worker contract runs in CI")
    return node


def test_the_worker_contract_passes() -> None:
    proc = subprocess.run(
        [_node(), "--test", "test/worker.test.mjs"],
        cwd=WORKER, capture_output=True, text=True, timeout=120, check=False,
    )
    assert proc.returncode == 0, proc.stdout[-4000:] + proc.stderr[-2000:]
    assert "# fail 0" in proc.stdout


def test_the_worker_and_the_fetcher_agree_on_header_names() -> None:
    source = (WORKER / "src" / "worker.js").read_text()
    assert f'const TOKEN_HEADER = "{weather.PROXY_TOKEN_HEADER}";' in source
    assert f'const ERROR_HEADER = "{weather.PROXY_ERROR_HEADER}";' in source


def test_the_worker_allowlists_exactly_the_path_the_fetcher_calls() -> None:
    import config

    source = (WORKER / "src" / "worker.js").read_text()
    assert f'"{config.OPENMETEO_FORECAST_PATH}": "https://api.open-meteo.com",' in source
    assert config.OPENMETEO_FORECAST_URL == "https://api.open-meteo.com" + config.OPENMETEO_FORECAST_PATH
