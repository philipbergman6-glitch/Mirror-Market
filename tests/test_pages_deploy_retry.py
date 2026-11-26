"""Every Pages deploy survives the artifact-listing lag (#617).

`deploy-pages` lists the run's artifacts ~0.5 s after `upload-pages-artifact`
finalizes, and the listing can lag: "Found 0 artifact(s)" / "No artifacts named
github-pages" on Refresh Prices runs 44065512441 (attempt 1), 45328204442,
45344562996 and 45375495287, and on daily build 45536043506, whose 30 s retry
found the artifact and deployed. Refresh Prices had no retry, so each was a red
run for an edition that was valid. These tests pin the shape both deploying
workflows must keep.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

WORKFLOWS = Path(__file__).resolve().parent.parent / ".github" / "workflows"
DEPLOYING = ("deploy-dashboard.yml", "refresh-prices.yml")


def _steps(name: str) -> list[dict]:
    doc = yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))
    return [step for job in doc["jobs"].values() for step in job.get("steps", [])]


def _deploys(name: str) -> list[dict]:
    return [s for s in _steps(name) if str(s.get("uses", "")).startswith("actions/deploy-pages@")]


@pytest.mark.parametrize("name", DEPLOYING)
def test_deploy_is_retried_and_the_last_attempt_still_fails_the_run(name):
    deploys = _deploys(name)
    assert len(deploys) >= 2, f"{name}: one deploy-pages call, no retry for the listing lag"
    assert all(d.get("continue-on-error") is True for d in deploys[:-1])
    assert not deploys[-1].get("continue-on-error"), f"{name}: every attempt failing must stay red"
    for retry in deploys[1:]:
        assert "steps.deployment.outcome == 'failure'" in str(retry.get("if", ""))


@pytest.mark.parametrize("name", DEPLOYING)
def test_a_wait_precedes_every_retry(name):
    steps = _steps(name)
    for retry in _deploys(name)[1:]:
        before = steps[steps.index(retry) - 1]
        assert str(before.get("run", "")).startswith("sleep "), f"{name}: retry {retry['id']} fires with no wait"


@pytest.mark.parametrize("name", DEPLOYING)
def test_every_page_url_reader_accepts_whichever_attempt_deployed(name):
    text = (WORKFLOWS / name).read_text(encoding="utf-8")
    ids = [d["id"] for d in _deploys(name)]
    for line in text.splitlines():
        if ".outputs.page_url" in line:
            for step_id in ids:
                assert f"steps.{step_id}.outputs.page_url" in line, f"{name}: {line.strip()!r} misses {step_id}"


def test_refresh_prices_deploys_the_artifact_this_attempt_uploaded():
    """A re-run must not meet its first attempt's artifact (re-run of 44065512441:
    "Multiple artifacts named github-pages ... count is 2")."""
    steps = _steps("refresh-prices.yml")
    upload = next(s for s in steps if str(s.get("uses", "")).startswith("actions/upload-pages-artifact@"))
    name = upload["with"]["name"]
    assert "github.run_attempt" in name
    assert all(d["with"]["artifact_name"] == name for d in _deploys("refresh-prices.yml"))
