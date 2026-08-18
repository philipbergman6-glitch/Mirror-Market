"""CI alerter: turn publication-stage failures into a GitHub issue.

Runs as a separate job after build-deploy (always()), receiving the job
results as arguments. Companion to scripts/ci_layer_alert.py, which covers
*data layer* outages inside the pipeline step; this one distinguishes CI-gate,
page-generation, candidate-contract, Pages-deployment, and public-smoke
failures.

- any classified publication stage failed -> alert with the precise stage
- build-deploy skipped + check failed      -> alert (CI gate blocked deploy)
- build-deploy and public smoke succeeded  -> close any open alert issue
- anything else (cancelled, skipped+ok)   -> no-op

One rolling issue (label `ci-deploy-alert`) carries the whole outage:
created on the first failing run, commented on subsequent ones, closed by
the first green run. Uses the gh CLI with the workflow-provided GH_TOKEN.

Usage:
    python scripts/ci_deploy_alert.py <build> <check> [<generation> <promotion> <deployment> <postdeploy>]

where each result is a GitHub Actions job result: success | failure |
cancelled | skipped. Unknown values hard-fail.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

ALERT_LABEL = "ci-deploy-alert"
ALERT_TITLE = "CI: dashboard deploy failed"
VALID_RESULTS = {"success", "failure", "cancelled", "skipped"}


def _gh(*args: str) -> str:
    """Run a gh CLI command and return stdout; raises on non-zero exit."""
    result = subprocess.run(
        ["gh", *args], capture_output=True, text=True, check=True,
    )
    return result.stdout


def run_url() -> str:
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    if repo and run_id:
        return f"{server}/{repo}/actions/runs/{run_id}"
    return "(run URL unavailable — not running in GitHub Actions?)"


def build_alert_body(
    build_result: str,
    check_result: str,
    generation_result: str = "",
    promotion_result: str = "",
    deployment_result: str = "",
    postdeploy_result: str = "",
) -> str:
    if generation_result == "failure":
        reason = (
            "The **page-generation step failed**. The private candidate was rejected, "
            "so the last trustworthy public edition remains in place."
        )
    elif promotion_result == "failure":
        reason = (
            "The **candidate promotion contract failed**. The private candidate was not deployed, "
            "so the last trustworthy public edition remains in place."
        )
    elif postdeploy_result == "failure":
        reason = "The **post-deployment public smoke test failed** after Pages reported a deployment."
    elif build_result == "failure" and deployment_result == "skipped":
        reason = (
            "The **build failed before page generation or deployment** (for example, an "
            "upstream pipeline or history-persistence failure). The data-layer alert carries "
            "the per-source classification; the public edition was left unchanged."
        )
    elif deployment_result == "failure" or build_result == "failure":
        reason = (
            "The **GitHub Pages deployment failed** after candidate verification; "
            "all retry attempts were exhausted."
        )
    else:
        reason = (
            "The **CI check gate failed**, so the deploy never ran — "
            "the dashboard did not update."
        )
    return f"{reason}\n\nRun: {run_url()}"


def find_open_alert_issues() -> list[int]:
    out = _gh(
        "issue", "list", "--label", ALERT_LABEL, "--state", "open",
        "--json", "number", "--limit", "20",
    )
    return [issue["number"] for issue in json.loads(out)]


def raise_alert(body: str) -> None:
    existing = find_open_alert_issues()
    if existing:
        logger.info("Commenting on open alert issue #%d", existing[0])
        _gh("issue", "comment", str(existing[0]), "--body", body)
        return
    # --force makes label creation idempotent (updates if it exists)
    _gh(
        "label", "create", ALERT_LABEL, "--force",
        "--description", "Automated dashboard-deploy-outage alert",
        "--color", "B60205",
    )
    logger.info("Opening new alert issue")
    _gh(
        "issue", "create", "--title", ALERT_TITLE,
        "--label", ALERT_LABEL, "--body", body,
    )


def clear_alert() -> None:
    existing = find_open_alert_issues()
    if not existing:
        logger.info("Green deploy, no open alert issue — nothing to do")
        return
    # close all — a list-consistency race can leave a duplicate open
    for number in existing:
        logger.info("Green deploy — closing alert issue #%d", number)
        _gh(
            "issue", "close", str(number), "--comment",
            f"Dashboard deployed successfully in a green run.\n\nRun: {run_url()}",
        )


def main(argv: list[str]) -> int:
    if len(argv) not in (3, 7):
        logger.error("Usage: ci_deploy_alert.py <build> <check> [<generation> <promotion> <deployment> <postdeploy>]")
        return 2
    build_result, check_result = argv[1], argv[2]
    detail_results = argv[3:7] if len(argv) == 7 else ["", "", "", ""]
    for name, value in (("build_deploy", build_result), ("check", check_result)):
        if value not in VALID_RESULTS:
            logger.error("Invalid %s result %r — expected one of %s", name, value, sorted(VALID_RESULTS))
            return 2

    if build_result == "failure" or (build_result == "skipped" and check_result == "failure"):
        raise_alert(build_alert_body(build_result, check_result, *detail_results))
    elif build_result == "success":
        clear_alert()
    else:
        logger.info(
            "No action for build_deploy=%s / check=%s (cancelled or skipped without failure)",
            build_result, check_result,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
