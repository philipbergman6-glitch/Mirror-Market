"""CI alerter: turn pipeline layer failures into a GitHub issue.

Runs in the deploy workflow right after `python main.py` (even when that
step failed). Reads the machine-readable run summary main.py writes to
data/storage/pipeline_status.json and:

- status file missing        -> the pipeline crashed before finishing: alert
- hard_failures non-empty    -> alert (transport/parse outages, not quiet
                                empties like a SAFEX holiday)
- clean run                  -> close any open alert issue

One rolling issue (label `ci-alert`) carries the whole outage: created on
the first failing run, commented on subsequent ones, closed by the first
green run. Uses the gh CLI with the workflow-provided GH_TOKEN. Exits
non-zero on gh errors; the workflow step is continue-on-error so a broken
alerter never blocks the dashboard deploy.

Usage:
    python scripts/ci_layer_alert.py [path/to/pipeline_status.json]
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

ALERT_LABEL = "ci-alert"
ALERT_TITLE = "CI: data pipeline layer failures"
DEFAULT_STATUS_PATH = Path(__file__).resolve().parent.parent / "data" / "storage" / "pipeline_status.json"


def _gh(*args: str) -> str:
    """Run a gh CLI command and return stdout; raises on non-zero exit."""
    result = subprocess.run(
        ["gh", *args], capture_output=True, text=True, check=True,
    )
    return result.stdout


def load_status(path: Path) -> dict | None:
    """Parse the status file; None means the pipeline never finished."""
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def run_url() -> str:
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    if repo and run_id:
        return f"{server}/{repo}/actions/runs/{run_id}"
    return "(run URL unavailable — not running in GitHub Actions?)"


def build_alert_body(status: dict | None) -> str:
    """Markdown body for the alert issue / follow-up comment."""
    if status is None:
        return (
            "The data pipeline **crashed before writing its run summary** — "
            "no layer results are available (early abort, e.g. history "
            f"import failure or an unhandled exception).\n\nRun: {run_url()}"
        )
    classes = status.get("classifications") or {
        "upstream_failure": status.get("hard_failures", [])
    }
    labels = (
        ("upstream_failure", "Upstream or ingest failure"),
        ("stale_last_known_good", "Stale last-known-good data"),
        ("incomplete_key_coverage", "Incomplete key coverage"),
    )
    lines = ["The data pipeline finished with degraded production layers:", ""]
    for key, heading in labels:
        layers = classes.get(key, [])
        if layers:
            lines += [f"**{heading}:**", *[f"- `{layer}`" for layer in layers], ""]
    no_publication = classes.get("no_publication", [])
    if no_publication:
        lines += [
            "Legitimate no-publication layers (informational, not outages):",
            *[f"- `{layer}`" for layer in no_publication],
            "",
        ]
    critical = status.get("critical_failures", [])
    if critical:
        lines += ["", f"**Critical layers failed:** {', '.join(critical)} — the deploy was blocked."]
    lines += [f"Run: {run_url()}"]
    return "\n".join(lines)


def find_open_alert_issue() -> int | None:
    out = _gh(
        "issue", "list", "--label", ALERT_LABEL, "--state", "open",
        "--json", "number", "--limit", "1",
    )
    issues = json.loads(out)
    return issues[0]["number"] if issues else None


def raise_alert(body: str) -> None:
    existing = find_open_alert_issue()
    if existing is not None:
        logger.info("Commenting on open alert issue #%d", existing)
        _gh("issue", "comment", str(existing), "--body", body)
        return
    # --force makes label creation idempotent (updates if it exists)
    _gh(
        "label", "create", ALERT_LABEL, "--force",
        "--description", "Automated pipeline-outage alert",
        "--color", "D93F0B",
    )
    logger.info("Opening new alert issue")
    _gh(
        "issue", "create", "--title", ALERT_TITLE,
        "--label", ALERT_LABEL, "--body", body,
    )


def clear_alert(status: dict) -> None:
    existing = find_open_alert_issue()
    if existing is None:
        logger.info("Clean run, no open alert issue — nothing to do")
        return
    logger.info("Clean run — closing alert issue #%d", existing)
    _gh(
        "issue", "close", str(existing), "--comment",
        f"All layers recovered in a green run ({len(status.get('succeeded', []))} "
        f"succeeded, 0 hard failures).\n\nRun: {run_url()}",
    )


def main(argv: list[str]) -> int:
    path = Path(argv[1]) if len(argv) > 1 else DEFAULT_STATUS_PATH
    status = load_status(path)

    if status is None:
        logger.error("Status file missing at %s — treating as pipeline crash", path)
        raise_alert(build_alert_body(None))
        return 0

    hard_failures = status.get("hard_failures", [])
    if hard_failures:
        logger.warning("Hard failures this run: %s", ", ".join(hard_failures))
        raise_alert(build_alert_body(status))
    else:
        clear_alert(status)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
