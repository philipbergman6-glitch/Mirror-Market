"""Give snapshot-only sources a second chance the same day (#254).

Several layers are snapshot-only: the upstream serves the current day and
overwrites it tomorrow (AMS Gulf bids, India mandi, SAFEX, AgRural, MAGyP —
the set `pipeline/history.py` exists for). The daily pipeline gets exactly
one shot at each of them, so a single layer failure loses that observation
permanently. It has happened: the 2026-08-05 Gulf bid edition died on one
parse failure, and 3 of India mandi's first 4 days died on a throttled key.

This script is the second shot. It runs from its own cron slot a couple of
hours behind the daily build, looks at what that build actually did, and
sends a `repository_dispatch` back to the deploy workflow if — and only if —
the day's last completed pipeline run hard-failed.

WHY THE DECISION IS NOT "was the run red"
    A hard layer failure does not fail the run: `main.py` exits non-zero only
    for a critical layer or a systemic sweep. The 2026-08-05 loss happened
    inside a run GitHub showed as green. The signal is `hard_failures` in the
    run summary main.py writes, which is the *same* key `ci_layer_alert.py`
    keys its outage issue on — deliberately not a third notion of "failed".

WHY A CHAIN IS IMPOSSIBLE
    Two independent reasons, both structural rather than conventional:
    1. Nothing but this script sends the marker event, and this script is
       driven by a cron that fires once per weekday. A retry run cannot
       trigger anything, because runs do not trigger retries — the clock does.
    2. `decide()` refuses outright once a run carrying the marker event
       (`repository_dispatch`) exists for today, whatever its conclusion. The
       marker is the trigger type itself, so it cannot be forgotten or lost:
       a retry is a `repository_dispatch` run by construction.

Usage:
    python scripts/ci_retry_decision.py           # decide and dispatch
    python scripts/ci_retry_decision.py --dry-run # decide and report only
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

# The workflow whose runs are judged, and which the retry re-triggers.
WORKFLOW_FILE = "deploy-dashboard.yml"
# The artifact build-deploy uploads carrying main.py's run summary.
STATUS_ARTIFACT = "pipeline-status"
STATUS_FILENAME = "pipeline_status.json"
# The trigger a retry run arrives on — and therefore the marker that makes a
# retry identifiable in the Actions list and blocks a second one.
RETRY_EVENT = "repository_dispatch"
DISPATCH_TYPE = "pipeline-retry"
# Conclusions that mean the run never reached a verdict on the layers.
INCONCLUSIVE = frozenset({"cancelled", "skipped", "stale", "action_required"})
# Only a full run asks for the snapshot-only layers; a --fast run does not.
FULL_MODE = "full"
# A retry must finish inside the UTC day it is recovering. Past this, the
# snapshot upstreams have already rolled to tomorrow's edition, so the run
# could not recover the lost day even if it succeeded — and its own
# `repository_dispatch` run would land on tomorrow's date and consume
# tomorrow's one retry. Generous against a ~20-minute pipeline.
LATEST_DISPATCH_UTC_MINUTE = 23 * 60 + 20


@dataclass(frozen=True)
class Run:
    """One workflow run, as `gh run list --json` reports it."""

    id: int
    event: str
    status: str
    conclusion: str | None
    created_at: datetime

    @classmethod
    def from_api(cls, payload: dict) -> Run:
        raw = payload["createdAt"]
        try:
            created = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:  # a run we cannot date is a run we cannot judge
            raise ValueError(f"unparseable run createdAt: {raw!r}") from exc
        return cls(
            id=int(payload["databaseId"]),
            event=str(payload["event"]),
            status=str(payload["status"]),
            conclusion=payload.get("conclusion") or None,
            created_at=created.astimezone(timezone.utc),
        )


@dataclass(frozen=True)
class Decision:
    retry: bool
    reason: str
    failed_run_id: int | None = None


def decide(
    *,
    runs: Sequence[Run],
    now: datetime,
    status_of: Callable[[int], dict | None],
) -> Decision:
    """Should a retry run be dispatched? Pure — `status_of` is injected.

    `now` is the UTC moment of the decision; it fixes which day's runs count
    and whether a retry could still land inside that day. `status_of(run_id)`
    returns that run's parsed pipeline_status.json, or None when the run
    never produced one (a crash before the summary write).
    """
    today: date = now.date()
    if now.hour * 60 + now.minute >= LATEST_DISPATCH_UTC_MINUTE:
        return Decision(
            False,
            f"too late in the UTC day ({now:%H:%M}) — a retry could not finish "
            "before the snapshot upstreams roll to tomorrow's edition",
        )

    todays = [r for r in runs if r.created_at.date() == today]

    if any(r.event == RETRY_EVENT for r in todays):
        return Decision(False, "a retry run has already happened today — one per day, no chains")

    completed = sorted(
        (r for r in todays if r.status == "completed" and r.conclusion not in INCONCLUSIVE),
        key=lambda r: r.created_at,
    )
    if not completed:
        if todays:
            return Decision(
                False,
                "the day's pipeline run is still in flight — it has not had its shot yet",
            )
        return Decision(False, "no pipeline run has completed today")

    latest = completed[-1]
    status = status_of(latest.id)
    if status is None:
        return Decision(
            True,
            f"run {latest.id} left no run summary — the pipeline crashed before finishing",
            latest.id,
        )

    mode = status.get("mode")
    if mode != FULL_MODE:
        return Decision(
            False,
            f"run {latest.id} was a '{mode}' refresh, which never asks for the "
            "snapshot-only layers — it cannot judge them",
        )

    hard_failures = status.get("hard_failures") or []
    if hard_failures:
        return Decision(
            True,
            f"run {latest.id} hard-failed {len(hard_failures)} layer(s): "
            f"{', '.join(hard_failures)}",
            latest.id,
        )
    return Decision(False, f"run {latest.id} finished with no hard failures")


# ── the gh shell around the decision ─────────────────────────────────


def _gh(*args: str) -> str:
    result = subprocess.run(["gh", *args], capture_output=True, text=True, check=True)
    return result.stdout


def fetch_runs(limit: int = 30) -> list[Run]:
    out = _gh(
        "run", "list",
        "--workflow", WORKFLOW_FILE,
        "--branch", "main",
        "--limit", str(limit),
        "--json", "databaseId,event,status,conclusion,createdAt",
    )
    return [Run.from_api(item) for item in json.loads(out)]


def fetch_status(run_id: int) -> dict | None:
    """Download the run's status artifact. None if it has none, or it is junk."""
    with tempfile.TemporaryDirectory() as tmp:
        try:
            _gh("run", "download", str(run_id), "--name", STATUS_ARTIFACT, "--dir", tmp)
        except subprocess.CalledProcessError:
            logger.info("Run %d published no %s artifact", run_id, STATUS_ARTIFACT)
            return None
        path = Path(tmp) / STATUS_FILENAME
        if not path.exists():
            logger.warning("Artifact for run %d has no %s", run_id, STATUS_FILENAME)
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning("Run %d wrote an unreadable %s", run_id, STATUS_FILENAME)
            return None


def dispatch_retry(decision: Decision) -> None:
    repo = os.environ.get("GITHUB_REPOSITORY")
    if not repo:
        raise RuntimeError("GITHUB_REPOSITORY is unset — cannot dispatch a retry")
    payload = json.dumps({
        "event_type": DISPATCH_TYPE,
        "client_payload": {
            "reason": decision.reason,
            "failed_run_id": decision.failed_run_id,
        },
    })
    subprocess.run(
        ["gh", "api", "--method", "POST", f"/repos/{repo}/dispatches", "--input", "-"],
        input=payload, text=True, capture_output=True, check=True,
    )
    logger.info("Dispatched '%s' to %s", DISPATCH_TYPE, repo)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="decide but do not dispatch")
    args = parser.parse_args(argv)

    decision = decide(
        runs=fetch_runs(),
        now=datetime.now(timezone.utc),
        status_of=fetch_status,
    )
    if not decision.retry:
        logger.info("No retry: %s", decision.reason)
        return 0

    logger.warning("Retry warranted: %s", decision.reason)
    if args.dry_run:
        logger.info("--dry-run: not dispatching")
        return 0
    dispatch_retry(decision)
    return 0


if __name__ == "__main__":
    sys.exit(main())
