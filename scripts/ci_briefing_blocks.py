"""CI relay: a briefing block that failed reaches the run page and an issue (B45 #562).

Since #558 a snapshot block whose builder raised is recorded in the archived
snapshot as ``failed_blocks: {name: reason}`` — but the edition still
publishes, with that block withheld, and the warning lived only in the deploy
log. Neither ``scripts/ci_layer_alert.py`` (layers, before the briefing is
built) nor ``scripts/ci_deploy_alert.py`` ("the edition was not promoted")
can see it. This relay reads it and:

- prints one ``::warning::`` annotation per failed block, so it shows on the
  run summary page;
- keeps a rolling issue (label ``ci-briefing-blocks``): opened on the first
  failing run, commented **only when the set of failed blocks changes**,
  closed by a run whose snapshot says ``failed_blocks == {}``.

The source is the ``briefings`` row *this run* archived in the runner's DB —
the row ``generated_at`` at or after ``PIPELINE_STARTED_AT``, the same "rows
written after this instant" rule ``scripts/record_layer_runs.py`` grades
``data_freshness`` by. The DB is seeded from the committed CSV, so an older
row is a previous run's and says nothing about this one.

"Before" is what the open issue last said (a marker in each body/comment),
not the previous archived row: the deploy runs on every push to main, and
``INSERT OR REPLACE`` keyed on the date erases an earlier same-day run's row.

Two outcomes are neither failing nor clean (invariant 2), and leave the issue
as it was with a warning annotation: no row archived by this run, and a
snapshot without the ``failed_blocks`` key (schema v1–v3: never recorded).
Anything unreadable — no start stamp, no DB, a snapshot or ``failed_blocks``
of the wrong shape — raises. The workflow step is ``continue-on-error``, so a
broken relay never reddens a run.

Usage:
    python scripts/ci_briefing_blocks.py [--started-at ISO] [--db PATH]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config  # noqa: E402 — needs the project root on sys.path

logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

LABEL = "ci-briefing-blocks"
TITLE = "CI: briefing blocks failed — withheld from the published edition"
# The stored format of briefings.generated_at (pipeline.store.save_briefing).
GENERATED_AT_FORMAT = "%Y-%m-%d %H:%M:%S"
_MARKER = re.compile(r"<!-- ci-briefing-blocks: (\[.*?\]) -->")


class BriefingBlocksError(RuntimeError):
    """The relay cannot tell what this run's briefing recorded."""


def _gh(*args: str) -> str:
    result = subprocess.run(["gh", *args], capture_output=True, text=True, check=True)
    return result.stdout


def run_url() -> str:
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    if repo and run_id:
        return f"{server}/{repo}/actions/runs/{run_id}"
    return "(run URL unavailable — not running in GitHub Actions?)"


def _escape(value: str) -> str:
    """Workflow-command data escaping: ``%``, CR and LF would end or corrupt the command."""
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def annotation(name: str, reason: str) -> str:
    return f"::warning title=Briefing block failed::{_escape(f'{name} — {reason}')}"


def this_runs_briefing(db: Path, started: datetime) -> tuple[str, str | None] | None:
    """``(briefing_date, snapshot_json)`` of the newest row this run archived; None if none."""
    if not db.exists():
        raise BriefingBlocksError(f"no database at {db}")
    since = started.astimezone(timezone.utc).strftime(GENERATED_AT_FORMAT)
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        row = conn.execute(
            "SELECT briefing_date, snapshot_json FROM briefings WHERE generated_at >= ? "
            "ORDER BY briefing_date DESC, generated_at DESC LIMIT 1",
            (since,),
        ).fetchone()
    finally:
        conn.close()
    return None if row is None else (row[0], row[1])


def failed_blocks(snapshot_json: str | None) -> dict[str, str] | None:
    """The snapshot's ``failed_blocks``; None when the key is absent (never recorded)."""
    try:
        snapshot = json.loads(snapshot_json or "")
    except json.JSONDecodeError as exc:
        raise BriefingBlocksError(f"snapshot_json is not JSON: {exc}") from exc
    if not isinstance(snapshot, dict):
        raise BriefingBlocksError(f"snapshot_json is a {type(snapshot).__name__}, not an object")
    if "failed_blocks" not in snapshot:
        return None
    failed = snapshot["failed_blocks"]
    if not isinstance(failed, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in failed.items()
    ):
        raise BriefingBlocksError(f"failed_blocks is not a {{name: reason}} object: {failed!r}")
    return failed


def find_open_issue() -> int | None:
    out = _gh("issue", "list", "--label", LABEL, "--state", "open",
              "--json", "number", "--limit", "1")
    issues = json.loads(out)
    return issues[0]["number"] if issues else None


def last_reported(thread_json: str) -> set[str] | None:
    """The block set the issue's newest marker names; None when no marker is readable."""
    thread = json.loads(thread_json)
    bodies = [thread.get("body") or "", *[c.get("body") or "" for c in thread.get("comments") or []]]
    for body in reversed(bodies):
        markers = _MARKER.findall(body)
        if markers:
            return set(json.loads(markers[-1]))
    return None


def build_body(briefing_date: str, failed: dict[str, str], *, before: set[str] | None) -> str:
    lines = [
        f"The briefing archived for **{briefing_date}** withheld {len(failed)} block(s) "
        "whose builder raised. The edition still published, without them:",
        "",
        *[f"- `{name}` — {reason}" for name, reason in sorted(failed.items())],
        "",
    ]
    if before is not None:
        newly = sorted(set(failed) - before)
        recovered = sorted(before - set(failed))
        if newly:
            lines += [f"**Newly failing:** {', '.join(f'`{n}`' for n in newly)}"]
        if recovered:
            lines += [f"**Recovered:** {', '.join(f'`{n}`' for n in recovered)}"]
        if newly or recovered:
            lines.append("")
    lines += [
        "Source: `failed_blocks` in the `briefings` row's `snapshot_json` "
        "(committed as `data/history/briefings.csv`).",
        f"Run: {run_url()}",
        f"<!-- ci-briefing-blocks: {json.dumps(sorted(failed))} -->",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--started-at", default=os.environ.get("PIPELINE_STARTED_AT"),
                        help="UTC instant the pipeline step began (default: $PIPELINE_STARTED_AT)")
    parser.add_argument("--db", type=Path, default=Path(config.DB_PATH))
    args = parser.parse_args(argv)

    if not args.started_at:
        raise BriefingBlocksError("--started-at / PIPELINE_STARTED_AT is not set")
    started = datetime.fromisoformat(args.started_at.replace("Z", "+00:00"))
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)

    row = this_runs_briefing(args.db, started)
    if row is None:
        print("::warning title=No briefing archived::This run archived no briefings row "
              f"(none generated at or after {args.started_at}), so whether any block failed "
              f"is unknown — the {LABEL} issue is left as it was.")
        return 0
    briefing_date, snapshot_json = row
    failed = failed_blocks(snapshot_json)
    if failed is None:
        print(f"::warning title=Briefing failures never recorded::The {briefing_date} snapshot "
              "has no failed_blocks key (schema before v4): failures were never recorded, which "
              f"is not the same as none — the {LABEL} issue is left as it was.")
        return 0

    for name, reason in sorted(failed.items()):
        print(annotation(name, reason))
    existing = find_open_issue()

    if not failed:
        if existing is None:
            logger.info("Every briefing block built (%s) and no open %s issue", briefing_date, LABEL)
            return 0
        logger.info("Every briefing block built (%s) — closing #%d", briefing_date, existing)
        _gh("issue", "close", str(existing), "--comment",
            f"Every briefing block built in the {briefing_date} snapshot "
            f"(`failed_blocks == {{}}`).\n\nRun: {run_url()}")
        return 0

    logger.warning("Briefing blocks failed (%s): %s", briefing_date, ", ".join(sorted(failed)))
    if existing is None:
        _gh("label", "create", LABEL, "--force", "--description",
            "Automated alert: briefing snapshot blocks that raised", "--color", "FBCA04")
        _gh("issue", "create", "--title", TITLE, "--label", LABEL,
            "--body", build_body(briefing_date, failed, before=None))
        return 0

    before = last_reported(_gh("issue", "view", str(existing), "--json", "body,comments"))
    if before == set(failed):
        logger.info("Failed blocks unchanged since #%d last said — no comment", existing)
        return 0
    _gh("issue", "comment", str(existing), "--body", build_body(briefing_date, failed, before=before))
    return 0


if __name__ == "__main__":
    sys.exit(main())
