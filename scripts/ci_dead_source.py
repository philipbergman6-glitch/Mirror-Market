"""CI relay: a layer unavailable on consecutive sessions is a dead source (B34 #520 §4).

The per-run ``ci-alert`` comment fires on every failure, so a one-run blip
and a source dead for a week read the same; the only way to see "dead for
five sessions" was to diff a thread by eye. This relay keeps a distinct
rolling issue (label ``ci-dead-source``) for the second case:

- a layer unavailable on ``config.DEAD_SOURCE_SESSIONS`` consecutive
  sessions (all production layers; availability-basket layers tagged
  *decision-impairing*) → open the issue, or comment on it;
- a dead layer that recovers → comment, with the streak it closed;
- nothing dead → close the issue.

It comments **only on a state change**, so a bad week sends fewer messages,
not more. The streak is computed from ``data/history/layer_runs.csv`` by
``pipeline.reliability.dead_layers`` — never a second streak table. "Before"
is the same computation over the record without this run's rows, so the
relay needs no memory of its own.

Like ``scripts/ci_layer_alert.py`` it is a relay: the workflow step is
``continue-on-error`` and a broken relay never reddens a run.

Usage:
    python scripts/ci_dead_source.py [--history-dir PATH]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config  # noqa: E402 — needs the project root on sys.path
from pipeline import history, layer_runs, reliability  # noqa: E402
from pipeline.reliability import Streak  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

DEAD_LABEL = "ci-dead-source"
DEAD_TITLE = "CI: dead source — layer unavailable for consecutive sessions"


def _gh(*args: str) -> str:
    result = subprocess.run(["gh", *args], capture_output=True, text=True, check=True)
    return result.stdout


def find_open_issue() -> int | None:
    out = _gh("issue", "list", "--label", DEAD_LABEL, "--state", "open",
              "--json", "number", "--limit", "1")
    issues = json.loads(out)
    return issues[0]["number"] if issues else None


def run_url() -> str:
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    if repo and run_id:
        return f"{server}/{repo}/actions/runs/{run_id}"
    return "(run URL unavailable — not running in GitHub Actions?)"


def _line(streak: Streak) -> str:
    tag = " — **decision-impairing** (availability basket)" if streak.layer in config.AVAILABILITY_BASKET else ""
    if streak.opens_at_record_start:
        # The record cannot see past its own first session, so the streak is a
        # floor; printing that session as the start would date the outage (#566).
        span = (
            f"at least {streak.sessions} consecutive sessions — every one on record "
            f"(the record begins {streak.first_session.isoformat()}; through "
            f"{streak.last_session.isoformat()})"
        )
    else:
        span = (
            f"{streak.sessions} consecutive sessions "
            f"({streak.first_session.isoformat()} → {streak.last_session.isoformat()})"
        )
    return f"- `{streak.layer}`{tag}: unavailable {span}, last graded `{streak.last_status}`"


def build_body(now: dict[str, Streak], newly: list[str], recovered: dict[str, Streak]) -> str:
    lines: list[str] = []
    if newly:
        lines += [f"**Now dead** (≥ {config.DEAD_SOURCE_SESSIONS} sessions unavailable):",
                  *[_line(now[name]) for name in newly], ""]
    if recovered:
        lines += ["**Recovered:**", *[
            f"- `{name}` is available again after {streak.sessions} unavailable session(s) "
            f"({streak.first_session.isoformat()} → {streak.last_session.isoformat()})"
            for name, streak in sorted(recovered.items())
        ], ""]
    still = sorted(set(now) - set(newly))
    if still:
        lines += ["**Still dead:**", *[_line(now[name]) for name in still], ""]
    lines += [
        "Sessions, not runs: a weekday with no run at all counts as unavailable (`no_run`). "
        "Record: `data/history/layer_runs.csv`.",
        f"Run: {run_url()}",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--history-dir", type=Path, default=None,
                        help="the data/history/ holding layer_runs.csv (default: config.HISTORY_DIR)")
    args = parser.parse_args(argv)
    if args.history_dir is not None:
        history.HISTORY_DIR = str(args.history_dir)

    rows = layer_runs.read_runs()
    run_id, _workflow = layer_runs.run_identity(os.environ)
    now = reliability.dead_layers(rows)
    before = reliability.dead_layers([r for r in rows if r.run_id != run_id])
    newly = sorted(set(now) - set(before))
    recovered = {name: before[name] for name in sorted(set(before) - set(now))}
    existing = find_open_issue()

    if not now:
        if existing is None:
            logger.info("No dead source and no open %s issue — nothing to do", DEAD_LABEL)
            return 0
        logger.info("No dead source — closing #%d", existing)
        _gh("issue", "close", str(existing), "--comment", build_body(now, newly, recovered))
        return 0

    if existing is None:
        logger.warning("Dead source(s): %s — opening a %s issue", ", ".join(now), DEAD_LABEL)
        _gh("label", "create", DEAD_LABEL, "--force", "--description",
            "Automated alert: a layer unavailable for consecutive sessions", "--color", "B60205")
        _gh("issue", "create", "--title", DEAD_TITLE, "--label", DEAD_LABEL,
            "--body", build_body(now, sorted(now), recovered))
        return 0

    if newly or recovered:
        logger.warning("Dead-source state changed: new %s, recovered %s", newly, list(recovered))
        _gh("issue", "comment", str(existing), "--body", build_body(now, newly, recovered))
    else:
        logger.info("Dead sources unchanged (%s) — no comment", ", ".join(now))
    return 0


if __name__ == "__main__":
    sys.exit(main())
