"""CI relay: warn before GitHub's 60-day inactivity rule disables the schedules (#6).

In a public repository GitHub disables scheduled workflows "when no repository
activity has occurred in 60 days". That is how the site went dark in July
2026: the "Update Dashboard" schedule was set to ``disabled_inactivity``
~60 days after the last commit, with nothing surfaced. The docs never define
"activity", and in particular never say whether the deploy's own daily
``github-actions[bot]`` history commits reset the clock. This repo has never
gone 60 days without a human commit since those began (2026-07-30), so it is
unknown here too.

A staleness check living inside a scheduled workflow dies with the schedule,
so this relay warns *before* the deadline, while every workflow still runs:

- **disabled** — any workflow reads ``disabled_inactivity``: the clock already
  ran out. Error annotation, rolling ``ci-inactivity-clock`` issue.
- **warning** — the newest non-bot commit on ``main`` is at least
  ``config.SCHEDULE_INACTIVITY_WARN_DAYS`` old (or none in the lookback):
  warning annotation and the issue, naming the deadline.
- **past_deadline_active** — ``config.SCHEDULE_INACTIVITY_CONCLUDE_DAYS``
  (a margin past GitHub's ``GITHUB_SCHEDULE_INACTIVITY_DAYS``, whose disable
  is a background sweep, not a timer) have passed without a non-bot commit and
  nothing is disabled: evidence that some activity — the bot commits, or the
  relays' own issue traffic — holds the clock off. Notice annotation; the
  issue is closed with that evidence. Between the deadline and the margin the
  state stays **warning**.
- **ok** — otherwise; an open issue is closed.

It comments only on a state change (a marker in each body/comment). Like the
other ``ci_*`` relays the workflow step is ``continue-on-error``; malformed API
data raises rather than reading as "fine".

Usage:
    python scripts/ci_inactivity_clock.py
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config  # noqa: E402 — needs the project root on sys.path

logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

LABEL = "ci-inactivity-clock"
TITLE = "CI: scheduled workflows near GitHub's 60-day inactivity disable"
DISABLED_STATE = "disabled_inactivity"
# Fetch a little past the deadline so "none in the lookback" means "past it".
LOOKBACK_DAYS = config.SCHEDULE_INACTIVITY_CONCLUDE_DAYS + 7
_MARKER = re.compile(r"<!-- ci-inactivity-clock: (\w+) -->")


@dataclass(frozen=True)
class Clock:
    state: str  # ok | warning | past_deadline_active | disabled
    last_human_at: datetime | None  # None: no non-bot commit inside the lookback
    days: int | None
    disabled: list[str] = field(default_factory=list)

    @property
    def deadline(self) -> datetime | None:
        if self.last_human_at is None:
            return None
        return self.last_human_at + timedelta(days=config.GITHUB_SCHEDULE_INACTIVITY_DAYS)


def _parse_time(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"commit date is not a string: {value!r}")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def is_human(commit: dict) -> bool:
    """A commit whose GitHub author is a ``User``. Bots and unlinked authors don't count.

    An unlinked author (``author: null``) is left out on purpose: the warning
    then fires early, never late.
    """
    author = commit.get("author")
    return isinstance(author, dict) and author.get("type") == "User"


def assess(commits: list[dict], workflows: list[dict], now: datetime) -> Clock:
    if not workflows:
        raise ValueError("the workflows API listed no workflows — refusing to read that as healthy")
    disabled = sorted(w["path"] for w in workflows if w["state"] == DISABLED_STATE)
    human_times = [_parse_time(c["commit"]["committer"]["date"]) for c in commits if is_human(c)]
    last = max(human_times) if human_times else None
    days = (now - last).days if last is not None else None
    if disabled:
        state = "disabled"
    elif days is None or days >= config.SCHEDULE_INACTIVITY_CONCLUDE_DAYS:
        state = "past_deadline_active"
    elif days >= config.SCHEDULE_INACTIVITY_WARN_DAYS:
        state = "warning"
    else:
        state = "ok"
    return Clock(state=state, last_human_at=last, days=days, disabled=disabled)


def _sentence(text: str) -> str:
    return text[:1].upper() + text[1:]


def _since(clock: Clock) -> str:
    if clock.last_human_at is None:
        return f"no non-bot commit on `main` in the last {LOOKBACK_DAYS} days"
    return (f"the newest non-bot commit on `main` landed {clock.last_human_at:%Y-%m-%d %H:%M} UTC, "
            f"{clock.days} days ago")


def annotation(clock: Clock) -> str | None:
    if clock.state == "disabled":
        return (f"::error title=Schedules disabled by GitHub::{', '.join(clock.disabled)} "
                f"read {DISABLED_STATE}; re-enable with `gh workflow enable` (#6)")
    if clock.state == "warning":
        assert clock.deadline is not None
        return (f"::warning title=Inactivity clock::{_since(clock)}; GitHub may disable every "
                f"scheduled workflow from {clock.deadline:%Y-%m-%d} (#6)")
    if clock.state == "past_deadline_active":
        return (f"::notice title=Inactivity clock::{_since(clock)} and no workflow is disabled — "
                "evidence that some activity (bot commits or the relays' own issue traffic) holds the clock off (#6)")
    return None


def build_body(clock: Clock, run: str) -> str:
    lines: list[str]
    if clock.state == "disabled":
        lines = [
            f"**GitHub has disabled scheduled workflows for inactivity** ({_since(clock)}):",
            *[f"- `{path}` — `{DISABLED_STATE}`" for path in clock.disabled],
            "",
            "Re-enable each one (`gh workflow enable <file>`), then push any commit to `main`. "
            "The daily github-actions[bot] history commits did not hold the clock off.",
        ]
    elif clock.state == "warning":
        assert clock.deadline is not None
        lines = [
            f"{_sentence(_since(clock))}. GitHub disables a public repository's scheduled "
            f"workflows after {config.GITHUB_SCHEDULE_INACTIVITY_DAYS} days without repository "
            f"activity, so they may stop from **{clock.deadline:%Y-%m-%d}**.",
            "",
            "GitHub's docs don't say whether the deploy's daily github-actions[bot] history "
            "commits count as activity. Any human commit to `main` resets this clock for certain. "
            f"If every workflow is still active {config.SCHEDULE_INACTIVITY_CONCLUDE_DAYS} days after "
            "the last human commit, this issue closes itself with that evidence.",
        ]
    elif clock.state == "past_deadline_active":
        lines = [
            f"{_sentence(_since(clock))}, at least "
            f"{config.SCHEDULE_INACTIVITY_CONCLUDE_DAYS - config.GITHUB_SCHEDULE_INACTIVITY_DAYS} days past "
            f"GitHub's {config.GITHUB_SCHEDULE_INACTIVITY_DAYS}-day line, and no workflow reads "
            f"`{DISABLED_STATE}`: evidence that the bot's daily commits (or other activity) "
            "reset the clock — record it on #6.",
        ]
    else:
        lines = [f"Clock reset: {_since(clock)}."]
    lines += ["", f"Run: {run}", f"<!-- ci-inactivity-clock: {clock.state} -->"]
    return "\n".join(lines)


def last_reported(thread: dict) -> str | None:
    bodies = [thread.get("body") or "", *[c.get("body") or "" for c in thread.get("comments") or []]]
    for body in reversed(bodies):
        found = _MARKER.findall(body)
        if found:
            return str(found[-1])
    return None


def _gh(*args: str) -> str:
    return subprocess.run(["gh", *args], capture_output=True, text=True, check=True).stdout


def run_url() -> str:
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    if repo and run_id:
        return f"{server}/{repo}/actions/runs/{run_id}"
    return "(run URL unavailable — not running in GitHub Actions?)"


def fetch(repo: str, now: datetime) -> tuple[list[dict], list[dict]]:
    since = (now - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    pages = json.loads(_gh("api", "--paginate", "--slurp",
                           f"repos/{repo}/commits?sha=main&since={since}&per_page=100"))
    commits = [c for page in pages for c in page]
    workflows = json.loads(_gh("api", f"repos/{repo}/actions/workflows?per_page=100"))["workflows"]
    return commits, workflows


def relay(clock: Clock, run: str) -> None:
    out = _gh("issue", "list", "--label", LABEL, "--state", "open", "--json", "number", "--limit", "1")
    issues = json.loads(out)
    existing = issues[0]["number"] if issues else None
    body = build_body(clock, run)

    if clock.state in ("ok", "past_deadline_active"):
        if existing is not None:
            logger.info("Inactivity clock %s — closing #%d", clock.state, existing)
            _gh("issue", "close", str(existing), "--comment", body)
        return

    if existing is None:
        logger.warning("Inactivity clock %s — opening a %s issue", clock.state, LABEL)
        _gh("label", "create", LABEL, "--force", "--description",
            "Automated alert: scheduled workflows near or past GitHub's inactivity disable",
            "--color", "B60205")
        _gh("issue", "create", "--title", TITLE, "--label", LABEL, "--body", body)
        return

    thread = json.loads(_gh("issue", "view", str(existing), "--json", "body,comments"))
    if last_reported(thread) != clock.state:
        _gh("issue", "comment", str(existing), "--body", body)
    else:
        logger.info("Inactivity clock still %s — no comment", clock.state)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"),
                        help="owner/name (default: $GITHUB_REPOSITORY)")
    args = parser.parse_args(argv)
    if not args.repo:
        parser.error("--repo is required outside GitHub Actions")

    now = datetime.now(timezone.utc)
    commits, workflows = fetch(args.repo, now)
    clock = assess(commits, workflows, now)
    logger.info("Inactivity clock: %s — %s", clock.state, _since(clock))
    line = annotation(clock)
    if line:
        print(line)
    relay(clock, run_url())
    return 0


if __name__ == "__main__":
    sys.exit(main())
