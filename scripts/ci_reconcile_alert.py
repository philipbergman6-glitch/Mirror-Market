"""CI relay: a v1-vs-trusted reconciliation divergence opens a rolling issue (#239).

Owner decision on #239 (2026-11-25): the ``reconcile-trusted`` job stays as a
drift detector on probation until the 2027-03-01 checkpoint. Its alarm is one
rolling issue (label ``ci-reconcile-divergence``), **never a red Update
Dashboard run**: a red deploy run must keep meaning "the edition did not go
out", and this job does not gate the publish.

Per pilot, from its JSON report in ``data/reconciliation/`` and the exit code
the workflow captured for its step:

- exit 0/1 with a matching report → graded: ``reconciled``, ``diverged``, or a
  no-session/no-circular day (nothing to compare);
- exit 2 → ``unavailable`` (upstream down or a payload that would not parse —
  not evidence about the trusted path, so it never opens the issue);
- no exit code → ``not_run`` (an earlier step was cancelled);
- anything else — exit 1 without a diverged report, exit 0 without a report, an
  unknown code → ``errored``. A crashed reconciler is a dead detector, and a
  detector that dies silently is the failure invariant 1 forbids, so it alarms.

A pilot that graded nothing this run (no session, unavailable, not run,
errored) **carries** its previous divergences: not re-checking a leg is not
the same as it having recovered. The state is the set of alarm keys
(``cbot:Soybean Meal``, ``fx:ZAR/USD``, ``magyp:FOB``, ``<pilot>:errored``),
written into each issue body or comment as a hidden marker; "before" is the
newest marker on the open issue, so the relay needs no memory of its own.

It comments **only on a state change**, and closes the issue once nothing is
diverged. FX pairs in ``trust.read_path.V1_SOURCE_DIFFERS`` (CNY) are never
graded by ``scripts/reconcile_fx.py``; they are listed here as "not compared:
v1 source differs" and cannot open the issue.

Like ``scripts/ci_dead_source.py`` it is a relay: the workflow step is
``continue-on-error`` and a broken relay never reddens a run. On a CI runner
``data/reconciliation/`` is gitignored, so every report it reads is this run's.

Usage:
    python scripts/ci_reconcile_alert.py --exit magyp=0 --exit cbot=1 --exit fx=0 [--report-dir PATH]
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
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent

logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

ALARM_LABEL = "ci-reconcile-divergence"
ALARM_TITLE = "CI: trusted reconciliation diverged from v1"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "data" / "reconciliation"

#: Pilot key → (report file, display name), in body order.
PILOTS: dict[str, tuple[str, str]] = {
    "magyp": ("magyp_fob.json", "MAGyP FOB"),
    "cbot": ("cbot_benchmarks.json", "CBOT named contracts (DT-16)"),
    "fx": ("fx.json", "Required FX"),
}

GRADED_STATUSES = frozenset({"reconciled", "diverged"})
NOTHING_TO_COMPARE = frozenset({"no_session", "no_circular"})

_MARKER = "reconcile-alarm-state"
_MARKER_RE = re.compile(r"<!-- " + _MARKER + r": (.*?) -->")


@dataclass
class PilotResult:
    """What one pilot's step said this run."""

    pilot: str
    state: str  # reconciled | diverged | nothing_to_compare | unavailable | not_run | errored
    detail: str
    diverged: dict[str, str] = field(default_factory=dict)  # alarm key → one-line reason
    not_compared: list[str] = field(default_factory=list)

    @property
    def graded(self) -> bool:
        return self.state in GRADED_STATUSES


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


# ---------------------------------------------------------------------------
# Reading the reports
# ---------------------------------------------------------------------------


def _counts(reconciliation: dict[str, Any]) -> str:
    return (
        f"{reconciliation.get('matched_rows', 0)} matched, "
        f"{len(reconciliation.get('missing_in_trusted') or [])} only in v1, "
        f"{len(reconciliation.get('missing_in_legacy') or [])} only in trusted, "
        f"{len(reconciliation.get('field_differences') or [])} field differences"
    )


def _diverged_items(pilot: str, report: dict[str, Any]) -> dict[str, str]:
    if pilot == "magyp":
        reconciliation = report.get("reconciliation") or {}
        return {"magyp:FOB": f"circular {report.get('resolved_day')}: {_counts(reconciliation)}"}
    if pilot == "cbot":
        rules = report.get("rejected_by_rule") or {}
        rule_note = (
            "; ledger rejected by rule: " + ", ".join(f"`{rule}` ×{n}" for rule, n in sorted(rules.items()))
            if rules else ""
        )
        return {
            f"cbot:{item['commodity']}": _counts(item["reconciliation"]) + rule_note
            for item in report.get("commodities") or []
            if not (item.get("reconciliation") or {}).get("reconciled", False)
        }
    if pilot == "fx":
        session = report.get("session")
        by_pair = {item.get("pair"): item for item in report.get("pairs") or []}
        return {
            f"fx:{pair}": f"session {session}: "
            + _counts((by_pair.get(pair) or {}).get("reconciliation") or {})
            for pair in report.get("diverged_pairs") or []
        }
    raise ValueError(f"unknown pilot {pilot!r}")


def classify(pilot: str, exit_code: str | None, report_dir: Path) -> PilotResult:
    """One pilot's state from its step's exit code and its report."""
    filename, _ = PILOTS[pilot]
    path = report_dir / filename
    report: dict[str, Any] | None = None
    if path.exists():
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            return PilotResult(pilot, "errored", f"report {filename} is not JSON ({exc})")
    not_compared = [
        f"{item.get('pair')} — {item.get('reason')}" for item in (report or {}).get("not_compared") or []
    ]

    code = (exit_code or "").strip()
    if code == "":
        return PilotResult(pilot, "not_run", "the step did not run", not_compared=not_compared)
    if code == "2":
        return PilotResult(pilot, "unavailable", "upstream unavailable or unparseable (exit 2)")
    if code not in {"0", "1"}:
        return PilotResult(pilot, "errored", f"unexpected exit code {code}")
    if report is None:
        return PilotResult(pilot, "errored", f"exit {code} but no report {filename} — the reconciler crashed")

    status = str(report.get("status"))
    if code == "1" and status != "diverged":
        return PilotResult(pilot, "errored", f"exit 1 with report status `{status}` — the reconciler crashed")
    if code == "0" and status == "diverged":
        return PilotResult(pilot, "errored", "exit 0 with a diverged report — the exit code and report disagree")
    if status in NOTHING_TO_COMPARE:
        return PilotResult(pilot, "nothing_to_compare", f"`{status}`: nothing published to compare",
                           not_compared=not_compared)
    if status not in GRADED_STATUSES:
        return PilotResult(pilot, "errored", f"report status `{status}` is not one this relay knows")
    diverged = _diverged_items(pilot, report) if status == "diverged" else {}
    if status == "diverged" and not diverged:
        return PilotResult(pilot, "errored", "report says diverged but names no diverged leg")
    return PilotResult(pilot, status, "reconciled" if status == "reconciled" else "diverged",
                       diverged=diverged, not_compared=not_compared)


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


def marker(keys: set[str]) -> str:
    return f"<!-- {_MARKER}: {json.dumps(sorted(keys))} -->"


def parse_state(text: str) -> set[str] | None:
    """The alarm keys of the newest marker in ``text``; None when there is none."""
    found = _MARKER_RE.findall(text)
    if not found:
        return None
    return set(json.loads(found[-1]))


def current_keys(results: list[PilotResult], before: set[str]) -> tuple[set[str], set[str]]:
    """This run's alarm keys, and which of them were carried rather than re-checked."""
    now: set[str] = set()
    carried: set[str] = set()
    for result in results:
        if result.state == "errored":
            now.add(f"{result.pilot}:errored")
        if result.graded:
            now |= set(result.diverged)
            continue
        prior = {key for key in before if key.startswith(f"{result.pilot}:") and not key.endswith(":errored")}
        now |= prior
        carried |= prior
    return now, carried


def find_open_issue() -> int | None:
    out = _gh("issue", "list", "--label", ALARM_LABEL, "--state", "open", "--json", "number", "--limit", "1")
    issues = json.loads(out)
    return issues[0]["number"] if issues else None


def issue_state(number: int) -> set[str] | None:
    thread = json.loads(_gh("issue", "view", str(number), "--json", "body,comments"))
    texts = [thread.get("body") or "", *[(c.get("body") or "") for c in thread.get("comments") or []]]
    state: set[str] | None = None
    for text in texts:
        found = parse_state(text)
        if found is not None:
            state = found
    return state


# ---------------------------------------------------------------------------
# Body
# ---------------------------------------------------------------------------


def build_body(results: list[PilotResult], now: set[str], before: set[str], carried: set[str]) -> str:
    lines: list[str] = []
    newly = sorted(now - before)
    recovered = sorted(before - now)
    reasons = {key: reason for result in results for key, reason in result.diverged.items()}
    errored = {f"{r.pilot}:errored": r.detail for r in results if r.state == "errored"}

    def describe(key: str) -> str:
        if key in carried:
            return f"- `{key}` — carried: not re-checked this run"
        return f"- `{key}` — {reasons.get(key) or errored.get(key) or 'see the run'}"

    if newly:
        lines += ["**Now diverged:**", *[describe(k) for k in newly], ""]
    if recovered:
        lines += ["**Recovered:**", *[f"- `{k}` reconciles again" for k in recovered], ""]
    still = sorted(now & before)
    if still:
        lines += ["**Still diverged:**", *[describe(k) for k in still], ""]
    if not now:
        lines += ["Every graded leg reconciles with v1 this run.", ""]

    lines += ["| Pilot | This run |", "|---|---|"]
    for result in results:
        lines.append(f"| {PILOTS[result.pilot][1]} | `{result.state}` — {result.detail} |")
    lines.append("")
    not_compared = [item for result in results for item in result.not_compared]
    if not_compared:
        lines += ["Not graded (never opens this issue):", *[f"- {item}" for item in not_compared], ""]
    lines += [
        "A divergence means the v1 and trusted paths parsed **one shared download** differently. "
        "It is a drift alarm, not a publish gate: the edition went out regardless, and this never "
        "reddens the Update Dashboard run (owner decision, #239). Reports: the run's "
        "`trusted-reconciliation` artifact.",
        f"Run: {run_url()}",
        "",
        marker(now),
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _parse_exits(values: list[str]) -> dict[str, str]:
    exits: dict[str, str] = {}
    for value in values:
        pilot, sep, code = value.partition("=")
        if not sep or pilot not in PILOTS:
            raise SystemExit(f"--exit expects <pilot>=<code> with pilot in {sorted(PILOTS)}; got {value!r}")
        exits[pilot] = code
    return exits


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--exit", action="append", default=[], dest="exits",
                        help="<pilot>=<exit code> for each reconcile step (empty code = not run)")
    args = parser.parse_args(argv)
    exits = _parse_exits(args.exits)

    results = [classify(pilot, exits.get(pilot), args.report_dir) for pilot in PILOTS]
    for result in results:
        logger.info("%s: %s — %s", result.pilot, result.state, result.detail)

    existing = find_open_issue()
    before: set[str] = set()
    known_before = True
    if existing is not None:
        found = issue_state(existing)
        known_before = found is not None
        before = found or set()
    now, carried = current_keys(results, before)

    if not now:
        if existing is None:
            logger.info("Nothing diverged and no open %s issue — nothing to do", ALARM_LABEL)
            return 0
        logger.info("Nothing diverged — closing #%d", existing)
        _gh("issue", "close", str(existing), "--comment", build_body(results, now, before, carried))
        return 0

    if existing is None:
        logger.warning("Reconciliation alarm: %s — opening a %s issue", ", ".join(sorted(now)), ALARM_LABEL)
        _gh("label", "create", ALARM_LABEL, "--force", "--description",
            "Automated alert: the trusted path diverged from v1 (#239)", "--color", "B60205")
        _gh("issue", "create", "--title", ALARM_TITLE, "--label", ALARM_LABEL,
            "--body", build_body(results, now, set(), carried))
        return 0

    if now != before or not known_before:
        logger.warning("Reconciliation alarm changed: %s → %s", sorted(before), sorted(now))
        _gh("issue", "comment", str(existing), "--body", build_body(results, now, before, carried))
    else:
        logger.info("Reconciliation alarm unchanged (%s) — no comment", ", ".join(sorted(now)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
