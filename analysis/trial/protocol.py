"""The trial protocol document, generated from the code that measures it (Phase 5).

Requirement 1 asks for a versioned protocol with instructions, definitions,
confidentiality boundaries and success criteria. It is *generated* rather than
written, and that is the whole point: a protocol living in a markdown file drifts
from the enum the metrics are computed over, and the drift is invisible because
both halves keep working. A participant told to spend ten minutes on the morning brief
while ``TaskId.MORNING_BRIEF.target_minutes`` says fifteen produces a timeliness
score that means nothing, and nothing in either file would ever say so.

So every task's title, decision question, success criterion, time target and
cadence comes from :data:`analysis.trial.domain._TASK_SPECS`; every issue class
and severity comes from its enum's ``meaning``; every threshold comes from
``config.TRIAL_DECISION_THRESHOLDS``. Editing the document means editing the
code, which is the only way the two can be kept honest.

The document is written to ``docs/trial/PROTOCOL.md``. That path is *inside*
``docs/`` and that is deliberate and safe: the protocol is instructions, contains
no trial data, and is not on the promotion contract's path list, so it is not
uploaded with the site. The records it instructs people to keep go somewhere
else entirely, and the document says so in its own confidentiality section.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from analysis.trial.domain import (
    ExternalTool,
    IssueClass,
    Outcome,
    Severity,
    TaskId,
)
from analysis.trial.floor import floor_spec, real_decision_tasks

__all__ = ["protocol_markdown", "protocol_version", "task_reference", "write_protocol"]


def protocol_version() -> str:
    import config

    return str(getattr(config, "TRIAL_PROTOCOL_VERSION", "1.0.0"))


def _thresholds_table() -> list[str]:
    import config

    thresholds: dict[str, dict[str, float]] = getattr(config, "TRIAL_DECISION_THRESHOLDS", {})
    lower: frozenset[str] = getattr(config, "TRIAL_LOWER_IS_BETTER", frozenset())
    lines = ["| Metric | Go at | No-go at | Direction |", "|---|---|---|---|"]
    for key, bars in thresholds.items():
        direction = "lower is better" if key in lower else "higher is better"
        lines.append(f"| `{key}` | {bars.get('go')} | {bars.get('no_go')} | {direction} |")
    return lines


def _repo_relative(path: str | Path) -> str:
    """Render a configured directory relative to the checkout, where it is inside one.

    ``config.TRIAL_RECORD_DIR`` is absolutised against the repository root, so
    quoting it verbatim wrote the generating machine's home directory into a
    committed document — one that then differed on every other checkout, which
    is how the "the committed protocol matches what the code generates" check
    found it. A path deliberately moved outside the repository (via
    ``MIRROR_TRIAL_DIR``) is printed in full, because there the absolute
    location *is* the instruction.
    """
    target = Path(path)
    try:
        return target.resolve().relative_to(Path(__file__).resolve().parents[2]).as_posix()
    except ValueError:
        return str(target)


def protocol_markdown() -> str:
    """The whole protocol as one markdown document."""
    import config

    version = protocol_version()
    window = getattr(config, "TRIAL_WINDOW_TRADING_DAYS", 30)
    min_participants = getattr(config, "TRIAL_MIN_PARTICIPANTS", 2)
    min_obs = getattr(config, "TRIAL_MIN_OBSERVATIONS", 10)
    floor = floor_spec()
    real_tasks = real_decision_tasks()
    real_task_refs = ", ".join(
        f"`{task.value}` (task {list(TaskId).index(task) + 1})" for task in real_tasks
    )
    scale = getattr(config, "TRIAL_CONFIDENCE_SCALE", (1, 2, 3, 4, 5))
    record_dir = _repo_relative(getattr(config, "TRIAL_RECORD_DIR", "data/reference/trial"))
    private_dir = _repo_relative(getattr(config, "TRIAL_PRIVATE_OUTPUT_DIR", "data/workspace/trial"))

    lines: list[str] = [
        f"# Mirror Market — participant validation protocol v{version}",
        "",
        "*This document is generated from `analysis/trial/` by "
        "`python scripts/trial.py protocol`. Do not edit it by hand: the task "
        "definitions, issue classes and thresholds below are the same objects the "
        "metrics are computed from, and editing the prose would put the "
        "instructions and the measurement out of step without either one "
        "complaining.*",
        "",
        "## What this trial measures",
        "",
        "One question: **does Mirror Market reduce external terminal, broker and "
        "spreadsheet use without increasing decision risk?**",
        "",
        "Two halves, and both must hold. A product that halves the lookups while "
        "producing one wrong number a participant would have sized off has failed, and "
        "the metrics are built so that it cannot pass by trading one against the "
        "other — correctness rates are graded separately from lookup counts and "
        "neither is blended into a single score.",
        "",
        "**The comparison baseline is self-reported.** There is no instrumented "
        "terminal session to measure against and none is being built; the "
        "`external_lookups_per_task` metric — every screen switch a participant "
        "logs, with the question that forced it — is the baseline and the "
        "headline number at once.",
        "",
        "## Who takes part — a participant",
        "",
        "A **participant** is a **physical buyer**: someone who prices or books "
        "physical soy product (beans, meal, oil) at least weekly — procurement, "
        "origination, or the physical desk at an importer, crusher, feed miller "
        "or trading house. Futures-only traders, brokers and analysts are not "
        "participants. Participation is unpaid in both directions; a participant "
        "gets a private desk edition for the trial and six months after it, and a "
        "vote on the first-screen contract.",
        "",
        "## Shape of the trial",
        "",
        f"- **{window} trading days.**",
        f"- **At least {min_participants} participants (physical buyers) at the "
        "decision floor** (below). One participant's habits are not a finding.",
        f"- **At least {min_obs} observations** before any single metric is graded; "
        "below that the metric reports `insufficient` rather than a number.",
        "- **Sessions are captured on a form and transcribed by the desk.** "
        "Participants never touch the repository: after each decision they "
        "complete a ~2-minute form (≤15 minutes a day), and once a week the desk "
        "transcribes the export with `python scripts/trial.py transcribe "
        "<export.csv>` into the private record — through the same validation a "
        "hand-written record gets. One 30-minute weekly debrief.",
        "- **Ten recurring tasks**, listed below. Each is framed as a *decision*, "
        "not as a page to look at: the test is whether the participant can answer the "
        "question, not whether the page loaded.",
        "- **One session record per task attempt**, including the attempts that "
        "fail. An abandoned session is data; a session nobody logged is not.",
        "- **One day observation per trading day**, whether or not anyone ran a "
        "session. This is the only way availability and deployment reliability get "
        "measured, because the days the product broke are exactly the days nobody "
        "logs a session. It also carries the release stamp a transcribed session "
        "is pinned to, so a day without one cannot be transcribed.",
        "",
        "## The decision floor — what \"means anything\"",
        "",
        "Per participant, over the window:",
        "",
        f"- **at least {floor.sessions} logged sessions**,",
        f"- **covering at least {floor.tasks} of the ten tasks**,",
        f"- **including at least {floor.real_decisions} real cargo/basis decisions** — "
        f"completed sessions in {real_task_refs}. An abandoned session in one of "
        "those tasks is a session, not a decision.",
        "",
        f"Trial-wide: **at least {floor.required_participants} participants at the "
        "floor**, else the verdict is `insufficient` — no verdict at all. A "
        "participant below the floor is **reported, not graded**: their standing "
        "and each shortfall appear in the private weekly review, and nothing of "
        "theirs is averaged into a verdict. The floor supersedes the v1 "
        "ten-sessions-total rule; it lives in `config.TRIAL_DECISION_FLOOR` and "
        "`config.TRIAL_REAL_DECISION_TASKS`.",
        "",
        "## The ten tasks",
        "",
    ]

    for index, task in enumerate(TaskId, start=1):
        lines += [
            f"### {index}. {task.label}",
            "",
            f"- **Cadence**: {task.cadence}",
            f"- **Time target**: {task.target_minutes} minutes "
            "(a target, not a benchmark — this project has no instrumented "
            "terminal session to compare against)",
            "",
            f"**The decision.** {task.decision_question}",
            "",
            f"**Success.** {task.success_criteria}",
            "",
        ]

    lines += [
        "## What to record, every session",
        "",
        "| Field | Meaning |",
        "|---|---|",
        "| participant | The participant's handle. Never a full name. See confidentiality below. |",
        "| task | One of the ten above. |",
        "| trading_day / start / end | The session's own clock. Timestamps must "
        "carry a timezone. |",
        "| outcome | " + ", ".join(f"`{o.value}`" for o in Outcome) + " |",
        "| decision or output | What you concluded. Required when the outcome is "
        "`completed`. |",
        "| pages used | Which Mirror Market pages you actually opened. |",
        "| external lookups | Every time you left the product — see below. |",
        "| missing / stale / wrong | Logged as issues, classified. |",
        "| false and missed alerts | Logged as issues, classified. |",
        f"| confidence | {min(scale)}–{max(scale)}. How much you trust the answer "
        "you reached. |",
        "| would act | Would you place, size or price a real trade off this? |",
        "| notes and evidence | Free text. Private. |",
        "",
        "A session that did **not** complete must carry at least one issue saying "
        "why. That rule is enforced by the record type, not by review: an "
        "unexplained failure is the one record that teaches nothing.",
        "",
        "## External lookups",
        "",
        "Every time you go outside Mirror Market to finish a task, log it — with "
        "**the question the product could not answer**. That question is the single "
        "most valuable output of this trial, and the record refuses to be saved "
        "without one. A lookup count tells us a participant left; the question tells us "
        "why, and it is the input to the backlog.",
        "",
        "Tools: " + ", ".join(f"`{tool.value}`" for tool in ExternalTool) + ". Use "
        "`other` only with the tool named in text.",
        "",
        "## Issue classification",
        "",
        "| Class | Meaning |",
        "|---|---|",
    ]
    for issue_class in IssueClass:
        lines.append(f"| `{issue_class.value}` | {issue_class.meaning} |")
    lines += [
        "",
        "The first three are **correctness** classes. One occurrence promotes "
        "straight to the backlog with no corroboration needed, because being wrong "
        "once is already the finding. `upstream_outage` is deliberately *not* one "
        "of them: a source being down is measured as availability, and counting it "
        "as this product being wrong would make an honest outage look like a defect.",
        "",
        "## Severity",
        "",
        "| Severity | Meaning |",
        "|---|---|",
    ]
    for severity in Severity:
        lines.append(f"| `{severity.value}` | {severity.meaning} |")
    lines += [
        "",
        "A correctness issue may not be filed as `minor`. The record type refuses "
        "it: if a number was wrong, the question is how wrong, not whether it "
        "mattered.",
        "",
        "## Confidentiality — binding",
        "",
        "- **Every participant signs a one-page participation letter** before their "
        "first session. It states what is recorded, where it lives, that only the "
        "aggregate projection is ever shared, and their right to withdraw and have "
        "their records deleted. No NDA. The letter sits on top of the enforced "
        "handle-only, gitignored, aggregate-projection regime below; it does not "
        "replace it.",
        "- **The design partner** — the warmest recruit — decides the first-screen "
        "contract with the desk before the trial starts. That is onboarding, not a "
        "different confidentiality standing: the same letter, the same handle, the "
        "same record.",
        "- **Do not publish participant names, firms, positions, counterparties, "
        "contact notes or commercial decisions.** Anywhere, in any form — including "
        "the public issue tracker, which records roles only.",
        "- Use a **handle**, not a name, in the `participant` field. Three characters "
        "minimum. Pick something that is not a substring of ordinary English — the "
        "leak guard searches free text for it.",
        f"- Trial records live in `{record_dir}` and are **gitignored**. They are "
        "YAML files, not database rows, specifically because every table in this "
        "project round-trips through `data/history/*.csv`, which is committed to a "
        "public repository. A trial table would publish participant identity by "
        "construction.",
        f"- Generated private output goes to `{private_dir}` — outside `docs/`, and "
        "absent from the site promotion contract, so it can never reach GitHub "
        "Pages.",
        "- Anything shared outside the desk is the **aggregate** projection, which "
        "does not build the private fields at all and is checked by a recursive "
        "guard before it is written.",
        "- Do not put a position, a cargo, a price you were shown, or a "
        "counterparty name in a `summary` field. Those go in `notes` and "
        "`evidence`, which never leave the private record.",
        "",
        "## Decision thresholds",
        "",
        "These are the bars. They live in `config.TRIAL_DECISION_THRESHOLDS` so "
        "that \"why did it say no-go\" is a lookup rather than an argument.",
        "",
    ]
    lines += _thresholds_table()
    lines += [
        "",
        "A metric between the two bars is `hold`. Two overrides sit above the "
        "arithmetic: **any open blocker is a no-go** whatever the rates say, and a "
        f"window with fewer than {min_participants} participants at the decision "
        "floor returns `insufficient` rather than a verdict.",
        "",
        "## Weekly and final output",
        "",
        "- **Weekly**: each participant's standing against the decision floor, "
        "what worked, what failed, the top unmet questions, the metric trend "
        "against the prior week, recommended changes, and a go/no-go for wider "
        "use — read against the floor, so it says `insufficient` until the "
        "window holds enough. `python scripts/trial.py review`.",
        f"- **Final**: a {window}-day scorecard across precision, accuracy, "
        "reliability, timeliness, physical usefulness, futures usefulness, "
        "opportunity usefulness, UX and participant trust. Every dimension states the "
        "arithmetic behind it, and a dimension without enough observations scores "
        "nothing at all rather than a default. `python scripts/trial.py scorecard`.",
        "",
        "## Reproducibility",
        "",
        "Every session is stamped with the git commit and a fingerprint of the "
        "layer freshness table at the moment it ran. `python scripts/trial.py "
        "reproduce <session-id>` reports whether that result can be produced again, "
        "and what has moved if not. A session run against uncommitted changes is "
        "recorded as **not reproducible** rather than refused — a hotfix session is "
        "a legitimate session — and the share of findings arriving on unreproducible "
        "builds is itself a reported metric.",
        "",
        "## Failure drills",
        "",
        "Five drills simulate a critical source outage, partial key coverage, a "
        "stale payload, a page-generation failure and a deployment failure, then "
        "check that the product degrades the way it claims to. Run them with "
        "`python scripts/trial.py drills`. They touch no production database, write "
        "nothing into `docs/`, and make no network call.",
        "",
        "Each drill carries a **participant prompt**: show the degraded surface to a "
        "participant who has not been told what broke, and record what they can tell. "
        "That answer is the drill's real result; the assertions only prove the "
        "mechanism fired.",
        "",
        f"---\n\n*Protocol v{version}. Generated from `analysis/trial/domain.py` and "
        "`config.py`.*",
    ]
    return "\n".join(lines) + "\n"


def write_protocol(path: str | Path | None = None) -> Path:
    """Write ``docs/trial/PROTOCOL.md``. Returns the path written."""
    target = Path(path) if path is not None else Path(__file__).resolve().parents[2] / "docs" / "trial" / "PROTOCOL.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(protocol_markdown(), encoding="utf-8")
    return target


def task_reference() -> dict[str, dict[str, Any]]:
    """The task specs as plain data, for a CLI or a page that needs them."""
    return {
        task.value: {
            "label": task.label,
            "question": task.decision_question,
            "success": task.success_criteria,
            "target_minutes": task.target_minutes,
            "cadence": task.cadence,
        }
        for task in TaskId
    }
