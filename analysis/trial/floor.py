"""The per-participant decision floor (A6 #303, built by #412).

The trial's verdict is read only off participants who logged enough to mean
something. A6 set the floor: per participant, at least
``config.TRIAL_DECISION_FLOOR["sessions"]`` sessions over the window, across at
least ``["tasks"]`` distinct tasks, including at least ``["real_decisions"]``
completed sessions in the real cargo/basis tasks
(``config.TRIAL_REAL_DECISION_TASKS``). Trial-wide, at least
``config.TRIAL_MIN_PARTICIPANTS`` must stand at the floor or the verdict is
``insufficient`` — no verdict at all.

Three rules carry this module:

* **A participant below the floor is reported, not graded.** Their standing
  and each shortfall are named in the private review so the desk can see who
  is short and by how much. They are never silently dropped — a floor that
  quietly discards a participant is a floor nobody can audit.
* **A real decision is a completed session.** An abandoned origin comparison is
  a session, and it is data, but it is not a cargo decision; counting it as one
  would let a participant reach the floor on tasks they gave up on.
* **Only completed, in-task sessions count — nothing is interpolated.** No
  pro-rating against days elapsed, no projection of where a participant will
  be next week. The floor is a count of records that exist.

Standard library only, like ``domain.py``: a floor that could read config at
import time would make the number a property of the environment rather than of
the record set. ``config`` is read at call time and validated — a task name that
is not a :class:`TaskId` raises rather than counting nothing.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from analysis.trial.domain import (
    AUDIENCE_AGGREGATE,
    AUDIENCE_PRIVATE,
    Outcome,
    SessionRecord,
    TaskId,
    TrialError,
)

__all__ = [
    "FloorSpec",
    "FloorResult",
    "ParticipantStanding",
    "decision_floor",
    "floor_spec",
    "real_decision_tasks",
]


@dataclass(frozen=True)
class FloorSpec:
    """The three per-participant bars and the trial-wide participant count."""

    sessions: int
    tasks: int
    real_decisions: int
    required_participants: int

    def to_dict(self) -> dict[str, int]:
        return {
            "sessions": self.sessions,
            "tasks": self.tasks,
            "real_decisions": self.real_decisions,
            "required_participants": self.required_participants,
        }


def floor_spec() -> FloorSpec:
    """Read and validate the floor from ``config``. Hard-fails on a bad shape."""
    import config

    raw = getattr(config, "TRIAL_DECISION_FLOOR", None)
    if not isinstance(raw, dict):
        raise TrialError("config.TRIAL_DECISION_FLOOR must be a dict of three integer bars")
    expected = {"sessions", "tasks", "real_decisions"}
    if set(raw) != expected:
        raise TrialError(
            f"config.TRIAL_DECISION_FLOOR must carry exactly {sorted(expected)}, got {sorted(raw)}"
        )
    for key, value in raw.items():
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            raise TrialError(f"config.TRIAL_DECISION_FLOOR[{key!r}] must be a positive integer, got {value!r}")
    required = getattr(config, "TRIAL_MIN_PARTICIPANTS", 2)
    if not isinstance(required, int) or required < 1:
        raise TrialError(f"config.TRIAL_MIN_PARTICIPANTS must be a positive integer, got {required!r}")
    if raw["tasks"] > len(TaskId):
        raise TrialError(
            f"config.TRIAL_DECISION_FLOOR['tasks'] = {raw['tasks']} exceeds the {len(TaskId)} tasks that exist"
        )
    return FloorSpec(
        sessions=raw["sessions"],
        tasks=raw["tasks"],
        real_decisions=raw["real_decisions"],
        required_participants=required,
    )


def real_decision_tasks() -> tuple[TaskId, ...]:
    """The tasks whose completed session is a real cargo/basis decision."""
    import config

    names = getattr(config, "TRIAL_REAL_DECISION_TASKS", ())
    if not names:
        raise TrialError("config.TRIAL_REAL_DECISION_TASKS is empty — no task counts as a real decision")
    out: list[TaskId] = []
    for name in names:
        try:
            out.append(TaskId(name))
        except ValueError:
            raise TrialError(
                f"config.TRIAL_REAL_DECISION_TASKS names {name!r}, which is not a trial task"
            ) from None
    return tuple(out)


@dataclass(frozen=True)
class ParticipantStanding:
    """One participant's count against the floor. Handle included: private by default."""

    participant: str
    sessions: int
    tasks_covered: int
    real_decisions: int
    spec: FloorSpec

    @property
    def shortfalls(self) -> tuple[str, ...]:
        out: list[str] = []
        if self.sessions < self.spec.sessions:
            out.append(f"{self.sessions} of {self.spec.sessions} sessions")
        if self.tasks_covered < self.spec.tasks:
            out.append(f"{self.tasks_covered} of {self.spec.tasks} tasks")
        if self.real_decisions < self.spec.real_decisions:
            out.append(f"{self.real_decisions} of {self.spec.real_decisions} real decisions")
        return tuple(out)

    @property
    def at_floor(self) -> bool:
        return not self.shortfalls

    def to_dict(self, *, audience: str = AUDIENCE_PRIVATE) -> dict[str, Any]:
        """Private: with the handle. Aggregate: counts only, no identity."""
        payload: dict[str, Any] = {
            "sessions": self.sessions,
            "tasks_covered": self.tasks_covered,
            "real_decisions": self.real_decisions,
            "at_floor": self.at_floor,
        }
        if audience == AUDIENCE_PRIVATE:
            payload["participant"] = self.participant
            payload["shortfalls"] = list(self.shortfalls)
        elif audience != AUDIENCE_AGGREGATE:
            raise TrialError(f"unknown audience {audience!r}")
        return payload


@dataclass(frozen=True)
class FloorResult:
    """Every participant's standing, and whether the trial as a whole may be graded."""

    standings: tuple[ParticipantStanding, ...]
    spec: FloorSpec

    @property
    def at_floor(self) -> tuple[ParticipantStanding, ...]:
        return tuple(s for s in self.standings if s.at_floor)

    @property
    def below_floor(self) -> tuple[ParticipantStanding, ...]:
        return tuple(s for s in self.standings if not s.at_floor)

    @property
    def met(self) -> bool:
        return len(self.at_floor) >= self.spec.required_participants

    def for_participant(self, participant: str) -> ParticipantStanding:
        key = participant.strip().lower()
        for standing in self.standings:
            if standing.participant == key:
                return standing
        raise TrialError(f"no sessions from participant {participant!r} in this window")

    @property
    def reason(self) -> str:
        """One sentence a verdict can quote. Counts only — safe for any audience.

        The verdict reason travels in the aggregate projection, so it must not
        carry a handle. Who is short, and by how much, is :meth:`standing_lines`.
        """
        spec = self.spec
        bar = (
            f"≥{spec.sessions} sessions over ≥{spec.tasks} tasks with "
            f"≥{spec.real_decisions} real decisions"
        )
        head = (
            f"{len(self.at_floor)} of {len(self.standings)} participant(s) at the decision floor "
            f"({bar}); the protocol needs {spec.required_participants}"
        )
        return head if self.met else f"insufficient — {head}"

    def standing_lines(self) -> tuple[str, ...]:
        """One line per participant, handle included. Private audience only."""
        out: list[str] = []
        for s in self.standings:
            state = "at floor" if s.at_floor else "below floor: " + ", ".join(s.shortfalls)
            out.append(
                f"{s.participant} — {s.sessions} session(s), {s.tasks_covered} task(s), "
                f"{s.real_decisions} real decision(s) — {state}"
            )
        return tuple(out)

    def to_dict(self, *, audience: str = AUDIENCE_PRIVATE) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "participants": len(self.standings),
            "at_floor": len(self.at_floor),
            "required_participants": self.spec.required_participants,
            "floor": self.spec.to_dict(),
            "met": self.met,
        }
        if audience == AUDIENCE_PRIVATE:
            payload["standings"] = [s.to_dict(audience=audience) for s in self.standings]
            payload["reason"] = self.reason
        elif audience != AUDIENCE_AGGREGATE:
            raise TrialError(f"unknown audience {audience!r}")
        return payload


def decision_floor(sessions: Iterable[SessionRecord]) -> FloorResult:
    """Count every participant against the floor. Pure: no filesystem, config read once."""
    spec = floor_spec()
    real = frozenset(real_decision_tasks())
    counts: dict[str, list[SessionRecord]] = {}
    for session in sessions:
        counts.setdefault(session.participant.strip().lower(), []).append(session)
    standings = tuple(
        ParticipantStanding(
            participant=handle,
            sessions=len(records),
            tasks_covered=len({s.task for s in records}),
            real_decisions=sum(
                1 for s in records if s.task in real and s.outcome is Outcome.COMPLETED
            ),
            spec=spec,
        )
        for handle, records in sorted(counts.items())
    )
    return FloorResult(standings=standings, spec=spec)
