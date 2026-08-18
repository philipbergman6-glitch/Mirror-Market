"""The private trial record store (Phase 5).

Two kinds of document, each one YAML file per trading day, under a gitignored
directory:

    data/reference/trial/sessions/YYYY-MM-DD.yml   a list of SessionRecord
    data/reference/trial/days/YYYY-MM-DD.yml       one DayObservation

They are separate directories rather than two shapes in one file because they
are written at different times by different people: a session is written by a
trader when they finish a task, a day observation is computed by the operator
once, after the deploy. One file holding both would mean every session write
had to re-read and re-write the day's product state.

Same contract, and the same reasoning, as
``analysis/opportunities/workflow.py`` and ``analysis/futures/positions.py``:

* a **missing directory** is an empty trial — the correct state for a clone that
  has run no sessions, and the metrics report ``insufficient`` rather than zero;
* a **present but malformed** file raises and stops the run, because "no sessions
  recorded" and "a session recorded wrongly" are different states and only one of
  them is safe to report as a rate;
* an **unknown field** raises rather than being ignored — a typo'd ``note`` where
  ``notes`` was meant would silently drop the one thing the file exists to record,
  and here that silent drop would land in a go/no-go decision.

Why YAML on disk and not a SQLite table: every table this project persists
round-trips through ``data/history/*.csv``, which is **committed to a public
git repository**. A trial record carries a trader's identity, their decision and
their notes, so a table is the wrong shape by construction — the persistence
mechanism would publish it. That is not a rule this module enforces; it is the
reason this module exists instead of a ``save_trial_sessions``.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from analysis.trial.domain import (
    DayObservation,
    ExternalLookup,
    ExternalTool,
    Issue,
    IssueClass,
    Outcome,
    ReleaseStamp,
    SessionRecord,
    Severity,
    TaskId,
    TrialError,
)

log = logging.getLogger(__name__)

__all__ = [
    "DAY_FIELDS",
    "DAYS_SUBDIR",
    "ISSUE_FIELDS",
    "LOOKUP_FIELDS",
    "RELEASE_FIELDS",
    "SESSIONS_SUBDIR",
    "SESSION_FIELDS",
    "DayObservationSet",
    "SessionSet",
    "day_to_document",
    "load_day_observations",
    "load_sessions",
    "parse_day_observation",
    "parse_sessions",
    "session_to_document",
]

SESSIONS_SUBDIR = "sessions"
DAYS_SUBDIR = "days"

SESSION_FIELDS = frozenset({
    "trader",
    "task",
    "trading_day",
    "started_at",
    "ended_at",
    "outcome",
    "confidence",
    "would_act",
    "decision",
    "pages_used",
    "external_lookups",
    "issues",
    "notes",
    "evidence",
    "protocol_version",
    "release",
})

RELEASE_FIELDS = frozenset({
    "code_revision",
    "data_fingerprint",
    "captured_at",
    "edition_id",
    "dirty",
    "layer_count",
})

LOOKUP_FIELDS = frozenset({"tool", "unanswered_question", "answer_found", "minutes", "tool_detail"})

DAY_FIELDS = frozenset({
    "trading_day",
    "edition_published",
    "edition_current",
    "critical_layers_expected",
    "critical_layers_available",
    "degraded_layers",
    "drill",
    "note",
    "release",
})

ISSUE_FIELDS = frozenset({
    "classification",
    "summary",
    "evidence",
    "affected_decision",
    "severity",
    "page",
    "expected",
    "observed",
})


# ---------------------------------------------------------------------------
# Coercion helpers — one per scalar kind, each naming where it failed
# ---------------------------------------------------------------------------
def _require(raw: Any, field: str, where: str) -> Any:
    if field not in raw:
        raise TrialError(f"{where}: missing required field {field!r}")
    return raw[field]


def _as_text(value: Any, field: str, where: str, *, allow_empty: bool = False) -> str:
    if value is None:
        value = ""
    text = str(value).strip()
    if not text and not allow_empty:
        raise TrialError(f"{where}: {field} must be a non-empty string")
    return text


def _as_date(value: Any, field: str, where: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError) as exc:
        raise TrialError(f"{where}: {field} must be an ISO date, got {value!r}") from exc


def _as_datetime(value: Any, field: str, where: str) -> datetime:
    """An ISO timestamp that carries an offset. Naive timestamps are refused.

    The trial runs across Chicago, London and Singapore desks; subtracting two
    naive clocks recorded on different continents produces a duration that is
    wrong by hours and looks entirely plausible. The offset is one character of
    typing (``Z``) and the CLI writes it for you.
    """
    stamp = value
    if not isinstance(stamp, datetime):
        try:
            stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError) as exc:
            raise TrialError(
                f"{where}: {field} must be an ISO timestamp with an offset, e.g. "
                f"2026-08-19T07:05:00Z — got {value!r}"
            ) from exc
    if stamp.tzinfo is None:
        raise TrialError(
            f"{where}: {field} has no timezone offset ({value!r}). Append 'Z' for UTC or "
            "the desk's own offset — two naive clocks on two continents subtract to a "
            "duration that is wrong and looks right."
        )
    return stamp


def _as_int(value: Any, field: str, where: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise TrialError(f"{where}: {field} must be a whole number, got {value!r}") from exc


def _as_bool(value: Any, field: str, where: str) -> bool:
    if isinstance(value, bool):
        return value
    raise TrialError(
        f"{where}: {field} must be true or false, got {value!r} — a blank or a 'maybe' "
        "here becomes a rate this trial is read on"
    )


def _as_float(value: Any, field: str, where: str) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise TrialError(f"{where}: {field} must be a number, got {value!r}") from exc


def _as_tuple(value: Any, field: str, where: str) -> tuple[str, ...]:
    """A list of strings, leniently accepting a single bare string."""
    if value is None:
        return ()
    if isinstance(value, str):
        return (value.strip(),) if value.strip() else ()
    if not isinstance(value, list):
        raise TrialError(f"{where}: {field} must be a list of strings, got {type(value).__name__}")
    return tuple(str(item).strip() for item in value if str(item).strip())


def _enum(enum_cls: Any, value: Any, field: str, where: str, *, default: str | None = None) -> Any:
    raw = value if value not in (None, "") else default
    if raw is None:
        raise TrialError(f"{where}: missing required field {field!r}")
    try:
        return enum_cls(str(raw).strip().lower())
    except ValueError as exc:
        known = [member.value for member in enum_cls]
        raise TrialError(f"{where}: unknown {field} {value!r}; known: {known}") from exc


def _closed_fields(raw: Any, allowed: frozenset[str], where: str, kind: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise TrialError(f"{where}: expected a {kind} mapping, got {type(raw).__name__}")
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise TrialError(
            f"{where}: unknown {kind} field(s) {unknown} — known: {sorted(allowed)}. "
            "A typo here silently drops the thing the record exists to capture."
        )
    return raw


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------
def _parse_release(raw: Any, where: str) -> ReleaseStamp:
    data = _closed_fields(raw, RELEASE_FIELDS, where, "release")
    return ReleaseStamp(
        code_revision=_as_text(_require(data, "code_revision", where), "code_revision", where),
        data_fingerprint=_as_text(
            _require(data, "data_fingerprint", where), "data_fingerprint", where
        ),
        captured_at=_as_datetime(_require(data, "captured_at", where), "captured_at", where),
        edition_id=(
            _as_text(data["edition_id"], "edition_id", where)
            if data.get("edition_id") not in (None, "")
            else None
        ),
        dirty=_as_bool(data.get("dirty", False), "dirty", where),
        layer_count=(
            _as_int(data["layer_count"], "layer_count", where)
            if data.get("layer_count") is not None
            else None
        ),
    )


def _parse_lookup(raw: Any, where: str) -> ExternalLookup:
    data = _closed_fields(raw, LOOKUP_FIELDS, where, "external lookup")
    return ExternalLookup(
        tool=_enum(ExternalTool, data.get("tool"), "tool", where),
        unanswered_question=_as_text(
            _require(data, "unanswered_question", where), "unanswered_question", where
        ),
        answer_found=_as_bool(_require(data, "answer_found", where), "answer_found", where),
        minutes=_as_float(data.get("minutes"), "minutes", where),
        tool_detail=(
            _as_text(data["tool_detail"], "tool_detail", where)
            if data.get("tool_detail") not in (None, "")
            else None
        ),
    )


def _parse_issue(raw: Any, where: str) -> Issue:
    data = _closed_fields(raw, ISSUE_FIELDS, where, "issue")
    return Issue(
        classification=_enum(IssueClass, data.get("classification"), "classification", where),
        summary=_as_text(_require(data, "summary", where), "summary", where),
        evidence=_as_text(_require(data, "evidence", where), "evidence", where),
        affected_decision=_as_text(
            _require(data, "affected_decision", where), "affected_decision", where
        ),
        severity=_enum(Severity, data.get("severity"), "severity", where, default="minor"),
        page=(
            _as_text(data["page"], "page", where) if data.get("page") not in (None, "") else None
        ),
        expected=(
            _as_text(data["expected"], "expected", where)
            if data.get("expected") not in (None, "")
            else None
        ),
        observed=(
            _as_text(data["observed"], "observed", where)
            if data.get("observed") not in (None, "")
            else None
        ),
    )


def _parse_session(raw: Any, where: str, *, source_file: str | None) -> SessionRecord:
    data = _closed_fields(raw, SESSION_FIELDS, where, "session")
    lookups = data.get("external_lookups") or []
    issues = data.get("issues") or []
    if not isinstance(lookups, list):
        raise TrialError(f"{where}: external_lookups must be a list")
    if not isinstance(issues, list):
        raise TrialError(f"{where}: issues must be a list")

    return SessionRecord(
        trader=_as_text(_require(data, "trader", where), "trader", where),
        task=_enum(TaskId, data.get("task"), "task", where),
        trading_day=_as_date(_require(data, "trading_day", where), "trading_day", where),
        started_at=_as_datetime(_require(data, "started_at", where), "started_at", where),
        ended_at=_as_datetime(_require(data, "ended_at", where), "ended_at", where),
        outcome=_enum(Outcome, data.get("outcome"), "outcome", where),
        confidence=_as_int(_require(data, "confidence", where), "confidence", where),
        would_act=_as_bool(_require(data, "would_act", where), "would_act", where),
        release=_parse_release(_require(data, "release", where), f"{where}.release"),
        decision=_as_text(data.get("decision", ""), "decision", where, allow_empty=True),
        pages_used=_as_tuple(data.get("pages_used"), "pages_used", where),
        external_lookups=tuple(
            _parse_lookup(item, f"{where}.external_lookups[{index}]")
            for index, item in enumerate(lookups)
        ),
        issues=tuple(
            _parse_issue(item, f"{where}.issues[{index}]") for index, item in enumerate(issues)
        ),
        notes=_as_tuple(data.get("notes"), "notes", where),
        evidence=_as_tuple(data.get("evidence"), "evidence", where),
        protocol_version=_as_text(
            data.get("protocol_version") or _default_protocol_version(),
            "protocol_version",
            where,
        ),
        source_file=source_file,
    )


def _default_protocol_version() -> str:
    import config

    return str(getattr(config, "TRIAL_PROTOCOL_VERSION", "1.0.0"))


def parse_sessions(
    document: Any, *, where: str, source_file: str | None = None
) -> tuple[SessionRecord, ...]:
    """Parse one loaded YAML document. Pure — no filesystem, no config.

    Split from :func:`load_sessions` so the malformed-document matrix can be
    tested without writing files, the same way ``workflow.parse_records`` is.
    """
    if document is None:
        return ()
    if not isinstance(document, list):
        raise TrialError(
            f"{where}: expected a YAML list of session records, got {type(document).__name__}"
        )
    return tuple(
        _parse_session(item, f"{where}[{index}]", source_file=source_file)
        for index, item in enumerate(document)
    )


@dataclass(frozen=True)
class SessionSet:
    """Every session recorded so far, and where each came from.

    Duplicate session ids raise. Two records with the same trader, task, day and
    start time are the same sitting written twice — most likely a file copied and
    edited — and silently keeping both would double-count that session in every
    rate the trial is judged on.
    """

    sessions: tuple[SessionRecord, ...] = ()
    loaded_from: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        ids = [session.session_id for session in self.sessions]
        duplicates = sorted({key for key in ids if ids.count(key) > 1})
        if duplicates:
            raise TrialError(
                f"duplicate session record(s) {duplicates} — the same trader, task, day and "
                "start time recorded twice would be counted twice in every trial metric"
            )

    @property
    def is_empty(self) -> bool:
        return not self.sessions

    @property
    def traders(self) -> tuple[str, ...]:
        return tuple(sorted({session.trader for session in self.sessions}))

    @property
    def trading_days(self) -> tuple[date, ...]:
        return tuple(sorted({session.trading_day for session in self.sessions}))

    def for_trader(self, trader: str) -> tuple[SessionRecord, ...]:
        key = trader.strip().lower()
        return tuple(s for s in self.sessions if s.trader.strip().lower() == key)

    def for_task(self, task: TaskId) -> tuple[SessionRecord, ...]:
        return tuple(s for s in self.sessions if s.task is task)

    def between(self, start: date, end: date) -> SessionSet:
        """Sessions in an inclusive day range, for the weekly review windows."""
        return SessionSet(
            sessions=tuple(s for s in self.sessions if start <= s.trading_day <= end),
            loaded_from=self.loaded_from,
        )


def _resolve(directory: str | os.PathLike[str] | None, subdir: str) -> Path:
    import config

    root = Path(str(directory) if directory is not None else config.TRIAL_RECORD_DIR)
    return root / subdir


def load_sessions(directory: str | os.PathLike[str] | None = None) -> SessionSet:
    """Read every ``*.yml`` in ``<trial dir>/sessions/``.

    A missing directory is an empty trial, logged and not raised: a fresh clone
    has run no sessions, and that is a legitimate state for every consumer here.
    A present but malformed file raises.
    """
    root = _resolve(directory, SESSIONS_SUBDIR)
    if not root.is_dir():
        log.info("no trial session directory at %s — no sessions have been run", root)
        return SessionSet()


    sessions: list[SessionRecord] = []
    files: list[str] = []
    for path in sorted(root.glob("*.yml")):
        document = _load_yaml(path)
        sessions.extend(_parse_named(parse_sessions, document, path))
        files.append(str(path))
    return SessionSet(sessions=tuple(sessions), loaded_from=tuple(files))


def _load_yaml(path: Path) -> Any:
    """Read one YAML file, or raise a :class:`TrialError` naming the file.

    PyYAML's own ``ScannerError`` is perfectly informative about *where in the
    text* it gave up, and says nothing about which trial file it was reading or
    what the operator should do about it. A trader running ``trial.py check``
    between calls needs the filename first.
    """
    import yaml

    try:
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise TrialError(
            f"{path}: not valid YAML ({exc.__class__.__name__}). The file is not skipped — a "
            f"skipped record is a metric computed over the wrong denominator — so fix it or "
            f"move it out of the directory. Underlying error: {exc}"
        ) from exc


def _parse_named(parser: Any, document: Any, path: Path) -> Any:
    """Run a parser, ensuring any validation failure names the file it came from.

    The record types validate themselves, which is right — but they know nothing
    about files, so a confidence of 99 raised a perfectly true message that left
    the operator grepping twenty-two files for it.
    """
    try:
        return parser(document, where=str(path), source_file=str(path))
    except TrialError as exc:
        message = str(exc)
        if str(path) in message:
            raise
        raise TrialError(f"{path}: {message}") from exc


# ---------------------------------------------------------------------------
# Day observations
# ---------------------------------------------------------------------------
def parse_day_observation(
    document: Any, *, where: str, source_file: str | None = None
) -> DayObservation:
    """Parse one day document. Pure — no filesystem, no config."""
    data = _closed_fields(document, DAY_FIELDS, where, "day observation")
    return DayObservation(
        trading_day=_as_date(_require(data, "trading_day", where), "trading_day", where),
        edition_published=_as_bool(
            _require(data, "edition_published", where), "edition_published", where
        ),
        edition_current=_as_bool(
            _require(data, "edition_current", where), "edition_current", where
        ),
        critical_layers_expected=_as_int(
            _require(data, "critical_layers_expected", where), "critical_layers_expected", where
        ),
        critical_layers_available=_as_int(
            _require(data, "critical_layers_available", where), "critical_layers_available", where
        ),
        release=_parse_release(_require(data, "release", where), f"{where}.release"),
        degraded_layers=_as_tuple(data.get("degraded_layers"), "degraded_layers", where),
        drill=(
            _as_text(data["drill"], "drill", where) if data.get("drill") not in (None, "") else None
        ),
        note=_as_text(data.get("note", ""), "note", where, allow_empty=True),
        source_file=source_file,
    )


@dataclass(frozen=True)
class DayObservationSet:
    """One observation per trading day. Two for one day is a contradiction."""

    days: tuple[DayObservation, ...] = ()
    loaded_from: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        seen = [day.trading_day for day in self.days]
        duplicates = sorted({d.isoformat() for d in seen if seen.count(d) > 1})
        if duplicates:
            raise TrialError(
                f"two day observations for {duplicates} — the product had one state per day, "
                "and two records of it would weight that day twice in availability"
            )

    @property
    def is_empty(self) -> bool:
        return not self.days

    def between(self, start: date, end: date) -> DayObservationSet:
        return DayObservationSet(
            days=tuple(day for day in self.days if start <= day.trading_day <= end),
            loaded_from=self.loaded_from,
        )


def load_day_observations(
    directory: str | os.PathLike[str] | None = None,
) -> DayObservationSet:
    """Read every ``*.yml`` in ``<trial dir>/days/``. Missing directory is empty."""
    root = _resolve(directory, DAYS_SUBDIR)
    if not root.is_dir():
        log.info("no trial day directory at %s — the product's own days were not observed", root)
        return DayObservationSet()


    days: list[DayObservation] = []
    files: list[str] = []
    for path in sorted(root.glob("*.yml")):
        document = _load_yaml(path)
        if document is None:
            continue
        days.append(_parse_named(parse_day_observation, document, path))
        files.append(str(path))
    return DayObservationSet(days=tuple(days), loaded_from=tuple(files))


def day_to_document(day: DayObservation) -> dict[str, Any]:
    """The YAML shape of one day observation — the inverse of the parser."""
    return {
        "trading_day": day.trading_day.isoformat(),
        "edition_published": day.edition_published,
        "edition_current": day.edition_current,
        "critical_layers_expected": day.critical_layers_expected,
        "critical_layers_available": day.critical_layers_available,
        "degraded_layers": list(day.degraded_layers),
        "drill": day.drill,
        "note": day.note,
        "release": {
            "code_revision": day.release.code_revision,
            "data_fingerprint": day.release.data_fingerprint,
            "captured_at": day.release.captured_at.isoformat(),
            "edition_id": day.release.edition_id,
            "dirty": day.release.dirty,
            "layer_count": day.release.layer_count,
        },
    }


def session_to_document(session: SessionRecord) -> dict[str, Any]:
    """The YAML shape of one session — the inverse of :func:`parse_sessions`.

    Used by ``scripts/trial.py`` so the interactive runbook and a hand-written
    file produce byte-identical records, and pinned by a round-trip test. A
    writer that drifts from its own reader is how a record store grows fields
    nothing reads.
    """
    document: dict[str, Any] = {
        "trader": session.trader,
        "task": session.task.value,
        "trading_day": session.trading_day.isoformat(),
        "started_at": session.started_at.isoformat(),
        "ended_at": session.ended_at.isoformat(),
        "outcome": session.outcome.value,
        "confidence": session.confidence,
        "would_act": session.would_act,
        "decision": session.decision,
        "pages_used": list(session.pages_used),
        "external_lookups": [
            {
                "tool": lookup.tool.value,
                "unanswered_question": lookup.unanswered_question,
                "answer_found": lookup.answer_found,
                "minutes": lookup.minutes,
                "tool_detail": lookup.tool_detail,
            }
            for lookup in session.external_lookups
        ],
        "issues": [
            {
                "classification": issue.classification.value,
                "summary": issue.summary,
                "evidence": issue.evidence,
                "affected_decision": issue.affected_decision,
                "severity": issue.severity.value,
                "page": issue.page,
                "expected": issue.expected,
                "observed": issue.observed,
            }
            for issue in session.issues
        ],
        "notes": list(session.notes),
        "evidence": list(session.evidence),
        "protocol_version": session.protocol_version,
        "release": {
            "code_revision": session.release.code_revision,
            "data_fingerprint": session.release.data_fingerprint,
            "captured_at": session.release.captured_at.isoformat(),
            "edition_id": session.release.edition_id,
            "dirty": session.release.dirty,
            "layer_count": session.release.layer_count,
        },
    }
    return document
