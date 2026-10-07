"""Desk-side transcription: a form export becomes session YAML (A6 #303, #412).

Participants never touch the repo. After each decision they complete a short
form; once a week the desk exports it as CSV and runs
``python scripts/trial.py transcribe <export.csv>``, which appends one session
per row to ``<trial dir>/sessions/<trading_day>.yml``.

Three rules carry this module:

**Same parser, same refusals.** A row is shaped into the YAML document a
hand-written session would be, then handed to :func:`records.parse_sessions`.
There is no second validator: the closed field set, the "an abandoned session
needs an issue" rule, the timezone rule and every other hard failure apply to a
form row exactly as they apply to a file, because a form must not be a way to
smuggle in a record the YAML path would refuse.

**The release stamp is read, never invented.** A participant cannot know which
commit or which freshness table they were looking at, so the form does not ask.
The stamp comes from the trading day's own ``days/<day>.yml`` observation — the
one the desk recorded after that day's deploy. A day with no observation cannot
be transcribed: the session would carry a stamp that was not observed, and a
wrong stamp is worse than no session.

**All rows or none.** Every row is parsed and checked for duplicates against
the existing store before the first byte is written. A bad row in the middle of
an export leaves the store exactly as it was, so a re-run after the fix never
double-counts the rows that were already good.

Column contract (``FORM_COLUMNS`` plus numbered groups):

    participant, task, trading_day, started_at, ended_at, outcome,
    confidence, would_act                               required
    decision, pages_used, notes, evidence               optional; lists split on "|"
    lookup_N_tool, lookup_N_question, lookup_N_answer_found,
    lookup_N_minutes, lookup_N_tool_detail              one external lookup per N
    issue_N_classification, issue_N_severity, issue_N_summary,
    issue_N_evidence, issue_N_affected_decision,
    issue_N_page, issue_N_expected, issue_N_observed    one issue per N

A numbered group whose every cell is blank is an unused form slot and is
skipped; a group with any cell filled is parsed in full and fails on what is
missing. An unknown column is a hard failure, not a warning.
"""

from __future__ import annotations

import csv
import logging
import os
import re
from collections.abc import Iterable, Mapping
from datetime import date
from pathlib import Path
from typing import Any

from analysis.trial.domain import ReleaseStamp, SessionRecord, TrialError
from analysis.trial.records import (
    SESSIONS_SUBDIR,
    SessionSet,
    _resolve,
    load_day_observations,
    load_sessions,
    parse_sessions,
    session_to_document,
)

log = logging.getLogger(__name__)

__all__ = [
    "FORM_COLUMNS",
    "ISSUE_GROUP_FIELDS",
    "LOOKUP_GROUP_FIELDS",
    "REQUIRED_COLUMNS",
    "parse_form_rows",
    "read_form_csv",
    "transcribe_csv",
    "write_transcribed",
]

#: Scalar columns a form row may carry. Every one is a session field.
FORM_COLUMNS: tuple[str, ...] = (
    "participant",
    "task",
    "trading_day",
    "started_at",
    "ended_at",
    "outcome",
    "confidence",
    "would_act",
    "decision",
    "pages_used",
    "notes",
    "evidence",
)

REQUIRED_COLUMNS: frozenset[str] = frozenset({
    "participant",
    "task",
    "trading_day",
    "started_at",
    "ended_at",
    "outcome",
    "confidence",
    "would_act",
})

#: Multi-valued text columns, split on ``|``.
_LIST_COLUMNS: frozenset[str] = frozenset({"pages_used", "notes", "evidence"})

#: Form suffix → record field, per external lookup.
LOOKUP_GROUP_FIELDS: dict[str, str] = {
    "tool": "tool",
    "question": "unanswered_question",
    "answer_found": "answer_found",
    "minutes": "minutes",
    "tool_detail": "tool_detail",
}

#: Form suffix → record field, per issue.
ISSUE_GROUP_FIELDS: dict[str, str] = {
    "classification": "classification",
    "severity": "severity",
    "summary": "summary",
    "evidence": "evidence",
    "affected_decision": "affected_decision",
    "page": "page",
    "expected": "expected",
    "observed": "observed",
}

_GROUP_RE = re.compile(r"^(lookup|issue)_(\d+)_([a-z_]+)$")

_TRUE = frozenset({"true", "yes", "y", "1"})
_FALSE = frozenset({"false", "no", "n", "0"})


def _bool_cell(value: str, field: str, where: str) -> bool | str:
    """A form's yes/no as a bool; anything else is passed through to fail loudly."""
    text = value.strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    # Hand the raw text to the record parser, whose _as_bool refuses it with
    # the same message a YAML 'maybe' gets.
    return value


def _int_cell(value: str, field: str, where: str) -> Any:
    text = value.strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        raise TrialError(f"{where}: {field} must be a whole number, got {value!r}") from None


def _list_cell(value: str) -> list[str]:
    return [part.strip() for part in value.split("|") if part.strip()]


def _classify_columns(columns: Iterable[str], where: str) -> tuple[dict[str, dict[int, dict[str, str]]], list[str]]:
    """Split the header into scalar columns and numbered groups. Unknown → raise."""
    groups: dict[str, dict[int, dict[str, str]]] = {"lookup": {}, "issue": {}}
    scalars: list[str] = []
    unknown: list[str] = []
    for column in columns:
        name = (column or "").strip()
        if name in FORM_COLUMNS:
            scalars.append(name)
            continue
        match = _GROUP_RE.match(name)
        if match:
            kind, index, suffix = match.group(1), int(match.group(2)), match.group(3)
            fields = LOOKUP_GROUP_FIELDS if kind == "lookup" else ISSUE_GROUP_FIELDS
            if suffix not in fields:
                unknown.append(name)
                continue
            groups[kind].setdefault(index, {})[suffix] = column
            continue
        unknown.append(name)
    if unknown:
        raise TrialError(
            f"{where}: unknown column(s) {sorted(unknown)} — known scalar columns are "
            f"{list(FORM_COLUMNS)}, groups are lookup_N_<{'|'.join(LOOKUP_GROUP_FIELDS)}> and "
            f"issue_N_<{'|'.join(ISSUE_GROUP_FIELDS)}>. A typo here would silently drop the thing "
            "the form exists to capture."
        )
    missing = sorted(REQUIRED_COLUMNS - set(scalars))
    if missing:
        raise TrialError(f"{where}: missing required column(s) {missing}")
    return groups, scalars


def _group_documents(
    row: Mapping[str, str],
    groups: dict[int, dict[str, str]],
    fields: dict[str, str],
    *,
    bool_fields: frozenset[str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for index in sorted(groups):
        cells = {suffix: (row.get(column) or "") for suffix, column in groups[index].items()}
        if not any(value.strip() for value in cells.values()):
            continue  # an unused form slot
        document: dict[str, Any] = {}
        for suffix, value in cells.items():
            field = fields[suffix]
            if not value.strip():
                continue
            document[field] = _bool_cell(value, field, "") if field in bool_fields else value.strip()
        out.append(document)
    return out


def _row_document(
    row: Mapping[str, str],
    *,
    groups: dict[str, dict[int, dict[str, str]]],
    where: str,
    release_for: Mapping[date, ReleaseStamp],
    directory: Path,
) -> dict[str, Any]:
    """Shape one CSV row into the YAML document the record parser expects."""
    cell = {name: (row.get(name) or "") for name in FORM_COLUMNS}
    document: dict[str, Any] = {
        "participant": cell["participant"],
        "task": cell["task"].strip(),
        "trading_day": cell["trading_day"].strip(),
        "started_at": cell["started_at"].strip(),
        "ended_at": cell["ended_at"].strip(),
        "outcome": cell["outcome"].strip(),
        "confidence": _int_cell(cell["confidence"], "confidence", where),
        "would_act": _bool_cell(cell["would_act"], "would_act", where),
        "decision": cell["decision"].strip(),
        "pages_used": _list_cell(cell["pages_used"]),
        "notes": _list_cell(cell["notes"]),
        "evidence": _list_cell(cell["evidence"]),
        "external_lookups": _group_documents(
            row, groups["lookup"], LOOKUP_GROUP_FIELDS, bool_fields=frozenset({"answer_found"})
        ),
        "issues": _group_documents(row, groups["issue"], ISSUE_GROUP_FIELDS, bool_fields=frozenset()),
    }

    # The release stamp: the day's recorded observation, or a refusal.
    try:
        day = date.fromisoformat(document["trading_day"])
    except ValueError:
        raise TrialError(f"{where}: trading_day must be an ISO date, got {document['trading_day']!r}") from None
    stamp = release_for.get(day)
    if stamp is None:
        raise TrialError(
            f"{where}: no day observation for {day.isoformat()} under {directory / 'days'} — the "
            "session's release stamp is read from that day's recorded edition, never invented. "
            "Record the day (`trial.py day --date "
            f"{day.isoformat()}`) only if that day's edition is what is checked out; otherwise this "
            "session cannot be stamped and is withheld."
        )
    document["release"] = {
        "code_revision": stamp.code_revision,
        "data_fingerprint": stamp.data_fingerprint,
        "captured_at": stamp.captured_at.isoformat(),
        "edition_id": stamp.edition_id,
        "dirty": stamp.dirty,
        "layer_count": stamp.layer_count,
    }
    return document


def parse_form_rows(
    rows: Iterable[Mapping[str, str]],
    *,
    directory: str | os.PathLike[str] | None,
    where: str,
) -> tuple[SessionRecord, ...]:
    """Every row as a validated :class:`SessionRecord`, or a :class:`TrialError`.

    ``directory`` is the trial record directory; its ``days/`` subdirectory
    supplies the release stamps. Pure apart from that read — nothing is written.
    """
    rows = list(rows)
    if not rows:
        raise TrialError(f"{where}: no rows — an empty export transcribes nothing, and says so")
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    groups, _scalars = _classify_columns(columns, where)

    root = _resolve(directory, "")  # the trial record directory itself
    observations = load_day_observations(root)
    release_for = {day.trading_day: day.release for day in observations.days}

    records: list[SessionRecord] = []
    for index, row in enumerate(rows, start=1):
        row_where = f"{where}[row {index}]"
        document = _row_document(
            row, groups=groups, where=row_where, release_for=release_for, directory=root
        )
        records.extend(parse_sessions([document], where=row_where, source_file=None))
    return tuple(records)


def read_form_csv(path: str | os.PathLike[str]) -> list[dict[str, str]]:
    """The export's rows as dicts. A missing file raises; so does an empty one."""
    source = Path(path)
    if not source.is_file():
        raise TrialError(f"form export {source} does not exist")
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise TrialError(f"{source}: no header row")
        return [dict(row) for row in reader]


def write_transcribed(
    records: Iterable[SessionRecord],
    *,
    directory: str | os.PathLike[str] | None = None,
) -> list[Path]:
    """Append validated sessions to their day files. Duplicates refuse before any write."""
    import yaml

    from analysis.trial.sanitize import assert_private_path

    new = tuple(records)
    target_dir = assert_private_path(_resolve(directory, SESSIONS_SUBDIR), where="session directory")
    existing = load_sessions(directory)
    # SessionSet raises on a duplicate id — within the export, or against the store.
    SessionSet(sessions=existing.sessions + new, loaded_from=existing.loaded_from)

    by_day: dict[date, list[dict[str, Any]]] = {}
    for record in new:
        by_day.setdefault(record.trading_day, []).append(session_to_document(record))

    written: list[Path] = []
    target_dir.mkdir(parents=True, exist_ok=True)
    for day in sorted(by_day):
        target = target_dir / f"{day.isoformat()}.yml"
        text = yaml.safe_dump(by_day[day], sort_keys=False, allow_unicode=True)
        if target.exists() and target.stat().st_size > 0:
            with target.open("a", encoding="utf-8") as handle:
                handle.write("\n" + text)
        else:
            target.write_text(text, encoding="utf-8")
        written.append(target)
    return written


def transcribe_csv(
    path: str | os.PathLike[str],
    *,
    directory: str | os.PathLike[str] | None = None,
) -> list[Path]:
    """The whole entry point: CSV export → validated sessions → day files."""
    from analysis.trial.sanitize import assert_private_path

    assert_private_path(_resolve(directory, SESSIONS_SUBDIR), where="session directory")
    rows = read_form_csv(path)
    records = parse_form_rows(rows, directory=directory, where=str(path))
    written = write_transcribed(records, directory=directory)
    log.info("transcribed %d session(s) into %d day file(s)", len(records), len(written))
    return written
