"""The source reliability record (B34 #520): one row per (run, layer), append-only.

``data/history/data_freshness.csv`` is current state — one row per layer,
overwritten every run — so it cannot say whether a layer was there last
Tuesday, nor see a run that died before the history commit. This store can.
The requirements are ``docs/plans/source-reliability-a19.md`` §2.

It is a CSV and nothing else, deliberately outside ``HISTORY_TABLES``: that
export rewrites a PK-keyed snapshot wholesale, and an append-only log must
never pass through a path that can reorder or drop its rows. Two files, both
under ``data/history/`` and both written only by the deploy workflow's record
step (invariant 6 — never by a PR):

``layer_runs.csv``    one row per (run, layer), the columns in ``COLUMNS``
``layer_alerts.csv``  one row per (run, layer) that run's ``ci-alert`` named,
                      the receipt ``scripts/ci_layer_alert.py`` writes after
                      gh accepted the comment — the evidence for
                      "surfaced within one run" (§3, T4)

The roster a run is filed under, the API keys it had and the layers it held
private are the ingesting run's own evidence (B38 #535): ``main.py`` writes
them (``build_run_evidence``, in the pipeline process, where the keys are)
and the recorder reads them back (``parse_run_evidence``). The record step
runs from a checkout the snapshot step may have rebased onto a moved main, in
a step with no API keys in its env — so the recorder path reads neither the
checkout's roster nor ``os.environ`` to decide what a run was. Privacy alone
also consults the checkout: a layer private in either list is blanked,
because withholding is the safe direction (invariant 4).

Nothing derived is stored: age at render, streaks and recovery are computed
by ``pipeline.reliability`` from these columns, so a stored figure can never
disagree with its inputs (invariant 2). A row already recorded is never
rewritten — an append that names an existing ``(run_id, layer_name)`` is
refused whole.
"""

from __future__ import annotations

import csv
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, fields
from datetime import date, datetime, timedelta, timezone
from typing import Any

from config import API_KEY_CATALOG, PRIVATE_LAYERS, missing_api_keys
from pipeline import grading

LAYER_RUNS_FILENAME = "layer_runs.csv"
LAYER_ALERTS_FILENAME = "layer_alerts.csv"
#: Written by ``main.py`` at the start of every run, next to the run summary
#: (gitignored data/storage/): the evidence a run that crashes before its
#: summary still leaves behind.
RUN_EVIDENCE_FILENAME = "run_evidence.json"
#: The run summary's key for the same block.
RUN_EVIDENCE_KEY = "run_evidence"

#: The graded word, unchanged, plus the two a run can say about a layer it
#: never graded. No new state words (A9 §4).
STATUS_SKIPPED_UNCONFIGURED = "skipped_unconfigured"
STATUS_NOT_RUN = "not_run"
RUN_STATUSES: tuple[str, ...] = (
    *grading.FRESHNESS_STATUSES, STATUS_SKIPPED_UNCONFIGURED, STATUS_NOT_RUN,
)

ALERT_COLUMNS: tuple[str, ...] = ("run_id", "layer_name")
#: The ending every row this module writes carries (B42 #555). Rows already
#: stored keep theirs — the committed record holds CRLF rows from before —
#: and both read back identically through ``csv.reader``.
RECORD_LINE_ENDING = "\n"


class LayerRunsError(RuntimeError):
    """The record cannot be read or appended as asked — never patched around."""


@dataclass(frozen=True)
class LayerRun:
    """One layer's verdict in one run.

    ``session_date`` is the CBOT session the run's edition serves (the
    settlement guard's answer, walked back to a weekday), not the runner's
    calendar day. ``observed_at`` is the run's own ``data_freshness``
    observation; ``rendered_at`` is the promoted edition's manifest
    ``generated_at``, ``None`` when nothing was promoted. A private layer
    carries neither.
    """

    run_id: str
    workflow: str
    run_started_at: str
    session_date: date
    layer_name: str
    status: str
    observed_at: str | None
    rendered_at: str | None


COLUMNS: tuple[str, ...] = tuple(f.name for f in fields(LayerRun))


def _history_dir() -> str:
    # Resolved at call time through pipeline.history so the one redirect a
    # test applies (HISTORY_DIR → tmp_path) covers this store too.
    from pipeline import history

    return history.HISTORY_DIR


def layer_runs_path() -> str:
    return os.path.join(_history_dir(), LAYER_RUNS_FILENAME)


def layer_alerts_path() -> str:
    return os.path.join(_history_dir(), LAYER_ALERTS_FILENAME)


# ── session and identity ──────────────────────────────────────────────────


def session_date_for(moment: datetime) -> date:
    """The weekday CBOT session an edition built at ``moment`` serves.

    The settlement guard's own answer (``EXCHANGE_SESSION``), walked back
    over the weekend: the guard works in calendar days, and a Saturday
    dispatch serves Friday's session. Weekdays only — there is no exchange
    holiday calendar, so a holiday run records against the holiday itself.
    """
    from fetchers._settlement import EXCHANGE_SESSION

    session = EXCHANGE_SESSION.last_settled_session(_as_utc(moment))
    while session.weekday() >= 5:
        session -= timedelta(days=1)
    return session


def run_identity(env: Mapping[str, str]) -> tuple[str, str]:
    """``(run_id, workflow)`` from the GitHub Actions environment.

    A re-run attempt is a separate run with its own rows, so attempt 2+ is
    suffixed; the retry workflow's dispatch is visible in ``workflow`` as the
    ``repository_dispatch`` event.
    """
    run_id = env.get("GITHUB_RUN_ID", "").strip()
    if not run_id:
        raise LayerRunsError("GITHUB_RUN_ID is not set — a record row needs the run it came from")
    attempt = env.get("GITHUB_RUN_ATTEMPT", "1").strip() or "1"
    if attempt != "1":
        run_id = f"{run_id}-{attempt}"
    workflow = env.get("GITHUB_WORKFLOW", "").strip() or "unknown"
    event = env.get("GITHUB_EVENT_NAME", "").strip() or "unknown"
    return run_id, f"{workflow}/{event}"


def _as_utc(moment: datetime) -> datetime:
    """A naive instant is UTC — every stamp this record handles is written in UTC."""
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def _parse_stamp(value: str) -> datetime:
    return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _utc_stamp(moment: datetime) -> str:
    return _as_utc(moment).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── the ingesting run's evidence (B38 #535) ───────────────────────────────


def build_run_evidence(started_at: datetime, roster: Sequence[str]) -> dict[str, Any]:
    """What a run is, said by the run itself: the JSON block ``main.py`` writes.

    ``roster`` is the production roster the run ran under; ``api_keys`` holds,
    per catalog key, whether it was set and which layers it skips without;
    ``private`` the layers the run held private. A boolean, never the value
    — this block lands in files a CI artifact carries. ``configured`` is
    ``config.missing_api_keys``' answer, the same one the pipeline's own
    startup warning gives. Called by the pipeline process only: it reads the
    environment, which is the point there and the bug in the recorder.
    """
    missing = missing_api_keys()
    return {
        "started_at": _utc_stamp(started_at),
        "roster": list(roster),
        "api_keys": {
            name: {"configured": name not in missing, "skips": list(spec.skips)}
            for name, spec in API_KEY_CATALOG.items()
        },
        "private": list(PRIVATE_LAYERS),
    }


@dataclass(frozen=True)
class RunEvidence:
    """A run's roster, the layers its unset keys skip and its private layers, as it recorded them."""

    started_at: datetime
    roster: tuple[str, ...]
    unconfigured: frozenset[str]
    private: frozenset[str]

    def configured(self, layer: str) -> bool:
        """False when an API key the layer is skipped without was unset in the run."""
        return layer not in self.unconfigured

    def is_private(self, layer: str) -> bool:
        """Private in the run or in this checkout — a mid-run rename or addition
        must never publish a private layer's observation (invariant 4)."""
        return layer in self.private or layer in PRIVATE_LAYERS


def parse_run_evidence(block: object, *, source: str, not_before: datetime) -> RunEvidence:
    """Read a ``build_run_evidence`` block back, refusing anything short of it.

    Self-contained: the keys and the layers each one skips are the run's
    own, so a key a mid-run merge added to or retired from this checkout's
    catalog changes nothing about what the run had. Every entry must carry a
    boolean — a guessed one invents a status. ``not_before`` is the pipeline
    step's start stamp; a block older than it is a previous run's file, not
    this run's word.
    """
    where = f"{source}: run evidence"
    if not isinstance(block, Mapping):
        raise LayerRunsError(f"{where} is not an object")
    try:
        started = _parse_stamp(str(block["started_at"]))
    except (KeyError, ValueError) as exc:
        raise LayerRunsError(f"{where} has no valid started_at") from exc
    floor = _as_utc(not_before)
    if started < floor:
        raise LayerRunsError(
            f"{where} predates this run (started_at {_utc_stamp(started)} < {_utc_stamp(floor)}) "
            "— a previous run's file"
        )
    roster = block.get("roster")
    if (
        not isinstance(roster, list) or not roster
        or not all(isinstance(name, str) and name for name in roster)
        or len(set(roster)) != len(roster)
    ):
        raise LayerRunsError(f"{where}: roster must be a non-empty list of distinct layer keys")
    keys = block.get("api_keys")
    if not isinstance(keys, Mapping) or not keys:
        raise LayerRunsError(f"{where}: api_keys is missing or empty")
    unconfigured: set[str] = set()
    for name, spec in keys.items():
        if not isinstance(spec, Mapping):
            raise LayerRunsError(f"{where}: api_keys.{name} is not an object")
        configured, skips = spec.get("configured"), spec.get("skips")
        if not isinstance(configured, bool):
            raise LayerRunsError(f"{where}: api_keys.{name}.configured is not a boolean")
        if not isinstance(skips, list) or not all(isinstance(layer, str) for layer in skips):
            raise LayerRunsError(f"{where}: api_keys.{name}.skips is not a list of layer keys")
        if not configured:
            unconfigured.update(skips)
    private = block.get("private")
    if not isinstance(private, list) or not all(isinstance(layer, str) for layer in private):
        raise LayerRunsError(f"{where}: private is not a list of layer keys")
    return RunEvidence(
        started_at=started, roster=tuple(roster),
        unconfigured=frozenset(unconfigured), private=frozenset(private),
    )


def rows_for_run(
    *,
    run_id: str,
    workflow: str,
    run_started_at: datetime,
    freshness: Mapping[str, Mapping[str, object]],
    evidence: RunEvidence,
    requested: Iterable[str] | None,
    rendered_at: str | None,
) -> list[LayerRun]:
    """One row per layer of the run's own roster (``evidence.roster``).

    A layer's grade is its ``data_freshness`` row only when that row was
    written by *this* run (``last_attempt`` at or after ``run_started_at``):
    the runner's DB is seeded from the committed CSV, so an untouched row is
    yesterday's verdict, and reading it as today's would invent a run.
    Ungraded layers are ``skipped_unconfigured`` when the run summary says
    they were asked for and the run's evidence says an API key they need was
    unset, else ``not_run``.
    ``requested=None`` means no run summary exists (the pipeline crashed),
    and then nothing can be said beyond ``not_run``.
    """
    started = _as_utc(run_started_at)
    session = session_date_for(started)
    asked = None if requested is None else set(requested)
    stamp = _utc_stamp(started)
    rows: list[LayerRun] = []
    for layer in evidence.roster:
        fresh = freshness.get(layer)
        attempt = fresh.get("last_attempt") if fresh else None
        status, observed = STATUS_NOT_RUN, None
        if fresh and attempt and _parse_stamp(str(attempt)) >= started:
            status = str(fresh.get("status") or "")
            raw = fresh.get("observed_at")
            observed = str(raw) if raw not in (None, "") and raw == raw else None
        elif asked is not None and layer in asked and not evidence.configured(layer):
            status = STATUS_SKIPPED_UNCONFIGURED
        private = evidence.is_private(layer)
        rows.append(LayerRun(
            run_id=run_id,
            workflow=workflow,
            run_started_at=stamp,
            session_date=session,
            layer_name=layer,
            status=status,
            observed_at=None if private else observed,
            rendered_at=None if private else rendered_at,
        ))
    return rows


# ── store ─────────────────────────────────────────────────────────────────


def _validate_shape(row: LayerRun) -> None:
    """What every row must be, whenever it was written."""
    where = f"layer_runs row ({row.run_id!r}, {row.layer_name!r})"
    if not row.run_id:
        raise LayerRunsError(f"{where}: run_id is blank")
    if not row.layer_name or not row.status:
        raise LayerRunsError(f"{where}: layer_name and status are required")
    if not isinstance(row.session_date, date) or row.session_date.weekday() >= 5:
        raise LayerRunsError(f"{where}: session_date {row.session_date!r} is not a weekday date")
    try:
        _parse_stamp(row.run_started_at)
    except ValueError as exc:
        raise LayerRunsError(f"{where}: run_started_at {row.run_started_at!r} is not a timestamp") from exc


def _validate(row: LayerRun, evidence: RunEvidence) -> None:
    """A row about to be appended: shape, plus its run's roster and today's vocabulary.

    Only on the write path, and against the roster the run itself recorded,
    not the checkout's: the recorder may run from code a mid-run merge moved
    on, and a layer that merge renamed is still this run's layer. A stored
    row is checked for shape alone, because it was valid against the roster
    of its own day: a layer retired or renamed later must not make the whole
    record unreadable — and so unappendable — from then on.
    """
    _validate_shape(row)
    where = f"layer_runs row ({row.run_id!r}, {row.layer_name!r})"
    if row.layer_name not in evidence.roster:
        raise LayerRunsError(f"{where}: layer is not in the run's roster")
    if row.status not in RUN_STATUSES:
        raise LayerRunsError(f"{where}: status {row.status!r} is not one of {list(RUN_STATUSES)}")
    if evidence.is_private(row.layer_name) and (row.observed_at or row.rendered_at):
        raise LayerRunsError(
            f"{where}: private layer carries layer_name + status only (invariant 4)"
        )


def _encode(row: LayerRun) -> list[str]:
    return [
        row.run_id, row.workflow, row.run_started_at, row.session_date.isoformat(),
        row.layer_name, row.status, row.observed_at or "", row.rendered_at or "",
    ]


def _read_csv(path: str, columns: tuple[str, ...]) -> list[list[str]]:
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header is None or tuple(header) != columns:
            raise LayerRunsError(f"{path}: header {header} is not {list(columns)}")
        out = []
        for line, raw in enumerate(reader, start=2):
            if len(raw) != len(columns):
                raise LayerRunsError(f"{path}:{line}: {len(raw)} fields, expected {len(columns)}")
            out.append(raw)
        return out


def _append_csv(path: str, columns: tuple[str, ...], lines: list[list[str]]) -> None:
    """Existing file + ``lines``, written atomically so git never commits half a file.

    Byte-append-only (B42 #555): an existing file — header-only included —
    is copied with ``newline=""``, so whatever endings it holds pass through unchanged and
    its bytes are an exact prefix of the result; new rows are LF. The B34
    append read the body in universal-newline mode and so rewrote each
    previous block's CRLF to LF — every record commit showed deletions.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out, lineterminator=RECORD_LINE_ENDING)
        if os.path.exists(path):
            with open(path, newline="", encoding="utf-8") as src:
                body = src.read()
            out.write(body if body.endswith("\n") else body + RECORD_LINE_ENDING)
        else:
            writer.writerow(columns)
        writer.writerows(lines)
    os.replace(tmp, path)


def read_runs() -> list[LayerRun]:
    """Every recorded row, in file (append) order. Absent store → ``[]``."""
    rows = []
    for raw in _read_csv(layer_runs_path(), COLUMNS):
        rec = dict(zip(COLUMNS, raw, strict=True))
        try:
            session = date.fromisoformat(rec["session_date"])
        except ValueError as exc:
            raise LayerRunsError(f"layer_runs: bad session_date {rec['session_date']!r}") from exc
        row = LayerRun(
            run_id=rec["run_id"], workflow=rec["workflow"], run_started_at=rec["run_started_at"],
            session_date=session, layer_name=rec["layer_name"], status=rec["status"],
            observed_at=rec["observed_at"] or None, rendered_at=rec["rendered_at"] or None,
        )
        _validate_shape(row)
        rows.append(row)
    return rows


def append_runs(evidence: RunEvidence, rows: Iterable[LayerRun]) -> int:
    """Append one run's ``rows``, checked against its ``evidence``.

    Refuses the whole batch if any (run, layer) is already there.
    """
    batch = list(rows)
    seen: set[tuple[str, str]] = set()
    for row in batch:
        _validate(row, evidence)
        key = (row.run_id, row.layer_name)
        if key in seen:
            raise LayerRunsError(f"layer_runs: {key} appears twice in one append")
        seen.add(key)
    existing = read_runs()
    clash = sorted(seen & {(r.run_id, r.layer_name) for r in existing})
    if clash:
        raise LayerRunsError(
            f"layer_runs: {len(clash)} (run_id, layer_name) row(s) already recorded, "
            f"first {clash[0]} — the record is append-only and never rewritten"
        )
    _append_csv(layer_runs_path(), COLUMNS, [_encode(r) for r in batch])
    return len(batch)


def read_alerts() -> set[tuple[str, str]]:
    """``(run_id, layer_name)`` pairs a run's own ``ci-alert`` comment named."""
    return {(raw[0], raw[1]) for raw in _read_csv(layer_alerts_path(), ALERT_COLUMNS)}


def append_alerts(evidence: RunEvidence, run_id: str, layers: Iterable[str]) -> int:
    """Record the layers ``run_id``'s alert named, against its roster. Same append-only refusal."""
    if not run_id:
        raise LayerRunsError("layer_alerts: run_id is blank")
    names = sorted(set(layers))
    unknown = [n for n in names if n not in evidence.roster]
    if unknown:
        raise LayerRunsError(f"layer_alerts: {unknown} not in the run's roster")
    path = layer_alerts_path()
    existing = _read_csv(path, ALERT_COLUMNS)
    clash = sorted({(run_id, n) for n in names} & {(r[0], r[1]) for r in existing})
    if clash:
        raise LayerRunsError(f"layer_alerts: {clash[0]} already recorded — append-only")
    if names:
        _append_csv(path, ALERT_COLUMNS, [[run_id, n] for n in names])
    return len(names)
