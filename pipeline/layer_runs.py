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

Nothing derived is stored: age at render, streaks and recovery are computed
by ``pipeline.reliability`` from these columns, so a stored figure can never
disagree with its inputs (invariant 2). A row already recorded is never
rewritten — an append that names an existing ``(run_id, layer_name)`` is
refused whole.
"""

from __future__ import annotations

import csv
import os
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, fields
from datetime import date, datetime, timedelta, timezone

from config import API_KEY_CATALOG, PRIVATE_LAYERS, PRODUCTION_LAYER_KEYS
from pipeline import grading

LAYER_RUNS_FILENAME = "layer_runs.csv"
LAYER_ALERTS_FILENAME = "layer_alerts.csv"

#: The graded word, unchanged, plus the two a run can say about a layer it
#: never graded. No new state words (A9 §4).
STATUS_SKIPPED_UNCONFIGURED = "skipped_unconfigured"
STATUS_NOT_RUN = "not_run"
RUN_STATUSES: tuple[str, ...] = (
    *grading.FRESHNESS_STATUSES, STATUS_SKIPPED_UNCONFIGURED, STATUS_NOT_RUN,
)

ALERT_COLUMNS: tuple[str, ...] = ("run_id", "layer_name")


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

    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    session = EXCHANGE_SESSION.last_settled_session(moment)
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


def layer_configured(layer: str, env: Mapping[str, str] | None = None) -> bool:
    """False when an API key the layer is skipped without is unset."""
    env = os.environ if env is None else env
    return all(
        (env.get(name) or "").strip()
        for name, spec in API_KEY_CATALOG.items()
        if layer in spec.skips
    )


def _parse_stamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def rows_for_run(
    *,
    run_id: str,
    workflow: str,
    run_started_at: datetime,
    freshness: Mapping[str, Mapping[str, object]],
    requested: Iterable[str] | None,
    configured: Callable[[str], bool],
    rendered_at: str | None,
) -> list[LayerRun]:
    """Every production layer's row for one run.

    A layer's grade is its ``data_freshness`` row only when that row was
    written by *this* run (``last_attempt`` at or after ``run_started_at``):
    the runner's DB is seeded from the committed CSV, so an untouched row is
    yesterday's verdict, and reading it as today's would invent a run.
    Ungraded layers are ``skipped_unconfigured`` when the run summary says
    they were asked for and their API key is unset, else ``not_run``.
    ``requested=None`` means no run summary exists (the pipeline crashed),
    and then nothing can be said beyond ``not_run``.
    """
    started = run_started_at if run_started_at.tzinfo else run_started_at.replace(tzinfo=timezone.utc)
    session = session_date_for(started)
    asked = None if requested is None else set(requested)
    stamp = started.strftime("%Y-%m-%dT%H:%M:%SZ")
    rows: list[LayerRun] = []
    for layer in PRODUCTION_LAYER_KEYS:
        fresh = freshness.get(layer)
        attempt = fresh.get("last_attempt") if fresh else None
        status, observed = STATUS_NOT_RUN, None
        if fresh and attempt and _parse_stamp(str(attempt)) >= started:
            status = str(fresh.get("status") or "")
            raw = fresh.get("observed_at")
            observed = str(raw) if raw not in (None, "") and raw == raw else None
        elif asked is not None and layer in asked and not configured(layer):
            status = STATUS_SKIPPED_UNCONFIGURED
        private = layer in PRIVATE_LAYERS
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


def _validate(row: LayerRun) -> None:
    """A row about to be appended: shape, plus today's roster and vocabulary.

    Only on the write path. A stored row is checked for shape alone, because
    it was valid against the roster of its own day: a layer retired or renamed
    later must not make the whole record unreadable — and so unappendable —
    from then on.
    """
    _validate_shape(row)
    where = f"layer_runs row ({row.run_id!r}, {row.layer_name!r})"
    if row.layer_name not in PRODUCTION_LAYER_KEYS:
        raise LayerRunsError(f"{where}: layer is not in config.PRODUCTION_LAYER_KEYS")
    if row.status not in RUN_STATUSES:
        raise LayerRunsError(f"{where}: status {row.status!r} is not one of {list(RUN_STATUSES)}")
    if row.layer_name in PRIVATE_LAYERS and (row.observed_at or row.rendered_at):
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


def _append_csv(path: str, columns: tuple[str, ...], existing: int, lines: list[list[str]]) -> None:
    """Existing file + ``lines``, written atomically so git never commits half a file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as out:
        if existing:
            with open(path, encoding="utf-8") as src:
                body = src.read()
            out.write(body if body.endswith("\n") else body + "\n")
        else:
            csv.writer(out).writerow(columns)
        csv.writer(out).writerows(lines)
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


def append_runs(rows: Iterable[LayerRun]) -> int:
    """Append ``rows``; refuse the whole batch if any (run, layer) is already there."""
    batch = list(rows)
    seen: set[tuple[str, str]] = set()
    for row in batch:
        _validate(row)
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
    _append_csv(layer_runs_path(), COLUMNS, len(existing), [_encode(r) for r in batch])
    return len(batch)


def read_alerts() -> set[tuple[str, str]]:
    """``(run_id, layer_name)`` pairs a run's own ``ci-alert`` comment named."""
    return {(raw[0], raw[1]) for raw in _read_csv(layer_alerts_path(), ALERT_COLUMNS)}


def append_alerts(run_id: str, layers: Iterable[str]) -> int:
    """Record the layers ``run_id``'s alert named. Same append-only refusal."""
    if not run_id:
        raise LayerRunsError("layer_alerts: run_id is blank")
    names = sorted(set(layers))
    unknown = [n for n in names if n not in PRODUCTION_LAYER_KEYS]
    if unknown:
        raise LayerRunsError(f"layer_alerts: {unknown} not in config.PRODUCTION_LAYER_KEYS")
    path = layer_alerts_path()
    existing = _read_csv(path, ALERT_COLUMNS)
    clash = sorted({(run_id, n) for n in names} & {(r[0], r[1]) for r in existing})
    if clash:
        raise LayerRunsError(f"layer_alerts: {clash[0]} already recorded — append-only")
    if names:
        _append_csv(path, ALERT_COLUMNS, len(existing), [[run_id, n] for n in names])
    return len(names)
