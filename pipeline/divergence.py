"""Quarantine guard for the store layer (T19 · F9 #67; A2 #299 via B5 #311).

`INSERT OR REPLACE` is what makes the pipeline safe to re-run, and it is
also what lets one corrupted fetch destroy good data: a bad close for a
date already stored overwrites the good one under the same key, silently,
with no trace that a better number was ever there. And a bad close for a
*new* date — the 10× unit misread on today's session — used to sail
through with a warning, because there was nothing under its key to
disagree with.

This module is the guard. Before a write, every incoming row of a
registered table is judged by one of two rules, with one threshold
(`SAME_PK_DIVERGENCE_QUARANTINE_THRESHOLD`, 20%) and one verdict:

- **Same-PK revision.** The key is already stored with a non-zero value and
  the incoming value disagrees past the threshold. A re-print of one
  session that moves a fifth is not a correction any venue publishes.
- **Day-over-day move.** The key is not stored; the incoming value is
  judged against the latest *earlier accepted* session of the same series
  — a row already stored, or one accepted earlier in this same frame,
  whichever is later. 20% is deliberately above CBOT's expanded limits,
  so a move past it cannot be a legitimate session. A series with no
  stored history at all is not move-checked: the first observation of a
  series is not an anomaly, and on the ephemeral CI database the
  self-healing tables would otherwise quarantine their own fifteen-year
  history against itself.

Either way the row is **held**, not written: appended to
`quarantined_revisions`, logged at ERROR, and the stored value stands. A
held row is never the predecessor of the next one — a bad print must not
drag the following good session down with it.

The dividing line A2 drew: quarantine what might be true; reject what can
never be. A big session is a fact about the market and must store (clean
keeps warning past 10%). A partial bar is rejected outright in
`pipeline.clean`, because nothing could ever release it.

**Release is evidence-only.** No manual path, no clock. The one carve-out
is *confirmation*: a later, independent fetch (a different pipeline run,
see `pipeline.quarantine.current_run_id`) re-serving a held value within
`QUARANTINE_CONFIRMATION_TOLERANCE` is two observations agreeing against
one stored value. The incoming row stores and the quarantine row records
its release. This exists for the tables that cannot self-heal (CEPEA,
SAFEX, mandi — upstream serves only today); under pure evidence-only a
wrongly-held legitimate correction there would be lost forever. A replay
inside the same run is one observation and releases nothing.

**Quarantine, not crash.** A corrupt upstream day should not take the run
down — the good rows in the same frame still write, and the held ones are
recorded loudly enough to act on: `pipeline.quarantine` tallies every
verdict for the run summary, the CI alert and the briefing.
"""

from __future__ import annotations

import json
import logging
from bisect import bisect_left
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, NamedTuple

import pandas as pd

from config import (
    QUARANTINE_CONFIRMATION_TOLERANCE,
    SAME_PK_DIVERGENCE_QUARANTINE_THRESHOLD,
)
from pipeline import quarantine

logger = logging.getLogger(__name__)

KIND_SAME_PK = "same_pk_revision"
KIND_DAILY_MOVE = "daily_move"


class TableGuard(NamedTuple):
    """What the guard screens on one table."""

    value_column: str
    key_cols: tuple[str, ...]
    date_column: str

    @property
    def series_cols(self) -> tuple[str, ...]:
        """The key columns that name the series — everything but the date."""
        return tuple(c for c in self.key_cols if c != self.date_column)


# Table → the one value column screened, the primary-key columns that
# identify "the same observation", and which of those is the session date.
#
# Only price-bearing tables belong here. A weather series, a tonnage or a
# positioning report legitimately swings by any amount on revision, and a
# guard that fired on them would train its own readers to ignore it.
GUARDED_TABLES: dict[str, TableGuard] = {
    # Named by the ticket: the two core price/FX tables.
    "prices": TableGuard("Close", ("commodity", "Date"), "Date"),
    "currencies": TableGuard("Close", ("pair", "Date"), "Date"),
    # Layer 11b named-contract bars: same venue and provider as `prices`.
    # Listed contracts self-heal by re-download, but expired contracts'
    # rows are the only copy anywhere (Yahoo delists the symbol), so a
    # silent overwrite here is exactly the unrecoverable kind.
    "contract_bars": TableGuard("Close", ("ticker", "Date"), "Date"),
    # Snapshot-only price tables (pipeline.history.HISTORY_TABLES). Their
    # upstreams publish only the current session, so the committed CSV is the
    # only copy and an overwrite here is unrecoverable — the sharpest form of
    # the defect this guard exists for.
    "brazil_spot_prices": TableGuard("price_brl", ("Date", "commodity"), "Date"),
    "safex_prices": TableGuard("Close", ("Date", "commodity"), "Date"),
    "india_domestic_prices": TableGuard("Close", ("Date", "commodity"), "Date"),
}

# How many held rows are named individually before the log collapses to a
# count. A wholesale-corrupt fetch should be one legible alert, not a
# thousand lines nobody reads to the end of.
_MAX_LOGGED_ROWS = 5


class QuarantinedRevision(NamedTuple):
    """One held row, with everything needed to argue with the verdict."""

    table_name: str
    row_key: str
    value_column: str
    stored_value: float
    incoming_value: float
    divergence: float
    threshold: float
    row_json: str
    label: str
    detected_at: str
    kind: str
    run_id: str


class ReleasedRevision(NamedTuple):
    """One previously held row that this run's independent fetch confirmed."""

    table_name: str
    row_key: str
    value_column: str
    recorded_incoming: float
    released_at: str
    released_run_id: str


class _PriorHold(NamedTuple):
    incoming_value: float
    run_id: str | None
    released: bool


def _key_of(values: tuple[Any, ...]) -> str:
    """Stable text form of a primary key, for lookup and for storage."""
    return json.dumps(["" if v is None else str(v) for v in values])


def _jsonable(record: Mapping[Any, Any]) -> dict[str, Any]:
    """NaN → null. `json.dumps` writes a bare `NaN`, which is not JSON, and a
    quarantine record nobody can parse is not an audit trail."""
    return {
        str(key): None if isinstance(value, float) and pd.isna(value) else value
        for key, value in record.items()
    }


def _as_float(value: Any) -> float | None:
    """The value as a float, or None when it is not a number.

    A non-numeric cell is not *invalid* here, it is unmeasurable: divergence
    is a ratio and there is none to take. It passes through to the write,
    where the destination column's own typing is the authority — a screen
    that refused what the column would accept would be a second schema,
    and one nobody asked for.
    """
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _date_key(value: Any) -> str | None:
    """A session date as a sortable ISO day, or None when there is none.

    Every `save_*` ISO-formats its date column before `_save` (so this is
    normally a pass-through), but a frame that reaches the guard with a
    Timestamp must still sort beside the stored TEXT dates.
    """
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    text = str(value)
    return text[:10] if text else None


def _series_of(row: Mapping[str, Any], guard: TableGuard) -> str:
    return "/".join(str(row.get(c)) for c in guard.series_cols)


def _stored(
    conn, table: str, guard: TableGuard, frame: pd.DataFrame
) -> tuple[dict[str, float], dict[str, list[tuple[str, float]]]]:
    """What is already stored for the series present in `frame`.

    Returns the same-PK lookup (row key → value) and, per series, every
    stored session as a date-sorted list — the material of the day-over-day
    rule. Scoped to the frame's own series — commodity, pair, ticker — so a
    save of one series never scans the whole table. NULL values are
    "never learned" and appear in neither: there is nothing to diverge from.
    """
    anchor = guard.series_cols[0]
    anchors = sorted({str(v) for v in frame[anchor].tolist() if v is not None})
    if not anchors:
        return {}, {}
    placeholders = ",".join("?" * len(anchors))
    columns = [*guard.key_cols, guard.value_column]
    sql = (  # noqa: S608 — table, columns and key names come from GUARDED_TABLES
        f"SELECT {','.join(columns)} FROM {table} WHERE {anchor} IN ({placeholders})"
    )
    by_key: dict[str, float] = {}
    sessions: dict[str, list[tuple[str, float]]] = {}
    for row in conn.execute(sql, anchors).fetchall():
        record = dict(zip(columns, row, strict=True))
        value = _as_float(record[guard.value_column])
        if value is None:
            continue
        by_key[_key_of(tuple(record[c] for c in guard.key_cols))] = value
        date = _date_key(record[guard.date_column])
        if date is not None:
            sessions.setdefault(_series_of(record, guard), []).append((date, value))
    for entries in sessions.values():
        entries.sort()
    return by_key, sessions


def _prior_holds(conn, table: str, guard: TableGuard, frame: pd.DataFrame) -> dict[str, list[_PriorHold]]:
    """Earlier verdicts on the keys in `frame`, for the confirmation rule."""
    keys = sorted({
        _key_of(tuple(row.get(c) for c in guard.key_cols))
        for row in frame[list(guard.key_cols)].to_dict(orient="records")
    })
    holds: dict[str, list[_PriorHold]] = {}
    for start in range(0, len(keys), 500):
        chunk = keys[start:start + 500]
        placeholders = ",".join("?" * len(chunk))
        rows = conn.execute(
            "SELECT row_key, incoming_value, run_id, released_at "
            "FROM quarantined_revisions "
            f"WHERE table_name = ? AND value_column = ? AND row_key IN ({placeholders})",
            [table, guard.value_column, *chunk],
        ).fetchall()
        for row_key, incoming, run_id, released_at in rows:
            value = _as_float(incoming)
            if value is None:
                continue
            holds.setdefault(row_key, []).append(
                _PriorHold(value, run_id, released_at is not None)
            )
    return holds


def _confirmed(incoming: float, holds: list[_PriorHold], run_id: str) -> list[_PriorHold] | None:
    """The earlier holds this incoming value confirms, or None if it confirms none.

    Independent means a different run. Within tolerance means the same
    number: float noise and a provider's re-rounding, not a different bad
    value. The returned list may be empty — every matching hold was already
    released — in which case the value is simply accepted again.
    """
    matching = [
        h for h in holds
        if h.run_id != run_id and h.incoming_value != 0
        and abs(incoming - h.incoming_value) / abs(h.incoming_value)
        <= QUARANTINE_CONFIRMATION_TOLERANCE
    ]
    if not matching:
        return None
    return [h for h in matching if not h.released]


def _latest_before(sessions: list[tuple[str, float]], date: str) -> tuple[str, float] | None:
    position = bisect_left(sessions, (date, float("-inf")))
    return sessions[position - 1] if position else None


def screen(
    conn,
    table: str,
    frame: pd.DataFrame,
    key_cols: list[str],
    label: str,
) -> tuple[pd.DataFrame, list[QuarantinedRevision], list[ReleasedRevision]]:
    """Split `frame` into rows safe to write, rows to hold, and holds released.

    Returns the frame itself (not a copy) when nothing is held back, which
    is the overwhelmingly common case and the one that must stay cheap.

    A row is held only when the table is registered, the incoming value is
    a number, a reference exists (the stored same-PK value, else the latest
    earlier accepted session of the series), that reference is non-zero,
    they disagree by more than the threshold, and no earlier independent
    run held this same value. Anything else writes — a first observation,
    a stored zero (a ratio against which is not a measurement), a stored
    NULL, a series with no history. Erring toward writing is the point: a
    guard that swallowed legitimate rows would be a silent failure of its
    own.
    """
    registered = GUARDED_TABLES.get(table)
    if registered is None or frame.empty:
        return frame, [], []
    guard = registered
    if guard.value_column not in frame.columns:
        return frame, [], []
    if tuple(key_cols) != guard.key_cols or any(c not in frame.columns for c in guard.key_cols):
        # The caller keys this table differently from the registry. Screening
        # on a key that is not the row's identity would compare unrelated
        # observations, so decline rather than guess.
        logger.warning(
            "divergence screen skipped for %s: caller keys on %s, registry on %s",
            table, list(key_cols), list(guard.key_cols),
        )
        return frame, [], []

    threshold = float(SAME_PK_DIVERGENCE_QUARANTINE_THRESHOLD)
    stored_by_key, stored_sessions = _stored(conn, table, guard, frame)
    if not stored_by_key:
        # Nothing stored for any series in the frame: no same-PK rule and no
        # day-over-day rule can apply — and a prior hold needs a stored
        # value it disagreed with, so there is nothing to release either.
        return frame, [], []
    prior_holds = _prior_holds(conn, table, guard, frame)

    run_id = quarantine.current_run_id()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    held: list[QuarantinedRevision] = []
    held_positions: list[int] = []
    released: list[ReleasedRevision] = []

    # to_dict rather than itertuples: itertuples silently renames any column
    # that is not a Python identifier, and a screen that quietly stopped
    # finding its value column would be the failure mode this guard is for.
    rows: list[dict[str, Any]] = [
        {str(k): v for k, v in record.items()} for record in frame.to_dict(orient="records")
    ]
    # Walk each series in session order so an accepted row can be the
    # predecessor of the next one; positions are what we hold by.
    order = sorted(
        range(len(rows)),
        key=lambda i: (_series_of(rows[i], guard), _date_key(rows[i].get(guard.date_column)) or ""),
    )
    accepted_latest: dict[str, tuple[str, float]] = {}

    def accept(series: str, date: str | None, value: float) -> None:
        if date is None:
            return
        latest = accepted_latest.get(series)
        if latest is None or date >= latest[0]:
            accepted_latest[series] = (date, value)

    for position in order:
        row = rows[position]
        incoming = _as_float(row.get(guard.value_column))
        if incoming is None:
            continue
        series = _series_of(row, guard)
        date = _date_key(row.get(guard.date_column))
        row_key = _key_of(tuple(row.get(c) for c in guard.key_cols))

        same_pk = stored_by_key.get(row_key)
        if same_pk is not None:
            if same_pk == 0:
                accept(series, date, incoming)
                continue
            kind, reference = KIND_SAME_PK, same_pk
        else:
            sessions = stored_sessions.get(series)
            if not sessions or date is None:
                # No stored history for this series (or no session date to
                # order by): the first observation is not an anomaly.
                accept(series, date, incoming)
                continue
            reference_session = _latest_before(sessions, date)
            in_frame = accepted_latest.get(series)
            if in_frame is not None and in_frame[0] < date and (
                reference_session is None or in_frame[0] >= reference_session[0]
            ):
                reference_session = in_frame
            if reference_session is None or reference_session[1] == 0:
                accept(series, date, incoming)
                continue
            kind, reference = KIND_DAILY_MOVE, reference_session[1]

        gap = abs(incoming - reference) / abs(reference)
        if gap <= threshold:
            accept(series, date, incoming)
            continue

        confirmed = _confirmed(incoming, prior_holds.get(row_key, []), run_id)
        if confirmed is not None:
            accept(series, date, incoming)
            for hold in confirmed:
                released.append(
                    ReleasedRevision(table, row_key, guard.value_column, hold.incoming_value, now, run_id)
                )
                quarantine.note(
                    table=table, label=label, kind=kind, series=series,
                    date=date or "", divergence=gap, released=True,
                )
            continue

        held.append(
            QuarantinedRevision(
                table_name=table,
                row_key=row_key,
                value_column=guard.value_column,
                stored_value=reference,
                incoming_value=incoming,
                divergence=gap,
                threshold=threshold,
                row_json=json.dumps(_jsonable(row), default=str),
                label=label,
                detected_at=now,
                kind=kind,
                run_id=run_id,
            )
        )
        held_positions.append(position)
        quarantine.note(
            table=table, label=label, kind=kind, series=series,
            date=date or "", divergence=gap, released=False,
        )

    if released:
        _log_released(table, label, released)
    if not held:
        return frame, [], released

    _log(table, label, held)
    # Positional, not by index label: a frame carrying duplicate index labels
    # would drop every row sharing a held row's label, silently discarding
    # good observations — the opposite of what this function is for.
    dropped = set(held_positions)
    accepted = frame.iloc[[i for i in range(len(frame)) if i not in dropped]]
    return accepted, held, released


def _log(table: str, label: str, held: list[QuarantinedRevision]) -> None:
    """Flag the quarantine loudly — this is the alert, not a debug aid."""
    for revision in held[:_MAX_LOGGED_ROWS]:
        reference = (
            "would overwrite stored" if revision.kind == KIND_SAME_PK
            else "moved day-over-day from the latest accepted session"
        )
        logger.error(
            "QUARANTINE [%s] %s key=%s: %s=%s %s %s "
            "(%.1f%% divergence, threshold %.0f%%) — the stored value stands "
            "and the incoming row is held",
            label, table, revision.row_key, revision.value_column,
            revision.incoming_value, reference, revision.stored_value,
            revision.divergence * 100, revision.threshold * 100,
        )
    if len(held) > _MAX_LOGGED_ROWS:
        logger.error(
            "QUARANTINE [%s] %s: %d rows held in total (%d shown) — a fetch this "
            "wrong across the board is an upstream fault, not a revision",
            label, table, len(held), _MAX_LOGGED_ROWS,
        )


def _log_released(table: str, label: str, released: list[ReleasedRevision]) -> None:
    for revision in released[:_MAX_LOGGED_ROWS]:
        logger.warning(
            "QUARANTINE RELEASED [%s] %s key=%s: %s=%s re-served by an independent "
            "later fetch — two observations agree against the stored value, which "
            "is now replaced",
            label, table, revision.row_key, revision.value_column,
            revision.recorded_incoming,
        )
    if len(released) > _MAX_LOGGED_ROWS:
        logger.warning(
            "QUARANTINE RELEASED [%s] %s: %d rows released in total (%d shown)",
            label, table, len(released), _MAX_LOGGED_ROWS,
        )


def record(conn, held: list[QuarantinedRevision]) -> int:
    """Persist held rows to `quarantined_revisions`. Returns rows written.

    Uses the caller's connection so the quarantine lands in the same
    transaction as the accepted rows: a run that rolled back its write must
    not leave behind a record of a rejection that never happened.
    """
    if not held:
        return 0
    conn.executemany(
        """INSERT OR REPLACE INTO quarantined_revisions
           (table_name, row_key, value_column, stored_value, incoming_value,
            divergence, threshold, row_json, label, detected_at, kind, run_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [tuple(revision) for revision in held],
    )
    return len(held)


def release(conn, released: list[ReleasedRevision]) -> int:
    """Mark confirmed holds released, in the write's own transaction.

    Same reasoning as `record`: a release row without the released value
    actually stored would be a lie, so both commit or neither does.
    """
    if not released:
        return 0
    conn.executemany(
        """UPDATE quarantined_revisions
           SET released_at = ?, released_run_id = ?
           WHERE table_name = ? AND row_key = ? AND value_column = ?
             AND incoming_value = ? AND released_at IS NULL""",
        [
            (r.released_at, r.released_run_id, r.table_name, r.row_key, r.value_column, r.recorded_incoming)
            for r in released
        ],
    )
    return len(released)
