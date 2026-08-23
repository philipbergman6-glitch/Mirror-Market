"""Same-PK divergence quarantine for the store layer (T19 · F9, #67).

`INSERT OR REPLACE` is what makes the pipeline safe to re-run, and it is
also what lets one corrupted fetch destroy good data: a bad close for a
date already stored overwrites the good one under the same key, silently,
with no trace that a better number was ever there.

This module is the guard. Before a write, the incoming rows for a
registered table are compared against what is already stored **for their
own primary key** — not against yesterday, not against a rolling band.
A row whose price disagrees with its predecessor by more than
`SAME_PK_DIVERGENCE_QUARANTINE_THRESHOLD` is held back, appended to
`quarantined_revisions`, and logged at ERROR. The stored value stands.

Three deliberate narrownesses:

- **Same key, not same neighbourhood.** A genuine 25% session is a fact
  about the market and must store; a 25% *revision of one already-stored
  session* is a fact about the fetch. Only the second is caught here, and
  `pipeline.clean._validate_price_data` keeps warning about the first.
- **One column per table.** The close is what every downstream analysis
  reads, so it is what is screened; the rest of the row travels with it,
  because holding back a row's close while writing its high would leave a
  bar nobody could reconcile.
- **Quarantine, not crash.** A corrupt upstream day should not take the
  run down — the good rows in the same frame still write, and the held
  ones are recorded loudly enough to act on. What a crash would protect
  against, the stored value already survives.

**Where this bites.** For the self-healing layers (`prices`, `currencies`)
a bad overwrite would repair itself on the next run, so the guard mostly
buys a clean local database and an alert. The tables that cannot self-heal
— CEPEA/AgRural, SAFEX, mandi, whose upstreams serve only today — are the
ones where an overwrite is permanent, and they are registered for exactly
that reason. Note the corollary: on the ephemeral CI database the
self-healing tables start empty each morning, so nothing is stored to
diverge from and the guard is inert there by construction.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, NamedTuple

import pandas as pd

from config import SAME_PK_DIVERGENCE_QUARANTINE_THRESHOLD

logger = logging.getLogger(__name__)

# Table → the one value column screened against its stored predecessor, and
# the primary-key columns that identify "the same observation".
#
# Only price-bearing tables belong here. A weather series, a tonnage or a
# positioning report legitimately swings by any amount on revision, and a
# guard that fired on them would train its own readers to ignore it.
GUARDED_TABLES: dict[str, tuple[str, tuple[str, ...]]] = {
    # Named by the ticket: the two core price/FX tables.
    "prices": ("Close", ("commodity", "Date")),
    "currencies": ("Close", ("pair", "Date")),
    # Layer 11b per-contract closes: same venue and provider as `prices`,
    # same self-healing caveat — the guard mostly buys a clean local database.
    "contract_history": ("close", ("ticker", "date")),
    # Snapshot-only price tables (pipeline.history.HISTORY_TABLES). Their
    # upstreams publish only the current session, so the committed CSV is the
    # only copy and an overwrite here is unrecoverable — the sharpest form of
    # the defect this guard exists for.
    "brazil_spot_prices": ("price_brl", ("Date", "commodity")),
    "safex_prices": ("Close", ("Date", "commodity")),
    "india_domestic_prices": ("Close", ("Date", "commodity")),
}

# How many held rows are named individually before the log collapses to a
# count. A wholesale-corrupt fetch should be one legible alert, not a
# thousand lines nobody reads to the end of.
_MAX_LOGGED_ROWS = 5


class QuarantinedRevision(NamedTuple):
    """One rejected row, with everything needed to argue with the verdict."""

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
    that raised on it would take a whole save down over a row it has no
    opinion about.
    """
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _stored_values(
    conn, table: str, value_column: str, key_cols: tuple[str, ...], frame: pd.DataFrame
) -> dict[str, float]:
    """Read the currently-stored value for every key present in `frame`.

    Scoped to the frame's own values of the first key column — commodity,
    pair, gauge — so a save of one series never scans the whole table.
    """
    anchor = key_cols[0]
    anchors = sorted({str(v) for v in frame[anchor].tolist() if v is not None})
    if not anchors:
        return {}
    placeholders = ",".join("?" * len(anchors))
    sql = (  # noqa: S608 — table, columns and key names come from GUARDED_TABLES
        f"SELECT {','.join(key_cols)}, {value_column} FROM {table} "
        f"WHERE {anchor} IN ({placeholders})"
    )
    stored: dict[str, float] = {}
    for row in conn.execute(sql, anchors).fetchall():
        value = row[-1]
        if value is None:
            # NULL is "never learned" — there is nothing to diverge from.
            continue
        stored[_key_of(tuple(row[:-1]))] = float(value)
    return stored


def screen(
    conn,
    table: str,
    frame: pd.DataFrame,
    key_cols: list[str],
    label: str,
) -> tuple[pd.DataFrame, list[QuarantinedRevision]]:
    """Split `frame` into rows safe to write and rows to quarantine.

    Returns the frame itself (not a copy) when nothing is held back, which
    is the overwhelmingly common case and the one that must stay cheap.

    A row is held only when all of these are true: the table is registered,
    the key is already stored, the stored value is a non-zero number, the
    incoming value is a number, and they disagree by more than the
    threshold. Anything else writes — including a first observation, a
    stored zero (a ratio against which is not a measurement) and a stored
    NULL. Erring toward writing is the point: a guard that swallowed
    legitimate rows would be a silent failure of its own.
    """
    registered = GUARDED_TABLES.get(table)
    if registered is None or frame.empty:
        return frame, []
    value_column, guarded_keys = registered
    if value_column not in frame.columns:
        return frame, []
    if tuple(key_cols) != guarded_keys or any(c not in frame.columns for c in guarded_keys):
        # The caller keys this table differently from the registry. Screening
        # on a key that is not the row's identity would compare unrelated
        # observations, so decline rather than guess.
        logger.warning(
            "divergence screen skipped for %s: caller keys on %s, registry on %s",
            table, list(key_cols), list(guarded_keys),
        )
        return frame, []

    threshold = float(SAME_PK_DIVERGENCE_QUARANTINE_THRESHOLD)
    stored = _stored_values(conn, table, value_column, guarded_keys, frame)
    if not stored:
        return frame, []

    detected_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    held: list[QuarantinedRevision] = []
    held_positions: list[int] = []

    # to_dict rather than itertuples: itertuples silently renames any column
    # that is not a Python identifier, and a screen that quietly stopped
    # finding its value column would be the failure mode this guard is for.
    for position, row in enumerate(frame.to_dict(orient="records")):
        incoming = _as_float(row.get(value_column))
        if incoming is None:
            continue
        row_key = _key_of(tuple(row.get(c) for c in guarded_keys))
        previous = stored.get(row_key)
        if previous is None or previous == 0:
            continue
        gap = abs(incoming - previous) / abs(previous)
        if gap <= threshold:
            continue
        held.append(
            QuarantinedRevision(
                table_name=table,
                row_key=row_key,
                value_column=value_column,
                stored_value=previous,
                incoming_value=incoming,
                divergence=gap,
                threshold=threshold,
                row_json=json.dumps(_jsonable(row), default=str),
                label=label,
                detected_at=detected_at,
            )
        )
        held_positions.append(position)

    if not held:
        return frame, []

    _log(table, label, held)
    # Positional, not by index label: a frame carrying duplicate index labels
    # would drop every row sharing a held row's label, silently discarding
    # good observations — the opposite of what this function is for.
    dropped = set(held_positions)
    accepted = frame.iloc[[i for i in range(len(frame)) if i not in dropped]]
    return accepted, held


def _log(table: str, label: str, held: list[QuarantinedRevision]) -> None:
    """Flag the quarantine loudly — this is the alert, not a debug aid."""
    for revision in held[:_MAX_LOGGED_ROWS]:
        logger.error(
            "QUARANTINE [%s] %s key=%s: %s=%s would overwrite stored %s "
            "(%.1f%% divergence, threshold %.0f%%) — the stored value stands "
            "and the incoming row is held",
            label, table, revision.row_key, revision.value_column,
            revision.incoming_value, revision.stored_value,
            revision.divergence * 100, revision.threshold * 100,
        )
    if len(held) > _MAX_LOGGED_ROWS:
        logger.error(
            "QUARANTINE [%s] %s: %d rows held in total (%d shown) — a fetch this "
            "wrong across the board is an upstream fault, not a revision",
            label, table, len(held), _MAX_LOGGED_ROWS,
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
            divergence, threshold, row_json, label, detected_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [tuple(revision) for revision in held],
    )
    return len(held)
