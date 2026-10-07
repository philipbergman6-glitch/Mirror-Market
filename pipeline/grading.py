"""The success-state vocabulary — one axis, seven words (A3 #300, built by B3 #309).

``data_freshness.status`` is the only grading axis a layer has. Completeness
is *not* a second axis: ``keys_returned / keys_expected`` record the measure
and ``status`` carries the verdict. The words, in gate order:

======================  =====================================================
``failed``              transport / parse / shape failure, or all-empty on a
                        layer that can never legitimately be empty
``no_publication``      the source ran and had nothing to say (a quiet day)
``incomplete``          non-empty but below the ``LAYER_MIN_KEYS`` floor —
                        an outage; recency is not judged
``stale``               above the floor but the newest observation is past
                        the layer's ``LAYER_MAX_DATA_AGE_DAYS`` budget — even
                        when the fetch was also partial (invariant 11: a
                        wrong-age number is worse than a gap)
``usable_partial``      above the floor, inside the budget, but short of the
                        full catalog — renderable and fresh; degraded, not
                        broken
``success``             every catalog key returned and the payload is inside
                        its age budget
``disabled``            intentionally switched off; neither fresh nor an outage
======================  =====================================================

``usable_partial`` **advances** ``last_success``: freezing the clock over one
missing key of 24 would poison every downstream staleness surface with a
false outage. The degradation is carried by the status word and the
coverage columns instead, and it escalates to a *catalog-drift* alert only
after ``config.USABLE_PARTIAL_ESCALATION_RUNS`` consecutive runs — the fix
for a key that stays missing is a catalog edit (a delisted contract, a
renamed region), so amber always means *transient*.

The streak that drives that escalation lives in ``layer_partial_streak``,
written by ``pipeline.store.save_freshness`` on every freshness write — the
one choke point every verdict passes through — so a streak can never
survive a run that graded anything other than ``usable_partial``. It
round-trips through ``data/history/`` like ``data_freshness`` does, because
"three consecutive runs" is unmeasurable on a runner that forgets.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

#: Every word ``data_freshness.status`` may carry. Order is documentation
#: only; the gate order is in ``main._finalize_layer``.
FRESHNESS_STATUSES: tuple[str, ...] = (
    "success", "usable_partial", "incomplete", "stale", "failed",
    "no_publication", "disabled",
)

#: The states that stamp a fresh ``last_success`` (A3 §3).
ADVANCES_LAST_SUCCESS: frozenset[str] = frozenset({"success", "usable_partial"})

#: The states a surface warns on as *our* outage. ``usable_partial`` is
#: deliberately absent: it is a note, never a warning (A3 §5/§6).
OUTAGE_STATUSES: frozenset[str] = frozenset({"failed", "stale", "incomplete"})

STATUS_USABLE_PARTIAL = "usable_partial"


def missing_catalog_keys(catalog: Mapping[str, object], data: Mapping[str, object]) -> list[str]:
    """Catalog keys the payload did not answer for: absent, ``None`` or empty.

    Per-key fetchers insert into their results dict only after a successful
    fetch, so an absent key is a key that never answered; an empty frame is a
    key that answered with nothing. Both are "missing" for coverage.

    The *verdict* is by count (``keys_returned < keys_expected``, #182's
    denominator); this names the shortfall. Every production fetcher keys its
    results by the catalog's own names, so the names are exact. A payload
    keyed some other way cannot be named key-by-key, and the list then
    carries one labelled line saying so rather than a guess — withheld with a
    reason, never padded (invariant 2).
    """
    answered = sum(
        1 for frame in data.values()
        if frame is not None and not getattr(frame, "empty", False)
    )
    short = len(catalog) - answered
    extra = [k for k in data if k not in catalog]
    if extra:
        logger.warning(
            "payload carries %d key(s) the catalog does not list (%s) — the "
            "shortfall cannot be named key-by-key",
            len(extra), ", ".join(map(str, extra[:5])),
        )
        if short <= 0:
            return []
        return [f"{short} key(s) unnamed: payload keys are not catalog names"]
    return [
        str(key) for key in catalog
        if data.get(key) is None or getattr(data.get(key), "empty", False)
    ]


@dataclass(frozen=True)
class PartialStreak:
    """How many consecutive runs a layer has graded ``usable_partial``."""

    layer: str
    consecutive_runs: int
    missing_keys: list[str]
    first_seen: str
    last_seen: str

    def escalates(self, threshold: int) -> bool:
        return self.consecutive_runs >= threshold


def update_partial_streak(
    conn: sqlite3.Connection,
    layer: str,
    status: str,
    missing_keys: Iterable[str] | None,
    now: str | None = None,
) -> int:
    """Advance or clear ``layer``'s streak for this run; return the new length.

    Called from ``save_freshness`` inside its own transaction. A
    ``usable_partial`` write increments (or starts) the streak and records
    the keys missing *this* run; any other status deletes the row, so the
    count can only ever describe an unbroken run of partials.
    """
    if status != STATUS_USABLE_PARTIAL:
        conn.execute("DELETE FROM layer_partial_streak WHERE layer_name = ?", (layer,))
        return 0
    keys = sorted(str(k) for k in (missing_keys or ()))
    if not keys:
        raise ValueError(
            f"{layer}: status='usable_partial' needs the missing keys it is partial on"
        )
    stamp = now or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    row = conn.execute(
        "SELECT consecutive_runs, first_seen FROM layer_partial_streak WHERE layer_name = ?",
        (layer,),
    ).fetchone()
    if row:
        runs = int(row[0]) + 1
        first_seen = row[1]
    else:
        runs = 1
        first_seen = stamp
    conn.execute(
        """INSERT OR REPLACE INTO layer_partial_streak
           (layer_name, consecutive_runs, missing_keys, first_seen, last_seen)
           VALUES (?, ?, ?, ?, ?)""",
        (layer, runs, json.dumps(keys), first_seen, stamp),
    )
    return runs


def read_partial_streaks(conn: sqlite3.Connection | None = None) -> dict[str, PartialStreak]:
    """Every live streak, keyed by layer. Empty when no layer is partial."""
    if conn is None:
        # Through pipeline.store so the one backend the store is wired to —
        # and the one a test redirects — is the one read here.
        from pipeline import store
        from pipeline.connection import managed_connection

        with managed_connection(store.get_connection()) as owned:
            return read_partial_streaks(owned)
    try:
        rows = conn.execute(
            "SELECT layer_name, consecutive_runs, missing_keys, first_seen, last_seen "
            "FROM layer_partial_streak ORDER BY layer_name"
        ).fetchall()
    except sqlite3.OperationalError as exc:
        # A database from before this table existed has no streaks to report;
        # anything else is a real fault and must surface.
        if "no such table" not in str(exc):
            raise
        return {}
    out: dict[str, PartialStreak] = {}
    for layer, runs, keys, first_seen, last_seen in rows:
        decoded = json.loads(keys) if keys else []
        if not isinstance(decoded, list):
            raise ValueError(f"{layer}: layer_partial_streak.missing_keys is not a list: {keys!r}")
        out[layer] = PartialStreak(
            layer=layer, consecutive_runs=int(runs), missing_keys=[str(k) for k in decoded],
            first_seen=str(first_seen), last_seen=str(last_seen),
        )
    return out


def catalog_drift_layers(
    conn: sqlite3.Connection | None = None, threshold: int | None = None,
) -> dict[str, PartialStreak]:
    """Streaks at or past the escalation threshold — the catalog-drift set."""
    if threshold is None:
        import config

        threshold = config.USABLE_PARTIAL_ESCALATION_RUNS
    return {
        layer: streak for layer, streak in read_partial_streaks(conn).items()
        if streak.escalates(threshold)
    }


def is_outage(status: object) -> bool:
    """True for the states a surface should warn on as our own outage."""
    return isinstance(status, str) and status in OUTAGE_STATUSES
