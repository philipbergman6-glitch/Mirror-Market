"""Phase 5 — the trader validation trial.

A 30-trading-day shadow trial of the whole product, not of any one feature.
Phases 1-4 each shipped a surface; this phase measures whether a professional
soy trader, working real days, reaches for Mirror Market instead of a terminal —
and whether doing so costs them anything in decision risk.

Nothing in this package invents a participant, a session, a lookup or a
measurement. Every number reported here is computed from a record a human
wrote, and a metric with too few of those says ``insufficient`` rather than
producing a rate that reads like evidence.

Module map, in the order the trial uses them:

    domain.py     the vocabulary — tasks, issue classes, the session record
    release.py    what code and what data produced a result, and re-checking it
    records.py    the private YAML record store (gitignored, never published)
    metrics.py    the eleven metrics, each with its own insufficiency rule
    drills.py     the five failure drills, run against the real grading code
    backlog.py    a validated finding becomes a prioritized backlog item
    review.py     the weekly review, the 30-day scorecard, and the go/no-go
    sanitize.py   the aggregate projection, and the guard that proves it safe
"""

from __future__ import annotations

__all__ = ["domain", "metrics", "records", "release"]
