"""Run-level ledger of quarantine events, and the seams that surface them.

The verdicts themselves are made at the write, in `pipeline.divergence`
(which rule held a row, which later fetch released one). This module is
the process-wide tally of those verdicts for one pipeline run, and the
three places they are shown:

- the run summary `main.py` writes to `pipeline_status.json` (`summary`),
- the CI outage issue `scripts/ci_layer_alert.py` opens (`alert_lines`),
- the briefing's signals block, as a `warning` (`briefing_signals`).

A2 (#299) put the plumbing here on purpose: a fetch contradicting stored
data is exactly the "can I trust this number" question a buyer has, and
until B5 (#311) a quarantine surfaced only as an ERROR line that died with
the CI runner. Rejected partial bars are deliberately *not* here — they
are log-only (`pipeline.clean._drop_partial_bars`), because holiday
half-bars are routine and an alert that fires on routine trains its
readers to ignore it.

**Run identity.** `run_id` is one token per pipeline process. The release
rule needs it: a held value is released by a *later, independent* fetch,
and the only independence the store can see is "a different run than the
one that held it". A replay inside the same run — the same corrupted
frame saved twice — is one observation, not two.

**What a signal says.** The table, the series, the date, the rule and the
size of the disagreement. Never the held value as if it were a price: a
quarantined number is a number the store refused to believe, and printing
it in a briefing would publish exactly what the guard keeps out.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from typing import Any

import config

logger = logging.getLogger(__name__)

# How many events the run summary carries individually. A wholesale-corrupt
# fetch is one legible alert plus a count, not a thousand-line JSON blob.
MAX_SUMMARY_EVENTS = 50

_run_id: str = uuid.uuid4().hex
_events: list[dict[str, Any]] = []


def current_run_id() -> str:
    """The token identifying this pipeline process's fetches."""
    return _run_id


def reset() -> None:
    """Start a new run: new identity, empty ledger.

    `main.run()` calls this beside its other per-run clears. Tests call it
    to stage "tomorrow's run" against the same database.
    """
    global _run_id
    _run_id = uuid.uuid4().hex
    _events.clear()


def note(
    *,
    table: str,
    label: str,
    kind: str,
    series: str,
    date: str,
    divergence: float,
    released: bool,
) -> None:
    """Record one verdict for the run summary. Called by `pipeline.divergence`."""
    _events.append(
        {
            "table": table,
            "label": label,
            "kind": kind,
            "series": series,
            "date": date,
            "divergence": float(divergence),
            "released": bool(released),
        }
    )


def summary() -> dict[str, Any]:
    """This run's tally, in the shape written to `pipeline_status.json`.

    `held` counts rows the store refused this run; `released` counts rows a
    previous run held that this run's independent fetch confirmed and
    stored. Both are zero on a clean run, and a clean run is the norm.
    """
    by_table: dict[str, dict[str, int]] = {}
    for event in _events:
        bucket = by_table.setdefault(event["table"], {"held": 0, "released": 0})
        bucket["released" if event["released"] else "held"] += 1
    return {
        "run_id": _run_id,
        "held": sum(1 for e in _events if not e["released"]),
        "released": sum(1 for e in _events if e["released"]),
        "by_table": by_table,
        "events": [dict(e) for e in _events[:MAX_SUMMARY_EVENTS]],
        "events_truncated": max(0, len(_events) - MAX_SUMMARY_EVENTS),
    }


def describe(run_summary: dict[str, Any]) -> str:
    """One log line for the end-of-run summary."""
    parts = [f"{t}: {c['held']} held, {c['released']} released"
             for t, c in sorted(run_summary.get("by_table", {}).items())]
    return (
        f"Quarantine: {run_summary.get('held', 0)} row(s) held, "
        f"{run_summary.get('released', 0)} released this run"
        + (f" ({'; '.join(parts)})" if parts else "")
    )


# ---------------------------------------------------------------------------
# Read side — the briefing and the CI alert run after main.py, in their own
# processes, and read the summary main.py wrote rather than recompute it.
# ---------------------------------------------------------------------------


def read_run_summary() -> dict[str, Any] | None:
    """The last pipeline run's quarantine block, or None.

    None is a legal state, not an error: a briefing generated without a
    pipeline run in front of it (the local dev loop) has no run to report,
    and a status file written before B5 has no quarantine block.
    """
    path = os.path.join(config.STORAGE_DIR, "pipeline_status.json")
    try:
        with open(path, encoding="utf-8") as fh:
            status = json.load(fh)
    except (OSError, ValueError):
        return None
    block = status.get("quarantine") if isinstance(status, dict) else None
    return block if isinstance(block, dict) else None


_KIND_TEXT = {
    "same_pk_revision": "re-served a different value for an already-stored session",
    "daily_move": "moved day-over-day against the latest accepted session",
}


def _what_happened(event: dict[str, Any]) -> str:
    kind = _KIND_TEXT.get(str(event.get("kind")), str(event.get("kind")))
    pct = float(event.get("divergence") or 0.0) * 100
    return f"{kind} by {pct:.0f}%"


def briefing_signals(run_summary: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Quarantine events as briefing signals — a `warning` per held row.

    A release is `info`: the doubt was dropped on evidence, and the reader
    should know the series was once in question, not be alarmed by it.
    """
    if not run_summary:
        return []
    signals: list[dict[str, Any]] = []
    for event in run_summary.get("events", []):
        series = str(event.get("series") or event.get("table") or "")
        date = str(event.get("date") or "")
        where = f"{event.get('table')}/{series} {date}".strip()
        if event.get("released"):
            signals.append({
                "date": date,
                "commodity": series,
                "signal_type": "quarantine_released",
                "severity": "info",
                "description": (
                    f"Quarantine released: {where} — an independent later fetch "
                    f"re-served the held value, which now stores"
                ),
            })
        else:
            signals.append({
                "date": date,
                "commodity": series,
                "signal_type": "quarantine",
                "severity": "warning",
                "description": (
                    f"Quarantine: {where} held — the fetch {_what_happened(event)} "
                    f"(threshold {config.SAME_PK_DIVERGENCE_QUARANTINE_THRESHOLD * 100:.0f}%); "
                    f"the stored value stands and the held row is not published"
                ),
            })
    truncated = int(run_summary.get("events_truncated") or 0)
    if truncated:
        signals.append({
            "date": "",
            "commodity": "",
            "signal_type": "quarantine",
            "severity": "warning",
            "description": (
                f"Quarantine: {truncated} further event(s) not listed — a fetch "
                f"this wrong across the board is an upstream fault, not a revision"
            ),
        })
    return signals


def alert_lines(run_summary: dict[str, Any] | None) -> list[str]:
    """Markdown lines for the CI alert issue. Empty when nothing was held."""
    if not run_summary or not run_summary.get("held"):
        return []
    lines = ["**Quarantined revisions (stored value stands, held row not published):**"]
    for event in run_summary.get("events", []):
        if event.get("released"):
            continue
        lines.append(
            f"- `{event.get('table')}` {event.get('series')} {event.get('date')} — "
            f"{_what_happened(event)}"
        )
    released = int(run_summary.get("released") or 0)
    if released:
        lines.append(f"- {released} previously held row(s) released by this run's independent fetch")
    truncated = int(run_summary.get("events_truncated") or 0)
    if truncated:
        lines.append(f"- … and {truncated} more not listed")
    lines.append("")
    return lines
