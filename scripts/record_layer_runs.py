"""Append this run's rows to the source reliability record (B34 #520).

Runs in the deploy workflow after the publication outcome is known, on every
main run that was not cancelled — including one whose pipeline crashed, which
is the run the old record could not see. Writes:

- ``data/history/layer_runs.csv``: one row per production layer, graded from
  the rows *this* run wrote to ``data_freshness`` (``pipeline.layer_runs``).
- ``data/history/layer_alerts.csv``: the layers this run's ``ci-alert``
  comment named, from the receipt ``scripts/ci_layer_alert.py`` left.

``rendered_at`` is the candidate manifest's ``generated_at`` when the deploy
outcome is ``success`` and blank otherwise. Hard-fails on anything ambiguous
(no run id, no start stamp, a promoted edition without its manifest, another
run's receipt): a guessed row is worse than a missing one, and a missing one
shows up in the record as ``no_run``.

Usage:
    python scripts/record_layer_runs.py --started-at 2026-10-19T20:05:00Z \\
        --manifest "$RUNNER_TEMP/mirror-market-candidate/manifest.json" \\
        --deployment-outcome success
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config  # noqa: E402 — needs the project root on sys.path
from pipeline import history, layer_runs  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

STORAGE = Path(config.STORAGE_DIR)


def _freshness() -> dict[str, dict[str, object]]:
    """This runner's ``data_freshness`` rows, keyed by layer. No table → ``{}``."""
    from pipeline import store
    from pipeline.connection import managed_connection

    with managed_connection(store.get_connection()) as conn:
        try:
            cursor = conn.execute(
                "SELECT layer_name, status, last_attempt, observed_at FROM data_freshness"
            )
        except sqlite3.OperationalError as exc:
            # A pipeline that died before the schema existed graded nothing.
            if "no such table" not in str(exc):
                raise
            return {}
        return {
            str(name): {"status": status, "last_attempt": attempt, "observed_at": observed}
            for name, status, attempt, observed in cursor.fetchall()
        }


def _rendered_at(manifest: Path | None, outcome: str) -> str | None:
    if outcome != "success":
        return None
    if manifest is None or not manifest.exists():
        raise layer_runs.LayerRunsError(
            f"deployment outcome is success but the edition manifest {manifest} is absent"
        )
    generated = json.loads(manifest.read_text(encoding="utf-8"))["edition"]["generated_at"]
    if not generated:
        raise layer_runs.LayerRunsError(f"{manifest}: edition.generated_at is blank")
    return str(generated)


def _receipt_layers(path: Path) -> list[str]:
    if not path.exists():
        return []
    receipt = json.loads(path.read_text(encoding="utf-8"))
    own = os.environ.get("GITHUB_RUN_ID")
    if receipt.get("run_id") != own:
        raise layer_runs.LayerRunsError(
            f"{path} belongs to run {receipt.get('run_id')!r}, not this run {own!r}"
        )
    return list(receipt.get("layers") or [])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--started-at", default=os.environ.get("PIPELINE_STARTED_AT"),
                        help="UTC instant the pipeline step began (default: $PIPELINE_STARTED_AT)")
    parser.add_argument("--status", type=Path, default=STORAGE / "pipeline_status.json")
    parser.add_argument("--receipt", type=Path, default=STORAGE / "alert_receipt.json")
    parser.add_argument("--manifest", type=Path, default=None)
    parser.add_argument("--deployment-outcome", default="",
                        help="success | failure | skipped — only success stamps rendered_at")
    parser.add_argument("--history-dir", type=Path, default=None,
                        help="the data/history/ to append to (default: config.HISTORY_DIR); the "
                             "workflow passes a fresh worktree of main so this run's code records "
                             "onto main's newest record")
    args = parser.parse_args(argv)
    if args.history_dir is not None:
        history.HISTORY_DIR = str(args.history_dir)

    if not args.started_at:
        raise layer_runs.LayerRunsError("--started-at / PIPELINE_STARTED_AT is not set")
    started = datetime.fromisoformat(args.started_at.replace("Z", "+00:00"))
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)

    run_id, workflow = layer_runs.run_identity(os.environ)
    status = json.loads(args.status.read_text(encoding="utf-8")) if args.status.exists() else None
    if status is None:
        logger.warning("no run summary at %s — the pipeline crashed; ungraded layers are not_run", args.status)

    rows = layer_runs.rows_for_run(
        run_id=run_id,
        workflow=workflow,
        run_started_at=started,
        freshness=_freshness(),
        requested=None if status is None else list(status.get("layers_requested") or []),
        configured=layer_runs.layer_configured,
        rendered_at=_rendered_at(args.manifest, args.deployment_outcome),
    )
    written = layer_runs.append_runs(rows)
    alerted = layer_runs.append_alerts(run_id, _receipt_layers(args.receipt))
    tally: dict[str, int] = {}
    for row in rows:
        tally[row.status] = tally.get(row.status, 0) + 1
    logger.info(
        "layer_runs: run %s (%s) session %s — %d rows %s; %d alert receipt row(s)",
        run_id, workflow, rows[0].session_date.isoformat() if rows else "-",
        written, dict(sorted(tally.items())), alerted,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
