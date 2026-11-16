"""Push or restore the durable database seed in Cloudflare R2.

    python scripts/db_snapshot.py pull                # before main.py
    python scripts/db_snapshot.py push --run-id ID --built-at ISO8601

The logic and its states live in ``pipeline.db_snapshot``; this wrapper only
turns them into exit codes and GitHub annotations, so a run page says which
seed it started from without anyone opening the log.

Exit codes: 0 — done, or deliberately nothing to do (unconfigured / no seed
yet), each annotated; 1 — a configured operation failed, nothing placed.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config  # noqa: E402 — needs the project root on sys.path
from pipeline import db_snapshot  # noqa: E402

logger = logging.getLogger("db_snapshot")


def _annotate(level: str, title: str, message: str) -> None:
    print(f"::{level} title={title}::{message}", flush=True)


def _pull(db_path: Path) -> int:
    cfg = db_snapshot.config_from_env()
    if cfg is None:
        _annotate(
            "warning",
            "DB seed unconfigured",
            "R2 secrets not set — this run rebuilds from data/history CSVs and re-fetches every layer.",
        )
        return 0
    result = db_snapshot.pull(db_path, db_snapshot.R2Store(cfg))
    if result.state == "no_seed":
        _annotate(
            "notice",
            "No DB seed yet",
            f"Nothing at {db_snapshot.LATEST_KEY} — first seeded run; rebuilding from CSVs.",
        )
        return 0
    meta = result.metadata
    _annotate(
        "notice",
        "DB seed restored",
        f"Started from run {meta.get(db_snapshot.META_RUN_ID, '?')} built "
        f"{meta.get(db_snapshot.META_BUILT_AT, '?')}; "
        f"{len(result.cleared_tables)} history tables left to the committed CSVs.",
    )
    return 0


def _push(db_path: Path, run_id: str, built_at: str) -> int:
    cfg = db_snapshot.config_from_env()
    if cfg is None:
        _annotate(
            "warning",
            "DB snapshot unconfigured",
            "R2 secrets not set — this verified database was not saved durably.",
        )
        return 0
    try:
        when = datetime.fromisoformat(built_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise db_snapshot.SnapshotError(f"--built-at is not ISO 8601: {built_at!r}") from exc
    result = db_snapshot.push(db_path, db_snapshot.R2Store(cfg), run_id=run_id, built_at=when)
    _annotate(
        "notice",
        "DB snapshot saved",
        f"{', '.join(result.keys)} ({result.gz_bytes:,} bytes gz)",
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--db", type=Path, default=Path(config.DB_PATH))
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("pull", help="restore the latest verified seed before the pipeline")
    push = sub.add_parser("push", help="save this run's verified database")
    push.add_argument("--run-id", required=True)
    push.add_argument("--built-at", required=True, help="ISO 8601, e.g. $PIPELINE_STARTED_AT")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    try:
        if args.command == "pull":
            return _pull(args.db)
        return _push(args.db, args.run_id, args.built_at)
    except Exception as exc:  # every failure is one loud line, then exit 1
        title = "DB seed restore failed" if args.command == "pull" else "DB snapshot push failed"
        _annotate("error", title, f"{type(exc).__name__}: {exc}")
        logger.exception(title)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
