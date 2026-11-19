"""Push or restore the durable database seed in Cloudflare R2.

    python scripts/db_snapshot.py pull                # before main.py
    python scripts/db_snapshot.py pull --require-fresh  # the quick refresh
    python scripts/db_snapshot.py push --run-id ID --built-at ISO8601

The logic and its states live in ``pipeline.db_snapshot``; this wrapper only
turns them into exit codes and GitHub annotations, so a run page says which
seed it started from without anyone opening the log.

Exit codes: 0 — done, or deliberately nothing to do (unconfigured / no seed
yet / stale seed refused), each annotated; 1 — a configured operation failed,
nothing placed.

Under GitHub Actions, ``pull`` writes ``state=<state>`` to $GITHUB_OUTPUT, and
``history_period=<period>`` only when the restored seed earns the short
window — so a missing output (crash, failed restore) leaves main.py on its
full-history default rather than on a guess.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config  # noqa: E402 — needs the project root on sys.path
from pipeline import db_snapshot  # noqa: E402

logger = logging.getLogger("db_snapshot")


def _annotate(level: str, title: str, message: str) -> None:
    print(f"::{level} title={title}::{message}", flush=True)


def _output(**values: str) -> None:
    """Append step outputs when running under GitHub Actions."""
    path = os.environ.get("GITHUB_OUTPUT")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as fh:
        for key, value in values.items():
            fh.write(f"{key}={value}\n")


def _pull(db_path: Path, require_fresh: bool) -> int:
    cfg = db_snapshot.config_from_env()
    if cfg is None:
        _output(state="unconfigured")
        _annotate(
            "warning",
            "DB seed unconfigured",
            "R2 secrets not set — this run rebuilds from data/history CSVs and re-fetches every layer.",
        )
        return 0
    max_age = config.SEED_SHORT_WINDOW_MAX_AGE_DAYS if require_fresh else None
    result = db_snapshot.pull(db_path, db_snapshot.R2Store(cfg), max_age_days=max_age)
    if result.state == "no_seed":
        _output(state="no_seed")
        _annotate(
            "notice",
            "No DB seed yet",
            f"Nothing at {db_snapshot.LATEST_KEY} — first seeded run; rebuilding from CSVs.",
        )
        return 0
    meta = result.metadata
    if result.state == "stale_seed":
        _output(state="stale_seed")
        _annotate(
            "warning",
            "DB seed too old",
            f"Seed from run {meta.get(db_snapshot.META_RUN_ID, '?')} is {result.age_days:.1f} days old "
            f"(limit {config.SEED_SHORT_WINDOW_MAX_AGE_DAYS}) — not used.",
        )
        return 0
    period, reason = db_snapshot.history_period_for(result)
    _output(state="restored")
    if period == config.FAST_REFRESH_HISTORY_PERIOD:
        _output(history_period=period)
    _annotate(
        "notice",
        "DB seed restored",
        f"Started from run {meta.get(db_snapshot.META_RUN_ID, '?')} built "
        f"{meta.get(db_snapshot.META_BUILT_AT, '?')}; "
        f"{len(result.cleared_tables)} history tables left to the committed CSVs; {reason}.",
    )
    return 0


def _push(db_path: Path, run_id: str, built_at: str, public_manifest: Path | None) -> int:
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
    result = db_snapshot.push(
        db_path, db_snapshot.R2Store(cfg), run_id=run_id, built_at=when,
        public_manifest=public_manifest,
    )
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
    pull = sub.add_parser("pull", help="restore the latest verified seed before the pipeline")
    pull.add_argument(
        "--require-fresh",
        action="store_true",
        help="refuse (do not place) a seed older than config.SEED_SHORT_WINDOW_MAX_AGE_DAYS",
    )
    sub.add_parser("verify", help="run a read-only recovery drill against the current seed and archive")
    recover = sub.add_parser("recover", help="restore an intact dated archive to an unused database path")
    recover.add_argument("--key", required=True)
    push = sub.add_parser("push", help="save this run's verified database")
    push.add_argument("--public-manifest", type=Path, help="manifest from the verified public candidate")
    push.add_argument("--run-id", required=True)
    push.add_argument("--built-at", required=True, help="ISO 8601, e.g. $PIPELINE_STARTED_AT")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    try:
        if args.command == "pull":
            return _pull(args.db, args.require_fresh)
        if args.command == "verify":
            cfg = db_snapshot.config_from_env()
            if cfg is None:
                raise db_snapshot.SnapshotError("R2 credentials are required for a recovery drill")
            archive_key = db_snapshot.verify_latest_archive(db_snapshot.R2Store(cfg))
            _annotate("notice", "R2 recovery verified", archive_key)
            return 0
        if args.command == "recover":
            if not args.key.startswith("daily/"):
                raise db_snapshot.SnapshotError("recover requires a dated daily archive key")
            cfg = db_snapshot.config_from_env()
            if cfg is None:
                raise db_snapshot.SnapshotError("R2 credentials are required for archive recovery")
            result = db_snapshot.pull(
                args.db, db_snapshot.R2Store(cfg), key=args.key, preserve_history=True
            )
            if result.state != "restored":
                raise db_snapshot.SnapshotError(f"archive not found: {args.key}")
            return 0
        return _push(args.db, args.run_id, args.built_at, args.public_manifest)
    except Exception as exc:  # every failure is one loud line, then exit 1
        title = "DB snapshot push failed" if args.command == "push" else "DB seed restore failed"
        _annotate("error", title, f"{type(exc).__name__}: {exc}")
        logger.exception(title)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
