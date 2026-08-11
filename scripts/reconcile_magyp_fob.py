"""Reconcile the v1 MAGyP FOB fetcher against trusted ingestion.

This is the trusted path's first non-test consumer. It reads nothing and
writes nothing that the dashboard depends on: it fetches one published
circular, runs it through both the v1 parser and trusted ingestion, and
reports every row and field on which the two disagree.

The point is not the report on any single day. It is that ``trust/`` runs
daily against live data, so drift between it and the v1 pipeline surfaces
the morning it appears instead of months later.

Usage:
    python scripts/reconcile_magyp_fob.py
    python scripts/reconcile_magyp_fob.py --day 2026-08-11 --output report.json

Exit codes:
    0  reconciled, or no circular published in the lookback window
    1  the two paths disagree
    2  the circular could not be fetched or parsed
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import MAGYP_FOB_LOOKBACK_DAYS  # noqa: E402
from fetchers.magyp_fob import _parse_posts  # noqa: E402
from pipeline.results import ScraperShapeError  # noqa: E402
from trust.magyp_fob import (  # noqa: E402
    MagypArtifactReplay,
    ReconciliationReport,
    dual_write_magyp_fob_replay,
    fetch_magyp_fob_artifact,
    posts_from_replay,
)
from trust.repository import TemporaryDirectoryTrustRepository  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "reconciliation" / "magyp_fob.json"

STATUS_RECONCILED = "reconciled"
STATUS_DIVERGED = "diverged"
STATUS_NO_CIRCULAR = "no_circular"

EXIT_OK = 0
EXIT_DIVERGED = 1
EXIT_UNAVAILABLE = 2


@dataclass(frozen=True)
class ResolvedCircular:
    """One published circular, fetched once and shared by both parsers."""

    day: date
    replay: MagypArtifactReplay
    posts: list[dict[str, Any]]


@dataclass(frozen=True)
class ReconciliationRun:
    """What both paths made of one circular, and where they disagreed."""

    status: str
    resolved_day: date | None
    content_hash: str | None
    legacy_rows: int
    trusted_rows: int
    report: ReconciliationReport | None
    checked_at: datetime

    @property
    def exit_code(self) -> int:
        if self.status == STATUS_DIVERGED:
            return EXIT_DIVERGED
        return EXIT_OK

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "status": self.status,
            "resolved_day": self.resolved_day.isoformat() if self.resolved_day else None,
            "content_hash": self.content_hash,
            "legacy_rows": self.legacy_rows,
            "trusted_rows": self.trusted_rows,
            "checked_at": self.checked_at.isoformat(),
        }
        if self.report is not None:
            payload["reconciliation"] = {
                "reconciled": self.report.reconciled,
                "matched_rows": self.report.matched_rows,
                "missing_in_trusted": [dict(row) for row in self.report.missing_in_trusted],
                "missing_in_legacy": [dict(row) for row in self.report.missing_in_legacy],
                "field_differences": [dict(diff) for diff in self.report.field_differences],
            }
        return payload


def resolve_published_circular(
    *,
    today: date,
    lookback_days: int = MAGYP_FOB_LOOKBACK_DAYS,
    fetch_artifact=None,
) -> ResolvedCircular | None:
    """Walk back to the most recent published circular, fetching it once.

    Both paths must see the *same* circular. The v1 fetcher resolves its own
    publication day by walking back over weekends and holidays, and
    ``fetch_magyp_fob_artifact`` takes an explicit day; if each side resolved
    independently a divergence could mean "different circular" rather than
    "different parse", which would make the report worthless. So the day is
    resolved once here and the fetched bytes are handed to both parsers.
    """
    fetch_artifact = fetch_artifact or fetch_magyp_fob_artifact
    for offset in range(lookback_days + 1):
        day = date.fromordinal(today.toordinal() - offset)
        if day.weekday() >= 5:  # Sat/Sun never publish
            continue
        replay = fetch_artifact(day)
        posts = posts_from_replay(replay)
        if not posts:
            log.info("MAGyP FOB: no circular for %s (holiday?)", day)
            continue
        return ResolvedCircular(day=day, replay=replay, posts=posts)
    return None


def reconcile_once(
    *,
    day: date | None = None,
    today: date | None = None,
    repository_root: str | Path | None = None,
    fetch_artifact=None,
    now: datetime | None = None,
) -> ReconciliationRun:
    """Run one circular through both paths and account for every difference."""
    checked_at = now or datetime.now(timezone.utc)
    # Resolved late, not as a default argument: a default binds the real
    # network fetcher at import time, which silently defeats monkeypatching
    # and lets a test reach MAGyP for real.
    fetch_artifact = fetch_artifact or fetch_magyp_fob_artifact

    if day is not None:
        replay = fetch_artifact(day)
        posts = posts_from_replay(replay)
        circular = ResolvedCircular(day=day, replay=replay, posts=posts) if posts else None
    else:
        circular = resolve_published_circular(
            today=today or date.today(), fetch_artifact=fetch_artifact
        )

    if circular is None:
        # A holiday is not a divergence. Grading an unpublished day as a
        # failure would make every Argentine holiday look like a defect and
        # train the owner to ignore the job.
        log.info("MAGyP FOB: no circular published in the lookback window")
        return ReconciliationRun(
            status=STATUS_NO_CIRCULAR,
            resolved_day=None,
            content_hash=None,
            legacy_rows=0,
            trusted_rows=0,
            report=None,
            checked_at=checked_at,
        )

    # The v1 half of the dual write: exactly what fetch_magyp_fob() does to
    # the same bytes once its own day walk has landed on this circular.
    legacy_frame = _parse_posts(circular.posts)

    with _repository_dir(repository_root) as root:
        repository = TemporaryDirectoryTrustRepository(root)
        result = dual_write_magyp_fob_replay(repository, circular.replay, legacy_frame)

    report = result.reconciliation
    trusted_rows = report.matched_rows + len(report.missing_in_legacy)
    status = STATUS_RECONCILED if report.reconciled else STATUS_DIVERGED
    return ReconciliationRun(
        status=status,
        resolved_day=circular.day,
        content_hash=circular.replay.artifact.reference.content_hash,
        legacy_rows=len(legacy_frame),
        trusted_rows=trusted_rows,
        report=report,
        checked_at=checked_at,
    )


class _repository_dir:
    """Yield the durable root, using a scratch directory when none is given."""

    def __init__(self, root: str | Path | None) -> None:
        self._root = root
        self._tmp: tempfile.TemporaryDirectory | None = None

    def __enter__(self) -> Path:
        if self._root is not None:
            path = Path(self._root)
            path.mkdir(parents=True, exist_ok=True)
            return path
        self._tmp = tempfile.TemporaryDirectory(prefix="magyp-reconcile-")
        return Path(self._tmp.name)

    def __exit__(self, *exc_info: object) -> None:
        if self._tmp is not None:
            self._tmp.cleanup()


def write_report(run: ReconciliationRun, output: str | Path) -> Path:
    """Persist the report as JSON so CI can attach it to the run."""
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(run.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _log_summary(run: ReconciliationRun) -> None:
    if run.status == STATUS_NO_CIRCULAR:
        return
    report = run.report
    assert report is not None  # every other status carries one
    if report.reconciled:
        log.info(
            "MAGyP FOB %s: reconciled — %d rows matched on both paths",
            run.resolved_day, report.matched_rows,
        )
        return
    log.error(
        "MAGyP FOB %s: DIVERGED — %d matched, %d only in v1, %d only in trusted, "
        "%d field differences",
        run.resolved_day,
        report.matched_rows,
        len(report.missing_in_trusted),
        len(report.missing_in_legacy),
        len(report.field_differences),
    )
    for diff in report.field_differences[:10]:
        log.error("  %s %s: v1=%r trusted=%r", diff["key"], diff["field"], diff["legacy"], diff["trusted"])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--day", type=date.fromisoformat, default=None,
                        help="reconcile this circular date (default: most recent published)")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help=f"where to write the JSON report (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--repository", type=Path, default=None,
                        help="durable trust repository root (default: a scratch directory)")
    args = parser.parse_args(argv)

    try:
        run = reconcile_once(day=args.day, repository_root=args.repository)
    except (requests.RequestException, ScraperShapeError) as exc:
        # Upstream being down is not evidence about the trusted path, so it
        # gets its own exit code rather than reading as a divergence.
        log.error("MAGyP FOB: reconciliation could not run — %s", exc)
        return EXIT_UNAVAILABLE

    _log_summary(run)
    path = write_report(run, args.output)
    log.info("MAGyP FOB: report written to %s", path)
    return run.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
