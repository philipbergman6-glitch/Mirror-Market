"""Reconcile the v1 forward-curve fetcher against trusted ingestion (DT-16).

The second non-test consumer of ``trust/``, and the one that gates the CBOT
named-contract read-path switch. It fetches each soy leg's contract months
*once*, runs those same bars through both the v1 parser
(``fetchers.forward_curve.fetch_forward_curve``) and trusted ingestion, and
reports every row and field on which the two disagree.

Fetching once is the whole design. Yahoo answers each contract with its own
last bar and both paths would otherwise resolve their own session; a
divergence could then mean "different download" rather than "different
parse", which makes the report worthless as cutover evidence. So a memoising
downloader is installed for the run and both paths draw from it.

Usage:
    python scripts/reconcile_cbot_benchmarks.py
    python scripts/reconcile_cbot_benchmarks.py --commodity Soybeans --output report.json

Exit codes:
    0  reconciled, or no session published for any leg
    1  the two paths disagree
    2  the provider could not be reached, or a payload could not be parsed
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

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import fetchers.forward_curve as forward_curve  # noqa: E402
from trust.cbot_benchmarks import (  # noqa: E402
    BENCHMARK_COMMODITIES,
    BenchmarkArtifactReplay,
    bars_from_replay,
    dual_write_cbot_benchmarks,
    fetch_cbot_benchmark_artifact,
)
from trust.read_path import CBOT_BENCHMARK_DATASET_KEYS  # noqa: E402
from trust.reconciliation import ReconciliationReport  # noqa: E402
from trust.repository import TemporaryDirectoryTrustRepository  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "reconciliation" / "cbot_benchmarks.json"

# The default run is the whole soy complex, taken from the adapter's own
# registered set rather than restated here: the complex moves together or not
# at all, and a crush struck from a trusted bean and a v1 oil would be two
# provenances in one number.

STATUS_RECONCILED = "reconciled"
STATUS_DIVERGED = "diverged"
STATUS_NO_SESSION = "no_session"

EXIT_OK = 0
EXIT_DIVERGED = 1
EXIT_UNAVAILABLE = 2


class _MemoisingDownloader:
    """One download per ticker, shared by both parsers.

    Not a performance optimisation. Both paths must read the *same* bars or a
    reported difference cannot be attributed to the parse, and the settlement
    guard means a run straddling the cutoff would otherwise hand one path a
    session the other never saw.
    """

    def __init__(self, download=None) -> None:
        self._download = download or forward_curve.fetch_one
        self._frames: dict[str, pd.DataFrame] = {}

    def __call__(self, ticker: str, period: str = "5d") -> pd.DataFrame:
        if ticker not in self._frames:
            self._frames[ticker] = self._download(ticker, period=period)
        return self._frames[ticker]

    @property
    def ticker_count(self) -> int:
        return len(self._frames)


@dataclass(frozen=True)
class CommodityReconciliation:
    """What both paths made of one commodity's curve."""

    commodity: str
    content_hash: str
    legacy_rows: int
    trusted_rows: int
    report: ReconciliationReport

    def to_dict(self) -> dict[str, Any]:
        return {
            "commodity": self.commodity,
            "content_hash": self.content_hash,
            "legacy_rows": self.legacy_rows,
            "trusted_rows": self.trusted_rows,
            "reconciliation": self.report.to_dict(),
        }


@dataclass(frozen=True)
class ReconciliationRun:
    """One reconciliation cycle across every requested benchmark leg."""

    status: str
    run_id: str | None
    commodities: tuple[CommodityReconciliation, ...]
    quarantined_revision_ids: tuple[str, ...]
    checked_at: datetime
    dataset_keys: tuple[str, ...] = CBOT_BENCHMARK_DATASET_KEYS

    @property
    def exit_code(self) -> int:
        return EXIT_DIVERGED if self.status == STATUS_DIVERGED else EXIT_OK

    @property
    def reconciled(self) -> bool:
        return all(item.report.reconciled for item in self.commodities)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "run_id": self.run_id,
            "checked_at": self.checked_at.isoformat(),
            "dataset_keys": list(self.dataset_keys),
            # Reported, never graded: a quarantined leg is the ledger doing its
            # job, and v1 has no such state, so it is not a divergence. It is
            # here because a cutover should not be enabled on a day the ledger
            # is holding legs back without the operator knowing.
            "quarantined_revision_ids": list(self.quarantined_revision_ids),
            "commodities": [item.to_dict() for item in self.commodities],
        }


def reconcile_once(
    *,
    commodities: tuple[str, ...] = BENCHMARK_COMMODITIES,
    today: date | None = None,
    repository_root: str | Path | None = None,
    download=None,
    now: datetime | None = None,
) -> ReconciliationRun:
    """Run each commodity's curve through both paths and account for every difference."""

    checked_at = now or datetime.now(timezone.utc)
    downloader = _MemoisingDownloader(download)

    replays: list[BenchmarkArtifactReplay] = []
    legacy_frames: dict[str, pd.DataFrame] = {}
    for commodity in commodities:
        replay = fetch_cbot_benchmark_artifact(commodity, today=today, download=downloader)
        replays.append(replay)
        legacy_frames[commodity] = _legacy_frame(commodity, downloader, today=today)

    if not any(bars_from_replay(replay) for replay in replays):
        # Every leg empty means the provider published no session — a weekend,
        # a holiday, or a run before the settlement guard's cutoff. Grading
        # that as a failure would fire the job every Saturday.
        log.info("CBOT benchmarks: no session published for any leg")
        return ReconciliationRun(
            status=STATUS_NO_SESSION,
            run_id=None,
            commodities=(),
            quarantined_revision_ids=(),
            checked_at=checked_at,
        )

    with _repository_dir(repository_root) as root:
        repository = TemporaryDirectoryTrustRepository(root)
        result = dual_write_cbot_benchmarks(repository, replays, legacy_frames)
        trusted_counts = {
            item.commodity: len(item.accepted_revision_ids) for item in result.ingestion.datasets
        }

    items = tuple(
        CommodityReconciliation(
            commodity=replay.commodity,
            content_hash=replay.artifact.reference.content_hash,
            legacy_rows=len(legacy_frames[replay.commodity]),
            trusted_rows=trusted_counts.get(replay.commodity, 0),
            report=result.reconciliations[replay.commodity],
        )
        for replay in replays
    )
    status = STATUS_RECONCILED if result.reconciled else STATUS_DIVERGED
    return ReconciliationRun(
        status=status,
        run_id=result.ingestion.run_id,
        commodities=items,
        quarantined_revision_ids=result.ingestion.quarantined_revision_ids,
        checked_at=checked_at,
    )


def _legacy_frame(commodity: str, download, *, today: date | None) -> pd.DataFrame:
    """The v1 parse of the same bars, with its own downloader swapped out.

    ``fetch_forward_curve`` imports ``fetch_one`` into its own namespace at
    import time, so the substitution has to happen on that name. It is
    restored unconditionally: leaving a memoising downloader installed would
    freeze the module's view of the market for the rest of the process.
    """

    original = forward_curve.fetch_one
    forward_curve.fetch_one = download
    try:
        return forward_curve.fetch_forward_curve(commodity, today=today)
    finally:
        forward_curve.fetch_one = original


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
        self._tmp = tempfile.TemporaryDirectory(prefix="cbot-reconcile-")
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
    if run.status == STATUS_NO_SESSION:
        return
    for item in run.commodities:
        report = item.report
        if report.reconciled:
            log.info(
                "CBOT %s: reconciled — %d legs matched on both paths",
                item.commodity, report.matched_rows,
            )
            continue
        log.error(
            "CBOT %s: DIVERGED — %d matched, %d only in v1, %d only in trusted, %d field differences",
            item.commodity,
            report.matched_rows,
            len(report.missing_in_trusted),
            len(report.missing_in_legacy),
            len(report.field_differences),
        )
        for diff in report.field_differences[:10]:
            log.error("  %s %s: v1=%r trusted=%r", diff["key"], diff["field"], diff["legacy"], diff["trusted"])
    if run.quarantined_revision_ids:
        log.warning(
            "CBOT benchmarks: %d leg(s) quarantined by the ledger this run — "
            "reconciliation is not cutover evidence while legs are being held back",
            len(run.quarantined_revision_ids),
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--commodity", action="append", dest="commodities", default=None,
                        choices=list(BENCHMARK_COMMODITIES),
                        help="reconcile only this leg (repeatable; default: the whole soy complex)")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help=f"where to write the JSON report (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--repository", type=Path, default=None,
                        help="durable trust repository root (default: a scratch directory)")
    args = parser.parse_args(argv)

    commodities = tuple(args.commodities) if args.commodities else BENCHMARK_COMMODITIES
    try:
        run = reconcile_once(commodities=commodities, repository_root=args.repository)
    except (requests.RequestException, ValueError) as exc:
        # ValueError covers BenchmarkShapeError, which subclasses it: a payload
        # whose shape moved is an upstream fact, not a parse disagreement.
        # Upstream being down is not evidence about the trusted path, so it
        # gets its own exit code rather than reading as a divergence.
        log.error("CBOT benchmarks: reconciliation could not run — %s", exc)
        return EXIT_UNAVAILABLE

    _log_summary(run)
    path = write_report(run, args.output)
    log.info("CBOT benchmarks: report written to %s", path)
    return run.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
