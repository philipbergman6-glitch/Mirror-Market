"""Reconcile the v1 currency fetcher against trusted ingestion (#194, pilot 3).

The third non-test consumer of ``trust/``. It downloads each required pair's
bars *once*, runs those same bars through both the v1 path
(the FX session guard, then
``pipeline.clean.clean_ohlcv`` and the ``currencies`` projection) and trusted
ingestion, and reports every row and field on which the two disagree.

Downloading once is the whole design. Yahoo answers each pair with its own
last bar and both paths would otherwise resolve their own session; a
divergence could then mean "different download" rather than "different parse",
which makes the report worthless as cutover evidence.

The download is deliberately the **unguarded** one. ``download_bars`` returns
the provider frame including the session in progress; the v1 half then applies
its own settlement guard and the trusted half applies its own cutoff, sourced
from the same ``FX_SESSION`` rule. Handing both paths a pre-trimmed frame
would leave the trusted cutoff untested and the pilot green while proving
nothing.

Both paths are pinned to **one session** — the newest one v1 would publish.
Comparing a five-day window would let a pair that printed one extra day read
as a divergence.

Usage:
    python scripts/reconcile_fx.py
    python scripts/reconcile_fx.py --pair BRL/USD --output report.json

Exit codes:
    0  reconciled, or no session published for any pair
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

import fetchers.yfinance as yfinance_fetcher  # noqa: E402
from fetchers._settlement import FX_SESSION, drop_unsettled_session  # noqa: E402
from trust.fx_ingestion import (  # noqa: E402
    DEFAULT_PERIOD,
    FX_DATASET_KEYS,
    FX_PAIRS,
    FxArtifactReplay,
    dual_write_fx,
    fetch_fx_artifact,
    legacy_currency_frame,
    ticker_for,
    trusted_currency_frame,
)
from trust.reconciliation import ReconciliationReport  # noqa: E402
from trust.repository import TemporaryDirectoryTrustRepository  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "reconciliation" / "fx.json"

STATUS_RECONCILED = "reconciled"
STATUS_DIVERGED = "diverged"
STATUS_NO_SESSION = "no_session"

EXIT_OK = 0
EXIT_DIVERGED = 1
EXIT_UNAVAILABLE = 2


class _MemoisingDownloader:
    """One unguarded download per ticker, shared by both parsers."""

    def __init__(self, download=None) -> None:
        self._download = download or yfinance_fetcher.download_bars
        self._frames: dict[str, pd.DataFrame] = {}

    def __call__(self, ticker: str, period: str = DEFAULT_PERIOD) -> pd.DataFrame:
        if ticker not in self._frames:
            self._frames[ticker] = self._download(ticker, period=period)
        return self._frames[ticker]

    @property
    def ticker_count(self) -> int:
        return len(self._frames)


@dataclass(frozen=True)
class PairReconciliation:
    """What both paths made of one pair's session."""

    pair: str
    content_hash: str
    legacy_rows: int
    trusted_rows: int
    report: ReconciliationReport

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair": self.pair,
            "content_hash": self.content_hash,
            "legacy_rows": self.legacy_rows,
            "trusted_rows": self.trusted_rows,
            "reconciliation": self.report.to_dict(),
        }


@dataclass(frozen=True)
class ReconciliationRun:
    """One reconciliation cycle across every requested pair."""

    status: str
    run_id: str | None
    session: date | None
    pairs: tuple[PairReconciliation, ...]
    quarantined_revision_ids: tuple[str, ...]
    unfinished_sessions: tuple[tuple[str, date], ...]
    checked_at: datetime
    dataset_keys: tuple[str, ...] = FX_DATASET_KEYS

    @property
    def exit_code(self) -> int:
        return EXIT_DIVERGED if self.status == STATUS_DIVERGED else EXIT_OK

    @property
    def reconciled(self) -> bool:
        return all(item.report.reconciled for item in self.pairs)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "run_id": self.run_id,
            "session": self.session.isoformat() if self.session else None,
            "checked_at": self.checked_at.isoformat(),
            "dataset_keys": list(self.dataset_keys),
            # Reported, never graded: v1 has no quarantine state, so a held-back
            # rate is not a divergence. It is here because a cutover should not
            # be enabled on a day the ledger is holding rates back.
            "quarantined_revision_ids": list(self.quarantined_revision_ids),
            # Also reported, never graded: both paths drop an unfinished FX
            # session, so this is the two agreeing. It is the durable record
            # that they agreed for the *stated* reason.
            "unfinished_sessions": [
                {"pair": pair, "session": session.isoformat()} for pair, session in self.unfinished_sessions
            ],
            "pairs": [item.to_dict() for item in self.pairs],
        }


def reconcile_once(
    *,
    pairs: tuple[str, ...] = FX_PAIRS,
    period: str = DEFAULT_PERIOD,
    repository_root: str | Path | None = None,
    download=None,
    now: datetime | None = None,
) -> ReconciliationRun:
    """Run each pair's bars through both paths and account for every difference."""

    checked_at = now or datetime.now(timezone.utc)
    downloader = _MemoisingDownloader(download)

    replays: list[FxArtifactReplay] = []
    legacy_frames: dict[str, pd.DataFrame] = {}
    for pair in pairs:
        replays.append(
            fetch_fx_artifact(pair, download=downloader, period=period, retrieved_at=checked_at)
        )
        legacy_frames[pair] = legacy_currency_frame(
            pair, _v1_frame(pair, downloader, period=period, now=checked_at)
        )

    session = _resolve_session(legacy_frames)
    if session is None:
        # Every pair empty means the provider published no finished session —
        # a weekend, a holiday, or a run before the 17:00 New York rollover.
        # Grading that as a failure would fire the job every Saturday.
        log.info("Required FX: no finished session published for any pair")
        return ReconciliationRun(
            status=STATUS_NO_SESSION,
            run_id=None,
            session=None,
            pairs=(),
            quarantined_revision_ids=(),
            unfinished_sessions=(),
            checked_at=checked_at,
        )

    with _repository_dir(repository_root) as root:
        repository = TemporaryDirectoryTrustRepository(root)
        result = dual_write_fx(
            repository,
            replays,
            legacy_frames,
            session=session,
            ingested_at=checked_at,
            now=checked_at,
        )
        # Counted on the *pinned* frame, not on accepted revisions: the
        # provider window carries several sessions and the report is about one.
        trusted_counts = {
            replay.pair: len(trusted_currency_frame(repository, replay.pair, session=session))
            for replay in replays
        }

    items = tuple(
        PairReconciliation(
            pair=replay.pair,
            content_hash=replay.artifact.reference.content_hash,
            legacy_rows=int((legacy_frames[replay.pair]["Date"] == session.isoformat()).sum()),
            trusted_rows=trusted_counts.get(replay.pair, 0),
            report=result.reconciliations[replay.pair],
        )
        for replay in replays
    )
    status = STATUS_RECONCILED if result.reconciled else STATUS_DIVERGED
    return ReconciliationRun(
        status=status,
        run_id=result.ingestion.run_id,
        session=session,
        pairs=items,
        quarantined_revision_ids=result.ingestion.quarantined_revision_ids,
        unfinished_sessions=result.ingestion.unfinished_sessions,
        checked_at=checked_at,
    )


def _resolve_session(legacy_frames: dict[str, pd.DataFrame]) -> date | None:
    """The newest session v1 would publish, resolved once for both paths.

    Taken from the v1 side because that is the frame the dashboard reads today;
    the trusted side is then pinned to it rather than choosing its own, so a
    pair that printed one extra day cannot read as a divergence.
    """

    dates = [
        str(value)
        for frame in legacy_frames.values()
        if not frame.empty
        for value in frame["Date"].tolist()
        if value and not pd.isna(value)
    ]
    if not dates:
        return None
    return date.fromisoformat(max(dates))


def _v1_frame(pair: str, download, *, period: str, now: datetime) -> pd.DataFrame:
    """The v1 half of the dual write: the shared bars under v1's own guard.

    This is ``fetch_one``'s body with the download already made — the same
    ``drop_unsettled_session`` call, the same ``FX_SESSION`` rule — rather than
    a call to ``fetch_one`` itself, for one reason: the guard's clock has to be
    the *run's* instant. ``fetch_one`` reads the wall clock, so a run straddling
    the 17:00 New York rollover would judge the two paths at two different
    moments and report the difference as a divergence. Nothing else about v1's
    policy is restated here; the rule and the drop are both imported.
    """

    return drop_unsettled_session(
        download(ticker_for(pair), period=period),
        label=ticker_for(pair),
        rule=FX_SESSION,
        now=now,
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
        self._tmp = tempfile.TemporaryDirectory(prefix="fx-reconcile-")
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
    for item in run.pairs:
        report = item.report
        if report.reconciled and item.legacy_rows == 0:
            log.warning(
                "FX %s: no rate for %s on either path — the pair did not print this session",
                item.pair, run.session,
            )
            continue
        if report.reconciled:
            log.info("FX %s %s: reconciled on both paths", item.pair, run.session)
            continue
        log.error(
            "FX %s %s: DIVERGED — %d matched, %d only in v1, %d only in trusted, %d field differences",
            item.pair,
            run.session,
            report.matched_rows,
            len(report.missing_in_trusted),
            len(report.missing_in_legacy),
            len(report.field_differences),
        )
        for diff in report.field_differences[:10]:
            log.error("  %s %s: v1=%r trusted=%r", diff["key"], diff["field"], diff["legacy"], diff["trusted"])
    for pair, session in run.unfinished_sessions:
        log.info("FX %s: %s was an unfinished session and was priced by neither path", pair, session)
    if run.quarantined_revision_ids:
        log.warning(
            "Required FX: %d rate(s) quarantined by the ledger this run — "
            "reconciliation is not cutover evidence while rates are being held back",
            len(run.quarantined_revision_ids),
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pair", action="append", dest="pairs", default=None,
                        choices=list(FX_PAIRS),
                        help="reconcile only this pair (repeatable; default: every required pair)")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help=f"where to write the JSON report (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--repository", type=Path, default=None,
                        help="durable trust repository root (default: a scratch directory)")
    args = parser.parse_args(argv)

    pairs = tuple(args.pairs) if args.pairs else FX_PAIRS
    try:
        run = reconcile_once(pairs=pairs, repository_root=args.repository)
    except (requests.RequestException, ValueError) as exc:
        # ValueError covers FxShapeError, which subclasses it: a payload whose
        # shape moved is an upstream fact, not a parse disagreement. Upstream
        # being down is not evidence about the trusted path, so it gets its own
        # exit code rather than reading as a divergence.
        log.error("Required FX: reconciliation could not run — %s", exc)
        return EXIT_UNAVAILABLE

    _log_summary(run)
    path = write_report(run, args.output)
    log.info("Required FX: report written to %s", path)
    return run.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
