"""Trusted ingestion for the conversion-critical FX pairs (#194, pilot 3).

The third pilot dataset, and the one that decides what a physical price *is*
in dollars. Layer 7's four required pairs — BRL, CNY, INR and ZAR against USD
— are the inputs every ``home_per_mt`` leg converts at, so a wrong rate is a
wrong landed cost on a whole origin rather than a wrong cell in an FX table.

Where :mod:`trust.fx` reads FX revisions already in a repository, this module
is the writer: it captures a provider payload as a raw artifact, parses
candidates, runs the contracted quality rules, and appends revisions whatever
their disposition.

**Why this pilot drops a bar instead of rejecting one.**
:mod:`trust.cbot_benchmarks` proves the settlement machinery: its contracts
carry ``settlement.confirmed`` at REJECT severity, so an unfinished bar is
ingested, refused, and leaves a durable Finding. The FX contracts carry no
such rule, deliberately — spot FX has no settlement in the CBOT sense, the
market runs continuously from Sunday 17:00 New York to Friday 17:00, and
there is no honest "settled" state to assert. But that means an unfinished FX
bar would be *accepted*, land in the trusted frame, and diverge from a v1 path
that dropped it — a one-row divergence on every pre-cutoff run, every day. A
job that cries wolf daily is worse than no job.

So this adapter applies the same time-based cutoff v1 applies, by importing
:data:`fetchers._settlement.FX_SESSION` rather than restating the rollover.
Two definitions of "finished" that can drift apart is exactly the duplication
the trusted path exists to remove. What it does *not* do is silently forget:
the dropped session is recorded as a ``fx.session-unfinished`` Finding at
WARNING severity, and named on the ingestion result. v1 emits a log line that
scrolls away; this leaves a queryable record of the bar it refused to price.

**The candle is carried, the volume is not.** Yahoo reports a volume of zero
on FX bars — an artefact of a market with no central tape, not an observation
that nothing traded. Invariant 2 says a blank is never a zero, so no volume is
recorded at all. ``currencies`` in v1 has no volume column for the same reason.

The artifact is metadata-only: a provider frame is library output rather than
a fetched document, so the content hash covers the canonical capture of the
bars, not upstream bytes. That is deliberate and matches the Yahoo rights
position, which records raw-content retention as prohibited.

Nothing here changes the v1 path. ``fetchers/yfinance.py`` still writes
``currencies`` and every conversion still reads it, until :mod:`trust.read_path`
is switched on for these datasets and the evidence supports it.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import pandas as pd

from config import CURRENCY_TICKERS
from fetchers._settlement import FX_SESSION
from trust.dataset_health import DatasetHealthInput, evaluate_dataset_health
from trust.domain import (
    ArtifactReference,
    CandidateObservation,
    DatasetResult,
    DatasetResultStatus,
    EligibilityScope,
    Finding,
    FindingSeverity,
    FxPairIdentity,
    ObservationIdentity,
    ObservationRevision,
    QualityState,
    RawArtifact,
    Run,
    RunStatus,
    Timestamp,
)
from trust.quality import (
    NumericValidationPolicy,
    QualityEvaluation,
    QualityRuleEngine,
    generic_candidate_quality_rules,
)
from trust.read_path import REQUIRED_FX_DATASET_KEYS
from trust.reconciliation import ReconciliationReport, reconcile_frames
from trust.registry import FX_CONTRACTS, REQUIRED_FX_PAIRS, DatasetContract
from trust.repository import TrustRepository

PARSER_VERSION = "required-fx/trusted-v1"
RUN_CODE_REVISION = "mirror-market-required-fx-trusted-v1"
MEDIA_TYPE = "application/json"

#: A synthetic retrieval URL. The bars come from a library call rather than a
#: response with a URL, so the scheme says what it is instead of pointing at a
#: page that does not exist.
RETRIEVAL_URL_TEMPLATE = "yfinance:chart/{ticker}?period={period}"

#: Default provider window. Five sessions is enough to carry the previous
#: accepted close a daily-move check needs without pulling history the
#: reconciliation would then have to pin away.
DEFAULT_PERIOD = "5d"

#: Day-over-day move on one pair beyond which the revision is quarantined for
#: review instead of accepted.
#:
#: 10% is above anything these four currencies do in an orderly session and
#: below every mechanical defect worth catching — an inverted quote, a units
#: change, a redenomination. A genuine devaluation past it is *held*, not
#: dropped: the revision is durable, it is named in the reconciliation report,
#: and a human decides. Deliberately tighter than the 20% benchmark threshold,
#: which has CBOT's own expanded limits to clear.
DAILY_MOVE_QUARANTINE_THRESHOLD = Decimal("0.10")


def _dataset_key(pair: str) -> str:
    return f"fx-{pair.lower().replace('/', '-')}"


def _contracts_by_pair() -> Mapping[str, DatasetContract]:
    """Pair -> registry contract, matched on the dataset key, never on order.

    ``FX_CONTRACTS`` happens to be built in ``REQUIRED_FX_PAIRS`` order today.
    Zipping on that would put a Rand rate under the Real's contract the day
    that changes — a mislabelled conversion input, which is worse than a
    crash. So the key is recomputed and checked.
    """

    by_key = {contract.dataset.key: contract for contract in FX_CONTRACTS}
    missing = [pair for pair in REQUIRED_FX_PAIRS if _dataset_key(pair) not in by_key]
    if missing:
        raise RuntimeError(f"required FX pairs have no registry contract: {', '.join(missing)}")
    return {pair: by_key[_dataset_key(pair)] for pair in REQUIRED_FX_PAIRS}


#: Pair -> registry contract. Read off the registry rather than restated, so a
#: pair added to ``REQUIRED_FX_PAIRS`` cannot be silently uningested.
_CONTRACT_BY_PAIR: Mapping[str, DatasetContract] = _contracts_by_pair()

FX_PAIRS: tuple[str, ...] = tuple(REQUIRED_FX_PAIRS)

#: The registry dataset keys of the four required pairs. Re-exported from the
#: read-path switch, where the cutover names live, rather than listed twice.
FX_DATASET_KEYS: tuple[str, ...] = REQUIRED_FX_DATASET_KEYS


class FxShapeError(ValueError):
    """The provider payload is not the shape this parser was written against."""


def contract_for(pair: str) -> DatasetContract:
    """The registry contract for one required pair."""

    try:
        return _CONTRACT_BY_PAIR[pair]
    except KeyError as exc:
        raise KeyError(f"{pair} is not a required FX pair with a trusted contract") from exc


def ticker_for(pair: str) -> str:
    """The v1 provider ticker for one required pair.

    Read from ``config.CURRENCY_TICKERS`` — the same map Layer 7 fetches from —
    so the trusted path cannot ask the provider for a different instrument than
    v1 does and report the answer as a parse difference.
    """

    contract_for(pair)
    try:
        return CURRENCY_TICKERS[pair]
    except KeyError as exc:  # pragma: no cover - a contracted pair v1 does not fetch
        raise KeyError(f"{pair} has no ticker in config.CURRENCY_TICKERS") from exc


def last_settled_fx_session(now: datetime) -> date:
    """The newest FX session whose bar has finished at ``now``.

    Delegated to the shared guard's ``FX_SESSION`` rule. The cutoff is defined
    once, in config, and read here — never restated.
    """

    return FX_SESSION.last_settled_session(_utc(now))


# ---------------------------------------------------------------------------
# Raw artifacts and replay
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FxProviderBar:
    """One provider daily bar for one currency pair.

    ``close`` is the headline rate; open/high/low are the candle it came off
    and may be absent, because a provider that omits them is a different fact
    from a provider that reports zero.
    """

    pair: str
    session_date: date
    close: Decimal
    open: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "pair", str(self.pair).strip().upper())
        if not self.pair:
            raise ValueError("provider bar requires a pair")
        if not isinstance(self.session_date, date) or isinstance(self.session_date, datetime):
            raise ValueError("provider bar session_date must be a date")
        for field_name in ("close", "open", "high", "low"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _decimal(value, f"provider_bar.{field_name}"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair": self.pair,
            "session_date": self.session_date.isoformat(),
            "close": _decimal_text(self.close),
            "open": _decimal_text(self.open),
            "high": _decimal_text(self.high),
            "low": _decimal_text(self.low),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FxProviderBar:
        try:
            return cls(
                pair=str(data["pair"]),
                session_date=date.fromisoformat(str(data["session_date"])),
                close=_decimal(data["close"], "provider_bar.close"),
                open=_optional_decimal(data.get("open"), "provider_bar.open"),
                high=_optional_decimal(data.get("high"), "provider_bar.high"),
                low=_optional_decimal(data.get("low"), "provider_bar.low"),
            )
        except (KeyError, ValueError, InvalidOperation) as exc:
            raise FxShapeError(f"Required FX: provider bar is malformed: {exc}") from exc


@dataclass(frozen=True)
class FxArtifactReplay:
    """A captured provider payload that can be parsed without another request."""

    pair: str
    artifact: RawArtifact
    content: bytes

    def __post_init__(self) -> None:
        digest = hashlib.sha256(self.content).hexdigest()
        if digest != self.artifact.reference.content_hash:
            raise ValueError("replay content does not match raw artifact hash")
        if self.artifact.byte_size != len(self.content):
            raise ValueError("replay content size does not match raw artifact metadata")
        contract_for(self.pair)


def artifact_from_bars(
    pair: str,
    bars: Sequence[FxProviderBar],
    *,
    period: str = DEFAULT_PERIOD,
    retrieved_at: datetime | None = None,
    response_status: int = 200,
) -> FxArtifactReplay:
    """Capture provider bars as a metadata-only raw artifact plus replay bytes."""

    contract = contract_for(pair)
    dataset = contract.dataset
    content = _canonical_content(pair, bars)
    digest = hashlib.sha256(content).hexdigest()
    reference = ArtifactReference(
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        dataset_key=dataset.key,
        content_hash=digest,
        content_retained=False,
        media_type=MEDIA_TYPE,
    )
    artifact = RawArtifact(
        reference=reference,
        retrieval_url=RETRIEVAL_URL_TEMPLATE.format(ticker=ticker_for(pair), period=period),
        retrieved_at=Timestamp(_utc(retrieved_at or datetime.now(timezone.utc))),
        response_status=response_status,
        byte_size=len(content),
        content=None,
    )
    return FxArtifactReplay(pair=pair, artifact=artifact, content=content)


def bars_from_replay(replay: FxArtifactReplay) -> tuple[FxProviderBar, ...]:
    """Decode replay bytes, hard-failing on anything but the captured shape."""

    try:
        payload = json.loads(replay.content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FxShapeError(f"Required FX: raw artifact is not valid JSON: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise FxShapeError("Required FX: artifact root is not a JSON object")
    if payload.get("pair") != replay.pair:
        raise FxShapeError("Required FX: artifact pair does not match the replay")
    raw_bars = payload.get("bars")
    if not isinstance(raw_bars, list):
        raise FxShapeError("Required FX: artifact 'bars' is not a list")
    return tuple(FxProviderBar.from_dict(item) for item in raw_bars)


def fetch_fx_artifact(
    pair: str,
    *,
    download=None,
    period: str = DEFAULT_PERIOD,
    retrieved_at: datetime | None = None,
) -> FxArtifactReplay:
    """Fetch one pair's provider bars and capture them as an artifact.

    ``download`` is the *unguarded* provider download
    (``fetchers.yfinance.download_bars``), not ``fetch_one``. The trusted path
    fetches upstream directly and applies its own policy: handed a frame v1's
    guard has already trimmed, this adapter's cutoff could never fire and the
    pilot would silently degrade to "we drop it too", green and proving
    nothing.
    """

    from fetchers.yfinance import download_bars  # imported late so tests need no network stack

    ticker = ticker_for(pair)
    frame = (download or download_bars)(ticker, period=period)
    return artifact_from_bars(
        pair,
        _bars_from_frame(pair, frame),
        period=period,
        retrieved_at=retrieved_at,
    )


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ParsedFxPayload:
    """Candidates for finished sessions, plus the sessions that were not."""

    candidates: tuple[CandidateObservation, ...]
    unfinished_sessions: tuple[date, ...] = ()


def parse_fx_candidates(
    replay: FxArtifactReplay,
    *,
    parsed_at: datetime | None = None,
    parser_version: str = PARSER_VERSION,
    now: datetime | None = None,
) -> ParsedFxPayload:
    """Turn replayed provider bars into candidates, dropping unfinished sessions."""

    bars = bars_from_replay(replay)
    parsed_instant = _utc(parsed_at or replay.artifact.retrieved_at.value)
    evaluated_at = _utc(now or parsed_instant)
    cutoff = last_settled_fx_session(evaluated_at)
    contract = contract_for(replay.pair)
    fx_pair = _fx_pair(replay.pair)

    candidates: list[CandidateObservation] = []
    unfinished: list[date] = []
    for bar in bars:
        if bar.pair != replay.pair:
            raise FxShapeError(
                f"Required FX: artifact for {replay.pair} carries a {bar.pair} bar"
            )
        if bar.session_date > cutoff:
            unfinished.append(bar.session_date)
            continue
        identity = ObservationIdentity(
            source_id=contract.dataset.source_id,
            dataset_id=contract.dataset.dataset_id,
            dataset_key=contract.dataset.key,
            commodity=str(contract.identity.fixed_fields["commodity"]) if contract.identity else "",
            product_form=fx_pair.product_form,
            venue="yahoo-finance",
            price_type="market-close",
            currency=fx_pair.quote_currency,
            unit=fx_pair.unit,
            effective_date=bar.session_date,
            fx_pair=fx_pair,
            source_record_id=ticker_for(replay.pair),
        )
        candidates.append(
            CandidateObservation(
                identity=identity,
                value=bar.close,
                artifact=replay.artifact.reference,
                parser_version=parser_version,
                parsed_at=parsed_instant,
                # The session the bar belongs to is genuinely observed; the
                # publication instant is not published at all, so it is left
                # absent rather than inferred from fetch time.
                observed_at=Timestamp(_session_instant(bar.session_date), inferred=True),
                open_value=bar.open,
                high_value=bar.high,
                low_value=bar.low,
                close_value=bar.close,
            )
        )
    return ParsedFxPayload(candidates=tuple(candidates), unfinished_sessions=tuple(sorted(set(unfinished))))


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FxDatasetIngestion:
    """What one pair's dataset produced in one run."""

    pair: str
    dataset_id: str
    artifact_id: str
    candidate_count: int
    accepted_revision_ids: tuple[str, ...]
    quarantined_revision_ids: tuple[str, ...]
    rejected_revision_ids: tuple[str, ...]
    unfinished_sessions: tuple[date, ...]
    finding_ids: tuple[str, ...]
    result: DatasetResult


@dataclass(frozen=True)
class FxTrustedIngestion:
    """One trusted collection cycle across every required pair."""

    run_id: str
    status: RunStatus
    datasets: tuple[FxDatasetIngestion, ...]
    error: str | None = None

    def dataset(self, pair: str) -> FxDatasetIngestion:
        for item in self.datasets:
            if item.pair == pair:
                return item
        raise KeyError(pair)

    @property
    def accepted_revision_ids(self) -> tuple[str, ...]:
        return tuple(sorted(rid for item in self.datasets for rid in item.accepted_revision_ids))

    @property
    def quarantined_revision_ids(self) -> tuple[str, ...]:
        return tuple(sorted(rid for item in self.datasets for rid in item.quarantined_revision_ids))

    @property
    def unfinished_sessions(self) -> tuple[tuple[str, date], ...]:
        return tuple(
            (item.pair, session) for item in self.datasets for session in item.unfinished_sessions
        )


def ingest_fx_replays(
    repository: TrustRepository,
    replays: Sequence[FxArtifactReplay],
    *,
    ingested_at: datetime | None = None,
    parser_version: str = PARSER_VERSION,
    now: datetime | None = None,
    holidays: Sequence[date] = (),
) -> FxTrustedIngestion:
    """Persist replayed provider payloads as validated observation revisions.

    One run covers every replay handed in: one run is one attempted collection
    cycle, and the four required pairs are collected together because a
    conversion needs all of them.
    """

    repository.initialize()
    stamp = _utc(ingested_at or datetime.now(timezone.utc))
    evaluated_at = _utc(now) if now is not None else stamp
    provisional = _run(stamp, RunStatus.RUNNING, {}, ended_at=None)
    run_id = provisional.run_id

    parser_versions: dict[str, str] = {}
    findings: list[Finding] = []
    ingestions: list[FxDatasetIngestion] = []

    for replay in replays:
        contract = contract_for(replay.pair)
        parser_versions[contract.dataset.dataset_id] = parser_version
        artifact_ref = repository.store_artifact(replay.artifact, contract)
        record_findings: list[Finding] = []
        try:
            parsed = parse_fx_candidates(
                replay, parsed_at=stamp, parser_version=parser_version, now=evaluated_at
            )
        except FxShapeError as exc:
            finding = _dataset_finding(
                run_id,
                contract,
                subject_id=artifact_ref.artifact_id,
                rule_id="fx.shape",
                severity=FindingSeverity.REJECT,
                evidence={"artifact_id": artifact_ref.artifact_id, "error": str(exc)},
                message=str(exc),
            )
            repository.store(finding)
            findings.append(finding)
            ingestions.append(
                _dataset_ingestion(
                    repository,
                    replay.pair,
                    contract,
                    artifact_ref,
                    run_id=run_id,
                    candidate_count=0,
                    revisions=(),
                    unfinished_sessions=(),
                    health_findings=(finding,),
                    record_findings=(finding,),
                    now=stamp,
                    holidays=holidays,
                    external_error=str(exc),
                )
            )
            continue

        for session in parsed.unfinished_sessions:
            # Durable, and deliberately only a WARNING: refusing to price an
            # unfinished session is the adapter working, not the dataset
            # failing. v1 logs the same drop and the line scrolls away.
            finding = _dataset_finding(
                run_id,
                contract,
                subject_id=f"{artifact_ref.artifact_id}:{session.isoformat()}",
                rule_id="fx.session-unfinished",
                severity=FindingSeverity.WARNING,
                evidence={
                    "session_date": session.isoformat(),
                    "last_settled_session": last_settled_fx_session(evaluated_at).isoformat(),
                    "rule": FX_SESSION.name,
                },
                message=(
                    f"{replay.pair} {session.isoformat()} is an unfinished FX session and was not priced"
                ),
            )
            repository.store(finding)
            record_findings.append(finding)

        engine = QualityRuleEngine(
            generic_candidate_quality_rules(
                contract,
                numeric_policy=NumericValidationPolicy(allow_zero=False, allow_negative=False),
            )
        )
        previous_values = _previous_accepted_values(repository, contract, parsed.candidates)
        revisions: list[ObservationRevision] = []
        for candidate in parsed.candidates:
            evaluation: QualityEvaluation = engine.evaluate(
                run_id=run_id,
                dataset_id=contract.dataset.dataset_id,
                subject_id=candidate.candidate_id,
                subject=candidate,
                dataset_context={
                    "previous_value_by_subject_id": previous_values,
                    "daily_move_quarantine_threshold": DAILY_MOVE_QUARANTINE_THRESHOLD,
                },
            )
            for finding in evaluation.findings:
                repository.store(finding)
                record_findings.append(finding)
            revisions.append(
                ObservationRevision(
                    identity=candidate.identity,
                    value=candidate.value,
                    ingested_at=stamp,
                    quality_state=evaluation.disposition,
                    public_eligible=_public_eligible(contract),
                    artifact=artifact_ref,
                    parser_version=parser_version,
                    observed_at=candidate.observed_at,
                    open_value=candidate.open_value,
                    high_value=candidate.high_value,
                    low_value=candidate.low_value,
                    close_value=candidate.close_value,
                    finding_ids=evaluation.finding_ids,
                )
            )
        repository.append_observation_revisions(revisions)
        findings.extend(record_findings)
        ingestions.append(
            _dataset_ingestion(
                repository,
                replay.pair,
                contract,
                artifact_ref,
                run_id=run_id,
                candidate_count=len(parsed.candidates),
                revisions=tuple(revisions),
                unfinished_sessions=parsed.unfinished_sessions,
                # Candidate-scope findings are resolved by their own
                # disposition and ride on the revisions; only dataset-scope
                # findings reach the dataset result.
                health_findings=(),
                record_findings=tuple(record_findings),
                now=stamp,
                holidays=holidays,
            )
        )

    summary = _findings_summary(findings)
    status = (
        RunStatus.SUCCEEDED
        if ingestions and all(item.result.status is DatasetResultStatus.SUCCESS for item in ingestions)
        else RunStatus.FAILED
    )
    completed = _run(stamp, status, parser_versions, ended_at=stamp, findings_summary=summary)
    repository.store(completed)
    return FxTrustedIngestion(run_id=completed.run_id, status=status, datasets=tuple(ingestions))


# ---------------------------------------------------------------------------
# Trusted reads and reconciliation
# ---------------------------------------------------------------------------

#: The column projection ``pipeline.store.save_currency_data`` writes, in its
#: order. Both sides of the reconciliation are put in this shape.
CURRENCY_COLUMNS = ["pair", "Date", "Open", "High", "Low", "Close"]


def legacy_currency_frame(
    pair: str,
    frame: pd.DataFrame,
    *,
    session: date | None = None,
) -> pd.DataFrame:
    """v1's own parse of a provider frame, in the shape it stores.

    ``clean_ohlcv`` and the ``save_currency_data`` projection are reused rather
    than restated: the point of reconciliation is to compare the two *parses*
    of one payload, so any step v1 performs that this restated would report a
    difference that is really a second implementation.
    """

    from pipeline.clean import clean_ohlcv
    from pipeline.store import _date

    contract_for(pair)
    if frame is None or getattr(frame, "empty", True):
        return pd.DataFrame(columns=CURRENCY_COLUMNS)
    cleaned = clean_ohlcv(frame, label=pair).reset_index().copy()
    cleaned["pair"] = pair
    cleaned["Date"] = _date(cleaned["Date"])
    result = cleaned[CURRENCY_COLUMNS]
    if session is not None:
        result = result[result["Date"] == session.isoformat()]
    return result.reset_index(drop=True)


def trusted_currency_frame(
    repository: TrustRepository,
    pair: str,
    *,
    scope: EligibilityScope = EligibilityScope.INTERNAL,
    session: date | None = None,
) -> pd.DataFrame:
    """Accepted current revisions for one pair, in the v1 ``currencies`` shape.

    Revisions whose current state is quarantined, rejected or superseded are
    simply absent. A missing rate is honest; a rate the ledger is holding back,
    rendered anyway, is not.
    """

    contract = contract_for(pair)
    rows = []
    for revision in _accepted_heads(repository, contract, scope=scope):
        identity = revision.identity
        if session is not None and identity.effective_date != session:
            continue
        rows.append(
            {
                "pair": pair,
                "Date": identity.effective_date.isoformat(),
                "Open": _float_or_none(revision.open_value),
                "High": _float_or_none(revision.high_value),
                "Low": _float_or_none(revision.low_value),
                "Close": float(revision.value),
            }
        )
    frame = pd.DataFrame(rows, columns=CURRENCY_COLUMNS)
    if frame.empty:
        return frame
    return frame.sort_values("Date").reset_index(drop=True)


def reconcile_fx(legacy: pd.DataFrame, trusted: pd.DataFrame) -> ReconciliationReport:
    """Account for every row and field difference between v1 and the ledger.

    The key is ``(pair, Date)`` — what ``currencies`` itself dedupes on, read
    off the upsert in ``pipeline.store.save_currency_data`` rather than guessed.
    """

    return reconcile_frames(
        legacy,
        trusted,
        key_columns=("pair", "Date"),
        value_columns=("Open", "High", "Low", "Close"),
        text_columns=("pair", "Date"),
    )


@dataclass(frozen=True)
class FxDualWriteResult:
    ingestion: FxTrustedIngestion
    reconciliations: Mapping[str, ReconciliationReport]

    @property
    def reconciled(self) -> bool:
        return all(report.reconciled for report in self.reconciliations.values())


def dual_write_fx(
    repository: TrustRepository,
    replays: Sequence[FxArtifactReplay],
    legacy_frames: Mapping[str, pd.DataFrame],
    *,
    session: date | None = None,
    ingested_at: datetime | None = None,
    parser_version: str = PARSER_VERSION,
    now: datetime | None = None,
) -> FxDualWriteResult:
    """Run trusted ingestion beside the v1 fetcher and compare both outputs."""

    ingestion = ingest_fx_replays(
        repository, replays, ingested_at=ingested_at, parser_version=parser_version, now=now
    )
    reconciliations = {
        replay.pair: reconcile_fx(
            _pinned(legacy_frames.get(replay.pair, pd.DataFrame(columns=CURRENCY_COLUMNS)), session),
            trusted_currency_frame(repository, replay.pair, session=session),
        )
        for replay in replays
    }
    return FxDualWriteResult(ingestion=ingestion, reconciliations=reconciliations)


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _pinned(frame: pd.DataFrame, session: date | None) -> pd.DataFrame:
    if session is None or frame.empty:
        return frame
    return frame[frame["Date"] == session.isoformat()].reset_index(drop=True)


def _dataset_ingestion(
    repository: TrustRepository,
    pair: str,
    contract: DatasetContract,
    artifact_ref: ArtifactReference,
    *,
    run_id: str,
    candidate_count: int,
    revisions: tuple[ObservationRevision, ...],
    unfinished_sessions: tuple[date, ...],
    health_findings: tuple[Finding, ...],
    record_findings: tuple[Finding, ...],
    now: datetime,
    holidays: Sequence[date],
    external_error: str | None = None,
) -> FxDatasetIngestion:
    known = _dataset_revisions(repository, contract)
    result = evaluate_dataset_health(
        DatasetHealthInput(
            run_id=run_id,
            contract=contract,
            candidate_count=candidate_count,
            accepted_revisions=known,
            now=now,
            findings=health_findings,
            artifacts=(artifact_ref,),
            external_error=external_error,
            holidays=tuple(holidays),
            # Yahoo's public-display right is recorded `unknown`, so these
            # revisions are `public_eligible=False` and stay fail-closed for
            # anything published. Health is an *ingestion* verdict; grading
            # coverage as zero would blame the source for a rights answer
            # nobody has recorded yet.
            eligibility_scope=EligibilityScope.INTERNAL,
        )
    )
    return FxDatasetIngestion(
        pair=pair,
        dataset_id=contract.dataset.dataset_id,
        artifact_id=artifact_ref.artifact_id,
        candidate_count=candidate_count,
        accepted_revision_ids=_ids(revisions, QualityState.ACCEPTED),
        quarantined_revision_ids=_ids(revisions, QualityState.QUARANTINED),
        rejected_revision_ids=_ids(revisions, QualityState.REJECTED),
        unfinished_sessions=unfinished_sessions,
        finding_ids=tuple(sorted({finding.finding_id for finding in record_findings})),
        result=result,
    )


def _ids(revisions: Sequence[ObservationRevision], state: QualityState) -> tuple[str, ...]:
    return tuple(sorted(item.revision_id for item in revisions if item.quality_state is state))


def _dataset_revisions(
    repository: TrustRepository, contract: DatasetContract
) -> tuple[ObservationRevision, ...]:
    return tuple(
        revision
        for revision in repository.all_observation_revisions()
        if revision.identity.dataset_id == contract.dataset.dataset_id
    )


def _accepted_heads(
    repository: TrustRepository,
    contract: DatasetContract,
    *,
    scope: EligibilityScope,
) -> tuple[ObservationRevision, ...]:
    """The current accepted revision of every observation in this dataset.

    Resolved through ``current_accepted_revision`` rather than by filtering the
    ledger here, so supersession and eligibility are decided in one place and a
    corrected rate cannot be read back beside the rate it replaced.
    """

    identities: dict[str, ObservationIdentity] = {}
    for revision in _dataset_revisions(repository, contract):
        identities.setdefault(revision.identity.observation_id, revision.identity)
    heads = []
    for identity in identities.values():
        head = repository.current_accepted_revision(identity, scope=scope)
        if head is not None:
            heads.append(head)
    return tuple(heads)


def _previous_accepted_values(
    repository: TrustRepository,
    contract: DatasetContract,
    candidates: Sequence[CandidateObservation],
) -> dict[str, Decimal]:
    """The accepted rate a candidate is judged against, keyed by candidate id.

    A re-print of a session already accepted is judged against *that* rate, so
    a provider revising a close by a tenth cannot land silently; failing that,
    against the most recent accepted earlier session. A candidate with neither
    has nothing to move from and is not move checked — the first observation of
    a pair is not an anomaly.
    """

    latest: tuple[date, datetime, Decimal] | None = None
    by_observation: dict[str, tuple[datetime, Decimal]] = {}
    for revision in _dataset_revisions(repository, contract):
        if revision.quality_state is not QualityState.ACCEPTED:
            continue
        identity = revision.identity
        value = Decimal(str(revision.value))
        seen_observation = by_observation.get(identity.observation_id)
        if seen_observation is None or revision.ingested_at >= seen_observation[0]:
            by_observation[identity.observation_id] = (revision.ingested_at, value)
        if latest is None or (identity.effective_date, revision.ingested_at) > (latest[0], latest[1]):
            latest = (identity.effective_date, revision.ingested_at, value)

    previous: dict[str, Decimal] = {}
    for candidate in candidates:
        same_observation = by_observation.get(candidate.identity.observation_id)
        if same_observation is not None:
            previous[candidate.candidate_id] = same_observation[1]
            continue
        if latest is not None and latest[0] < candidate.identity.effective_date:
            previous[candidate.candidate_id] = latest[2]
    return previous


def _run(
    at: datetime,
    status: RunStatus,
    parser_versions: Mapping[str, str],
    *,
    ended_at: datetime | None,
    findings_summary: Mapping[FindingSeverity, int] | None = None,
) -> Run:
    return Run(
        code_revision=RUN_CODE_REVISION,
        started_at=at,
        ended_at=ended_at,
        status=status,
        parser_versions=dict(parser_versions),
        findings_summary=dict(findings_summary or {}),
    )


def _findings_summary(findings: Iterable[Finding]) -> dict[FindingSeverity, int]:
    summary: dict[FindingSeverity, int] = {}
    for finding in findings:
        summary[finding.severity] = summary.get(finding.severity, 0) + 1
    return summary


def _dataset_finding(
    run_id: str,
    contract: DatasetContract,
    *,
    subject_id: str,
    rule_id: str,
    severity: FindingSeverity,
    evidence: Mapping[str, Any],
    message: str,
) -> Finding:
    return Finding(
        run_id=run_id,
        dataset_id=contract.dataset.dataset_id,
        subject_id=subject_id,
        rule_id=rule_id,
        rule_version="1.0.0",
        severity=severity,
        evidence=evidence,
        message=message,
    )


def _public_eligible(contract: DatasetContract) -> bool:
    rights = contract.rights
    return bool(rights and rights.publication_eligible)


def _canonical_content(pair: str, bars: Sequence[FxProviderBar]) -> bytes:
    payload = {
        "pair": pair,
        "provider": "yahoo-finance",
        "bars": [bar.to_dict() for bar in sorted(bars, key=lambda item: item.session_date)],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _bars_from_frame(pair: str, frame: pd.DataFrame) -> tuple[FxProviderBar, ...]:
    """Every dated row of a provider frame, with no session policy applied.

    A row without a close is not a bar: the provider answered for the day and
    priced nothing. It is absent rather than carried with a null rate.
    """

    if frame is None or getattr(frame, "empty", True):
        return ()
    bars: list[FxProviderBar] = []
    for stamp, row in zip(frame.index, frame.to_dict("records"), strict=True):
        moment = pd.Timestamp(stamp)
        if pd.isna(moment):
            raise FxShapeError(f"{pair}: provider frame has an undated bar")
        close = row.get("Close")
        if close is None or pd.isna(close):
            continue
        bars.append(
            FxProviderBar(
                pair=pair,
                session_date=moment.date(),
                close=_decimal(float(close), "provider_bar.close"),
                open=_row_decimal(row, "Open"),
                high=_row_decimal(row, "High"),
                low=_row_decimal(row, "Low"),
            )
        )
    return tuple(bars)


def _row_decimal(row: Mapping[Any, Any], column: str) -> Decimal | None:
    value = row.get(column)
    if value is None or pd.isna(value):
        return None
    return _decimal(float(value), f"provider_bar.{column.lower()}")


def _fx_pair(pair: str) -> FxPairIdentity:
    base, quote = pair.split("/", maxsplit=1)
    return FxPairIdentity(base_currency=base, quote_currency=quote)


def _session_instant(session_date: date) -> datetime:
    return datetime.combine(session_date, datetime.min.time(), tzinfo=timezone.utc)


def _float_or_none(value: Decimal | int | str | None) -> float | None:
    return None if value is None else float(value)


def _decimal(value: Decimal | int | float | str, field_name: str) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a decimal number") from exc
    if not result.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return result


def _optional_decimal(value: Any, field_name: str) -> Decimal | None:
    return None if value is None else _decimal(value, field_name)


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(timezone.utc)


__all__ = [
    "CURRENCY_COLUMNS",
    "DAILY_MOVE_QUARANTINE_THRESHOLD",
    "DEFAULT_PERIOD",
    "FX_DATASET_KEYS",
    "FX_PAIRS",
    "FxArtifactReplay",
    "FxDatasetIngestion",
    "FxDualWriteResult",
    "FxProviderBar",
    "FxShapeError",
    "FxTrustedIngestion",
    "PARSER_VERSION",
    "ParsedFxPayload",
    "artifact_from_bars",
    "bars_from_replay",
    "contract_for",
    "dual_write_fx",
    "fetch_fx_artifact",
    "ingest_fx_replays",
    "last_settled_fx_session",
    "legacy_currency_frame",
    "parse_fx_candidates",
    "reconcile_fx",
    "ticker_for",
    "trusted_currency_frame",
]
