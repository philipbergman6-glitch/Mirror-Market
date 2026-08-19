"""Trusted ingestion for CBOT named soy benchmark contracts (DT-16).

The second pilot dataset, and the first *critical* one. Where MAGyP proved
artifact capture and structured parsing against an official physical source,
this proves the four things a board price needs and a physical assessment does
not:

**A named contract, not a front month.** ``prices`` holds ``ZS=F``, a series
whose underlying contract Yahoo switches on its own schedule without saying
when. No hedge can be placed on it, and no roll-day gap in it is an economic
move. Every observation here identifies exchange, contract code and delivery
month, and a symbol that cannot be resolved to a contract of the dataset's own
product is refused rather than carried as an anonymous price.

**A settlement claim that is about the session, not the number.** Yahoo is a
delayed consumer endpoint; it publishes no settlement and this dataset's
``price_type`` says ``delayed-close`` accordingly. What the
``settlement.confirmed`` rule checks is narrower and still load-bearing: that
the bar is a *finished* session rather than the one in progress. An unfinished
bar is rejected, because storing it publishes a partial print as the day's
close — the exact defect ``fetchers/_settlement.py`` exists to prevent, here
enforced as a quality rule rather than a fetch-time drop, so a provider
substitution cannot lose it.

**A candle that has to be possible.** ``ohlc.relationship`` rejects a bar whose
high is below its open/low/close or whose low is above them. A frame like that
parses cleanly and is simply not a candle.

**An extreme move that quarantines rather than overwrites.** A day-over-day
move beyond :data:`DAILY_MOVE_QUARANTINE_THRESHOLD` is appended as a
*quarantined* revision. It is durable and auditable, it never reaches an
accepted query, and — the point — it does not displace the accepted history it
disagrees with. The previously accepted revision stays the current one.

Nothing here changes the v1 path. ``fetchers/forward_curve.py`` still writes
``forward_curve`` and the workstation still reads it, until
:mod:`trust.read_path` is switched on for these datasets and the evidence in
``docs/plans`` supports it.

One modelling decision is worth stating, because it looks like a loophole and
is not. ``DatasetResult`` refuses to be ``success`` while carrying a quarantine
or reject finding, which is right for *dataset-scope* findings — a stale
payload or a coverage shortfall is a fact about the whole dataset and cannot be
resolved by dropping a row. A *candidate-scope* finding is resolved by its own
disposition: the record it complains about was quarantined or rejected and is
not among the accepted revisions the result exposes. Those findings therefore
travel on the revision (``finding_ids``), on
:class:`BenchmarkDatasetIngestion`, and in the run manifest's findings summary
— all durable, all queryable — and only dataset-scope findings reach the
dataset result. Passing them all in would not make the result stricter; it
would make ``evaluate_dataset_health`` raise instead of returning a verdict.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, cast

import pandas as pd

from analysis.futures.domain import UnknownContract, parse_symbol, spec_for
from config import SETTLEMENT_TIMEZONE
from fetchers._settlement import session_is_settled
from fetchers.forward_curve import _build_contract_tickers
from trust.dataset_health import DatasetHealthInput, evaluate_dataset_health
from trust.domain import (
    ArtifactReference,
    CandidateObservation,
    ContractIdentity,
    DatasetResult,
    DatasetResultStatus,
    EligibilityScope,
    Finding,
    FindingSeverity,
    ObservationIdentity,
    ObservationRevision,
    QualityState,
    RawArtifact,
    Run,
    RunStatus,
    SettlementState,
    Timestamp,
)
from trust.quality import (
    NumericValidationPolicy,
    QualityEvaluation,
    QualityRuleEngine,
    generic_candidate_quality_rules,
)
from trust.reconciliation import ReconciliationReport, reconcile_frames
from trust.registry import SOY_BENCHMARK_CONTRACTS, DatasetContract
from trust.repository import TrustRepository

PARSER_VERSION = "cbot-named-contracts/trusted-v1"
RUN_CODE_REVISION = "mirror-market-cbot-named-contracts-trusted-v1"
MEDIA_TYPE = "application/json"

#: A synthetic retrieval URL. The artifact is the whole curve for one
#: commodity, assembled from one request per contract month, so there is no
#: single response URL to record. The scheme says what it is rather than
#: pointing at a page that does not exist.
RETRIEVAL_URL_TEMPLATE = "yfinance:chart/{commodity}?period=5d"

#: Day-over-day move on one named contract beyond which the revision is
#: quarantined for review instead of accepted.
#:
#: 20% is deliberately above every move CBOT's own rules permit in a session:
#: the *expanded* daily limits are 175c on a bean near 1000c (17.5%), 5.25c on
#: an oil near 45c (11.7%) and $35 on a meal near $300 (11.7%). So a move past
#: this cannot be a legitimate session and is either a unit change, a contract
#: mix-up or a bad bar — all of which want a human, not a silent accept and not
#: a silent drop.
DAILY_MOVE_QUARANTINE_THRESHOLD = Decimal("0.20")

#: How many contract months to walk, matching ``fetchers/forward_curve.py``'s
#: default. The trusted path must ask for the *same* set as v1 or a
#: reconciliation difference could mean "different contracts" rather than
#: "different parse".
CURVE_CONTRACT_COUNT = 6

#: Project commodity key -> (registry dataset key, product form).
_BENCHMARK_DATASETS: Mapping[str, tuple[str, str]] = {
    "Soybeans": ("cbot-soybean-named-contracts", "beans"),
    "Soybean Meal": ("cbot-soybean-meal-named-contracts", "meal"),
    "Soybean Oil": ("cbot-soybean-oil-named-contracts", "oil"),
}

BENCHMARK_COMMODITIES: tuple[str, ...] = tuple(_BENCHMARK_DATASETS)

_CONTRACT_BY_KEY: Mapping[str, DatasetContract] = {
    contract.dataset.key: contract for contract in SOY_BENCHMARK_CONTRACTS
}


class BenchmarkShapeError(ValueError):
    """The provider payload is not the shape this parser was written against."""


def contract_for(commodity: str) -> DatasetContract:
    """The registry contract for one project commodity key."""

    try:
        dataset_key, _ = _BENCHMARK_DATASETS[commodity]
    except KeyError as exc:
        raise KeyError(f"{commodity} is not a trusted soy benchmark dataset") from exc
    return _CONTRACT_BY_KEY[dataset_key]


# ---------------------------------------------------------------------------
# Raw artifacts and replay
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProviderBar:
    """One provider daily bar for one named contract.

    ``close`` is the headline value; open/high/low/volume are the candle it
    came off and may be absent, because a provider that omits them is a
    different fact from a provider that reports zero.
    """

    symbol: str
    session_date: date
    close: Decimal
    open: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None
    volume: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", str(self.symbol).strip().upper())
        if not self.symbol:
            raise ValueError("provider bar requires a symbol")
        if not isinstance(self.session_date, date) or isinstance(self.session_date, datetime):
            raise ValueError("provider bar session_date must be a date")
        for field_name in ("close", "open", "high", "low", "volume"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _decimal(value, f"provider_bar.{field_name}"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "session_date": self.session_date.isoformat(),
            "close": _decimal_text(self.close),
            "open": _decimal_text(self.open),
            "high": _decimal_text(self.high),
            "low": _decimal_text(self.low),
            "volume": _decimal_text(self.volume),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ProviderBar:
        try:
            return cls(
                symbol=str(data["symbol"]),
                session_date=date.fromisoformat(str(data["session_date"])),
                close=_decimal(data["close"], "provider_bar.close"),
                open=_optional_decimal(data.get("open"), "provider_bar.open"),
                high=_optional_decimal(data.get("high"), "provider_bar.high"),
                low=_optional_decimal(data.get("low"), "provider_bar.low"),
                volume=_optional_decimal(data.get("volume"), "provider_bar.volume"),
            )
        except (KeyError, ValueError, InvalidOperation) as exc:
            raise BenchmarkShapeError(f"CBOT benchmarks: provider bar is malformed: {exc}") from exc


@dataclass(frozen=True)
class BenchmarkArtifactReplay:
    """A captured provider payload that can be parsed without another request."""

    commodity: str
    artifact: RawArtifact
    content: bytes

    def __post_init__(self) -> None:
        digest = hashlib.sha256(self.content).hexdigest()
        if digest != self.artifact.reference.content_hash:
            raise ValueError("replay content does not match raw artifact hash")
        if self.artifact.byte_size != len(self.content):
            raise ValueError("replay content size does not match raw artifact metadata")
        if self.commodity not in _BENCHMARK_DATASETS:
            raise ValueError(f"{self.commodity} is not a trusted soy benchmark dataset")


def artifact_from_bars(
    commodity: str,
    bars: Sequence[ProviderBar],
    *,
    retrieved_at: datetime | None = None,
    response_status: int = 200,
) -> BenchmarkArtifactReplay:
    """Capture provider bars as a metadata-only raw artifact plus replay bytes.

    Metadata-only because the Yahoo rights position records ``raw-content
    retention: prohibited``. The content hash is still computed, so a replay
    can be proved to be the payload the revisions were parsed from without the
    bytes ever entering the repository.
    """

    contract = contract_for(commodity)
    dataset = contract.dataset
    content = _canonical_content(commodity, bars)
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
        retrieval_url=RETRIEVAL_URL_TEMPLATE.format(commodity=commodity.replace(" ", "-").lower()),
        retrieved_at=Timestamp(_utc(retrieved_at or datetime.now(timezone.utc))),
        response_status=response_status,
        byte_size=len(content),
        content=None,
    )
    return BenchmarkArtifactReplay(commodity=commodity, artifact=artifact, content=content)


def bars_from_replay(replay: BenchmarkArtifactReplay) -> tuple[ProviderBar, ...]:
    """Decode replay bytes, hard-failing on anything but the captured shape."""

    try:
        payload = json.loads(replay.content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BenchmarkShapeError(f"CBOT benchmarks: raw artifact is not valid JSON: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise BenchmarkShapeError("CBOT benchmarks: artifact root is not a JSON object")
    if payload.get("commodity") != replay.commodity:
        raise BenchmarkShapeError("CBOT benchmarks: artifact commodity does not match the replay")
    raw_bars = payload.get("bars")
    if not isinstance(raw_bars, list):
        raise BenchmarkShapeError("CBOT benchmarks: artifact 'bars' is not a list")
    return tuple(ProviderBar.from_dict(bar) for bar in raw_bars)


def fetch_cbot_benchmark_artifact(
    commodity: str,
    *,
    today: date | None = None,
    download=None,
    retrieved_at: datetime | None = None,
) -> BenchmarkArtifactReplay:
    """Fetch one commodity's named-contract bars and capture them as an artifact.

    The ticker set comes from ``fetchers/forward_curve.py``'s own builder, not
    a second copy of the month rules: the trusted path must ask the provider
    for exactly what v1 asks for, or a reconciliation difference could mean
    "different contracts" rather than "different parse".
    """

    from fetchers.yfinance import fetch_one  # imported late so tests need no network stack

    contract_for(commodity)  # reject an unregistered commodity before any request
    spec = spec_for(commodity)
    download = download or fetch_one
    tickers = _build_contract_tickers(
        root=spec.root,
        exchange=spec.provider_suffix.lstrip(".") or "CBT",
        trading_months=_trading_months(commodity),
        num_contracts=CURVE_CONTRACT_COUNT,
        today=today,
    )
    bars: list[ProviderBar] = []
    for entry in tickers:
        frame = download(entry["ticker"], period="5d")
        bar = _bar_from_frame(entry["ticker"], frame)
        if bar is not None:
            bars.append(bar)
    return artifact_from_bars(commodity, bars, retrieved_at=retrieved_at)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ParsedBenchmarkPayload:
    """Candidates that resolved to a contract, plus the rows that did not."""

    candidates: tuple[CandidateObservation, ...]
    rejected_rows: tuple[tuple[str, str], ...] = ()  # (symbol, reason)


def parse_benchmark_candidates(
    replay: BenchmarkArtifactReplay,
    *,
    parsed_at: datetime | None = None,
    parser_version: str = PARSER_VERSION,
    now: datetime | None = None,
) -> ParsedBenchmarkPayload:
    """Turn replayed provider bars into trusted candidate observations."""

    bars = bars_from_replay(replay)
    parsed_instant = _utc(parsed_at or replay.artifact.retrieved_at.value)
    evaluated_at = _utc(now or parsed_instant)
    contract = contract_for(replay.commodity)
    _, product_form = _BENCHMARK_DATASETS[replay.commodity]
    expected_root = spec_for(replay.commodity).root

    candidates: list[CandidateObservation] = []
    rejected: list[tuple[str, str]] = []
    for bar in bars:
        try:
            named = parse_symbol(bar.symbol)
        except (UnknownContract, ValueError) as exc:
            rejected.append((bar.symbol, f"symbol does not resolve to a named contract: {exc}"))
            continue
        if named.spec.root != expected_root:
            rejected.append(
                (bar.symbol, f"symbol belongs to {named.spec.root}, not this dataset's {expected_root}")
            )
            continue
        identity = ObservationIdentity(
            source_id=contract.dataset.source_id,
            dataset_id=contract.dataset.dataset_id,
            dataset_key=contract.dataset.key,
            commodity=str(contract.identity.fixed_fields["commodity"]) if contract.identity else "",
            product_form=product_form,
            venue="cbot",
            price_type="delayed-close",
            currency="USD",
            unit=contract.units[0],
            effective_date=bar.session_date,
            contract=ContractIdentity(**named.trust_identity()),
            source_record_id=bar.symbol,
        )
        candidates.append(
            CandidateObservation(
                identity=identity,
                value=bar.close,
                artifact=replay.artifact.reference,
                parser_version=parser_version,
                parsed_at=parsed_instant,
                # The session the bar belongs to is genuinely observed by the
                # provider; the *publication* time is not published at all, so
                # it is left absent rather than inferred from fetch time.
                observed_at=Timestamp(_session_instant(bar.session_date), inferred=True),
                settlement_state=settlement_state_for(bar.session_date, evaluated_at),
                open_value=bar.open,
                high_value=bar.high,
                low_value=bar.low,
                close_value=bar.close,
                volume=bar.volume,
            )
        )
    return ParsedBenchmarkPayload(candidates=tuple(candidates), rejected_rows=tuple(rejected))


def settlement_state_for(session_date: date, now: datetime) -> SettlementState:
    """Whether ``session_date``'s bar is a finished session at ``now``.

    Time-based, like the v1 settlement guard and for the same reason: no field
    on a provider bar says "settled", and a heuristic on volume or range would
    be the silent guess the guard exists to remove.
    """

    venue_today = _utc(now).astimezone(_venue_zone()).date()
    if session_date > venue_today:
        return SettlementState.PRE_OPEN
    if session_date < venue_today:
        return SettlementState.SETTLED
    return SettlementState.SETTLED if session_is_settled(now) else SettlementState.OPEN


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BenchmarkDatasetIngestion:
    """What one commodity's dataset produced in one run."""

    commodity: str
    dataset_id: str
    artifact_id: str
    candidate_count: int
    accepted_revision_ids: tuple[str, ...]
    quarantined_revision_ids: tuple[str, ...]
    rejected_revision_ids: tuple[str, ...]
    finding_ids: tuple[str, ...]
    result: DatasetResult


@dataclass(frozen=True)
class BenchmarkTrustedIngestion:
    """One trusted collection cycle across every benchmark dataset."""

    run_id: str
    status: RunStatus
    datasets: tuple[BenchmarkDatasetIngestion, ...]
    error: str | None = None

    def dataset(self, commodity: str) -> BenchmarkDatasetIngestion:
        for item in self.datasets:
            if item.commodity == commodity:
                return item
        raise KeyError(commodity)

    @property
    def accepted_revision_ids(self) -> tuple[str, ...]:
        return tuple(sorted(rid for item in self.datasets for rid in item.accepted_revision_ids))

    @property
    def quarantined_revision_ids(self) -> tuple[str, ...]:
        return tuple(sorted(rid for item in self.datasets for rid in item.quarantined_revision_ids))


def ingest_cbot_benchmark_replays(
    repository: TrustRepository,
    replays: Sequence[BenchmarkArtifactReplay],
    *,
    ingested_at: datetime | None = None,
    parser_version: str = PARSER_VERSION,
    holidays: Sequence[date] = (),
) -> BenchmarkTrustedIngestion:
    """Persist replayed provider payloads as validated observation revisions.

    One run covers every replay handed in, because one run is one attempted
    collection cycle and the three soy legs are collected together. Findings
    and revisions are written whatever the disposition: a rejected candle and a
    quarantined move are both durable, and only their quality state differs.
    """

    repository.initialize()
    now = _utc(ingested_at or datetime.now(timezone.utc))
    provisional = _run(now, RunStatus.RUNNING, {}, ended_at=None)
    run_id = provisional.run_id

    parser_versions: dict[str, str] = {}
    findings: list[Finding] = []
    ingestions: list[BenchmarkDatasetIngestion] = []

    for replay in replays:
        contract = contract_for(replay.commodity)
        parser_versions[contract.dataset.dataset_id] = parser_version
        artifact_ref = repository.store_artifact(replay.artifact, contract)
        record_findings: list[Finding] = []
        try:
            parsed = parse_benchmark_candidates(
                replay, parsed_at=now, parser_version=parser_version, now=now
            )
        except BenchmarkShapeError as exc:
            finding = _dataset_finding(
                run_id,
                contract,
                subject_id=artifact_ref.artifact_id,
                rule_id="benchmark.shape",
                severity=FindingSeverity.REJECT,
                evidence={"artifact_id": artifact_ref.artifact_id, "error": str(exc)},
                message=str(exc),
            )
            repository.store(finding)
            findings.append(finding)
            ingestions.append(
                _dataset_ingestion(
                    repository,
                    replay.commodity,
                    contract,
                    artifact_ref,
                    run_id=run_id,
                    candidate_count=0,
                    revisions=(),
                    health_findings=(finding,),
                    record_findings=(finding,),
                    now=now,
                    holidays=holidays,
                    external_error=str(exc),
                )
            )
            continue

        for symbol, reason in parsed.rejected_rows:
            finding = _dataset_finding(
                run_id,
                contract,
                subject_id=f"{artifact_ref.artifact_id}:{symbol}",
                rule_id="contract.symbol",
                severity=FindingSeverity.REJECT,
                evidence={"symbol": symbol, "reason": reason},
                message=f"Provider row {symbol} is not a contract of this dataset's product",
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
                    ingested_at=now,
                    quality_state=evaluation.disposition,
                    public_eligible=_public_eligible(contract),
                    artifact=artifact_ref,
                    parser_version=parser_version,
                    observed_at=candidate.observed_at,
                    settlement_state=candidate.settlement_state,
                    open_value=candidate.open_value,
                    high_value=candidate.high_value,
                    low_value=candidate.low_value,
                    close_value=candidate.close_value,
                    volume=candidate.volume,
                    finding_ids=evaluation.finding_ids,
                )
            )
        repository.append_observation_revisions(revisions)
        findings.extend(record_findings)
        ingestions.append(
            _dataset_ingestion(
                repository,
                replay.commodity,
                contract,
                artifact_ref,
                run_id=run_id,
                candidate_count=len(parsed.candidates),
                revisions=tuple(revisions),
                # Candidate-scope findings are resolved by their own
                # disposition and ride on the revisions; only dataset-scope
                # findings belong in the dataset result. See the module
                # docstring.
                health_findings=(),
                record_findings=tuple(record_findings),
                now=now,
                holidays=holidays,
            )
        )

    summary = _findings_summary(findings)
    status = (
        RunStatus.SUCCEEDED
        if ingestions and all(item.result.status is DatasetResultStatus.SUCCESS for item in ingestions)
        else RunStatus.FAILED
    )
    completed = _run(now, status, parser_versions, ended_at=now, findings_summary=summary)
    repository.store(completed)
    return BenchmarkTrustedIngestion(
        run_id=completed.run_id,
        status=status,
        datasets=tuple(ingestions),
    )


# ---------------------------------------------------------------------------
# Corrections
# ---------------------------------------------------------------------------


def append_benchmark_correction(
    repository: TrustRepository,
    identity: ObservationIdentity,
    *,
    corrected_value: Decimal | int | str,
    reason: str,
    correction_type: str = "operator-replacement",
    corrected_at: datetime | None = None,
    scope: EligibilityScope = EligibilityScope.INTERNAL,
) -> ObservationRevision:
    """Append a corrected revision that supersedes the current accepted one.

    The prior revision is never rewritten or deleted: it stays queryable, it
    stays linked from the correction, and any edition that pinned it can still
    be reproduced. That is what makes a correction different from an overwrite.
    """

    prior = repository.current_accepted_revision(identity, scope=scope)
    if prior is None:
        raise ValueError("cannot correct an observation with no accepted revision")
    revision = ObservationRevision(
        identity=identity,
        value=corrected_value,
        ingested_at=_utc(corrected_at or datetime.now(timezone.utc)),
        quality_state=QualityState.ACCEPTED,
        public_eligible=prior.public_eligible,
        artifact=prior.artifact,
        parser_version=prior.parser_version,
        observed_at=prior.observed_at,
        settlement_state=prior.settlement_state,
        supersedes_revision_id=prior.revision_id,
        correction_type=correction_type,
        correction_reason=reason,
    )
    repository.append_observation_revision(revision)
    return revision


# ---------------------------------------------------------------------------
# Trusted reads and reconciliation
# ---------------------------------------------------------------------------


def trusted_curve_frame(
    repository: TrustRepository,
    commodity: str,
    *,
    scope: EligibilityScope = EligibilityScope.INTERNAL,
    as_of: date | None = None,
) -> pd.DataFrame:
    """Accepted current revisions for one commodity, in the v1 curve shape.

    Only the newest session is returned, because a forward curve is a snapshot
    of one moment — the same rule the v1 fetcher applies at write time. Legs
    whose current revision is quarantined, rejected or superseded are simply
    absent; a shorter curve is honest, a curve padded with a refused leg is not.
    """

    contract = contract_for(commodity)
    columns = [
        "commodity",
        "contract_month",
        "label",
        "ticker",
        "close",
        "observation_date",
        "volume",
        "open_interest",
        "revision_id",
    ]
    heads = _accepted_heads(repository, contract, scope=scope, as_of=as_of)
    if not heads:
        return pd.DataFrame(columns=columns)
    session = max(revision.identity.effective_date for revision in heads)
    rows = []
    for revision in heads:
        if revision.identity.effective_date != session:
            continue
        identity = revision.identity
        assert identity.contract is not None  # the contract.named rule guarantees it
        delivery = date.fromisoformat(f"{identity.contract.delivery_month}-01")
        rows.append(
            {
                "commodity": commodity,
                "contract_month": delivery.isoformat(),
                "label": delivery.strftime("%b %Y"),
                "ticker": identity.source_record_id,
                "close": float(revision.value),
                "observation_date": identity.effective_date.isoformat(),
                "volume": None if revision.volume is None else float(revision.volume),
                "open_interest": None,
                "revision_id": revision.revision_id,
            }
        )
    frame = pd.DataFrame(rows, columns=columns)
    if frame.empty:
        return frame
    return frame.sort_values("contract_month").reset_index(drop=True)


def reconcile_cbot_benchmarks(legacy: pd.DataFrame, trusted: pd.DataFrame) -> ReconciliationReport:
    """Account for every row and field difference between v1 and the ledger."""

    return reconcile_frames(
        legacy,
        trusted,
        key_columns=("commodity", "contract_month", "ticker"),
        value_columns=("label", "close", "observation_date", "volume"),
        text_columns=("commodity", "contract_month", "label", "ticker", "observation_date"),
    )


@dataclass(frozen=True)
class BenchmarkDualWriteResult:
    ingestion: BenchmarkTrustedIngestion
    reconciliations: Mapping[str, ReconciliationReport]

    @property
    def reconciled(self) -> bool:
        return all(report.reconciled for report in self.reconciliations.values())


def dual_write_cbot_benchmarks(
    repository: TrustRepository,
    replays: Sequence[BenchmarkArtifactReplay],
    legacy_frames: Mapping[str, pd.DataFrame],
    *,
    ingested_at: datetime | None = None,
    parser_version: str = PARSER_VERSION,
) -> BenchmarkDualWriteResult:
    """Run trusted ingestion beside the v1 fetcher and compare both outputs."""

    ingestion = ingest_cbot_benchmark_replays(
        repository, replays, ingested_at=ingested_at, parser_version=parser_version
    )
    reconciliations = {
        replay.commodity: reconcile_cbot_benchmarks(
            legacy_frames.get(replay.commodity, pd.DataFrame()),
            trusted_curve_frame(repository, replay.commodity),
        )
        for replay in replays
    }
    return BenchmarkDualWriteResult(ingestion=ingestion, reconciliations=reconciliations)


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _dataset_ingestion(
    repository: TrustRepository,
    commodity: str,
    contract: DatasetContract,
    artifact_ref: ArtifactReference,
    *,
    run_id: str,
    candidate_count: int,
    revisions: tuple[ObservationRevision, ...],
    health_findings: tuple[Finding, ...],
    record_findings: tuple[Finding, ...],
    now: datetime,
    holidays: Sequence[date],
    external_error: str | None = None,
) -> BenchmarkDatasetIngestion:
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
            # anything published. Health is an *ingestion* verdict, and grading
            # this dataset's coverage as zero would blame the source for a
            # rights answer nobody has recorded yet.
            eligibility_scope=EligibilityScope.INTERNAL,
        )
    )
    return BenchmarkDatasetIngestion(
        commodity=commodity,
        dataset_id=contract.dataset.dataset_id,
        artifact_id=artifact_ref.artifact_id,
        candidate_count=candidate_count,
        accepted_revision_ids=_ids(revisions, QualityState.ACCEPTED),
        quarantined_revision_ids=_ids(revisions, QualityState.QUARANTINED),
        rejected_revision_ids=_ids(revisions, QualityState.REJECTED),
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
    as_of: date | None = None,
) -> tuple[ObservationRevision, ...]:
    """The current accepted revision of every observation in this dataset.

    Resolved through ``current_accepted_revision`` rather than by filtering the
    ledger here, so supersession and eligibility are decided in exactly one
    place and a corrected value cannot be read back alongside the value it
    replaced.
    """

    identities: dict[str, ObservationIdentity] = {}
    for revision in _dataset_revisions(repository, contract):
        if as_of is not None and revision.identity.effective_date > as_of:
            continue
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
    """The accepted value a candidate is judged against, keyed by candidate id.

    The comparison is made against the same *contract*, not the same commodity:
    a bean July and a bean November are different instruments, and the spread
    between them is not a day's move.

    Two things can be "the previous value", and the same-session one is the
    sharper case. If this observation already has an accepted revision — the
    provider re-printed the session, or an operator is about to — the candidate
    is judged against *that*, because a re-print disagreeing with the accepted
    close by a third is precisely the change that must not land silently.
    Failing that, it is judged against the most recent accepted earlier
    session. A candidate with neither has nothing to move from and is not move
    checked, which is right: the first observation of a contract is not an
    anomaly.
    """

    by_session: dict[tuple[str, str], tuple[date, datetime, Decimal]] = {}
    by_observation: dict[str, tuple[datetime, Decimal]] = {}
    for revision in _dataset_revisions(repository, contract):
        if revision.quality_state is not QualityState.ACCEPTED:
            continue
        identity = revision.identity
        if identity.contract is None:
            continue
        value = Decimal(str(revision.value))
        seen_observation = by_observation.get(identity.observation_id)
        if seen_observation is None or revision.ingested_at >= seen_observation[0]:
            by_observation[identity.observation_id] = (revision.ingested_at, value)
        key = (identity.contract.code, identity.contract.delivery_month)
        seen = by_session.get(key)
        if seen is None or (identity.effective_date, revision.ingested_at) > (seen[0], seen[1]):
            by_session[key] = (identity.effective_date, revision.ingested_at, value)

    previous: dict[str, Decimal] = {}
    for candidate in candidates:
        identity = candidate.identity
        if identity.contract is None:
            continue
        same_observation = by_observation.get(identity.observation_id)
        if same_observation is not None:
            previous[candidate.candidate_id] = same_observation[1]
            continue
        seen = by_session.get((identity.contract.code, identity.contract.delivery_month))
        if seen is not None and seen[0] < identity.effective_date:
            previous[candidate.candidate_id] = seen[2]
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


def _canonical_content(commodity: str, bars: Sequence[ProviderBar]) -> bytes:
    payload = {
        "commodity": commodity,
        "provider": "yahoo-finance",
        "bars": [bar.to_dict() for bar in sorted(bars, key=lambda bar: bar.symbol)],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _bar_from_frame(ticker: str, frame: pd.DataFrame) -> ProviderBar | None:
    if frame is None or getattr(frame, "empty", True):
        # Not yet listed, or expired and delisted by the provider. Both are
        # absences of a contract, not absences of a price.
        return None
    last = frame.iloc[-1]
    close = last.get("Close")
    if close is None or pd.isna(close):
        return None
    stamp = pd.Timestamp(frame.index[-1])
    if pd.isna(stamp):
        raise BenchmarkShapeError(f"{ticker}: provider frame has an undated last bar")
    return ProviderBar(
        symbol=ticker,
        session_date=stamp.date(),
        close=_decimal(float(close), "provider_bar.close"),
        open=_frame_decimal(last, "Open"),
        high=_frame_decimal(last, "High"),
        low=_frame_decimal(last, "Low"),
        volume=_frame_decimal(last, "Volume"),
    )


def _frame_decimal(row, column: str) -> Decimal | None:
    value = row.get(column)
    if value is None or pd.isna(value):
        return None
    return _decimal(float(value), f"provider_bar.{column.lower()}")


def _trading_months(commodity: str) -> list[int]:
    from config import FORWARD_CURVE_CONTRACTS

    months = cast(Sequence[int], FORWARD_CURVE_CONTRACTS[commodity]["months"])
    return list(months)


def _venue_zone():
    from zoneinfo import ZoneInfo

    return ZoneInfo(SETTLEMENT_TIMEZONE)


def _session_instant(session_date: date) -> datetime:
    return datetime.combine(session_date, datetime.min.time(), tzinfo=timezone.utc)


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
    "BENCHMARK_COMMODITIES",
    "BenchmarkArtifactReplay",
    "BenchmarkDatasetIngestion",
    "BenchmarkDualWriteResult",
    "BenchmarkShapeError",
    "BenchmarkTrustedIngestion",
    "DAILY_MOVE_QUARANTINE_THRESHOLD",
    "PARSER_VERSION",
    "ParsedBenchmarkPayload",
    "ProviderBar",
    "append_benchmark_correction",
    "artifact_from_bars",
    "bars_from_replay",
    "contract_for",
    "dual_write_cbot_benchmarks",
    "fetch_cbot_benchmark_artifact",
    "ingest_cbot_benchmark_replays",
    "parse_benchmark_candidates",
    "reconcile_cbot_benchmarks",
    "settlement_state_for",
    "trusted_curve_frame",
]
