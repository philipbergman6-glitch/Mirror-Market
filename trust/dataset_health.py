"""Whole-dataset coverage and freshness evaluation for trusted ingestion."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from trust.domain import (
    ArtifactReference,
    DatasetResult,
    DatasetResultStatus,
    Finding,
    FindingSeverity,
    FreshnessState,
    ObservationIdentity,
    ObservationRevision,
    QualityState,
    ValidationPolicy,
)
from trust.registry import CadenceKind, DatasetContract


@dataclass(frozen=True)
class DatasetHealthInput:
    """Inputs needed to evaluate a dataset result without source IO."""

    run_id: str
    contract: DatasetContract
    candidate_count: int
    accepted_revisions: tuple[ObservationRevision, ...]
    now: datetime
    findings: tuple[Finding, ...] = ()
    artifacts: tuple[ArtifactReference, ...] = ()
    external_error: str | None = None
    last_known_good_as_of: date | None = None
    holidays: tuple[date, ...] = ()


def evaluate_dataset_health(input: DatasetHealthInput) -> DatasetResult:
    """Return a complete dataset result after coverage and freshness checks.

    Forecast-dated revisions are ignored for coverage and freshness. The
    evaluator only exposes accepted revision IDs when the whole dataset is
    current and meets the contract coverage threshold.
    """

    contract = input.contract
    _require_complete_contract(contract)
    assert contract.coverage is not None
    assert contract.freshness is not None
    assert contract.validation is not None

    if input.external_error is not None:
        return DatasetResult(
            run_id=input.run_id,
            dataset_id=contract.dataset.dataset_id,
            status=DatasetResultStatus.EXTERNAL_FAILURE,
            candidate_count=input.candidate_count,
            accepted_revision_ids=(),
            artifact_references=input.artifacts,
            findings=input.findings,
            coverage=Decimal("0"),
            freshness=FreshnessState.UNAVAILABLE,
            eligible=False,
            validation_policy=contract.validation,
            as_of_date=input.last_known_good_as_of,
            error=input.external_error,
        )

    evaluation_date = input.now.astimezone(ZoneInfo(contract.cadence.timezone)).date()
    observable = tuple(
        revision
        for revision in input.accepted_revisions
        if _revision_is_accepted_for_dataset(revision, contract)
        and not revision.identity.effective_date > evaluation_date
    )
    latest_as_of = max((revision.identity.effective_date for revision in observable), default=None)
    current_revisions = tuple(
        revision
        for revision in observable
        if latest_as_of is not None and revision.identity.effective_date == latest_as_of
    )
    observed_keys = {_coverage_key(revision.identity, contract.coverage.key_fields) for revision in current_revisions}
    expected_keys = set(contract.coverage.expected_keys)
    coverage = Decimal(len(observed_keys & expected_keys)) / Decimal(len(expected_keys))

    artifacts = _artifact_references(input.artifacts, current_revisions)
    freshness = _freshness_state(contract, latest_as_of, input.now, input.holidays)
    findings = tuple(input.findings)

    if (
        input.candidate_count == 0
        and not current_revisions
        and _publication_is_quiet(contract, input.now, input.holidays)
    ):
        return DatasetResult(
            run_id=input.run_id,
            dataset_id=contract.dataset.dataset_id,
            status=DatasetResultStatus.LEGITIMATE_EMPTY,
            candidate_count=0,
            accepted_revision_ids=(),
            artifact_references=artifacts,
            findings=findings,
            coverage=coverage,
            freshness=FreshnessState.CURRENT,
            eligible=False,
            validation_policy=ValidationPolicy(
                contract.validation.rule_ids,
                allows_empty_publication=True,
            ),
            as_of_date=latest_as_of,
        )

    if coverage < contract.coverage.minimum_coverage:
        finding = _finding(
            input.run_id,
            contract,
            "coverage.minimum",
            FindingSeverity.REJECT,
            {
                "coverage": str(coverage),
                "minimum_coverage": str(contract.coverage.minimum_coverage),
                "observed_keys": sorted(observed_keys),
                "missing_keys": sorted(expected_keys - observed_keys),
                "as_of_date": latest_as_of,
            },
            "Dataset coverage is below the contract minimum",
        )
        return DatasetResult(
            run_id=input.run_id,
            dataset_id=contract.dataset.dataset_id,
            status=DatasetResultStatus.CONTRACT_FAILURE,
            candidate_count=input.candidate_count,
            accepted_revision_ids=(),
            artifact_references=artifacts,
            findings=(*findings, finding),
            coverage=coverage,
            freshness=freshness,
            eligible=False,
            validation_policy=contract.validation,
            as_of_date=latest_as_of or input.last_known_good_as_of,
            error="coverage contract failed",
        )

    if freshness is not FreshnessState.CURRENT:
        finding = _finding(
            input.run_id,
            contract,
            "freshness.stale",
            FindingSeverity.QUARANTINE,
            {
                "as_of_date": latest_as_of,
                "stale_after_hours": contract.freshness.stale_after_hours,
            },
            "Dataset is stale against the freshness contract",
        )
        return DatasetResult(
            run_id=input.run_id,
            dataset_id=contract.dataset.dataset_id,
            status=DatasetResultStatus.QUARANTINED,
            candidate_count=input.candidate_count,
            accepted_revision_ids=(),
            artifact_references=artifacts,
            findings=(*findings, finding),
            coverage=coverage,
            freshness=freshness,
            eligible=False,
            validation_policy=contract.validation,
            as_of_date=latest_as_of or input.last_known_good_as_of,
        )

    return DatasetResult(
        run_id=input.run_id,
        dataset_id=contract.dataset.dataset_id,
        status=DatasetResultStatus.SUCCESS,
        candidate_count=input.candidate_count,
        accepted_revision_ids=tuple(revision.revision_id for revision in current_revisions),
        artifact_references=artifacts,
        findings=findings,
        coverage=coverage,
        freshness=FreshnessState.CURRENT,
        eligible=True,
        validation_policy=contract.validation,
        as_of_date=latest_as_of,
    )


def last_expected_publication_date(
    contract: DatasetContract,
    now: datetime,
    *,
    holidays: Iterable[date] = (),
) -> date | None:
    """Return the most recent contract publication date at or before ``now``."""

    if contract.cadence is None:
        raise ValueError("dataset health evaluation requires a cadence contract")
    local_date = now.astimezone(ZoneInfo(contract.cadence.timezone)).date()
    holiday_set = set(holidays)
    if contract.cadence.kind is CadenceKind.EVENT_DRIVEN:
        return None
    for offset in range(370):
        candidate = local_date - timedelta(days=offset)
        if candidate in holiday_set:
            continue
        if _matches_publication_calendar(contract, candidate):
            return candidate
    return None


def _require_complete_contract(contract: DatasetContract) -> None:
    missing = [
        name
        for name in ("cadence", "coverage", "freshness", "validation")
        if getattr(contract, name) is None
    ]
    if missing:
        raise ValueError(f"dataset health evaluation requires contract fields: {', '.join(missing)}")


def _revision_is_accepted_for_dataset(revision: ObservationRevision, contract: DatasetContract) -> bool:
    return (
        revision.identity.dataset_id == contract.dataset.dataset_id
        and revision.quality_state is QualityState.ACCEPTED
        and revision.public_eligible
    )


def _coverage_key(identity: ObservationIdentity, key_fields: tuple[str, ...]) -> str:
    values: list[str] = []
    identity_dict = identity.identity_dict()
    for field_name in key_fields:
        value = identity_dict[field_name]
        if isinstance(value, Mapping):
            values.append(":".join(str(value[key]) for key in sorted(value)))
        else:
            values.append(str(value))
    return ":".join(values).lower()


def _artifact_references(
    explicit: tuple[ArtifactReference, ...],
    revisions: tuple[ObservationRevision, ...],
) -> tuple[ArtifactReference, ...]:
    by_id = {artifact.artifact_id: artifact for artifact in explicit}
    for revision in revisions:
        if revision.artifact is not None:
            by_id[revision.artifact.artifact_id] = revision.artifact
    return tuple(sorted(by_id.values(), key=lambda artifact: artifact.artifact_id))


def _freshness_state(
    contract: DatasetContract,
    as_of_date: date | None,
    now: datetime,
    holidays: tuple[date, ...],
) -> FreshnessState:
    if as_of_date is None:
        return FreshnessState.UNAVAILABLE
    expected = last_expected_publication_date(contract, now, holidays=holidays)
    if expected is not None and as_of_date >= expected:
        return FreshnessState.CURRENT
    local_now = now.astimezone(ZoneInfo(contract.cadence.timezone))
    age_hours = (local_now.date() - as_of_date).days * 24
    return FreshnessState.CURRENT if age_hours <= contract.freshness.stale_after_hours else FreshnessState.STALE


def _publication_is_quiet(
    contract: DatasetContract,
    now: datetime,
    holidays: tuple[date, ...],
) -> bool:
    local_date = now.astimezone(ZoneInfo(contract.cadence.timezone)).date()
    return local_date in set(holidays) or not _matches_publication_calendar(contract, local_date)


def _matches_publication_calendar(contract: DatasetContract, candidate: date) -> bool:
    cadence = contract.cadence
    if cadence is None:
        return False
    if cadence.kind in {CadenceKind.BUSINESS_DAILY, CadenceKind.WEEKLY}:
        return cadence.publication_weekdays is not None and candidate.weekday() in cadence.publication_weekdays
    if cadence.kind is CadenceKind.MONTHLY:
        return cadence.publication_days_of_month is not None and candidate.day in cadence.publication_days_of_month
    return False


def _finding(
    run_id: str,
    contract: DatasetContract,
    rule_id: str,
    severity: FindingSeverity,
    evidence: Mapping[str, object],
    message: str,
) -> Finding:
    return Finding(
        run_id=run_id,
        dataset_id=contract.dataset.dataset_id,
        subject_id="dataset",
        rule_id=rule_id,
        rule_version="1.0.0",
        severity=severity,
        evidence=evidence,
        message=message,
    )
