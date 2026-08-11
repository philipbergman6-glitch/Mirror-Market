"""DT-13 tests for dataset-level coverage and freshness evaluation."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from trust import (
    MAGYP_FOB_CONTRACT,
    ArtifactReference,
    CadenceContract,
    CadenceKind,
    DatasetHealthInput,
    DatasetResultStatus,
    FindingSeverity,
    FreshnessContract,
    FreshnessState,
    ObservationIdentity,
    ObservationRevision,
    QualityState,
    Run,
    RunStatus,
    evaluate_dataset_health,
    last_expected_publication_date,
)

NOW = datetime(2026, 8, 10, 18, 0, tzinfo=timezone.utc)


def _run_id() -> str:
    return Run(
        code_revision="82e3cbc578023efdafb87ed1e09c5ea8b357eca5",
        started_at=NOW - timedelta(minutes=2),
        ended_at=NOW,
        status=RunStatus.SUCCEEDED,
    ).run_id


def _artifact() -> ArtifactReference:
    dataset = MAGYP_FOB_CONTRACT.dataset
    return ArtifactReference(
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        dataset_key=dataset.key,
        content_hash=hashlib.sha256(b"magyp-fob").hexdigest(),
        content_retained=False,
        media_type="application/json",
    )


def _revision(product_form: str, *, effective_date: date = date(2026, 8, 10)) -> ObservationRevision:
    dataset = MAGYP_FOB_CONTRACT.dataset
    identity = ObservationIdentity(
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        dataset_key=dataset.key,
        commodity="soybean",
        product_form=product_form,
        location="argentina-up-river",
        price_type="official-fob",
        currency="USD",
        unit="usd-mt",
        effective_date=effective_date,
        source_record_id=f"{effective_date.isoformat()}-{product_form}",
    )
    return ObservationRevision(
        identity=identity,
        value=Decimal("499.50"),
        ingested_at=NOW,
        quality_state=QualityState.ACCEPTED,
        public_eligible=True,
        artifact=_artifact(),
        parser_version="magyp-fob/1.0.0",
    )


def _health_input(
    *,
    revisions: tuple[ObservationRevision, ...],
    now: datetime = NOW,
    candidate_count: int | None = None,
    external_error: str | None = None,
    holidays: tuple[date, ...] = (),
) -> DatasetHealthInput:
    return DatasetHealthInput(
        run_id=_run_id(),
        contract=MAGYP_FOB_CONTRACT,
        candidate_count=len(revisions) if candidate_count is None else candidate_count,
        accepted_revisions=revisions,
        now=now,
        external_error=external_error,
        last_known_good_as_of=date(2026, 8, 8),
        holidays=holidays,
    )


def test_complete_dataset_is_current_and_edition_eligible() -> None:
    revisions = (_revision("beans"), _revision("meal"), _revision("oil"))

    result = evaluate_dataset_health(_health_input(revisions=revisions))

    assert result.status is DatasetResultStatus.SUCCESS
    assert result.coverage == Decimal("1")
    assert result.freshness is FreshnessState.CURRENT
    assert result.eligible is True
    assert result.as_of_date == date(2026, 8, 10)
    assert result.accepted_revision_ids == tuple(sorted(revision.revision_id for revision in revisions))


def test_partial_dataset_fails_coverage_before_revisions_can_leak_to_edition() -> None:
    result = evaluate_dataset_health(_health_input(revisions=(_revision("beans"), _revision("meal"))))

    assert result.status is DatasetResultStatus.CONTRACT_FAILURE
    assert result.coverage == Decimal("0.6666666666666666666666666667")
    assert result.eligible is False
    assert result.accepted_revision_ids == ()
    assert result.findings[0].rule_id == "coverage.minimum"
    assert result.findings[0].severity is FindingSeverity.REJECT
    assert result.findings[0].evidence["missing_keys"] == ("soybean:oil",)


def test_stale_dataset_returns_last_known_good_as_of_without_exposing_revisions() -> None:
    stale_date = date(2026, 8, 5)
    revisions = (
        _revision("beans", effective_date=stale_date),
        _revision("meal", effective_date=stale_date),
        _revision("oil", effective_date=stale_date),
    )

    result = evaluate_dataset_health(_health_input(revisions=revisions))

    assert result.status is DatasetResultStatus.QUARANTINED
    assert result.coverage == Decimal("1")
    assert result.freshness is FreshnessState.STALE
    assert result.eligible is False
    assert result.accepted_revision_ids == ()
    assert result.as_of_date == stale_date
    assert result.findings[0].rule_id == "freshness.stale"


def test_legitimate_holiday_empty_publication_is_not_source_failure() -> None:
    result = evaluate_dataset_health(
        _health_input(
            revisions=(),
            candidate_count=0,
            now=datetime(2026, 8, 10, 18, 0, tzinfo=timezone.utc),
            holidays=(date(2026, 8, 10),),
        )
    )

    assert result.status is DatasetResultStatus.LEGITIMATE_EMPTY
    assert result.freshness is FreshnessState.CURRENT
    assert result.eligible is False
    assert result.accepted_revision_ids == ()


def test_failed_dataset_preserves_explicit_last_known_good_date() -> None:
    result = evaluate_dataset_health(
        _health_input(
            revisions=(),
            candidate_count=0,
            external_error="source timed out",
        )
    )

    assert result.status is DatasetResultStatus.EXTERNAL_FAILURE
    assert result.freshness is FreshnessState.UNAVAILABLE
    assert result.as_of_date == date(2026, 8, 8)
    assert result.error == "source timed out"


def test_weekly_and_monthly_publication_calendars_come_from_contract() -> None:
    weekly = replace(
        MAGYP_FOB_CONTRACT,
        cadence=CadenceContract(
            CadenceKind.WEEKLY,
            168,
            "UTC",
            publication_weekdays=(2,),
        ),
        freshness=FreshnessContract(stale_after_hours=240),
    )
    monthly = replace(
        MAGYP_FOB_CONTRACT,
        cadence=CadenceContract(
            CadenceKind.MONTHLY,
            720,
            "UTC",
            publication_days_of_month=(10,),
        ),
        freshness=FreshnessContract(stale_after_hours=960),
    )

    assert last_expected_publication_date(
        weekly,
        datetime(2026, 8, 14, 12, tzinfo=timezone.utc),
    ) == date(2026, 8, 12)
    assert last_expected_publication_date(
        monthly,
        datetime(2026, 8, 31, 12, tzinfo=timezone.utc),
    ) == date(2026, 8, 10)


def test_forecast_rows_cannot_make_observed_history_look_current() -> None:
    old_date = date(2026, 8, 5)
    future_date = date(2026, 8, 20)
    revisions = (
        _revision("beans", effective_date=old_date),
        _revision("meal", effective_date=old_date),
        _revision("oil", effective_date=old_date),
        _revision("beans", effective_date=future_date),
        _revision("meal", effective_date=future_date),
        _revision("oil", effective_date=future_date),
    )

    result = evaluate_dataset_health(_health_input(revisions=revisions))

    assert result.status is DatasetResultStatus.QUARANTINED
    assert result.freshness is FreshnessState.STALE
    assert result.as_of_date == old_date
    assert result.accepted_revision_ids == ()
