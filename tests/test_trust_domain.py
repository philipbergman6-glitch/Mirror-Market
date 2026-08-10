"""DT-01 contract tests for the Data Trust Foundation vocabulary."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from trust import (
    ArtifactReference,
    CandidateObservation,
    ContractIdentity,
    Correction,
    CorrectionDecision,
    Dataset,
    DatasetResult,
    DatasetResultStatus,
    DeliveryWindow,
    Edition,
    EditionStatus,
    Finding,
    FindingSeverity,
    FreshnessState,
    ObservationIdentity,
    ObservationRevision,
    Promotion,
    QualityState,
    RawArtifact,
    Run,
    RunStatus,
    Source,
    Timestamp,
    ValidationPolicy,
    evaluate_run_status,
)

NOW = datetime(2026, 8, 10, 12, 30, tzinfo=timezone.utc)
ARTIFACT_CONTENT = b'{"prices":[]}'
HASH_A = hashlib.sha256(ARTIFACT_CONTENT).hexdigest()
HASH_B = "b" * 64


@pytest.fixture
def values() -> dict[str, object]:
    source = Source(key="magyp", name="Argentina MAGyP", attribution="MAGyP")
    dataset = Dataset(source_id=source.source_id, key="official-fob", name="Official FOB prices")
    artifact = ArtifactReference(
        source_id=source.source_id,
        dataset_id=dataset.dataset_id,
        dataset_key=dataset.key,
        content_hash=HASH_A,
        content_retained=True,
        media_type="application/json",
    )
    raw_artifact = RawArtifact(
        reference=artifact,
        retrieval_url="https://example.test/magyp-fob",
        retrieved_at=Timestamp(NOW - timedelta(minutes=1)),
        response_status=200,
        byte_size=len(ARTIFACT_CONTENT),
        content=ARTIFACT_CONTENT,
    )
    identity = ObservationIdentity(
        source_id=source.source_id,
        dataset_id=dataset.dataset_id,
        dataset_key=dataset.key,
        commodity="soybean",
        product_form="meal",
        location="up-river",
        price_type="fob",
        currency="USD",
        unit="usd-mt",
        delivery_window=DeliveryWindow(date(2026, 8, 15), date(2026, 9, 15), "Aug/Sep"),
        effective_date=date(2026, 8, 10),
        source_record_id="MAGYP-20260810-4",
    )
    run = Run(
        code_revision="82e3cbc578023efdafb87ed1e09c5ea8b357eca5",
        started_at=NOW,
        ended_at=NOW + timedelta(minutes=2),
        status=RunStatus.SUCCEEDED,
    )
    finding = Finding(
        run_id=run.run_id,
        dataset_id=dataset.dataset_id,
        subject_id=identity.observation_id,
        rule_id="price.near-limit",
        rule_version="1.0.0",
        severity=FindingSeverity.WARNING,
        evidence={"limit": Decimal("500.00"), "observed": Decimal("499.50")},
        message="Price is near the configured upper limit",
    )
    candidate = CandidateObservation(
        identity=identity,
        value=Decimal("499.50"),
        artifact=artifact,
        parser_version="magyp-fob/1.0.0",
        parsed_at=NOW,
        source_published_at=Timestamp(NOW - timedelta(hours=1)),
    )
    revision = ObservationRevision(
        identity=identity,
        value=Decimal("499.50"),
        open_value=Decimal("498.00"),
        high_value=Decimal("501.00"),
        low_value=Decimal("497.25"),
        close_value=Decimal("499.50"),
        volume=10,
        artifact=artifact,
        source_published_at=Timestamp(NOW - timedelta(hours=1)),
        observed_at=Timestamp(NOW - timedelta(hours=2), inferred=False),
        ingested_at=NOW,
        parser_version="magyp-fob/1.0.0",
        quality_state=QualityState.ACCEPTED,
        finding_ids=(finding.finding_id,),
        public_eligible=True,
    )
    result = DatasetResult(
        run_id=run.run_id,
        dataset_id=dataset.dataset_id,
        status=DatasetResultStatus.SUCCESS,
        candidate_count=1,
        accepted_revision_ids=(revision.revision_id,),
        artifact_references=(artifact,),
        findings=(finding,),
        coverage=Decimal("1.00"),
        freshness=FreshnessState.CURRENT,
        eligible=True,
        validation_policy=ValidationPolicy(("identity.required", "value.positive")),
        as_of_date=date(2026, 8, 10),
    )
    completed_run = replace(
        run,
        dataset_result_ids=(result.dataset_result_id,),
        parser_versions={dataset.dataset_id: "magyp-fob/1.0.0"},
        findings_summary={FindingSeverity.WARNING: 1},
    )
    edition = Edition(
        run_id=completed_run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.VERIFIED,
        revision_ids=(revision.revision_id,),
        generated_artifact_hashes={"dashboard.html": HASH_B},
    )
    correction = Correction(
        prior_revision_id=revision.revision_id,
        decision=CorrectionDecision.REJECT,
        operator="owner@example.com",
        reason="Source subsequently withdrew the publication",
        evidence_references=("source-notice-2026-08-10",),
        decided_at=NOW + timedelta(hours=1),
    )
    promotion = Promotion(
        edition_id=edition.edition_id,
        promoted_at=NOW + timedelta(minutes=4),
        verification_evidence=("edition-contract-check", "artifact-hash-check"),
    )
    return {
        "source": source,
        "dataset": dataset,
        "artifact": artifact,
        "raw_artifact": raw_artifact,
        "identity": identity,
        "candidate": candidate,
        "revision": revision,
        "finding": finding,
        "dataset_result": result,
        "run": completed_run,
        "edition": edition,
        "promotion": promotion,
        "correction": correction,
    }


def test_every_canonical_record_round_trips_without_losing_meaning(values: dict[str, object]) -> None:
    for value in values.values():
        serialized = value.to_dict()  # type: ignore[attr-defined]
        round_tripped = type(value).from_dict(json.loads(json.dumps(serialized)))

        assert round_tripped == value
        assert round_tripped.to_dict() == serialized
        assert serialized["schema_version"] == 1


def test_nested_value_objects_round_trip() -> None:
    timestamp = Timestamp(NOW, inferred=True)
    window = DeliveryWindow(date(2026, 8, 1), date(2026, 8, 31), "August")
    contract = ContractIdentity("cme", "zsx26", "2026-11")

    assert Timestamp.from_dict(timestamp.to_dict()) == timestamp
    assert DeliveryWindow.from_dict(window.to_dict()) == window
    assert ContractIdentity.from_dict(contract.to_dict()) == contract


@pytest.mark.parametrize(
    ("enum_type", "valid_values"),
    [
        (FindingSeverity, {"warning", "quarantine", "reject"}),
        (QualityState, {"legacy", "accepted", "quarantined", "rejected", "superseded"}),
        (FreshnessState, {"current", "stale", "unavailable"}),
        (RunStatus, {"running", "succeeded", "failed"}),
        (EditionStatus, {"candidate", "verified", "promoted", "deployment-failed", "superseded"}),
    ],
)
def test_closed_states_accept_only_the_documented_values(enum_type: type, valid_values: set[str]) -> None:
    assert {state.value for state in enum_type} == valid_values
    with pytest.raises(ValueError):
        enum_type("unknown")


def test_models_reject_raw_unknown_state_values(values: dict[str, object]) -> None:
    run = values["run"]
    finding = values["finding"]
    edition = values["edition"]

    with pytest.raises(ValueError, match="not-a-run-state"):
        replace(run, status="not-a-run-state")
    with pytest.raises(ValueError, match="not-a-severity"):
        replace(finding, severity="not-a-severity")
    with pytest.raises(ValueError, match="not-an-edition-state"):
        replace(edition, status="not-an-edition-state")


def test_incomplete_observation_identities_are_rejected(values: dict[str, object]) -> None:
    identity = values["identity"]

    with pytest.raises(ValueError, match="venue or location"):
        replace(identity, venue=None, location=None)
    with pytest.raises(ValueError, match="cannot combine a contract and delivery window"):
        replace(identity, contract=ContractIdentity("cme", "ZSX26", "2026-11"))
    with pytest.raises(ValueError, match="currency"):
        replace(identity, currency="US")


def test_non_legacy_revision_requires_artifact_and_parser(values: dict[str, object]) -> None:
    revision = values["revision"]

    with pytest.raises(ValueError, match="artifact and parser version"):
        replace(revision, artifact=None)
    with pytest.raises(ValueError, match="artifact and parser version"):
        replace(revision, parser_version=None)

    legacy = replace(
        revision,
        quality_state=QualityState.LEGACY,
        artifact=None,
        parser_version=None,
    )
    assert legacy.quality_state is QualityState.LEGACY


def test_equivalent_inputs_have_stable_identifiers(values: dict[str, object]) -> None:
    source = values["source"]
    identity = values["identity"]
    revision = values["revision"]

    equivalent_source = Source(key=" MAGYP ", name="A renamed display label")
    equivalent_identity = replace(identity, currency="usd")
    equivalent_revision = replace(revision, value=Decimal("499.5000"))

    assert equivalent_source.source_id == source.source_id
    assert equivalent_identity.observation_id == identity.observation_id
    assert equivalent_revision.revision_id == revision.revision_id


def test_stable_ids_match_equivalent_records_and_distinguish_new_attempts(
    values: dict[str, object],
) -> None:
    candidate = values["candidate"]
    revision = values["revision"]

    equivalent_candidate = replace(
        candidate,
        parsed_at=candidate.parsed_at.astimezone(timezone(timedelta(hours=3))),
    )
    equivalent_revision = replace(
        revision,
        ingested_at=revision.ingested_at.astimezone(timezone(timedelta(hours=3))),
    )
    later_candidate = replace(candidate, parsed_at=NOW + timedelta(days=1))
    later_revision = replace(revision, ingested_at=NOW + timedelta(days=1))

    assert equivalent_candidate.candidate_id == candidate.candidate_id
    assert equivalent_revision.revision_id == revision.revision_id
    assert later_candidate.candidate_id != candidate.candidate_id
    assert later_revision.revision_id != revision.revision_id
    assert replace(candidate.artifact).artifact_id == candidate.artifact.artifact_id


def test_equivalent_instants_produce_the_same_run_identifier() -> None:
    offset_time = NOW.astimezone(timezone(timedelta(hours=3)))
    utc_run = Run("abc123", NOW, None, RunStatus.RUNNING)
    offset_run = Run("abc123", offset_time, None, RunStatus.RUNNING)

    assert utc_run.run_id == offset_run.run_id
    assert offset_run.started_at == NOW


def test_named_contracts_and_delivery_windows_are_part_of_observation_identity(
    values: dict[str, object],
) -> None:
    physical = values["identity"]
    later_window = replace(
        physical,
        delivery_window=DeliveryWindow(date(2026, 9, 1), date(2026, 9, 30)),
    )
    november = replace(
        physical,
        location=None,
        venue="cme",
        delivery_window=None,
        contract=ContractIdentity("cme", "ZSX26", "2026-11"),
    )
    january = replace(november, contract=ContractIdentity("cme", "ZSF27", "2027-01"))

    assert physical.observation_id != later_window.observation_id
    assert november.observation_id != january.observation_id
    assert (
        len({physical.observation_id, later_window.observation_id, november.observation_id, january.observation_id})
        == 4
    )


def test_delivery_window_display_label_is_not_part_of_logical_identity(
    values: dict[str, object],
) -> None:
    identity = values["identity"]
    assert identity.delivery_window is not None
    expanded_label = replace(
        identity,
        delivery_window=replace(identity.delivery_window, label="August through September"),
    )

    assert expanded_label.observation_id == identity.observation_id


def test_candidate_and_revision_reject_artifact_from_another_dataset(
    values: dict[str, object],
) -> None:
    candidate = values["candidate"]
    revision = values["revision"]
    source = values["source"]
    other_dataset = Dataset(source.source_id, "other-dataset", "Other dataset")
    wrong_artifact = replace(
        candidate.artifact,
        dataset_id=other_dataset.dataset_id,
        dataset_key=other_dataset.key,
    )

    with pytest.raises(ValueError, match="same source and dataset"):
        replace(candidate, artifact=wrong_artifact)
    with pytest.raises(ValueError, match="same source and dataset"):
        replace(revision, artifact=wrong_artifact)


def test_dataset_relationship_and_identifier_kinds_are_validated(values: dict[str, object]) -> None:
    source = values["source"]
    run = values["run"]

    with pytest.raises(ValueError, match="src identifier"):
        Dataset(run.run_id, "wrong-kind", "Wrong kind")
    with pytest.raises(ValueError, match="does not match source and dataset key"):
        ObservationIdentity(
            source_id=source.source_id,
            dataset_id=f"dst_{'d' * 64}",
            dataset_key="invented",
            commodity="soybean",
            product_form="beans",
            price_type="spot",
            currency="USD",
            unit="usd-mt",
            effective_date=date(2026, 8, 10),
            location="paranagua",
        )


def test_serialized_identifier_tampering_is_rejected(values: dict[str, object]) -> None:
    serialized = values["revision"].to_dict()
    serialized["revision_id"] = f"rev_{'0' * 64}"

    with pytest.raises(ValueError, match="does not match"):
        ObservationRevision.from_dict(serialized)


def test_timestamp_requires_timezone_and_preserves_inference_label() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        Timestamp(datetime(2026, 8, 10, 12, 30))

    inferred = Timestamp(NOW, inferred=True)
    assert Timestamp.from_dict(inferred.to_dict()).inferred is True


def test_finding_evidence_round_trip_restores_numeric_date_and_timestamp_types(
    values: dict[str, object],
) -> None:
    finding = replace(
        values["finding"],
        evidence={
            "threshold": Decimal("1.20"),
            "market_date": date(2026, 8, 10),
            "published_at": NOW,
        },
    )

    round_tripped = Finding.from_dict(json.loads(json.dumps(finding.to_dict())))

    assert round_tripped.evidence == finding.evidence
    assert isinstance(round_tripped.evidence["threshold"], Decimal)
    assert isinstance(round_tripped.evidence["market_date"], date)
    assert isinstance(round_tripped.evidence["published_at"], datetime)

    with pytest.raises(TypeError):
        round_tripped.evidence["threshold"] = Decimal("2")


def test_candidate_cannot_express_publishability_or_quality(values: dict[str, object]) -> None:
    candidate = values["candidate"]

    assert not hasattr(candidate, "quality_state")
    assert not hasattr(candidate, "public_eligible")
    assert candidate.to_dict()["effective_date_inferred"] is False


def test_promotion_is_an_evidenced_pointer_change_with_stable_identity(
    values: dict[str, object],
) -> None:
    promotion = values["promotion"]

    equivalent = replace(
        promotion,
        verification_evidence=tuple(reversed(promotion.verification_evidence)),
    )
    later_operation = replace(promotion, promoted_at=promotion.promoted_at + timedelta(minutes=5))
    assert equivalent.promotion_id == promotion.promotion_id
    assert later_operation.promotion_id != promotion.promotion_id

    with pytest.raises(ValueError, match="verification evidence"):
        replace(promotion, verification_evidence=())
    with pytest.raises(ValueError, match="must change"):
        replace(promotion, previous_edition_id=promotion.edition_id)


def test_successful_and_failed_run_manifests_round_trip(values: dict[str, object]) -> None:
    successful = values["run"]
    failed = Run(
        code_revision="failing-revision",
        started_at=NOW + timedelta(hours=1),
        ended_at=NOW + timedelta(hours=1, minutes=2),
        status=RunStatus.FAILED,
        findings_summary={FindingSeverity.REJECT: 1},
    )

    for manifest in (successful, failed):
        serialized = json.loads(json.dumps(manifest.to_dict()))
        restored = Run.from_dict(serialized)

        assert restored == manifest
        assert restored.manifest_hash == manifest.manifest_hash
        assert serialized["manifest_hash"] == manifest.manifest_hash

    assert failed.dataset_result_ids == ()
    assert failed.parser_versions == {}


def test_manifest_hashes_are_deterministic_and_cover_pinned_outputs(values: dict[str, object]) -> None:
    run = values["run"]
    edition = values["edition"]
    extra_dataset_id = f"dst_{'c' * 64}"
    equivalent_run = replace(
        run,
        parser_versions={
            extra_dataset_id: "derived/2.0.0",
            **dict(run.parser_versions),
        },
        findings_summary={FindingSeverity.REJECT: 0, **dict(run.findings_summary)},
    )
    reordered_run = replace(
        run,
        parser_versions={
            **dict(run.parser_versions),
            extra_dataset_id: "derived/2.0.0",
        },
        findings_summary={**dict(run.findings_summary), FindingSeverity.REJECT: 0},
    )
    derived_revision_id = f"rev_{'c' * 64}"
    pinned_edition = replace(edition, derived_revision_ids=(derived_revision_id,))

    assert equivalent_run.manifest_hash == reordered_run.manifest_hash
    assert pinned_edition.manifest_hash != edition.manifest_hash
    assert replace(edition, revision_ids=(f"rev_{'d' * 64}",)).manifest_hash != edition.manifest_hash
    assert replace(edition, generated_artifact_hashes={"dashboard.html": HASH_A}).manifest_hash != edition.manifest_hash


def test_manifest_round_trip_rejects_content_hash_tampering(values: dict[str, object]) -> None:
    serialized_run = values["run"].to_dict()
    serialized_run["status"] = RunStatus.FAILED.value
    serialized_edition = values["edition"].to_dict()
    serialized_edition["generated_artifact_hashes"] = {"dashboard.html": HASH_A}
    serialized_with_extra_content = values["run"].to_dict()
    serialized_with_extra_content["unexpected"] = "tampered"
    serialized_with_normalized_content = values["run"].to_dict()
    serialized_with_normalized_content["findings_summary"] = {
        FindingSeverity.WARNING.value: 1.9
    }

    with pytest.raises(ValueError, match="manifest_hash does not match"):
        Run.from_dict(serialized_run)
    with pytest.raises(ValueError, match="manifest_hash does not match"):
        Edition.from_dict(serialized_edition)
    with pytest.raises(ValueError, match="manifest_hash does not match"):
        Run.from_dict(serialized_with_extra_content)
    with pytest.raises(ValueError, match="manifest_hash does not match"):
        Run.from_dict(serialized_with_normalized_content)


def test_correction_replacement_is_explicit_and_evidenced(values: dict[str, object]) -> None:
    revision = values["revision"]

    with pytest.raises(ValueError, match="evidence"):
        Correction(
            revision.revision_id,
            CorrectionDecision.APPROVE,
            "owner",
            "Reviewed against source",
            (),
            NOW,
        )
    with pytest.raises(ValueError, match="replacement_revision_id"):
        Correction(
            revision.revision_id,
            CorrectionDecision.REPLACE,
            "owner",
            "Corrected source value",
            ("source-correction",),
            NOW,
        )
    with pytest.raises(ValueError, match="type and reason"):
        replace(revision, correction_type="source-correction", correction_reason=None)
    with pytest.raises(ValueError, match="type and reason"):
        replace(revision, correction_type=None, correction_reason="Corrected by source")


def test_dataset_result_exposes_the_complete_ingestion_outcome(values: dict[str, object]) -> None:
    artifact = values["artifact"]
    finding = values["finding"]
    revision = values["revision"]
    run = values["run"]
    dataset = values["dataset"]
    result = DatasetResult(
        run_id=run.run_id,
        dataset_id=dataset.dataset_id,
        status=DatasetResultStatus.SUCCESS,
        candidate_count=1,
        accepted_revision_ids=(revision.revision_id,),
        artifact_references=(artifact,),
        findings=(finding,),
        coverage=Decimal("1"),
        freshness=FreshnessState.CURRENT,
        eligible=True,
        validation_policy=ValidationPolicy(("identity.required", "value.positive")),
    )

    assert result.artifact_references == (artifact,)
    assert result.artifact_ids == (artifact.artifact_id,)
    assert result.candidate_count == 1
    assert result.accepted_count == 1
    assert result.accepted_revision_ids == (revision.revision_id,)
    assert result.findings == (finding,)
    assert result.finding_ids == (finding.finding_id,)
    assert result.coverage == Decimal("1")
    assert result.freshness is FreshnessState.CURRENT
    assert result.eligible is True
    assert DatasetResult.from_dict(json.loads(json.dumps(result.to_dict()))) == result


def test_every_dataset_result_state_is_explicit_and_round_trips(values: dict[str, object]) -> None:
    successful = values["dataset_result"]
    warning = values["finding"]
    legitimate_empty = replace(
        successful,
        status=DatasetResultStatus.LEGITIMATE_EMPTY,
        candidate_count=0,
        accepted_revision_ids=(),
        artifact_references=(),
        findings=(),
        validation_policy=ValidationPolicy(("publication.present",), allows_empty_publication=True),
    )
    external_failure = replace(
        successful,
        status=DatasetResultStatus.EXTERNAL_FAILURE,
        candidate_count=0,
        accepted_revision_ids=(),
        artifact_references=(),
        findings=(),
        coverage=Decimal("0"),
        freshness=FreshnessState.UNAVAILABLE,
        eligible=False,
        error="source timed out",
    )
    contract_failure = replace(
        successful,
        status=DatasetResultStatus.CONTRACT_FAILURE,
        accepted_revision_ids=(),
        findings=(replace(warning, severity=FindingSeverity.REJECT),),
        coverage=Decimal("0"),
        eligible=False,
        error="coverage contract failed",
    )
    quarantined = replace(
        successful,
        status=DatasetResultStatus.QUARANTINED,
        accepted_revision_ids=(),
        findings=(replace(warning, severity=FindingSeverity.QUARANTINE),),
        coverage=Decimal("0"),
        eligible=False,
    )
    results = (successful, legitimate_empty, external_failure, contract_failure, quarantined)

    assert {result.status for result in results} == set(DatasetResultStatus)
    assert all(
        DatasetResult.from_dict(json.loads(json.dumps(result.to_dict()))) == result
        for result in results
    )


def test_dataset_result_rejects_contradictory_state_combinations(values: dict[str, object]) -> None:
    successful = values["dataset_result"]
    warning = values["finding"]
    reject = replace(warning, severity=FindingSeverity.REJECT)
    quarantine = replace(warning, severity=FindingSeverity.QUARANTINE)
    contradictions = (
        {"findings": (reject,)},
        {"findings": (quarantine,)},
        {"accepted_revision_ids": ()},
        {"artifact_references": ()},
        {"freshness": FreshnessState.UNAVAILABLE},
        {"error": "success cannot carry an error"},
        {
            "status": DatasetResultStatus.LEGITIMATE_EMPTY,
            "candidate_count": 0,
            "accepted_revision_ids": (),
            "artifact_references": (),
            "findings": (),
        },
        {
            "status": DatasetResultStatus.LEGITIMATE_EMPTY,
            "accepted_revision_ids": (),
            "validation_policy": ValidationPolicy(
                ("publication.present",),
                allows_empty_publication=True,
            ),
        },
        {
            "status": DatasetResultStatus.EXTERNAL_FAILURE,
            "accepted_revision_ids": (),
            "eligible": True,
            "error": "source timed out",
        },
        {
            "status": DatasetResultStatus.EXTERNAL_FAILURE,
            "accepted_revision_ids": (),
            "eligible": False,
        },
        {
            "status": DatasetResultStatus.EXTERNAL_FAILURE,
            "eligible": False,
            "error": "source timed out",
        },
        {
            "status": DatasetResultStatus.CONTRACT_FAILURE,
            "accepted_revision_ids": (),
            "eligible": False,
            "error": "contract failed",
        },
        {
            "status": DatasetResultStatus.QUARANTINED,
            "accepted_revision_ids": (),
            "eligible": False,
        },
        {
            "status": DatasetResultStatus.QUARANTINED,
            "accepted_revision_ids": (),
            "findings": (quarantine,),
            "eligible": True,
        },
    )

    for changes in contradictions:
        with pytest.raises(ValueError):
            replace(successful, **changes)


def test_run_status_evaluation_consumes_only_dataset_results(values: dict[str, object]) -> None:
    successful = values["dataset_result"]
    critical_dataset_ids = (successful.dataset_id,)
    first_supporting_failure = replace(
        successful,
        dataset_id=f"dst_{'c' * 64}",
        status=DatasetResultStatus.EXTERNAL_FAILURE,
        candidate_count=0,
        accepted_revision_ids=(),
        artifact_references=(),
        findings=(),
        coverage=Decimal("0"),
        freshness=FreshnessState.UNAVAILABLE,
        eligible=False,
        error="source timed out",
    )
    second_supporting_failure = replace(
        first_supporting_failure,
        dataset_id=f"dst_{'d' * 64}",
    )

    assert evaluate_run_status(
        (successful,),
        critical_dataset_ids=critical_dataset_ids,
        max_hard_failures=1,
    ) is RunStatus.SUCCEEDED
    assert evaluate_run_status(
        (successful, first_supporting_failure),
        critical_dataset_ids=critical_dataset_ids,
        max_hard_failures=1,
    ) is RunStatus.SUCCEEDED
    assert evaluate_run_status(
        (successful, first_supporting_failure, second_supporting_failure),
        critical_dataset_ids=critical_dataset_ids,
        max_hard_failures=1,
    ) is RunStatus.FAILED
    assert evaluate_run_status(
        (
            replace(
                successful,
                status=DatasetResultStatus.QUARANTINED,
                accepted_revision_ids=(),
                findings=(replace(values["finding"], severity=FindingSeverity.QUARANTINE),),
                eligible=False,
            ),
        ),
        critical_dataset_ids=(),
        max_hard_failures=0,
    ) is RunStatus.FAILED
    assert evaluate_run_status(
        (replace(successful, eligible=False),),
        critical_dataset_ids=critical_dataset_ids,
        max_hard_failures=1,
    ) is RunStatus.FAILED
    assert evaluate_run_status(
        (replace(successful, freshness=FreshnessState.STALE),),
        critical_dataset_ids=critical_dataset_ids,
        max_hard_failures=1,
    ) is RunStatus.FAILED
    assert evaluate_run_status(
        (
            replace(
                successful,
                status=DatasetResultStatus.LEGITIMATE_EMPTY,
                candidate_count=0,
                accepted_revision_ids=(),
                validation_policy=ValidationPolicy(
                    ("publication.present",),
                    allows_empty_publication=True,
                ),
            ),
        ),
        critical_dataset_ids=critical_dataset_ids,
        max_hard_failures=1,
    ) is RunStatus.FAILED
    assert evaluate_run_status(
        (),
        critical_dataset_ids=critical_dataset_ids,
        max_hard_failures=1,
    ) is RunStatus.FAILED


def test_validation_policy_deserialization_does_not_coerce_truthy_values() -> None:
    with pytest.raises(ValueError, match="must be a boolean"):
        ValidationPolicy.from_dict(
            {
                "rule_ids": ["publication.present"],
                "allows_empty_publication": "false",
            }
        )
