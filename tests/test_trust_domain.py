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
        artifact_ids=(artifact.artifact_id,),
        finding_ids=(finding.finding_id,),
        coverage=Decimal("1.00"),
        freshness=FreshnessState.CURRENT,
        eligible=True,
        as_of_date=date(2026, 8, 10),
    )
    completed_run = replace(run, dataset_result_ids=(result.dataset_result_id,))
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
