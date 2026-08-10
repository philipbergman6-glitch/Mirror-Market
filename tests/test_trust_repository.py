"""Shared durable-record and raw-artifact behavior for every repository adapter."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, BinaryIO

import pytest

import trust.repository as repository_module
from trust import (
    MAGYP_FOB_CONTRACT,
    ArtifactReference,
    ArtifactRetentionError,
    CurrentEditionConflict,
    Dataset,
    GitDirectoryTrustRepository,
    ImmutableRecordConflict,
    Promotion,
    RawArtifact,
    RawRetention,
    RepositoryFormatError,
    RightsAction,
    RightsDecision,
    Run,
    RunStatus,
    TemporaryDirectoryTrustRepository,
    Timestamp,
    TrustRepository,
)

NOW = datetime(2026, 8, 10, 12, 30, tzinfo=timezone.utc)
EDITION_A = f"edn_{'a' * 64}"
EDITION_B = f"edn_{'b' * 64}"
ARTIFACT_CONTENT = b'{"prices":[]}'
ARTIFACT_HASH = "6921ac105efddb540edd50aeafe47c11c581f6949111f6342ce7bbf074245741"


@pytest.fixture(params=("temporary", "git"))
def repository(request: pytest.FixtureRequest, tmp_path: Path) -> TrustRepository:
    if request.param == "temporary":
        adapter: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "temporary-repository")
    else:
        adapter = GitDirectoryTrustRepository(tmp_path / "git-worktree")
    assert isinstance(adapter, TrustRepository)
    return adapter


@pytest.fixture
def run() -> Run:
    return Run(
        code_revision="6da43a8436b33d69f41762bdd72d7139ad415cd1",
        started_at=NOW,
        ended_at=NOW + timedelta(minutes=2),
        status=RunStatus.SUCCEEDED,
    )


@pytest.fixture
def content_contract():
    rights = MAGYP_FOB_CONTRACT.rights
    assert rights is not None
    decisions = dict(rights.decisions)
    decisions[RightsAction.RAW_CONTENT_RETENTION] = RightsDecision.ALLOWED
    return replace(
        MAGYP_FOB_CONTRACT,
        raw_retention=RawRetention.CONTENT,
        rights=replace(rights, decisions=decisions),
    )


@pytest.fixture
def artifact(content_contract) -> RawArtifact:
    dataset = content_contract.dataset
    reference = ArtifactReference(
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        dataset_key=dataset.key,
        content_hash=ARTIFACT_HASH,
        content_retained=True,
        media_type="application/json",
    )
    return RawArtifact(
        reference=reference,
        retrieval_url="https://example.test/magyp-fob",
        retrieved_at=Timestamp(NOW - timedelta(minutes=1)),
        response_status=200,
        byte_size=len(ARTIFACT_CONTENT),
        content=ARTIFACT_CONTENT,
    )


def test_initialization_is_idempotent_and_preserves_records(repository: TrustRepository, run: Run) -> None:
    repository.initialize()
    repository.initialize()
    repository.store(run)
    repository.initialize()

    restored = repository.read(Run, run.run_id)

    assert restored == run
    assert restored is not None
    assert restored.to_dict()["schema_version"] == run.schema_version


def test_identical_immutable_writes_are_idempotent(repository: TrustRepository, run: Run) -> None:
    repository.store(run)
    repository.store(run)

    assert repository.read(Run, run.run_id) == run


def test_conflicting_immutable_rewrites_are_rejected(repository: TrustRepository, run: Run) -> None:
    repository.store(run)
    conflicting = replace(run, status=RunStatus.FAILED)

    with pytest.raises(ImmutableRecordConflict, match=run.run_id):
        repository.store(conflicting)

    assert repository.read(Run, run.run_id) == run


def test_missing_records_return_none(repository: TrustRepository, run: Run) -> None:
    assert repository.read(Run, run.run_id) is None


def test_permitted_content_artifacts_round_trip(
    repository: TrustRepository,
    artifact: RawArtifact,
    content_contract,
) -> None:
    reference = repository.store_artifact(artifact, content_contract)

    assert reference == artifact.reference
    assert repository.read_artifact(reference.artifact_id) == artifact


def test_content_retention_is_rejected_when_dataset_policy_does_not_allow_it(
    repository: TrustRepository,
    artifact: RawArtifact,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with monkeypatch.context() as patch:
        patch.setattr(repository_module, "_write_and_sync", _fail_unexpected_write)
        with pytest.raises(ArtifactRetentionError, match="raw content retention"):
            repository.store_artifact(artifact, MAGYP_FOB_CONTRACT)

    assert repository.read_artifact(artifact.reference.artifact_id) is None


def test_conflicting_artifact_metadata_is_rejected(
    repository: TrustRepository,
    artifact: RawArtifact,
    content_contract,
) -> None:
    repository.store_artifact(artifact, content_contract)
    conflicting = replace(artifact, retrieval_url="https://example.test/conflicting-fetch")

    with pytest.raises(ImmutableRecordConflict, match=artifact.reference.artifact_id):
        repository.store_artifact(conflicting, content_contract)

    assert repository.read_artifact(artifact.reference.artifact_id) == artifact


def test_metadata_only_artifacts_retain_fetch_metadata_without_content(
    repository: TrustRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset = MAGYP_FOB_CONTRACT.dataset
    reference = ArtifactReference(
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        dataset_key=dataset.key,
        content_hash=ARTIFACT_HASH,
        content_retained=False,
        media_type="application/json",
    )
    artifact = RawArtifact(
        reference=reference,
        retrieval_url="https://example.test/magyp-fob",
        retrieved_at=Timestamp(NOW - timedelta(minutes=1)),
        response_status=200,
        byte_size=len(ARTIFACT_CONTENT),
        content=None,
    )

    written_contents: list[bytes] = []
    real_write = repository_module._write_and_sync

    def capture_writes(temporary_file: BinaryIO, contents: bytes) -> None:
        written_contents.append(contents)
        real_write(temporary_file, contents)

    with monkeypatch.context() as patch:
        patch.setattr(repository_module, "_write_and_sync", capture_writes)
        repository.store_artifact(artifact, MAGYP_FOB_CONTRACT)

    restored = repository.read_artifact(reference.artifact_id)
    assert restored == artifact
    assert restored is not None
    assert restored.content is None
    assert restored.to_dict() == {
        "schema_version": 1,
        "record_type": "raw-artifact",
        "artifact_id": reference.artifact_id,
        "reference": reference.to_dict(),
        "retrieval_url": "https://example.test/magyp-fob",
        "retrieved_at": {"value": "2026-08-10T12:29:00+00:00", "inferred": False},
        "response_status": 200,
        "byte_size": 13,
        "content_base64": None,
    }
    assert len(written_contents) == 1
    assert ARTIFACT_CONTENT not in written_contents[0]


def test_repeated_content_is_deduplicated(
    repository: TrustRepository,
    artifact: RawArtifact,
    content_contract,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = repository.store_artifact(artifact, content_contract)

    with monkeypatch.context() as patch:
        patch.setattr(repository_module.os, "replace", _fail_replace)
        second = repository.store_artifact(artifact, content_contract)

    assert first == second
    assert repository.read_artifact(first.artifact_id) == artifact


def test_content_payloads_are_deduplicated_across_artifact_records(
    repository: TrustRepository,
    artifact: RawArtifact,
    content_contract,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository.store_artifact(artifact, content_contract)
    second_dataset = Dataset(
        source_id=content_contract.dataset.source_id,
        key="official-soy-fob-mirror",
        name="Official soy-complex FOB mirror",
    )
    second_contract = replace(content_contract, dataset=second_dataset)
    second_reference = ArtifactReference(
        source_id=second_dataset.source_id,
        dataset_id=second_dataset.dataset_id,
        dataset_key=second_dataset.key,
        content_hash=ARTIFACT_HASH,
        content_retained=True,
        media_type="application/json",
    )
    second_artifact = replace(artifact, reference=second_reference)
    real_replace = repository_module.os.replace

    def reject_duplicate_payload(source: Path, destination: Path) -> None:
        if source.read_bytes() == ARTIFACT_CONTENT:
            raise AssertionError("content payload was written twice")
        real_replace(source, destination)

    with monkeypatch.context() as patch:
        patch.setattr(repository_module.os, "replace", reject_duplicate_payload)
        repository.store_artifact(second_artifact, second_contract)

    assert repository.read_artifact(artifact.reference.artifact_id) == artifact
    assert repository.read_artifact(second_reference.artifact_id) == second_artifact


def test_interrupted_payload_writes_do_not_create_readable_artifacts(
    repository: TrustRepository,
    artifact: RawArtifact,
    content_contract,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with monkeypatch.context() as patch:
        patch.setattr(repository_module, "_write_and_sync", _write_partially_then_fail)
        with pytest.raises(OSError, match="simulated interrupted payload"):
            repository.store_artifact(artifact, content_contract)

    assert repository.read_artifact(artifact.reference.artifact_id) is None

    repository.store_artifact(artifact, content_contract)
    assert repository.read_artifact(artifact.reference.artifact_id) == artifact


def test_conflicting_content_at_an_existing_hash_is_rejected(
    repository: TrustRepository,
    artifact: RawArtifact,
    content_contract,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload_paths: list[Path] = []
    real_replace = repository_module.os.replace

    def capture_payload_path(source: Path, destination: Path) -> None:
        if source.read_bytes() == ARTIFACT_CONTENT:
            payload_paths.append(destination)
        real_replace(source, destination)

    with monkeypatch.context() as patch:
        patch.setattr(repository_module.os, "replace", capture_payload_path)
        repository.store_artifact(artifact, content_contract)

    assert len(payload_paths) == 1
    payload_paths[0].write_bytes(b"truncated")

    with pytest.raises(ImmutableRecordConflict, match=ARTIFACT_HASH):
        repository.store_artifact(artifact, content_contract)


def test_every_durable_record_requires_a_schema_version(repository: TrustRepository, run: Run) -> None:
    class MissingSchemaRecord:
        def to_dict(self) -> dict[str, Any]:
            payload = run.to_dict()
            del payload["schema_version"]
            return payload

    with pytest.raises(RepositoryFormatError, match="schema_version"):
        repository.store(MissingSchemaRecord())


def test_noncanonical_record_serialization_is_rejected(repository: TrustRepository, run: Run) -> None:
    class ContradictoryRunRecord:
        def to_dict(self) -> dict[str, Any]:
            payload = run.to_dict()
            payload["status"] = RunStatus.FAILED.value
            return payload

    with pytest.raises(RepositoryFormatError, match="invalid canonical run"):
        repository.store(ContradictoryRunRecord())

    assert repository.read(Run, run.run_id) is None


def test_current_edition_replacement_is_atomic(repository: TrustRepository, monkeypatch: pytest.MonkeyPatch) -> None:
    first = Promotion(
        edition_id=EDITION_A,
        promoted_at=NOW,
        verification_evidence=("edition-contract-check",),
    )
    second = Promotion(
        edition_id=EDITION_B,
        previous_edition_id=EDITION_A,
        promoted_at=NOW + timedelta(minutes=1),
        verification_evidence=("edition-contract-check",),
    )
    repository.replace_current_edition(first)
    repository.replace_current_edition(first)

    with monkeypatch.context() as patch:
        patch.setattr(repository_module.os, "replace", _fail_replace)
        with pytest.raises(OSError, match="simulated atomic replacement failure"):
            repository.replace_current_edition(second)

    assert repository.current_edition() == first

    repository.replace_current_edition(second)

    current = repository.current_edition()
    assert current == second
    assert current is not None
    assert current.to_dict()["schema_version"] == second.schema_version


def test_current_edition_rejects_stale_pointer_updates(repository: TrustRepository) -> None:
    first = Promotion(
        edition_id=EDITION_A,
        promoted_at=NOW,
        verification_evidence=("edition-contract-check",),
    )
    stale = Promotion(
        edition_id=EDITION_B,
        promoted_at=NOW + timedelta(minutes=1),
        verification_evidence=("edition-contract-check",),
    )
    repository.replace_current_edition(first)

    with pytest.raises(CurrentEditionConflict, match="current edition changed"):
        repository.replace_current_edition(stale)

    assert repository.current_edition() == first


def _fail_replace(source: Path, destination: Path) -> None:
    raise OSError("simulated atomic replacement failure")


def _fail_unexpected_write(temporary_file: BinaryIO, contents: bytes) -> None:
    raise AssertionError("retention policy was not checked before writing")


def _write_partially_then_fail(temporary_file: BinaryIO, contents: bytes) -> None:
    temporary_file.write(contents[: len(contents) // 2])
    temporary_file.flush()
    raise OSError("simulated interrupted payload")
