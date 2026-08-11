"""Shared durable-record and raw-artifact behavior for every repository adapter."""

from __future__ import annotations

import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from multiprocessing import get_context
from pathlib import Path
from threading import Barrier
from typing import Any, BinaryIO

import pytest

import trust.repository as repository_module
from trust import (
    MAGYP_FOB_CONTRACT,
    ArtifactReference,
    ArtifactRetentionError,
    Correction,
    CorrectionDecision,
    CurrentEditionConflict,
    Dataset,
    Edition,
    EditionPromotionError,
    EditionStatus,
    Finding,
    FindingSeverity,
    GitDirectoryTrustRepository,
    ImmutableRecordConflict,
    ObservationIdentity,
    ObservationRevision,
    Promotion,
    QualityState,
    RawArtifact,
    RawRetention,
    RepositoryFormatError,
    RightsAction,
    RightsDecision,
    Run,
    RunStatus,
    SupersessionCycleError,
    TemporaryDirectoryTrustRepository,
    Timestamp,
    TrustRepository,
)

NOW = datetime(2026, 8, 10, 12, 30, tzinfo=timezone.utc)
ARTIFACT_CONTENT = b'{"prices":[]}'
ARTIFACT_HASH = "6921ac105efddb540edd50aeafe47c11c581f6949111f6342ce7bbf074245741"
DT08Record = Finding | Run | Correction | Edition
DT08RecordCase = tuple[DT08Record, type[Any], str]
DT08RecordCaseFactory = Callable[[Run, ObservationRevision], DT08RecordCase]


@pytest.fixture(params=("temporary", "git"))
def repository(request: pytest.FixtureRequest, tmp_path: Path) -> TrustRepository:
    if request.param == "temporary":
        adapter: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "temporary-repository")
    else:
        adapter = GitDirectoryTrustRepository(tmp_path / "git-worktree")
    assert isinstance(adapter, TrustRepository)
    return adapter


@pytest.fixture(params=("temporary", "git"))
def repository_pair(request: pytest.FixtureRequest, tmp_path: Path) -> tuple[TrustRepository, TrustRepository]:
    if request.param == "temporary":
        root = tmp_path / "temporary-repository"
        return TemporaryDirectoryTrustRepository(root), TemporaryDirectoryTrustRepository(root)
    root = tmp_path / "git-worktree"
    return GitDirectoryTrustRepository(root), GitDirectoryTrustRepository(root)


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


@pytest.fixture
def observation_revision(artifact: RawArtifact) -> ObservationRevision:
    dataset = MAGYP_FOB_CONTRACT.dataset
    identity = ObservationIdentity(
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        dataset_key=dataset.key,
        commodity="soybean",
        product_form="beans",
        location="up-river",
        price_type="fob",
        currency="USD",
        unit="usd-mt",
        effective_date=date(2026, 8, 10),
    )
    return ObservationRevision(
        identity=identity,
        value=Decimal("499.50"),
        ingested_at=NOW,
        quality_state=QualityState.ACCEPTED,
        public_eligible=True,
        artifact=artifact.reference,
        parser_version="magyp-fob/1.0.0",
    )


def _edition_for(
    run: Run,
    *,
    status: EditionStatus = EditionStatus.VERIFIED,
    created_at: datetime = NOW + timedelta(minutes=3),
) -> Edition:
    return Edition(
        run_id=run.run_id,
        created_at=created_at,
        status=status,
        revision_ids=(),
    )


def _finding_record_case(
    run: Run,
    observation_revision: ObservationRevision,
) -> DT08RecordCase:
    finding = Finding(
        run_id=run.run_id,
        dataset_id=observation_revision.identity.dataset_id,
        subject_id=observation_revision.revision_id,
        rule_id="price.move",
        rule_version="1",
        severity=FindingSeverity.QUARANTINE,
        evidence={"candidate": "499.50"},
        message="Price move requires review",
    )
    return finding, Finding, finding.finding_id


def _run_record_case(run: Run, observation_revision: ObservationRevision) -> DT08RecordCase:
    del observation_revision
    return run, Run, run.run_id


def _correction_record_case(
    run: Run,
    observation_revision: ObservationRevision,
) -> DT08RecordCase:
    del run
    correction = Correction(
        prior_revision_id=observation_revision.revision_id,
        decision=CorrectionDecision.APPROVE,
        operator="operator@example.test",
        reason="Source document confirms the reported value",
        evidence_references=("artifact-page-2",),
        decided_at=NOW + timedelta(minutes=3),
    )
    return correction, Correction, correction.correction_id


def _edition_record_case(run: Run, observation_revision: ObservationRevision) -> DT08RecordCase:
    del observation_revision
    edition = _edition_for(run)
    return edition, Edition, edition.edition_id


DT08_RECORD_CASES: tuple[object, ...] = (
    pytest.param(_finding_record_case, id="finding"),
    pytest.param(_run_record_case, id="run"),
    pytest.param(_correction_record_case, id="correction"),
    pytest.param(_edition_record_case, id="edition"),
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


def test_observation_revisions_append_idempotently_and_sort_deterministically(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
) -> None:
    later_revision = replace(
        observation_revision,
        value=Decimal("501.25"),
        ingested_at=NOW + timedelta(minutes=5),
    )

    repository.append_observation_revision(later_revision)
    repository.append_observation_revision(observation_revision)
    repository.append_observation_revision(observation_revision)

    assert repository.observation_revisions(observation_revision.identity) == (
        observation_revision,
        later_revision,
    )


def test_interrupted_observation_append_can_be_retried_without_a_partial_record(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with monkeypatch.context() as patch:
        patch.setattr(repository_module, "_write_and_sync", _write_partially_then_fail)
        with pytest.raises(OSError, match="simulated interrupted payload"):
            repository.append_observation_revision(observation_revision)

    assert repository.read(ObservationRevision, observation_revision.revision_id) is None
    assert repository.observation_revisions(observation_revision.identity) == ()

    repository.append_observation_revision(observation_revision)

    assert repository.read(ObservationRevision, observation_revision.revision_id) == observation_revision


def test_observation_append_can_be_retried_after_rename_before_directory_sync(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with monkeypatch.context() as patch:
        patch.setattr(repository_module, "_sync_directory", _fail_directory_sync)
        with pytest.raises(OSError, match="simulated directory sync failure"):
            repository.append_observation_revision(observation_revision)

    repository.append_observation_revision(observation_revision)

    assert repository.read(ObservationRevision, observation_revision.revision_id) == observation_revision


def test_concurrent_idempotent_observation_appends_create_one_revision(
    repository_pair: tuple[TrustRepository, TrustRepository],
    observation_revision: ObservationRevision,
) -> None:
    barrier = Barrier(8)

    def append_once(repository: TrustRepository) -> None:
        barrier.wait()
        repository.append_observation_revision(observation_revision)

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(append_once, repository_pair[index % 2]) for index in range(8)]
        for future in futures:
            future.result()

    assert repository_pair[0].observation_revisions(observation_revision.identity) == (observation_revision,)
    assert repository_pair[1].observation_revisions(observation_revision.identity) == (observation_revision,)


def test_concurrent_distinct_observation_appends_preserve_every_revision(
    repository_pair: tuple[TrustRepository, TrustRepository],
    observation_revision: ObservationRevision,
) -> None:
    revisions = tuple(
        replace(observation_revision, value=Decimal(f"{500 + index}.25"))
        for index in range(8)
    )
    barrier = Barrier(len(revisions))

    def append_once(repository: TrustRepository, revision: ObservationRevision) -> None:
        barrier.wait()
        repository.append_observation_revision(revision)

    with ThreadPoolExecutor(max_workers=len(revisions)) as executor:
        futures = [
            executor.submit(append_once, repository_pair[index % 2], revision)
            for index, revision in enumerate(revisions)
        ]
        for future in futures:
            future.result()

    expected = tuple(sorted(revisions, key=lambda revision: revision.revision_id))
    assert repository_pair[0].observation_revisions(observation_revision.identity) == expected
    assert repository_pair[1].observation_revisions(observation_revision.identity) == expected


@pytest.mark.parametrize("adapter_kind", ("temporary", "git"))
@pytest.mark.parametrize("append_mode", ("idempotent", "distinct"))
def test_concurrent_observation_appends_are_safe_across_processes(
    tmp_path: Path,
    observation_revision: ObservationRevision,
    adapter_kind: str,
    append_mode: str,
) -> None:
    repository_root = tmp_path / f"{adapter_kind}-repository"
    revisions = tuple(
        observation_revision
        if append_mode == "idempotent"
        else replace(observation_revision, value=Decimal(f"{500 + index}.25"))
        for index in range(4)
    )
    context = get_context("fork")
    barrier = context.Barrier(len(revisions))
    processes = [
        context.Process(
            target=_append_observation_in_process,
            args=(adapter_kind, repository_root, revision.to_dict(), barrier),
        )
        for revision in revisions
    ]

    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0

    repository = _repository_for_kind(adapter_kind, repository_root)
    expected = tuple(sorted(set(revisions), key=lambda revision: revision.revision_id))
    assert repository.observation_revisions(observation_revision.identity) == expected


def test_current_accepted_revision_breaks_ingestion_time_ties_by_revision_identifier(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
) -> None:
    tied = replace(observation_revision, value=Decimal("501.25"))
    expected = max((observation_revision, tied), key=lambda revision: revision.revision_id)
    repository.append_observation_revision(expected)
    repository.append_observation_revision(tied if expected is observation_revision else observation_revision)

    assert repository.current_accepted_revision(observation_revision.identity) == expected
    assert repository.revision_effective_at(observation_revision.identity, NOW) == expected


def test_generic_store_and_read_agree_with_observation_ledger_methods(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
) -> None:
    replacement = replace(
        observation_revision,
        value=Decimal("501.25"),
        ingested_at=NOW + timedelta(minutes=5),
        supersedes_revision_id=observation_revision.revision_id,
    )

    repository.store(observation_revision)
    repository.append_observation_revision(replacement)

    assert repository.read(ObservationRevision, observation_revision.revision_id) == observation_revision
    assert repository.read(ObservationRevision, replacement.revision_id) == replacement
    assert repository.observation_revisions(observation_revision.identity) == (
        observation_revision,
        replacement,
    )


def test_batch_observation_append_is_idempotent_and_queryable(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
) -> None:
    later_revision = replace(
        observation_revision,
        value=Decimal("501.25"),
        ingested_at=NOW + timedelta(minutes=5),
    )

    repository.append_observation_revisions((observation_revision, later_revision, observation_revision))

    assert repository.read(ObservationRevision, observation_revision.revision_id) == observation_revision
    assert repository.observation_revisions(observation_revision.identity) == (
        observation_revision,
        later_revision,
    )


def test_observation_queries_remain_compatible_with_the_dt05_flat_layout(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
    tmp_path: Path,
) -> None:
    _write_dt05_observation(repository, observation_revision, tmp_path)

    assert repository.read(ObservationRevision, observation_revision.revision_id) == observation_revision
    assert repository.observation_revisions(observation_revision.identity) == (observation_revision,)
    assert repository.current_accepted_revision(observation_revision.identity) == observation_revision
    assert repository.revision_effective_at(observation_revision.identity, NOW) == observation_revision


def test_appending_an_existing_dt05_revision_is_idempotent(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
    tmp_path: Path,
) -> None:
    _write_dt05_observation(repository, observation_revision, tmp_path)

    repository.append_observation_revision(observation_revision)

    assert repository.observation_revisions(observation_revision.identity) == (observation_revision,)


def test_batch_appending_an_existing_dt05_revision_is_idempotent(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
    tmp_path: Path,
) -> None:
    _write_dt05_observation(repository, observation_revision, tmp_path)

    repository.append_observation_revisions((observation_revision,))

    assert repository.observation_revisions(observation_revision.identity) == (observation_revision,)


def test_conflicting_revision_identifier_in_the_dt05_layout_is_rejected(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
    tmp_path: Path,
) -> None:
    _write_dt05_observation(repository, observation_revision, tmp_path)
    conflicting = replace(observation_revision, value=Decimal("888.00"))
    object.__setattr__(conflicting, "revision_id", observation_revision.revision_id)

    with pytest.raises(ImmutableRecordConflict, match=observation_revision.revision_id):
        repository.append_observation_revision(conflicting)

    assert repository.read(ObservationRevision, observation_revision.revision_id) == observation_revision


def test_observation_revision_uses_the_dataset_and_effective_year_partition(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
    tmp_path: Path,
) -> None:
    repository.append_observation_revision(observation_revision)

    durable_paths = tuple(tmp_path.rglob(f"{observation_revision.revision_id}.json"))

    assert len(durable_paths) == 1
    assert durable_paths[0].parts[-4:] == (
        "observations",
        observation_revision.identity.dataset_id,
        "2026",
        f"{observation_revision.revision_id}.json",
    )


@pytest.mark.parametrize("contradictory_partition", ("dataset", "year"))
def test_revision_reads_reject_records_outside_their_canonical_partition(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
    monkeypatch: pytest.MonkeyPatch,
    contradictory_partition: str,
) -> None:
    durable_path = _capture_observation_path(repository, observation_revision, monkeypatch)
    if contradictory_partition == "dataset":
        misplaced_path = (
            durable_path.parent.parent.parent
            / f"dst_{'f' * 64}"
            / durable_path.parent.name
            / durable_path.name
        )
    else:
        misplaced_path = durable_path.parent.parent / "2025" / durable_path.name
    misplaced_path.parent.mkdir(parents=True)
    durable_path.replace(misplaced_path)

    with pytest.raises(RepositoryFormatError, match="partition"):
        repository.read(ObservationRevision, observation_revision.revision_id)
    assert repository.observation_revisions(observation_revision.identity) == ()


def test_observation_operations_are_isolated_from_unrelated_year_partitions(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unrelated_identity = replace(observation_revision.identity, effective_date=date(2027, 1, 2))
    unrelated_revision = replace(
        observation_revision,
        identity=unrelated_identity,
        ingested_at=NOW + timedelta(minutes=1),
    )
    repository.append_observation_revision(observation_revision)
    unrelated_path = _capture_observation_path(repository, unrelated_revision, monkeypatch)
    _rewrite_observation_payload(
        unrelated_path,
        supersedes_revision_id=f"rev_{'f' * 64}",
    )

    assert repository.read(ObservationRevision, observation_revision.revision_id) == observation_revision
    assert repository.observation_revisions(observation_revision.identity) == (observation_revision,)
    assert repository.current_accepted_revision(observation_revision.identity) == observation_revision
    assert repository.revision_effective_at(observation_revision.identity, NOW) == observation_revision

    later_revision = replace(
        observation_revision,
        value=Decimal("501.25"),
        ingested_at=NOW + timedelta(minutes=2),
    )
    repository.append_observation_revision(later_revision)
    assert repository.observation_revisions(observation_revision.identity) == (
        observation_revision,
        later_revision,
    )


def test_explicit_supersession_selects_the_latest_accepted_revision_without_erasing_history(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
) -> None:
    replacement = replace(
        observation_revision,
        value=Decimal("501.25"),
        ingested_at=NOW + timedelta(minutes=5),
        supersedes_revision_id=observation_revision.revision_id,
    )

    repository.append_observation_revision(observation_revision)
    repository.append_observation_revision(replacement)

    assert repository.current_accepted_revision(observation_revision.identity) == replacement
    assert repository.observation_revisions(observation_revision.identity) == (
        observation_revision,
        replacement,
    )


def test_conflicting_observation_revision_identifiers_are_rejected(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
) -> None:
    repository.append_observation_revision(observation_revision)
    conflicting = replace(observation_revision, value=Decimal("888.00"))
    object.__setattr__(conflicting, "revision_id", observation_revision.revision_id)

    with pytest.raises(ImmutableRecordConflict, match=observation_revision.revision_id):
        repository.append_observation_revision(conflicting)

    assert repository.observation_revisions(observation_revision.identity) == (observation_revision,)


def test_revision_identifier_conflicts_are_rejected_across_partitions(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
) -> None:
    repository.append_observation_revision(observation_revision)
    other_identity = replace(
        observation_revision.identity,
        effective_date=date(2027, 1, 2),
    )
    conflicting = replace(observation_revision, identity=other_identity)
    object.__setattr__(conflicting, "revision_id", observation_revision.revision_id)

    with pytest.raises(ImmutableRecordConflict, match=observation_revision.revision_id):
        repository.append_observation_revision(conflicting)

    assert repository.observation_revisions(other_identity) == ()


def test_batch_revision_identifier_conflicts_are_rejected_across_partitions(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
) -> None:
    repository.append_observation_revision(observation_revision)
    other_identity = replace(
        observation_revision.identity,
        effective_date=date(2027, 1, 2),
    )
    conflicting = replace(observation_revision, identity=other_identity)
    object.__setattr__(conflicting, "revision_id", observation_revision.revision_id)

    with pytest.raises(ImmutableRecordConflict, match=observation_revision.revision_id):
        repository.append_observation_revisions((conflicting,))

    assert repository.observation_revisions(other_identity) == ()


def test_supersession_cycles_are_rejected(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
) -> None:
    object.__setattr__(
        observation_revision,
        "supersedes_revision_id",
        observation_revision.revision_id,
    )

    with pytest.raises(SupersessionCycleError, match="cycle"):
        repository.append_observation_revision(observation_revision)

    assert repository.observation_revisions(observation_revision.identity) == ()


def test_dangling_supersession_targets_are_rejected(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
) -> None:
    dangling = replace(
        observation_revision,
        value=Decimal("501.25"),
        ingested_at=NOW + timedelta(minutes=1),
        supersedes_revision_id=f"rev_{'f' * 64}",
    )

    with pytest.raises(RepositoryFormatError, match="not in the ledger"):
        repository.append_observation_revision(dangling)

    assert repository.observation_revisions(observation_revision.identity) == ()


def test_cross_identity_supersession_is_rejected(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
) -> None:
    other_identity = replace(observation_revision.identity, location="rosario")
    other_revision = replace(
        observation_revision,
        identity=other_identity,
        value=Decimal("501.25"),
        ingested_at=NOW + timedelta(minutes=1),
        supersedes_revision_id=observation_revision.revision_id,
    )
    repository.append_observation_revision(observation_revision)

    with pytest.raises(RepositoryFormatError, match="same observation identity"):
        repository.append_observation_revision(other_revision)

    assert repository.observation_revisions(observation_revision.identity) == (observation_revision,)
    assert repository.observation_revisions(other_identity) == ()


def test_queries_reject_an_indirect_dangling_supersession_chain(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    middle = replace(
        observation_revision,
        value=Decimal("500.25"),
        ingested_at=NOW + timedelta(minutes=1),
    )
    successor = replace(
        observation_revision,
        value=Decimal("501.25"),
        ingested_at=NOW + timedelta(minutes=2),
        supersedes_revision_id=middle.revision_id,
    )
    repository.append_observation_revision(observation_revision)
    middle_path = _capture_observation_path(repository, middle, monkeypatch)
    repository.append_observation_revision(successor)
    _rewrite_observation_payload(
        middle_path,
        supersedes_revision_id=f"rev_{'f' * 64}",
    )

    with pytest.raises(RepositoryFormatError, match="not in the ledger"):
        repository.current_accepted_revision(observation_revision.identity)
    with pytest.raises(RepositoryFormatError, match="not in the ledger"):
        repository.read(ObservationRevision, successor.revision_id)


def test_queries_reject_an_indirect_supersession_cycle(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    other = replace(
        observation_revision,
        value=Decimal("501.25"),
        ingested_at=NOW + timedelta(minutes=1),
    )
    first_path = _capture_observation_path(repository, observation_revision, monkeypatch)
    other_path = _capture_observation_path(repository, other, monkeypatch)
    _rewrite_observation_payload(first_path, supersedes_revision_id=other.revision_id)
    _rewrite_observation_payload(other_path, supersedes_revision_id=observation_revision.revision_id)

    with pytest.raises(SupersessionCycleError, match="cycle"):
        repository.observation_revisions(observation_revision.identity)
    with pytest.raises(SupersessionCycleError, match="cycle"):
        repository.append_observation_revision(
            replace(observation_revision, value=Decimal("502.25"), ingested_at=NOW + timedelta(minutes=2))
        )


def test_queries_reject_malformed_cross_identity_supersession(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    other_identity = replace(observation_revision.identity, location="rosario")
    other = replace(
        observation_revision,
        identity=other_identity,
        value=Decimal("501.25"),
        ingested_at=NOW + timedelta(minutes=1),
    )
    repository.append_observation_revision(observation_revision)
    other_path = _capture_observation_path(repository, other, monkeypatch)
    _rewrite_observation_payload(
        other_path,
        supersedes_revision_id=observation_revision.revision_id,
    )

    with pytest.raises(RepositoryFormatError, match="same observation identity"):
        repository.current_accepted_revision(observation_revision.identity)


def test_current_accepted_revision_excludes_every_ineligible_state_without_hiding_history(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
) -> None:
    ineligible = (
        replace(
            observation_revision,
            value=Decimal("500.00"),
            ingested_at=NOW + timedelta(minutes=1),
            quality_state=QualityState.REJECTED,
        ),
        replace(
            observation_revision,
            value=Decimal("501.00"),
            ingested_at=NOW + timedelta(minutes=2),
            quality_state=QualityState.QUARANTINED,
        ),
        replace(
            observation_revision,
            value=Decimal("502.00"),
            ingested_at=NOW + timedelta(minutes=3),
            quality_state=QualityState.SUPERSEDED,
        ),
        replace(
            observation_revision,
            value=Decimal("503.00"),
            ingested_at=NOW + timedelta(minutes=4),
            quality_state=QualityState.LEGACY,
            artifact=None,
            parser_version=None,
        ),
        replace(
            observation_revision,
            value=Decimal("504.00"),
            ingested_at=NOW + timedelta(minutes=5),
            public_eligible=False,
        ),
    )

    repository.append_observation_revision(observation_revision)
    for revision in ineligible:
        repository.append_observation_revision(revision)

    assert repository.current_accepted_revision(observation_revision.identity) == observation_revision
    assert repository.observation_revisions(observation_revision.identity) == (
        observation_revision,
        *ineligible,
    )


@pytest.mark.parametrize(
    ("quality_state", "public_eligible"),
    (
        (QualityState.REJECTED, True),
        (QualityState.QUARANTINED, True),
        (QualityState.SUPERSEDED, True),
        (QualityState.LEGACY, True),
        (QualityState.ACCEPTED, False),
    ),
)
def test_ineligible_successors_do_not_suppress_the_accepted_head(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
    quality_state: QualityState,
    public_eligible: bool,
) -> None:
    successor = replace(
        observation_revision,
        value=Decimal("501.25"),
        ingested_at=NOW + timedelta(minutes=5),
        quality_state=quality_state,
        public_eligible=public_eligible,
        artifact=None if quality_state is QualityState.LEGACY else observation_revision.artifact,
        parser_version=None if quality_state is QualityState.LEGACY else observation_revision.parser_version,
        supersedes_revision_id=observation_revision.revision_id,
    )
    repository.append_observation_revision(observation_revision)
    repository.append_observation_revision(successor)

    assert repository.current_accepted_revision(observation_revision.identity) == observation_revision
    assert repository.revision_effective_at(
        observation_revision.identity,
        NOW + timedelta(minutes=10),
    ) == observation_revision
    assert repository.observation_revisions(observation_revision.identity) == (
        observation_revision,
        successor,
    )


def test_revision_effective_at_uses_only_information_available_at_that_time(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
) -> None:
    replacement = replace(
        observation_revision,
        value=Decimal("501.25"),
        ingested_at=NOW + timedelta(minutes=5),
        supersedes_revision_id=observation_revision.revision_id,
    )
    repository.append_observation_revision(observation_revision)
    repository.append_observation_revision(replacement)

    assert repository.revision_effective_at(
        observation_revision.identity,
        NOW - timedelta(seconds=1),
    ) is None
    assert repository.revision_effective_at(
        observation_revision.identity,
        NOW + timedelta(minutes=4),
    ) == observation_revision
    assert repository.revision_effective_at(
        observation_revision.identity,
        NOW + timedelta(minutes=5),
    ) == replacement


@pytest.mark.parametrize(
    "successor_ingested_at",
    (
        NOW - timedelta(seconds=1),
        datetime(2026, 8, 10, 15, 29, 59, tzinfo=timezone(timedelta(hours=3))),
    ),
)
def test_successor_cannot_predate_the_revision_it_supersedes(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
    successor_ingested_at: datetime,
) -> None:
    successor = replace(
        observation_revision,
        value=Decimal("501.25"),
        ingested_at=successor_ingested_at,
        supersedes_revision_id=observation_revision.revision_id,
    )
    repository.append_observation_revision(observation_revision)

    with pytest.raises(RepositoryFormatError, match="cannot predate"):
        repository.append_observation_revision(successor)

    assert repository.observation_revisions(observation_revision.identity) == (observation_revision,)


@pytest.mark.parametrize(
    "successor_ingested_at",
    (
        datetime(2026, 8, 10, 15, 30, tzinfo=timezone(timedelta(hours=3))),
        NOW + timedelta(seconds=1),
    ),
)
def test_successor_may_be_ingested_at_the_same_or_a_later_instant(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
    successor_ingested_at: datetime,
) -> None:
    successor = replace(
        observation_revision,
        value=Decimal("501.25"),
        ingested_at=successor_ingested_at,
        supersedes_revision_id=observation_revision.revision_id,
    )
    repository.append_observation_revision(observation_revision)
    repository.append_observation_revision(successor)

    assert repository.revision_effective_at(
        observation_revision.identity,
        successor.ingested_at,
    ) == successor


def test_point_in_time_queries_reject_a_durable_successor_that_predates_its_target(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    successor = replace(
        observation_revision,
        value=Decimal("501.25"),
        ingested_at=NOW + timedelta(minutes=1),
        supersedes_revision_id=observation_revision.revision_id,
    )
    repository.append_observation_revision(observation_revision)
    successor_path = _capture_observation_path(repository, successor, monkeypatch)
    _rewrite_observation_payload(
        successor_path,
        ingested_at=(NOW - timedelta(seconds=1)).isoformat(),
    )

    with pytest.raises(RepositoryFormatError, match="cannot predate"):
        repository.revision_effective_at(
            observation_revision.identity,
            NOW + timedelta(minutes=2),
        )
    with pytest.raises(RepositoryFormatError, match="cannot predate"):
        repository.current_accepted_revision(observation_revision.identity)


def test_queries_reject_temporal_inversion_inside_a_longer_durable_chain(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    middle = replace(
        observation_revision,
        value=Decimal("500.25"),
        ingested_at=NOW + timedelta(minutes=1),
        supersedes_revision_id=observation_revision.revision_id,
    )
    successor = replace(
        observation_revision,
        value=Decimal("501.25"),
        ingested_at=NOW + timedelta(minutes=2),
        supersedes_revision_id=middle.revision_id,
    )
    repository.append_observation_revision(observation_revision)
    middle_path = _capture_observation_path(repository, middle, monkeypatch)
    repository.append_observation_revision(successor)
    _rewrite_observation_payload(
        middle_path,
        ingested_at=(NOW + timedelta(minutes=3)).isoformat(),
    )

    with pytest.raises(RepositoryFormatError, match="cannot predate"):
        repository.current_accepted_revision(observation_revision.identity)


def test_revision_effective_at_rejects_a_naive_requested_at(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
) -> None:
    repository.append_observation_revision(observation_revision)

    with pytest.raises(ValueError, match="timezone-aware"):
        repository.revision_effective_at(
            observation_revision.identity,
            datetime(2026, 8, 10, 12, 30),
        )


def test_revision_effective_at_compares_non_utc_requested_at_by_instant(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
) -> None:
    repository.append_observation_revision(observation_revision)

    requested_at = datetime(2026, 8, 10, 15, 30, tzinfo=timezone(timedelta(hours=3)))

    assert repository.revision_effective_at(observation_revision.identity, requested_at) == observation_revision


def test_conflicting_immutable_rewrites_are_rejected(repository: TrustRepository, run: Run) -> None:
    repository.store(run)
    conflicting = replace(run, status=RunStatus.FAILED)

    with pytest.raises(ImmutableRecordConflict, match=run.run_id):
        repository.store(conflicting)

    assert repository.read(Run, run.run_id) == run


def test_missing_records_return_none(repository: TrustRepository, run: Run) -> None:
    assert repository.read(Run, run.run_id) is None


def test_dt08_records_persist_and_reload_across_repository_instances(
    repository_pair: tuple[TrustRepository, TrustRepository],
    run: Run,
    observation_revision: ObservationRevision,
) -> None:
    finding = Finding(
        run_id=run.run_id,
        dataset_id=observation_revision.identity.dataset_id,
        subject_id=observation_revision.revision_id,
        rule_id="price.move",
        rule_version="1",
        severity=FindingSeverity.QUARANTINE,
        evidence={"prior": "495.00", "candidate": "499.50"},
        message="Price move requires review",
    )
    correction = Correction(
        prior_revision_id=observation_revision.revision_id,
        decision=CorrectionDecision.APPROVE,
        operator="operator@example.test",
        reason="Source document confirms the reported value",
        evidence_references=("artifact-page-2",),
        decided_at=NOW + timedelta(minutes=3),
    )
    edition = _edition_for(run)
    writer, reader = repository_pair

    writer.append_observation_revision(observation_revision)
    for record in (finding, run, correction, edition):
        writer.store(record)

    assert reader.read(Finding, finding.finding_id) == finding
    assert reader.read(Run, run.run_id) == run
    assert reader.read(Correction, correction.correction_id) == correction
    assert reader.read(Edition, edition.edition_id) == edition


def test_corrections_append_approval_and_replacement_decisions(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
) -> None:
    replacement = replace(
        observation_revision,
        value=Decimal("501.25"),
        ingested_at=NOW + timedelta(minutes=4),
        supersedes_revision_id=observation_revision.revision_id,
    )
    approval = Correction(
        prior_revision_id=observation_revision.revision_id,
        decision=CorrectionDecision.APPROVE,
        operator="operator@example.test",
        reason="Source document confirms the reported value",
        evidence_references=("artifact-page-2",),
        decided_at=NOW + timedelta(minutes=3),
    )
    replacement_decision = Correction(
        prior_revision_id=observation_revision.revision_id,
        replacement_revision_id=replacement.revision_id,
        decision=CorrectionDecision.REPLACE,
        operator="operator@example.test",
        reason="Publisher issued a corrected value",
        evidence_references=("publisher-correction-notice",),
        decided_at=NOW + timedelta(minutes=5),
    )
    repository.append_observation_revision(observation_revision)
    repository.append_observation_revision(replacement)

    repository.store(approval)
    repository.store(replacement_decision)

    assert repository.read(Correction, approval.correction_id) == approval
    assert repository.read(Correction, replacement_decision.correction_id) == replacement_decision


@pytest.mark.parametrize("record_case", DT08_RECORD_CASES)
def test_dt08_immutable_writes_are_idempotent(
    repository: TrustRepository,
    run: Run,
    observation_revision: ObservationRevision,
    record_case: DT08RecordCaseFactory,
) -> None:
    record, decoder, record_id = record_case(run, observation_revision)

    repository.store(record)
    repository.store(record)

    assert repository.read(decoder, record_id) == record


@pytest.mark.parametrize("record_case", DT08_RECORD_CASES)
def test_interrupted_dt08_writes_can_be_retried_without_partial_records(
    repository: TrustRepository,
    run: Run,
    observation_revision: ObservationRevision,
    monkeypatch: pytest.MonkeyPatch,
    record_case: DT08RecordCaseFactory,
) -> None:
    record, decoder, record_id = record_case(run, observation_revision)

    with monkeypatch.context() as patch:
        patch.setattr(repository_module, "_write_and_sync", _write_partially_then_fail)
        with pytest.raises(OSError, match="simulated interrupted payload"):
            repository.store(record)

    assert repository.read(decoder, record_id) is None

    repository.store(record)

    assert repository.read(decoder, record_id) == record


@pytest.mark.parametrize("adapter_kind", ("temporary", "git"))
@pytest.mark.parametrize("store_mode", ("idempotent", "conflicting"))
def test_dt08_immutable_writes_are_safe_across_processes(
    tmp_path: Path,
    run: Run,
    adapter_kind: str,
    store_mode: str,
) -> None:
    repository_root = tmp_path / f"{adapter_kind}-repository"
    payloads = (
        (run.to_dict(),) * 4
        if store_mode == "idempotent"
        else (run.to_dict(), replace(run, status=RunStatus.FAILED).to_dict())
    )
    context = get_context("fork")
    barrier = context.Barrier(len(payloads))
    outcomes = context.Queue()
    processes = [
        context.Process(
            target=_store_immutable_record_in_process,
            args=(adapter_kind, repository_root, payload, Run, barrier, outcomes),
        )
        for payload in payloads
    ]

    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0

    results = sorted(outcomes.get(timeout=5) for _ in processes)
    repository = _repository_for_kind(adapter_kind, repository_root)
    restored = repository.read(Run, run.run_id)
    if store_mode == "idempotent":
        assert results == ["stored"] * 4
        assert restored == run
    else:
        assert results == ["conflict", "stored"]
        assert restored in (run, replace(run, status=RunStatus.FAILED))


@pytest.mark.parametrize("adapter_kind", ("temporary", "git"))
@pytest.mark.parametrize("store_mode", ("idempotent", "conflicting"))
def test_partitioned_finding_writes_are_safe_across_processes(
    tmp_path: Path,
    run: Run,
    observation_revision: ObservationRevision,
    adapter_kind: str,
    store_mode: str,
) -> None:
    repository_root = tmp_path / f"{adapter_kind}-repository"
    finding, _, _ = _finding_record_case(run, observation_revision)
    assert isinstance(finding, Finding)
    payloads = (
        (finding.to_dict(),) * 4
        if store_mode == "idempotent"
        else (finding.to_dict(), replace(finding, message="Conflicting explanation").to_dict())
    )
    context = get_context("fork")
    barrier = context.Barrier(len(payloads))
    outcomes = context.Queue()
    processes = [
        context.Process(
            target=_store_immutable_record_in_process,
            args=(adapter_kind, repository_root, payload, Finding, barrier, outcomes),
        )
        for payload in payloads
    ]

    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0

    results = sorted(outcomes.get(timeout=5) for _ in processes)
    repository = _repository_for_kind(adapter_kind, repository_root)
    restored = repository.read(Finding, finding.finding_id)
    if store_mode == "idempotent":
        assert results == ["stored"] * 4
        assert restored == finding
    else:
        assert results == ["conflict", "stored"]
        assert restored in (finding, replace(finding, message="Conflicting explanation"))


@pytest.mark.parametrize(
    ("invalid_field", "invalid_value"),
    (
        ("reason", "   "),
        ("evidence_references", []),
    ),
)
def test_repository_rejects_corrections_without_reason_or_evidence(
    repository: TrustRepository,
    observation_revision: ObservationRevision,
    invalid_field: str,
    invalid_value: object,
) -> None:
    correction = Correction(
        prior_revision_id=observation_revision.revision_id,
        decision=CorrectionDecision.APPROVE,
        operator="operator@example.test",
        reason="Source document confirms the reported value",
        evidence_references=("artifact-page-2",),
        decided_at=NOW + timedelta(minutes=3),
    )

    class InvalidCorrectionRecord:
        def to_dict(self) -> dict[str, Any]:
            return {**correction.to_dict(), invalid_field: invalid_value}

    with pytest.raises(RepositoryFormatError, match="invalid canonical correction"):
        repository.store(InvalidCorrectionRecord())

    assert repository.read(Correction, correction.correction_id) is None


def test_findings_are_immutable(repository: TrustRepository, run: Run) -> None:
    finding = Finding(
        run_id=run.run_id,
        dataset_id=MAGYP_FOB_CONTRACT.dataset.dataset_id,
        subject_id="dataset",
        rule_id="coverage.minimum",
        rule_version="1",
        severity=FindingSeverity.REJECT,
        evidence={"coverage": "0.50"},
        message="Coverage is below contract",
    )
    repository.store(finding)

    with pytest.raises(ImmutableRecordConflict, match=finding.finding_id):
        repository.store(replace(finding, message="Conflicting explanation"))

    assert repository.read(Finding, finding.finding_id) == finding


def test_findings_use_the_run_partition(
    repository: TrustRepository,
    run: Run,
    observation_revision: ObservationRevision,
    tmp_path: Path,
) -> None:
    finding, _, _ = _finding_record_case(run, observation_revision)
    assert isinstance(finding, Finding)

    repository.store(finding)

    durable_paths = tuple(tmp_path.rglob(f"{finding.finding_id}.json"))
    assert len(durable_paths) == 1
    assert durable_paths[0].parts[-3:] == (
        "findings",
        run.run_id,
        f"{finding.finding_id}.json",
    )


def test_finding_reads_remain_compatible_with_the_dt05_flat_layout(
    repository: TrustRepository,
    run: Run,
    observation_revision: ObservationRevision,
    tmp_path: Path,
) -> None:
    finding, _, _ = _finding_record_case(run, observation_revision)
    assert isinstance(finding, Finding)
    repository.initialize()
    findings_directory = next(tmp_path.rglob("findings"))
    legacy_path = findings_directory / f"{finding.finding_id}.json"
    legacy_path.write_text(
        json.dumps(
            finding.to_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    assert repository.read(Finding, finding.finding_id) == finding

    repository.store(finding)

    assert tuple(tmp_path.rglob(f"{finding.finding_id}.json")) == (legacy_path,)
    assert repository.read(Finding, finding.finding_id) == finding


def test_edition_status_changes_create_new_immutable_manifest_versions(
    repository: TrustRepository,
    run: Run,
) -> None:
    candidate = _edition_for(run, status=EditionStatus.CANDIDATE)
    conflicting_rewrite = replace(candidate, status=EditionStatus.VERIFIED)
    verified = replace(
        candidate,
        created_at=candidate.created_at + timedelta(minutes=1),
        status=EditionStatus.VERIFIED,
    )
    repository.store(candidate)

    with pytest.raises(ImmutableRecordConflict, match=candidate.edition_id):
        repository.store(conflicting_rewrite)

    repository.store(verified)

    assert verified.edition_id != candidate.edition_id
    assert repository.read(Edition, candidate.edition_id) == candidate
    assert repository.read(Edition, verified.edition_id) == verified


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


def test_current_edition_replacement_is_atomic(
    repository: TrustRepository,
    run: Run,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_edition = _edition_for(run)
    second_edition = _edition_for(
        run,
        created_at=first_edition.created_at + timedelta(minutes=1),
    )
    first = Promotion(
        edition_id=first_edition.edition_id,
        promoted_at=NOW,
        verification_evidence=("edition-contract-check",),
    )
    second = Promotion(
        edition_id=second_edition.edition_id,
        previous_edition_id=first_edition.edition_id,
        promoted_at=NOW + timedelta(minutes=1),
        verification_evidence=("edition-contract-check",),
    )
    repository.store(run)
    repository.store(first_edition)
    repository.store(second_edition)
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


def test_stale_pointer_conflict_precedes_target_eligibility(
    repository: TrustRepository,
    run: Run,
) -> None:
    first_edition = _edition_for(run)
    candidate = _edition_for(
        run,
        status=EditionStatus.CANDIDATE,
        created_at=first_edition.created_at + timedelta(minutes=1),
    )
    first = Promotion(
        edition_id=first_edition.edition_id,
        promoted_at=NOW,
        verification_evidence=("edition-contract-check",),
    )
    stale = Promotion(
        edition_id=candidate.edition_id,
        promoted_at=NOW + timedelta(minutes=1),
        verification_evidence=("edition-contract-check",),
    )
    repository.store(run)
    repository.store(first_edition)
    repository.store(candidate)
    repository.replace_current_edition(first)

    with pytest.raises(CurrentEditionConflict, match="current edition changed"):
        repository.replace_current_edition(stale)

    assert repository.current_edition() == first


@pytest.mark.parametrize(
    "status",
    (
        EditionStatus.CANDIDATE,
        EditionStatus.DEPLOYMENT_FAILED,
        EditionStatus.SUPERSEDED,
    ),
)
def test_current_edition_rejects_ineligible_edition_statuses(
    repository: TrustRepository,
    run: Run,
    status: EditionStatus,
) -> None:
    candidate = _edition_for(run, status=status)
    promotion = Promotion(
        edition_id=candidate.edition_id,
        promoted_at=NOW + timedelta(minutes=4),
        verification_evidence=("edition-contract-check",),
    )
    repository.store(run)
    repository.store(candidate)

    with pytest.raises(EditionPromotionError, match=status.value):
        repository.replace_current_edition(promotion)

    assert repository.current_edition() is None


def test_failed_pointer_update_preserves_the_prior_current_edition(
    repository: TrustRepository,
    run: Run,
) -> None:
    verified = _edition_for(run)
    rejected = _edition_for(
        run,
        status=EditionStatus.DEPLOYMENT_FAILED,
        created_at=verified.created_at + timedelta(minutes=1),
    )
    current = Promotion(
        edition_id=verified.edition_id,
        promoted_at=NOW + timedelta(minutes=4),
        verification_evidence=("edition-contract-check",),
    )
    rejected_update = Promotion(
        edition_id=rejected.edition_id,
        previous_edition_id=verified.edition_id,
        promoted_at=NOW + timedelta(minutes=5),
        verification_evidence=("edition-contract-check",),
    )
    repository.store(run)
    repository.store(verified)
    repository.store(rejected)
    repository.replace_current_edition(current)

    with pytest.raises(EditionPromotionError, match="deployment-failed"):
        repository.replace_current_edition(rejected_update)

    assert repository.current_edition() == current


def test_promoted_edition_status_is_eligible_for_the_current_pointer(
    repository: TrustRepository,
    run: Run,
) -> None:
    promoted_edition = _edition_for(run, status=EditionStatus.PROMOTED)
    promotion = Promotion(
        edition_id=promoted_edition.edition_id,
        promoted_at=NOW + timedelta(minutes=4),
        verification_evidence=("edition-contract-check",),
    )
    repository.store(run)
    repository.store(promoted_edition)

    repository.replace_current_edition(promotion)

    assert repository.current_edition() == promotion


@pytest.mark.parametrize("adapter_kind", ("temporary", "git"))
def test_current_pointer_updates_are_safe_across_processes(
    tmp_path: Path,
    run: Run,
    adapter_kind: str,
) -> None:
    repository_root = tmp_path / f"{adapter_kind}-repository"
    repository = _repository_for_kind(adapter_kind, repository_root)
    first_edition = _edition_for(run)
    second_edition = _edition_for(
        run,
        created_at=first_edition.created_at + timedelta(minutes=1),
    )
    promotions = (
        Promotion(
            edition_id=first_edition.edition_id,
            promoted_at=NOW + timedelta(minutes=5),
            verification_evidence=("edition-contract-check",),
        ),
        Promotion(
            edition_id=second_edition.edition_id,
            promoted_at=NOW + timedelta(minutes=5),
            verification_evidence=("edition-contract-check",),
        ),
    )
    repository.store(run)
    repository.store(first_edition)
    repository.store(second_edition)
    context = get_context("fork")
    barrier = context.Barrier(len(promotions))
    outcomes = context.Queue()
    processes = [
        context.Process(
            target=_replace_current_edition_in_process,
            args=(adapter_kind, repository_root, promotion.to_dict(), barrier, outcomes),
        )
        for promotion in promotions
    ]

    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0

    assert sorted(outcomes.get(timeout=5) for _ in processes) == ["conflict", "updated"]
    assert repository.current_edition() in promotions


def test_current_edition_rejects_an_edition_from_a_failed_run(
    repository: TrustRepository,
    run: Run,
) -> None:
    current_edition = _edition_for(run)
    failed_run = Run(
        code_revision=run.code_revision,
        started_at=NOW + timedelta(minutes=10),
        ended_at=NOW + timedelta(minutes=12),
        status=RunStatus.FAILED,
    )
    failed_edition = Edition(
        run_id=failed_run.run_id,
        created_at=NOW + timedelta(minutes=13),
        status=EditionStatus.VERIFIED,
        revision_ids=(),
    )
    current = Promotion(
        edition_id=current_edition.edition_id,
        promoted_at=NOW + timedelta(minutes=4),
        verification_evidence=("edition-contract-check",),
    )
    failed_update = Promotion(
        edition_id=failed_edition.edition_id,
        previous_edition_id=current_edition.edition_id,
        promoted_at=NOW + timedelta(minutes=14),
        verification_evidence=("edition-contract-check",),
    )
    repository.store(run)
    repository.store(current_edition)
    repository.store(failed_run)
    repository.store(failed_edition)
    repository.replace_current_edition(current)

    with pytest.raises(EditionPromotionError, match="failed run"):
        repository.replace_current_edition(failed_update)

    assert repository.current_edition() == current


def _fail_replace(source: Path, destination: Path) -> None:
    raise OSError("simulated atomic replacement failure")


def _fail_unexpected_write(temporary_file: BinaryIO, contents: bytes) -> None:
    raise AssertionError("retention policy was not checked before writing")


def _fail_directory_sync(directory: Path) -> None:
    raise OSError("simulated directory sync failure")


def _write_partially_then_fail(temporary_file: BinaryIO, contents: bytes) -> None:
    temporary_file.write(contents[: len(contents) // 2])
    temporary_file.flush()
    raise OSError("simulated interrupted payload")


def _capture_observation_path(
    repository: TrustRepository,
    revision: ObservationRevision,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    destinations: list[Path] = []
    real_replace = repository_module.os.replace

    def capture_destination(source: Path, destination: Path) -> None:
        if destination.name == f"{revision.revision_id}.json":
            destinations.append(destination)
        real_replace(source, destination)

    with monkeypatch.context() as patch:
        patch.setattr(repository_module.os, "replace", capture_destination)
        repository.append_observation_revision(revision)

    assert len(destinations) == 1
    return destinations[0]


def _rewrite_observation_payload(path: Path, **changes: object) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.update(changes)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_dt05_observation(
    repository: TrustRepository,
    revision: ObservationRevision,
    tmp_path: Path,
) -> Path:
    repository.initialize()
    observations_directory = next(tmp_path.rglob("observations"))
    legacy_path = observations_directory / f"{revision.revision_id}.json"
    legacy_path.write_text(
        json.dumps(
            revision.to_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return legacy_path


def _repository_for_kind(adapter_kind: str, repository_root: Path) -> TrustRepository:
    if adapter_kind == "temporary":
        return TemporaryDirectoryTrustRepository(repository_root)
    return GitDirectoryTrustRepository(repository_root)


def _append_observation_in_process(
    adapter_kind: str,
    repository_root: Path,
    revision_payload: dict[str, Any],
    barrier: Any,
) -> None:
    repository = _repository_for_kind(adapter_kind, repository_root)
    revision = ObservationRevision.from_dict(revision_payload)
    barrier.wait()
    repository.append_observation_revision(revision)


def _store_immutable_record_in_process(
    adapter_kind: str,
    repository_root: Path,
    record_payload: dict[str, Any],
    decoder: type[Any],
    barrier: Any,
    outcomes: Any,
) -> None:
    repository = _repository_for_kind(adapter_kind, repository_root)
    record = decoder.from_dict(record_payload)
    barrier.wait()
    try:
        repository.store(record)
    except ImmutableRecordConflict:
        outcomes.put("conflict")
    else:
        outcomes.put("stored")


def _replace_current_edition_in_process(
    adapter_kind: str,
    repository_root: Path,
    promotion_payload: dict[str, Any],
    barrier: Any,
    outcomes: Any,
) -> None:
    repository = _repository_for_kind(adapter_kind, repository_root)
    promotion = Promotion.from_dict(promotion_payload)
    barrier.wait()
    try:
        repository.replace_current_edition(promotion)
    except CurrentEditionConflict:
        outcomes.put("conflict")
    else:
        outcomes.put("updated")
