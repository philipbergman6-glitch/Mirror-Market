"""Shared DT-05 behavior suite for every durable trust repository adapter."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

import trust.repository as repository_module
from trust import (
    CurrentEditionConflict,
    GitDirectoryTrustRepository,
    ImmutableRecordConflict,
    Promotion,
    RepositoryFormatError,
    Run,
    RunStatus,
    TemporaryDirectoryTrustRepository,
    TrustRepository,
)

NOW = datetime(2026, 8, 10, 12, 30, tzinfo=timezone.utc)
EDITION_A = f"edn_{'a' * 64}"
EDITION_B = f"edn_{'b' * 64}"


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
