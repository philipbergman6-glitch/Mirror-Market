"""DT-10 tests for rebuilding the SQLite query cache from trusted records."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from trust import (
    MAGYP_FOB_CONTRACT,
    ArtifactReference,
    Edition,
    EditionStatus,
    GitDirectoryTrustRepository,
    ObservationIdentity,
    ObservationRevision,
    QualityState,
    Run,
    RunStatus,
    TemporaryDirectoryTrustRepository,
    TrustRepository,
    build_query_cache,
)

NOW = datetime(2026, 8, 10, 12, 30, tzinfo=timezone.utc)


@pytest.fixture(params=("temporary", "git"))
def repository(request: pytest.FixtureRequest, tmp_path: Path) -> TrustRepository:
    if request.param == "temporary":
        return TemporaryDirectoryTrustRepository(tmp_path / "temporary-repository")
    return GitDirectoryTrustRepository(tmp_path / "git-worktree")


def _artifact() -> ArtifactReference:
    dataset = MAGYP_FOB_CONTRACT.dataset
    return ArtifactReference(
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        dataset_key=dataset.key,
        content_hash="6921ac105efddb540edd50aeafe47c11c581f6949111f6342ce7bbf074245741",
        content_retained=False,
        media_type="application/json",
    )


def _identity(*, location: str = "up-river") -> ObservationIdentity:
    dataset = MAGYP_FOB_CONTRACT.dataset
    return ObservationIdentity(
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        dataset_key=dataset.key,
        commodity="soybean",
        product_form="beans",
        location=location,
        price_type="fob",
        currency="USD",
        unit="usd-mt",
        effective_date=date(2026, 8, 10),
    )


def _revision(
    *,
    value: str = "499.50",
    identity: ObservationIdentity | None = None,
    ingested_at: datetime = NOW,
    quality_state: QualityState = QualityState.ACCEPTED,
    public_eligible: bool = True,
    supersedes_revision_id: str | None = None,
) -> ObservationRevision:
    return ObservationRevision(
        identity=identity or _identity(),
        value=Decimal(value),
        ingested_at=ingested_at,
        quality_state=quality_state,
        public_eligible=public_eligible,
        artifact=None if quality_state is QualityState.LEGACY else _artifact(),
        parser_version=None if quality_state is QualityState.LEGACY else "magyp-fob/1.0.0",
        supersedes_revision_id=supersedes_revision_id,
    )


def _run() -> Run:
    return Run(
        code_revision="6da43a8436b33d69f41762bdd72d7139ad415cd1",
        started_at=NOW,
        ended_at=NOW + timedelta(minutes=2),
        status=RunStatus.SUCCEEDED,
    )


def _cache_rows(cache_path: Path) -> list[tuple[str, str, str]]:
    with closing(sqlite3.connect(cache_path)) as conn:
        return conn.execute(
            "SELECT revision_id, value, quality_state FROM trusted_observations ORDER BY revision_id"
        ).fetchall()


def _cache_revision_ids(cache_path: Path) -> tuple[str, ...]:
    return tuple(row[0] for row in _cache_rows(cache_path))


def test_build_query_delete_and_rebuild_matches_repository_heads(
    repository: TrustRepository,
    tmp_path: Path,
) -> None:
    first = _revision(value="499.50")
    replacement = _revision(
        value="501.25",
        ingested_at=NOW + timedelta(minutes=5),
        supersedes_revision_id=first.revision_id,
    )
    other = _revision(value="450.00", identity=_identity(location="rosario"))
    repository.append_observation_revisions((first, replacement, other))
    cache_path = tmp_path / "trusted-query-cache.sqlite"

    first_build = build_query_cache(repository, cache_path)
    first_rows = _cache_rows(cache_path)
    cache_path.unlink()
    second_build = build_query_cache(repository, cache_path)

    assert first_build.revision_count == 2
    assert second_build.revision_count == 2
    assert _cache_rows(cache_path) == first_rows
    assert _cache_revision_ids(cache_path) == tuple(
        sorted(
            (
                repository.current_accepted_revision(first.identity).revision_id,  # type: ignore[union-attr]
                repository.current_accepted_revision(other.identity).revision_id,  # type: ignore[union-attr]
            )
        )
    )


def test_query_cache_excludes_quarantined_rejected_and_legacy_by_default(
    repository: TrustRepository,
    tmp_path: Path,
) -> None:
    accepted = _revision(value="499.50")
    quarantined = _revision(value="501.00", ingested_at=NOW + timedelta(minutes=1), quality_state=QualityState.QUARANTINED)
    rejected = _revision(value="502.00", ingested_at=NOW + timedelta(minutes=2), quality_state=QualityState.REJECTED)
    legacy = _revision(value="498.00", ingested_at=NOW + timedelta(minutes=3), quality_state=QualityState.LEGACY, public_eligible=False)
    repository.append_observation_revisions((accepted, quarantined, rejected, legacy))

    build_query_cache(repository, tmp_path / "default.sqlite")
    explicit_legacy = build_query_cache(repository, tmp_path / "legacy.sqlite", include_legacy=True)

    assert _cache_revision_ids(tmp_path / "default.sqlite") == (accepted.revision_id,)
    assert _cache_revision_ids(tmp_path / "legacy.sqlite") == tuple(sorted((accepted.revision_id, legacy.revision_id)))
    assert explicit_legacy.include_legacy is True


def test_edition_pinned_rebuild_stays_stable_after_newer_revisions_arrive(
    repository: TrustRepository,
    tmp_path: Path,
) -> None:
    pinned = _revision(value="499.50")
    newer = _revision(
        value="505.00",
        ingested_at=NOW + timedelta(minutes=5),
        supersedes_revision_id=pinned.revision_id,
    )
    run = _run()
    edition = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.VERIFIED,
        revision_ids=(pinned.revision_id,),
    )
    repository.store(run)
    repository.append_observation_revision(pinned)
    repository.store(edition)
    cache_path = tmp_path / "edition.sqlite"

    before = build_query_cache(repository, cache_path, mode="edition", edition_id=edition.edition_id)
    before_rows = _cache_rows(cache_path)
    repository.append_observation_revision(newer)
    after = build_query_cache(repository, cache_path, mode="edition", edition_id=edition.edition_id)
    accepted = build_query_cache(repository, tmp_path / "accepted.sqlite")

    assert before.edition_id == edition.edition_id
    assert after.edition_id == edition.edition_id
    assert _cache_rows(cache_path) == before_rows == [(pinned.revision_id, "499.5", "accepted")]
    assert accepted.revision_count == 1
    assert _cache_revision_ids(tmp_path / "accepted.sqlite") == (newer.revision_id,)
