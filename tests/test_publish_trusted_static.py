from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from scripts import publish_trusted_static
from trust import (
    MAGYP_FOB_CONTRACT,
    ArtifactReference,
    DatasetResult,
    DatasetResultStatus,
    Edition,
    EditionStatus,
    FreshnessState,
    ObservationIdentity,
    ObservationRevision,
    QualityState,
    Run,
    RunStatus,
    TemporaryDirectoryTrustRepository,
    ValidationPolicy,
)

NOW = datetime(2026, 8, 10, 12, 30, tzinfo=timezone.utc)


def test_publish_trusted_static_renders_candidate_directory_without_touching_public_docs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_path = tmp_path / "trust"
    edition = _seed_trusted_edition(repository_path, EditionStatus.CANDIDATE)
    public_docs = tmp_path / "docs"
    public_docs.mkdir()
    public_index = public_docs / "index.html"
    public_index.write_text("current public dashboard", encoding="utf-8")
    captured_trust_states = []

    monkeypatch.setattr(
        publish_trusted_static,
        "static_site_candidate_renderer",
        _renderer_factory(captured_trust_states),
    )

    result = publish_trusted_static.publish_trusted_static_edition(
        trust_repository_path=repository_path,
        edition_id=edition.edition_id,
        candidate_root=tmp_path / "candidates",
    )

    candidate_dashboard = tmp_path / "candidates" / edition.edition_id / "index.html"
    assert result.deployed is False
    assert result.deployment_evidence == ()
    assert result.candidate_output_dir == candidate_dashboard.parent
    assert result.generated_artifact_paths == {"dashboard": candidate_dashboard}
    assert result.cache_path == candidate_dashboard.parent / "trusted-query-cache.sqlite"
    assert result.cache_path.is_file()
    assert candidate_dashboard.read_text(encoding="utf-8") == f"<main>{edition.edition_id}</main>"
    assert public_index.read_text(encoding="utf-8") == "current public dashboard"
    assert captured_trust_states[0].edition_id == edition.edition_id
    assert captured_trust_states[0].critical_numbers[0].revision_id == edition.revision_ids[0]


def test_publish_trusted_static_deploys_only_when_explicitly_requested(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_path = tmp_path / "trust"
    edition = _seed_trusted_edition(repository_path, EditionStatus.VERIFIED)
    deployed = []

    monkeypatch.setattr(publish_trusted_static, "static_site_candidate_renderer", _renderer_factory([]))
    monkeypatch.setattr(
        publish_trusted_static,
        "static_site_deployer",
        lambda *, public_dir: _deployer(deployed, public_dir),
    )

    result = publish_trusted_static.publish_trusted_static_edition(
        trust_repository_path=repository_path,
        edition_id=edition.edition_id,
        candidate_root=tmp_path / "candidates",
        deploy=True,
        public_dir=tmp_path / "public",
    )

    assert result.deployed is True
    assert result.deployment_evidence == ("deployed.dashboard.index.html",)
    assert deployed == [(edition.edition_id, tmp_path / "public")]


def test_publish_trusted_static_rejects_non_candidate_or_verified_editions(tmp_path: Path) -> None:
    repository_path = tmp_path / "trust"
    edition = _seed_trusted_edition(repository_path, EditionStatus.DEPLOYMENT_FAILED)

    with pytest.raises(ValueError, match="candidate or verified"):
        publish_trusted_static.publish_trusted_static_edition(
            trust_repository_path=repository_path,
            edition_id=edition.edition_id,
            candidate_root=tmp_path / "candidates",
        )


def _seed_trusted_edition(repository_path: Path, status: EditionStatus) -> Edition:
    repository = TemporaryDirectoryTrustRepository(repository_path)
    revision = _revision()
    run = _run()
    result = _dataset_result(run.run_id, revision)
    run = _run(dataset_result_ids=(result.dataset_result_id,))
    edition = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=status,
        revision_ids=(revision.revision_id,),
    )
    repository.store(run)
    repository.append_observation_revision(revision)
    repository.store(result)
    repository.store(edition)
    return edition


def _run(*, dataset_result_ids: tuple[str, ...] = ()) -> Run:
    return Run(
        code_revision="6da43a8436b33d69f41762bdd72d7139ad415cd1",
        started_at=NOW,
        ended_at=NOW + timedelta(minutes=2),
        status=RunStatus.SUCCEEDED,
        dataset_result_ids=dataset_result_ids,
    )


def _revision() -> ObservationRevision:
    return ObservationRevision(
        identity=_identity(),
        value=Decimal("499.50"),
        ingested_at=NOW,
        quality_state=QualityState.ACCEPTED,
        public_eligible=True,
        artifact=_artifact(),
        parser_version="magyp-fob/1.0.0",
    )


def _identity() -> ObservationIdentity:
    dataset = MAGYP_FOB_CONTRACT.dataset
    return ObservationIdentity(
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        dataset_key=dataset.key,
        commodity="soybean",
        product_form="beans",
        location="up-river",
        price_type="fob",
        currency="USD",
        unit="usd-mt",
        effective_date=NOW.date(),
    )


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


def _dataset_result(run_id: str, revision: ObservationRevision) -> DatasetResult:
    assert revision.artifact is not None
    return DatasetResult(
        run_id=run_id,
        dataset_id=revision.identity.dataset_id,
        status=DatasetResultStatus.SUCCESS,
        candidate_count=1,
        accepted_revision_ids=(revision.revision_id,),
        artifact_references=(revision.artifact,),
        findings=(),
        coverage="1",
        freshness=FreshnessState.CURRENT,
        eligible=True,
        validation_policy=ValidationPolicy(("coverage.minimum", "freshness.maximum-age")),
        as_of_date=NOW.date(),
    )


def _renderer_factory(captured_trust_states):
    def factory(*, public_trust_state, include_players: bool = False):
        captured_trust_states.append(public_trust_state)

        def render(cache_path: Path, output_dir: Path, edition: Edition):
            assert cache_path == output_dir / "trusted-query-cache.sqlite"
            assert include_players is False
            dashboard = output_dir / "index.html"
            dashboard.write_text(f"<main>{edition.edition_id}</main>", encoding="utf-8")
            return {"dashboard": dashboard}

        return render

    return factory


def _deployer(deployed, public_dir: Path):
    def deploy(edition: Edition, render) -> tuple[str, ...]:
        deployed.append((edition.edition_id, public_dir))
        assert render.generated_artifact_paths["dashboard"].is_file()
        return ("deployed.dashboard.index.html",)

    return deploy
