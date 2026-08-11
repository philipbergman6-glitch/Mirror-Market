"""DT-19 tests for building and verifying candidate editions."""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from trust import (
    AGRURAL_PARANAGUA_CONTRACT,
    FX_CONTRACTS,
    MAGYP_FOB_CONTRACT,
    PILOT_REGISTRY,
    SOY_BENCHMARK_CONTRACTS,
    ArtifactReference,
    CandidateEditionRender,
    CandidateEditionVerification,
    CandidateSemanticContract,
    ContractIdentity,
    CriticalEditionContract,
    DatasetContract,
    DatasetResult,
    DatasetResultStatus,
    Edition,
    EditionStatus,
    FreshnessState,
    FxPairIdentity,
    ObservationIdentity,
    ObservationRevision,
    Promotion,
    QualityState,
    Run,
    RunStatus,
    TemporaryDirectoryTrustRepository,
    TrustRepository,
    ValidationPolicy,
    build_and_verify_candidate_edition,
    critical_edition_contract_from_registry,
    render_candidate_edition,
    verify_candidate_edition,
    verify_candidate_generated_artifacts,
    verify_candidate_semantics,
    verify_durable_candidate_edition,
)

NOW = datetime(2026, 8, 10, 12, 30, tzinfo=timezone.utc)
SOURCE_REVISION_ID = "rev_" + "1" * 64
MISSING_SOURCE_REVISION_ID = "rev_" + "2" * 64
MISSING_DERIVED_REVISION_ID = "rev_" + "3" * 64
DERIVED_REVISION_ID = "rev_" + "4" * 64
BRIEFING_HASH = "a" * 64
DASHBOARD_HASH = "b" * 64


@dataclass(frozen=True)
class CriticalRegistryScenario:
    run: Run
    revisions: tuple[ObservationRevision, ...]
    derived_revision: ObservationRevision
    results: tuple[DatasetResult, ...]
    candidate: Edition


def test_fully_valid_candidate_verifies_when_all_required_inputs_outputs_and_artifacts_are_present() -> None:
    candidate = Edition(
        run_id=_run().run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(SOURCE_REVISION_ID,),
        derived_revision_ids=(DERIVED_REVISION_ID,),
        generated_artifact_hashes={
            "briefing": BRIEFING_HASH,
            "dashboard": DASHBOARD_HASH,
        },
    )
    contract = CriticalEditionContract(
        required_revision_ids=(SOURCE_REVISION_ID,),
        required_derived_revision_ids=(DERIVED_REVISION_ID,),
        required_generated_artifact_keys=("briefing", "dashboard"),
    )

    verdict = verify_candidate_edition(candidate, contract)

    assert verdict.verified is True
    assert verdict.status is EditionStatus.VERIFIED


def test_candidate_edition_cannot_verify_without_all_required_inputs_and_derived_outputs() -> None:
    candidate = Edition(
        run_id=_run().run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(SOURCE_REVISION_ID,),
        derived_revision_ids=(),
    )
    contract = CriticalEditionContract(
        required_revision_ids=(SOURCE_REVISION_ID, MISSING_SOURCE_REVISION_ID),
        required_derived_revision_ids=(MISSING_DERIVED_REVISION_ID,),
    )

    verdict = verify_candidate_edition(candidate, contract)

    assert verdict.verified is False
    assert verdict.status is EditionStatus.DEPLOYMENT_FAILED
    assert verdict.missing_revision_ids == (MISSING_SOURCE_REVISION_ID,)
    assert verdict.missing_derived_revision_ids == (MISSING_DERIVED_REVISION_ID,)


def test_candidate_edition_cannot_verify_without_required_generated_artifact_hashes() -> None:
    candidate = Edition(
        run_id=_run().run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(SOURCE_REVISION_ID,),
        derived_revision_ids=(DERIVED_REVISION_ID,),
        generated_artifact_hashes={"briefing": BRIEFING_HASH},
    )
    contract = CriticalEditionContract(
        required_revision_ids=(SOURCE_REVISION_ID,),
        required_derived_revision_ids=(DERIVED_REVISION_ID,),
        required_generated_artifact_keys=("briefing", "dashboard"),
    )

    verdict = verify_candidate_edition(candidate, contract)

    assert verdict.verified is False
    assert verdict.status is EditionStatus.DEPLOYMENT_FAILED
    assert verdict.missing_generated_artifact_keys == ("dashboard",)


def test_durable_candidate_edition_cannot_verify_without_its_run_manifest(tmp_path) -> None:
    repository: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    run = _run()
    candidate = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(SOURCE_REVISION_ID,),
        derived_revision_ids=(DERIVED_REVISION_ID,),
        generated_artifact_hashes={"briefing": BRIEFING_HASH},
    )
    repository.store(candidate)
    contract = CriticalEditionContract(
        required_revision_ids=(SOURCE_REVISION_ID,),
        required_derived_revision_ids=(DERIVED_REVISION_ID,),
        required_generated_artifact_keys=("briefing",),
    )

    verdict = verify_durable_candidate_edition(repository, candidate.edition_id, contract)

    assert verdict.verified is False
    assert verdict.status is EditionStatus.DEPLOYMENT_FAILED
    assert verdict.missing_run_manifest is True


def test_durable_candidate_edition_cannot_verify_without_its_edition_manifest(tmp_path) -> None:
    repository: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "trust")

    verdict = verify_durable_candidate_edition(
        repository,
        "edn_" + "1" * 64,
        CriticalEditionContract(required_revision_ids=(SOURCE_REVISION_ID,)),
    )

    assert verdict.verified is False
    assert verdict.status is EditionStatus.DEPLOYMENT_FAILED
    assert verdict.missing_edition_manifest is True


def test_registry_critical_contract_requires_core_price_and_fx_datasets_not_contextual_supporting_data() -> None:
    contract = critical_edition_contract_from_registry(
        PILOT_REGISTRY,
        required_derived_revision_ids=(DERIVED_REVISION_ID,),
    )

    assert set(contract.required_dataset_ids) == {
        dataset_contract.dataset.dataset_id
        for dataset_contract in (AGRURAL_PARANAGUA_CONTRACT, *SOY_BENCHMARK_CONTRACTS, *FX_CONTRACTS)
    }
    assert contract.required_derived_revision_ids == (DERIVED_REVISION_ID,)
    assert MAGYP_FOB_CONTRACT.dataset.dataset_id not in contract.required_dataset_ids
    assert contract.required_generated_artifact_keys == ("briefing", "dashboard")


def test_registry_critical_contract_rejects_missing_core_price_fx_and_brazil_physical_datasets(tmp_path) -> None:
    for missing_contract in (
        SOY_BENCHMARK_CONTRACTS[0],
        FX_CONTRACTS[0],
        AGRURAL_PARANAGUA_CONTRACT,
    ):
        repository: TrustRepository = TemporaryDirectoryTrustRepository(
            tmp_path / missing_contract.dataset.key / "trust"
        )
        scenario = _critical_registry_scenario(missing_contracts=(missing_contract,))
        repository.store(scenario.run)
        for revision in (*scenario.revisions, scenario.derived_revision):
            repository.append_observation_revision(revision)
        for result in scenario.results:
            repository.store(result)
        repository.store(scenario.candidate)

        verdict = verify_durable_candidate_edition(
            repository,
            scenario.candidate.edition_id,
            critical_edition_contract_from_registry(
                PILOT_REGISTRY,
                required_derived_revision_ids=(scenario.derived_revision.revision_id,),
            ),
        )

        assert verdict.verified is False
        assert verdict.missing_dataset_ids == (missing_contract.dataset.dataset_id,)


def test_registry_critical_contract_rejects_quarantined_dataset_revision(tmp_path) -> None:
    repository: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    scenario = _critical_registry_scenario(
        degraded_revisions={AGRURAL_PARANAGUA_CONTRACT.dataset.dataset_id: QualityState.QUARANTINED}
    )
    repository.store(scenario.run)
    for revision in (*scenario.revisions, scenario.derived_revision):
        repository.append_observation_revision(revision)
    for result in scenario.results:
        repository.store(result)
    repository.store(scenario.candidate)

    verdict = verify_durable_candidate_edition(
        repository,
        scenario.candidate.edition_id,
        critical_edition_contract_from_registry(
            PILOT_REGISTRY,
            required_derived_revision_ids=(scenario.derived_revision.revision_id,),
        ),
    )

    assert verdict.verified is False
    assert verdict.critical_failures == (
        f"dataset-revision.not-public-eligible.{scenario.revisions[0].revision_id}",
        f"dataset-revision.quarantined.{scenario.revisions[0].revision_id}",
        f"derived-input-revision.not-public-eligible.{scenario.revisions[0].revision_id}",
        f"derived-input-revision.quarantined.{scenario.revisions[0].revision_id}",
    )


def test_registry_critical_contract_rejects_current_dataset_without_accepted_revision(tmp_path) -> None:
    repository: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    run_without_results = _run()
    result = _empty_dataset_result_for_contract(run_without_results.run_id, AGRURAL_PARANAGUA_CONTRACT)
    run = _run(dataset_result_ids=(result.dataset_result_id,))
    candidate = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(),
        generated_artifact_hashes={"briefing": BRIEFING_HASH, "dashboard": DASHBOARD_HASH},
    )
    repository.store(run)
    repository.store(result)
    repository.store(candidate)

    verdict = verify_durable_candidate_edition(
        repository,
        candidate.edition_id,
        CriticalEditionContract(required_dataset_ids=(AGRURAL_PARANAGUA_CONTRACT.dataset.dataset_id,)),
    )

    assert verdict.verified is False
    assert verdict.critical_failures == (
        f"dataset.legitimate-empty.{AGRURAL_PARANAGUA_CONTRACT.dataset.dataset_id}",
        f"dataset.no-accepted-revisions.{AGRURAL_PARANAGUA_CONTRACT.dataset.dataset_id}",
    )


def test_valid_candidate_allows_degraded_contextual_dataset_when_critical_datasets_are_current(tmp_path) -> None:
    repository: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    critical_scenario = _critical_registry_scenario()
    critical_contract = critical_edition_contract_from_registry(
        PILOT_REGISTRY,
        required_derived_revision_ids=(critical_scenario.derived_revision.revision_id,),
    )
    contextual_result = _dataset_result_for_contract(
        run_id=critical_scenario.run.run_id,
        contract=MAGYP_FOB_CONTRACT,
        revision_id="rev_" + "f" * 64,
        freshness=FreshnessState.STALE,
    )
    run = _run(dataset_result_ids=tuple(result.dataset_result_id for result in (*critical_scenario.results, contextual_result)))
    candidate = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=tuple(revision.revision_id for revision in critical_scenario.revisions),
        derived_revision_ids=(critical_scenario.derived_revision.revision_id,),
        generated_artifact_hashes={"briefing": BRIEFING_HASH, "dashboard": DASHBOARD_HASH},
    )
    repository.store(run)
    for revision in (*critical_scenario.revisions, critical_scenario.derived_revision):
        repository.append_observation_revision(revision)
    for result in (*critical_scenario.results, contextual_result):
        repository.store(result)
    repository.store(candidate)

    verdict = verify_durable_candidate_edition(repository, candidate.edition_id, critical_contract)

    assert verdict.verified is True
    assert verdict.status is EditionStatus.VERIFIED


def test_durable_candidate_edition_rejects_stale_critical_dataset_result(tmp_path) -> None:
    repository: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    revision = _revision(value="499.50")
    run_without_results = _run()
    result = _dataset_result(run_without_results.run_id, revision, freshness=FreshnessState.STALE)
    run = _run(dataset_result_ids=(result.dataset_result_id,))
    candidate = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(revision.revision_id,),
    )
    repository.store(run)
    repository.append_observation_revision(revision)
    repository.store(result)
    repository.store(candidate)

    verdict = verify_durable_candidate_edition(
        repository,
        candidate.edition_id,
        CriticalEditionContract(required_dataset_ids=(revision.identity.dataset_id,)),
    )

    assert verdict.verified is False
    assert verdict.status is EditionStatus.DEPLOYMENT_FAILED
    assert verdict.critical_failures == (f"dataset.stale.{revision.identity.dataset_id}",)


def test_durable_candidate_edition_rejects_dataset_result_from_another_run(tmp_path) -> None:
    repository: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    revision = _revision(value="499.50")
    other_run = Run(
        code_revision="other",
        started_at=NOW,
        ended_at=NOW + timedelta(minutes=2),
        status=RunStatus.SUCCEEDED,
    )
    result = _dataset_result(other_run.run_id, revision)
    run = _run(dataset_result_ids=(result.dataset_result_id,))
    candidate = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(revision.revision_id,),
    )
    repository.store(run)
    repository.store(other_run)
    repository.append_observation_revision(revision)
    repository.store(result)
    repository.store(candidate)

    verdict = verify_durable_candidate_edition(
        repository,
        candidate.edition_id,
        CriticalEditionContract(required_dataset_ids=(revision.identity.dataset_id,)),
    )

    assert verdict.verified is False
    assert verdict.missing_dataset_ids == (revision.identity.dataset_id,)
    assert verdict.critical_failures == (f"dataset-result.run-mismatch.{revision.identity.dataset_id}",)


def test_durable_candidate_edition_rejects_quarantined_critical_revision(tmp_path) -> None:
    repository: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    revision = _revision(value="499.50", quality_state=QualityState.QUARANTINED)
    run = _run()
    candidate = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(revision.revision_id,),
    )
    repository.store(run)
    repository.append_observation_revision(revision)
    repository.store(candidate)

    verdict = verify_durable_candidate_edition(
        repository,
        candidate.edition_id,
        CriticalEditionContract(required_revision_ids=(revision.revision_id,)),
    )

    assert verdict.verified is False
    assert verdict.status is EditionStatus.DEPLOYMENT_FAILED
    assert verdict.critical_failures == (
        f"revision.not-public-eligible.{revision.revision_id}",
        f"revision.quarantined.{revision.revision_id}",
    )


def test_durable_candidate_edition_rejects_derived_revision_with_quarantined_input(tmp_path) -> None:
    repository: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    input_revision = _revision(value="499.50", quality_state=QualityState.QUARANTINED)
    derived_revision = _derived_revision(input_revision)
    run = _run()
    candidate = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(input_revision.revision_id,),
        derived_revision_ids=(derived_revision.revision_id,),
    )
    repository.store(run)
    repository.append_observation_revision(input_revision)
    repository.append_observation_revision(derived_revision)
    repository.store(candidate)

    verdict = verify_durable_candidate_edition(
        repository,
        candidate.edition_id,
        CriticalEditionContract(required_derived_revision_ids=(derived_revision.revision_id,)),
    )

    assert verdict.verified is False
    assert verdict.critical_failures == (
        f"derived-input-revision.not-public-eligible.{input_revision.revision_id}",
        f"derived-input-revision.quarantined.{input_revision.revision_id}",
    )


def test_candidate_rendering_uses_edition_pinned_cache_and_candidate_output_location(tmp_path) -> None:
    repository: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    pinned = _revision(value="499.50")
    newer = _revision(
        value="505.00",
        ingested_at=NOW + timedelta(minutes=5),
        supersedes_revision_id=pinned.revision_id,
    )
    run = _run()
    candidate = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(pinned.revision_id,),
    )
    repository.store(run)
    repository.append_observation_revision(pinned)
    repository.store(candidate)
    repository.append_observation_revision(newer)
    public_output = tmp_path / "public" / "dashboard.html"
    public_output.parent.mkdir()
    public_output.write_text("current edition", encoding="utf-8")
    observed: dict[str, object] = {}

    def render_from_cache(cache_path: Path, output_dir: Path, edition: Edition) -> tuple[Path, ...]:
        with sqlite3.connect(cache_path) as conn:
            observed["metadata"] = dict(conn.execute("SELECT key, value FROM cache_metadata").fetchall())
            observed["revision_ids"] = tuple(
                row[0] for row in conn.execute("SELECT revision_id FROM trusted_observations").fetchall()
            )
        output = output_dir / "dashboard.html"
        output.write_text(edition.edition_id, encoding="utf-8")
        return (output,)

    rendered = render_candidate_edition(
        repository,
        candidate.edition_id,
        tmp_path / "candidates",
        render_from_cache,
    )

    assert isinstance(rendered, CandidateEditionRender)
    assert rendered.edition_id == candidate.edition_id
    assert rendered.cache_build.mode == "edition"
    assert rendered.cache_build.edition_id == candidate.edition_id
    assert rendered.output_dir == tmp_path / "candidates" / candidate.edition_id
    assert rendered.generated_artifact_paths == {"dashboard": rendered.output_dir / "dashboard.html"}
    assert rendered.generated_paths == (rendered.output_dir / "dashboard.html",)
    assert (rendered.output_dir / "dashboard.html").read_text(encoding="utf-8") == candidate.edition_id
    assert public_output.read_text(encoding="utf-8") == "current edition"
    assert observed["metadata"] == {
        "mode": "edition",
        "edition_id": candidate.edition_id,
        "include_legacy": "0",
        "revision_count": "1",
    }
    assert observed["revision_ids"] == (pinned.revision_id,)


def test_candidate_renderer_can_map_dashboard_artifact_to_existing_static_index_filename(tmp_path) -> None:
    repository: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    pinned = _revision(value="499.50")
    run = _run()
    candidate = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(pinned.revision_id,),
    )
    candidate = _with_generated_artifact_hashes(candidate, dashboard_name="index.html")
    repository.store(run)
    repository.append_observation_revision(pinned)
    repository.store(candidate)

    def render_static_site(cache_path: Path, output_dir: Path, edition: Edition) -> dict[str, Path]:
        del cache_path
        briefing = output_dir / "briefing.md"
        dashboard = output_dir / "index.html"
        briefing.write_bytes(_briefing_bytes(edition.edition_id))
        dashboard.write_bytes(_dashboard_bytes(edition.edition_id))
        return {"briefing": briefing, "dashboard": dashboard}

    result = build_and_verify_candidate_edition(
        repository,
        candidate.edition_id,
        tmp_path / "candidates",
        render_static_site,
        CriticalEditionContract(required_revision_ids=(pinned.revision_id,)),
        CandidateSemanticContract(required_briefing_sections=("prices", "freshness", "quality")),
    )

    assert result.verdict.verified is True
    assert result.render is not None
    assert result.render.generated_artifact_paths["dashboard"].name == "index.html"


def test_candidate_generated_artifact_verification_rejects_hash_mismatch(tmp_path) -> None:
    briefing = tmp_path / "briefing.md"
    dashboard = tmp_path / "dashboard.html"
    briefing.write_text("Edition briefing", encoding="utf-8")
    dashboard.write_text("Dashboard content", encoding="utf-8")
    candidate = Edition(
        run_id=_run().run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(SOURCE_REVISION_ID,),
        generated_artifact_hashes={
            "briefing": "b5f054a37bacf864e68ad3565a104468a3942a3fe7d0624ac85dda336cb23180",
            "dashboard": DASHBOARD_HASH,
        },
    )

    verdict = verify_candidate_generated_artifacts(
        candidate,
        {
            "briefing": briefing,
            "dashboard": dashboard,
        },
    )

    assert verdict.verified is False
    assert verdict.status is EditionStatus.DEPLOYMENT_FAILED
    assert verdict.mismatched_generated_artifact_keys == ("dashboard",)


def test_valid_briefing_can_contain_the_word_failed_without_semantic_rejection(tmp_path) -> None:
    candidate = Edition(
        run_id=_run().run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(SOURCE_REVISION_ID,),
    )
    briefing = tmp_path / "briefing.md"
    dashboard = tmp_path / "dashboard.html"
    briefing.write_text(
        "\n".join(
            (
                "# Trader Briefing",
                f"Edition: {candidate.edition_id}",
                "## Prices",
                "The word failed appears here as ordinary market commentary, not a render failure.",
                "## Freshness",
                "Freshness summary: all critical datasets are current.",
                "## Quality",
                "Quality summary: critical observations are accepted.",
            )
        ),
        encoding="utf-8",
    )
    dashboard.write_text(
        f"<main data-edition-id='{candidate.edition_id}'>Freshness summary Quality summary</main>",
        encoding="utf-8",
    )
    contract = CandidateSemanticContract(required_briefing_sections=("prices", "freshness", "quality"))

    verdict = verify_candidate_semantics(candidate, briefing, dashboard, contract)

    assert verdict.verified is True
    assert verdict.status is EditionStatus.VERIFIED


def test_candidate_semantics_reject_missing_required_briefing_section(tmp_path) -> None:
    candidate = Edition(
        run_id=_run().run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(SOURCE_REVISION_ID,),
    )
    briefing = tmp_path / "briefing.md"
    dashboard = tmp_path / "dashboard.html"
    briefing.write_text(
        "\n".join(
            (
                "# Trader Briefing",
                f"Edition: {candidate.edition_id}",
                "## Prices",
                "## Freshness",
                "Freshness summary: all critical datasets are current.",
                "Quality summary: critical observations are accepted.",
            )
        ),
        encoding="utf-8",
    )
    dashboard.write_text(
        f"<main data-edition-id='{candidate.edition_id}'>Freshness summary Quality summary</main>",
        encoding="utf-8",
    )
    contract = CandidateSemanticContract(required_briefing_sections=("prices", "freshness", "quality"))

    verdict = verify_candidate_semantics(candidate, briefing, dashboard, contract)

    assert verdict.verified is False
    assert verdict.status is EditionStatus.DEPLOYMENT_FAILED
    assert verdict.semantic_failures == ("briefing.section.quality",)


def test_failed_semantic_checks_create_rejected_edition_manifest_and_preserve_current_edition(tmp_path) -> None:
    repository: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    pinned = _revision(value="499.50")
    run = _run()
    current_edition = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=1),
        status=EditionStatus.VERIFIED,
        revision_ids=(pinned.revision_id,),
    )
    candidate = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(pinned.revision_id,),
    )
    current = Promotion(
        edition_id=current_edition.edition_id,
        promoted_at=NOW + timedelta(minutes=2),
        verification_evidence=("edition-contract-check",),
    )
    candidate = _with_generated_artifact_hashes(candidate, include_quality_section=False)
    repository.store(run)
    repository.append_observation_revision(pinned)
    repository.store(current_edition)
    repository.store(candidate)
    repository.replace_current_edition(current)

    def render_missing_quality_section(cache_path: Path, output_dir: Path, edition: Edition) -> tuple[Path, ...]:
        del cache_path
        briefing = output_dir / "briefing.md"
        dashboard = output_dir / "dashboard.html"
        briefing.write_text(
            "\n".join(
                (
                    "# Trader Briefing",
                    f"Edition: {edition.edition_id}",
                    "## Prices",
                    "## Freshness",
                    "Freshness summary: all critical datasets are current.",
                    "Quality summary: critical observations are accepted.",
                )
            ),
            encoding="utf-8",
        )
        dashboard.write_text(
            f"<main data-edition-id='{edition.edition_id}'>Freshness summary Quality summary</main>",
            encoding="utf-8",
        )
        return (briefing, dashboard)

    result = build_and_verify_candidate_edition(
        repository,
        candidate.edition_id,
        tmp_path / "candidates",
        render_missing_quality_section,
        CriticalEditionContract(required_revision_ids=(pinned.revision_id,)),
        CandidateSemanticContract(required_briefing_sections=("prices", "freshness", "quality")),
    )

    assert isinstance(result, CandidateEditionVerification)
    assert result.verdict.verified is False
    assert result.verdict.semantic_failures == ("briefing.section.quality",)
    assert result.failed_edition_id is not None
    failed = repository.read(Edition, result.failed_edition_id)
    assert failed is not None
    assert failed.status is EditionStatus.DEPLOYMENT_FAILED
    assert failed.revision_ids == candidate.revision_ids
    assert repository.current_edition() == current


def test_pre_render_critical_failure_creates_rejected_manifest_without_rendering(tmp_path) -> None:
    repository: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    pinned = _revision(value="499.50")
    run = _run()
    candidate = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(pinned.revision_id,),
    )
    repository.store(run)
    repository.append_observation_revision(pinned)
    repository.store(candidate)
    rendered = False

    def renderer(cache_path: Path, output_dir: Path, edition: Edition) -> tuple[Path, ...]:
        nonlocal rendered
        del cache_path, output_dir, edition
        rendered = True
        return ()

    result = build_and_verify_candidate_edition(
        repository,
        candidate.edition_id,
        tmp_path / "candidates",
        renderer,
        CriticalEditionContract(required_revision_ids=(MISSING_SOURCE_REVISION_ID,)),
        CandidateSemanticContract(required_briefing_sections=("prices", "freshness", "quality")),
    )

    assert rendered is False
    assert result.render is None
    assert result.verdict.verified is False
    assert result.verdict.missing_revision_ids == (MISSING_SOURCE_REVISION_ID,)
    assert result.failed_edition_id is not None
    failed = repository.read(Edition, result.failed_edition_id)
    assert failed is not None
    assert failed.status is EditionStatus.DEPLOYMENT_FAILED


def test_missing_briefing_artifact_returns_failed_verdict_and_rejected_manifest(tmp_path) -> None:
    repository: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    pinned = _revision(value="499.50")
    run = _run()
    candidate = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(pinned.revision_id,),
        generated_artifact_hashes={"briefing": BRIEFING_HASH, "dashboard": _hash_bytes(_dashboard_bytes(""))},
    )
    repository.store(run)
    repository.append_observation_revision(pinned)
    repository.store(candidate)

    def render_dashboard_only(cache_path: Path, output_dir: Path, edition: Edition) -> tuple[Path, ...]:
        del cache_path
        dashboard = output_dir / "dashboard.html"
        dashboard.write_bytes(_dashboard_bytes(edition.edition_id))
        return (dashboard,)

    result = build_and_verify_candidate_edition(
        repository,
        candidate.edition_id,
        tmp_path / "candidates",
        render_dashboard_only,
        CriticalEditionContract(required_revision_ids=(pinned.revision_id,)),
        CandidateSemanticContract(required_briefing_sections=("prices", "freshness", "quality")),
    )

    assert result.verdict.verified is False
    assert result.verdict.missing_generated_artifact_keys == ("briefing",)
    assert result.verdict.semantic_failures == ("briefing.missing",)
    assert result.failed_edition_id is not None


def test_returned_but_unwritten_artifact_returns_failed_verdict_and_rejected_manifest(tmp_path) -> None:
    repository: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    pinned = _revision(value="499.50")
    run = _run()
    candidate = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(pinned.revision_id,),
        generated_artifact_hashes={
            "briefing": BRIEFING_HASH,
            "dashboard": _hash_bytes(_dashboard_bytes("")),
        },
    )
    repository.store(run)
    repository.append_observation_revision(pinned)
    repository.store(candidate)

    def render_unwritten_briefing(cache_path: Path, output_dir: Path, edition: Edition) -> dict[str, Path]:
        del cache_path
        dashboard = output_dir / "dashboard.html"
        dashboard.write_bytes(_dashboard_bytes(edition.edition_id))
        return {"briefing": output_dir / "briefing.md", "dashboard": dashboard}

    result = build_and_verify_candidate_edition(
        repository,
        candidate.edition_id,
        tmp_path / "candidates",
        render_unwritten_briefing,
        CriticalEditionContract(required_revision_ids=(pinned.revision_id,)),
        CandidateSemanticContract(required_briefing_sections=("prices", "freshness", "quality")),
    )

    assert result.verdict.verified is False
    assert result.verdict.missing_generated_artifact_keys == ("briefing",)
    assert result.verdict.semantic_failures == ("briefing.missing",)
    assert result.failed_edition_id is not None


def test_missing_generated_artifact_hash_blocks_integrated_verification(tmp_path) -> None:
    repository: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    pinned = _revision(value="499.50")
    run = _run()
    candidate = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(pinned.revision_id,),
        generated_artifact_hashes={"briefing": _hash_bytes(_briefing_bytes(""))},
    )
    repository.store(run)
    repository.append_observation_revision(pinned)
    repository.store(candidate)

    result = build_and_verify_candidate_edition(
        repository,
        candidate.edition_id,
        tmp_path / "candidates",
        _render_valid_candidate,
        CriticalEditionContract(required_revision_ids=(pinned.revision_id,)),
        CandidateSemanticContract(required_briefing_sections=("prices", "freshness", "quality")),
    )

    assert result.verdict.verified is False
    assert result.verdict.missing_generated_artifact_keys == ("dashboard",)
    assert result.verdict.mismatched_generated_artifact_keys == ("briefing",)
    assert result.failed_edition_id is not None


def test_generated_artifact_hash_mismatch_blocks_integrated_verification(tmp_path) -> None:
    repository: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    pinned = _revision(value="499.50")
    run = _run()
    candidate = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(pinned.revision_id,),
        generated_artifact_hashes={"briefing": BRIEFING_HASH, "dashboard": DASHBOARD_HASH},
    )
    repository.store(run)
    repository.append_observation_revision(pinned)
    repository.store(candidate)

    result = build_and_verify_candidate_edition(
        repository,
        candidate.edition_id,
        tmp_path / "candidates",
        _render_valid_candidate,
        CriticalEditionContract(required_revision_ids=(pinned.revision_id,)),
        CandidateSemanticContract(required_briefing_sections=("prices", "freshness", "quality")),
    )

    assert result.verdict.verified is False
    assert result.verdict.mismatched_generated_artifact_keys == ("briefing", "dashboard")
    assert result.failed_edition_id is not None
    assert result.verified_edition_id is None


def test_successful_candidate_verification_creates_verified_manifest_without_promoting(tmp_path) -> None:
    repository: TrustRepository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    pinned = _revision(value="499.50")
    run = _run()
    candidate = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=(pinned.revision_id,),
    )
    candidate = _with_generated_artifact_hashes(candidate)
    repository.store(run)
    repository.append_observation_revision(pinned)
    repository.store(candidate)

    result = build_and_verify_candidate_edition(
        repository,
        candidate.edition_id,
        tmp_path / "candidates",
        _render_valid_candidate,
        CriticalEditionContract(required_revision_ids=(pinned.revision_id,)),
        CandidateSemanticContract(required_briefing_sections=("prices", "freshness", "quality")),
    )

    assert result.verdict.verified is True
    assert result.verified_edition_id is not None
    verified = repository.read(Edition, result.verified_edition_id)
    assert verified is not None
    assert verified.status is EditionStatus.VERIFIED
    assert verified.revision_ids == candidate.revision_ids
    assert result.failed_edition_id is None
    assert repository.current_edition() is None


def _run(*, dataset_result_ids: tuple[str, ...] = ()) -> Run:
    return Run(
        code_revision="6da43a8436b33d69f41762bdd72d7139ad415cd1",
        started_at=NOW,
        ended_at=NOW + timedelta(minutes=2),
        status=RunStatus.SUCCEEDED,
        dataset_result_ids=dataset_result_ids,
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


def _revision(
    *,
    value: str,
    ingested_at: datetime = NOW,
    quality_state: QualityState = QualityState.ACCEPTED,
    supersedes_revision_id: str | None = None,
) -> ObservationRevision:
    return ObservationRevision(
        identity=_identity(),
        value=Decimal(value),
        ingested_at=ingested_at,
        quality_state=quality_state,
        public_eligible=quality_state is QualityState.ACCEPTED,
        artifact=_artifact(),
        parser_version="magyp-fob/1.0.0",
        supersedes_revision_id=supersedes_revision_id,
    )


def _derived_revision(input_revision: ObservationRevision) -> ObservationRevision:
    return ObservationRevision(
        identity=input_revision.identity,
        value=Decimal("499.50"),
        ingested_at=NOW + timedelta(minutes=1),
        quality_state=QualityState.ACCEPTED,
        public_eligible=True,
        calculation_id="usd-mt-conversion",
        calculation_version="1.0.0",
        input_revision_ids=(input_revision.revision_id,),
    )


def _dataset_result(
    run_id: str,
    revision: ObservationRevision,
    *,
    freshness: FreshnessState = FreshnessState.CURRENT,
) -> DatasetResult:
    return DatasetResult(
        run_id=run_id,
        dataset_id=revision.identity.dataset_id,
        status=DatasetResultStatus.SUCCESS,
        candidate_count=1,
        accepted_revision_ids=(revision.revision_id,),
        artifact_references=(revision.artifact,),
        findings=(),
        coverage="1",
        freshness=freshness,
        eligible=True,
        validation_policy=ValidationPolicy(("coverage.minimum", "freshness.maximum-age")),
        as_of_date=NOW.date(),
    )


def _empty_dataset_result_for_contract(run_id: str, contract: DatasetContract) -> DatasetResult:
    artifact = ArtifactReference(
        source_id=contract.dataset.source_id,
        dataset_id=contract.dataset.dataset_id,
        dataset_key=contract.dataset.key,
        content_hash=_hash_bytes((contract.dataset.dataset_id + ":empty").encode()),
        content_retained=False,
        media_type="text/plain",
    )
    return DatasetResult(
        run_id=run_id,
        dataset_id=contract.dataset.dataset_id,
        status=DatasetResultStatus.LEGITIMATE_EMPTY,
        candidate_count=0,
        accepted_revision_ids=(),
        artifact_references=(artifact,),
        findings=(),
        coverage="1",
        freshness=FreshnessState.CURRENT,
        eligible=True,
        validation_policy=ValidationPolicy(("identity.required",), allows_empty_publication=True),
        as_of_date=NOW.date(),
    )


def _dataset_result_for_contract(
    *,
    run_id: str,
    contract: DatasetContract,
    revision_id: str,
    freshness: FreshnessState = FreshnessState.CURRENT,
) -> DatasetResult:
    artifact = ArtifactReference(
        source_id=contract.dataset.source_id,
        dataset_id=contract.dataset.dataset_id,
        dataset_key=contract.dataset.key,
        content_hash=_hash_bytes(contract.dataset.dataset_id.encode()),
        content_retained=False,
        media_type="text/plain",
    )
    return DatasetResult(
        run_id=run_id,
        dataset_id=contract.dataset.dataset_id,
        status=DatasetResultStatus.SUCCESS,
        candidate_count=1,
        accepted_revision_ids=(revision_id,),
        artifact_references=(artifact,),
        findings=(),
        coverage="1",
        freshness=freshness,
        eligible=True,
        validation_policy=ValidationPolicy(("identity.required", "value.positive")),
        as_of_date=NOW.date(),
    )


def _critical_registry_scenario(
    *,
    missing_contracts: tuple[DatasetContract, ...] = (),
    degraded_revisions: dict[str, QualityState] | None = None,
) -> CriticalRegistryScenario:
    contracts = tuple(
        contract
        for contract in (AGRURAL_PARANAGUA_CONTRACT, *SOY_BENCHMARK_CONTRACTS, *FX_CONTRACTS)
        if contract not in missing_contracts
    )
    degraded_revisions = degraded_revisions or {}
    revisions = tuple(
        _revision_for_contract(
            contract,
            quality_state=degraded_revisions.get(contract.dataset.dataset_id, QualityState.ACCEPTED),
        )
        for contract in contracts
    )
    run = _run()
    results = tuple(
        _dataset_result_for_contract(
            run_id=run.run_id,
            contract=contract,
            revision_id=revision.revision_id,
        )
        for contract, revision in zip(contracts, revisions, strict=True)
    )
    derived_input = revisions[0] if revisions else _revision(value="499.50")
    derived_revision = _derived_revision(derived_input)
    run = _run(dataset_result_ids=tuple(result.dataset_result_id for result in results))
    candidate = Edition(
        run_id=run.run_id,
        created_at=NOW + timedelta(minutes=3),
        status=EditionStatus.CANDIDATE,
        revision_ids=tuple(revision.revision_id for revision in revisions),
        derived_revision_ids=(derived_revision.revision_id,),
        generated_artifact_hashes={"briefing": BRIEFING_HASH, "dashboard": DASHBOARD_HASH},
    )
    return CriticalRegistryScenario(
        run=run,
        revisions=revisions,
        derived_revision=derived_revision,
        results=results,
        candidate=candidate,
    )


def _revision_for_contract(
    contract: DatasetContract,
    *,
    quality_state: QualityState = QualityState.ACCEPTED,
) -> ObservationRevision:
    identity = _identity_for_contract(contract)
    artifact = ArtifactReference(
        source_id=contract.dataset.source_id,
        dataset_id=contract.dataset.dataset_id,
        dataset_key=contract.dataset.key,
        content_hash=_hash_bytes((contract.dataset.dataset_id + identity.observation_id).encode()),
        content_retained=False,
        media_type="text/plain",
    )
    return ObservationRevision(
        identity=identity,
        value=Decimal("499.50"),
        ingested_at=NOW,
        quality_state=quality_state,
        public_eligible=quality_state is QualityState.ACCEPTED,
        artifact=artifact,
        parser_version=f"{contract.dataset.key}/test",
    )


def _identity_for_contract(contract: DatasetContract) -> ObservationIdentity:
    assert contract.identity is not None
    fixed = contract.identity.fixed_fields
    fx_pair = None
    if "fx_pair" in contract.identity.required_fields:
        base, quote = fixed["product_form"].split("-")
        fx_pair = FxPairIdentity(base_currency=base.upper(), quote_currency=quote.upper())
    named_contract = None
    if "contract" in contract.identity.required_fields:
        named_contract = ContractIdentity(exchange=fixed.get("venue", "cbot"), code="ZS", delivery_month="2026-08")
    return ObservationIdentity(
        source_id=contract.dataset.source_id,
        dataset_id=contract.dataset.dataset_id,
        dataset_key=contract.dataset.key,
        commodity=fixed.get("commodity", "soybean"),
        product_form=fixed.get("product_form", "beans"),
        venue=fixed.get("venue"),
        location=fixed.get("location"),
        price_type=fixed.get("price_type", "settlement"),
        currency=fixed.get("currency", "usd"),
        unit=fixed.get("unit", contract.units[0]),
        contract=named_contract,
        fx_pair=fx_pair,
        effective_date=NOW.date(),
    )


def _render_valid_candidate(cache_path: Path, output_dir: Path, edition: Edition) -> tuple[Path, ...]:
    del cache_path
    briefing = output_dir / "briefing.md"
    dashboard = output_dir / "dashboard.html"
    briefing.write_bytes(_briefing_bytes(edition.edition_id))
    dashboard.write_bytes(_dashboard_bytes(edition.edition_id))
    return (briefing, dashboard)


def _with_generated_artifact_hashes(
    candidate: Edition,
    *,
    include_quality_section: bool = True,
    dashboard_name: str = "dashboard.html",
) -> Edition:
    dashboard_key = "dashboard" if dashboard_name == "index.html" else Path(dashboard_name).stem
    return Edition(
        run_id=candidate.run_id,
        created_at=candidate.created_at,
        status=candidate.status,
        revision_ids=candidate.revision_ids,
        derived_revision_ids=candidate.derived_revision_ids,
        generated_artifact_hashes={
            "briefing": _hash_bytes(
                _briefing_bytes(candidate.edition_id, include_quality_section=include_quality_section)
            ),
            dashboard_key: _hash_bytes(_dashboard_bytes(candidate.edition_id)),
        },
    )


def _briefing_bytes(edition_id: str, *, include_quality_section: bool = True) -> bytes:
    lines = [
        "# Trader Briefing",
        f"Edition: {edition_id}",
        "## Prices",
        "## Freshness",
        "Freshness summary: all critical datasets are current.",
    ]
    if include_quality_section:
        lines.append("## Quality")
    lines.append("Quality summary: critical observations are accepted.")
    return "\n".join(lines).encode("utf-8")


def _dashboard_bytes(edition_id: str) -> bytes:
    return f"<main data-edition-id='{edition_id}'>Freshness summary Quality summary</main>".encode()


def _hash_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()
