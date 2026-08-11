"""Candidate edition verification for the Data Trust Foundation."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from trust.domain import (
    DatasetResult,
    DatasetResultStatus,
    Edition,
    EditionStatus,
    FreshnessState,
    ObservationRevision,
    QualityState,
    Run,
)
from trust.query_cache import QueryCacheBuild, build_query_cache
from trust.registry import ContractRegistry, Criticality

if TYPE_CHECKING:
    from trust.repository import TrustRepository

_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
_REVISION_ID_RE = re.compile(r"^rev_[0-9a-f]{64}$")
_DATASET_ID_RE = re.compile(r"^dst_[0-9a-f]{64}$")
GeneratedArtifactPaths = Mapping[str, str | Path] | tuple[Path, ...]
CandidateRenderer = Callable[[Path, Path, Edition], GeneratedArtifactPaths]


def _revision_ids(values: tuple[str, ...], field_name: str) -> tuple[str, ...]:
    normalized = tuple(str(value) for value in values)
    if any(not _REVISION_ID_RE.fullmatch(value) for value in normalized):
        raise ValueError(f"{field_name} must contain revision identifiers")
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} cannot contain duplicates")
    return tuple(sorted(normalized))


def _keys(values: tuple[str, ...], field_name: str) -> tuple[str, ...]:
    normalized = tuple(str(value).strip().lower() for value in values)
    if any(not _KEY_RE.fullmatch(value) for value in normalized):
        raise ValueError(f"{field_name} must contain artifact keys")
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} cannot contain duplicates")
    return tuple(sorted(normalized))


def _dataset_ids(values: tuple[str, ...], field_name: str) -> tuple[str, ...]:
    normalized = tuple(str(value) for value in values)
    if any(not _DATASET_ID_RE.fullmatch(value) for value in normalized):
        raise ValueError(f"{field_name} must contain dataset identifiers")
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} cannot contain duplicates")
    return tuple(sorted(normalized))


def _reason_codes(values: tuple[str, ...], field_name: str) -> tuple[str, ...]:
    normalized = tuple(str(value).strip().lower() for value in values)
    if any(not re.fullmatch(r"[a-z0-9][a-z0-9._-]*", value) for value in normalized):
        raise ValueError(f"{field_name} must contain reason codes")
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} cannot contain duplicates")
    return tuple(sorted(normalized))


@dataclass(frozen=True)
class CriticalEditionContract:
    """The source revisions, derived revisions, and artifacts required before verification."""

    required_revision_ids: tuple[str, ...] = ()
    required_derived_revision_ids: tuple[str, ...] = ()
    required_generated_artifact_keys: tuple[str, ...] = ()
    required_dataset_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "required_revision_ids",
            _revision_ids(self.required_revision_ids, "critical_edition_contract.required_revision_ids"),
        )
        object.__setattr__(
            self,
            "required_derived_revision_ids",
            _revision_ids(
                self.required_derived_revision_ids,
                "critical_edition_contract.required_derived_revision_ids",
            ),
        )
        object.__setattr__(
            self,
            "required_generated_artifact_keys",
            _keys(
                self.required_generated_artifact_keys,
                "critical_edition_contract.required_generated_artifact_keys",
            ),
        )
        object.__setattr__(
            self,
            "required_dataset_ids",
            _dataset_ids(
                self.required_dataset_ids,
                "critical_edition_contract.required_dataset_ids",
            ),
        )


@dataclass(frozen=True)
class EditionVerificationVerdict:
    """Semantic verification result for a candidate edition manifest."""

    verified: bool
    status: EditionStatus
    missing_revision_ids: tuple[str, ...] = ()
    missing_derived_revision_ids: tuple[str, ...] = ()
    missing_generated_artifact_keys: tuple[str, ...] = ()
    mismatched_generated_artifact_keys: tuple[str, ...] = ()
    semantic_failures: tuple[str, ...] = ()
    missing_dataset_ids: tuple[str, ...] = ()
    critical_failures: tuple[str, ...] = ()
    missing_edition_manifest: bool = False
    missing_run_manifest: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", EditionStatus(self.status))
        if not isinstance(self.missing_edition_manifest, bool):
            raise ValueError("edition_verification.missing_edition_manifest must be a boolean")
        if not isinstance(self.missing_run_manifest, bool):
            raise ValueError("edition_verification.missing_run_manifest must be a boolean")
        object.__setattr__(
            self,
            "missing_revision_ids",
            _revision_ids(self.missing_revision_ids, "edition_verification.missing_revision_ids"),
        )
        object.__setattr__(
            self,
            "missing_derived_revision_ids",
            _revision_ids(
                self.missing_derived_revision_ids,
                "edition_verification.missing_derived_revision_ids",
            ),
        )
        object.__setattr__(
            self,
            "missing_generated_artifact_keys",
            _keys(
                self.missing_generated_artifact_keys,
                "edition_verification.missing_generated_artifact_keys",
            ),
        )
        object.__setattr__(
            self,
            "mismatched_generated_artifact_keys",
            _keys(
                self.mismatched_generated_artifact_keys,
                "edition_verification.mismatched_generated_artifact_keys",
            ),
        )
        object.__setattr__(
            self,
            "semantic_failures",
            _reason_codes(self.semantic_failures, "edition_verification.semantic_failures"),
        )
        object.__setattr__(
            self,
            "missing_dataset_ids",
            _dataset_ids(self.missing_dataset_ids, "edition_verification.missing_dataset_ids"),
        )
        object.__setattr__(
            self,
            "critical_failures",
            _reason_codes(self.critical_failures, "edition_verification.critical_failures"),
        )
        if self.verified and (
            self.status is not EditionStatus.VERIFIED
            or self.missing_revision_ids
            or self.missing_derived_revision_ids
            or self.missing_generated_artifact_keys
            or self.mismatched_generated_artifact_keys
            or self.semantic_failures
            or self.missing_dataset_ids
            or self.critical_failures
            or self.missing_edition_manifest
            or self.missing_run_manifest
        ):
            raise ValueError("verified edition verdicts cannot contain missing requirements")
        if not self.verified and self.status is EditionStatus.VERIFIED:
            raise ValueError("failed edition verdicts cannot use verified status")


@dataclass(frozen=True)
class CandidateEditionRender:
    """Rendered candidate output and the edition-pinned cache used to produce it."""

    edition_id: str
    output_dir: Path
    cache_build: QueryCacheBuild
    generated_artifact_paths: Mapping[str, Path]

    def __post_init__(self) -> None:
        if not re.fullmatch(r"^edn_[0-9a-f]{64}$", self.edition_id):
            raise ValueError("candidate_render.edition_id must be an edition identifier")
        object.__setattr__(self, "output_dir", Path(self.output_dir))
        generated_artifact_paths = {
            _keys((key,), "candidate_render.generated_artifact_paths key")[0]: Path(path)
            for key, path in self.generated_artifact_paths.items()
        }
        object.__setattr__(self, "generated_artifact_paths", generated_artifact_paths)

    @property
    def generated_paths(self) -> tuple[Path, ...]:
        return tuple(self.generated_artifact_paths[key] for key in sorted(self.generated_artifact_paths))


@dataclass(frozen=True)
class CandidateEditionVerification:
    """End-to-end candidate verification result."""

    edition_id: str
    render: CandidateEditionRender | None
    verdict: EditionVerificationVerdict
    verified_edition_id: str | None = None
    failed_edition_id: str | None = None

    def __post_init__(self) -> None:
        if not re.fullmatch(r"^edn_[0-9a-f]{64}$", self.edition_id):
            raise ValueError("candidate_verification.edition_id must be an edition identifier")
        if self.verified_edition_id is not None and not re.fullmatch(
            r"^edn_[0-9a-f]{64}$",
            self.verified_edition_id,
        ):
            raise ValueError("candidate_verification.verified_edition_id must be an edition identifier")
        if self.failed_edition_id is not None and not re.fullmatch(r"^edn_[0-9a-f]{64}$", self.failed_edition_id):
            raise ValueError("candidate_verification.failed_edition_id must be an edition identifier")


@dataclass(frozen=True)
class CandidateSemanticContract:
    """Semantic assertions required for rendered candidate artifacts."""

    required_briefing_sections: tuple[str, ...]
    require_edition_id: bool = True
    require_freshness_summary: bool = True
    require_quality_summary: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "required_briefing_sections",
            _keys(self.required_briefing_sections, "candidate_semantics.required_briefing_sections"),
        )
        if not isinstance(self.require_edition_id, bool):
            raise ValueError("candidate_semantics.require_edition_id must be a boolean")
        if not isinstance(self.require_freshness_summary, bool):
            raise ValueError("candidate_semantics.require_freshness_summary must be a boolean")
        if not isinstance(self.require_quality_summary, bool):
            raise ValueError("candidate_semantics.require_quality_summary must be a boolean")


def verify_candidate_edition(
    edition: Edition,
    contract: CriticalEditionContract,
) -> EditionVerificationVerdict:
    """Verify that a candidate manifest pins every critical source and derived output."""

    if edition.status is not EditionStatus.CANDIDATE:
        raise ValueError("only candidate editions can be verified")

    missing_revision_ids = tuple(
        revision_id
        for revision_id in contract.required_revision_ids
        if revision_id not in edition.revision_ids
    )
    missing_derived_revision_ids = tuple(
        revision_id
        for revision_id in contract.required_derived_revision_ids
        if revision_id not in edition.derived_revision_ids
    )
    missing_generated_artifact_keys = tuple(
        key
        for key in contract.required_generated_artifact_keys
        if key not in edition.generated_artifact_hashes
    )
    if (
        missing_revision_ids
        or missing_derived_revision_ids
        or missing_generated_artifact_keys
    ):
        return EditionVerificationVerdict(
            verified=False,
            status=EditionStatus.DEPLOYMENT_FAILED,
            missing_revision_ids=missing_revision_ids,
            missing_derived_revision_ids=missing_derived_revision_ids,
            missing_generated_artifact_keys=missing_generated_artifact_keys,
        )
    return EditionVerificationVerdict(verified=True, status=EditionStatus.VERIFIED)


def critical_edition_contract_from_registry(
    registry: ContractRegistry,
    *,
    required_derived_revision_ids: tuple[str, ...],
    required_generated_artifact_keys: tuple[str, ...] = ("briefing", "dashboard"),
) -> CriticalEditionContract:
    """Build the dataset-level candidate gate from registry criticality."""

    return CriticalEditionContract(
        required_derived_revision_ids=required_derived_revision_ids,
        required_dataset_ids=tuple(
            contract.dataset.dataset_id
            for contract in registry.datasets
            if contract.criticality is Criticality.CRITICAL
        ),
        required_generated_artifact_keys=required_generated_artifact_keys,
    )


def verify_candidate_semantics(
    edition: Edition,
    briefing_path: str | Path,
    dashboard_path: str | Path,
    contract: CandidateSemanticContract,
) -> EditionVerificationVerdict:
    """Verify required semantic content in rendered briefing and dashboard artifacts."""

    briefing = Path(briefing_path).read_text(encoding="utf-8")
    dashboard = Path(dashboard_path).read_text(encoding="utf-8")
    failures: list[str] = []
    normalized_briefing = briefing.lower()
    normalized_dashboard = dashboard.lower()

    if not briefing.strip():
        failures.append("briefing.empty")
    if not dashboard.strip():
        failures.append("dashboard.empty")
    if contract.require_edition_id and (
        edition.edition_id not in briefing
        or edition.edition_id not in dashboard
    ):
        failures.append("edition-id.missing")
    for section in contract.required_briefing_sections:
        section_pattern = re.escape(section).replace("\\-", "[- ]")
        if not re.search(rf"(?m)^#+\s+{section_pattern}\s*$", normalized_briefing):
            failures.append(f"briefing.section.{section}")
    if contract.require_freshness_summary and "freshness summary" not in (
        normalized_briefing + "\n" + normalized_dashboard
    ):
        failures.append("freshness-summary.missing")
    if contract.require_quality_summary and "quality summary" not in (
        normalized_briefing + "\n" + normalized_dashboard
    ):
        failures.append("quality-summary.missing")

    if failures:
        return EditionVerificationVerdict(
            verified=False,
            status=EditionStatus.DEPLOYMENT_FAILED,
            semantic_failures=tuple(failures),
        )
    return EditionVerificationVerdict(verified=True, status=EditionStatus.VERIFIED)


def verify_durable_candidate_edition(
    repository: TrustRepository,
    edition_id: str,
    contract: CriticalEditionContract,
) -> EditionVerificationVerdict:
    """Verify a candidate edition only after its durable manifests are present."""

    edition = repository.read(Edition, edition_id)
    if edition is None:
        return EditionVerificationVerdict(
            verified=False,
            status=EditionStatus.DEPLOYMENT_FAILED,
            missing_edition_manifest=True,
        )
    run = repository.read(Run, edition.run_id)
    if run is None:
        return EditionVerificationVerdict(
            verified=False,
            status=EditionStatus.DEPLOYMENT_FAILED,
            missing_run_manifest=True,
        )
    manifest_verdict = verify_candidate_edition(edition, contract)
    durable_verdict = _verify_durable_critical_state(repository, run, edition, contract)
    return _combine_verdicts((manifest_verdict, durable_verdict))


def _verify_durable_critical_state(
    repository: TrustRepository,
    run: Run,
    edition: Edition,
    contract: CriticalEditionContract,
) -> EditionVerificationVerdict:
    missing_revision_ids: list[str] = []
    critical_failures: list[str] = []
    for revision_id in contract.required_revision_ids:
        revision = repository.read(ObservationRevision, revision_id)
        if revision is None:
            missing_revision_ids.append(revision_id)
            continue
        critical_failures.extend(_critical_revision_failures(revision, "revision"))

    missing_derived_revision_ids: list[str] = []
    for revision_id in contract.required_derived_revision_ids:
        revision = repository.read(ObservationRevision, revision_id)
        if revision is None:
            missing_derived_revision_ids.append(revision_id)
            continue
        if revision.calculation_id is None:
            critical_failures.append(f"derived-revision.not-derived.{revision_id}")
        critical_failures.extend(_critical_revision_failures(revision, "derived-revision"))
        for input_revision_id in revision.input_revision_ids:
            input_revision = repository.read(ObservationRevision, input_revision_id)
            if input_revision is None:
                missing_revision_ids.append(input_revision_id)
                continue
            critical_failures.extend(_critical_revision_failures(input_revision, "derived-input-revision"))

    dataset_results, dataset_result_failures = _dataset_results_by_dataset(repository, run)
    critical_failures.extend(dataset_result_failures)
    missing_dataset_ids: list[str] = []
    for dataset_id in contract.required_dataset_ids:
        result = dataset_results.get(dataset_id)
        if result is None:
            missing_dataset_ids.append(dataset_id)
            continue
        if result.status is not DatasetResultStatus.SUCCESS:
            critical_failures.append(f"dataset.{result.status.value}.{dataset_id}")
        if not result.eligible:
            critical_failures.append(f"dataset.not-eligible.{dataset_id}")
        if result.freshness is not FreshnessState.CURRENT:
            critical_failures.append(f"dataset.{result.freshness.value}.{dataset_id}")
        if not result.accepted_revision_ids:
            critical_failures.append(f"dataset.no-accepted-revisions.{dataset_id}")
        missing_pins = tuple(
            revision_id
            for revision_id in result.accepted_revision_ids
            if revision_id not in edition.revision_ids and revision_id not in edition.derived_revision_ids
        )
        if missing_pins:
            critical_failures.append(f"dataset.accepted-revisions-not-pinned.{dataset_id}")
        for revision_id in result.accepted_revision_ids:
            revision = repository.read(ObservationRevision, revision_id)
            if revision is None:
                missing_revision_ids.append(revision_id)
                continue
            if revision.identity.dataset_id != dataset_id:
                critical_failures.append(f"dataset.revision-mismatch.{dataset_id}")
            critical_failures.extend(_critical_revision_failures(revision, "dataset-revision"))

    if missing_revision_ids or missing_derived_revision_ids or missing_dataset_ids or critical_failures:
        return EditionVerificationVerdict(
            verified=False,
            status=EditionStatus.DEPLOYMENT_FAILED,
            missing_revision_ids=tuple(missing_revision_ids),
            missing_derived_revision_ids=tuple(missing_derived_revision_ids),
            missing_dataset_ids=tuple(missing_dataset_ids),
            critical_failures=tuple(critical_failures),
        )
    return EditionVerificationVerdict(verified=True, status=EditionStatus.VERIFIED)


def _critical_revision_failures(revision: ObservationRevision, prefix: str) -> tuple[str, ...]:
    failures: list[str] = []
    if revision.quality_state is not QualityState.ACCEPTED:
        failures.append(f"{prefix}.{revision.quality_state.value}.{revision.revision_id}")
    if not revision.public_eligible:
        failures.append(f"{prefix}.not-public-eligible.{revision.revision_id}")
    return tuple(failures)


def _dataset_results_by_dataset(
    repository: TrustRepository,
    run: Run,
) -> tuple[dict[str, DatasetResult], tuple[str, ...]]:
    results: dict[str, DatasetResult] = {}
    failures: list[str] = []
    for dataset_result_id in run.dataset_result_ids:
        result = repository.read(DatasetResult, dataset_result_id)
        if result is None:
            failures.append(f"dataset-result.missing.{dataset_result_id}")
            continue
        if result.run_id != run.run_id:
            failures.append(f"dataset-result.run-mismatch.{result.dataset_id}")
            continue
        results[result.dataset_id] = result
    return results, tuple(failures)


def verify_candidate_generated_artifacts(
    edition: Edition,
    artifact_paths_by_key: Mapping[str, str | Path],
    *,
    required_artifact_keys: tuple[str, ...] = (),
) -> EditionVerificationVerdict:
    """Verify generated artifact files against the hashes pinned in the edition manifest."""

    missing_keys: list[str] = []
    mismatched_keys: list[str] = []
    normalized_paths = {
        _keys((key,), "artifact_paths_by_key key")[0]: Path(path)
        for key, path in artifact_paths_by_key.items()
    }
    for key in _keys(required_artifact_keys, "required_artifact_keys"):
        if key not in edition.generated_artifact_hashes or key not in normalized_paths:
            missing_keys.append(key)
    for key, expected_hash in edition.generated_artifact_hashes.items():
        if key in missing_keys:
            continue
        path = normalized_paths.get(key)
        if path is None or not path.is_file():
            missing_keys.append(key)
            continue
        actual_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual_hash != expected_hash:
            mismatched_keys.append(key)

    if missing_keys or mismatched_keys:
        return EditionVerificationVerdict(
            verified=False,
            status=EditionStatus.DEPLOYMENT_FAILED,
            missing_generated_artifact_keys=tuple(missing_keys),
            mismatched_generated_artifact_keys=tuple(mismatched_keys),
        )
    return EditionVerificationVerdict(verified=True, status=EditionStatus.VERIFIED)


def render_candidate_edition(
    repository: TrustRepository,
    edition_id: str,
    candidate_root: str | Path,
    renderer: CandidateRenderer,
) -> CandidateEditionRender:
    """Render a candidate edition from an edition-pinned cache without replacing public output."""

    edition = repository.read(Edition, edition_id)
    if edition is None:
        raise ValueError(f"edition {edition_id} is not durable")
    if edition.status is not EditionStatus.CANDIDATE:
        raise ValueError("only candidate editions can be rendered")
    output_dir = Path(candidate_root) / edition.edition_id
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_build = build_query_cache(
        repository,
        output_dir / "trusted-query-cache.sqlite",
        mode="edition",
        edition_id=edition.edition_id,
    )
    generated_paths = _generated_artifact_paths(renderer(cache_build.cache_path, output_dir, edition))
    return CandidateEditionRender(
        edition_id=edition.edition_id,
        output_dir=output_dir,
        cache_build=cache_build,
        generated_artifact_paths=generated_paths,
    )


def build_and_verify_candidate_edition(
    repository: TrustRepository,
    edition_id: str,
    candidate_root: str | Path,
    renderer: CandidateRenderer,
    critical_contract: CriticalEditionContract,
    semantic_contract: CandidateSemanticContract,
) -> CandidateEditionVerification:
    """Render and verify a candidate, preserving current edition on failure."""

    edition = repository.read(Edition, edition_id)
    if edition is None:
        raise ValueError(f"edition {edition_id} is not durable")
    critical_verdict = verify_durable_candidate_edition(repository, edition_id, critical_contract)
    if not critical_verdict.verified:
        failed_edition_id = _store_rejected_edition(repository, edition)
        return CandidateEditionVerification(
            edition_id=edition.edition_id,
            render=None,
            verdict=critical_verdict,
            failed_edition_id=failed_edition_id,
        )

    render = render_candidate_edition(repository, edition_id, candidate_root, renderer)
    generated_paths_by_key = dict(render.generated_artifact_paths)
    artifact_verdict = verify_candidate_generated_artifacts(
        edition,
        generated_paths_by_key,
        required_artifact_keys=(
            "briefing",
            "dashboard",
            *critical_contract.required_generated_artifact_keys,
        ),
    )
    missing_artifact_semantics = tuple(
        f"{key}.missing"
        for key in ("briefing", "dashboard")
        if key not in generated_paths_by_key or not generated_paths_by_key[key].is_file()
    )
    if missing_artifact_semantics:
        semantic_verdict = EditionVerificationVerdict(
            verified=False,
            status=EditionStatus.DEPLOYMENT_FAILED,
            semantic_failures=missing_artifact_semantics,
        )
    else:
        semantic_verdict = verify_candidate_semantics(
            edition,
            generated_paths_by_key["briefing"],
            generated_paths_by_key["dashboard"],
            semantic_contract,
        )
    verdict = _combine_verdicts((semantic_verdict, artifact_verdict))
    if verdict.verified:
        verified_edition_id = _store_verified_edition(repository, edition)
        failed_edition_id = None
    else:
        verified_edition_id = None
        failed_edition_id = _store_rejected_edition(repository, edition)
    return CandidateEditionVerification(
        edition_id=edition.edition_id,
        render=render,
        verdict=verdict,
        verified_edition_id=verified_edition_id,
        failed_edition_id=failed_edition_id,
    )


def _generated_artifact_paths(paths: GeneratedArtifactPaths) -> dict[str, Path]:
    if isinstance(paths, Mapping):
        return {_keys((key,), "generated artifact key")[0]: Path(path) for key, path in paths.items()}
    return {_keys((path.stem,), "generated artifact path stem")[0]: path for path in paths}


def _store_verified_edition(repository: TrustRepository, edition: Edition) -> str:
    verified = _copy_edition_with_status(edition, EditionStatus.VERIFIED)
    repository.store(verified)
    return verified.edition_id


def _store_rejected_edition(repository: TrustRepository, edition: Edition) -> str:
    failed = _copy_edition_with_status(edition, EditionStatus.DEPLOYMENT_FAILED)
    repository.store(failed)
    return failed.edition_id


def _copy_edition_with_status(edition: Edition, status: EditionStatus) -> Edition:
    return Edition(
        run_id=edition.run_id,
        created_at=edition.created_at + timedelta(microseconds=1),
        status=status,
        revision_ids=edition.revision_ids,
        derived_revision_ids=edition.derived_revision_ids,
        generated_artifact_hashes=edition.generated_artifact_hashes,
    )


def _combine_verdicts(verdicts: tuple[EditionVerificationVerdict, ...]) -> EditionVerificationVerdict:
    failed = tuple(verdict for verdict in verdicts if not verdict.verified)
    if not failed:
        return EditionVerificationVerdict(verified=True, status=EditionStatus.VERIFIED)
    return EditionVerificationVerdict(
        verified=False,
        status=EditionStatus.DEPLOYMENT_FAILED,
        missing_revision_ids=_merge_tuple(verdict.missing_revision_ids for verdict in failed),
        missing_derived_revision_ids=_merge_tuple(verdict.missing_derived_revision_ids for verdict in failed),
        missing_generated_artifact_keys=_merge_tuple(verdict.missing_generated_artifact_keys for verdict in failed),
        mismatched_generated_artifact_keys=_merge_tuple(
            verdict.mismatched_generated_artifact_keys for verdict in failed
        ),
        semantic_failures=_merge_tuple(verdict.semantic_failures for verdict in failed),
        missing_dataset_ids=_merge_tuple(verdict.missing_dataset_ids for verdict in failed),
        critical_failures=_merge_tuple(verdict.critical_failures for verdict in failed),
        missing_edition_manifest=any(verdict.missing_edition_manifest for verdict in failed),
        missing_run_manifest=any(verdict.missing_run_manifest for verdict in failed),
    )


def _merge_tuple(values: Iterable[tuple[str, ...]]) -> tuple[str, ...]:
    return tuple(sorted({value for group in values for value in group}))
