"""Git-native durable storage behind one trust-repository interface.

Callers provide canonical trust-domain records.  The adapters own collection
layout, deterministic JSON serialization, schema validation, locking, and
atomic file replacement; no caller constructs a durable storage path.

Raw artifact metadata and permitted content share the same seam.  Rights and
retention policy are checked before content-addressed payloads are written.
"""

from __future__ import annotations

import base64
import fcntl
import json
import os
import re
import tempfile
import threading
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, Protocol, TypeVar, runtime_checkable

from trust.domain import (
    ArtifactReference,
    Correction,
    DatasetResult,
    Edition,
    EditionStatus,
    EligibilityScope,
    Finding,
    ObservationIdentity,
    ObservationRevision,
    Promotion,
    RawArtifact,
    Run,
    RunStatus,
    revision_is_eligible,
)
from trust.registry import DatasetContract, RawRetention, RightsAction

_REPOSITORY_DIRECTORIES = (
    "registry",
    "artifacts",
    "observations",
    "findings",
    "runs",
    "editions",
    "corrections",
)
_CANONICAL_ID_RE = re.compile(r"^[a-z][a-z0-9_]*_[0-9a-f]{64}$")


class TrustRepositoryError(Exception):
    """Base error for durable trust repository operations."""


class RepositoryFormatError(TrustRepositoryError):
    """A record is missing or contradicts its durable envelope."""


class ImmutableRecordConflict(TrustRepositoryError):
    """An immutable identifier already contains different canonical data."""


class CurrentEditionConflict(TrustRepositoryError):
    """A pointer update was based on a different current edition."""


class EditionPromotionError(TrustRepositoryError):
    """An edition or its originating run is not eligible for promotion."""


class ArtifactRetentionError(TrustRepositoryError):
    """Raw artifact content contradicts its dataset retention contract."""


class SupersessionCycleError(TrustRepositoryError):
    """An observation supersession link would create a cycle."""


class VersionedRecord(Protocol):
    """Structural interface implemented by canonical durable domain records."""

    def to_dict(self) -> dict[str, Any]: ...


RecordT = TypeVar("RecordT", covariant=True)


class RecordDecoder(Protocol[RecordT]):
    """A canonical record class capable of restoring its serialized form."""

    def from_dict(self, data: Mapping[str, Any]) -> RecordT: ...


@runtime_checkable
class TrustRepository(Protocol):
    """The sole seam through which callers access durable trust records."""

    def initialize(self) -> None: ...

    def store(self, record: VersionedRecord) -> None: ...

    def read(self, decoder: RecordDecoder[RecordT], record_id: str) -> RecordT | None: ...

    def store_artifact(self, artifact: RawArtifact, contract: DatasetContract) -> ArtifactReference: ...

    def read_artifact(self, artifact_id: str) -> RawArtifact | None: ...

    def append_observation_revision(self, revision: ObservationRevision) -> None: ...

    def append_observation_revisions(self, revisions: Sequence[ObservationRevision]) -> None: ...

    def all_observation_revisions(self) -> tuple[ObservationRevision, ...]: ...

    def observation_revisions(self, identity: ObservationIdentity) -> tuple[ObservationRevision, ...]: ...

    def current_accepted_revision(
        self,
        identity: ObservationIdentity,
        *,
        scope: EligibilityScope = EligibilityScope.PUBLIC,
    ) -> ObservationRevision | None: ...

    def revision_effective_at(
        self,
        identity: ObservationIdentity,
        requested_at: datetime,
        *,
        scope: EligibilityScope = EligibilityScope.PUBLIC,
    ) -> ObservationRevision | None: ...

    def replace_current_edition(self, promotion: Promotion) -> None: ...

    def current_edition(self) -> Promotion | None: ...


@dataclass(frozen=True)
class _RecordLayout:
    directory: tuple[str, ...]
    id_field: str
    id_prefix: str


@dataclass(frozen=True)
class _SupersessionNode:
    observation_id: object
    ingested_at: datetime | None
    supersedes_revision_id: str | None


_RECORD_LAYOUTS = {
    "observation-revision": _RecordLayout(("observations",), "revision_id", "rev"),
    "finding": _RecordLayout(("findings",), "finding_id", "fnd"),
    "dataset-result": _RecordLayout(("runs", "dataset-results"), "dataset_result_id", "dsr"),
    "run": _RecordLayout(("runs",), "run_id", "run"),
    "edition": _RecordLayout(("editions",), "edition_id", "edn"),
    "correction": _RecordLayout(("corrections",), "correction_id", "cor"),
}

_RECORD_DECODERS: dict[str, RecordDecoder[Any]] = {
    "observation-revision": ObservationRevision,
    "finding": Finding,
    "dataset-result": DatasetResult,
    "run": Run,
    "edition": Edition,
    "correction": Correction,
}
_DECODER_RECORD_TYPES = {decoder: record_type for record_type, decoder in _RECORD_DECODERS.items()}


class _DirectoryTrustRepository:
    """Shared filesystem implementation used by both directory adapters."""

    def __init__(self, durable_root: str | os.PathLike[str]) -> None:
        self._durable_root = Path(durable_root)
        self._thread_lock = threading.RLock()

    def initialize(self) -> None:
        self._durable_root.mkdir(parents=True, exist_ok=True)
        for directory in _REPOSITORY_DIRECTORIES:
            (self._durable_root / directory).mkdir(exist_ok=True)

    def store(self, record: VersionedRecord) -> None:
        if isinstance(record, ObservationRevision):
            self.append_observation_revision(record)
            return
        payload = dict(record.to_dict())
        record_type = payload.get("record_type")
        if not isinstance(record_type, str) or record_type not in _RECORD_LAYOUTS:
            raise RepositoryFormatError(f"unsupported immutable record type: {record_type!r}")
        self._validate_schema_version(payload)
        try:
            canonical_record = _RECORD_DECODERS[record_type].from_dict(payload)
        except (KeyError, TypeError, ValueError) as exc:
            raise RepositoryFormatError(f"invalid canonical {record_type} record") from exc
        if self._serialize(canonical_record.to_dict()) != self._serialize(payload):
            raise RepositoryFormatError(f"non-canonical {record_type} serialization")
        if isinstance(canonical_record, ObservationRevision):
            self.append_observation_revision(canonical_record)
            return
        layout = _RECORD_LAYOUTS[record_type]
        record_id = self._record_id(payload, layout)
        contents = self._serialize(payload)
        if isinstance(canonical_record, Finding):
            with self._exclusive_lock():
                existing = self._read_finding(record_id)
                if existing is not None:
                    if self._serialize(existing.to_dict()) != contents:
                        raise ImmutableRecordConflict(
                            f"immutable record {record_id} already exists with different data"
                        )
                    return
                self._store_immutable(
                    self._finding_path(canonical_record),
                    contents,
                    f"immutable record {record_id}",
                )
            return
        destination = self._record_path(layout, record_id)

        with self._exclusive_lock():
            self._store_immutable(destination, contents, f"immutable record {record_id}")

    def read(self, decoder: RecordDecoder[RecordT], record_id: str) -> RecordT | None:
        record_type = _DECODER_RECORD_TYPES.get(decoder)
        if record_type is None:
            raise RepositoryFormatError(f"unsupported record decoder: {decoder!r}")
        layout = _RECORD_LAYOUTS[record_type]
        self._validate_identifier(record_id, layout.id_prefix)
        if record_type == "observation-revision":
            revision = self._read_observation_revision(record_id)
            if revision is None:
                return None
            return self._observation_ledger(revision.identity).get(record_id)  # type: ignore[return-value]
        if record_type == "finding":
            return self._read_finding(record_id)  # type: ignore[return-value]
        path = self._record_path(layout, record_id)
        if not path.exists():
            return None
        payload = self._read_identified_payload(path, record_type, layout.id_field)
        try:
            return decoder.from_dict(payload)
        except (KeyError, TypeError, ValueError) as exc:
            raise RepositoryFormatError(f"invalid durable {record_type} record") from exc

    def store_artifact(self, artifact: RawArtifact, contract: DatasetContract) -> ArtifactReference:
        reference = artifact.reference
        self._validate_artifact_policy(reference, contract)
        metadata = artifact.to_dict()
        metadata["content_base64"] = None
        metadata_contents = self._serialize(metadata)
        metadata_path = self._artifact_metadata_path(reference.artifact_id)

        with self._exclusive_lock():
            if artifact.content is not None:
                payload_path = self._artifact_payload_path(reference.content_hash)
                self._store_immutable(payload_path, artifact.content, f"content hash {reference.content_hash}")
            self._store_immutable(
                metadata_path,
                metadata_contents,
                f"immutable artifact metadata {reference.artifact_id}",
            )
        return reference

    def read_artifact(self, artifact_id: str) -> RawArtifact | None:
        self._validate_identifier(artifact_id, "art")
        metadata_path = self._artifact_metadata_path(artifact_id)
        if not metadata_path.exists():
            return None
        metadata = self._read_payload(metadata_path)
        if metadata.get("record_type") != "raw-artifact" or metadata.get("artifact_id") != artifact_id:
            raise RepositoryFormatError(f"artifact metadata {artifact_id} contradicts its durable identifier")
        try:
            reference = ArtifactReference.from_dict(metadata["reference"])
            if reference.content_retained:
                content = self._artifact_payload_path(reference.content_hash).read_bytes()
                metadata["content_base64"] = base64.b64encode(content).decode("ascii")
            return RawArtifact.from_dict(metadata)
        except (KeyError, OSError, TypeError, ValueError) as exc:
            raise RepositoryFormatError(f"invalid durable raw artifact {artifact_id}") from exc

    def append_observation_revision(self, revision: ObservationRevision) -> None:
        with self._exclusive_lock():
            prepared = self._prepare_new_revision_or_skip_existing(revision)
            if prepared is None:
                return
            canonical_revision, contents = prepared
            ledger = self._observation_ledger(canonical_revision.identity)
            self._validate_appended_supersession(
                canonical_revision,
                self._ledger_with_external_supersession_target(canonical_revision, ledger),
            )
            destination = self._observation_revision_path(canonical_revision)
            self._store_immutable(destination, contents, f"immutable record {canonical_revision.revision_id}")

    def append_observation_revisions(self, revisions: Sequence[ObservationRevision]) -> None:
        with self._exclusive_lock():
            prepared = tuple(
                prepared
                for revision in revisions
                if (prepared := self._prepare_new_revision_or_skip_existing(revision)) is not None
            )
            if all(revision.supersedes_revision_id is None for revision, _contents in prepared):
                self._append_independent_observation_revisions(prepared)
                return
            pending_by_identity: dict[str, dict[str, ObservationRevision]] = {}
            for revision, contents in prepared:
                ledger = pending_by_identity.get(revision.identity.observation_id)
                if ledger is None:
                    ledger = self._observation_ledger(revision.identity)
                    pending_by_identity[revision.identity.observation_id] = ledger
                if revision.revision_id in ledger:
                    if self._serialize(ledger[revision.revision_id].to_dict()) != contents:
                        raise ImmutableRecordConflict(
                            f"immutable record {revision.revision_id} already exists with different data"
                        )
                    continue
                self._validate_appended_supersession(
                    revision,
                    self._ledger_with_external_supersession_target(revision, ledger),
                )
                destination = self._observation_revision_path(revision)
                self._store_immutable(destination, contents, f"immutable record {revision.revision_id}")
                ledger[revision.revision_id] = revision

    def all_observation_revisions(self) -> tuple[ObservationRevision, ...]:
        revisions = self._all_observation_ledger()
        return tuple(sorted(revisions.values(), key=lambda revision: (revision.ingested_at, revision.revision_id)))

    def _append_independent_observation_revisions(
        self,
        prepared: Sequence[tuple[ObservationRevision, bytes]],
    ) -> None:
        pending_contents: dict[str, bytes] = {}
        for revision, contents in prepared:
            existing_contents = pending_contents.get(revision.revision_id)
            if existing_contents is not None:
                if existing_contents != contents:
                    raise ImmutableRecordConflict(
                        f"immutable record {revision.revision_id} already exists with different data"
                    )
                continue
            pending_contents[revision.revision_id] = contents
            destination = self._observation_revision_path(revision)
            self._store_immutable(destination, contents, f"immutable record {revision.revision_id}")

    def observation_revisions(self, identity: ObservationIdentity) -> tuple[ObservationRevision, ...]:
        revisions = [
            revision
            for revision in self._observation_ledger(identity).values()
            if revision.identity.observation_id == identity.observation_id
        ]
        return tuple(sorted(revisions, key=lambda revision: (revision.ingested_at, revision.revision_id)))

    def current_accepted_revision(
        self,
        identity: ObservationIdentity,
        *,
        scope: EligibilityScope = EligibilityScope.PUBLIC,
    ) -> ObservationRevision | None:
        return self._accepted_head(self.observation_revisions(identity), scope)

    def revision_effective_at(
        self,
        identity: ObservationIdentity,
        requested_at: datetime,
        *,
        scope: EligibilityScope = EligibilityScope.PUBLIC,
    ) -> ObservationRevision | None:
        if not isinstance(requested_at, datetime) or requested_at.tzinfo is None or requested_at.utcoffset() is None:
            raise ValueError("requested_at must be a timezone-aware datetime")
        instant = requested_at.astimezone(timezone.utc)
        revisions = tuple(
            revision for revision in self.observation_revisions(identity) if revision.ingested_at <= instant
        )
        return self._accepted_head(revisions, scope)

    @staticmethod
    def _accepted_head(
        revisions: tuple[ObservationRevision, ...],
        scope: EligibilityScope = EligibilityScope.PUBLIC,
    ) -> ObservationRevision | None:
        superseded_ids = {
            revision.supersedes_revision_id
            for revision in revisions
            if revision_is_eligible(revision, scope) and revision.supersedes_revision_id is not None
        }
        eligible = [
            revision
            for revision in revisions
            if revision_is_eligible(revision, scope) and revision.revision_id not in superseded_ids
        ]
        return eligible[-1] if eligible else None

    def replace_current_edition(self, promotion: Promotion) -> None:
        payload = promotion.to_dict()
        self._validate_schema_version(payload)
        contents = self._serialize(payload)
        destination = self._durable_root / "current-edition.json"

        with self._exclusive_lock():
            current = self._read_promotion(destination)
            if current is not None and destination.read_bytes() == contents:
                return
            current_edition_id = current.edition_id if current else None
            if promotion.previous_edition_id != current_edition_id:
                raise CurrentEditionConflict(
                    "current edition changed before pointer replacement: "
                    f"expected {promotion.previous_edition_id!r}, found {current_edition_id!r}"
                )
            edition = self.read(Edition, promotion.edition_id)
            if edition is None:
                raise EditionPromotionError(f"edition {promotion.edition_id} is not durable")
            if edition.status not in {EditionStatus.VERIFIED, EditionStatus.PROMOTED}:
                raise EditionPromotionError(
                    f"edition {promotion.edition_id} has ineligible {edition.status.value} status"
                )
            run = self.read(Run, edition.run_id)
            if run is None:
                raise EditionPromotionError(f"edition {promotion.edition_id} has no durable run")
            if run.status is not RunStatus.SUCCEEDED:
                raise EditionPromotionError(
                    f"edition {promotion.edition_id} belongs to {run.status.value} run {run.run_id}"
                )
            self._atomic_replace(destination, contents)

    def current_edition(self) -> Promotion | None:
        return self._read_promotion(self._durable_root / "current-edition.json")

    @contextmanager
    def _exclusive_lock(self) -> Iterator[None]:
        self.initialize()
        with self._thread_lock:
            descriptor = os.open(self._durable_root, os.O_RDONLY)
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)

    def _record_path(self, layout: _RecordLayout, record_id: str) -> Path:
        directory = self._durable_root.joinpath(*layout.directory)
        directory.mkdir(parents=True, exist_ok=True)
        return directory / f"{record_id}.json"

    def _finding_path(self, finding: Finding) -> Path:
        directory = self._durable_root / "findings" / finding.run_id
        directory.mkdir(parents=True, exist_ok=True)
        return directory / f"{finding.finding_id}.json"

    def _read_finding(self, finding_id: str) -> Finding | None:
        path = self._fixed_id_path("findings", finding_id, partition_depth=1, record_label="finding")
        if path is None:
            return None
        payload = self._read_identified_payload(path, "finding", "finding_id")
        try:
            finding = Finding.from_dict(payload)
        except (KeyError, TypeError, ValueError) as exc:
            raise RepositoryFormatError(f"invalid durable finding record {path.name}") from exc
        findings_root = self._durable_root / "findings"
        if path.parent != findings_root and (
            path.parent.parent != findings_root or path.parent.name != finding.run_id
        ):
            raise RepositoryFormatError(f"finding {finding.finding_id} contradicts its durable partition")
        return finding

    def _observation_partition(self, identity: ObservationIdentity) -> Path:
        return self._durable_root / "observations" / identity.dataset_id / str(identity.effective_date.year)

    def _observation_revision_path(self, revision: ObservationRevision) -> Path:
        directory = self._observation_partition(revision.identity)
        directory.mkdir(parents=True, exist_ok=True)
        return directory / f"{revision.revision_id}.json"

    def _read_observation_revision(self, revision_id: str) -> ObservationRevision | None:
        path = self._fixed_id_path(
            "observations",
            revision_id,
            partition_depth=2,
            record_label="observation revision",
        )
        if path is None:
            return None
        payload = self._read_identified_payload(path, "observation-revision", "revision_id")
        revision = self._decode_observation_revision(path, payload)
        self._validate_observation_partition(path, revision)
        return revision

    def _observation_ledger(self, identity: ObservationIdentity) -> dict[str, ObservationRevision]:
        serialized: dict[str, Mapping[str, Any]] = {}
        paths: dict[str, Path] = {}
        observations_root = self._durable_root / "observations"
        revision_paths = (
            *observations_root.glob("rev_*.json"),
            *self._observation_partition(identity).glob("rev_*.json"),
        )
        for path in sorted(revision_paths):
            payload = self._read_payload(path)
            if payload.get("record_type") != "observation-revision":
                raise RepositoryFormatError(f"record {path.name} is not an observation-revision")
            revision_id = payload.get("revision_id")
            if revision_id != path.stem:
                raise RepositoryFormatError(f"record {path.name} contradicts its durable identifier")
            if revision_id in serialized:
                raise RepositoryFormatError(f"duplicate durable observation revision {revision_id}")
            serialized[path.stem] = payload
            paths[path.stem] = path

        self._validate_serialized_supersession_graph(serialized)
        ledger: dict[str, ObservationRevision] = {}
        for revision_id, serialized_payload in serialized.items():
            path = paths[revision_id]
            revision = self._decode_observation_revision(path, serialized_payload)
            self._validate_observation_partition(path, revision)
            ledger[revision.revision_id] = revision
        return ledger

    def _all_observation_ledger(self) -> dict[str, ObservationRevision]:
        serialized: dict[str, Mapping[str, Any]] = {}
        paths: dict[str, Path] = {}
        observations_root = self._durable_root / "observations"
        revision_paths = (
            *observations_root.glob("rev_*.json"),
            *observations_root.glob("*/*/rev_*.json"),
        )
        for path in sorted(revision_paths):
            payload = self._read_payload(path)
            if payload.get("record_type") != "observation-revision":
                raise RepositoryFormatError(f"record {path.name} is not an observation-revision")
            revision_id = payload.get("revision_id")
            if revision_id != path.stem:
                raise RepositoryFormatError(f"record {path.name} contradicts its durable identifier")
            if revision_id in serialized:
                raise RepositoryFormatError(f"duplicate durable observation revision {revision_id}")
            serialized[path.stem] = payload
            paths[path.stem] = path

        self._validate_serialized_supersession_graph(serialized)
        ledger: dict[str, ObservationRevision] = {}
        for revision_id, serialized_payload in serialized.items():
            path = paths[revision_id]
            revision = self._decode_observation_revision(path, serialized_payload)
            self._validate_observation_partition(path, revision)
            ledger[revision.revision_id] = revision
        return ledger

    def _decode_observation_revision(
        self,
        path: Path,
        payload: Mapping[str, Any],
    ) -> ObservationRevision:
        try:
            revision = ObservationRevision.from_dict(payload)
        except (KeyError, TypeError, ValueError) as exc:
            raise RepositoryFormatError(f"invalid durable observation-revision record {path.name}") from exc
        if self._serialize(revision.to_dict()) != self._serialize(payload):
            raise RepositoryFormatError(f"non-canonical durable observation-revision record {path.name}")
        return revision

    def _prepared_revision(self, revision: ObservationRevision) -> tuple[ObservationRevision, bytes]:
        payload = revision.to_dict()
        layout = _RECORD_LAYOUTS["observation-revision"]
        record_id = self._record_id(payload, layout)
        if revision.supersedes_revision_id == revision.revision_id:
            raise SupersessionCycleError("observation supersession cycle cannot include the revision itself")
        self._validate_schema_version(payload)
        contents = self._serialize(payload)
        try:
            canonical_revision = ObservationRevision.from_dict(payload)
        except (KeyError, TypeError, ValueError) as exc:
            raise RepositoryFormatError("invalid canonical observation-revision record") from exc
        if canonical_revision.revision_id != record_id:
            raise RepositoryFormatError("observation-revision record contradicts its durable identifier")
        if self._serialize(canonical_revision.to_dict()) != contents:
            raise RepositoryFormatError("non-canonical observation-revision serialization")
        return canonical_revision, contents

    def _prepare_new_revision_or_skip_existing(
        self,
        revision: ObservationRevision,
    ) -> tuple[ObservationRevision, bytes] | None:
        payload = revision.to_dict()
        layout = _RECORD_LAYOUTS["observation-revision"]
        record_id = self._record_id(payload, layout)
        contents = self._serialize(payload)
        if self._revision_exists_idempotently(record_id, contents):
            return None
        return self._prepared_revision(revision)

    def _revision_exists_idempotently(self, revision_id: str, contents: bytes) -> bool:
        existing = self._read_observation_revision(revision_id)
        if existing is None:
            return False
        if self._serialize(existing.to_dict()) != contents:
            raise ImmutableRecordConflict(f"immutable record {revision_id} already exists with different data")
        return True

    def _ledger_with_external_supersession_target(
        self,
        revision: ObservationRevision,
        ledger: Mapping[str, ObservationRevision],
    ) -> Mapping[str, ObservationRevision]:
        superseded_id = revision.supersedes_revision_id
        if superseded_id is None or superseded_id in ledger:
            return ledger
        superseded = self._read_observation_revision(superseded_id)
        if superseded is None:
            return ledger
        return {**ledger, superseded_id: superseded}

    def _validate_observation_partition(self, path: Path, revision: ObservationRevision) -> None:
        observations_root = self._durable_root / "observations"
        if path.parent != observations_root and (
            path.parent.parent.name != revision.identity.dataset_id
            or path.parent.name != str(revision.identity.effective_date.year)
        ):
            raise RepositoryFormatError(
                f"observation revision {revision.revision_id} contradicts its durable partition"
            )

    @staticmethod
    def _validate_serialized_supersession_graph(records: Mapping[str, Mapping[str, Any]]) -> None:
        nodes: dict[str, _SupersessionNode] = {}
        for revision_id, payload in records.items():
            superseded_id = payload.get("supersedes_revision_id")
            if superseded_id is not None and not isinstance(superseded_id, str):
                raise RepositoryFormatError("superseded revision identifier is invalid")
            try:
                ingested_at = datetime.fromisoformat(str(payload.get("ingested_at")))
            except ValueError:
                ingested_at = None
            nodes[revision_id] = _SupersessionNode(
                observation_id=payload.get("observation_id"),
                ingested_at=ingested_at,
                supersedes_revision_id=superseded_id,
            )
        _DirectoryTrustRepository._validate_complete_supersession_graph(nodes)

    @staticmethod
    def _validate_complete_supersession_graph(nodes: Mapping[str, _SupersessionNode]) -> None:
        links: dict[str, str] = {}
        edges: list[tuple[_SupersessionNode, _SupersessionNode]] = []
        for revision_id, node in nodes.items():
            superseded_id = node.supersedes_revision_id
            if superseded_id is None:
                continue
            prior = _DirectoryTrustRepository._validated_supersession_prior(node, nodes.get(superseded_id))
            links[revision_id] = superseded_id
            edges.append((node, prior))
        _DirectoryTrustRepository._reject_supersession_cycles(links)
        for node, prior in edges:
            _DirectoryTrustRepository._validate_supersession_time(node, prior)

    @staticmethod
    def _validate_appended_supersession(
        revision: ObservationRevision,
        ledger: Mapping[str, ObservationRevision],
    ) -> None:
        superseded_id = revision.supersedes_revision_id
        if superseded_id is None:
            return
        prior = ledger.get(superseded_id)
        node = _DirectoryTrustRepository._supersession_node(revision)
        prior_node = _DirectoryTrustRepository._validated_supersession_prior(
            node,
            _DirectoryTrustRepository._supersession_node(prior) if prior is not None else None,
        )
        _DirectoryTrustRepository._validate_supersession_time(node, prior_node)

    @staticmethod
    def _supersession_node(revision: ObservationRevision) -> _SupersessionNode:
        return _SupersessionNode(
            observation_id=revision.identity.observation_id,
            ingested_at=revision.ingested_at,
            supersedes_revision_id=revision.supersedes_revision_id,
        )

    @staticmethod
    def _validated_supersession_prior(
        node: _SupersessionNode,
        prior: _SupersessionNode | None,
    ) -> _SupersessionNode:
        superseded_id = node.supersedes_revision_id
        if superseded_id is None:  # pragma: no cover - callers only validate linked nodes
            raise AssertionError("supersession validation requires a linked node")
        if prior is None:
            raise RepositoryFormatError(f"superseded revision {superseded_id} is not in the ledger")
        if prior.observation_id != node.observation_id:
            raise RepositoryFormatError("a revision can only supersede the same observation identity")
        return prior

    @staticmethod
    def _validate_supersession_time(node: _SupersessionNode, prior: _SupersessionNode) -> None:
        if (
            node.ingested_at is not None
            and node.ingested_at.tzinfo is not None
            and node.ingested_at.utcoffset() is not None
            and prior.ingested_at is not None
            and prior.ingested_at.tzinfo is not None
            and prior.ingested_at.utcoffset() is not None
            and node.ingested_at < prior.ingested_at
        ):
            raise RepositoryFormatError("a successor revision cannot predate the revision it supersedes")

    @staticmethod
    def _reject_supersession_cycles(links: Mapping[str, str]) -> None:
        complete: set[str] = set()
        for revision_id in links:
            if revision_id in complete:
                continue
            path: list[str] = []
            visiting: set[str] = set()
            cursor = revision_id
            while cursor in links and cursor not in complete:
                if cursor in visiting:
                    raise SupersessionCycleError("observation supersession cycle detected")
                visiting.add(cursor)
                path.append(cursor)
                cursor = links[cursor]
            complete.update(path)

    @staticmethod
    def _validate_artifact_policy(reference: ArtifactReference, contract: DatasetContract) -> None:
        dataset = contract.dataset
        if (
            reference.source_id != dataset.source_id
            or reference.dataset_id != dataset.dataset_id
            or reference.dataset_key != dataset.key
        ):
            raise ArtifactRetentionError("raw artifact does not belong to the supplied dataset contract")
        rights = contract.rights
        if reference.content_retained and (
            contract.raw_retention is not RawRetention.CONTENT
            or rights is None
            or not rights.allows(RightsAction.RAW_CONTENT_RETENTION)
        ):
            raise ArtifactRetentionError("dataset policy does not allow raw content retention")

    def _artifact_metadata_path(self, artifact_id: str) -> Path:
        directory = self._durable_root / "artifacts" / "metadata"
        directory.mkdir(parents=True, exist_ok=True)
        return directory / f"{artifact_id}.json"

    def _artifact_payload_path(self, content_hash: str) -> Path:
        directory = self._durable_root / "artifacts" / "payloads" / "sha256"
        directory.mkdir(parents=True, exist_ok=True)
        return directory / content_hash

    def _fixed_id_path(
        self,
        collection: str,
        record_id: str,
        *,
        partition_depth: int,
        record_label: str,
    ) -> Path | None:
        collection_root = self._durable_root / collection
        legacy_path = collection_root / f"{record_id}.json"
        paths = [legacy_path] if legacy_path.exists() else []
        partition_glob = "/".join((*(["*"] * partition_depth), f"{record_id}.json"))
        paths.extend(collection_root.glob(partition_glob))
        if not paths:
            return None
        if len(paths) > 1:
            raise RepositoryFormatError(f"duplicate durable {record_label} {record_id}")
        return paths[0]

    def _store_immutable(self, destination: Path, contents: bytes, label: str) -> None:
        if destination.exists():
            if destination.read_bytes() == contents:
                return
            raise ImmutableRecordConflict(f"{label} already exists with different data")
        self._atomic_replace(destination, contents)

    @staticmethod
    def _record_id(payload: Mapping[str, Any], layout: _RecordLayout) -> str:
        record_id = payload.get(layout.id_field)
        if not isinstance(record_id, str):
            raise RepositoryFormatError(f"record is missing {layout.id_field}")
        _DirectoryTrustRepository._validate_identifier(record_id, layout.id_prefix)
        return record_id

    @staticmethod
    def _validate_identifier(record_id: str, expected_prefix: str) -> None:
        if not isinstance(record_id, str) or not _CANONICAL_ID_RE.fullmatch(record_id):
            raise RepositoryFormatError("record identifier is not canonical")
        if not record_id.startswith(f"{expected_prefix}_"):
            raise RepositoryFormatError(f"record identifier must use the {expected_prefix} prefix")

    @staticmethod
    def _validate_schema_version(payload: Mapping[str, Any]) -> None:
        schema_version = payload.get("schema_version")
        if not isinstance(schema_version, int) or isinstance(schema_version, bool) or schema_version <= 0:
            raise RepositoryFormatError("durable records require a positive integer schema_version")

    @staticmethod
    def _serialize(payload: Mapping[str, Any]) -> bytes:
        try:
            return (
                json.dumps(
                    payload,
                    allow_nan=False,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                + "\n"
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise RepositoryFormatError("durable record is not deterministic JSON") from exc

    def _read_payload(self, path: Path) -> dict[str, Any]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise RepositoryFormatError(f"cannot read durable record {path.name}") from exc
        if not isinstance(payload, dict):
            raise RepositoryFormatError(f"durable record {path.name} must contain a JSON object")
        self._validate_schema_version(payload)
        return payload

    def _read_identified_payload(
        self,
        path: Path,
        record_type: str,
        id_field: str,
    ) -> dict[str, Any]:
        payload = self._read_payload(path)
        if payload.get("record_type") != record_type:
            raise RepositoryFormatError(f"record {path.name} is not a {record_type}")
        if payload.get(id_field) != path.stem:
            raise RepositoryFormatError(f"record {path.name} contradicts its durable identifier")
        return payload

    def _read_promotion(self, path: Path) -> Promotion | None:
        if not path.exists():
            return None
        payload = self._read_payload(path)
        if payload.get("record_type") != "promotion":
            raise RepositoryFormatError("current-edition pointer must contain a promotion record")
        try:
            return Promotion.from_dict(payload)
        except (KeyError, TypeError, ValueError) as exc:
            raise RepositoryFormatError("current-edition pointer is invalid") from exc

    @staticmethod
    def _atomic_replace(destination: Path, contents: bytes) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
        )
        temporary_path = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as temporary_file:
                _write_and_sync(temporary_file, contents)
            os.replace(temporary_path, destination)
            _sync_directory(destination.parent)
        finally:
            temporary_path.unlink(missing_ok=True)


class TemporaryDirectoryTrustRepository(_DirectoryTrustRepository):
    """Trust repository adapter rooted directly in a caller-owned temp directory."""


class GitDirectoryTrustRepository(_DirectoryTrustRepository):
    """Production adapter rooted at ``data/v2`` inside a Git working tree."""

    def __init__(self, repository_root: str | os.PathLike[str]) -> None:
        super().__init__(Path(repository_root) / "data" / "v2")


def _write_and_sync(temporary_file: BinaryIO, contents: bytes) -> None:
    temporary_file.write(contents)
    temporary_file.flush()
    os.fsync(temporary_file.fileno())


def _sync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = [
    "ArtifactRetentionError",
    "CurrentEditionConflict",
    "EditionPromotionError",
    "GitDirectoryTrustRepository",
    "ImmutableRecordConflict",
    "RepositoryFormatError",
    "SupersessionCycleError",
    "TemporaryDirectoryTrustRepository",
    "TrustRepository",
    "TrustRepositoryError",
]
