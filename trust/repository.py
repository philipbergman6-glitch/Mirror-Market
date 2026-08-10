"""Git-native durable storage behind one trust-repository interface.

Callers provide canonical trust-domain records.  The adapters own collection
layout, deterministic JSON serialization, schema validation, locking, and
atomic file replacement; no caller constructs a durable storage path.

Raw artifact content is deliberately not supported here.  DT-06 extends this
module with the rights-aware, content-addressed artifact behavior.
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import tempfile
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Protocol, TypeVar, runtime_checkable

from trust.domain import (
    Correction,
    DatasetResult,
    Edition,
    Finding,
    ObservationRevision,
    Promotion,
    Run,
)

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

    def replace_current_edition(self, promotion: Promotion) -> None: ...

    def current_edition(self) -> Promotion | None: ...


@dataclass(frozen=True)
class _RecordLayout:
    directory: tuple[str, ...]
    id_field: str
    id_prefix: str


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
        layout = _RECORD_LAYOUTS[record_type]
        record_id = self._record_id(payload, layout)
        contents = self._serialize(payload)
        destination = self._record_path(layout, record_id)

        with self._exclusive_lock():
            if destination.exists():
                if destination.read_bytes() == contents:
                    return
                raise ImmutableRecordConflict(f"immutable record {record_id} already exists with different data")
            self._atomic_replace(destination, contents)

    def read(self, decoder: RecordDecoder[RecordT], record_id: str) -> RecordT | None:
        record_type = _DECODER_RECORD_TYPES.get(decoder)
        if record_type is None:
            raise RepositoryFormatError(f"unsupported record decoder: {decoder!r}")
        layout = _RECORD_LAYOUTS[record_type]
        self._validate_identifier(record_id, layout.id_prefix)
        path = self._record_path(layout, record_id)
        if not path.exists():
            return None
        payload = self._read_payload(path)
        if payload.get("record_type") != record_type:
            raise RepositoryFormatError(f"record {record_id} is not a {record_type}")
        if payload.get(layout.id_field) != record_id:
            raise RepositoryFormatError(f"record {record_id} contradicts its durable identifier")
        try:
            return decoder.from_dict(payload)
        except (KeyError, TypeError, ValueError) as exc:
            raise RepositoryFormatError(f"invalid durable {record_type} record") from exc

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
    "CurrentEditionConflict",
    "GitDirectoryTrustRepository",
    "ImmutableRecordConflict",
    "RepositoryFormatError",
    "TemporaryDirectoryTrustRepository",
    "TrustRepository",
    "TrustRepositoryError",
]
