"""Canonical value objects for the Data Trust Foundation.

This module deliberately depends only on the Python standard library.  It is
the shared vocabulary used at trust-module boundaries; persistence, network,
SQLite, and dataframe concerns belong in adapters outside this module.

Identifiers are SHA-256 hashes of canonical JSON identity payloads.  Mapping
order, set-like tuple order, decimal scale, and timezone offsets therefore do
not make equivalent domain values acquire different identifiers.

Record identifiers cover the complete immutable assertion.  Artifact IDs are
the content-addressed replay key: trusted ingestion can recognize an artifact
before constructing another timestamped candidate or revision.  This keeps
immutable records conflict-free while enabling artifact-level idempotency.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from types import MappingProxyType
from typing import Any, ClassVar, TypeVar, cast

SCHEMA_VERSION = 1
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_EnumT = TypeVar("_EnumT", bound=Enum)


class FindingSeverity(str, Enum):
    WARNING = "warning"
    QUARANTINE = "quarantine"
    REJECT = "reject"


class QualityState(str, Enum):
    LEGACY = "legacy"
    ACCEPTED = "accepted"
    QUARANTINED = "quarantined"
    REJECTED = "rejected"
    SUPERSEDED = "superseded"


class FreshnessState(str, Enum):
    CURRENT = "current"
    STALE = "stale"
    UNAVAILABLE = "unavailable"


class RunStatus(str, Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class EditionStatus(str, Enum):
    CANDIDATE = "candidate"
    VERIFIED = "verified"
    PROMOTED = "promoted"
    DEPLOYMENT_FAILED = "deployment-failed"
    SUPERSEDED = "superseded"


class DatasetResultStatus(str, Enum):
    SUCCESS = "success"
    LEGITIMATE_EMPTY = "legitimate-empty"
    EXTERNAL_FAILURE = "external-failure"
    CONTRACT_FAILURE = "contract-failure"
    QUARANTINED = "quarantined"


class CorrectionDecision(str, Enum):
    APPROVE = "approve"
    REPLACE = "replace"
    REJECT = "reject"


def _enum(enum_type: type[_EnumT], value: _EnumT | str) -> _EnumT:
    return value if isinstance(value, enum_type) else enum_type(value)


def _text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _key(value: str, field_name: str) -> str:
    normalized = _text(value, field_name).lower()
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]*", normalized):
        raise ValueError(f"{field_name} must contain only letters, numbers, '.', '_' or '-'")
    return normalized


def _identifier(value: str, field_name: str, expected_prefix: str | None = None) -> str:
    value = _text(value, field_name)
    prefix = expected_prefix or r"[a-z][a-z0-9_]*"
    if not re.fullmatch(rf"{prefix}_[0-9a-f]{{64}}", value):
        expected = f"a {expected_prefix} identifier" if expected_prefix else "a canonical trust identifier"
        raise ValueError(f"{field_name} must be {expected}")
    return value


def _hash(value: str, field_name: str = "content_hash") -> str:
    value = _text(value, field_name).lower()
    if not _SHA256_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 hex digest")
    return value


def _date(value: date, field_name: str) -> date:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a date")
    return value


def _utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


def _decimal(value: Decimal | int | str, field_name: str) -> Decimal:
    if isinstance(value, (bool, float)):
        raise ValueError(f"{field_name} must use Decimal, int, or a decimal string")
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a decimal number") from exc
    if not result.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return result


def _decimal_text(value: Decimal) -> str:
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _sorted_unique(values: tuple[str, ...], field_name: str) -> tuple[str, ...]:
    normalized = tuple(_text(value, field_name) for value in values)
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} cannot contain duplicates")
    return tuple(sorted(normalized))


def _sorted_identifiers(values: tuple[str, ...], field_name: str, prefix: str) -> tuple[str, ...]:
    normalized = tuple(_identifier(value, field_name, prefix) for value in values)
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} cannot contain duplicates")
    return tuple(sorted(normalized))


@dataclass(frozen=True)
class ValidationPolicy:
    """The canonical validation contract applied to one dataset result."""

    rule_ids: tuple[str, ...]
    allows_empty_publication: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "rule_ids", _sorted_unique(self.rule_ids, "validation.rule_ids"))
        if not isinstance(self.allows_empty_publication, bool):
            raise ValueError("validation.allows_empty_publication must be a boolean")

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_ids": list(self.rule_ids),
            "allows_empty_publication": self.allows_empty_publication,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ValidationPolicy:
        return cls(
            rule_ids=tuple(str(value) for value in data["rule_ids"]),
            allows_empty_publication=data["allows_empty_publication"],
        )


def _json_value(value: Any, field_name: str = "value") -> Any:
    """Return a deterministic JSON-native copy, rejecting ambiguous values."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{field_name} cannot contain non-finite floats")
        return value
    if isinstance(value, Decimal):
        return {"$decimal": _decimal_text(value)}
    if isinstance(value, datetime):
        return {"$datetime": _utc(value, field_name).isoformat()}
    if isinstance(value, date):
        return {"$date": value.isoformat()}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{field_name} mapping keys must be strings")
            result[key] = _json_value(item, f"{field_name}.{key}")
        return {key: result[key] for key in sorted(result)}
    if isinstance(value, (list, tuple)):
        return [_json_value(item, field_name) for item in value]
    raise ValueError(f"{field_name} contains unsupported value {type(value).__name__}")


def _restore_json_value(value: Any, field_name: str = "value") -> Any:
    """Restore the typed values encoded by :func:`_json_value`."""
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, Mapping):
        if set(value) == {"$decimal"}:
            return _decimal(str(value["$decimal"]), field_name)
        if set(value) == {"$datetime"}:
            return _utc(datetime.fromisoformat(str(value["$datetime"])), field_name)
        if set(value) == {"$date"}:
            return date.fromisoformat(str(value["$date"]))
        return {key: _restore_json_value(item, f"{field_name}.{key}") for key, item in sorted(value.items())}
    if isinstance(value, list):
        return [_restore_json_value(item, field_name) for item in value]
    raise ValueError(f"{field_name} contains unsupported serialized value {type(value).__name__}")


def _freeze_domain_value(value: Any) -> Any:
    """Deep-freeze identity-bearing structured values."""
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze_domain_value(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_domain_value(item) for item in value)
    return value


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(_json_value(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _stable_id(prefix: str, identity: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(_canonical_json(identity).encode("utf-8")).hexdigest()
    return f"{prefix}_{digest}"


def _manifest_hash(payload: Mapping[str, Any]) -> str:
    """Hash a normalized manifest payload, excluding only the hash itself."""
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _dataset_id(source_id: str, dataset_key: str) -> str:
    return _stable_id("dst", {"source_id": source_id, "key": dataset_key})


def _validate_dataset_relationship(
    source_id: str,
    dataset_id: str,
    dataset_key: str,
    field_name: str,
) -> None:
    if dataset_id != _dataset_id(source_id, dataset_key):
        raise ValueError(f"{field_name} dataset_id does not match source and dataset key")


def _envelope(data: Mapping[str, Any], record_type: str) -> None:
    if data.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"unsupported {record_type} schema version: {data.get('schema_version')!r}")
    if data.get("record_type") != record_type:
        raise ValueError(f"expected record_type {record_type!r}")


def _check_serialized_id(data: Mapping[str, Any], field_name: str, actual: str) -> None:
    if data.get(field_name) != actual:
        raise ValueError(f"serialized {field_name} does not match the record identity")


def _check_serialized_manifest_hash(data: Mapping[str, Any], actual: str) -> None:
    serialized_hash = data.get("manifest_hash")
    serialized_payload = {key: value for key, value in data.items() if key != "manifest_hash"}
    if serialized_hash != _manifest_hash(serialized_payload) or serialized_hash != actual:
        raise ValueError("serialized manifest_hash does not match the manifest content")


@dataclass(frozen=True)
class Timestamp:
    value: datetime
    inferred: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", _utc(self.value, "timestamp"))

    def to_dict(self) -> dict[str, Any]:
        return {"value": self.value.isoformat(), "inferred": self.inferred}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Timestamp:
        return cls(value=datetime.fromisoformat(str(data["value"])), inferred=bool(data["inferred"]))


@dataclass(frozen=True)
class DeliveryWindow:
    start: date
    end: date
    label: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "start", _date(self.start, "delivery_window.start"))
        object.__setattr__(self, "end", _date(self.end, "delivery_window.end"))
        if self.end < self.start:
            raise ValueError("delivery_window.end cannot precede start")
        if self.label is not None:
            object.__setattr__(self, "label", _text(self.label, "delivery_window.label"))

    def to_dict(self) -> dict[str, Any]:
        return {"start": self.start.isoformat(), "end": self.end.isoformat(), "label": self.label}

    def identity_dict(self) -> dict[str, str]:
        """Logical window identity; labels are presentation/source metadata."""
        return {"start": self.start.isoformat(), "end": self.end.isoformat()}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DeliveryWindow:
        return cls(
            start=date.fromisoformat(str(data["start"])),
            end=date.fromisoformat(str(data["end"])),
            label=data.get("label"),
        )


@dataclass(frozen=True)
class ContractIdentity:
    exchange: str
    code: str
    delivery_month: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "exchange", _key(self.exchange, "contract.exchange"))
        object.__setattr__(self, "code", _text(self.code, "contract.code").upper())
        month = _text(self.delivery_month, "contract.delivery_month")
        try:
            date.fromisoformat(f"{month}-01")
        except ValueError as exc:
            raise ValueError("contract.delivery_month must be YYYY-MM") from exc
        object.__setattr__(self, "delivery_month", month)

    def to_dict(self) -> dict[str, str]:
        return {"exchange": self.exchange, "code": self.code, "delivery_month": self.delivery_month}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ContractIdentity:
        return cls(exchange=str(data["exchange"]), code=str(data["code"]), delivery_month=str(data["delivery_month"]))


@dataclass(frozen=True)
class Source:
    key: str
    name: str
    attribution: str | None = None
    source_id: str = field(init=False)
    schema_version: ClassVar[int] = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "key", _key(self.key, "source.key"))
        object.__setattr__(self, "name", _text(self.name, "source.name"))
        if self.attribution is not None:
            object.__setattr__(self, "attribution", _text(self.attribution, "source.attribution"))
        object.__setattr__(self, "source_id", _stable_id("src", {"key": self.key}))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "record_type": "source",
            "source_id": self.source_id,
            "key": self.key,
            "name": self.name,
            "attribution": self.attribution,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Source:
        _envelope(data, "source")
        result = cls(key=str(data["key"]), name=str(data["name"]), attribution=data.get("attribution"))
        _check_serialized_id(data, "source_id", result.source_id)
        return result


@dataclass(frozen=True)
class Dataset:
    source_id: str
    key: str
    name: str
    dataset_id: str = field(init=False)
    schema_version: ClassVar[int] = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_id", _identifier(self.source_id, "dataset.source_id", "src"))
        object.__setattr__(self, "key", _key(self.key, "dataset.key"))
        object.__setattr__(self, "name", _text(self.name, "dataset.name"))
        object.__setattr__(self, "dataset_id", _dataset_id(self.source_id, self.key))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "record_type": "dataset",
            "dataset_id": self.dataset_id,
            "source_id": self.source_id,
            "key": self.key,
            "name": self.name,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Dataset:
        _envelope(data, "dataset")
        result = cls(source_id=str(data["source_id"]), key=str(data["key"]), name=str(data["name"]))
        _check_serialized_id(data, "dataset_id", result.dataset_id)
        return result


@dataclass(frozen=True)
class ArtifactReference:
    source_id: str
    dataset_id: str
    dataset_key: str
    content_hash: str
    content_retained: bool
    media_type: str | None = None
    artifact_id: str = field(init=False)
    schema_version: ClassVar[int] = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_id", _identifier(self.source_id, "artifact.source_id", "src"))
        object.__setattr__(self, "dataset_id", _identifier(self.dataset_id, "artifact.dataset_id", "dst"))
        object.__setattr__(self, "dataset_key", _key(self.dataset_key, "artifact.dataset_key"))
        _validate_dataset_relationship(
            self.source_id,
            self.dataset_id,
            self.dataset_key,
            "artifact",
        )
        object.__setattr__(self, "content_hash", _hash(self.content_hash))
        if self.media_type is not None:
            object.__setattr__(self, "media_type", _text(self.media_type, "artifact.media_type").lower())
        object.__setattr__(
            self,
            "artifact_id",
            _stable_id(
                "art", {"source_id": self.source_id, "dataset_id": self.dataset_id, "content_hash": self.content_hash}
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "record_type": "artifact-reference",
            "artifact_id": self.artifact_id,
            "source_id": self.source_id,
            "dataset_id": self.dataset_id,
            "dataset_key": self.dataset_key,
            "content_hash": self.content_hash,
            "content_retained": self.content_retained,
            "media_type": self.media_type,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ArtifactReference:
        _envelope(data, "artifact-reference")
        result = cls(
            source_id=str(data["source_id"]),
            dataset_id=str(data["dataset_id"]),
            dataset_key=str(data["dataset_key"]),
            content_hash=str(data["content_hash"]),
            content_retained=bool(data["content_retained"]),
            media_type=data.get("media_type"),
        )
        _check_serialized_id(data, "artifact_id", result.artifact_id)
        return result


@dataclass(frozen=True)
class RawArtifact:
    """An immutable fetched response plus retrieval metadata.

    Metadata-only artifacts carry a content hash and byte size but no content,
    matching sources whose retention policy prohibits storing response bytes.
    """

    reference: ArtifactReference
    retrieval_url: str
    retrieved_at: Timestamp
    response_status: int
    byte_size: int
    content: bytes | None = None
    schema_version: ClassVar[int] = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "retrieval_url",
            _text(self.retrieval_url, "raw_artifact.retrieval_url"),
        )
        if (
            not isinstance(self.response_status, int)
            or isinstance(self.response_status, bool)
            or not 100 <= self.response_status <= 599
        ):
            raise ValueError("raw_artifact.response_status must be an HTTP status integer")
        if not isinstance(self.byte_size, int) or isinstance(self.byte_size, bool) or self.byte_size < 0:
            raise ValueError("raw_artifact.byte_size must be a non-negative integer")
        if self.reference.content_retained:
            if self.content is None:
                raise ValueError("a content-retained artifact requires content bytes")
            if not isinstance(self.content, bytes):
                raise ValueError("raw_artifact.content must be bytes")
            if len(self.content) != self.byte_size:
                raise ValueError("raw_artifact.byte_size does not match retained content")
            digest = hashlib.sha256(self.content).hexdigest()
            if digest != self.reference.content_hash:
                raise ValueError("raw_artifact.content does not match its content hash")
        elif self.content is not None:
            raise ValueError("a metadata-only artifact cannot retain content bytes")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "record_type": "raw-artifact",
            "artifact_id": self.reference.artifact_id,
            "reference": self.reference.to_dict(),
            "retrieval_url": self.retrieval_url,
            "retrieved_at": self.retrieved_at.to_dict(),
            "response_status": self.response_status,
            "byte_size": self.byte_size,
            "content_base64": (base64.b64encode(self.content).decode("ascii") if self.content is not None else None),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> RawArtifact:
        _envelope(data, "raw-artifact")
        encoded_content = data.get("content_base64")
        try:
            content = base64.b64decode(str(encoded_content), validate=True) if encoded_content is not None else None
        except ValueError as exc:
            raise ValueError("raw_artifact.content_base64 is invalid") from exc
        result = cls(
            reference=ArtifactReference.from_dict(data["reference"]),
            retrieval_url=str(data["retrieval_url"]),
            retrieved_at=Timestamp.from_dict(data["retrieved_at"]),
            response_status=int(data["response_status"]),
            byte_size=int(data["byte_size"]),
            content=content,
        )
        _check_serialized_id(data, "artifact_id", result.reference.artifact_id)
        return result


@dataclass(frozen=True)
class ObservationIdentity:
    source_id: str
    dataset_id: str
    dataset_key: str
    commodity: str
    product_form: str
    price_type: str
    currency: str
    unit: str
    effective_date: date
    venue: str | None = None
    location: str | None = None
    contract: ContractIdentity | None = None
    delivery_window: DeliveryWindow | None = None
    source_record_id: str | None = None
    observation_id: str = field(init=False)
    schema_version: ClassVar[int] = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_id", _identifier(self.source_id, "observation.source_id", "src"))
        object.__setattr__(self, "dataset_id", _identifier(self.dataset_id, "observation.dataset_id", "dst"))
        object.__setattr__(self, "dataset_key", _key(self.dataset_key, "observation.dataset_key"))
        _validate_dataset_relationship(
            self.source_id,
            self.dataset_id,
            self.dataset_key,
            "observation",
        )
        for field_name in ("commodity", "product_form", "price_type", "unit"):
            object.__setattr__(self, field_name, _key(getattr(self, field_name), f"observation.{field_name}"))
        object.__setattr__(self, "currency", _text(self.currency, "observation.currency").upper())
        if not re.fullmatch(r"[A-Z]{3}", self.currency):
            raise ValueError("observation.currency must be a three-letter code")
        object.__setattr__(self, "effective_date", _date(self.effective_date, "observation.effective_date"))
        if self.venue is None and self.location is None:
            raise ValueError("observation identity requires a venue or location")
        if self.venue is not None:
            object.__setattr__(self, "venue", _key(self.venue, "observation.venue"))
        if self.location is not None:
            object.__setattr__(self, "location", _key(self.location, "observation.location"))
        if self.contract is not None and self.delivery_window is not None:
            raise ValueError("observation identity cannot combine a contract and delivery window")
        if self.source_record_id is not None:
            object.__setattr__(self, "source_record_id", _text(self.source_record_id, "observation.source_record_id"))
        identity = self.identity_dict()
        if self.delivery_window is not None:
            identity["delivery_window"] = self.delivery_window.identity_dict()
        object.__setattr__(self, "observation_id", _stable_id("obs", identity))

    def identity_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "dataset_id": self.dataset_id,
            "dataset_key": self.dataset_key,
            "commodity": self.commodity,
            "product_form": self.product_form,
            "venue": self.venue,
            "location": self.location,
            "price_type": self.price_type,
            "currency": self.currency,
            "unit": self.unit,
            "contract": self.contract.to_dict() if self.contract else None,
            "delivery_window": self.delivery_window.to_dict() if self.delivery_window else None,
            "effective_date": self.effective_date.isoformat(),
            "source_record_id": self.source_record_id,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "record_type": "observation-identity",
            "observation_id": self.observation_id,
            **self.identity_dict(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ObservationIdentity:
        _envelope(data, "observation-identity")
        contract = data.get("contract")
        delivery_window = data.get("delivery_window")
        result = cls(
            source_id=str(data["source_id"]),
            dataset_id=str(data["dataset_id"]),
            dataset_key=str(data["dataset_key"]),
            commodity=str(data["commodity"]),
            product_form=str(data["product_form"]),
            price_type=str(data["price_type"]),
            currency=str(data["currency"]),
            unit=str(data["unit"]),
            effective_date=date.fromisoformat(str(data["effective_date"])),
            venue=data.get("venue"),
            location=data.get("location"),
            contract=ContractIdentity.from_dict(contract) if isinstance(contract, Mapping) else None,
            delivery_window=DeliveryWindow.from_dict(delivery_window) if isinstance(delivery_window, Mapping) else None,
            source_record_id=data.get("source_record_id"),
        )
        _check_serialized_id(data, "observation_id", result.observation_id)
        return result


@dataclass(frozen=True)
class CandidateObservation:
    """A parsed record that has not completed validation and is not publishable."""

    identity: ObservationIdentity
    value: Decimal | int | str
    artifact: ArtifactReference
    parser_version: str
    parsed_at: datetime
    source_published_at: Timestamp | None = None
    observed_at: Timestamp | None = None
    effective_date_inferred: bool = False
    candidate_id: str = field(init=False)
    schema_version: ClassVar[int] = SCHEMA_VERSION

    def __post_init__(self) -> None:
        value = _decimal(self.value, "candidate.value")
        object.__setattr__(self, "value", value)
        if (
            self.artifact.source_id != self.identity.source_id
            or self.artifact.dataset_id != self.identity.dataset_id
            or self.artifact.dataset_key != self.identity.dataset_key
        ):
            raise ValueError("candidate identity and artifact must reference the same source and dataset")
        object.__setattr__(self, "parser_version", _text(self.parser_version, "candidate.parser_version"))
        object.__setattr__(self, "parsed_at", _utc(self.parsed_at, "candidate.parsed_at"))
        object.__setattr__(
            self,
            "candidate_id",
            _stable_id(
                "cnd",
                {
                    "observation_id": self.identity.observation_id,
                    "value": _decimal_text(value),
                    "artifact_id": self.artifact.artifact_id,
                    "parser_version": self.parser_version,
                    "parsed_at": self.parsed_at.isoformat(),
                    "source_published_at": (self.source_published_at.to_dict() if self.source_published_at else None),
                    "observed_at": self.observed_at.to_dict() if self.observed_at else None,
                    "effective_date_inferred": self.effective_date_inferred,
                },
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "record_type": "candidate-observation",
            "candidate_id": self.candidate_id,
            "identity": self.identity.to_dict(),
            "value": _decimal_text(cast(Decimal, self.value)),
            "artifact": self.artifact.to_dict(),
            "parser_version": self.parser_version,
            "parsed_at": self.parsed_at.isoformat(),
            "source_published_at": (self.source_published_at.to_dict() if self.source_published_at else None),
            "observed_at": self.observed_at.to_dict() if self.observed_at else None,
            "effective_date_inferred": self.effective_date_inferred,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CandidateObservation:
        _envelope(data, "candidate-observation")
        source_published = data.get("source_published_at")
        observed = data.get("observed_at")
        result = cls(
            identity=ObservationIdentity.from_dict(data["identity"]),
            value=str(data["value"]),
            artifact=ArtifactReference.from_dict(data["artifact"]),
            parser_version=str(data["parser_version"]),
            parsed_at=datetime.fromisoformat(str(data["parsed_at"])),
            source_published_at=(
                Timestamp.from_dict(source_published) if isinstance(source_published, Mapping) else None
            ),
            observed_at=(Timestamp.from_dict(observed) if isinstance(observed, Mapping) else None),
            effective_date_inferred=bool(data["effective_date_inferred"]),
        )
        _check_serialized_id(data, "candidate_id", result.candidate_id)
        return result


@dataclass(frozen=True)
class ObservationRevision:
    identity: ObservationIdentity
    value: Decimal | int | str
    ingested_at: datetime
    quality_state: QualityState
    public_eligible: bool
    artifact: ArtifactReference | None = None
    parser_version: str | None = None
    source_published_at: Timestamp | None = None
    observed_at: Timestamp | None = None
    effective_date_inferred: bool = False
    open_value: Decimal | int | str | None = None
    high_value: Decimal | int | str | None = None
    low_value: Decimal | int | str | None = None
    close_value: Decimal | int | str | None = None
    volume: Decimal | int | str | None = None
    finding_ids: tuple[str, ...] = ()
    supersedes_revision_id: str | None = None
    correction_type: str | None = None
    correction_reason: str | None = None
    revision_id: str = field(init=False)
    schema_version: ClassVar[int] = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", _decimal(self.value, "revision.value"))
        object.__setattr__(self, "ingested_at", _utc(self.ingested_at, "revision.ingested_at"))
        object.__setattr__(self, "quality_state", _enum(QualityState, self.quality_state))
        if self.quality_state is not QualityState.LEGACY and (self.artifact is None or self.parser_version is None):
            raise ValueError("non-legacy revisions require an artifact and parser version")
        if self.artifact is not None and (
            self.artifact.source_id != self.identity.source_id
            or self.artifact.dataset_id != self.identity.dataset_id
            or self.artifact.dataset_key != self.identity.dataset_key
        ):
            raise ValueError("revision identity and artifact must reference the same source and dataset")
        if self.parser_version is not None:
            object.__setattr__(self, "parser_version", _text(self.parser_version, "revision.parser_version"))
        for field_name in ("open_value", "high_value", "low_value", "close_value", "volume"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _decimal(value, f"revision.{field_name}"))
        object.__setattr__(
            self,
            "finding_ids",
            _sorted_identifiers(self.finding_ids, "revision.finding_ids", "fnd"),
        )
        if self.supersedes_revision_id is not None:
            object.__setattr__(
                self,
                "supersedes_revision_id",
                _identifier(self.supersedes_revision_id, "revision.supersedes_revision_id", "rev"),
            )
        if self.correction_type is not None:
            object.__setattr__(self, "correction_type", _key(self.correction_type, "revision.correction_type"))
        if self.correction_reason is not None:
            object.__setattr__(self, "correction_reason", _text(self.correction_reason, "revision.correction_reason"))
        if (self.correction_type is None) != (self.correction_reason is None):
            raise ValueError("revision correction type and reason must be provided together")
        object.__setattr__(self, "revision_id", _stable_id("rev", self.assertion_dict()))

    def assertion_dict(self) -> dict[str, Any]:
        decimal_fields = ("value", "open_value", "high_value", "low_value", "close_value", "volume")
        result: dict[str, Any] = {
            "observation_id": self.identity.observation_id,
            "ingested_at": self.ingested_at.isoformat(),
            "quality_state": self.quality_state.value,
            "public_eligible": self.public_eligible,
            "artifact_id": self.artifact.artifact_id if self.artifact else None,
            "parser_version": self.parser_version,
            "source_published_at": self.source_published_at.to_dict() if self.source_published_at else None,
            "observed_at": self.observed_at.to_dict() if self.observed_at else None,
            "effective_date_inferred": self.effective_date_inferred,
            "finding_ids": list(self.finding_ids),
            "supersedes_revision_id": self.supersedes_revision_id,
            "correction_type": self.correction_type,
            "correction_reason": self.correction_reason,
        }
        for field_name in decimal_fields:
            value = getattr(self, field_name)
            result[field_name] = _decimal_text(value) if value is not None else None
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "record_type": "observation-revision",
            "revision_id": self.revision_id,
            "identity": self.identity.to_dict(),
            "artifact": self.artifact.to_dict() if self.artifact else None,
            **self.assertion_dict(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ObservationRevision:
        _envelope(data, "observation-revision")
        artifact = data.get("artifact")
        source_published = data.get("source_published_at")
        observed = data.get("observed_at")
        result = cls(
            identity=ObservationIdentity.from_dict(data["identity"]),
            value=str(data["value"]),
            ingested_at=datetime.fromisoformat(str(data["ingested_at"])),
            quality_state=QualityState(str(data["quality_state"])),
            public_eligible=bool(data["public_eligible"]),
            artifact=ArtifactReference.from_dict(artifact) if isinstance(artifact, Mapping) else None,
            parser_version=data.get("parser_version"),
            source_published_at=Timestamp.from_dict(source_published)
            if isinstance(source_published, Mapping)
            else None,
            observed_at=Timestamp.from_dict(observed) if isinstance(observed, Mapping) else None,
            effective_date_inferred=bool(data.get("effective_date_inferred", False)),
            open_value=data.get("open_value"),
            high_value=data.get("high_value"),
            low_value=data.get("low_value"),
            close_value=data.get("close_value"),
            volume=data.get("volume"),
            finding_ids=tuple(data.get("finding_ids", ())),
            supersedes_revision_id=data.get("supersedes_revision_id"),
            correction_type=data.get("correction_type"),
            correction_reason=data.get("correction_reason"),
        )
        _check_serialized_id(data, "revision_id", result.revision_id)
        return result


@dataclass(frozen=True)
class Finding:
    run_id: str
    dataset_id: str
    subject_id: str
    rule_id: str
    rule_version: str
    severity: FindingSeverity
    evidence: Mapping[str, Any]
    message: str
    finding_id: str = field(init=False)
    schema_version: ClassVar[int] = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", _identifier(self.run_id, "finding.run_id", "run"))
        object.__setattr__(self, "dataset_id", _identifier(self.dataset_id, "finding.dataset_id", "dst"))
        object.__setattr__(self, "subject_id", _text(self.subject_id, "finding.subject_id"))
        object.__setattr__(self, "rule_id", _key(self.rule_id, "finding.rule_id"))
        object.__setattr__(self, "rule_version", _text(self.rule_version, "finding.rule_version"))
        object.__setattr__(self, "severity", _enum(FindingSeverity, self.severity))
        evidence = _restore_json_value(_json_value(self.evidence, "finding.evidence"), "finding.evidence")
        object.__setattr__(self, "evidence", _freeze_domain_value(evidence))
        object.__setattr__(self, "message", _text(self.message, "finding.message"))
        object.__setattr__(self, "finding_id", _stable_id("fnd", self.identity_dict()))

    def identity_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "dataset_id": self.dataset_id,
            "subject_id": self.subject_id,
            "rule_id": self.rule_id,
            "rule_version": self.rule_version,
            "severity": self.severity.value,
            "evidence": _json_value(self.evidence, "finding.evidence"),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "record_type": "finding",
            "finding_id": self.finding_id,
            **self.identity_dict(),
            "message": self.message,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Finding:
        _envelope(data, "finding")
        result = cls(
            run_id=str(data["run_id"]),
            dataset_id=str(data["dataset_id"]),
            subject_id=str(data["subject_id"]),
            rule_id=str(data["rule_id"]),
            rule_version=str(data["rule_version"]),
            severity=FindingSeverity(str(data["severity"])),
            evidence=_restore_json_value(data["evidence"], "finding.evidence"),
            message=str(data["message"]),
        )
        _check_serialized_id(data, "finding_id", result.finding_id)
        return result


@dataclass(frozen=True)
class DatasetResult:
    run_id: str
    dataset_id: str
    status: DatasetResultStatus
    candidate_count: int
    accepted_revision_ids: tuple[str, ...]
    artifact_references: tuple[ArtifactReference, ...]
    findings: tuple[Finding, ...]
    coverage: Decimal | int | str
    freshness: FreshnessState
    eligible: bool
    validation_policy: ValidationPolicy
    as_of_date: date | None = None
    error: str | None = None
    dataset_result_id: str = field(init=False)
    schema_version: ClassVar[int] = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", _identifier(self.run_id, "dataset_result.run_id", "run"))
        object.__setattr__(
            self,
            "dataset_id",
            _identifier(self.dataset_id, "dataset_result.dataset_id", "dst"),
        )
        object.__setattr__(self, "status", _enum(DatasetResultStatus, self.status))
        if (
            not isinstance(self.candidate_count, int)
            or isinstance(self.candidate_count, bool)
            or self.candidate_count < 0
        ):
            raise ValueError("dataset_result.candidate_count must be a non-negative integer")
        object.__setattr__(
            self,
            "accepted_revision_ids",
            _sorted_identifiers(
                self.accepted_revision_ids,
                "dataset_result.accepted_revision_ids",
                "rev",
            ),
        )
        artifacts = tuple(sorted(self.artifact_references, key=lambda artifact: artifact.artifact_id))
        if len(artifacts) != len({artifact.artifact_id for artifact in artifacts}):
            raise ValueError("dataset_result.artifact_references cannot contain duplicates")
        if any(artifact.dataset_id != self.dataset_id for artifact in artifacts):
            raise ValueError("dataset result artifacts must belong to its dataset")
        object.__setattr__(self, "artifact_references", artifacts)
        findings = tuple(sorted(self.findings, key=lambda finding: finding.finding_id))
        if len(findings) != len({finding.finding_id for finding in findings}):
            raise ValueError("dataset_result.findings cannot contain duplicates")
        if any(finding.run_id != self.run_id or finding.dataset_id != self.dataset_id for finding in findings):
            raise ValueError("dataset result findings must belong to its run and dataset")
        object.__setattr__(self, "findings", findings)
        if len(self.accepted_revision_ids) > self.candidate_count:
            raise ValueError("accepted revisions cannot exceed candidate count")
        coverage = _decimal(self.coverage, "dataset_result.coverage")
        object.__setattr__(self, "coverage", coverage)
        if coverage < 0 or coverage > 1:
            raise ValueError("dataset_result.coverage must be between 0 and 1")
        object.__setattr__(self, "freshness", _enum(FreshnessState, self.freshness))
        if not isinstance(self.eligible, bool):
            raise ValueError("dataset_result.eligible must be a boolean")
        if not isinstance(self.validation_policy, ValidationPolicy):
            raise ValueError("dataset_result.validation_policy must be a ValidationPolicy")
        if self.as_of_date is not None:
            object.__setattr__(self, "as_of_date", _date(self.as_of_date, "dataset_result.as_of_date"))
        if self.error is not None:
            object.__setattr__(self, "error", _text(self.error, "dataset_result.error"))
        failure_statuses = {
            DatasetResultStatus.EXTERNAL_FAILURE,
            DatasetResultStatus.CONTRACT_FAILURE,
        }
        successful_statuses = {
            DatasetResultStatus.SUCCESS,
            DatasetResultStatus.LEGITIMATE_EMPTY,
        }
        severities = {finding.severity for finding in self.findings}
        if self.status in successful_statuses and severities & {
            FindingSeverity.QUARANTINE,
            FindingSeverity.REJECT,
        }:
            raise ValueError("a successful dataset result cannot contain unresolved quarantine or reject findings")
        if self.status in successful_statuses and self.freshness is FreshnessState.UNAVAILABLE:
            raise ValueError("a successful dataset result must have an available freshness state")
        if self.status is DatasetResultStatus.SUCCESS and not self.accepted_revision_ids:
            raise ValueError("a successful dataset result requires an accepted revision")
        if self.accepted_revision_ids and not self.artifact_references:
            raise ValueError("accepted revisions require an artifact reference")
        if self.status is DatasetResultStatus.LEGITIMATE_EMPTY:
            if not self.validation_policy.allows_empty_publication:
                raise ValueError("a legitimate empty result requires a dataset contract that permits it")
            if self.candidate_count != 0:
                raise ValueError("a legitimate empty result cannot contain candidate rows")
        if self.status is not DatasetResultStatus.SUCCESS and self.accepted_revision_ids:
            raise ValueError("only a successful dataset result may expose accepted revisions")
        if self.status in failure_statuses:
            if self.eligible:
                raise ValueError("a failed dataset result cannot be eligible")
            if self.error is None:
                raise ValueError("an external or contract failure requires an error")
        elif self.error is not None:
            raise ValueError("only an external or contract failure may contain an error")
        if self.status is DatasetResultStatus.CONTRACT_FAILURE and FindingSeverity.REJECT not in severities:
            raise ValueError("a contract failure requires a reject finding")
        if self.status is DatasetResultStatus.QUARANTINED:
            if self.eligible:
                raise ValueError("a quarantined dataset result cannot be eligible")
            if FindingSeverity.QUARANTINE not in severities or FindingSeverity.REJECT in severities:
                raise ValueError("a quarantined dataset result requires quarantine findings without rejects")
        object.__setattr__(
            self, "dataset_result_id", _stable_id("dsr", {"run_id": self.run_id, "dataset_id": self.dataset_id})
        )

    @property
    def accepted_count(self) -> int:
        return len(self.accepted_revision_ids)

    @property
    def artifact_ids(self) -> tuple[str, ...]:
        return tuple(artifact.artifact_id for artifact in self.artifact_references)

    @property
    def finding_ids(self) -> tuple[str, ...]:
        return tuple(finding.finding_id for finding in self.findings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "record_type": "dataset-result",
            "dataset_result_id": self.dataset_result_id,
            "run_id": self.run_id,
            "dataset_id": self.dataset_id,
            "status": self.status.value,
            "candidate_count": self.candidate_count,
            "accepted_revision_ids": list(self.accepted_revision_ids),
            "artifact_references": [artifact.to_dict() for artifact in self.artifact_references],
            "findings": [finding.to_dict() for finding in self.findings],
            "coverage": _decimal_text(cast(Decimal, self.coverage)),
            "freshness": self.freshness.value,
            "eligible": self.eligible,
            "validation_policy": self.validation_policy.to_dict(),
            "as_of_date": self.as_of_date.isoformat() if self.as_of_date else None,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DatasetResult:
        _envelope(data, "dataset-result")
        result = cls(
            run_id=str(data["run_id"]),
            dataset_id=str(data["dataset_id"]),
            status=DatasetResultStatus(str(data["status"])),
            candidate_count=int(data["candidate_count"]),
            accepted_revision_ids=tuple(data["accepted_revision_ids"]),
            artifact_references=tuple(
                ArtifactReference.from_dict(artifact) for artifact in data["artifact_references"]
            ),
            findings=tuple(Finding.from_dict(finding) for finding in data["findings"]),
            coverage=str(data["coverage"]),
            freshness=FreshnessState(str(data["freshness"])),
            eligible=data["eligible"],
            validation_policy=ValidationPolicy.from_dict(data["validation_policy"]),
            as_of_date=date.fromisoformat(str(data["as_of_date"])) if data.get("as_of_date") else None,
            error=data.get("error"),
        )
        _check_serialized_id(data, "dataset_result_id", result.dataset_result_id)
        return result


def evaluate_run_status(
    results: Sequence[DatasetResult],
    *,
    critical_dataset_ids: Collection[str],
    max_hard_failures: int,
) -> RunStatus:
    """Evaluate terminal run status from typed dataset outcomes alone."""
    if (
        not isinstance(max_hard_failures, int)
        or isinstance(max_hard_failures, bool)
        or max_hard_failures < 0
    ):
        raise ValueError("max_hard_failures must be a non-negative integer")
    normalized_critical_ids = {
        _identifier(dataset_id, "critical_dataset_ids", "dst")
        for dataset_id in critical_dataset_ids
    }
    results_by_dataset = {result.dataset_id: result for result in results}
    if len(results_by_dataset) != len(results):
        raise ValueError("run status evaluation requires one result per dataset")
    if len({result.run_id for result in results}) > 1:
        raise ValueError("run status evaluation cannot combine results from different runs")

    critical_failure = any(
        dataset_id not in results_by_dataset
        or results_by_dataset[dataset_id].status is not DatasetResultStatus.SUCCESS
        or not results_by_dataset[dataset_id].eligible
        or results_by_dataset[dataset_id].freshness is not FreshnessState.CURRENT
        for dataset_id in normalized_critical_ids
    )
    hard_failure_statuses = {
        DatasetResultStatus.EXTERNAL_FAILURE,
        DatasetResultStatus.CONTRACT_FAILURE,
        DatasetResultStatus.QUARANTINED,
    }
    hard_failure_count = sum(
        result.status in hard_failure_statuses for result in results_by_dataset.values()
    )
    if critical_failure or hard_failure_count > max_hard_failures:
        return RunStatus.FAILED
    return RunStatus.SUCCEEDED


@dataclass(frozen=True)
class Run:
    code_revision: str
    started_at: datetime
    ended_at: datetime | None
    status: RunStatus
    dataset_result_ids: tuple[str, ...] = ()
    parser_versions: Mapping[str, str] = field(default_factory=dict)
    findings_summary: Mapping[FindingSeverity, int] = field(default_factory=dict)
    run_id: str = field(init=False)
    manifest_hash: str = field(init=False)
    schema_version: ClassVar[int] = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "code_revision", _text(self.code_revision, "run.code_revision"))
        object.__setattr__(self, "started_at", _utc(self.started_at, "run.started_at"))
        if self.ended_at is not None:
            object.__setattr__(self, "ended_at", _utc(self.ended_at, "run.ended_at"))
            if self.ended_at < self.started_at:
                raise ValueError("run.ended_at cannot precede started_at")
        object.__setattr__(self, "status", _enum(RunStatus, self.status))
        object.__setattr__(
            self,
            "dataset_result_ids",
            _sorted_identifiers(self.dataset_result_ids, "run.dataset_result_ids", "dsr"),
        )
        parser_versions = {
            _identifier(dataset_id, "run.parser_versions key", "dst"): _text(
                version,
                f"run.parser_versions.{dataset_id}",
            )
            for dataset_id, version in self.parser_versions.items()
        }
        object.__setattr__(
            self,
            "parser_versions",
            MappingProxyType({key: parser_versions[key] for key in sorted(parser_versions)}),
        )
        findings_summary: dict[FindingSeverity, int] = {}
        for raw_severity, count in self.findings_summary.items():
            severity = _enum(FindingSeverity, raw_severity)
            if not isinstance(count, int) or isinstance(count, bool) or count < 0:
                raise ValueError(f"run.findings_summary.{severity.value} must be a non-negative integer")
            findings_summary[severity] = count
        object.__setattr__(
            self,
            "findings_summary",
            MappingProxyType(dict(sorted(findings_summary.items(), key=lambda item: item[0].value))),
        )
        if self.status is RunStatus.RUNNING and self.ended_at is not None:
            raise ValueError("a running run cannot have ended_at")
        if self.status is not RunStatus.RUNNING and self.ended_at is None:
            raise ValueError("a terminal run requires ended_at")
        object.__setattr__(
            self,
            "run_id",
            _stable_id("run", {"code_revision": self.code_revision, "started_at": self.started_at.isoformat()}),
        )
        object.__setattr__(self, "manifest_hash", _manifest_hash(self._manifest_payload()))

    def _manifest_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "record_type": "run",
            "run_id": self.run_id,
            "code_revision": self.code_revision,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "status": self.status.value,
            "dataset_result_ids": list(self.dataset_result_ids),
            "parser_versions": dict(self.parser_versions),
            "findings_summary": {
                severity.value: count for severity, count in self.findings_summary.items()
            },
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self._manifest_payload(), "manifest_hash": self.manifest_hash}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Run:
        _envelope(data, "run")
        result = cls(
            code_revision=str(data["code_revision"]),
            started_at=datetime.fromisoformat(str(data["started_at"])),
            ended_at=datetime.fromisoformat(str(data["ended_at"])) if data.get("ended_at") else None,
            status=RunStatus(str(data["status"])),
            dataset_result_ids=tuple(data["dataset_result_ids"]),
            parser_versions={str(key): str(value) for key, value in data["parser_versions"].items()},
            findings_summary={
                FindingSeverity(str(severity)): int(count)
                for severity, count in data["findings_summary"].items()
            },
        )
        _check_serialized_id(data, "run_id", result.run_id)
        _check_serialized_manifest_hash(data, result.manifest_hash)
        return result


@dataclass(frozen=True)
class Edition:
    run_id: str
    created_at: datetime
    status: EditionStatus
    revision_ids: tuple[str, ...]
    derived_revision_ids: tuple[str, ...] = ()
    generated_artifact_hashes: Mapping[str, str] = field(default_factory=dict)
    edition_id: str = field(init=False)
    manifest_hash: str = field(init=False)
    schema_version: ClassVar[int] = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", _identifier(self.run_id, "edition.run_id", "run"))
        object.__setattr__(self, "created_at", _utc(self.created_at, "edition.created_at"))
        object.__setattr__(self, "status", _enum(EditionStatus, self.status))
        object.__setattr__(
            self,
            "revision_ids",
            _sorted_identifiers(self.revision_ids, "edition.revision_ids", "rev"),
        )
        object.__setattr__(
            self,
            "derived_revision_ids",
            _sorted_identifiers(
                self.derived_revision_ids,
                "edition.derived_revision_ids",
                "rev",
            ),
        )
        hashes = {
            str(key): _hash(value, f"edition.generated_artifact_hashes.{key}")
            for key, value in self.generated_artifact_hashes.items()
        }
        object.__setattr__(
            self,
            "generated_artifact_hashes",
            MappingProxyType({key: hashes[key] for key in sorted(hashes)}),
        )
        object.__setattr__(
            self,
            "edition_id",
            _stable_id(
                "edn",
                {
                    "run_id": self.run_id,
                    "created_at": self.created_at.isoformat(),
                    "revision_ids": list(self.revision_ids),
                    "derived_revision_ids": list(self.derived_revision_ids),
                },
            ),
        )
        object.__setattr__(self, "manifest_hash", _manifest_hash(self._manifest_payload()))

    def _manifest_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "record_type": "edition",
            "edition_id": self.edition_id,
            "run_id": self.run_id,
            "created_at": self.created_at.isoformat(),
            "status": self.status.value,
            "revision_ids": list(self.revision_ids),
            "derived_revision_ids": list(self.derived_revision_ids),
            "generated_artifact_hashes": dict(self.generated_artifact_hashes),
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self._manifest_payload(), "manifest_hash": self.manifest_hash}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Edition:
        _envelope(data, "edition")
        result = cls(
            run_id=str(data["run_id"]),
            created_at=datetime.fromisoformat(str(data["created_at"])),
            status=EditionStatus(str(data["status"])),
            revision_ids=tuple(data["revision_ids"]),
            derived_revision_ids=tuple(data["derived_revision_ids"]),
            generated_artifact_hashes=data["generated_artifact_hashes"],
        )
        _check_serialized_id(data, "edition_id", result.edition_id)
        _check_serialized_manifest_hash(data, result.manifest_hash)
        return result


@dataclass(frozen=True)
class Promotion:
    """An append-only record of changing the promoted-edition pointer."""

    edition_id: str
    promoted_at: datetime
    verification_evidence: tuple[str, ...]
    previous_edition_id: str | None = None
    promotion_id: str = field(init=False)
    schema_version: ClassVar[int] = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "edition_id",
            _identifier(self.edition_id, "promotion.edition_id", "edn"),
        )
        if self.previous_edition_id is not None:
            object.__setattr__(
                self,
                "previous_edition_id",
                _identifier(
                    self.previous_edition_id,
                    "promotion.previous_edition_id",
                    "edn",
                ),
            )
            if self.previous_edition_id == self.edition_id:
                raise ValueError("promotion must change the current edition")
        object.__setattr__(self, "promoted_at", _utc(self.promoted_at, "promotion.promoted_at"))
        object.__setattr__(
            self,
            "verification_evidence",
            _sorted_unique(self.verification_evidence, "promotion.verification_evidence"),
        )
        if not self.verification_evidence:
            raise ValueError("promotion requires verification evidence")
        object.__setattr__(
            self,
            "promotion_id",
            _stable_id(
                "prm",
                {
                    "edition_id": self.edition_id,
                    "previous_edition_id": self.previous_edition_id,
                    "promoted_at": self.promoted_at.isoformat(),
                    "verification_evidence": list(self.verification_evidence),
                },
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "record_type": "promotion",
            "promotion_id": self.promotion_id,
            "edition_id": self.edition_id,
            "previous_edition_id": self.previous_edition_id,
            "promoted_at": self.promoted_at.isoformat(),
            "verification_evidence": list(self.verification_evidence),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Promotion:
        _envelope(data, "promotion")
        result = cls(
            edition_id=str(data["edition_id"]),
            previous_edition_id=data.get("previous_edition_id"),
            promoted_at=datetime.fromisoformat(str(data["promoted_at"])),
            verification_evidence=tuple(data["verification_evidence"]),
        )
        _check_serialized_id(data, "promotion_id", result.promotion_id)
        return result


@dataclass(frozen=True)
class Correction:
    prior_revision_id: str
    decision: CorrectionDecision
    operator: str
    reason: str
    evidence_references: tuple[str, ...]
    decided_at: datetime
    replacement_revision_id: str | None = None
    correction_id: str = field(init=False)
    schema_version: ClassVar[int] = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "prior_revision_id",
            _identifier(self.prior_revision_id, "correction.prior_revision_id", "rev"),
        )
        object.__setattr__(self, "decision", _enum(CorrectionDecision, self.decision))
        object.__setattr__(self, "operator", _text(self.operator, "correction.operator"))
        object.__setattr__(self, "reason", _text(self.reason, "correction.reason"))
        object.__setattr__(
            self, "evidence_references", _sorted_unique(self.evidence_references, "correction.evidence_references")
        )
        if not self.evidence_references:
            raise ValueError("correction requires at least one evidence reference")
        object.__setattr__(self, "decided_at", _utc(self.decided_at, "correction.decided_at"))
        if self.replacement_revision_id is not None:
            object.__setattr__(
                self,
                "replacement_revision_id",
                _identifier(
                    self.replacement_revision_id,
                    "correction.replacement_revision_id",
                    "rev",
                ),
            )
        if self.decision is CorrectionDecision.REPLACE and self.replacement_revision_id is None:
            raise ValueError("a replacement correction requires replacement_revision_id")
        if self.decision is not CorrectionDecision.REPLACE and self.replacement_revision_id is not None:
            raise ValueError("only a replacement correction may name replacement_revision_id")
        object.__setattr__(
            self,
            "correction_id",
            _stable_id(
                "cor",
                {
                    "prior_revision_id": self.prior_revision_id,
                    "decision": self.decision.value,
                    "operator": self.operator,
                    "reason": self.reason,
                    "evidence_references": list(self.evidence_references),
                    "decided_at": self.decided_at.isoformat(),
                    "replacement_revision_id": self.replacement_revision_id,
                },
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "record_type": "correction",
            "correction_id": self.correction_id,
            "prior_revision_id": self.prior_revision_id,
            "decision": self.decision.value,
            "operator": self.operator,
            "reason": self.reason,
            "evidence_references": list(self.evidence_references),
            "decided_at": self.decided_at.isoformat(),
            "replacement_revision_id": self.replacement_revision_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Correction:
        _envelope(data, "correction")
        result = cls(
            prior_revision_id=str(data["prior_revision_id"]),
            decision=CorrectionDecision(str(data["decision"])),
            operator=str(data["operator"]),
            reason=str(data["reason"]),
            evidence_references=tuple(data["evidence_references"]),
            decided_at=datetime.fromisoformat(str(data["decided_at"])),
            replacement_revision_id=data.get("replacement_revision_id"),
        )
        _check_serialized_id(data, "correction_id", result.correction_id)
        return result
