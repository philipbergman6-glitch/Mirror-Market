"""Source and dataset contracts for Mirror Market's trusted data path.

The registry is the only place that combines a DT-01 ``Source``/``Dataset``
identity with its operational and rights contracts.  Draft contracts may be
constructed with missing sections so validation can report all problems at
once; a ``ContractRegistry`` only contains complete, internally consistent
contracts.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from types import MappingProxyType
from typing import Any, ClassVar
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from trust.domain import Dataset, Source, ValidationPolicy

SCHEMA_VERSION = 1


def _text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _strings(values: Sequence[str], field_name: str) -> tuple[str, ...]:
    normalized = tuple(_text(value, field_name) for value in values)
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} cannot contain duplicates")
    return tuple(sorted(normalized))


def _record(data: Mapping[str, Any], record_type: str) -> None:
    if data.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"unsupported {record_type} schema version: {data.get('schema_version')!r}")
    if data.get("record_type") != record_type:
        raise ValueError(f"expected record_type {record_type!r}")


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


class CadenceKind(str, Enum):
    BUSINESS_DAILY = "business-daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    EVENT_DRIVEN = "event-driven"


class Criticality(str, Enum):
    CRITICAL = "critical"
    SUPPORTING = "supporting"


class RawRetention(str, Enum):
    CONTENT = "content"
    METADATA_ONLY = "metadata-only"


class RightsAction(str, Enum):
    RAW_CONTENT_RETENTION = "raw-content-retention"
    NORMALIZED_HISTORY_RETENTION = "normalized-history-retention"
    INTERNAL_DISPLAY = "internal-display"
    PUBLIC_DISPLAY = "public-display"
    DERIVED_PUBLICATION = "derived-publication"
    COMMERCIAL_USE = "commercial-use"
    REDISTRIBUTION = "redistribution"


class RightsDecision(str, Enum):
    ALLOWED = "allowed"
    PROHIBITED = "prohibited"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class CadenceContract:
    kind: CadenceKind
    expected_interval_hours: int
    timezone: str
    publication_weekdays: tuple[int, ...] | None = None
    publication_days_of_month: tuple[int, ...] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", CadenceKind(self.kind))
        if (
            not isinstance(self.expected_interval_hours, int)
            or isinstance(self.expected_interval_hours, bool)
            or self.expected_interval_hours <= 0
        ):
            raise ValueError("cadence.expected_interval_hours must be a positive integer")
        timezone_name = _text(self.timezone, "cadence.timezone")
        try:
            ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"cadence.timezone is not an IANA timezone: {timezone_name}") from exc
        object.__setattr__(self, "timezone", timezone_name)
        weekdays = self.publication_weekdays
        if weekdays is None and self.kind is CadenceKind.BUSINESS_DAILY:
            weekdays = (0, 1, 2, 3, 4)
        elif weekdays is None and self.kind is CadenceKind.WEEKLY:
            weekdays = (0,)
        if weekdays is not None:
            if (
                any(not isinstance(day, int) or isinstance(day, bool) or day < 0 or day > 6 for day in weekdays)
                or len(weekdays) != len(set(weekdays))
            ):
                raise ValueError("cadence.publication_weekdays must contain unique weekday integers 0-6")
            object.__setattr__(self, "publication_weekdays", tuple(sorted(weekdays)))
        month_days = self.publication_days_of_month
        if month_days is None and self.kind is CadenceKind.MONTHLY:
            month_days = (1,)
        if month_days is not None:
            if (
                any(not isinstance(day, int) or isinstance(day, bool) or day < 1 or day > 31 for day in month_days)
                or len(month_days) != len(set(month_days))
            ):
                raise ValueError("cadence.publication_days_of_month must contain unique day integers 1-31")
            object.__setattr__(self, "publication_days_of_month", tuple(sorted(month_days)))

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind.value,
            "expected_interval_hours": self.expected_interval_hours,
            "timezone": self.timezone,
            "publication_weekdays": list(self.publication_weekdays) if self.publication_weekdays else None,
            "publication_days_of_month": (
                list(self.publication_days_of_month) if self.publication_days_of_month else None
            ),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CadenceContract:
        return cls(
            kind=CadenceKind(str(data["kind"])),
            expected_interval_hours=int(data["expected_interval_hours"]),
            timezone=str(data["timezone"]),
            publication_weekdays=(
                tuple(int(value) for value in data["publication_weekdays"])
                if data.get("publication_weekdays") is not None
                else None
            ),
            publication_days_of_month=(
                tuple(int(value) for value in data["publication_days_of_month"])
                if data.get("publication_days_of_month") is not None
                else None
            ),
        )


@dataclass(frozen=True)
class IdentityContract:
    required_fields: tuple[str, ...]
    fixed_fields: Mapping[str, str]

    def __post_init__(self) -> None:
        required = _strings(self.required_fields, "identity.required_fields")
        fixed: dict[str, str] = {}
        for field_name, value in self.fixed_fields.items():
            normalized_name = _text(field_name, "identity.fixed_fields key")
            if normalized_name in fixed:
                raise ValueError("identity.fixed_fields cannot contain duplicates")
            fixed[normalized_name] = _text(value, f"identity.fixed_fields.{normalized_name}").lower()
        object.__setattr__(self, "required_fields", required)
        object.__setattr__(self, "fixed_fields", MappingProxyType(dict(sorted(fixed.items()))))

    def to_dict(self) -> dict[str, Any]:
        return {"required_fields": list(self.required_fields), "fixed_fields": dict(self.fixed_fields)}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> IdentityContract:
        return cls(
            required_fields=tuple(str(value) for value in data["required_fields"]),
            fixed_fields={str(key): str(value) for key, value in data["fixed_fields"].items()},
        )


@dataclass(frozen=True)
class FreshnessContract:
    stale_after_hours: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.stale_after_hours, int)
            or isinstance(self.stale_after_hours, bool)
            or self.stale_after_hours <= 0
        ):
            raise ValueError("freshness.stale_after_hours must be a positive integer")

    def to_dict(self) -> dict[str, int]:
        return {"stale_after_hours": self.stale_after_hours}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FreshnessContract:
        return cls(stale_after_hours=int(data["stale_after_hours"]))


@dataclass(frozen=True)
class CoverageContract:
    expected_keys: tuple[str, ...]
    key_fields: tuple[str, ...]
    minimum_coverage: Decimal | int | str

    def __post_init__(self) -> None:
        object.__setattr__(self, "expected_keys", _strings(self.expected_keys, "coverage.expected_keys"))
        object.__setattr__(self, "key_fields", _strings(self.key_fields, "coverage.key_fields"))
        if not self.expected_keys:
            raise ValueError("coverage.expected_keys cannot be empty")
        if not self.key_fields:
            raise ValueError("coverage.key_fields cannot be empty")
        minimum = _decimal(self.minimum_coverage, "coverage.minimum_coverage")
        if minimum < 0 or minimum > 1:
            raise ValueError("coverage.minimum_coverage must be between 0 and 1")
        object.__setattr__(self, "minimum_coverage", minimum)

    def to_dict(self) -> dict[str, Any]:
        return {
            "expected_keys": list(self.expected_keys),
            "key_fields": list(self.key_fields),
            "minimum_coverage": _decimal_text(self.minimum_coverage),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CoverageContract:
        return cls(
            expected_keys=tuple(str(value) for value in data["expected_keys"]),
            key_fields=tuple(str(value) for value in data["key_fields"]),
            minimum_coverage=str(data["minimum_coverage"]),
        )


@dataclass(frozen=True)
class RightsPolicy:
    decisions: Mapping[RightsAction, RightsDecision]
    required_attribution: str | None
    review_on: date | None
    evidence: tuple[str, ...]

    def __post_init__(self) -> None:
        normalized: dict[RightsAction, RightsDecision] = {}
        for raw_action, raw_decision in self.decisions.items():
            action = RightsAction(raw_action)
            if action in normalized:
                raise ValueError(f"rights decision for {action.value} is duplicated")
            normalized[action] = RightsDecision(raw_decision)
        object.__setattr__(self, "decisions", MappingProxyType(normalized))
        if self.required_attribution is not None:
            object.__setattr__(
                self,
                "required_attribution",
                _text(self.required_attribution, "rights.required_attribution"),
            )
        if self.review_on is not None and (
            not isinstance(self.review_on, date) or isinstance(self.review_on, datetime)
        ):
            raise ValueError("rights.review_on must be a date")
        object.__setattr__(self, "evidence", _strings(self.evidence, "rights.evidence"))

    def decision(self, action: RightsAction | str) -> RightsDecision:
        return self.decisions[RightsAction(action)]

    def allows(self, action: RightsAction | str) -> bool:
        return self.decisions.get(RightsAction(action)) is RightsDecision.ALLOWED

    @property
    def publication_eligible(self) -> bool:
        """Fail closed when any part of the recorded rights position is unknown."""
        return (
            set(self.decisions) == set(RightsAction)
            and RightsDecision.UNKNOWN not in self.decisions.values()
            and self.allows(RightsAction.PUBLIC_DISPLAY)
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "decisions": {action.value: self.decisions[action].value for action in sorted(self.decisions, key=str)},
            "required_attribution": self.required_attribution,
            "review_on": self.review_on.isoformat() if self.review_on else None,
            "evidence": list(self.evidence),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> RightsPolicy:
        review_on = data.get("review_on")
        return cls(
            decisions={
                RightsAction(str(action)): RightsDecision(str(decision))
                for action, decision in data["decisions"].items()
            },
            required_attribution=data.get("required_attribution"),
            review_on=date.fromisoformat(str(review_on)) if review_on else None,
            evidence=tuple(str(value) for value in data["evidence"]),
        )


@dataclass(frozen=True)
class DatasetContract:
    dataset: Dataset
    cadence: CadenceContract | None
    identity: IdentityContract | None
    coverage: CoverageContract | None
    freshness: FreshnessContract | None
    units: tuple[str, ...]
    criticality: Criticality | None
    validation: ValidationPolicy | None
    raw_retention: RawRetention | None
    rights: RightsPolicy | None
    schema_version: ClassVar[int] = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "units", _strings(self.units, "dataset_contract.units"))
        if self.criticality is not None:
            object.__setattr__(self, "criticality", Criticality(self.criticality))
        if self.raw_retention is not None:
            object.__setattr__(self, "raw_retention", RawRetention(self.raw_retention))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "record_type": "dataset-contract",
            "dataset": self.dataset.to_dict(),
            "cadence": self.cadence.to_dict() if self.cadence else None,
            "identity": self.identity.to_dict() if self.identity else None,
            "coverage": self.coverage.to_dict() if self.coverage else None,
            "freshness": self.freshness.to_dict() if self.freshness else None,
            "units": list(self.units),
            "criticality": self.criticality.value if self.criticality else None,
            "validation": self.validation.to_dict() if self.validation else None,
            "raw_retention": self.raw_retention.value if self.raw_retention else None,
            "rights": self.rights.to_dict() if self.rights else None,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DatasetContract:
        _record(data, "dataset-contract")
        cadence = data.get("cadence")
        identity = data.get("identity")
        coverage = data.get("coverage")
        freshness = data.get("freshness")
        validation = data.get("validation")
        rights = data.get("rights")
        return cls(
            dataset=Dataset.from_dict(data["dataset"]),
            cadence=CadenceContract.from_dict(cadence) if cadence else None,
            identity=IdentityContract.from_dict(identity) if identity else None,
            coverage=CoverageContract.from_dict(coverage) if coverage else None,
            freshness=FreshnessContract.from_dict(freshness) if freshness else None,
            units=tuple(str(value) for value in data["units"]),
            criticality=Criticality(str(data["criticality"])) if data.get("criticality") else None,
            validation=ValidationPolicy.from_dict(validation) if validation else None,
            raw_retention=RawRetention(str(data["raw_retention"])) if data.get("raw_retention") else None,
            rights=RightsPolicy.from_dict(rights) if rights else None,
        )


class RegistryValidationError(ValueError):
    """All deterministic validation failures found in a proposed registry."""

    def __init__(self, issues: Sequence[str]) -> None:
        self.issues = tuple(sorted(set(issues)))
        super().__init__("invalid contract registry: " + "; ".join(self.issues))


@dataclass(frozen=True, init=False)
class ContractRegistry:
    """An immutable, fully validated collection of source/dataset contracts."""

    _sources: tuple[Source, ...]
    _datasets: tuple[DatasetContract, ...]
    _source_by_id: Mapping[str, Source]
    _source_by_key: Mapping[str, Source]
    _dataset_by_id: Mapping[str, DatasetContract]
    schema_version: ClassVar[int] = SCHEMA_VERSION

    def __init__(self, sources: Sequence[Source], datasets: Sequence[DatasetContract]) -> None:
        source_values = tuple(sources)
        dataset_values = tuple(datasets)
        issues = self._validation_issues(source_values, dataset_values)
        if issues:
            raise RegistryValidationError(issues)
        object.__setattr__(self, "_sources", tuple(sorted(source_values, key=lambda source: source.source_id)))
        object.__setattr__(
            self,
            "_datasets",
            tuple(sorted(dataset_values, key=lambda contract: contract.dataset.dataset_id)),
        )
        object.__setattr__(
            self,
            "_source_by_id",
            MappingProxyType({source.source_id: source for source in self._sources}),
        )
        object.__setattr__(
            self,
            "_source_by_key",
            MappingProxyType({source.key: source for source in self._sources}),
        )
        object.__setattr__(
            self,
            "_dataset_by_id",
            MappingProxyType({contract.dataset.dataset_id: contract for contract in self._datasets}),
        )

    @property
    def sources(self) -> tuple[Source, ...]:
        return self._sources

    @property
    def datasets(self) -> tuple[DatasetContract, ...]:
        return self._datasets

    def source(self, source_id: str) -> Source:
        return self._source_by_id[source_id]

    def dataset(self, dataset_id: str) -> DatasetContract:
        return self._dataset_by_id[dataset_id]

    def dataset_by_key(self, source_key: str, dataset_key: str) -> DatasetContract:
        source = self._source_by_key[source_key]
        for contract in self._datasets:
            if contract.dataset.source_id == source.source_id and contract.dataset.key == dataset_key:
                return contract
        raise KeyError((source_key, dataset_key))

    def datasets_for_source(self, source_id: str) -> tuple[DatasetContract, ...]:
        self.source(source_id)
        return tuple(contract for contract in self._datasets if contract.dataset.source_id == source_id)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "record_type": "contract-registry",
            "sources": [source.to_dict() for source in self._sources],
            "datasets": [contract.to_dict() for contract in self._datasets],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ContractRegistry:
        _record(data, "contract-registry")
        return cls(
            sources=tuple(Source.from_dict(value) for value in data["sources"]),
            datasets=tuple(DatasetContract.from_dict(value) for value in data["datasets"]),
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ContractRegistry):
            return NotImplemented
        return self.sources == other.sources and self.datasets == other.datasets

    def __repr__(self) -> str:
        return f"ContractRegistry(sources={len(self.sources)}, datasets={len(self.datasets)})"

    @staticmethod
    def _validation_issues(sources: tuple[Source, ...], datasets: tuple[DatasetContract, ...]) -> tuple[str, ...]:
        issues: list[str] = []
        source_id_counts = Counter(source.source_id for source in sources)
        source_key_counts = Counter(source.key for source in sources)
        for source_id, count in source_id_counts.items():
            if count > 1:
                issues.append(f"duplicate source identifier {source_id}")
        for source_key, count in source_key_counts.items():
            if count > 1:
                issues.append(f"duplicate source key {source_key}")

        dataset_id_counts = Counter(contract.dataset.dataset_id for contract in datasets)
        for dataset_id, count in dataset_id_counts.items():
            if count > 1:
                issues.append(f"duplicate dataset identifier {dataset_id}")

        source_ids = set(source_id_counts)
        for contract in datasets:
            label = contract.dataset.key
            if contract.dataset.source_id not in source_ids:
                issues.append(f"dataset {label} references an unregistered source")
            missing_sections = (
                name
                for name in (
                    "cadence",
                    "identity",
                    "coverage",
                    "freshness",
                    "criticality",
                    "validation",
                    "raw_retention",
                    "rights",
                )
                if getattr(contract, name) is None
            )
            issues.extend(f"dataset {label} is missing {name}" for name in missing_sections)
            if not contract.units:
                issues.append(f"dataset {label} is missing units")

            if contract.identity is not None:
                required = set(contract.identity.required_fields)
                baseline = {"commodity", "product_form", "price_type", "currency", "unit", "effective_date"}
                absent = sorted(baseline - required)
                if absent:
                    issues.append(f"dataset {label} identity is missing required fields: {', '.join(absent)}")
                if not ({"venue", "location"} & required):
                    issues.append(f"dataset {label} identity requires venue or location")
                extra_fixed = sorted(set(contract.identity.fixed_fields) - required)
                if extra_fixed:
                    issues.append(f"dataset {label} fixed identity fields are not required: {', '.join(extra_fixed)}")
                fixed_unit = contract.identity.fixed_fields.get("unit")
                if fixed_unit is not None and fixed_unit not in contract.units:
                    issues.append(f"dataset {label} has a contradictory fixed unit and units contract")

            if contract.coverage is not None and contract.identity is not None:
                absent_key_fields = sorted(set(contract.coverage.key_fields) - set(contract.identity.required_fields))
                if absent_key_fields:
                    issues.append(f"dataset {label} coverage uses non-identity fields: {', '.join(absent_key_fields)}")

            if contract.validation is not None and not contract.validation.rule_ids:
                issues.append(f"dataset {label} is missing validation rules")

            rights = contract.rights
            if rights is not None:
                missing_actions = sorted(set(RightsAction) - set(rights.decisions), key=lambda action: action.value)
                issues.extend(
                    f"dataset {label} is missing rights decision {action.value}" for action in missing_actions
                )
                if rights.review_on is None:
                    issues.append(f"dataset {label} rights are missing a review date")
                if not rights.evidence:
                    issues.append(f"dataset {label} rights are missing evidence")
                raw_decision = rights.decisions.get(RightsAction.RAW_CONTENT_RETENTION)
                if contract.raw_retention is RawRetention.CONTENT and raw_decision is not RightsDecision.ALLOWED:
                    issues.append(f"dataset {label} has contradictory raw-content retention rights")
                if (
                    rights.decisions.get(RightsAction.PUBLIC_DISPLAY) is RightsDecision.ALLOWED
                    and rights.decisions.get(RightsAction.INTERNAL_DISPLAY) is RightsDecision.PROHIBITED
                ):
                    issues.append(f"dataset {label} has contradictory public/internal display rights")

            if (
                contract.cadence is not None
                and contract.freshness is not None
                and contract.freshness.stale_after_hours < contract.cadence.expected_interval_hours
            ):
                issues.append(f"dataset {label} has a contradictory freshness window and cadence")
        return tuple(issues)


_RIGHTS_REVIEW_ON = date(2026, 11, 10)


def _rights_policy(
    *,
    raw: RightsDecision,
    normalized: RightsDecision,
    internal: RightsDecision,
    public: RightsDecision,
    derived: RightsDecision,
    commercial: RightsDecision,
    redistribution: RightsDecision,
    attribution: str,
    evidence: str,
) -> RightsPolicy:
    return RightsPolicy(
        decisions={
            RightsAction.RAW_CONTENT_RETENTION: raw,
            RightsAction.NORMALIZED_HISTORY_RETENTION: normalized,
            RightsAction.INTERNAL_DISPLAY: internal,
            RightsAction.PUBLIC_DISPLAY: public,
            RightsAction.DERIVED_PUBLICATION: derived,
            RightsAction.COMMERCIAL_USE: commercial,
            RightsAction.REDISTRIBUTION: redistribution,
        },
        required_attribution=attribution,
        review_on=_RIGHTS_REVIEW_ON,
        evidence=(evidence,),
    )


_MAGYP_RIGHTS = _rights_policy(
    raw=RightsDecision.UNKNOWN,
    normalized=RightsDecision.UNKNOWN,
    internal=RightsDecision.ALLOWED,
    public=RightsDecision.UNKNOWN,
    derived=RightsDecision.UNKNOWN,
    commercial=RightsDecision.UNKNOWN,
    redistribution=RightsDecision.UNKNOWN,
    attribution="Argentina Secretaría de Agricultura (MAGyP/SAGyP)",
    evidence="Public MAGyP web service; formal reuse-rights review pending",
)

_YAHOO_MARKET_RIGHTS = _rights_policy(
    raw=RightsDecision.PROHIBITED,
    normalized=RightsDecision.UNKNOWN,
    internal=RightsDecision.ALLOWED,
    public=RightsDecision.UNKNOWN,
    derived=RightsDecision.UNKNOWN,
    commercial=RightsDecision.UNKNOWN,
    redistribution=RightsDecision.PROHIBITED,
    attribution="Yahoo Finance; underlying market venue as recorded",
    evidence="Existing delayed-data access; downstream publication rights review pending",
)

_AGRURAL_RIGHTS = _rights_policy(
    raw=RightsDecision.PROHIBITED,
    normalized=RightsDecision.UNKNOWN,
    internal=RightsDecision.ALLOWED,
    public=RightsDecision.UNKNOWN,
    derived=RightsDecision.UNKNOWN,
    commercial=RightsDecision.UNKNOWN,
    redistribution=RightsDecision.PROHIBITED,
    attribution="AgRural",
    evidence="Public AgRural price page; downstream publication rights review pending",
)

MAGYP_SOURCE = Source(
    key="magyp",
    name="Argentina MAGyP official FOB service",
    attribution="Argentina Secretaría de Agricultura (MAGyP/SAGyP)",
)
YAHOO_FINANCE_SOURCE = Source(
    key="yahoo-finance",
    name="Yahoo Finance market data",
    attribution="Yahoo Finance",
)
AGRURAL_SOURCE = Source(
    key="agrural",
    name="AgRural Brazil physical soybean prices",
    attribution="AgRural",
)

_BUSINESS_DAILY_ARGENTINA = CadenceContract(CadenceKind.BUSINESS_DAILY, 24, "America/Argentina/Buenos_Aires")
_BUSINESS_DAILY_CHICAGO = CadenceContract(CadenceKind.BUSINESS_DAILY, 24, "America/Chicago")
_BUSINESS_DAILY_UTC = CadenceContract(CadenceKind.BUSINESS_DAILY, 24, "UTC")
_DAILY_FRESHNESS = FreshnessContract(stale_after_hours=96)


def _pilot_contract(
    *,
    source: Source,
    key: str,
    name: str,
    cadence: CadenceContract,
    required_fields: tuple[str, ...],
    fixed_fields: Mapping[str, str],
    expected_keys: tuple[str, ...],
    coverage_key_fields: tuple[str, ...],
    unit: str,
    criticality: Criticality,
    rules: tuple[str, ...],
    raw_retention: RawRetention,
    rights: RightsPolicy,
) -> DatasetContract:
    return DatasetContract(
        dataset=Dataset(source.source_id, key, name),
        cadence=cadence,
        identity=IdentityContract(required_fields, fixed_fields),
        coverage=CoverageContract(
            expected_keys=expected_keys,
            key_fields=coverage_key_fields,
            minimum_coverage=Decimal("1"),
        ),
        freshness=_DAILY_FRESHNESS,
        units=(unit,),
        criticality=criticality,
        validation=ValidationPolicy(rules),
        raw_retention=raw_retention,
        rights=rights,
    )


MAGYP_FOB_CONTRACT = _pilot_contract(
    source=MAGYP_SOURCE,
    key="official-soy-fob",
    # Dataset *key* is deliberately unchanged though the contents widened to
    # sunflower (#162): the key is this dataset's stored identity, and
    # renaming it would orphan every observation already recorded under it.
    name="Official soy-complex and sunflower oil FOB prices",
    cadence=_BUSINESS_DAILY_ARGENTINA,
    required_fields=(
        "commodity",
        "product_form",
        "location",
        "price_type",
        "currency",
        "unit",
        "delivery_window",
        "effective_date",
        "source_record_id",
    ),
    fixed_fields={
        "location": "argentina-up-river",
        "price_type": "official-fob",
        "currency": "usd",
        "unit": "usd-mt",
    },
    # Full coverage is demanded of sunflower too: crude sunflower oil printed
    # on all 66 circulars published 2026-05-01 -> 2026-08-11, so a missing
    # sunflower key is an outage, not an off-day (#162).
    expected_keys=("soybean:beans", "soybean:meal", "soybean:oil", "sunflower:oil"),
    coverage_key_fields=("commodity", "product_form"),
    unit="usd-mt",
    criticality=Criticality.SUPPORTING,
    rules=("identity.required", "value.positive", "delivery-window.valid", "coverage.soy-complex"),
    raw_retention=RawRetention.METADATA_ONLY,
    rights=_MAGYP_RIGHTS,
)

AGRURAL_PARANAGUA_CONTRACT = _pilot_contract(
    source=AGRURAL_SOURCE,
    key="paranagua-soybean-fob",
    name="Paranagua soybean FOB physical price",
    cadence=_BUSINESS_DAILY_UTC,
    required_fields=(
        "commodity",
        "product_form",
        "location",
        "price_type",
        "currency",
        "unit",
        "effective_date",
    ),
    fixed_fields={
        "commodity": "soybean",
        "product_form": "beans",
        "location": "paranagua",
        "price_type": "physical-fob",
        "currency": "brl",
        "unit": "brl-mt",
    },
    expected_keys=("soybean:beans:paranagua",),
    coverage_key_fields=("commodity", "product_form", "location"),
    unit="brl-mt",
    criticality=Criticality.CRITICAL,
    rules=("identity.required", "value.positive", "coverage.paranagua-fob"),
    raw_retention=RawRetention.METADATA_ONLY,
    rights=_AGRURAL_RIGHTS,
)

_BENCHMARK_SPECS = (
    ("soybean", "beans", "cents-bu"),
    ("soybean-oil", "oil", "cents-lb"),
    ("soybean-meal", "meal", "usd-short-ton"),
)

SOY_BENCHMARK_CONTRACTS = tuple(
    _pilot_contract(
        source=YAHOO_FINANCE_SOURCE,
        key=f"cbot-{commodity}-named-contracts",
        name=f"CBOT {commodity.replace('-', ' ')} named-contract settlements",
        cadence=_BUSINESS_DAILY_CHICAGO,
        required_fields=(
            "commodity",
            "product_form",
            "venue",
            "price_type",
            "currency",
            "unit",
            "contract",
            "effective_date",
        ),
        fixed_fields={
            "commodity": commodity,
            "product_form": product_form,
            "venue": "cbot",
            "price_type": "settlement",
            "currency": "usd",
            "unit": unit,
        },
        expected_keys=(f"{commodity}:{product_form}",),
        coverage_key_fields=("commodity", "product_form"),
        unit=unit,
        criticality=Criticality.CRITICAL,
        rules=(
            "identity.required",
            "value.finite",
            "ohlc.relationship",
            "price.daily-move",
            "contract.named",
            "settlement.confirmed",
        ),
        raw_retention=RawRetention.METADATA_ONLY,
        rights=_YAHOO_MARKET_RIGHTS,
    )
    for commodity, product_form, unit in _BENCHMARK_SPECS
)

# FX inputs used by current published native-price conversions: Brazil
# physical/FOB, DCE, India mandi, and SAFEX respectively.  Context-only
# currency-board pairs are not part of the critical conversion contract.
REQUIRED_FX_PAIRS = ("BRL/USD", "CNY/USD", "INR/USD", "ZAR/USD")

FX_CONTRACTS = tuple(
    _pilot_contract(
        source=YAHOO_FINANCE_SOURCE,
        key=f"fx-{pair.lower().replace('/', '-')}",
        name=f"{pair} daily exchange rate",
        cadence=_BUSINESS_DAILY_UTC,
        required_fields=(
            "commodity",
            "product_form",
            "venue",
            "price_type",
            "currency",
            "unit",
            "fx_pair",
            "effective_date",
        ),
        fixed_fields={
            "commodity": "foreign-exchange",
            "product_form": pair.lower().replace("/", "-"),
            "venue": "yahoo-finance",
            "price_type": "market-close",
            "currency": "usd",
            "unit": f"usd-per-{pair.split('/')[0].lower()}",
        },
        expected_keys=(f"foreign-exchange:{pair.lower().replace('/', '-')}",),
        coverage_key_fields=("commodity", "product_form"),
        unit=f"usd-per-{pair.split('/')[0].lower()}",
        criticality=Criticality.CRITICAL,
        rules=("identity.required", "value.positive", "ohlc.relationship", "fx.daily-move", "fx.orientation"),
        raw_retention=RawRetention.METADATA_ONLY,
        rights=_YAHOO_MARKET_RIGHTS,
    )
    for pair in REQUIRED_FX_PAIRS
)

PILOT_REGISTRY = ContractRegistry(
    sources=(AGRURAL_SOURCE, MAGYP_SOURCE, YAHOO_FINANCE_SOURCE),
    datasets=(AGRURAL_PARANAGUA_CONTRACT, MAGYP_FOB_CONTRACT, *SOY_BENCHMARK_CONTRACTS, *FX_CONTRACTS),
)


__all__ = [
    "CadenceContract",
    "CadenceKind",
    "ContractRegistry",
    "CoverageContract",
    "Criticality",
    "AGRURAL_PARANAGUA_CONTRACT",
    "AGRURAL_SOURCE",
    "DatasetContract",
    "FX_CONTRACTS",
    "FreshnessContract",
    "IdentityContract",
    "MAGYP_FOB_CONTRACT",
    "MAGYP_SOURCE",
    "PILOT_REGISTRY",
    "REQUIRED_FX_PAIRS",
    "RawRetention",
    "RegistryValidationError",
    "RightsAction",
    "RightsDecision",
    "RightsPolicy",
    "SOY_BENCHMARK_CONTRACTS",
    "ValidationPolicy",
    "YAHOO_FINANCE_SOURCE",
]
