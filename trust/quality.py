"""Deterministic quality-rule execution for trusted observations."""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any

from trust.domain import (
    CandidateObservation,
    ContractIdentity,
    Finding,
    FindingSeverity,
    ObservationIdentity,
    ObservationRevision,
    QualityState,
    SettlementState,
    Timestamp,
)
from trust.registry import DatasetContract

_KEY_RE = re.compile(r"[a-z0-9][a-z0-9._-]*")


def _key(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    normalized = value.strip().lower()
    if not _KEY_RE.fullmatch(normalized):
        raise ValueError(f"{field_name} must contain only letters, numbers, '.', '_' or '-'")
    return normalized


def _text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


@dataclass(frozen=True)
class QualityRuleContext:
    """One subject plus dataset context presented to a quality rule."""

    run_id: str
    dataset_id: str
    subject_id: str
    subject: Any = None
    dataset_context: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "dataset_context", MappingProxyType(dict(self.dataset_context)))


@dataclass(frozen=True)
class RuleFinding:
    """A rule-local finding before run and dataset metadata are stamped on it."""

    evidence: Mapping[str, Any]
    message: str
    subject_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "message", _text(self.message, "rule_finding.message"))
        if self.subject_id is not None:
            object.__setattr__(self, "subject_id", _text(self.subject_id, "rule_finding.subject_id"))


RuleEvaluator = Callable[[QualityRuleContext], Iterable[RuleFinding]]


@dataclass(frozen=True)
class QualityRule:
    """A versioned deterministic rule registered with the engine."""

    rule_id: str
    version: str
    scope: str
    severity: FindingSeverity
    evaluate: RuleEvaluator

    def __post_init__(self) -> None:
        object.__setattr__(self, "rule_id", _key(self.rule_id, "quality_rule.rule_id"))
        object.__setattr__(self, "version", _text(self.version, "quality_rule.version"))
        object.__setattr__(self, "scope", _key(self.scope, "quality_rule.scope"))
        object.__setattr__(
            self,
            "severity",
            self.severity if isinstance(self.severity, FindingSeverity) else FindingSeverity(self.severity),
        )
        if not callable(self.evaluate):
            raise ValueError("quality_rule.evaluate must be callable")


@dataclass(frozen=True)
class QualityEvaluation:
    """Pure rule-engine output; callers decide whether and where to persist it."""

    findings: tuple[Finding, ...]
    disposition: QualityState

    @property
    def finding_ids(self) -> tuple[str, ...]:
        return tuple(finding.finding_id for finding in self.findings)


@dataclass(frozen=True)
class NumericValidationPolicy:
    """Dataset-independent numeric bounds for generic candidate validation."""

    allow_zero: bool = False
    allow_negative: bool = False
    minimum: Decimal | int | str | None = None
    maximum: Decimal | int | str | None = None

    def __post_init__(self) -> None:
        for field_name in ("minimum", "maximum"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _decimal(value, f"numeric_policy.{field_name}"))
        if self.minimum is not None and self.maximum is not None and self.maximum < self.minimum:
            raise ValueError("numeric_policy.maximum cannot be below minimum")


class QualityRuleEngine:
    """Execute quality rules without persistence, IO, or registration-order effects."""

    def __init__(self, rules: Iterable[QualityRule]) -> None:
        normalized = tuple(rules)
        identities = [(rule.rule_id, rule.version, rule.scope) for rule in normalized]
        if len(identities) != len(set(identities)):
            raise ValueError("quality rules cannot contain duplicate rule_id/version/scope registrations")
        self._rules = tuple(sorted(normalized, key=lambda rule: (rule.rule_id, rule.version, rule.scope)))

    @property
    def rules(self) -> tuple[QualityRule, ...]:
        return self._rules

    def evaluate(
        self,
        *,
        run_id: str,
        dataset_id: str,
        subject_id: str,
        subject: Any = None,
        dataset_context: Mapping[str, Any] | None = None,
    ) -> QualityEvaluation:
        context = QualityRuleContext(
            run_id=run_id,
            dataset_id=dataset_id,
            subject_id=subject_id,
            subject=subject,
            dataset_context=dataset_context or {},
        )
        findings_by_id: dict[str, Finding] = {}
        for rule in self._rules:
            for rule_finding in rule.evaluate(context):
                finding = Finding(
                    run_id=run_id,
                    dataset_id=dataset_id,
                    subject_id=rule_finding.subject_id or subject_id,
                    rule_id=rule.rule_id,
                    rule_version=rule.version,
                    severity=rule.severity,
                    evidence=rule_finding.evidence,
                    message=rule_finding.message,
                )
                existing = findings_by_id.get(finding.finding_id)
                if existing is None or finding.message < existing.message:
                    findings_by_id[finding.finding_id] = finding

        findings = tuple(sorted(findings_by_id.values(), key=lambda finding: finding.finding_id))
        return QualityEvaluation(findings=findings, disposition=_disposition(findings))


def generic_candidate_quality_rules(
    contract: DatasetContract,
    *,
    numeric_policy: NumericValidationPolicy | None = None,
    version: str = "1.0.0",
) -> tuple[QualityRule, ...]:
    """Return common DT-12 rules for one dataset contract.

    The rules accept either a ``CandidateObservation`` or a parsed mapping with
    ``identity`` and value/timestamp keys. Supporting mappings lets ingestion
    produce findings for malformed parser output that cannot become a domain
    candidate yet.
    """

    if contract.identity is None:
        raise ValueError("generic candidate validators require a dataset identity contract")
    policy = numeric_policy or NumericValidationPolicy()
    rules = [
        QualityRule("identity.required", version, "candidate", FindingSeverity.REJECT, _identity_required(contract)),
        QualityRule("identity.fixed-fields", version, "candidate", FindingSeverity.REJECT, _identity_fixed(contract)),
        QualityRule("identity.extra-fields", version, "candidate", FindingSeverity.REJECT, _identity_extra_fields),
        QualityRule("unit.recognized", version, "candidate", FindingSeverity.REJECT, _unit_recognized(contract)),
        QualityRule(
            "currency.recognized",
            version,
            "candidate",
            FindingSeverity.REJECT,
            _currency_recognized(contract),
        ),
        QualityRule("temporal.labels", version, "candidate", FindingSeverity.REJECT, _temporal_labels),
        QualityRule("value.finite", version, "candidate", FindingSeverity.REJECT, _value_finite),
        QualityRule("value.sign", version, "candidate", FindingSeverity.REJECT, _value_sign(policy)),
        QualityRule("value.range", version, "candidate", FindingSeverity.QUARANTINE, _value_range(policy)),
    ]
    requested_rules = set(contract.validation.rule_ids if contract.validation is not None else ())
    if "contract.named" in requested_rules:
        rules.append(QualityRule("contract.named", version, "candidate", FindingSeverity.REJECT, _contract_named))
    if "settlement.confirmed" in requested_rules:
        rules.append(
            QualityRule("settlement.confirmed", version, "candidate", FindingSeverity.REJECT, _settlement_confirmed)
        )
    if "ohlc.relationship" in requested_rules:
        rules.append(QualityRule("ohlc.relationship", version, "candidate", FindingSeverity.REJECT, _ohlc_relationship))
    if "price.daily-move" in requested_rules:
        rules.append(
            QualityRule("price.daily-move", version, "candidate", FindingSeverity.QUARANTINE, _price_daily_move)
        )
    return tuple(rules)


def _disposition(findings: tuple[Finding, ...]) -> QualityState:
    severities = {finding.severity for finding in findings}
    if FindingSeverity.REJECT in severities:
        return QualityState.REJECTED
    if FindingSeverity.QUARANTINE in severities:
        return QualityState.QUARANTINED
    return QualityState.ACCEPTED


_IDENTITY_FIELDS = {
    "source_id",
    "dataset_id",
    "dataset_key",
    "commodity",
    "product_form",
    "price_type",
    "currency",
    "unit",
    "effective_date",
    "venue",
    "location",
    "contract",
    "delivery_window",
    "source_record_id",
}


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


def _subject_mapping(subject: Any) -> Mapping[str, Any]:
    return subject if isinstance(subject, Mapping) else {}


def _identity_mapping(subject: Any) -> Mapping[str, Any]:
    if isinstance(subject, CandidateObservation):
        return _identity_from_object(subject.identity)
    if isinstance(subject, ObservationIdentity):
        return _identity_from_object(subject)
    subject_map = _subject_mapping(subject)
    raw_identity = subject_map.get("identity", subject_map)
    return raw_identity if isinstance(raw_identity, Mapping) else {}


def _identity_from_object(identity: ObservationIdentity) -> Mapping[str, Any]:
    return {
        "source_id": identity.source_id,
        "dataset_id": identity.dataset_id,
        "dataset_key": identity.dataset_key,
        "commodity": identity.commodity,
        "product_form": identity.product_form,
        "price_type": identity.price_type,
        "currency": identity.currency,
        "unit": identity.unit,
        "effective_date": identity.effective_date,
        "venue": identity.venue,
        "location": identity.location,
        "contract": identity.contract,
        "delivery_window": identity.delivery_window,
        "source_record_id": identity.source_record_id,
    }


def _value_for(subject: Any, field_name: str) -> Any:
    if isinstance(subject, CandidateObservation):
        return getattr(subject, field_name, None)
    if isinstance(subject, ObservationRevision):
        return getattr(subject, field_name, None)
    return _subject_mapping(subject).get(field_name)


def _normalized_text_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return str(value).strip().lower()


def _identity_required(contract: DatasetContract) -> RuleEvaluator:
    assert contract.identity is not None

    def evaluate(context: QualityRuleContext) -> tuple[RuleFinding, ...]:
        identity = _identity_mapping(context.subject)
        findings = []
        for field_name in contract.identity.required_fields:
            if _normalized_text_value(identity.get(field_name)) is None:
                findings.append(
                    RuleFinding(
                        evidence={"field": field_name},
                        message=f"Required identity field is missing: {field_name}",
                    )
                )
        return tuple(findings)

    return evaluate


def _identity_fixed(contract: DatasetContract) -> RuleEvaluator:
    assert contract.identity is not None

    def evaluate(context: QualityRuleContext) -> tuple[RuleFinding, ...]:
        identity = _identity_mapping(context.subject)
        findings = []
        for field_name, expected in contract.identity.fixed_fields.items():
            observed = _normalized_text_value(identity.get(field_name))
            if observed is not None and observed != expected:
                findings.append(
                    RuleFinding(
                        evidence={"field": field_name, "expected": expected, "observed": observed},
                        message=f"Identity field {field_name} does not match the dataset contract",
                    )
                )
        return tuple(findings)

    return evaluate


def _identity_extra_fields(context: QualityRuleContext) -> tuple[RuleFinding, ...]:
    if isinstance(context.subject, (CandidateObservation, ObservationIdentity)):
        return ()
    identity = _identity_mapping(context.subject)
    extra_keys = sorted(str(key) for key in identity if str(key) not in _IDENTITY_FIELDS)
    subject_extra = _subject_mapping(context.subject).get("identity_extra_fields", {})
    if isinstance(subject_extra, Mapping):
        extra_keys.extend(f"identity_extra_fields.{key}" for key in sorted(str(key) for key in subject_extra))
    elif subject_extra:
        extra_keys.append("identity_extra_fields")
    return tuple(
        RuleFinding(
            evidence={"field": field_name},
            message=f"Unknown identity field is not part of the canonical observation identity: {field_name}",
        )
        for field_name in sorted(set(extra_keys))
    )


def _unit_recognized(contract: DatasetContract) -> RuleEvaluator:
    units = set(contract.units)

    def evaluate(context: QualityRuleContext) -> tuple[RuleFinding, ...]:
        observed = _normalized_text_value(_identity_mapping(context.subject).get("unit"))
        if observed is not None and observed not in units:
            return (
                RuleFinding(
                    evidence={"expected": sorted(units), "observed": observed},
                    message="Unit is not recognized by the dataset contract",
                ),
            )
        return ()

    return evaluate


def _currency_recognized(contract: DatasetContract) -> RuleEvaluator:
    expected = None if contract.identity is None else contract.identity.fixed_fields.get("currency")

    def evaluate(context: QualityRuleContext) -> tuple[RuleFinding, ...]:
        observed = _normalized_text_value(_identity_mapping(context.subject).get("currency"))
        if observed is not None and len(observed) != 3:
            return (RuleFinding(evidence={"observed": observed}, message="Currency must be a three-letter code"),)
        if expected is not None and observed is not None and observed != expected:
            return (
                RuleFinding(
                    evidence={"expected": expected, "observed": observed},
                    message="Currency is not recognized by the dataset contract",
                ),
            )
        return ()

    return evaluate


def _temporal_labels(context: QualityRuleContext) -> tuple[RuleFinding, ...]:
    findings = []
    identity = _identity_mapping(context.subject)
    effective_date = identity.get("effective_date")
    if effective_date is not None and (
        not isinstance(effective_date, date) or isinstance(effective_date, datetime)
    ):
        findings.append(RuleFinding(evidence={"field": "effective_date"}, message="Effective date must be a date"))
    for field_name in ("source_published_at", "observed_at"):
        timestamp = _value_for(context.subject, field_name)
        if timestamp is not None and not _is_labeled_timestamp(timestamp):
            findings.append(
                RuleFinding(
                    evidence={"field": field_name},
                    message=f"{field_name} must carry a timezone-aware value and inferred label",
                )
            )
    for field_name in ("parsed_at", "ingested_at"):
        timestamp = _value_for(context.subject, field_name)
        if timestamp is not None and not _is_aware_datetime(timestamp):
            findings.append(
                RuleFinding(
                    evidence={"field": field_name},
                    message=f"{field_name} must be timezone-aware",
                )
            )
    inferred = _value_for(context.subject, "effective_date_inferred")
    if inferred is not None and not isinstance(inferred, bool):
        findings.append(
            RuleFinding(
                evidence={"field": "effective_date_inferred"},
                message="effective_date_inferred must be explicitly true or false",
            )
        )
    return tuple(findings)


def _is_labeled_timestamp(value: Any) -> bool:
    if isinstance(value, Timestamp):
        return True
    if not isinstance(value, Mapping) or set(value) != {"value", "inferred"}:
        return False
    return isinstance(value["inferred"], bool) and _is_aware_datetime(value["value"])


def _is_aware_datetime(value: Any) -> bool:
    if not isinstance(value, datetime):
        return False
    return value.tzinfo is not None and value.utcoffset() is not None


def _candidate_decimal(context: QualityRuleContext, field_name: str = "value") -> Decimal | None:
    raw_value = _value_for(context.subject, field_name)
    if raw_value is None:
        return None
    if isinstance(raw_value, (bool, float)):
        return None
    try:
        result = raw_value if isinstance(raw_value, Decimal) else Decimal(raw_value)
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _value_finite(context: QualityRuleContext) -> tuple[RuleFinding, ...]:
    raw_value = _value_for(context.subject, "value")
    if raw_value is None:
        return ()
    invalid_float = isinstance(raw_value, float) and not math.isfinite(raw_value)
    invalid_decimal = _candidate_decimal(context) is None
    if invalid_float or invalid_decimal:
        return (
            RuleFinding(
                evidence={"field": "value", "observed": str(raw_value)},
                message="Value must be finite decimal data",
            ),
        )
    return ()


def _value_sign(policy: NumericValidationPolicy) -> RuleEvaluator:
    def evaluate(context: QualityRuleContext) -> tuple[RuleFinding, ...]:
        value = _candidate_decimal(context)
        if value is None:
            return ()
        if value < 0 and not policy.allow_negative:
            return (RuleFinding(evidence={"observed": str(value)}, message="Value cannot be negative"),)
        if value == 0 and not policy.allow_zero:
            return (RuleFinding(evidence={"observed": "0"}, message="Value cannot be zero"),)
        return ()

    return evaluate


def _value_range(policy: NumericValidationPolicy) -> RuleEvaluator:
    def evaluate(context: QualityRuleContext) -> tuple[RuleFinding, ...]:
        value = _candidate_decimal(context)
        if value is None:
            return ()
        if policy.minimum is not None and value < policy.minimum:
            return (
                RuleFinding(
                    evidence={"minimum": policy.minimum, "observed": value},
                    message="Value is below the configured range",
                ),
            )
        if policy.maximum is not None and value > policy.maximum:
            return (
                RuleFinding(
                    evidence={"maximum": policy.maximum, "observed": value},
                    message="Value is above the configured range",
                ),
            )
        return ()

    return evaluate


def _contract_named(context: QualityRuleContext) -> tuple[RuleFinding, ...]:
    contract = _identity_mapping(context.subject).get("contract")
    if isinstance(contract, ContractIdentity):
        return ()
    if isinstance(contract, Mapping):
        missing = sorted({"exchange", "code", "delivery_month"} - set(str(key) for key in contract))
        if missing:
            return (
                RuleFinding(
                    evidence={"field": "contract", "missing": missing},
                    message="Named contract must include exchange, code, and delivery month",
                ),
            )
        return ()
    return (
        RuleFinding(
            evidence={"field": "contract", "observed": str(contract) if contract is not None else None},
            message="Named contract identity is required",
        ),
    )


def _settlement_confirmed(context: QualityRuleContext) -> tuple[RuleFinding, ...]:
    state = _settlement_state(context.subject)
    if state is SettlementState.SETTLED:
        return ()
    return (
        RuleFinding(
            evidence={"settlement_state": state.value if state is not None else None},
            message="Settlement observations must be final settled bars",
        ),
    )


def _settlement_state(subject: Any) -> SettlementState | None:
    raw = _value_for(subject, "settlement_state")
    if isinstance(raw, SettlementState):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            return SettlementState(raw.strip().lower())
        except ValueError:
            return None
    return None


def _ohlc_relationship(context: QualityRuleContext) -> tuple[RuleFinding, ...]:
    values = {field_name: _candidate_decimal(context, field_name) for field_name in _OHLC_FIELDS}
    present = {field_name: value for field_name, value in values.items() if value is not None}
    if not present:
        return ()
    if len(present) != len(_OHLC_FIELDS):
        return (
            RuleFinding(
                evidence={"missing": sorted(set(_OHLC_FIELDS) - set(present))},
                message="OHLC observations must provide a complete candle",
            ),
        )
    open_value = present["open_value"]
    high_value = present["high_value"]
    low_value = present["low_value"]
    close_value = present["close_value"]
    assert open_value is not None and high_value is not None and low_value is not None and close_value is not None
    if high_value < max(open_value, low_value, close_value) or low_value > min(open_value, high_value, close_value):
        return (
            RuleFinding(
                evidence={field_name: str(value) for field_name, value in present.items()},
                message="OHLC candle violates high/low relationships",
            ),
        )
    return ()


_OHLC_FIELDS = ("open_value", "high_value", "low_value", "close_value")


def _price_daily_move(context: QualityRuleContext) -> tuple[RuleFinding, ...]:
    value = _candidate_decimal(context)
    previous = _previous_value(context)
    if value is None or previous is None or previous <= 0:
        return ()
    threshold = _move_threshold(context)
    move = abs((value - previous) / previous)
    if move > threshold:
        return (
            RuleFinding(
                evidence={
                    "previous_value": str(previous),
                    "observed": str(value),
                    "absolute_move_ratio": str(move),
                    "threshold": str(threshold),
                },
                message="Price move exceeds the configured quarantine threshold",
            ),
        )
    return ()


def _previous_value(context: QualityRuleContext) -> Decimal | None:
    subject_value = _value_for(context.subject, "previous_value")
    if subject_value is not None:
        return _decimal_or_none(subject_value)
    previous_by_subject = context.dataset_context.get("previous_value_by_subject_id")
    if isinstance(previous_by_subject, Mapping):
        return _decimal_or_none(previous_by_subject.get(context.subject_id))
    previous_by_observation = context.dataset_context.get("previous_value_by_observation_id")
    observation_id = _identity_mapping(context.subject).get("observation_id")
    if isinstance(previous_by_observation, Mapping) and observation_id is not None:
        return _decimal_or_none(previous_by_observation.get(str(observation_id)))
    return _decimal_or_none(context.dataset_context.get("previous_value"))


def _move_threshold(context: QualityRuleContext) -> Decimal:
    configured = context.dataset_context.get("daily_move_quarantine_threshold")
    threshold = _decimal_or_none(configured) if configured is not None else None
    return threshold if threshold is not None and threshold > 0 else Decimal("0.25")


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or isinstance(value, (bool, float)):
        return None
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None
