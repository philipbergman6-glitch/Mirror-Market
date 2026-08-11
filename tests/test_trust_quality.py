"""DT-11 contract tests for deterministic quality-rule execution."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from trust import (
    Dataset,
    FindingSeverity,
    NumericValidationPolicy,
    QualityRule,
    QualityRuleContext,
    QualityRuleEngine,
    QualityState,
    RuleFinding,
    Run,
    RunStatus,
    Source,
    generic_candidate_quality_rules,
)
from trust.registry import PILOT_REGISTRY

NOW = datetime(2026, 8, 10, 12, 30, tzinfo=timezone.utc)


def _ids() -> tuple[str, str, str]:
    source = Source(key="magyp", name="Argentina MAGyP")
    dataset = Dataset(source_id=source.source_id, key="official-fob", name="Official FOB prices")
    run = Run(
        code_revision="82e3cbc578023efdafb87ed1e09c5ea8b357eca5",
        started_at=NOW,
        ended_at=NOW,
        status=RunStatus.SUCCEEDED,
    )
    return run.run_id, dataset.dataset_id, "row-1"


def _rule(
    rule_id: str,
    severity: FindingSeverity,
    findings: Iterable[RuleFinding],
    *,
    version: str = "1.0.0",
    scope: str = "candidate",
) -> QualityRule:
    return QualityRule(
        rule_id=rule_id,
        version=version,
        scope=scope,
        severity=severity,
        evaluate=lambda context: tuple(findings),
    )


def test_good_candidate_has_accepted_disposition_without_findings() -> None:
    run_id, dataset_id, subject_id = _ids()

    result = QualityRuleEngine(
        (
            _rule("price.positive", FindingSeverity.REJECT, ()),
            _rule("price.outlier", FindingSeverity.WARNING, ()),
        )
    ).evaluate(run_id=run_id, dataset_id=dataset_id, subject_id=subject_id)

    assert result.disposition is QualityState.ACCEPTED
    assert result.findings == ()
    assert result.finding_ids == ()


def test_warning_candidate_remains_accepted_with_warning_finding() -> None:
    run_id, dataset_id, subject_id = _ids()

    result = QualityRuleEngine(
        (
            _rule(
                "price.near-limit",
                FindingSeverity.WARNING,
                (
                    RuleFinding(
                        evidence={"limit": Decimal("500"), "observed": Decimal("499.5")},
                        message="Price is near the configured upper limit",
                    ),
                ),
            ),
        )
    ).evaluate(run_id=run_id, dataset_id=dataset_id, subject_id=subject_id)

    assert result.disposition is QualityState.ACCEPTED
    assert len(result.findings) == 1
    assert result.findings[0].severity is FindingSeverity.WARNING
    assert result.findings[0].rule_id == "price.near-limit"
    assert result.findings[0].rule_version == "1.0.0"
    assert result.findings[0].subject_id == subject_id


def test_quarantine_and_reject_examples_set_hard_dispositions() -> None:
    run_id, dataset_id, subject_id = _ids()

    quarantine = QualityRuleEngine(
        (
            _rule(
                "price.large-move",
                FindingSeverity.QUARANTINE,
                (RuleFinding(evidence={"previous": "450", "observed": "499.5"}, message="Large move requires review"),),
            ),
        )
    ).evaluate(run_id=run_id, dataset_id=dataset_id, subject_id=subject_id)
    reject = QualityRuleEngine(
        (
            _rule(
                "price.non-positive",
                FindingSeverity.REJECT,
                (RuleFinding(evidence={"observed": "0"}, message="Price must be positive"),),
            ),
        )
    ).evaluate(run_id=run_id, dataset_id=dataset_id, subject_id=subject_id)

    assert quarantine.disposition is QualityState.QUARANTINED
    assert reject.disposition is QualityState.REJECTED


def test_reject_outranks_quarantine_and_warning_regardless_of_finding_order() -> None:
    run_id, dataset_id, subject_id = _ids()
    warning = _rule(
        "price.near-limit",
        FindingSeverity.WARNING,
        (RuleFinding(evidence={"observed": "499.5"}, message="Near limit"),),
    )
    quarantine = _rule(
        "price.large-move",
        FindingSeverity.QUARANTINE,
        (RuleFinding(evidence={"observed": "499.5"}, message="Large move"),),
    )
    reject = _rule(
        "price.non-positive",
        FindingSeverity.REJECT,
        (RuleFinding(evidence={"observed": "-1"}, message="Price must be positive"),),
    )

    forward = QualityRuleEngine((warning, quarantine, reject)).evaluate(
        run_id=run_id,
        dataset_id=dataset_id,
        subject_id=subject_id,
    )
    reverse = QualityRuleEngine((reject, quarantine, warning)).evaluate(
        run_id=run_id,
        dataset_id=dataset_id,
        subject_id=subject_id,
    )

    assert forward.disposition is QualityState.REJECTED
    assert reverse.disposition is QualityState.REJECTED
    assert forward.finding_ids == reverse.finding_ids
    assert [finding.to_dict() for finding in forward.findings] == [finding.to_dict() for finding in reverse.findings]


def test_quarantine_outranks_accepted_with_warning() -> None:
    run_id, dataset_id, subject_id = _ids()

    result = QualityRuleEngine(
        (
            _rule(
                "price.near-limit",
                FindingSeverity.WARNING,
                (RuleFinding(evidence={"observed": "499.5"}, message="Near limit"),),
            ),
            _rule(
                "price.large-move",
                FindingSeverity.QUARANTINE,
                (RuleFinding(evidence={"observed": "499.5"}, message="Large move"),),
            ),
        )
    ).evaluate(run_id=run_id, dataset_id=dataset_id, subject_id=subject_id)

    assert result.disposition is QualityState.QUARANTINED


def test_duplicate_findings_from_identical_rule_and_evidence_collapse_deterministically() -> None:
    run_id, dataset_id, subject_id = _ids()
    duplicate = RuleFinding(evidence={"observed": Decimal("499.50")}, message="Near limit")
    same_identity_different_message = replace(duplicate, message="A lexically earlier duplicate")

    result = QualityRuleEngine(
        (
            _rule(
                "price.near-limit",
                FindingSeverity.WARNING,
                (duplicate, same_identity_different_message, duplicate),
            ),
        )
    ).evaluate(run_id=run_id, dataset_id=dataset_id, subject_id=subject_id)

    assert len(result.findings) == 1
    assert result.findings[0].message == "A lexically earlier duplicate"
    assert result.findings[0].evidence["observed"] == Decimal("499.5")


def test_context_exposes_subject_and_dataset_context_to_rules() -> None:
    run_id, dataset_id, subject_id = _ids()

    def evaluate(context: QualityRuleContext) -> tuple[RuleFinding, ...]:
        if context.subject["value"] <= context.dataset_context["minimum"]:
            return (RuleFinding(evidence={"observed": context.subject["value"]}, message="Value is too low"),)
        return ()

    result = QualityRuleEngine(
        (
            QualityRule(
                rule_id="price.minimum",
                version="2026-08-10",
                scope="candidate",
                severity=FindingSeverity.REJECT,
                evaluate=evaluate,
            ),
        )
    ).evaluate(
        run_id=run_id,
        dataset_id=dataset_id,
        subject_id=subject_id,
        subject={"value": 0},
        dataset_context={"minimum": 1},
    )

    assert result.disposition is QualityState.REJECTED
    assert result.findings[0].rule_version == "2026-08-10"


@pytest.mark.parametrize("contract", PILOT_REGISTRY.datasets)
def test_generic_validators_accept_pilot_contract_identity_minimums(contract) -> None:
    assert contract.identity is not None
    identity = {
        "source_id": contract.dataset.source_id,
        "dataset_id": contract.dataset.dataset_id,
        "dataset_key": contract.dataset.key,
        "effective_date": date(2026, 8, 10),
        **dict(contract.identity.fixed_fields),
    }
    for field_name in contract.identity.required_fields:
        identity.setdefault(field_name, _identity_value(field_name))

    result = QualityRuleEngine(generic_candidate_quality_rules(contract)).evaluate(
        run_id=_ids()[0],
        dataset_id=contract.dataset.dataset_id,
        subject_id="row-1",
        subject={
            "identity": identity,
            "value": "1.25",
            "parsed_at": NOW,
            "source_published_at": {"value": NOW - timedelta(hours=1), "inferred": False},
            "observed_at": {"value": datetime(2026, 8, 9, 23, 30, tzinfo=timezone(timedelta(hours=-5))), "inferred": False},
            "effective_date_inferred": False,
        },
    )

    assert result.disposition is QualityState.ACCEPTED
    assert result.findings == ()


def test_generic_identity_validator_rejects_missing_contract_required_fields() -> None:
    contract = PILOT_REGISTRY.dataset_by_key("magyp", "official-soy-fob")
    assert contract.identity is not None
    identity = {
        "source_id": contract.dataset.source_id,
        "dataset_id": contract.dataset.dataset_id,
        "dataset_key": contract.dataset.key,
        **dict(contract.identity.fixed_fields),
        "commodity": "soybean",
        "product_form": "meal",
        "effective_date": date(2026, 8, 10),
    }

    result = QualityRuleEngine(generic_candidate_quality_rules(contract)).evaluate(
        run_id=_ids()[0],
        dataset_id=contract.dataset.dataset_id,
        subject_id="row-1",
        subject={"identity": identity, "value": "499.5", "parsed_at": NOW},
    )

    assert result.disposition is QualityState.REJECTED
    assert {finding.evidence["field"] for finding in result.findings} == {"delivery_window", "source_record_id"}


def test_generic_unit_and_currency_validators_reject_contract_mismatches() -> None:
    contract = PILOT_REGISTRY.dataset_by_key("yahoo-finance", "cbot-soybean-named-contracts")
    assert contract.identity is not None
    identity = {
        "source_id": contract.dataset.source_id,
        "dataset_id": contract.dataset.dataset_id,
        "dataset_key": contract.dataset.key,
        "commodity": "soybean",
        "product_form": "beans",
        "venue": "cbot",
        "price_type": "settlement",
        "currency": "brl",
        "unit": "usd-mt",
        "contract": "ZSX26",
        "effective_date": date(2026, 8, 10),
    }

    result = QualityRuleEngine(generic_candidate_quality_rules(contract)).evaluate(
        run_id=_ids()[0],
        dataset_id=contract.dataset.dataset_id,
        subject_id="row-1",
        subject={"identity": identity, "value": "1000", "parsed_at": NOW},
    )

    assert result.disposition is QualityState.REJECTED
    assert {"unit.recognized", "currency.recognized"} <= {finding.rule_id for finding in result.findings}


def test_generic_temporal_validator_requires_distinct_inference_labels_and_timezone() -> None:
    contract = PILOT_REGISTRY.dataset_by_key("yahoo-finance", "fx-brl-usd")
    assert contract.identity is not None
    identity = {
        "source_id": contract.dataset.source_id,
        "dataset_id": contract.dataset.dataset_id,
        "dataset_key": contract.dataset.key,
        "effective_date": date(2026, 8, 10),
        **dict(contract.identity.fixed_fields),
    }

    result = QualityRuleEngine(generic_candidate_quality_rules(contract)).evaluate(
        run_id=_ids()[0],
        dataset_id=contract.dataset.dataset_id,
        subject_id="row-1",
        subject={
            "identity": identity,
            "value": "0.184",
            "parsed_at": datetime(2026, 8, 10, 12, 30),
            "source_published_at": datetime(2026, 8, 10, 8, 0, tzinfo=timezone.utc),
            "observed_at": {"value": datetime(2026, 8, 10, 8, 0), "inferred": True},
            "effective_date_inferred": "yes",
        },
    )

    assert result.disposition is QualityState.REJECTED
    assert {finding.evidence["field"] for finding in result.findings} == {
        "parsed_at",
        "source_published_at",
        "observed_at",
        "effective_date_inferred",
    }


@pytest.mark.parametrize("bad_value", ["NaN", "Infinity", "-Infinity", float("nan")])
def test_generic_numeric_validator_rejects_non_finite_values(bad_value) -> None:
    contract = PILOT_REGISTRY.dataset_by_key("yahoo-finance", "fx-cny-usd")

    result = QualityRuleEngine(generic_candidate_quality_rules(contract)).evaluate(
        run_id=_ids()[0],
        dataset_id=contract.dataset.dataset_id,
        subject_id="row-1",
        subject={"identity": _minimal_identity(contract), "value": bad_value, "parsed_at": NOW},
    )

    assert result.disposition is QualityState.REJECTED
    assert any(finding.rule_id == "value.finite" for finding in result.findings)


def test_generic_numeric_validator_rejects_impossible_sign_and_quarantines_configured_range() -> None:
    contract = PILOT_REGISTRY.dataset_by_key("magyp", "official-soy-fob")

    zero = QualityRuleEngine(generic_candidate_quality_rules(contract)).evaluate(
        run_id=_ids()[0],
        dataset_id=contract.dataset.dataset_id,
        subject_id="zero",
        subject={"identity": _minimal_identity(contract), "value": "0", "parsed_at": NOW},
    )
    out_of_range = QualityRuleEngine(
        generic_candidate_quality_rules(contract, numeric_policy=NumericValidationPolicy(minimum="100", maximum="900"))
    ).evaluate(
        run_id=_ids()[0],
        dataset_id=contract.dataset.dataset_id,
        subject_id="range",
        subject={"identity": _minimal_identity(contract), "value": "950", "parsed_at": NOW},
    )

    assert zero.disposition is QualityState.REJECTED
    assert any(finding.rule_id == "value.sign" for finding in zero.findings)
    assert out_of_range.disposition is QualityState.QUARANTINED
    assert out_of_range.findings[0].rule_id == "value.range"


def test_generic_identity_validator_rejects_unknown_extra_identity_fields() -> None:
    contract = PILOT_REGISTRY.dataset_by_key("yahoo-finance", "fx-zar-usd")
    identity = {**_minimal_identity(contract), "basis": "close"}

    result = QualityRuleEngine(generic_candidate_quality_rules(contract)).evaluate(
        run_id=_ids()[0],
        dataset_id=contract.dataset.dataset_id,
        subject_id="row-1",
        subject={
            "identity": identity,
            "identity_extra_fields": {"display_label": "BRL/USD"},
            "value": "0.056",
            "parsed_at": NOW,
        },
    )

    assert result.disposition is QualityState.REJECTED
    assert {finding.evidence["field"] for finding in result.findings} == {
        "basis",
        "identity_extra_fields.display_label",
    }


def _minimal_identity(contract) -> dict[str, object]:
    assert contract.identity is not None
    identity: dict[str, object] = {
        "source_id": contract.dataset.source_id,
        "dataset_id": contract.dataset.dataset_id,
        "dataset_key": contract.dataset.key,
        "effective_date": date(2026, 8, 10),
        **dict(contract.identity.fixed_fields),
    }
    for field_name in contract.identity.required_fields:
        identity.setdefault(field_name, _identity_value(field_name))
    return identity


def _identity_value(field_name: str) -> object:
    if field_name == "commodity":
        return "soybean"
    if field_name == "product_form":
        return "beans"
    if field_name == "venue":
        return "cbot"
    if field_name == "location":
        return "argentina-up-river"
    if field_name == "price_type":
        return "settlement"
    if field_name == "currency":
        return "usd"
    if field_name == "unit":
        return "usd-mt"
    if field_name == "contract":
        return "ZSX26"
    if field_name == "delivery_window":
        return {"start": date(2026, 8, 1), "end": date(2026, 8, 31)}
    if field_name == "source_record_id":
        return "source-row-1"
    if field_name == "effective_date":
        return date(2026, 8, 10)
    raise AssertionError(field_name)
