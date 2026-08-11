"""DT-02 contract tests for the trusted source/dataset registry."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import replace
from datetime import date

import pytest

from trust import Dataset, Source
from trust.registry import (
    AGRURAL_PARANAGUA_CONTRACT,
    PILOT_REGISTRY,
    REQUIRED_FX_PAIRS,
    CadenceContract,
    CadenceKind,
    ContractRegistry,
    CoverageContract,
    Criticality,
    DatasetContract,
    FreshnessContract,
    IdentityContract,
    RawRetention,
    RegistryValidationError,
    RightsAction,
    RightsDecision,
    RightsPolicy,
    ValidationPolicy,
)

REVIEW_ON = date(2027, 2, 10)


def rights(**overrides: RightsDecision) -> RightsPolicy:
    decisions = {action: RightsDecision.ALLOWED for action in RightsAction}
    decisions.update({RightsAction[action.upper()]: decision for action, decision in overrides.items()})
    return RightsPolicy(
        decisions=decisions,
        required_attribution="Test source",
        review_on=REVIEW_ON,
        evidence=("test-rights-review",),
    )


def dataset_contract(source: Source, key: str = "daily-prices") -> DatasetContract:
    return DatasetContract(
        dataset=Dataset(source.source_id, key, key.replace("-", " ").title()),
        cadence=CadenceContract(CadenceKind.BUSINESS_DAILY, 24, "UTC"),
        identity=IdentityContract(
            required_fields=(
                "commodity",
                "product_form",
                "venue",
                "price_type",
                "currency",
                "unit",
                "effective_date",
            ),
            fixed_fields={"venue": "test-venue"},
        ),
        coverage=CoverageContract(
            expected_keys=("test-venue",),
            key_fields=("venue",),
            minimum_coverage="1",
        ),
        freshness=FreshnessContract(stale_after_hours=72),
        units=("usd-mt",),
        criticality=Criticality.CRITICAL,
        validation=ValidationPolicy(("identity.required", "value.positive")),
        raw_retention=RawRetention.CONTENT,
        rights=rights(),
    )


def test_real_pilot_registry_is_complete_and_round_trips() -> None:
    serialized = PILOT_REGISTRY.to_dict()
    round_tripped = ContractRegistry.from_dict(json.loads(json.dumps(serialized)))

    assert round_tripped == PILOT_REGISTRY
    assert serialized["schema_version"] == 1
    assert len(PILOT_REGISTRY.sources) == 3
    assert len(PILOT_REGISTRY.datasets) == 9

    magyp = PILOT_REGISTRY.dataset_by_key("magyp", "official-soy-fob")
    assert magyp.identity is not None
    assert magyp.units == ("usd-mt",)
    assert magyp.coverage == CoverageContract(
        expected_keys=("soybean:beans", "soybean:meal", "soybean:oil"),
        key_fields=("commodity", "product_form"),
        minimum_coverage="1",
    )
    assert "delivery_window" in magyp.identity.required_fields
    assert magyp.criticality is Criticality.SUPPORTING

    agrural = PILOT_REGISTRY.dataset_by_key("agrural", "paranagua-soybean-fob")
    assert agrural == AGRURAL_PARANAGUA_CONTRACT
    assert agrural.identity is not None
    assert agrural.identity.fixed_fields["location"] == "paranagua"
    assert agrural.criticality is Criticality.CRITICAL

    benchmark_contracts = [contract for contract in PILOT_REGISTRY.datasets if contract.dataset.key.startswith("cbot-")]
    assert len(benchmark_contracts) == 3
    for contract in benchmark_contracts:
        assert contract.identity is not None
        assert contract.validation is not None
        assert "contract" in contract.identity.required_fields
        assert "settlement.confirmed" in contract.validation.rule_ids

    fx_contracts = [
        PILOT_REGISTRY.dataset_by_key("yahoo-finance", f"fx-{pair.lower().replace('/', '-')}")
        for pair in REQUIRED_FX_PAIRS
    ]
    product_forms: list[str] = []
    for contract in fx_contracts:
        assert contract.identity is not None
        assert contract.validation is not None
        product_forms.append(contract.identity.fixed_fields["product_form"])
        assert "fx.orientation" in contract.validation.rule_ids
    assert tuple(product_forms) == tuple(pair.lower().replace("/", "-") for pair in REQUIRED_FX_PAIRS)


def test_real_pilot_registry_encodes_reviewed_checkpoint_policies() -> None:
    assert REQUIRED_FX_PAIRS == ("BRL/USD", "CNY/USD", "INR/USD", "ZAR/USD")
    assert {
        contract.identity.fixed_fields["product_form"]
        for contract in PILOT_REGISTRY.datasets
        if contract.dataset.key.startswith("fx-") and contract.identity is not None
    } == {"brl-usd", "cny-usd", "inr-usd", "zar-usd"}

    for contract in PILOT_REGISTRY.datasets:
        assert contract.freshness == FreshnessContract(stale_after_hours=96)
        assert contract.raw_retention is RawRetention.METADATA_ONLY
        assert contract.rights is not None
        assert contract.rights.publication_eligible is False
        assert contract.rights.decision(RightsAction.PUBLIC_DISPLAY) is RightsDecision.UNKNOWN
        assert contract.rights.decision(RightsAction.DERIVED_PUBLICATION) is RightsDecision.UNKNOWN


def test_rights_are_explicit_and_unknown_is_fail_closed() -> None:
    assert {action.value for action in RightsAction} == {
        "raw-content-retention",
        "normalized-history-retention",
        "internal-display",
        "public-display",
        "derived-publication",
        "commercial-use",
        "redistribution",
    }
    for contract in PILOT_REGISTRY.datasets:
        assert contract.rights is not None
        assert set(contract.rights.decisions) == set(RightsAction)
        assert contract.rights.evidence
        assert contract.rights.review_on is not None

    benchmark = PILOT_REGISTRY.dataset_by_key("yahoo-finance", "cbot-soybean-named-contracts")
    assert benchmark.rights is not None
    assert benchmark.rights.decision(RightsAction.PUBLIC_DISPLAY) is RightsDecision.UNKNOWN
    assert benchmark.rights.publication_eligible is False


@pytest.mark.parametrize(
    "missing",
    ["cadence", "identity", "coverage", "freshness", "units", "criticality", "validation", "raw_retention", "rights"],
)
def test_registry_rejects_incomplete_dataset_contracts(missing: str) -> None:
    source = Source("test-source", "Test source", "Test source")
    complete = dataset_contract(source)
    if missing == "cadence":
        contract = replace(complete, cadence=None)
    elif missing == "identity":
        contract = replace(complete, identity=None)
    elif missing == "coverage":
        contract = replace(complete, coverage=None)
    elif missing == "freshness":
        contract = replace(complete, freshness=None)
    elif missing == "units":
        contract = replace(complete, units=())
    elif missing == "criticality":
        contract = replace(complete, criticality=None)
    elif missing == "validation":
        contract = replace(complete, validation=None)
    elif missing == "raw_retention":
        contract = replace(complete, raw_retention=None)
    else:
        contract = replace(complete, rights=None)

    with pytest.raises(RegistryValidationError, match=missing):
        ContractRegistry((source,), (contract,))


def test_registry_rejects_missing_source_and_duplicate_identifiers() -> None:
    source = Source("test-source", "Test source", "Test source")
    contract = dataset_contract(source)

    with pytest.raises(RegistryValidationError, match="unregistered source"):
        ContractRegistry((), (contract,))
    with pytest.raises(RegistryValidationError, match="duplicate source identifier"):
        ContractRegistry((source, source), ())
    with pytest.raises(RegistryValidationError, match="duplicate dataset identifier"):
        ContractRegistry((source,), (contract, contract))


def test_registry_rejects_missing_rights_decisions() -> None:
    source = Source("test-source", "Test source", "Test source")
    contract = dataset_contract(source)
    assert contract.rights is not None
    incomplete_rights = replace(
        contract.rights,
        decisions={
            action: decision
            for action, decision in contract.rights.decisions.items()
            if action is not RightsAction.REDISTRIBUTION
        },
    )

    with pytest.raises(RegistryValidationError, match="redistribution"):
        ContractRegistry((source,), (replace(contract, rights=incomplete_rights),))


@pytest.mark.parametrize(
    "contract",
    [
        lambda source: replace(
            dataset_contract(source),
            raw_retention=RawRetention.CONTENT,
            rights=rights(raw_content_retention=RightsDecision.PROHIBITED),
        ),
        lambda source: replace(
            dataset_contract(source),
            rights=rights(internal_display=RightsDecision.PROHIBITED),
        ),
        lambda source: replace(
            dataset_contract(source),
            freshness=FreshnessContract(stale_after_hours=12),
        ),
        lambda source: replace(
            dataset_contract(source),
            coverage=CoverageContract(
                expected_keys=("soybean",),
                key_fields=("not-an-identity-field",),
                minimum_coverage="1",
            ),
        ),
    ],
)
def test_registry_rejects_contradictory_contracts(contract: Callable[[Source], DatasetContract]) -> None:
    source = Source("test-source", "Test source", "Test source")

    with pytest.raises(RegistryValidationError):
        ContractRegistry((source,), (contract(source),))


def test_datasets_from_one_source_can_have_different_cadence_and_rights() -> None:
    source = Source("multi-dataset-source", "Multi-dataset source", "Test source")
    daily = dataset_contract(source, "daily")
    weekly = replace(
        dataset_contract(source, "weekly"),
        cadence=CadenceContract(CadenceKind.WEEKLY, 168, "UTC"),
        freshness=FreshnessContract(stale_after_hours=240),
        raw_retention=RawRetention.METADATA_ONLY,
        rights=rights(
            raw_content_retention=RightsDecision.UNKNOWN,
            public_display=RightsDecision.UNKNOWN,
        ),
    )

    registry = ContractRegistry((source,), (daily, weekly))

    assert registry.dataset(daily.dataset.dataset_id).cadence != registry.dataset(weekly.dataset.dataset_id).cadence
    assert daily.rights is not None
    assert weekly.rights is not None
    assert daily.rights.publication_eligible is True
    assert weekly.rights.publication_eligible is False


def test_validation_policy_records_whether_empty_publication_is_permitted() -> None:
    required = ValidationPolicy(("publication.present",))
    optional = ValidationPolicy(("publication.present",), allows_empty_publication=True)

    assert required.allows_empty_publication is False
    assert optional.allows_empty_publication is True
    assert ValidationPolicy.from_dict(optional.to_dict()) == optional
