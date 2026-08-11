from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from trust import (
    ArtifactReference,
    Dataset,
    DatasetResult,
    DatasetResultStatus,
    DerivedCalculation,
    DerivedInputRequirement,
    Finding,
    FindingSeverity,
    FreshnessState,
    ObservationIdentity,
    ObservationRevision,
    QualityState,
    RightsAction,
    RightsDecision,
    RightsPolicy,
    Run,
    RunStatus,
    Source,
    TemporaryDirectoryTrustRepository,
    ValidationPolicy,
    derive_observation,
)
from trust.registry import DatasetContract

NOW = datetime(2026, 8, 10, 12, tzinfo=timezone.utc)
HASH_A = hashlib.sha256(b"input-a").hexdigest()
HASH_B = hashlib.sha256(b"input-b").hexdigest()


def test_valid_aligned_calculation_records_provenance_and_is_idempotent() -> None:
    fixture = _fixture()
    calculation = _spread_calculation(fixture)

    first = derive_observation(
        calculation,
        revisions_by_input={"bid": fixture["bid_revision"], "ask": fixture["ask_revision"]},
        contracts_by_dataset=fixture["contracts"],
        results_by_dataset=fixture["results"],
    )
    second = derive_observation(
        calculation,
        revisions_by_input={"bid": fixture["bid_revision"], "ask": fixture["ask_revision"]},
        contracts_by_dataset=fixture["contracts"],
        results_by_dataset=fixture["results"],
    )

    assert first.available is True
    assert first.revision == second.revision
    assert first.revision is not None
    assert first.revision.value == Decimal("5")
    assert first.revision.calculation_id == "basis.spread"
    assert first.revision.calculation_version == "1.0.0"
    assert first.revision.input_revision_ids == tuple(
        sorted((fixture["ask_revision"].revision_id, fixture["bid_revision"].revision_id))
    )
    assert first.revision.artifact is None
    assert first.revision.parser_version is None


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda fixture: {"ask": None}, "ask.missing"),
        (
            lambda fixture: {"ask": replace(fixture["ask_revision"], quality_state=QualityState.QUARANTINED)},
            "ask.quality",
        ),
        (
            lambda fixture: {"ask": replace(fixture["ask_revision"], public_eligible=False)},
            "ask.public-eligible",
        ),
        (
            lambda fixture: {
                "contracts": {
                    **fixture["contracts"],
                    fixture["ask_dataset"].dataset_id: _contract(fixture["ask_dataset"], rights=False),
                }
            },
            "ask.rights",
        ),
        (
            lambda fixture: {
                "results": {
                    **fixture["results"],
                    fixture["ask_dataset"].dataset_id: replace(
                        fixture["ask_result"],
                        freshness=FreshnessState.STALE,
                        eligible=False,
                        status=DatasetResultStatus.QUARANTINED,
                        accepted_revision_ids=(),
                        findings=(fixture["finding"],),
                    ),
                }
            },
            "ask.freshness",
        ),
        (
            lambda fixture: {
                "ask": replace(
                    fixture["ask_revision"],
                    identity=replace(fixture["ask_identity"], effective_date=date(2026, 8, 9)),
                )
            },
            "ask.identity",
        ),
        (
            lambda fixture: {"calculation": _spread_calculation(fixture, output_date=date(2026, 8, 11))},
            "bid.date",
        ),
        (
            lambda fixture: {
                "ask": replace(fixture["ask_revision"], identity=replace(fixture["ask_identity"], currency="BRL"))
            },
            "ask.currency",
        ),
        (
            lambda fixture: {
                "ask": replace(fixture["ask_revision"], identity=replace(fixture["ask_identity"], unit="usd-bu"))
            },
            "ask.unit",
        ),
    ],
)
def test_ineligible_or_misaligned_input_returns_unavailable_without_partial_value(mutate, reason: str) -> None:
    fixture = _fixture()
    overrides = mutate(fixture)
    calculation = overrides.get("calculation", _spread_calculation(fixture, fail_if_called=True))
    revisions_by_input = {
        "bid": fixture["bid_revision"],
        "ask": overrides.get("ask", fixture["ask_revision"]),
    }

    result = derive_observation(
        calculation,
        revisions_by_input=revisions_by_input,
        contracts_by_dataset=overrides.get("contracts", fixture["contracts"]),
        results_by_dataset=overrides.get("results", fixture["results"]),
    )

    assert result.available is False
    assert result.revision is None
    assert reason in result.unavailable_reasons


def test_new_input_revision_produces_new_derived_revision_without_erasing_prior_result(tmp_path) -> None:
    fixture = _fixture()
    repo = TemporaryDirectoryTrustRepository(tmp_path)
    prior = derive_observation(
        _spread_calculation(fixture),
        revisions_by_input={"bid": fixture["bid_revision"], "ask": fixture["ask_revision"]},
        contracts_by_dataset=fixture["contracts"],
        results_by_dataset=fixture["results"],
    ).revision
    assert prior is not None
    repo.append_observation_revision(prior)

    new_ask = replace(fixture["ask_revision"], value=Decimal("111"), ingested_at=NOW + timedelta(hours=1))
    updated = derive_observation(
        _spread_calculation(fixture),
        revisions_by_input={"bid": fixture["bid_revision"], "ask": new_ask},
        contracts_by_dataset=fixture["contracts"],
        results_by_dataset=fixture["results"],
    ).revision
    assert updated is not None
    repo.append_observation_revision(updated)

    revisions = repo.observation_revisions(fixture["output_identity"])
    assert tuple(revision.revision_id for revision in revisions) == (prior.revision_id, updated.revision_id)
    assert updated.revision_id != prior.revision_id
    assert updated.input_revision_ids == tuple(sorted((fixture["bid_revision"].revision_id, new_ask.revision_id)))


def test_recursive_derived_revision_can_be_used_as_an_eligible_input() -> None:
    fixture = _fixture()
    spread = derive_observation(
        _spread_calculation(fixture),
        revisions_by_input={"bid": fixture["bid_revision"], "ask": fixture["ask_revision"]},
        contracts_by_dataset=fixture["contracts"],
        results_by_dataset=fixture["results"],
    ).revision
    assert spread is not None
    spread_result = replace(
        fixture["bid_result"],
        dataset_id=fixture["output_dataset"].dataset_id,
        accepted_revision_ids=(spread.revision_id,),
        artifact_references=(fixture["output_artifact"],),
    )

    doubled = derive_observation(
        DerivedCalculation(
            calculation_id="basis.double-spread",
            version="1.0.0",
            output_identity=replace(fixture["output_identity"], source_record_id="double-spread"),
            inputs=(DerivedInputRequirement("spread", fixture["output_identity"]),),
            calculate=lambda inputs: inputs["spread"].value * 2,
        ),
        revisions_by_input={"spread": spread},
        contracts_by_dataset={
            **fixture["contracts"],
            fixture["output_dataset"].dataset_id: _contract(fixture["output_dataset"]),
        },
        results_by_dataset={
            **fixture["results"],
            fixture["output_dataset"].dataset_id: spread_result,
        },
    ).revision

    assert doubled is not None
    assert doubled.value == Decimal("10")
    assert doubled.input_revision_ids == (spread.revision_id,)


def _fixture() -> dict[str, object]:
    run = Run(code_revision="abc123", started_at=NOW, ended_at=NOW + timedelta(minutes=1), status=RunStatus.SUCCEEDED)
    bid_source = Source("bid-src", "Bid source")
    ask_source = Source("ask-src", "Ask source")
    output_source = Source("derived-src", "Derived source")
    bid_dataset = Dataset(bid_source.source_id, "bids", "Bids")
    ask_dataset = Dataset(ask_source.source_id, "asks", "Asks")
    output_dataset = Dataset(output_source.source_id, "spreads", "Spreads")
    bid_artifact = _artifact(bid_source, bid_dataset, HASH_A)
    ask_artifact = _artifact(ask_source, ask_dataset, HASH_B)
    output_artifact = _artifact(output_source, output_dataset, hashlib.sha256(b"derived").hexdigest())
    bid_identity = _identity(bid_source, bid_dataset, source_record_id="bid")
    ask_identity = _identity(ask_source, ask_dataset, source_record_id="ask")
    output_identity = _identity(output_source, output_dataset, source_record_id="spread")
    bid_revision = _revision(bid_identity, bid_artifact, Decimal("100"), NOW)
    ask_revision = _revision(ask_identity, ask_artifact, Decimal("105"), NOW + timedelta(minutes=5))
    finding = Finding(
        run_id=run.run_id,
        dataset_id=ask_dataset.dataset_id,
        subject_id=ask_identity.observation_id,
        rule_id="freshness.stale",
        rule_version="1.0.0",
        severity=FindingSeverity.QUARANTINE,
        evidence={"as_of_date": date(2026, 8, 9)},
        message="Dataset is stale",
    )
    bid_result = _result(run, bid_dataset, bid_revision, bid_artifact)
    ask_result = _result(run, ask_dataset, ask_revision, ask_artifact)
    return {
        "run": run,
        "bid_source": bid_source,
        "ask_source": ask_source,
        "output_source": output_source,
        "bid_dataset": bid_dataset,
        "ask_dataset": ask_dataset,
        "output_dataset": output_dataset,
        "bid_artifact": bid_artifact,
        "ask_artifact": ask_artifact,
        "output_artifact": output_artifact,
        "bid_identity": bid_identity,
        "ask_identity": ask_identity,
        "output_identity": output_identity,
        "bid_revision": bid_revision,
        "ask_revision": ask_revision,
        "bid_result": bid_result,
        "ask_result": ask_result,
        "finding": finding,
        "contracts": {
            bid_dataset.dataset_id: _contract(bid_dataset),
            ask_dataset.dataset_id: _contract(ask_dataset),
        },
        "results": {
            bid_dataset.dataset_id: bid_result,
            ask_dataset.dataset_id: ask_result,
        },
    }


def _spread_calculation(
    fixture: dict[str, object],
    *,
    output_date: date = date(2026, 8, 10),
    fail_if_called: bool = False,
) -> DerivedCalculation:
    def calculate(inputs):
        if fail_if_called:
            raise AssertionError("calculator should not run for unavailable derived inputs")
        return inputs["ask"].value - inputs["bid"].value

    return DerivedCalculation(
        calculation_id="basis.spread",
        version="1.0.0",
        output_identity=replace(fixture["output_identity"], effective_date=output_date),
        inputs=(
            DerivedInputRequirement("bid", fixture["bid_identity"]),
            DerivedInputRequirement("ask", fixture["ask_identity"]),
        ),
        calculate=calculate,
    )


def _contract(dataset: Dataset, *, rights: bool = True) -> DatasetContract:
    decisions = {
        action: (RightsDecision.ALLOWED if rights else RightsDecision.PROHIBITED)
        for action in RightsAction
    }
    return DatasetContract(
        dataset=dataset,
        cadence=None,
        identity=None,
        coverage=None,
        freshness=None,
        units=("usd-mt",),
        criticality=None,
        validation=None,
        raw_retention=None,
        rights=RightsPolicy(decisions=decisions, required_attribution=None, review_on=None, evidence=("test",)),
    )


def _identity(source: Source, dataset: Dataset, *, source_record_id: str) -> ObservationIdentity:
    return ObservationIdentity(
        source_id=source.source_id,
        dataset_id=dataset.dataset_id,
        dataset_key=dataset.key,
        commodity="soybean",
        product_form="meal",
        price_type="fob",
        currency="USD",
        unit="usd-mt",
        effective_date=date(2026, 8, 10),
        location="up-river",
        source_record_id=source_record_id,
    )


def _artifact(source: Source, dataset: Dataset, content_hash: str) -> ArtifactReference:
    return ArtifactReference(
        source_id=source.source_id,
        dataset_id=dataset.dataset_id,
        dataset_key=dataset.key,
        content_hash=content_hash,
        content_retained=True,
    )


def _revision(
    identity: ObservationIdentity,
    artifact: ArtifactReference,
    value: Decimal,
    ingested_at: datetime,
) -> ObservationRevision:
    return ObservationRevision(
        identity=identity,
        value=value,
        ingested_at=ingested_at,
        quality_state=QualityState.ACCEPTED,
        public_eligible=True,
        artifact=artifact,
        parser_version="test/1.0.0",
    )


def _result(
    run: Run,
    dataset: Dataset,
    revision: ObservationRevision,
    artifact: ArtifactReference,
) -> DatasetResult:
    return DatasetResult(
        run_id=run.run_id,
        dataset_id=dataset.dataset_id,
        status=DatasetResultStatus.SUCCESS,
        candidate_count=1,
        accepted_revision_ids=(revision.revision_id,),
        artifact_references=(artifact,),
        findings=(),
        coverage=Decimal("1"),
        freshness=FreshnessState.CURRENT,
        eligible=True,
        validation_policy=ValidationPolicy(("test.rule",)),
        as_of_date=date(2026, 8, 10),
    )
