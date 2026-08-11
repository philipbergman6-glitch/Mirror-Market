"""Trusted derived observation calculation.

Derived calculations are pure functions over already trusted observation
revisions.  This module owns the gate in front of those functions so callers
cannot accidentally publish a partial or ineligible derived number.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, timezone
from decimal import Decimal
from types import MappingProxyType

from trust.domain import (
    DatasetResult,
    FreshnessState,
    ObservationIdentity,
    ObservationRevision,
    QualityState,
)
from trust.registry import DatasetContract, RightsAction

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
class DerivedInputRequirement:
    """One named input required by a derived calculation."""

    name: str
    identity: ObservationIdentity
    currency: str | None = None
    unit: str | None = None
    align_effective_date: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _key(self.name, "derived_input.name"))
        if self.currency is not None:
            currency = _text(self.currency, "derived_input.currency").upper()
            if not re.fullmatch(r"[A-Z]{3}", currency):
                raise ValueError("derived_input.currency must be a three-letter code")
            object.__setattr__(self, "currency", currency)
        if self.unit is not None:
            object.__setattr__(self, "unit", _key(self.unit, "derived_input.unit"))
        if not isinstance(self.align_effective_date, bool):
            raise ValueError("derived_input.align_effective_date must be a boolean")


DerivedCalculator = Callable[[Mapping[str, ObservationRevision]], Decimal | int | str]
USD_MT_CONVERSION_VERSION = "usd-mt/1.0.0"
_FX_INPUT_NAME = "fx"
_PRICE_INPUT_NAME = "price"
_USD_MT_FACTORS: Mapping[tuple[str, str, str], Decimal] = MappingProxyType(
    {
        ("soybean", "beans", "cents-bu"): Decimal("36.7437") / Decimal("100"),
        ("soybean-meal", "meal", "usd-short-ton"): Decimal("1") / Decimal("0.907185"),
        ("soybean-oil", "oil", "cents-lb"): Decimal("2204.62") / Decimal("100"),
        ("soybean", "beans", "usd-mt"): Decimal("1"),
        ("soybean-meal", "meal", "usd-mt"): Decimal("1"),
        ("soybean-oil", "oil", "usd-mt"): Decimal("1"),
    }
)


@dataclass(frozen=True)
class DerivedCalculation:
    """A versioned calculation and its complete input contract."""

    calculation_id: str
    version: str
    output_identity: ObservationIdentity
    inputs: tuple[DerivedInputRequirement, ...]
    calculate: DerivedCalculator

    def __post_init__(self) -> None:
        object.__setattr__(self, "calculation_id", _key(self.calculation_id, "derived_calculation.calculation_id"))
        object.__setattr__(self, "version", _text(self.version, "derived_calculation.version"))
        names = tuple(requirement.name for requirement in self.inputs)
        if not names:
            raise ValueError("derived_calculation.inputs cannot be empty")
        if len(names) != len(set(names)):
            raise ValueError("derived_calculation.inputs cannot contain duplicate names")
        if not callable(self.calculate):
            raise ValueError("derived_calculation.calculate must be callable")


@dataclass(frozen=True)
class DerivedObservationResult:
    """A derived calculation outcome.

    Unavailable results carry deterministic reason codes and never carry a
    partial numeric value.
    """

    revision: ObservationRevision | None
    unavailable_reasons: tuple[str, ...] = ()
    input_revision_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        reasons = tuple(
            sorted(
                _key(reason, "derived_result.unavailable_reasons")
                for reason in self.unavailable_reasons
            )
        )
        object.__setattr__(self, "unavailable_reasons", reasons)
        object.__setattr__(self, "input_revision_ids", tuple(sorted(self.input_revision_ids)))
        if (self.revision is None) == (not reasons):
            raise ValueError("derived result must contain either a revision or unavailable reasons")

    @property
    def available(self) -> bool:
        return self.revision is not None


def derive_observation(
    calculation: DerivedCalculation,
    *,
    revisions_by_input: Mapping[str, ObservationRevision | None],
    contracts_by_dataset: Mapping[str, DatasetContract],
    results_by_dataset: Mapping[str, DatasetResult],
) -> DerivedObservationResult:
    """Run a derived calculation only after every required input is eligible."""

    revisions: dict[str, ObservationRevision] = {}
    reasons: list[str] = []
    for requirement in calculation.inputs:
        revision = revisions_by_input.get(requirement.name)
        if revision is None:
            reasons.append(f"{requirement.name}.missing")
            continue
        revisions[requirement.name] = revision
        reasons.extend(
            _input_ineligibility_reasons(
                requirement,
                revision,
                calculation,
                contracts_by_dataset,
                results_by_dataset,
            )
        )

    if reasons:
        return DerivedObservationResult(
            revision=None,
            unavailable_reasons=tuple(reasons),
            input_revision_ids=tuple(revision.revision_id for revision in revisions.values()),
        )

    frozen_inputs = MappingProxyType(dict(revisions))
    value = calculation.calculate(frozen_inputs)
    input_revisions = tuple(revisions[name] for name in sorted(revisions))
    ingested_at = max(revision.ingested_at for revision in input_revisions).astimezone(timezone.utc)
    revision = ObservationRevision(
        identity=calculation.output_identity,
        value=value,
        ingested_at=ingested_at,
        quality_state=QualityState.ACCEPTED,
        public_eligible=True,
        calculation_id=calculation.calculation_id,
        calculation_version=calculation.version,
        input_revision_ids=tuple(revision.revision_id for revision in input_revisions),
    )
    return DerivedObservationResult(revision=revision, input_revision_ids=revision.input_revision_ids)


def derive_usd_mt_observation(
    *,
    price_revision: ObservationRevision | None,
    fx_revision: ObservationRevision | None = None,
    output_identity: ObservationIdentity | None = None,
    contracts_by_dataset: Mapping[str, DatasetContract],
    results_by_dataset: Mapping[str, DatasetResult],
    calculation_version: str = USD_MT_CONVERSION_VERSION,
    max_fx_age_days: int = 3,
) -> DerivedObservationResult:
    """Convert one trusted native commodity price into a USD/MT revision.

    The calculation is still delegated to :func:`derive_observation`; this
    wrapper adds the domain policy DT-18 needs: contract-specific native units,
    optional latest-known-good FX, and no output when conversion inputs are not
    usable.
    """

    if price_revision is None:
        return DerivedObservationResult(revision=None, unavailable_reasons=(_PRICE_INPUT_NAME + ".missing",))
    if max_fx_age_days < 0:
        raise ValueError("max_fx_age_days must be non-negative")

    price_identity = price_revision.identity
    output = output_identity or usd_mt_identity_for_price(price_identity)
    factor = _usd_mt_factor(price_identity)
    reasons = list(_usd_mt_identity_reasons(price_identity, output, factor))
    input_revisions: dict[str, ObservationRevision | None] = {_PRICE_INPUT_NAME: price_revision}

    if price_identity.currency == "USD":
        fx_identity = None
    else:
        if fx_revision is None:
            reasons.append(_FX_INPUT_NAME + ".missing")
            fx_identity = None
        else:
            fx_identity = fx_revision.identity
            input_revisions[_FX_INPUT_NAME] = fx_revision
            reasons.extend(
                _fx_alignment_reasons(
                    price_identity.currency,
                    price_identity.effective_date,
                    fx_revision,
                    max_fx_age_days,
                )
            )

    if reasons:
        return DerivedObservationResult(
            revision=None,
            unavailable_reasons=tuple(reasons),
            input_revision_ids=tuple(
                revision.revision_id for revision in input_revisions.values() if revision is not None
            ),
        )

    inputs = [DerivedInputRequirement(_PRICE_INPUT_NAME, price_identity)]
    if fx_identity is not None:
        inputs.append(
            DerivedInputRequirement(
                _FX_INPUT_NAME,
                fx_identity,
                currency="USD",
                unit=fx_identity.unit,
                align_effective_date=False,
            )
        )

    calculation = DerivedCalculation(
        calculation_id=_usd_mt_calculation_id(price_identity),
        version=calculation_version,
        output_identity=output,
        inputs=tuple(inputs),
        calculate=lambda revisions: _calculate_usd_mt(revisions, factor),
    )
    return derive_observation(
        calculation,
        revisions_by_input=input_revisions,
        contracts_by_dataset=contracts_by_dataset,
        results_by_dataset=results_by_dataset,
    )


def usd_mt_identity_for_price(identity: ObservationIdentity) -> ObservationIdentity:
    """Return the canonical USD/MT output identity for a native price identity."""

    return ObservationIdentity(
        source_id=identity.source_id,
        dataset_id=identity.dataset_id,
        dataset_key=identity.dataset_key,
        commodity=identity.commodity,
        product_form=identity.product_form,
        price_type=identity.price_type,
        currency="USD",
        unit="usd-mt",
        effective_date=identity.effective_date,
        venue=identity.venue,
        location=identity.location,
        contract=identity.contract,
        delivery_window=identity.delivery_window,
        source_record_id=identity.source_record_id,
    )


def _usd_mt_factor(identity: ObservationIdentity) -> Decimal | None:
    factor = _USD_MT_FACTORS.get((identity.commodity, identity.product_form, identity.unit))
    if factor is not None:
        return factor
    if identity.unit == f"{identity.currency.lower()}-mt":
        return Decimal("1")
    return None


def _usd_mt_identity_reasons(
    price_identity: ObservationIdentity,
    output_identity: ObservationIdentity,
    factor: Decimal | None,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if factor is None:
        reasons.append(_PRICE_INPUT_NAME + ".unit-conversion")
    if output_identity.currency != "USD":
        reasons.append("output.currency")
    if output_identity.unit != "usd-mt":
        reasons.append("output.unit")
    if output_identity.effective_date != price_identity.effective_date:
        reasons.append("output.date")
    return tuple(reasons)


def _fx_alignment_reasons(
    price_currency: str,
    price_date: date,
    fx_revision: ObservationRevision,
    max_fx_age_days: int,
) -> tuple[str, ...]:
    identity = fx_revision.identity
    reasons: list[str] = []
    if identity.fx_pair is None:
        reasons.append(_FX_INPUT_NAME + ".identity")
    else:
        if identity.fx_pair.base_currency != price_currency or identity.fx_pair.quote_currency != "USD":
            reasons.append(_FX_INPUT_NAME + ".pair")
        if identity.currency != "USD":
            reasons.append(_FX_INPUT_NAME + ".currency")
        expected_unit = f"usd-per-{price_currency.lower()}"
        if identity.unit != expected_unit:
            reasons.append(_FX_INPUT_NAME + ".unit")
    if identity.effective_date > price_date:
        reasons.append(_FX_INPUT_NAME + ".date")
    elif (price_date - identity.effective_date).days > max_fx_age_days:
        reasons.append(_FX_INPUT_NAME + ".stale")
    return tuple(reasons)


def _calculate_usd_mt(revisions: Mapping[str, ObservationRevision], factor: Decimal) -> Decimal:
    native_usd_mt = revisions[_PRICE_INPUT_NAME].value * factor
    fx = revisions.get(_FX_INPUT_NAME)
    if fx is None:
        return native_usd_mt
    return native_usd_mt * fx.value


def _usd_mt_calculation_id(identity: ObservationIdentity) -> str:
    return f"{identity.commodity}.{identity.product_form}.usd-mt"


def _input_ineligibility_reasons(
    requirement: DerivedInputRequirement,
    revision: ObservationRevision,
    calculation: DerivedCalculation,
    contracts_by_dataset: Mapping[str, DatasetContract],
    results_by_dataset: Mapping[str, DatasetResult],
) -> tuple[str, ...]:
    reasons: list[str] = []
    prefix = requirement.name
    if revision.identity.observation_id != requirement.identity.observation_id:
        reasons.append(f"{prefix}.identity")
    if revision.quality_state is not QualityState.ACCEPTED:
        reasons.append(f"{prefix}.quality")
    if not revision.public_eligible:
        reasons.append(f"{prefix}.public-eligible")
    contract = contracts_by_dataset.get(revision.identity.dataset_id)
    if contract is None or contract.rights is None or not contract.rights.allows(RightsAction.DERIVED_PUBLICATION):
        reasons.append(f"{prefix}.rights")
    result = results_by_dataset.get(revision.identity.dataset_id)
    if result is None or not result.eligible or result.freshness is not FreshnessState.CURRENT:
        reasons.append(f"{prefix}.freshness")
    if revision.identity.currency != (requirement.currency or requirement.identity.currency):
        reasons.append(f"{prefix}.currency")
    if revision.identity.unit != (requirement.unit or requirement.identity.unit):
        reasons.append(f"{prefix}.unit")
    if (
        requirement.align_effective_date
        and revision.identity.effective_date != calculation.output_identity.effective_date
    ):
        reasons.append(f"{prefix}.date")
    return tuple(reasons)
