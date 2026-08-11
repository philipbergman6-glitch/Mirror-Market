"""Trusted FX observation helpers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import pandas as pd

from trust.domain import FxPairIdentity, ObservationRevision, QualityState
from trust.registry import FX_CONTRACTS, REQUIRED_FX_PAIRS
from trust.repository import TrustRepository


@dataclass(frozen=True)
class RequiredFxCoverage:
    as_of_date: date
    required_pairs: tuple[str, ...]
    available_pairs: tuple[str, ...]
    missing_pairs: tuple[str, ...]
    stale_pairs: tuple[str, ...]
    max_age_days: int

    @property
    def eligible(self) -> bool:
        return not self.missing_pairs and not self.stale_pairs


def trusted_fx_frame(repository: TrustRepository) -> pd.DataFrame:
    """Return accepted trusted FX observations in the legacy currency shape."""

    rows = []
    fx_dataset_ids = {contract.dataset.dataset_id for contract in FX_CONTRACTS}
    for revision in repository.all_observation_revisions():
        identity = revision.identity
        if (
            identity.dataset_id not in fx_dataset_ids
            or identity.fx_pair is None
            or revision.quality_state is not QualityState.ACCEPTED
        ):
            continue
        rows.append(
            {
                "Date": identity.effective_date.isoformat(),
                "pair": identity.fx_pair.pair,
                "Close": float(revision.value),
                "quote_convention": identity.fx_pair.quote_convention,
                "unit": identity.unit,
                "revision_id": revision.revision_id,
            }
        )
    return pd.DataFrame(rows, columns=["Date", "pair", "Close", "quote_convention", "unit", "revision_id"])


def evaluate_required_fx_coverage(
    repository: TrustRepository,
    *,
    as_of_date: date,
    required_pairs: Sequence[str] = REQUIRED_FX_PAIRS,
    max_age_days: int = 3,
) -> RequiredFxCoverage:
    """Check that every conversion-critical FX pair has a recent accepted rate."""

    if max_age_days < 0:
        raise ValueError("max_age_days must be non-negative")
    required = tuple(_normalize_pair(pair) for pair in required_pairs)
    latest = _latest_accepted_by_pair(repository, as_of_date=as_of_date)
    missing = tuple(pair for pair in required if pair not in latest)
    stale = tuple(
        pair
        for pair in required
        if pair in latest and (as_of_date - latest[pair].identity.effective_date).days > max_age_days
    )
    return RequiredFxCoverage(
        as_of_date=as_of_date,
        required_pairs=required,
        available_pairs=tuple(pair for pair in required if pair in latest),
        missing_pairs=missing,
        stale_pairs=stale,
        max_age_days=max_age_days,
    )


def fx_pair_from_required_pair(pair: str) -> FxPairIdentity:
    base, quote = _normalize_pair(pair).split("/", maxsplit=1)
    return FxPairIdentity(base_currency=base, quote_currency=quote)


def _latest_accepted_by_pair(repository: TrustRepository, *, as_of_date: date) -> dict[str, ObservationRevision]:
    fx_dataset_ids = {contract.dataset.dataset_id for contract in FX_CONTRACTS}
    latest: dict[str, ObservationRevision] = {}
    for revision in repository.all_observation_revisions():
        identity = revision.identity
        if (
            identity.dataset_id not in fx_dataset_ids
            or identity.fx_pair is None
            or revision.quality_state is not QualityState.ACCEPTED
            or identity.effective_date > as_of_date
        ):
            continue
        pair = identity.fx_pair.pair
        previous = latest.get(pair)
        if previous is None or identity.effective_date > previous.identity.effective_date:
            latest[pair] = revision
    return latest


def _normalize_pair(pair: str) -> str:
    normalized = pair.strip().upper()
    FxPairIdentity(*normalized.split("/", maxsplit=1))
    return normalized


def decimal_fx_rate(value: Decimal | int | str) -> Decimal:
    """Small helper for callers that need decimal multiplication, not floats."""

    result = value if isinstance(value, Decimal) else Decimal(value)
    if not result.is_finite() or result <= 0:
        raise ValueError("FX rate must be a positive finite decimal")
    return result


__all__ = [
    "RequiredFxCoverage",
    "decimal_fx_rate",
    "evaluate_required_fx_coverage",
    "fx_pair_from_required_pair",
    "trusted_fx_frame",
]
