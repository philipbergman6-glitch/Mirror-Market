"""DT-17 tests for trusted FX observations."""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timezone
from decimal import Decimal

from trust import (
    ArtifactReference,
    FxPairIdentity,
    ObservationIdentity,
    ObservationRevision,
    QualityState,
    RawArtifact,
    TemporaryDirectoryTrustRepository,
    Timestamp,
    evaluate_required_fx_coverage,
    trusted_fx_frame,
)
from trust.registry import PILOT_REGISTRY

NOW = datetime(2026, 8, 10, 12, tzinfo=timezone.utc)
CONTENT_HASH = hashlib.sha256(b"fx").hexdigest()


def test_required_fx_coverage_blocks_missing_and_stale_pairs(tmp_path) -> None:
    repository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    repository.initialize()
    brl = _revision("BRL/USD", date(2026, 8, 10), "0.184")
    cny = _revision("CNY/USD", date(2026, 8, 6), "0.139")
    repository.append_observation_revisions((brl, cny))

    coverage = evaluate_required_fx_coverage(repository, as_of_date=date(2026, 8, 10), max_age_days=3)

    assert coverage.eligible is False
    assert coverage.available_pairs == ("BRL/USD", "CNY/USD")
    assert coverage.missing_pairs == ("INR/USD", "ZAR/USD")
    assert coverage.stale_pairs == ("CNY/USD",)


def test_trusted_fx_frame_preserves_market_date_orientation_and_revision(tmp_path) -> None:
    repository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    repository.initialize()
    revision = _revision("ZAR/USD", date(2026, 8, 10), "0.056")
    repository.append_observation_revision(revision)

    frame = trusted_fx_frame(repository)

    assert frame.to_dict("records") == [
        {
            "Date": "2026-08-10",
            "pair": "ZAR/USD",
            "Close": 0.056,
            "quote_convention": "quote-per-base",
            "unit": "usd-per-zar",
            "revision_id": revision.revision_id,
        }
    ]


def _revision(pair: str, effective_date: date, value: str) -> ObservationRevision:
    contract = PILOT_REGISTRY.dataset_by_key("yahoo-finance", f"fx-{pair.lower().replace('/', '-')}")
    dataset = contract.dataset
    fx_pair = FxPairIdentity(*pair.split("/"))
    artifact = ArtifactReference(
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        dataset_key=dataset.key,
        content_hash=CONTENT_HASH,
        content_retained=False,
        media_type="text/csv",
    )
    RawArtifact(
        reference=artifact,
        retrieval_url=f"https://example.test/{pair}",
        retrieved_at=Timestamp(NOW),
        response_status=200,
        byte_size=2,
        content=None,
    )
    identity = ObservationIdentity(
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        dataset_key=dataset.key,
        commodity="foreign-exchange",
        product_form=fx_pair.product_form,
        venue="yahoo-finance",
        price_type="market-close",
        currency=fx_pair.quote_currency,
        unit=fx_pair.unit,
        fx_pair=fx_pair,
        effective_date=effective_date,
    )
    return ObservationRevision(
        identity=identity,
        value=Decimal(value),
        ingested_at=NOW,
        quality_state=QualityState.ACCEPTED,
        public_eligible=False,
        artifact=artifact,
        parser_version="trusted-fx-test/1",
        observed_at=Timestamp(datetime.combine(effective_date, datetime.min.time(), tzinfo=timezone.utc)),
    )
