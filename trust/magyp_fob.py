"""Trusted ingestion adapter for Argentina MAGyP official FOB prices."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import pandas as pd
import requests

from config import MAGYP_FOB_URL, MAX_RETRIES, REQUEST_TIMEOUT
from fetchers._backoff import retry_sleep
from fetchers.magyp_fob import _parse_posts
from pipeline.results import ScraperShapeError
from trust.domain import (
    ArtifactReference,
    CandidateObservation,
    DeliveryWindow,
    Finding,
    FindingSeverity,
    ObservationIdentity,
    ObservationRevision,
    QualityState,
    RawArtifact,
    Run,
    RunStatus,
    Timestamp,
)
from trust.registry import MAGYP_FOB_CONTRACT
from trust.repository import TrustRepository

PARSER_VERSION = "magyp-fob/trusted-v1"
RUN_CODE_REVISION = "mirror-market-magyp-fob-trusted-v1"
MEDIA_TYPE = "application/json"

_PRODUCT_FORMS = {
    "Soybeans": "beans",
    "Soybean Meal": "meal",
    "Soybean Oil": "oil",
}


@dataclass(frozen=True)
class MagypArtifactReplay:
    """A raw MAGyP response that can be parsed without another network request."""

    artifact: RawArtifact
    content: bytes

    def __post_init__(self) -> None:
        digest = hashlib.sha256(self.content).hexdigest()
        if digest != self.artifact.reference.content_hash:
            raise ValueError("replay content does not match raw artifact hash")
        if self.artifact.byte_size != len(self.content):
            raise ValueError("replay content size does not match raw artifact metadata")


@dataclass(frozen=True)
class MagypTrustedIngestion:
    run_id: str
    artifact_id: str
    candidate_count: int
    accepted_revision_ids: tuple[str, ...]
    finding_ids: tuple[str, ...]
    status: RunStatus
    error: str | None = None


@dataclass(frozen=True)
class ReconciliationReport:
    matched_rows: int
    missing_in_trusted: tuple[Mapping[str, object], ...]
    missing_in_legacy: tuple[Mapping[str, object], ...]
    field_differences: tuple[Mapping[str, object], ...]

    @property
    def reconciled(self) -> bool:
        return not self.missing_in_trusted and not self.missing_in_legacy and not self.field_differences


@dataclass(frozen=True)
class MagypDualWriteResult:
    ingestion: MagypTrustedIngestion
    reconciliation: ReconciliationReport
    legacy_frame: pd.DataFrame


def fetch_magyp_fob_artifact(day: date) -> MagypArtifactReplay:
    """Fetch one MAGyP circular as a raw artifact, before JSON shape parsing."""

    params = {"Fecha": day.strftime("%d/%m/%Y")}
    last_error = "no attempts made"
    for attempt in range(1, MAX_RETRIES + 1):
        retrieved_at = datetime.now(timezone.utc)
        try:
            response = requests.get(MAGYP_FOB_URL, params=params, timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                return artifact_from_response(
                    content=response.content,
                    retrieval_url=response.url,
                    retrieved_at=retrieved_at,
                    response_status=response.status_code,
                )
            last_error = f"HTTP {response.status_code}"
        except requests.RequestException as exc:
            last_error = str(exc)
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)
    raise requests.RequestException(
        f"MAGyP FOB: {day} artifact fetch failed after {MAX_RETRIES} attempts ({last_error})"
    )


def artifact_from_response(
    *,
    content: bytes,
    retrieval_url: str = MAGYP_FOB_URL,
    retrieved_at: datetime | None = None,
    response_status: int = 200,
) -> MagypArtifactReplay:
    """Build a metadata-only raw artifact plus replay bytes for parsing."""

    now = _utc(retrieved_at or datetime.now(timezone.utc))
    dataset = MAGYP_FOB_CONTRACT.dataset
    digest = hashlib.sha256(content).hexdigest()
    reference = ArtifactReference(
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        dataset_key=dataset.key,
        content_hash=digest,
        content_retained=False,
        media_type=MEDIA_TYPE,
    )
    artifact = RawArtifact(
        reference=reference,
        retrieval_url=retrieval_url,
        retrieved_at=Timestamp(now),
        response_status=response_status,
        byte_size=len(content),
        content=None,
    )
    return MagypArtifactReplay(artifact=artifact, content=content)


def parse_magyp_candidates(
    replay: MagypArtifactReplay,
    *,
    parsed_at: datetime | None = None,
    parser_version: str = PARSER_VERSION,
) -> tuple[CandidateObservation, ...]:
    """Parse replay bytes into trusted candidate observations."""

    posts = _posts_from_replay(replay)
    frame = _parse_posts(posts)
    parsed_instant = _utc(parsed_at or replay.artifact.retrieved_at.value)
    return tuple(
        _candidate_from_row(row, replay.artifact.reference, parsed_instant, parser_version)
        for row in frame.to_dict("records")
    )


def ingest_magyp_fob_replay(
    repository: TrustRepository,
    replay: MagypArtifactReplay,
    *,
    ingested_at: datetime | None = None,
    parser_version: str = PARSER_VERSION,
) -> MagypTrustedIngestion:
    """Persist one replayed MAGyP artifact as trusted observation revisions."""

    repository.initialize()
    artifact_ref = repository.store_artifact(replay.artifact, MAGYP_FOB_CONTRACT)
    now = _utc(ingested_at or replay.artifact.retrieved_at.value)
    finding_ids: tuple[str, ...] = ()
    try:
        candidates = parse_magyp_candidates(replay, parsed_at=now, parser_version=parser_version)
    except ScraperShapeError as exc:
        failed_run = _run(
            now,
            RunStatus.FAILED,
            parser_version,
            findings_summary={FindingSeverity.REJECT: 1},
        )
        finding = Finding(
            run_id=failed_run.run_id,
            dataset_id=MAGYP_FOB_CONTRACT.dataset.dataset_id,
            subject_id=artifact_ref.artifact_id,
            rule_id="magyp.shape",
            rule_version="1",
            severity=FindingSeverity.REJECT,
            evidence={
                "artifact_id": artifact_ref.artifact_id,
                "content_hash": artifact_ref.content_hash,
                "error": str(exc),
            },
            message=str(exc),
        )
        repository.store(finding)
        repository.store(failed_run)
        return MagypTrustedIngestion(
            run_id=failed_run.run_id,
            artifact_id=artifact_ref.artifact_id,
            candidate_count=0,
            accepted_revision_ids=(),
            finding_ids=(finding.finding_id,),
            status=RunStatus.FAILED,
            error=str(exc),
        )

    revisions = tuple(
        ObservationRevision(
            identity=candidate.identity,
            value=candidate.value,
            ingested_at=now,
            quality_state=QualityState.ACCEPTED,
            public_eligible=_public_eligible(),
            artifact=artifact_ref,
            parser_version=parser_version,
            source_published_at=candidate.source_published_at,
            observed_at=candidate.observed_at,
            effective_date_inferred=candidate.effective_date_inferred,
        )
        for candidate in candidates
    )
    repository.append_observation_revisions(revisions)
    completed = _run(now, RunStatus.SUCCEEDED, parser_version)
    repository.store(completed)
    return MagypTrustedIngestion(
        run_id=completed.run_id,
        artifact_id=artifact_ref.artifact_id,
        candidate_count=len(candidates),
        accepted_revision_ids=tuple(sorted(revision.revision_id for revision in revisions)),
        finding_ids=finding_ids,
        status=RunStatus.SUCCEEDED,
    )


def dual_write_magyp_fob_replay(
    repository: TrustRepository,
    replay: MagypArtifactReplay,
    legacy_frame: pd.DataFrame,
    *,
    ingested_at: datetime | None = None,
    parser_version: str = PARSER_VERSION,
) -> MagypDualWriteResult:
    """Run trusted ingestion beside the existing dashboard dataframe path."""

    ingestion = ingest_magyp_fob_replay(
        repository,
        replay,
        ingested_at=ingested_at,
        parser_version=parser_version,
    )
    trusted_frame = trusted_magyp_fob_frame(repository)
    return MagypDualWriteResult(
        ingestion=ingestion,
        reconciliation=reconcile_magyp_fob(legacy_frame, trusted_frame),
        legacy_frame=legacy_frame.copy(),
    )


def trusted_magyp_fob_frame(repository: TrustRepository) -> pd.DataFrame:
    """Return accepted MAGyP revisions in the legacy dataframe shape."""

    rows = []
    for revision in repository.all_observation_revisions():
        identity = revision.identity
        if (
            identity.dataset_id != MAGYP_FOB_CONTRACT.dataset.dataset_id
            or revision.quality_state is not QualityState.ACCEPTED
            or revision.artifact is None
            or identity.delivery_window is None
        ):
            continue
        rows.append(
            {
                "date": identity.effective_date.isoformat(),
                "product": _legacy_product(identity.product_form),
                "position": identity.source_record_id,
                "ship_from": identity.delivery_window.start.strftime("%Y-%m"),
                "ship_to": identity.delivery_window.end.strftime("%Y-%m"),
                "price_usd_mt": float(revision.value),
            }
        )
    return pd.DataFrame(rows, columns=["date", "product", "position", "ship_from", "ship_to", "price_usd_mt"])


def reconcile_magyp_fob(legacy: pd.DataFrame, trusted: pd.DataFrame) -> ReconciliationReport:
    """Account for every row and field difference between old and trusted output."""

    key_cols = ("date", "position", "ship_from")
    value_cols = ("product", "ship_to", "price_usd_mt")
    legacy_map = _frame_map(legacy, key_cols)
    trusted_map = _frame_map(trusted, key_cols)
    legacy_keys = set(legacy_map)
    trusted_keys = set(trusted_map)

    missing_in_trusted = tuple(legacy_map[key] for key in sorted(legacy_keys - trusted_keys))
    missing_in_legacy = tuple(trusted_map[key] for key in sorted(trusted_keys - legacy_keys))
    differences: list[Mapping[str, object]] = []
    for key in sorted(legacy_keys & trusted_keys):
        legacy_row = legacy_map[key]
        trusted_row = trusted_map[key]
        for column in value_cols:
            if _comparable(legacy_row[column]) != _comparable(trusted_row[column]):
                differences.append(
                    {
                        "key": dict(zip(key_cols, key, strict=True)),
                        "field": column,
                        "legacy": legacy_row[column],
                        "trusted": trusted_row[column],
                    }
                )
    return ReconciliationReport(
        matched_rows=len(legacy_keys & trusted_keys),
        missing_in_trusted=missing_in_trusted,
        missing_in_legacy=missing_in_legacy,
        field_differences=tuple(differences),
    )


def _posts_from_replay(replay: MagypArtifactReplay) -> list[dict[str, Any]]:
    try:
        payload = json.loads(replay.content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ScraperShapeError(f"MAGyP FOB: raw artifact is not valid JSON: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise ScraperShapeError("MAGyP FOB: response root is not a JSON object")
    if "posts" in payload:
        posts = payload["posts"]
        if not isinstance(posts, list):
            raise ScraperShapeError("MAGyP FOB: 'posts' is not a list")
        if not all(isinstance(post, Mapping) for post in posts):
            raise ScraperShapeError("MAGyP FOB: 'posts' contains non-object rows")
        return [dict(post) for post in posts]
    if not payload:
        return []
    raise ScraperShapeError(
        "MAGyP FOB: response has keys but no 'posts' - schema changed "
        f"(keys: {sorted(str(key) for key in payload)[:10]})"
    )


def _candidate_from_row(
    row: Mapping[str, object],
    artifact: ArtifactReference,
    parsed_at: datetime,
    parser_version: str,
) -> CandidateObservation:
    product = str(row["product"])
    effective_date = date.fromisoformat(str(row["date"]))
    identity = ObservationIdentity(
        source_id=artifact.source_id,
        dataset_id=artifact.dataset_id,
        dataset_key=artifact.dataset_key,
        commodity="soybean",
        product_form=_PRODUCT_FORMS[product],
        location="argentina-up-river",
        price_type="official-fob",
        currency="USD",
        unit="usd-mt",
        effective_date=effective_date,
        delivery_window=_delivery_window(str(row["ship_from"]), str(row["ship_to"])),
        source_record_id=str(row["position"]),
    )
    published_at = datetime.combine(effective_date, datetime.min.time(), tzinfo=timezone.utc)
    return CandidateObservation(
        identity=identity,
        value=Decimal(str(row["price_usd_mt"])),
        artifact=artifact,
        parser_version=parser_version,
        parsed_at=parsed_at,
        source_published_at=Timestamp(published_at, inferred=True),
    )


def _delivery_window(ship_from: str, ship_to: str) -> DeliveryWindow:
    return DeliveryWindow(
        start=date.fromisoformat(f"{ship_from}-01"),
        end=date.fromisoformat(f"{ship_to}-01"),
        label=ship_from if ship_from == ship_to else f"{ship_from}/{ship_to}",
    )


def _run(
    at: datetime,
    status: RunStatus,
    parser_version: str,
    findings_summary: Mapping[FindingSeverity, int] | None = None,
) -> Run:
    dataset_id = MAGYP_FOB_CONTRACT.dataset.dataset_id
    return Run(
        code_revision=RUN_CODE_REVISION,
        started_at=at,
        ended_at=at,
        status=status,
        parser_versions={dataset_id: parser_version},
        findings_summary=findings_summary or {},
    )


def _public_eligible() -> bool:
    rights = MAGYP_FOB_CONTRACT.rights
    return bool(rights and rights.publication_eligible)


def _legacy_product(product_form: str) -> str:
    for product, form in _PRODUCT_FORMS.items():
        if form == product_form:
            return product
    raise ValueError(f"unknown MAGyP product form: {product_form}")


def _frame_map(frame: pd.DataFrame, key_cols: Sequence[str]) -> dict[tuple[object, ...], Mapping[str, object]]:
    if frame.empty:
        return {}
    normalized = frame.copy()
    for column in ("date", "ship_from", "ship_to", "position", "product"):
        if column in normalized.columns:
            normalized[column] = normalized[column].astype(str)
    rows: dict[tuple[object, ...], Mapping[str, object]] = {}
    for row in normalized.to_dict("records"):
        key = tuple(row[column] for column in key_cols)
        rows[key] = row
    return rows


def _comparable(value: object) -> object:
    if isinstance(value, float):
        return round(value, 8)
    return value


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(timezone.utc)


__all__ = [
    "MagypArtifactReplay",
    "MagypDualWriteResult",
    "MagypTrustedIngestion",
    "PARSER_VERSION",
    "ReconciliationReport",
    "artifact_from_response",
    "dual_write_magyp_fob_replay",
    "fetch_magyp_fob_artifact",
    "ingest_magyp_fob_replay",
    "parse_magyp_candidates",
    "reconcile_magyp_fob",
    "trusted_magyp_fob_frame",
]
