"""Trusted ingestion tests for the MAGyP Argentina official FOB adapter."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest

from pipeline.results import ScraperShapeError
from trust import MAGYP_FOB_CONTRACT, Finding, QualityState, TemporaryDirectoryTrustRepository
from trust.magyp_fob import (
    artifact_from_response,
    dual_write_magyp_fob_replay,
    fetch_magyp_fob_artifact,
    ingest_magyp_fob_replay,
    parse_magyp_candidates,
    posts_from_replay,
    reconcile_magyp_fob,
    trusted_magyp_fob_frame,
)

NOW = datetime(2026, 8, 10, 15, 0, tzinfo=timezone.utc)


def _post(posicion: str, precio: float, mes_desde: int = 8, mes_hasta: int = 8) -> dict:
    return {
        "fecha": "2026-08-05 00:00:00.000",
        "circular": "2031",
        "posicion": posicion,
        "precio": precio,
        "mesDesde": mes_desde,
        "añoDesde": 2026,
        "mesHasta": mes_hasta,
        "añoHasta": 2026,
    }


def _content(posts: list[dict]) -> bytes:
    return json.dumps({"posts": posts}, separators=(",", ":")).encode("utf-8")


def test_replay_parse_preserves_trusted_candidate_identity_fields() -> None:
    replay = artifact_from_response(
        content=_content([
            _post("12019000190C", 450),
            _post("15071000100Q", 1186),
            _post("23040010100B", 350, 9, 11),
        ]),
        retrieved_at=NOW,
    )

    candidates = parse_magyp_candidates(replay, parsed_at=NOW)

    assert len(candidates) == 3
    beans = next(item for item in candidates if item.identity.product_form == "beans")
    assert beans.identity.commodity == "soybean"
    assert beans.identity.source_record_id == "12019000190C"
    assert beans.identity.price_type == "official-fob"
    assert beans.identity.currency == "USD"
    assert beans.identity.unit == "usd-mt"
    assert beans.identity.effective_date.isoformat() == "2026-08-05"
    assert beans.identity.delivery_window is not None
    assert beans.identity.delivery_window.start.isoformat() == "2026-08-01"
    assert beans.identity.delivery_window.end.isoformat() == "2026-08-01"
    assert beans.value == Decimal("450.0")


def test_source_adapter_returns_raw_artifact_before_parsing(monkeypatch) -> None:
    content = b'{"renamed_posts":[]}'
    response = SimpleNamespace(
        status_code=200,
        url="https://example.test/magyp?Fecha=05/08/2026",
        content=content,
    )

    def fake_get(url, *, params, timeout):
        assert params == {"Fecha": "05/08/2026"}
        assert timeout > 0
        assert url
        return response

    monkeypatch.setattr("trust.magyp_fob.requests.get", fake_get)

    replay = fetch_magyp_fob_artifact(datetime(2026, 8, 5, tzinfo=timezone.utc).date())

    assert replay.content == content
    assert replay.artifact.retrieval_url == response.url
    assert replay.artifact.response_status == 200


def test_raw_artifact_replay_ingests_without_network_and_reconciles_with_legacy_output(tmp_path) -> None:
    repository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    legacy = pd.DataFrame(
        [
            {
                "date": "2026-08-05",
                "product": "Soybeans",
                "position": "12019000190C",
                "ship_from": "2026-08",
                "ship_to": "2026-08",
                "price_usd_mt": 450.0,
            },
            {
                "date": "2026-08-05",
                "product": "Soybean Meal",
                "position": "23040010100B",
                "ship_from": "2026-09",
                "ship_to": "2026-11",
                "price_usd_mt": 350.0,
            },
        ]
    )
    replay = artifact_from_response(
        content=_content([
            _post("12019000190C", 450),
            _post("23040010100B", 350, 9, 11),
        ]),
        retrieved_at=NOW,
    )

    result = ingest_magyp_fob_replay(repository, replay, ingested_at=NOW)
    trusted = trusted_magyp_fob_frame(repository)
    report = reconcile_magyp_fob(legacy, trusted)

    assert result.candidate_count == 2
    assert len(result.accepted_revision_ids) == 2
    assert repository.read_artifact(result.artifact_id) is not None
    assert report.reconciled
    assert report.matched_rows == 2


def test_dual_write_returns_legacy_frame_unchanged_with_reconciliation(tmp_path) -> None:
    repository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    legacy = pd.DataFrame(
        [
            {
                "date": "2026-08-05",
                "product": "Soybeans",
                "position": "12019000190C",
                "ship_from": "2026-08",
                "ship_to": "2026-08",
                "price_usd_mt": 450.0,
            },
        ]
    )
    replay = artifact_from_response(content=_content([_post("12019000190C", 450)]), retrieved_at=NOW)

    result = dual_write_magyp_fob_replay(repository, replay, legacy, ingested_at=NOW)

    pd.testing.assert_frame_equal(result.legacy_frame, legacy)
    assert result.reconciliation.reconciled
    assert len(result.ingestion.accepted_revision_ids) == 1


def test_duplicate_ingestion_is_idempotent(tmp_path) -> None:
    repository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    replay = artifact_from_response(content=_content([_post("12019000190C", 450)]), retrieved_at=NOW)

    first = ingest_magyp_fob_replay(repository, replay, ingested_at=NOW)
    second = ingest_magyp_fob_replay(repository, replay, ingested_at=NOW)

    assert second.accepted_revision_ids == first.accepted_revision_ids
    assert len(repository.all_observation_revisions()) == 1


def test_shape_drift_records_typed_finding_and_no_accepted_revisions(tmp_path) -> None:
    repository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    replay = artifact_from_response(content=b'{"items":[]}', retrieved_at=NOW)

    result = ingest_magyp_fob_replay(repository, replay, ingested_at=NOW)

    assert result.accepted_revision_ids == ()
    assert len(result.finding_ids) == 1
    finding = repository.read(Finding, result.finding_ids[0])
    assert finding is not None
    assert finding.rule_id == "magyp.shape"
    assert finding.subject_id == result.artifact_id
    assert repository.all_observation_revisions() == ()


def test_holiday_empty_publication_writes_artifact_but_no_revisions_or_findings(tmp_path) -> None:
    repository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    replay = artifact_from_response(content=b"{}", retrieved_at=NOW)

    result = ingest_magyp_fob_replay(repository, replay, ingested_at=NOW)

    assert result.candidate_count == 0
    assert result.accepted_revision_ids == ()
    assert result.finding_ids == ()
    assert repository.read_artifact(result.artifact_id) is not None
    assert repository.all_observation_revisions() == ()


def test_new_artifact_revision_does_not_erase_prior_result(tmp_path) -> None:
    repository = TemporaryDirectoryTrustRepository(tmp_path / "trust")
    first = artifact_from_response(content=_content([_post("12019000190C", 450)]), retrieved_at=NOW)
    second = artifact_from_response(content=_content([_post("12019000190C", 451)]), retrieved_at=NOW)

    ingest_magyp_fob_replay(repository, first, ingested_at=NOW)
    ingest_magyp_fob_replay(repository, second, ingested_at=datetime(2026, 8, 10, 16, 0, tzinfo=timezone.utc))

    revisions = repository.all_observation_revisions()
    assert len(revisions) == 2
    assert {revision.value for revision in revisions} == {Decimal("450.0"), Decimal("451.0")}
    assert {revision.quality_state for revision in revisions} == {QualityState.ACCEPTED}


def test_reconciliation_report_accounts_for_missing_rows_and_field_differences() -> None:
    legacy = pd.DataFrame(
        [
            {
                "date": "2026-08-05",
                "product": "Soybeans",
                "position": "12019000190C",
                "ship_from": "2026-08",
                "ship_to": "2026-08",
                "price_usd_mt": 450.0,
            },
            {
                "date": "2026-08-05",
                "product": "Soybean Meal",
                "position": "23040010100B",
                "ship_from": "2026-09",
                "ship_to": "2026-11",
                "price_usd_mt": 350.0,
            },
        ]
    )
    trusted = pd.DataFrame(
        [
            {
                "date": "2026-08-05",
                "product": "Soybeans",
                "position": "12019000190C",
                "ship_from": "2026-08",
                "ship_to": "2026-08",
                "price_usd_mt": 451.0,
            },
            {
                "date": "2026-08-05",
                "product": "Soybean Oil",
                "position": "15071000100Q",
                "ship_from": "2026-08",
                "ship_to": "2026-08",
                "price_usd_mt": 1186.0,
            },
        ]
    )

    report = reconcile_magyp_fob(legacy, trusted)

    assert not report.reconciled
    assert len(report.missing_in_trusted) == 1
    assert len(report.missing_in_legacy) == 1
    assert report.field_differences == (
        {
            "key": {
                "date": "2026-08-05",
                "position": "12019000190C",
                "ship_from": "2026-08",
            },
            "field": "price_usd_mt",
            "legacy": 450.0,
            "trusted": 451.0,
        },
    )


def test_magyp_contract_keeps_durable_raw_artifact_metadata_only() -> None:
    replay = artifact_from_response(content=_content([_post("12019000190C", 450)]), retrieved_at=NOW)

    assert not replay.artifact.reference.content_retained
    assert replay.artifact.content is None
    assert MAGYP_FOB_CONTRACT.dataset.dataset_id == replay.artifact.reference.dataset_id


def test_unpublished_date_answers_with_an_empty_json_array() -> None:
    """Observed live 2026-08-11: an unpublished date returns b"[]", not b"{}".

    The v1 fetcher's docstring claims an empty object; it only survives the
    real shape because `"posts" in []` is False and `not []` is True. The
    trusted parser used to hard-fail on every holiday.
    """
    replay = artifact_from_response(content=b"[]", retrieved_at=NOW)

    assert posts_from_replay(replay) == []
    assert parse_magyp_candidates(replay, parsed_at=NOW) == ()


def test_non_empty_array_root_is_still_a_schema_change() -> None:
    replay = artifact_from_response(content=b'[{"posicion":"12019000190C"}]', retrieved_at=NOW)

    with pytest.raises(ScraperShapeError, match="non-empty JSON array"):
        posts_from_replay(replay)
