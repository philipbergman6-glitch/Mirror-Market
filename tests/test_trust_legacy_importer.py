"""DT-09 tests for importing v1 history CSVs as legacy trust revisions."""

from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from multiprocessing import get_context
from pathlib import Path
from threading import Barrier

import pytest

import trust.repository as repository_module
from trust import (
    Finding,
    FindingSeverity,
    GitDirectoryTrustRepository,
    ImmutableRecordConflict,
    ObservationRevision,
    QualityState,
    RepositoryFormatError,
    TemporaryDirectoryTrustRepository,
    TrustRepository,
)
from trust.legacy_importer import import_v1_history


def _repository(kind: str, root: Path) -> TrustRepository:
    if kind == "temporary":
        return TemporaryDirectoryTrustRepository(root)
    return GitDirectoryTrustRepository(root)


@pytest.fixture(params=("temporary", "git"))
def repository(request: pytest.FixtureRequest, tmp_path: Path) -> TrustRepository:
    return _repository(str(request.param), tmp_path / f"{request.param}-repository")


def _write_history_file(history_dir: Path, name: str, text: str) -> Path:
    history_dir.mkdir(parents=True, exist_ok=True)
    path = history_dir / name
    path.write_text(text, encoding="utf-8", newline="")
    return path


def _hash_tree(root: Path) -> dict[str, str]:
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_imports_representative_history_fixtures(repository: TrustRepository, tmp_path: Path) -> None:
    history_dir = tmp_path / "history"
    _write_history_file(
        history_dir,
        "argentina_fob.csv",
        "date,product,position,ship_from,ship_to,price_usd_mt\n"
        "2026-08-06,Soybeans,12019000190C,2026-08,2026-09,451.0\n",
    )
    _write_history_file(
        history_dir,
        "forward_curve.csv",
        "commodity,contract_month,label,ticker,close,fetched_date\n"
        "Soybeans,2026-11-01,Nov 2026,ZSX26.CBT,1024.5,2026-08-06\n",
    )
    _write_history_file(
        history_dir,
        "wasde.csv",
        "commodity,year,attribute,value,unit,reference_period\n"
        "SOYBEANS,2025/26,Ending Stocks,290.0,Million Bushels,2026-08-12\n",
    )

    summary = import_v1_history(repository, history_dir)

    assert summary.input_rows == 3
    assert summary.imported_rows == 3
    assert summary.skipped_rows == 0
    assert summary.finding_count == 0
    revisions = [repository.read(ObservationRevision, revision_id) for revision_id in summary.revision_ids]
    assert all(revision is not None for revision in revisions)
    assert {revision.quality_state for revision in revisions if revision is not None} == {QualityState.LEGACY}
    assert all(revision.public_eligible is False for revision in revisions if revision is not None)
    assert all(revision.artifact is None for revision in revisions if revision is not None)
    assert all(revision.parser_version == "v1-history/legacy-import" for revision in revisions if revision is not None)


def test_imports_every_v1_history_observation_table(repository: TrustRepository, tmp_path: Path) -> None:
    history_dir = tmp_path / "history"
    fixtures = {
        "argentina_fob.csv": (
            "date,product,position,ship_from,ship_to,price_usd_mt\n"
            "2026-08-06,Soybeans,12019000190C,2026-08,2026-09,451.0\n"
        ),
        "brazil_estimates.csv": (
            "source,commodity,crop_year,attribute,value,unit,report_date\n"
            "CONAB,Soybeans,2025/26,Production,169.0,Million Metric Tons,2026-08-01\n"
        ),
        "brazil_spot_prices.csv": (
            "Date,commodity,price_brl,unit\n"
            "2025-07-11,Soybean (CONAB PR farmgate),1940.0,BRL/MT\n"
        ),
        "export_sales.csv": (
            "commodity,week_ending,country,net_sales,weekly_exports,accumulated_exports,outstanding_sales,unit\n"
            "Soybeans,2026-07-30,China,1200.0,800.0,3000.0,5000.0,MT\n"
        ),
        "forward_curve.csv": (
            "commodity,contract_month,label,ticker,close,fetched_date\n"
            "Soybeans,2026-11-01,Nov 2026,ZSX26.CBT,1024.5,2026-08-06\n"
        ),
        "gulf_bids.csv": (
            "report_date,commodity,location,delivery,sale_type,basis_low,basis_high,futures_month,"
            "basis_change,price_low,price_high,average,year_ago,freight\n"
            "2026-07-29,Soybeans,Gulf Coast Ports,Current,Bid,100.0,105.0,9,UNCH,10.49,"
            "10.54,10.515,9.7325,CIF-B\n"
        ),
        "india_domestic_prices.csv": (
            "Date,commodity,Open,High,Low,Close,Volume,unit\n"
            "2026-07-28,Soybeans,4300.0,4350.0,4280.0,4320.0,50.0,INR/100KG\n"
        ),
        "inspection_destinations.csv": (
            "week_ending,region,country,commodity,inspections_mt\n"
            "2026-07-30,ATLANTIC,JAMAICA,Soybeans,1200.0\n"
        ),
        "inspection_port_flows.csv": (
            "week_ending,region,port_area,commodity,inspections_mt\n"
            "2026-07-23,ATLANTIC,S. ATLANTIC,Soybeans,13200.0\n"
        ),
        "inspections.csv": "commodity,week_ending,inspections_mt\nSoybeans,2026-07-30,985000.0\n",
        "safex_prices.csv": "Date,commodity,Close,Volume,unit,contract\n2026-07-28,Soybean (SAFEX),8065.0,387.0,ZAR/MT,\n",
        "wasde.csv": (
            "commodity,year,attribute,value,unit,reference_period\n"
            "SOYBEANS,2025/26,Ending Stocks,290.0,Million Bushels,2026-08-12\n"
        ),
    }
    for name, text in fixtures.items():
        _write_history_file(history_dir, name, text)

    summary = import_v1_history(repository, history_dir)

    assert summary.input_rows == len(fixtures)
    assert summary.imported_rows == len(fixtures)
    assert summary.skipped_rows == 0
    assert summary.finding_count == 0
    assert len(summary.revision_ids) > summary.imported_rows


def test_rerunning_importer_is_idempotent(repository: TrustRepository, tmp_path: Path) -> None:
    history_dir = tmp_path / "history"
    _write_history_file(
        history_dir,
        "brazil_spot_prices.csv",
        "Date,commodity,price_brl,unit\n"
        "2025-07-11,Soybean (CONAB PR farmgate),1940.0,BRL/MT\n",
    )

    first = import_v1_history(repository, history_dir)
    second = import_v1_history(repository, history_dir)

    assert second == first
    revision = repository.read(ObservationRevision, first.revision_ids[0])
    assert revision is not None
    assert repository.observation_revisions(revision.identity) == (revision,)


def test_import_preserves_blank_and_null_provenance_distinctions(
    repository: TrustRepository,
    tmp_path: Path,
) -> None:
    history_dir = tmp_path / "history"
    _write_history_file(
        history_dir,
        "safex_prices.csv",
        "Date,commodity,Close,Volume,unit,contract\n"
        "2026-07-28,Soybean (SAFEX),8065.0,387.0,ZAR/MT,\n",
    )

    summary = import_v1_history(repository, history_dir)

    revision = repository.read(ObservationRevision, summary.revision_ids[0])
    assert revision is not None
    provenance = json.loads(revision.identity.source_record_id or "{}")
    assert provenance["key"]["Date"] == {"kind": "value", "value": "2026-07-28"}
    assert provenance["row"]["contract"] == {"kind": "blank"}
    assert provenance["row"].get("missing_contract") is None


def test_import_surfaces_malformed_and_unsupported_rows_as_findings(
    repository: TrustRepository,
    tmp_path: Path,
) -> None:
    history_dir = tmp_path / "history"
    _write_history_file(
        history_dir,
        "forward_curve.csv",
        "commodity,contract_month,label,ticker,close,fetched_date\n"
        "Soybeans,2026-11-01,Nov 2026,ZSX26.CBT,not-a-number,2026-08-06\n",
    )
    _write_history_file(
        history_dir,
        "inspections.csv",
        "commodity,week_ending,inspections_mt\n"
        "Soybeans,2026-07-30,985000.0,unexpected-extra-cell\n",
    )
    _write_history_file(
        history_dir,
        "briefings.csv",
        "briefing_date,text,signals_json,snapshot_json,generated_at\n"
        "2026-08-06,Daily note,{},{},2026-08-06T12:00:00+00:00\n",
    )

    summary = import_v1_history(repository, history_dir)

    assert summary.input_rows == 3
    assert summary.imported_rows == 0
    assert summary.skipped_rows == 3
    assert summary.finding_count == 3
    findings = [repository.read(Finding, finding_id) for finding_id in summary.finding_ids]
    assert {finding.rule_id for finding in findings if finding is not None} == {
        "legacy.malformed-row",
        "legacy.unsupported-table",
    }
    assert {finding.severity for finding in findings if finding is not None} == {FindingSeverity.REJECT}


def test_import_surfaces_duplicate_v1_primary_keys_as_findings(
    repository: TrustRepository,
    tmp_path: Path,
) -> None:
    history_dir = tmp_path / "history"
    _write_history_file(
        history_dir,
        "inspections.csv",
        "commodity,week_ending,inspections_mt\n"
        "Soybeans,2026-07-30,985000.0\n"
        "Soybeans,2026-07-30,990000.0\n",
    )

    summary = import_v1_history(repository, history_dir)

    assert summary.input_rows == 2
    assert summary.imported_rows == 0
    assert summary.skipped_rows == 2
    assert summary.finding_count == 2
    findings = [repository.read(Finding, finding_id) for finding_id in summary.finding_ids]
    assert {finding.rule_id for finding in findings if finding is not None} == {"legacy.malformed-row"}
    assert all("duplicate v1 primary key" in finding.message for finding in findings if finding is not None)


def test_import_keeps_existing_v1_files_byte_for_byte_unchanged(
    repository: TrustRepository,
    tmp_path: Path,
) -> None:
    history_dir = tmp_path / "history"
    _write_history_file(
        history_dir,
        "gulf_bids.csv",
        "report_date,commodity,location,delivery,sale_type,basis_low,basis_high,futures_month,"
        "basis_change,price_low,price_high,average,year_ago,freight\n"
        "2026-07-29,Soybeans,Gulf Coast Ports,Current,Bid,100.0,105.0,9,UNCH,10.49,"
        "10.54,10.515,9.7325,CIF-B\n",
    )
    before = _hash_tree(history_dir)

    import_v1_history(repository, history_dir)

    assert _hash_tree(history_dir) == before


def test_import_persists_and_reloads_legacy_revisions_and_findings(
    tmp_path: Path,
) -> None:
    history_dir = tmp_path / "history"
    _write_history_file(
        history_dir,
        "inspections.csv",
        "commodity,week_ending,inspections_mt\n"
        "Soybeans,2026-07-30,985000.0\n",
    )
    _write_history_file(
        history_dir,
        "unknown_table.csv",
        "date,value\n"
        "2026-07-30,10\n",
    )
    root = tmp_path / "repository"
    writer = TemporaryDirectoryTrustRepository(root)

    summary = import_v1_history(writer, history_dir)
    reader = TemporaryDirectoryTrustRepository(root)

    assert reader.read(ObservationRevision, summary.revision_ids[0]) is not None
    assert reader.read(Finding, summary.finding_ids[0]) is not None


def test_import_summary_reconciles_input_imported_skipped_and_findings(
    repository: TrustRepository,
    tmp_path: Path,
) -> None:
    history_dir = tmp_path / "history"
    _write_history_file(
        history_dir,
        "inspection_destinations.csv",
        "week_ending,region,country,commodity,inspections_mt\n"
        "2026-07-30,ATLANTIC,JAMAICA,Soybeans,1200.0\n"
        "2026-07-30,ATLANTIC,,Soybeans,1300.0\n",
    )

    summary = import_v1_history(repository, history_dir)

    assert summary.input_rows == summary.imported_rows + summary.skipped_rows
    assert summary.input_rows == 2
    assert summary.imported_rows == 1
    assert summary.skipped_rows == 1
    assert summary.finding_count == 1


def test_critical_current_queries_do_not_include_legacy_revisions_by_default(
    repository: TrustRepository,
    tmp_path: Path,
) -> None:
    history_dir = tmp_path / "history"
    _write_history_file(
        history_dir,
        "forward_curve.csv",
        "commodity,contract_month,label,ticker,close,fetched_date\n"
        "Soybeans,2026-11-01,Nov 2026,ZSX26.CBT,1024.5,2026-08-06\n",
    )

    summary = import_v1_history(repository, history_dir)
    revision = repository.read(ObservationRevision, summary.revision_ids[0])

    assert revision is not None
    assert repository.current_accepted_revision(revision.identity) is None
    assert repository.revision_effective_at(revision.identity, datetime(2026, 8, 11, tzinfo=timezone.utc)) is None


def test_importer_can_retry_after_interrupted_revision_write(
    repository: TrustRepository,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    history_dir = tmp_path / "history"
    _write_history_file(
        history_dir,
        "brazil_estimates.csv",
        "source,commodity,crop_year,attribute,value,unit,report_date\n"
        "CONAB,Soybeans,2025/26,Production,169.0,Million Metric Tons,2026-08-01\n",
    )

    with monkeypatch.context() as patch:
        patch.setattr(repository_module, "_write_and_sync", _write_partially_then_fail)
        with pytest.raises(OSError, match="simulated interrupted payload"):
            import_v1_history(repository, history_dir)

    summary = import_v1_history(repository, history_dir)

    assert summary.imported_rows == 1
    assert repository.read(ObservationRevision, summary.revision_ids[0]) is not None


def test_importer_reports_existing_immutable_conflict_before_later_rows(
    repository: TrustRepository,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    history_dir = tmp_path / "history"
    _write_history_file(
        history_dir,
        "brazil_spot_prices.csv",
        "Date,commodity,price_brl,unit\n"
        "2025-07-11,Soybean (CONAB PR farmgate),1940.0,BRL/MT\n",
    )

    def conflict(_revisions: object) -> None:
        raise ImmutableRecordConflict("immutable record already exists with different data")

    monkeypatch.setattr(repository, "append_observation_revisions", conflict)

    with pytest.raises(RepositoryFormatError, match="legacy import conflicts"):
        import_v1_history(repository, history_dir)


def test_concurrent_imports_are_idempotent_and_safe(repository: TrustRepository, tmp_path: Path) -> None:
    history_dir = tmp_path / "history"
    _write_history_file(
        history_dir,
        "inspection_port_flows.csv",
        "week_ending,region,port_area,commodity,inspections_mt\n"
        "2026-07-23,ATLANTIC,S. ATLANTIC,Soybeans,13200.0\n",
    )
    barrier = Barrier(6)

    def run_once() -> None:
        barrier.wait()
        import_v1_history(repository, history_dir)

    with ThreadPoolExecutor(max_workers=6) as executor:
        for future in [executor.submit(run_once) for _ in range(6)]:
            future.result()

    summary = import_v1_history(repository, history_dir)
    revision = repository.read(ObservationRevision, summary.revision_ids[0])
    assert revision is not None
    assert repository.observation_revisions(revision.identity) == (revision,)


@pytest.mark.parametrize("adapter_kind", ("temporary", "git"))
def test_concurrent_imports_are_safe_across_processes(tmp_path: Path, adapter_kind: str) -> None:
    history_dir = tmp_path / "history"
    _write_history_file(
        history_dir,
        "argentina_fob.csv",
        "date,product,position,ship_from,ship_to,price_usd_mt\n"
        "2026-08-06,Soybeans,12019000190C,2026-08,2026-08,451.0\n",
    )
    root = tmp_path / f"{adapter_kind}-repository"
    context = get_context("fork")
    barrier = context.Barrier(4)
    processes = [
        context.Process(target=_import_in_process, args=(adapter_kind, root, history_dir, barrier))
        for _ in range(4)
    ]

    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0

    summary = import_v1_history(_repository(adapter_kind, root), history_dir)
    revision = _repository(adapter_kind, root).read(ObservationRevision, summary.revision_ids[0])
    assert revision is not None
    assert _repository(adapter_kind, root).observation_revisions(revision.identity) == (revision,)


def _write_partially_then_fail(temporary_file, contents: bytes) -> None:  # type: ignore[no-untyped-def]
    temporary_file.write(contents[: max(1, len(contents) // 2)])
    temporary_file.flush()
    raise OSError("simulated interrupted payload")


def _import_in_process(adapter_kind: str, root: Path, history_dir: Path, barrier) -> None:  # type: ignore[no-untyped-def]
    barrier.wait()
    import_v1_history(_repository(adapter_kind, root), history_dir)
