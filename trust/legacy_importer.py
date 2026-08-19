"""Import v1 Git history CSVs into the v2 trust ledger as legacy revisions."""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from pipeline.history import HISTORY_TABLES
from trust.domain import (
    ContractIdentity,
    Dataset,
    DeliveryWindow,
    Finding,
    FindingSeverity,
    ObservationIdentity,
    ObservationRevision,
    QualityState,
    Run,
    RunStatus,
    Source,
    Timestamp,
)
from trust.repository import ImmutableRecordConflict, RepositoryFormatError, TrustRepository

PARSER_VERSION = "v1-history/legacy-import"
_LEGACY_SOURCE = Source(key="legacy-v1-history", name="Mirror Market v1 Git history")
_LEGACY_INGESTED_AT = datetime(2026, 8, 10, 0, 0, tzinfo=timezone.utc)


@dataclass(frozen=True)
class LegacyImportSummary:
    run_id: str
    input_rows: int
    imported_rows: int
    skipped_rows: int
    finding_count: int
    revision_ids: tuple[str, ...]
    finding_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.input_rows != self.imported_rows + self.skipped_rows:
            raise ValueError("legacy import summary does not reconcile input rows")
        if self.finding_count != len(self.finding_ids):
            raise ValueError("legacy import summary does not reconcile findings")


@dataclass(frozen=True)
class _LegacyDataset:
    table: str
    file_name: str
    dataset: Dataset


@dataclass(frozen=True)
class _LegacyRow:
    dataset: _LegacyDataset
    row_number: int
    row: Mapping[str, str | None]
    key: Mapping[str, str | None]
    malformed_reason: str | None = None

    @property
    def subject_id(self) -> str:
        return _provenance(self.dataset.file_name, self.dataset.table, self.key, self.row)


_Mapper = Callable[[_LegacyRow], tuple[ObservationRevision, ...]]


def import_v1_history(
    repository: TrustRepository,
    history_dir: str | Path,
) -> LegacyImportSummary:
    """Import v1 history CSV rows as non-publishable legacy revisions.

    The importer only reads CSV files under ``history_dir``.  Durable output is
    written through ``TrustRepository`` and is deterministic for a fixed input
    tree, so re-running an import creates no duplicate revisions or findings.
    """

    root = Path(history_dir)
    rows = tuple(_read_legacy_rows(root))
    run = _run_for_rows(rows)

    revision_ids: list[str] = []
    finding_ids: list[str] = []
    revisions: list[ObservationRevision] = []
    findings: list[Finding] = []
    imported_rows = 0
    skipped = 0
    for legacy_row in rows:
        if legacy_row.malformed_reason is not None:
            finding = _finding(
                run,
                legacy_row,
                rule_id="legacy.malformed-row",
                message=f"v1 history row cannot be imported: {legacy_row.malformed_reason}",
            )
            findings.append(finding)
            finding_ids.append(finding.finding_id)
            skipped += 1
            continue
        mapper = _MAPPERS.get(legacy_row.dataset.table)
        if mapper is None:
            finding = _finding(
                run,
                legacy_row,
                rule_id="legacy.unsupported-table",
                message=f"v1 history table {legacy_row.dataset.table} is not an observation source",
            )
            findings.append(finding)
            finding_ids.append(finding.finding_id)
            skipped += 1
            continue
        try:
            row_revisions = mapper(legacy_row)
        except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
            finding = _finding(
                run,
                legacy_row,
                rule_id="legacy.malformed-row",
                message=f"v1 history row cannot be imported: {exc}",
            )
            findings.append(finding)
            finding_ids.append(finding.finding_id)
            skipped += 1
            continue
        if not row_revisions:
            finding = _finding(
                run,
                legacy_row,
                rule_id="legacy.unsupported-row",
                message="v1 history row contains no importable observation values",
            )
            findings.append(finding)
            finding_ids.append(finding.finding_id)
            skipped += 1
            continue
        revisions.extend(row_revisions)
        revision_ids.extend(revision.revision_id for revision in row_revisions)
        imported_rows += 1

    for finding in findings:
        repository.store(finding)
    try:
        repository.append_observation_revisions(revisions)
    except ImmutableRecordConflict as exc:
        raise RepositoryFormatError(f"legacy import conflicts with durable revision: {exc}") from exc

    completed_run = Run(
        code_revision=run.code_revision,
        started_at=run.started_at,
        ended_at=run.ended_at,
        status=RunStatus.SUCCEEDED,
        findings_summary={FindingSeverity.REJECT: len(finding_ids)} if finding_ids else {},
    )
    repository.store(completed_run)
    return LegacyImportSummary(
        run_id=completed_run.run_id,
        input_rows=len(rows),
        imported_rows=imported_rows,
        skipped_rows=skipped,
        finding_count=len(finding_ids),
        revision_ids=tuple(sorted(revision_ids)),
        finding_ids=tuple(sorted(finding_ids)),
    )


def _read_legacy_rows(root: Path) -> Iterable[_LegacyRow]:
    table_names = tuple(dict.fromkeys((*HISTORY_TABLES, *(_table_name(path) for path in root.glob("*.csv")))))
    for table in sorted(table_names):
        path = root / f"{table}.csv"
        if not path.exists():
            continue
        dataset = _dataset(table)
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                if path.stat().st_size:
                    yield _LegacyRow(
                        dataset=dataset,
                        row_number=1,
                        row={},
                        key={},
                        malformed_reason="CSV header is missing",
                    )
                continue
            table_rows: list[_LegacyRow] = []
            for index, row in enumerate(reader, start=2):
                normalized = {field: row.get(field) for field in reader.fieldnames}
                key = _row_key(table, normalized)
                extra_cells = row.get(None)
                malformed_reason = None
                if extra_cells:
                    normalized["__extra_cells__"] = "|".join(extra_cells)
                    malformed_reason = "row contains more cells than the CSV header"
                table_rows.append(
                    _LegacyRow(
                        dataset=dataset,
                        row_number=index,
                        row=normalized,
                        key=key,
                        malformed_reason=malformed_reason,
                    )
                )
            key_counts = Counter(_provenance_key(item.key) for item in table_rows)
            for item in table_rows:
                if key_counts[_provenance_key(item.key)] <= 1:
                    yield item
                    continue
                reason = "duplicate v1 primary key"
                if item.malformed_reason is not None:
                    reason = f"{item.malformed_reason}; {reason}"
                yield _LegacyRow(
                    dataset=item.dataset,
                    row_number=item.row_number,
                    row=item.row,
                    key=item.key,
                    malformed_reason=reason,
                )


def _table_name(path: Path) -> str:
    return path.name.removesuffix(".csv")


def _dataset(table: str) -> _LegacyDataset:
    dataset = Dataset(source_id=_LEGACY_SOURCE.source_id, key=f"v1-{_slug(table)}", name=f"v1 {table} history")
    return _LegacyDataset(table=table, file_name=f"{table}.csv", dataset=dataset)


def _row_key(table: str, row: Mapping[str, str | None]) -> Mapping[str, str | None]:
    key_columns = HISTORY_TABLES.get(table)
    if key_columns is None:
        return dict(row)
    return {column: row.get(column) for column in key_columns}


def _provenance_key(values: Mapping[str, str | None]) -> str:
    return json.dumps(_encoded_mapping(values), sort_keys=True, separators=(",", ":"))


def _run_for_rows(rows: Sequence[_LegacyRow]) -> Run:
    digest = hashlib.sha256(
        json.dumps(
            [
                {
                    "file": row.dataset.file_name,
                    "row_number": row.row_number,
                    "row": _encoded_mapping(row.row),
                }
                for row in rows
            ],
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return Run(
        code_revision=f"v1-history-import:{digest}",
        started_at=_LEGACY_INGESTED_AT,
        ended_at=_LEGACY_INGESTED_AT,
        status=RunStatus.SUCCEEDED,
    )


def _finding(run: Run, legacy_row: _LegacyRow, *, rule_id: str, message: str) -> Finding:
    return Finding(
        run_id=run.run_id,
        dataset_id=legacy_row.dataset.dataset.dataset_id,
        subject_id=legacy_row.subject_id,
        rule_id=rule_id,
        rule_version="1",
        severity=FindingSeverity.REJECT,
        evidence={
            "file": legacy_row.dataset.file_name,
            "table": legacy_row.dataset.table,
            "row_number": legacy_row.row_number,
            "key": _encoded_mapping(legacy_row.key),
            "row": _encoded_mapping(legacy_row.row),
        },
        message=message,
    )


def _revision(
    legacy_row: _LegacyRow,
    *,
    value: Decimal,
    effective_date: date,
    commodity: str,
    product_form: str,
    price_type: str,
    currency: str,
    unit: str,
    venue: str | None = None,
    location: str | None = None,
    contract: ContractIdentity | None = None,
    delivery_window: DeliveryWindow | None = None,
    observed_at: date | None = None,
    volume: Decimal | None = None,
) -> ObservationRevision:
    dataset = legacy_row.dataset.dataset
    identity = ObservationIdentity(
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        dataset_key=dataset.key,
        commodity=_commodity(commodity),
        product_form=_slug(product_form),
        price_type=_slug(price_type),
        currency=currency,
        unit=_unit(unit),
        effective_date=effective_date,
        venue=_slug(venue) if venue else None,
        location=_slug(location) if location else None,
        contract=contract,
        delivery_window=delivery_window,
        source_record_id=legacy_row.subject_id,
    )
    return ObservationRevision(
        identity=identity,
        value=value,
        ingested_at=_LEGACY_INGESTED_AT,
        quality_state=QualityState.LEGACY,
        public_eligible=False,
        parser_version=PARSER_VERSION,
        observed_at=Timestamp(datetime.combine(observed_at, datetime.min.time(), tzinfo=timezone.utc))
        if observed_at
        else None,
        effective_date_inferred=False,
        volume=volume,
    )


def _argentina_fob(row: _LegacyRow) -> tuple[ObservationRevision, ...]:
    effective = _date(row.row["date"], "date")
    start = _month_start(_required(row.row, "ship_from"))
    end = _month_end(_required(row.row, "ship_to") or _required(row.row, "ship_from"))
    return (_revision(
        row,
        value=_decimal(row.row["price_usd_mt"], "price_usd_mt"),
        effective_date=effective,
        commodity=_required(row.row, "product"),
        product_form=_required(row.row, "product"),
        price_type="official-fob",
        currency="USD",
        unit="usd-mt",
        location="argentina-up-river",
        delivery_window=DeliveryWindow(start, end, f"{start:%Y-%m}/{end:%Y-%m}"),
        observed_at=effective,
    ),)


def _brazil_spot(row: _LegacyRow) -> tuple[ObservationRevision, ...]:
    commodity = _required(row.row, "commodity")
    return (_revision(
        row,
        value=_decimal(row.row["price_brl"], "price_brl"),
        effective_date=_date(row.row["Date"], "Date"),
        commodity=commodity,
        product_form=commodity,
        price_type="farmgate-spot",
        currency="BRL",
        unit=_required(row.row, "unit"),
        location=commodity,
    ),)


def _safex(row: _LegacyRow) -> tuple[ObservationRevision, ...]:
    commodity = _required(row.row, "commodity")
    return (_revision(
        row,
        value=_decimal(row.row["Close"], "Close"),
        effective_date=_date(row.row["Date"], "Date"),
        commodity=commodity,
        product_form=commodity,
        # JSE SAFEX via Grain SA publishes no settlement column at all — the
        # stored number is the session's last trade (#157). Calling it a
        # settlement in the identity made the row claim a mark it does not have.
        price_type="last-traded",
        currency="ZAR",
        unit=_required(row.row, "unit"),
        venue="safex",
        volume=_optional_decimal(row.row.get("Volume"), "Volume"),
    ),)


def _forward_curve(row: _LegacyRow) -> tuple[ObservationRevision, ...]:
    commodity = _required(row.row, "commodity")
    contract_month = _date(row.row["contract_month"], "contract_month")
    ticker = _required(row.row, "ticker")
    return (_revision(
        row,
        value=_decimal(row.row["close"], "close"),
        effective_date=_date(row.row["fetched_date"], "fetched_date"),
        commodity=commodity,
        product_form=commodity,
        # A forward-curve leg is a yfinance daily bar for one delivery month.
        # Delayed, and not verified to equal the exchange's settlement — see
        # `pricing.semantics.PriceType`.
        price_type="delayed-close",
        currency="USD",
        unit=_futures_unit(commodity),
        venue="cbot",
        contract=ContractIdentity("cbot", ticker, contract_month.strftime("%Y-%m")),
    ),)


def _wasde(row: _LegacyRow) -> tuple[ObservationRevision, ...]:
    return (_revision(
        row,
        value=_decimal(row.row["value"], "value"),
        effective_date=_date(row.row["reference_period"], "reference_period"),
        commodity=_required(row.row, "commodity"),
        product_form=_required(row.row, "commodity"),
        price_type=f"wasde-{_required(row.row, 'attribute')}",
        currency="XXX",
        unit=_required(row.row, "unit"),
        location="united-states",
    ),)


def _inspections(row: _LegacyRow) -> tuple[ObservationRevision, ...]:
    return (_revision(
        row,
        value=_decimal(row.row["inspections_mt"], "inspections_mt"),
        effective_date=_date(row.row["week_ending"], "week_ending"),
        commodity=_required(row.row, "commodity"),
        product_form=_required(row.row, "commodity"),
        price_type="export-inspections",
        currency="XXX",
        unit="mt",
        location="united-states",
    ),)


def _inspection_port_flows(row: _LegacyRow) -> tuple[ObservationRevision, ...]:
    return (_revision(
        row,
        value=_decimal(row.row["inspections_mt"], "inspections_mt"),
        effective_date=_date(row.row["week_ending"], "week_ending"),
        commodity=_required(row.row, "commodity"),
        product_form=_required(row.row, "commodity"),
        price_type="export-inspections",
        currency="XXX",
        unit="mt",
        location=f"{_required(row.row, 'region')}-{_required(row.row, 'port_area')}",
    ),)


def _inspection_destinations(row: _LegacyRow) -> tuple[ObservationRevision, ...]:
    return (_revision(
        row,
        value=_decimal(row.row["inspections_mt"], "inspections_mt"),
        effective_date=_date(row.row["week_ending"], "week_ending"),
        commodity=_required(row.row, "commodity"),
        product_form=_required(row.row, "commodity"),
        price_type="export-inspections",
        currency="XXX",
        unit="mt",
        location=f"{_required(row.row, 'region')}-{_required(row.row, 'country')}",
    ),)


def _brazil_estimates(row: _LegacyRow) -> tuple[ObservationRevision, ...]:
    return (_revision(
        row,
        value=_decimal(row.row["value"], "value"),
        effective_date=_date(row.row["report_date"], "report_date"),
        commodity=_required(row.row, "commodity"),
        product_form=_required(row.row, "commodity"),
        price_type=f"crop-estimate-{_required(row.row, 'attribute')}",
        currency="XXX",
        unit=_required(row.row, "unit"),
        location="brazil",
    ),)


def _gulf_bids(row: _LegacyRow) -> tuple[ObservationRevision, ...]:
    effective_date = _date(row.row["report_date"], "report_date")
    commodity = _required(row.row, "commodity")
    sale_type = _required(row.row, "sale_type")
    location = _required(row.row, "location")
    measures = (
        ("basis_low", "basis-low", "XXX", "cents-bu"),
        ("basis_high", "basis-high", "XXX", "cents-bu"),
        ("price_low", "price-low", "USD", "usd-bu"),
        ("price_high", "price-high", "USD", "usd-bu"),
        ("average", "average", "USD", "usd-bu"),
        ("year_ago", "year-ago", "USD", "usd-bu"),
    )
    return tuple(
        _revision(
            row,
            value=value,
            effective_date=effective_date,
            commodity=commodity,
            product_form=commodity,
            price_type=f"gulf-{sale_type}-{measure_name}",
            currency=currency,
            unit=unit,
            location=location,
        )
        for column, measure_name, currency, unit in measures
        if (value := _optional_decimal(row.row.get(column), column)) is not None
    )


def _india_domestic(row: _LegacyRow) -> tuple[ObservationRevision, ...]:
    commodity = _required(row.row, "commodity")
    return (_revision(
        row,
        value=_decimal(row.row["Close"], "Close"),
        effective_date=_date(row.row["Date"], "Date"),
        commodity=commodity,
        product_form=commodity,
        price_type="mandi-spot",
        currency="INR",
        unit=_required(row.row, "unit"),
        location="india-domestic",
        volume=_optional_decimal(row.row.get("Volume"), "Volume"),
    ),)


def _export_sales(row: _LegacyRow) -> tuple[ObservationRevision, ...]:
    effective_date = _date(row.row["week_ending"], "week_ending")
    commodity = _required(row.row, "commodity")
    unit = _required(row.row, "unit")
    location = f"united-states-{_required(row.row, 'country')}"
    measures = (
        ("net_sales", "net-sales"),
        ("weekly_exports", "weekly-exports"),
        ("accumulated_exports", "accumulated-exports"),
        ("outstanding_sales", "outstanding-sales"),
    )
    return tuple(
        _revision(
            row,
            value=value,
            effective_date=effective_date,
            commodity=commodity,
            product_form=commodity,
            price_type=f"export-sales-{measure_name}",
            currency="XXX",
            unit=unit,
            location=location,
        )
        for column, measure_name in measures
        if (value := _optional_decimal(row.row.get(column), column)) is not None
    )


_MAPPERS: dict[str, _Mapper] = {
    "argentina_fob": _argentina_fob,
    "brazil_estimates": _brazil_estimates,
    "brazil_spot_prices": _brazil_spot,
    "export_sales": _export_sales,
    "forward_curve": _forward_curve,
    "gulf_bids": _gulf_bids,
    "india_domestic_prices": _india_domestic,
    "inspection_destinations": _inspection_destinations,
    "inspection_port_flows": _inspection_port_flows,
    "inspections": _inspections,
    "safex_prices": _safex,
    "wasde": _wasde,
}


def _required(row: Mapping[str, str | None], column: str) -> str:
    value = row[column]
    if value is None or value == "":
        raise ValueError(f"{column} is required")
    return value


def _decimal(value: str | None, field_name: str) -> Decimal:
    if value is None or value == "":
        raise ValueError(f"{field_name} is required")
    result = Decimal(value)
    if not result.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return result


def _optional_decimal(value: str | None, field_name: str) -> Decimal | None:
    if value is None or value == "":
        return None
    return _decimal(value, field_name)


def _date(value: str | None, field_name: str) -> date:
    if value is None or value == "":
        raise ValueError(f"{field_name} is required")
    return date.fromisoformat(value)


def _month_start(value: str) -> date:
    return date.fromisoformat(f"{value}-01")


def _month_end(value: str) -> date:
    year, month = (int(part) for part in value.split("-"))
    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1).replace(day=1) - date.resolution


def _commodity(value: str) -> str:
    lowered = value.lower()
    if "soybean oil" in lowered:
        return "soybean-oil"
    if "soybean meal" in lowered:
        return "soybean-meal"
    if "soy" in lowered:
        return "soybean"
    return _slug(value)


def _futures_unit(commodity: str) -> str:
    normalized = _commodity(commodity)
    if normalized == "soybean-meal":
        return "usd-short-ton"
    if normalized == "soybean-oil":
        return "cents-lb"
    return "cents-bu"


def _unit(value: str) -> str:
    normalized = value.strip().lower()
    replacements = {
        "brl/mt": "brl-mt",
        "usd/mt": "usd-mt",
        "zar/mt": "zar-mt",
        "mt": "mt",
        "1000 ha": "thousand-ha",
        "million acres": "million-acres",
        "million bushels": "million-bushels",
        "million metric tons": "million-mt",
        "percent": "percent",
    }
    return replacements.get(normalized, _slug(value))


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9._-]+", "-", value.strip().lower()).strip("-._")
    if not slug:
        raise ValueError("identity value cannot be blank")
    return slug


def _provenance(
    file_name: str,
    table: str,
    key: Mapping[str, str | None],
    row: Mapping[str, str | None],
) -> str:
    return json.dumps(
        {
            "file": file_name,
            "table": table,
            "key": _encoded_mapping(key),
            "row": _encoded_mapping(row),
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _encoded_mapping(values: Mapping[str, str | None]) -> dict[str, dict[str, str] | dict[str, None]]:
    return {key: _encoded_cell(values.get(key)) for key in sorted(values)}


def _encoded_cell(value: str | None) -> dict[str, str] | dict[str, None]:
    if value is None:
        return {"kind": "null"}
    if value == "":
        return {"kind": "blank"}
    return {"kind": "value", "value": value}
