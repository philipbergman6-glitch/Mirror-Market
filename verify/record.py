"""The verification record: one schema, two uses (spec #639, default V5-Q3).

It is the verifier's output and the ledger page's only input, so the two
cannot drift apart. Fields are the spec's list, in its order, plus
``publisher`` (the key the licence rule and the page's attribution read) and
``withheld_reason``.

``None`` means "not learned / does not apply" and is written as JSON ``null``:
a pass has no disposition, a ``not checked`` record has no delta. A blank is
never a zero.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

from verify import licence

RESULTS = (
    "pass", "discrepancy", "metadata fail", "unverifiable", "not yet published", "not checked", "nothing to verify",
)
EVENT_CLASSES = ("ordinary", "contract_roll", "window_change", "fx_boundary", "pre_final_capture", "provider_revision")
KINDS = ("exact", "band", "recompute")
TRANSPORTS = ("same document, independent parser", "independent document")
DISPOSITIONS = ("explained", "unexplained")
_LEDGER_EDITION = re.compile(r"^L-\d{4}-\d{2}-\d{2}-\d+$")
_VERIFIER = re.compile(r"^(verify/[\w/]+\.py@[0-9a-f]{7,40}(\+dirty|\+tree-unchecked)?|human \([A-Z]{2,4}\))$")


class RecordError(ValueError):
    """A record that would be wrong, or would publish what it must not."""


@dataclass(frozen=True)
class Record:
    ledger_edition: str | None
    site_edition: str | None
    page: str
    page_sha256: str | None
    stratum: str
    series: str
    displayed_value: str | None
    displayed_unit: str | None
    observation_date: str | None
    contract_or_window: str | None
    fx_date: str | None
    event_class: str
    tier: str
    kind: str | None
    source_identity: str
    source_url: str | None
    source_vintage: str | None
    transport: str | None
    source_value: float | None
    source_unit: str | None
    conversion: str | None
    delta_native: float | None
    delta_usd_mt: float | None
    tolerance: str | None
    threshold: float | None
    band: str | None
    tolerance_version: str
    result: str
    material: bool | None
    disposition: str | None
    explanation: str | None
    owner: str | None
    issue: str | None
    verifier: str
    verified_at: str
    publisher: str | None
    withheld_reason: str | None = None


def _validate(record: Record) -> None:
    def require(ok: bool, why: str) -> None:
        if not ok:
            raise RecordError(f"{record.series} on {record.page}: {why}")

    require(record.result in RESULTS, f"result {record.result!r} is not one of {RESULTS}")
    require(record.event_class in EVENT_CLASSES, f"event_class {record.event_class!r} is not one of {EVENT_CLASSES}")
    require(record.kind is None or record.kind in KINDS, f"kind {record.kind!r} is not one of {KINDS}")
    require(record.transport is None or record.transport in TRANSPORTS, f"transport {record.transport!r}")
    require(record.disposition is None or record.disposition in DISPOSITIONS, f"disposition {record.disposition!r}")
    require(record.ledger_edition is None or bool(_LEDGER_EDITION.match(record.ledger_edition)),
            f"ledger_edition {record.ledger_edition!r} is not L-YYYY-MM-DD-n")
    require(bool(_VERIFIER.match(record.verifier)), f"verifier {record.verifier!r} is not <script>@<sha> or human (XX)")
    require(bool(record.tolerance_version), "no tolerance_version")
    if record.result in ("discrepancy", "metadata fail"):
        require(record.material is not None, "a discrepancy must say whether it is material")
        require(record.disposition is not None, "a discrepancy must say explained or unexplained")
    if record.result == "metadata fail":
        require(record.material is True, "a metadata fail is material at any delta")
    if record.result == "pass":
        require(record.material is False, "a pass is not material")
    if record.result in ("not checked", "unverifiable", "not yet published", "nothing to verify"):
        require(bool(record.explanation), f"a {record.result!r} record must carry its reason")
    if record.disposition == "unexplained":
        require(record.explanation is not None, "an unexplained record still says what was found")


def committed(record: Record) -> dict[str, Any]:
    """The record as it may be written to the public repo, licence rule applied."""
    _validate(record)
    withheld: list[str] = []
    out = record
    if record.publisher is not None and not licence.terms(record.publisher).show_source_value:
        if record.source_value is not None:
            _refuse_leak(record, record.source_value)
        out = replace(out, source_value=None)
        withheld.append(licence.WITHHELD_CAPTION)
    if record.stratum in licence.RESTRICTED_DISPLAYED_STRATA and record.displayed_value is not None:
        _refuse_leak(record, float(record.displayed_value.replace(",", "")), record.displayed_value)
        out = replace(out, displayed_value=None)
        withheld.append(licence.DISPLAYED_WITHHELD)
    if withheld:
        out = replace(out, withheld_reason=" ".join(withheld))
    return asdict(out)


def _refuse_leak(record: Record, value: float, shown: str | None = None) -> None:
    """A withheld value must not survive in a free-text field either."""
    forms = {f"{value:g}", f"{value:.2f}", f"{value:.4f}", f"{value:,.2f}"} | ({shown} if shown else set())
    for field in ("conversion", "explanation", "source_vintage", "tolerance"):
        text = getattr(record, field) or ""
        leaked = [form for form in forms if form in text]
        if leaked:
            raise RecordError(
                f"{record.series}: field {field!r} repeats a withheld value ({leaked[0]})"
            )


def write(records: list[Record], directory: Path, name: str) -> Path:
    """Append-only: one new JSONL file per run, never an existing one rewritten."""
    if not records:
        raise RecordError("no records to write")
    lines = [json.dumps(committed(record), ensure_ascii=False, sort_keys=False) for record in records]
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{name}.jsonl"
    with path.open("x", encoding="utf-8") as handle:  # "x": an existing record file is never overwritten
        handle.write("\n".join(lines) + "\n")
    return path


def read(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
