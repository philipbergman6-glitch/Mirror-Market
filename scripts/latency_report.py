"""Print the measured latency of every trader-critical layer.

    python scripts/latency_report.py                  # text, from the live DB
    python scripts/latency_report.py --json           # the manifest's latency block
    python scripts/latency_report.py --manifest URL   # read a published edition
    python scripts/latency_report.py --fail-on-breach # exit 1 if any layer misses
    python scripts/latency_report.py --fail-on-breach-layers prices,currencies
                                                      # exit 1 only for these
    python scripts/latency_report.py --fail-on-breach-layers prices \
        --late-start-is-not-a-breach                  # a scheduler-late start
                                                      # is reported, not failed

``--manifest`` is how the *product's* latency is measured rather than the
pipeline's: it reads a published ``manifest.json``, local path or URL, and
adds the one interval no process can observe from inside itself — generation
to public availability — by treating the moment the file could be fetched as
the moment it was readable.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from latency.domain import (  # noqa: E402
    LAYER_LATENCY_BY_KEY,
    Verdict,
    format_delta,
    started_after_objective,
)
from latency.measure import measure  # noqa: E402
from latency.report import to_dict, to_text  # noqa: E402


def _load_manifest(source: str) -> dict:
    if source.startswith(("http://", "https://")):
        with urlopen(source, timeout=30) as response:  # noqa: S310 — explicit target
            return json.loads(response.read().decode("utf-8"))
    return json.loads(Path(source).read_text(encoding="utf-8"))


def _parse_instant(raw: str | None) -> datetime | None:
    return datetime.fromisoformat(raw) if raw else None


def _late_starts(rows: list[dict]) -> set[str]:
    """Breaching layers whose fetch began after the objective had already lapsed.

    Only an acquisition breach on the newest bar qualifies, and never one whose
    pipeline leg (ours) breached — see ``latency.domain.started_after_objective``.
    A published manifest carries the pipeline leg as ``unknown``: it is written
    before the deploy it describes, so publication cannot be stamped in it.
    """
    late: set[str] = set()
    for row in rows:
        if row.get("verdict") != Verdict.BREACHES.value:
            continue
        if row.get("acquisition_verdict") != Verdict.BREACHES.value:
            continue
        if row.get("pipeline_verdict") == Verdict.BREACHES.value:
            continue
        spec = LAYER_LATENCY_BY_KEY.get(row["layer"])
        if spec is not None and started_after_objective(
            spec,
            _parse_instant(row.get("observed_at")),
            _parse_instant(row.get("fetch_started_at")),
        ):
            late.add(row["layer"])
    return late


def _report_from_manifest(source: str, as_json: bool) -> tuple[str, list[dict]]:
    """Render a published edition's latency, with publication measured.

    The fetch itself is the measurement: the file answered, so the edition was
    publicly readable at this instant. That makes ``generated_at -> now`` an
    upper bound on our publication delay rather than an exact figure — it
    includes however long the reader waited before asking. Reported as the
    bound it is.
    """
    fetched_at = datetime.now(timezone.utc)
    manifest = _load_manifest(source)
    block = manifest.get("latency", {})
    generated_raw = manifest.get("edition", {}).get("generated_at")
    rows = list(block.get("layers", []))

    if as_json:
        block = dict(block)
        block["public_readable_at"] = fetched_at.isoformat()
        return json.dumps(block, indent=2), rows

    lines = [f"published edition: {source}"]
    if generated_raw:
        generated = datetime.fromisoformat(generated_raw)
        lines.append(f"generated at      {generated.isoformat()}")
        lines.append(f"readable at       {fetched_at.isoformat()}")
        lines.append(
            "generation -> publicly readable: <= "
            f"{format_delta(fetched_at - generated)} (upper bound — includes the "
            "delay before this check asked)"
        )
    lines.append(f"edition mode      {manifest.get('edition', {}).get('mode')}")
    lines.append("")
    for row in block.get("layers", []):
        lines.append(
            f"{row['layer']:<20}{row['class']:<18}"
            f"age={format_delta(_td(row.get('age_s'))):>10}  "
            f"acquisition={format_delta(_td(row.get('acquisition_s'))):>10}  "
            f"{row.get('verdict')}"
        )
    # A venue holiday is not a breach and does not fail the gate, but it is
    # said out loud with its source so a quiet week is never mistaken for a
    # green one (#399).
    for row in block.get("layers", []):
        if row.get("verdict") == Verdict.CLOSED.value:
            lines.append(
                f"venue closed: {row['layer']} — "
                f"{row.get('closure_basis') or 'calendar basis missing from manifest'}"
            )
    return "\n".join(lines), rows


def _td(seconds):
    from datetime import timedelta

    return None if seconds is None else timedelta(seconds=seconds)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    parser.add_argument(
        "--manifest",
        help="path or URL of a published manifest.json to measure instead of the live DB",
    )
    parser.add_argument(
        "--fail-on-breach",
        action="store_true",
        help="exit 1 when any layer breaches its objective",
    )
    parser.add_argument(
        "--fail-on-breach-layers",
        help=(
            "comma-separated layer names: exit 1 only when one of THESE "
            "breaches. This is the CI gate for a scheduled run, scoped to the "
            "layers that run just fetched — an unscoped gate would be red "
            "every day on layers whose acquisition breaches structurally "
            "(COT carries a 3-day provider delay against a 24h target; the "
            "daily build re-stamps dce 13-17h after the Dalian close)."
        ),
    )
    parser.add_argument(
        "--late-start-is-not-a-breach",
        action="store_true",
        help=(
            "with --fail-on-breach-layers: a gated breach whose fetch began after "
            "the objective had already lapsed, on the newest bar, with our own "
            "pipeline leg not breached, is reported as LATE START instead of failing. For "
            "the slot whose lateness is GitHub's scheduler alone — never for the "
            "08:00 DCE backstop, whose red is the alarm that the outside trigger "
            "did not fire."
        ),
    )
    args = parser.parse_args(argv)
    gate_layers = (
        {name.strip() for name in args.fail_on_breach_layers.split(",") if name.strip()}
        if args.fail_on_breach_layers
        else None
    )
    if gate_layers is not None:
        from latency.domain import TRADER_CRITICAL_LAYERS

        unknown = gate_layers.difference(TRADER_CRITICAL_LAYERS)
        if unknown:
            # A typo'd gate layer would otherwise be a gate that never fires.
            raise SystemExit(
                f"unknown gate layer(s): {', '.join(sorted(unknown))}. "
                f"Known: {', '.join(TRADER_CRITICAL_LAYERS)}"
            )

    def _exit_code(rows: list[dict]) -> int:
        breaching_layers = [
            row["layer"] for row in rows if row.get("verdict") == Verdict.BREACHES.value
        ]
        if args.fail_on_breach and breaching_layers:
            return 1
        if gate_layers is not None:
            gated = sorted(gate_layers.intersection(breaching_layers))
            if args.late_start_is_not_a_breach:
                late = _late_starts([row for row in rows if row["layer"] in gated])
                for layer in sorted(late):
                    # Said out loud, never silent: the edition WAS late; it is
                    # the cause, not the fact, that this run cannot change.
                    print(
                        f"LATE START: {layer} — the fetch began after its objective "
                        "had lapsed on the newest bar; GitHub's scheduler start "
                        "delay, not the data or the pipeline"
                    )
                gated = [layer for layer in gated if layer not in late]
            if gated:
                print(f"BREACH in gated layer(s): {', '.join(gated)}", file=sys.stderr)
                return 1
        return 0

    if args.manifest:
        output, rows = _report_from_manifest(args.manifest, args.json)
        print(output)
        return _exit_code(rows)

    now = datetime.now(timezone.utc)
    measurements = measure()
    report = to_dict(measurements, now)
    print(json.dumps(report, indent=2) if args.json else to_text(measurements, now))
    return _exit_code(report["layers"])


if __name__ == "__main__":
    raise SystemExit(main())
