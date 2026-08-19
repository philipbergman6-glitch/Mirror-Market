"""Print the measured latency of every trader-critical layer.

    python scripts/latency_report.py                  # text, from the live DB
    python scripts/latency_report.py --json           # the manifest's latency block
    python scripts/latency_report.py --manifest URL   # read a published edition
    python scripts/latency_report.py --fail-on-breach # exit 1 if any layer misses

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

from latency.domain import Verdict, format_delta  # noqa: E402
from latency.measure import measure  # noqa: E402
from latency.report import to_dict, to_text  # noqa: E402


def _load_manifest(source: str) -> dict:
    if source.startswith(("http://", "https://")):
        with urlopen(source, timeout=30) as response:  # noqa: S310 — explicit target
            return json.loads(response.read().decode("utf-8"))
    return json.loads(Path(source).read_text(encoding="utf-8"))


def _report_from_manifest(source: str, as_json: bool) -> tuple[str, list[str]]:
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
    breaching = [
        row["layer"] for row in block.get("layers", [])
        if row.get("verdict") == Verdict.BREACHES.value
    ]

    if as_json:
        block = dict(block)
        block["public_readable_at"] = fetched_at.isoformat()
        return json.dumps(block, indent=2), breaching

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
    return "\n".join(lines), breaching


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
    args = parser.parse_args(argv)

    if args.manifest:
        output, breaching = _report_from_manifest(args.manifest, args.json)
        print(output)
        return 1 if (args.fail_on_breach and breaching) else 0

    now = datetime.now(timezone.utc)
    measurements = measure()
    print(json.dumps(to_dict(measurements, now), indent=2) if args.json
          else to_text(measurements, now))
    breaching = [m for m in measurements if m.verdict is Verdict.BREACHES]
    return 1 if (args.fail_on_breach and breaching) else 0


if __name__ == "__main__":
    raise SystemExit(main())
