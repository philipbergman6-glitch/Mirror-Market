"""The edition manifest — what this build published, and how old it was.

Written to ``<output_dir>/manifest.json`` by ``scripts/generate_site.py`` on
every run, fast or full. It carries four things, and each has a named
consumer:

``edition``   which build this was and what it produced
    Read by ``trust.site_promotion.verify_refresh_is_not_a_regression`` to
    decide whether a candidate may replace the live edition.

``coverage``  every production layer's status and newest observation
    The regression gate's evidence. A fast refresh that ran against a
    database missing two thirds of its layers would render a thin but
    perfectly valid-looking site; nothing in the HTML says "this edition
    knows less than the one it is replacing". The coverage block does.

``reliability``  per-layer availability over the last 30 weekday sessions,
    restarted after any no-run gap longer than T1 tolerates (#533)
    ``pipeline.reliability.reliability`` over ``data/history/layer_runs.csv``
    (B34 #520). Read by the A11 re-audit (#315) from the edition itself.

``latency``   the measured chain per trader-critical layer
    ``latency.report.to_dict``. Published rather than kept internal because
    the whole point of the phase is that a reader can check the age of what
    they are pricing off, and a JSON they can fetch is a stronger claim than
    a rendered "3h ago" they have to trust.

The manifest is deliberately NOT in ``trust.site_promotion.expected_site_paths``:
that tuple is HTML pages whose internal links are crawled, and a JSON file in
it would fail the crawl. It is checked for existence by the fast-refresh gate
instead.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

MANIFEST_FILENAME = "manifest.json"

# Bumped when a consumer would misread an older manifest. The fast-refresh
# gate refuses to compare across versions rather than guessing.
MANIFEST_SCHEMA_VERSION = 1


def _pipeline_status() -> dict[str, Any]:
    """The last pipeline run's summary, or an empty dict.

    Read rather than recomputed: ``main.py`` already writes it, and a second
    opinion about which layers ran would be a second thing to keep in step.
    Absent is a legal state — a site generated without a pipeline run in
    front of it (the local dev loop) has no mode to report, and reports none
    rather than claiming "full".
    """
    import config as _config

    path = Path(_config.STORAGE_DIR) / "pipeline_status.json"
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        logger.info("no pipeline status at %s — manifest mode will be 'unknown'", path)
        return {}


def _coverage() -> list[dict[str, Any]]:
    """Every production layer's freshness, as the regression gate reads it."""
    from config import PRODUCTION_LAYER_KEYS
    from pipeline.query import read_freshness

    try:
        frame = read_freshness()
    except Exception:  # noqa: BLE001 — a manifest must never fail a build
        logger.warning("could not read freshness for the manifest", exc_info=True)
        return []

    rows = (
        frame.set_index("layer_name").to_dict("index")
        if not frame.empty and "layer_name" in frame.columns
        else {}
    )

    def _iso(value: Any) -> str | None:
        if value is None or value != value:  # NaT/NaN
            return None
        try:
            return value.isoformat()
        except AttributeError:
            return str(value)

    coverage = []
    for layer in PRODUCTION_LAYER_KEYS:
        row = rows.get(layer)
        if row is None:
            coverage.append({"layer": layer, "status": "not-run", "observed_at": None,
                             "last_success": None, "rows_fetched": None})
            continue
        coverage.append({
            "layer": layer,
            "status": str(row.get("status") or "success"),
            "observed_at": _iso(row.get("observed_at")),
            "last_success": _iso(row.get("last_success")),
            "rows_fetched": (
                int(row["rows_fetched"])
                if row.get("rows_fetched") is not None and row["rows_fetched"] == row["rows_fetched"]
                else None
            ),
        })
    return coverage


def _reliability(generated_at: datetime) -> dict[str, Any]:
    """The source reliability figures (B34 #520) over ``layer_runs.csv``.

    The window closes on the session *before* the one this edition serves:
    that session's own runs are still being recorded (this run's rows land
    after promotion), so including it would count a session as ``no_run``
    that is merely unfinished. The session is resolved from
    ``PIPELINE_STARTED_AT`` when the deploy workflow stamped it — the same
    instant the record step files this run under — so a build straddling the
    settlement cutoff cannot close its window on its own unrecorded session.
    """
    import os

    from pipeline.layer_runs import read_alerts, read_runs, session_date_for
    from pipeline.reliability import previous_session, reliability

    try:
        stamp = os.environ.get("PIPELINE_STARTED_AT")
        moment = datetime.fromisoformat(stamp.replace("Z", "+00:00")) if stamp else generated_at
        through = previous_session(session_date_for(moment))
        return reliability(read_runs(), read_alerts(), through=through)
    except Exception as exc:  # noqa: BLE001 — a manifest must never fail a build
        logger.warning("could not compute reliability for the manifest", exc_info=True)
        return {"state": "unavailable", "reason": f"{type(exc).__name__}: {exc}"}


def build_manifest(
    *,
    generated_at: datetime,
    pages: list[dict[str, Any]],
    generation_seconds: float | None = None,
) -> dict[str, Any]:
    """Assemble the manifest for one generated edition.

    ``pages`` is ``[{"name", "url", "ok"}]`` — the orchestrator's own page
    results, so a tombstone is visible here as well as in the HTML.
    """
    from latency.measure import measure
    from latency.report import to_dict as latency_to_dict

    status = _pipeline_status()
    now = datetime.now(timezone.utc)

    try:
        measurements = measure(generated_at=generated_at)
        latency_block = latency_to_dict(measurements, now)
    except Exception:  # noqa: BLE001 — a manifest must never fail a build
        logger.warning("could not measure latency for the manifest", exc_info=True)
        latency_block = {"measured_at": now.isoformat(), "objectives": {}, "layers": []}

    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "edition": {
            # "unknown" when no pipeline ran in front of this build. It is a
            # third state on purpose: calling it "full" would let a local dev
            # build pass a gate that exists to compare production editions.
            "mode": str(status.get("mode") or "unknown"),
            "generated_at": generated_at.isoformat(),
            "generation_seconds": generation_seconds,
            "layers_requested": list(status.get("layers_requested") or []),
            "critical_failures": list(status.get("critical_failures") or []),
            "pages": pages,
        },
        "coverage": _coverage(),
        "latency": latency_block,
        "reliability": _reliability(generated_at),
    }


def write_manifest(output_dir: Path, manifest: dict[str, Any]) -> Path:
    path = Path(output_dir) / MANIFEST_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    return path
