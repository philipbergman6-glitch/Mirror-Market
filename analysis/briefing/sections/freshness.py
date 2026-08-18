"""Data freshness warnings — shown at the top of the briefing."""

import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

from config import freshness_limit_days
from pipeline.query import read_freshness

logger = logging.getLogger(__name__)


def _coverage(row) -> tuple[int, int] | None:
    """(keys_returned, keys_expected) when a layer ran below full coverage.

    None when coverage is undefined (NULL — a layer with no key catalog, or
    a run that died before it had a payload) or full. Rendering full
    coverage on every healthy layer every day is the badge-blindness the
    per-layer-cadence fix removed; only the degraded case is worth a line.
    """
    returned, expected = row.get("keys_returned"), row.get("keys_expected")
    if pd.isna(returned) or pd.isna(expected) or not expected:
        return None
    returned, expected = int(returned), int(expected)
    return (returned, expected) if returned < expected else None


def format() -> str:  # noqa: A001 — module-scope name, no conflict with builtin
    """Stale-layer warnings + per-commodity health summary."""
    sections = []

    freshness = read_freshness()
    layer_warnings = []
    if not freshness.empty:
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        for _, row in freshness.iterrows():
            layer = row["layer_name"]
            status = row.get("status")
            status = status if isinstance(status, str) else "success"
            last = row["last_success"]

            # An intentionally disabled layer is neither fresh nor an outage.
            # The dashboard gives it its own bucket and excludes it from the
            # counts; the briefing stays silent about it for the same reason —
            # warning every day about a layer we chose to switch off trains
            # the reader to skip this whole block.
            if status == "disabled":
                continue

            # Key coverage describes, it never grades (#182): the line
            # explains a verdict reached elsewhere, so it reads WARNING
            # alongside a failure and NOTE on a layer that still passed.
            coverage = _coverage(row)
            if coverage:
                returned, expected = coverage
                level = "WARNING" if status in {"failed", "stale", "incomplete"} else "NOTE"
                layer_warnings.append(
                    f"  {level}: {layer} returned {returned} of {expected} keys"
                )

            # A failed latest attempt means the briefing is showing whatever
            # the last good fetch left behind — flag it even if that data is
            # still inside the staleness window (dashboard path already does).
            if status in {"failed", "stale", "incomplete"}:
                label = {
                    "failed": "UPSTREAM FAILED",
                    "stale": "STALE LAST-KNOWN-GOOD",
                    "incomplete": "INCOMPLETE KEY COVERAGE",
                }[status]
                if pd.notna(last):
                    days_old = (now - last).days
                    layer_warnings.append(
                        f"  WARNING: {layer} {label} — "
                        f"showing data from {days_old} days ago"
                    )
                else:
                    layer_warnings.append(
                        f"  WARNING: {layer} {label} — no recorded success"
                    )
                continue

            if status == "no_publication":
                layer_warnings.append(
                    f"  NOTE: {layer} had a legitimate no-publication run"
                )
                continue

            # Layers publish on different cadences — weekly COT being 6 days
            # old is by design, not an outage.
            threshold = timedelta(days=freshness_limit_days(layer))
            if pd.notna(last):
                age = now - last
                if age > threshold:
                    days_old = age.days
                    layer_warnings.append(
                        f"  WARNING: {layer} data is {days_old} days old"
                    )
            else:
                # No successful fetch on record and the last attempt did not
                # fail — an empty layer that never produced data. Silent
                # before; it is the one case the dashboard also renders "never".
                layer_warnings.append(
                    f"  WARNING: {layer} has never fetched successfully"
                )

    if layer_warnings:
        sections.append("DATA FRESHNESS WARNINGS:\n" + "\n".join(layer_warnings))

    try:
        from analysis.health import run_health_check
        health = run_health_check()
        if health["issues"]:
            sections.append(health["summary"])
    except Exception:
        logger.warning("Health check unavailable for briefing", exc_info=True)

    return "\n\n".join(sections)
