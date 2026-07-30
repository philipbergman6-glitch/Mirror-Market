"""Data freshness warnings — shown at the top of the briefing."""

import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

from config import FRESHNESS_WARNING_DAYS, FRESHNESS_WARNING_DAYS_BY_LAYER
from pipeline.query import read_freshness

logger = logging.getLogger(__name__)


def format() -> str:  # noqa: A001 — module-scope name, no conflict with builtin
    """Stale-layer warnings + per-commodity health summary."""
    sections = []

    freshness = read_freshness()
    layer_warnings = []
    if not freshness.empty:
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        for _, row in freshness.iterrows():
            layer = row["layer_name"]
            # Layers publish on different cadences — weekly COT being 6 days
            # old is by design, not an outage.
            limit_days = FRESHNESS_WARNING_DAYS_BY_LAYER.get(
                layer, FRESHNESS_WARNING_DAYS
            )
            threshold = timedelta(days=limit_days)
            last = row["last_success"]
            if pd.notna(last):
                age = now - last
                if age > threshold:
                    days_old = age.days
                    layer_warnings.append(
                        f"  WARNING: {layer} data is {days_old} days old"
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
