"""Z-score helpers for converting raw observations into deviations from a baseline.

Used by COT positioning and weather anomalies — both ask "how unusual is the
current reading vs its own history?" rather than the raw number. Callers slice
the baseline themselves (trailing window, same-calendar-period, etc.) and pass
it in along with the latest observation.
"""

from __future__ import annotations

import math

import pandas as pd

DEFAULT_MIN_OBSERVATIONS = 26


def zscore(
    latest: float,
    baseline: pd.Series,
    *,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
) -> float | None:
    """Return (latest - mean) / std of `baseline`.

    Returns None when:
    - `latest` is NaN
    - `baseline` has fewer than `min_observations` non-NaN values
    - standard deviation is zero or NaN (no variance to normalize against)

    The caller is responsible for excluding `latest` from `baseline` if it
    shouldn't contribute to the mean/std (typically yes — comparing a point
    to a distribution that includes itself biases the score toward zero).
    """
    if latest is None or (isinstance(latest, float) and math.isnan(latest)):
        return None

    clean = baseline.dropna()
    if len(clean) < min_observations:
        return None

    std = float(clean.std())
    if std == 0 or math.isnan(std):
        return None

    return (float(latest) - float(clean.mean())) / std


def format_zscore(z: float | None) -> str:
    """Render a z-score as '+2.3σ' / '-1.4σ'. Returns '' when z is None."""
    if z is None:
        return ""
    sign = "+" if z >= 0 else ""
    return f"{sign}{z:.1f}σ"
