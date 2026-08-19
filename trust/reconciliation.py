"""Field-level reconciliation between the v1 pipeline and the trusted path.

Dual-write is temporary, and the only thing that makes it safe to end is a
report that accounts for *every* row and *every* field on which the two paths
disagree. A summary count would hide the case that matters: the same number of
rows on both sides, one of them wrong.

The comparison is deliberately dumb. It takes two dataframes already put in the
same shape by the dataset's own adapter, a key that identifies a row, and the
value columns that must agree. Anything cleverer — tolerance windows, fuzzy key
matching, "close enough" on a price — would be the reconciler deciding that a
divergence is acceptable, which is the reviewer's decision and not this
module's.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class ReconciliationReport:
    """What the two paths made of one payload, and where they disagreed."""

    matched_rows: int
    missing_in_trusted: tuple[Mapping[str, object], ...]
    missing_in_legacy: tuple[Mapping[str, object], ...]
    field_differences: tuple[Mapping[str, object], ...]

    @property
    def reconciled(self) -> bool:
        return not self.missing_in_trusted and not self.missing_in_legacy and not self.field_differences

    def to_dict(self) -> dict[str, object]:
        return {
            "reconciled": self.reconciled,
            "matched_rows": self.matched_rows,
            "missing_in_trusted": [dict(row) for row in self.missing_in_trusted],
            "missing_in_legacy": [dict(row) for row in self.missing_in_legacy],
            "field_differences": [dict(diff) for diff in self.field_differences],
        }


def reconcile_frames(
    legacy: pd.DataFrame,
    trusted: pd.DataFrame,
    *,
    key_columns: Sequence[str],
    value_columns: Sequence[str],
    text_columns: Sequence[str] = (),
    float_places: int = 8,
) -> ReconciliationReport:
    """Account for every row and field difference between two shaped frames.

    ``text_columns`` are normalized to strings on both sides before comparison:
    the v1 path builds its frames from a parser and the trusted path rebuilds
    them from the ledger, so a date can arrive as a ``date`` on one side and an
    ISO string on the other. That is a serialization difference, not a
    divergence, and reporting it as one would bury the real ones.
    """

    key_cols = tuple(key_columns)
    value_cols = tuple(value_columns)
    if not key_cols:
        raise ValueError("reconciliation requires at least one key column")
    if not value_cols:
        raise ValueError("reconciliation requires at least one value column")

    legacy_map = _frame_map(legacy, key_cols, text_columns)
    trusted_map = _frame_map(trusted, key_cols, text_columns)
    legacy_keys = set(legacy_map)
    trusted_keys = set(trusted_map)

    missing_in_trusted = tuple(legacy_map[key] for key in sorted(legacy_keys - trusted_keys))
    missing_in_legacy = tuple(trusted_map[key] for key in sorted(trusted_keys - legacy_keys))
    differences: list[Mapping[str, object]] = []
    for key in sorted(legacy_keys & trusted_keys):
        legacy_row = legacy_map[key]
        trusted_row = trusted_map[key]
        for column in value_cols:
            legacy_value = legacy_row.get(column)
            trusted_value = trusted_row.get(column)
            if _comparable(legacy_value, float_places) != _comparable(trusted_value, float_places):
                differences.append(
                    {
                        "key": dict(zip(key_cols, key, strict=True)),
                        "field": column,
                        "legacy": legacy_value,
                        "trusted": trusted_value,
                    }
                )
    return ReconciliationReport(
        matched_rows=len(legacy_keys & trusted_keys),
        missing_in_trusted=missing_in_trusted,
        missing_in_legacy=missing_in_legacy,
        field_differences=tuple(differences),
    )


def _frame_map(
    frame: pd.DataFrame,
    key_columns: Sequence[str],
    text_columns: Sequence[str],
) -> dict[tuple[object, ...], Mapping[str, object]]:
    if frame.empty:
        return {}
    normalized = frame.copy()
    for column in text_columns:
        if column in normalized.columns:
            normalized[column] = normalized[column].astype(str)
    rows: dict[tuple[object, ...], Mapping[str, object]] = {}
    for record in normalized.to_dict("records"):
        row = {str(column): value for column, value in record.items()}
        key = tuple(row[column] for column in key_columns)
        rows[key] = row
    return rows


def _comparable(value: object, float_places: int) -> object:
    if isinstance(value, float):
        return round(value, float_places)
    return value


__all__ = ["ReconciliationReport", "reconcile_frames"]
