"""What the public may see of one rostered layer's freshness (#567, #659).

One answer for every public surface that names a layer — the edition
manifest (``app.manifest``), the Layer Freshness table and masthead
(``scripts.generate_html``) and the MCP freshness tools — so the rule cannot
drift between them again.

- A **public** layer shows its ``data_freshness`` row as stored; no row
  means it never ran, which each surface words itself (``not-run``).
- A **private** layer (``config.PRIVATE_LAYERS``) shows its status and
  nothing else — no observation, stamp, age or row count — the same rule the
  run record applies (``pipeline.layer_runs.rows_for_run``). With no row,
  which is CI's shape (the layer runs only where ``MATIF_PRIVATE_FETCH`` is
  set), the status is the record's own word for it, ``skipped_unconfigured``:
  CI was not configured to ask, which is neither late nor failed
  (invariant 1). A private layer is never held against the public edition's
  on-schedule tally.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

import config
from pipeline.layer_runs import STATUS_SKIPPED_UNCONFIGURED

# The data_freshness columns that date a run. save_freshness stamps
# last_attempt on every write, so any row a private layer's run leaves
# carries at least one of them.
DATED_FRESHNESS_COLUMNS: tuple[str, ...] = (
    "last_success", "last_attempt", "observed_at",
    "fetch_started_at", "fetch_completed_at", "stored_at",
)


class PrivateLayerLeak(RuntimeError):
    """A publishing surface holds a dated freshness row for a private layer.

    A private layer runs only where its switch is set (``MATIF_PRIVATE_FETCH``:
    the desk, never CI). Its dated row in a database the history export or the
    MCP reads means the switch reached a publishing machine — a breach to
    report, never a row to blank (owner decision on #567, 2026-11-25).
    """


def assert_no_dated_private_rows(rows: Iterable[Mapping[str, Any]], *, where: str) -> None:
    """Refuse when any ``data_freshness`` row of a private layer carries a date."""
    leaked = sorted(
        str(row.get("layer_name")) for row in rows
        if is_private(str(row.get("layer_name")))
        and any(row.get(column) not in (None, "") for column in DATED_FRESHNESS_COLUMNS)
    )
    if leaked:
        raise PrivateLayerLeak(
            f"{where}: data_freshness holds a dated row for private layer(s) {leaked} "
            f"(config.PRIVATE_LAYERS={config.PRIVATE_LAYERS}). A private layer runs only on the "
            f"desk ({config.MATIF_FETCH_ENV}); its dates must not reach a public surface (#567)."
        )


def is_private(layer: str) -> bool:
    """True when the layer's output is the private desk edition only."""
    return layer in config.PRIVATE_LAYERS


def public_freshness(layer: str, row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """The part of ``row`` (the layer's ``data_freshness`` row, or None) the public may see.

    None only for a public layer with no row. A private layer always yields
    ``{"layer_name", "status"}`` and no other field.
    """
    if not is_private(layer):
        return None if row is None else dict(row)
    if row is None:
        return {"layer_name": layer, "status": STATUS_SKIPPED_UNCONFIGURED}
    # A row written before the status column existed reads "success", the
    # convention every freshness surface already applies to it.
    status = row.get("status")
    return {"layer_name": layer, "status": status if isinstance(status, str) and status else "success"}
