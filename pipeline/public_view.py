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

from collections.abc import Mapping
from typing import Any

import config
from pipeline.layer_runs import STATUS_SKIPPED_UNCONFIGURED


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
