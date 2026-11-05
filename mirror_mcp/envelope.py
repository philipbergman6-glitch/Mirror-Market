"""One envelope for every tool: ``{state, reason, data, meta}``.

``state``/``reason``/``data`` are the repo's block vocabulary (``app.blocks``:
ok / empty / absent); a non-ok state always carries a reason. ``meta`` adds
what a reader needs to quote a number honestly: the price type and its caveat,
the confidence ceiling derived from it (never asserted), and the claim words
that type permits.
"""

from __future__ import annotations

from typing import Any

from app.blocks import STATE_OK, STATES
from pricing import policy, semantics


def meta_for(quote_kind: str | None, **extra: Any) -> dict[str, Any]:
    """Price-semantics fields for a block whose source prints ``quote_kind``.

    ``None`` for every field when there is no price quote kind: a weather or
    freshness payload has no price type, and the reader must not infer one.
    """
    out: dict[str, Any] = {
        "price_type": None, "price_type_label": None, "caveat": None,
        "confidence": None, "permitted_claims": [],
    }
    if quote_kind and semantics.is_price_quote_kind(quote_kind):
        price_type = semantics.price_type_for_quote_kind(quote_kind)
        out.update({
            "price_type": price_type.value,
            "price_type_label": price_type.label,
            "caveat": price_type.caveat,
            "confidence": semantics.confidence_for_quote_kind(quote_kind).value,
            "permitted_claims": sorted(k.value for k in policy.permitted_claims((price_type,))),
        })
    out.update(extra)
    return out


def envelope(state: str, reason: str = "", data: Any = None, *, meta: dict[str, Any]) -> dict[str, Any]:
    if state not in STATES:
        raise ValueError(f"state must be one of {STATES}, got {state!r}")
    if state != STATE_OK and not reason:
        raise ValueError(f"a {state!r} envelope must carry a reason")
    return {"state": state, "reason": reason, "data": data, "meta": meta}
