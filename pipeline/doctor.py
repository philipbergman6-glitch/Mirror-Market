"""Setup doctor: which API keys are set, and what each missing one costs (#354).

``python main.py --doctor`` runs this and nothing else — no fetch, no DB,
no site. It answers the question invariant 1 raised and nothing recorded:
a key-gated layer whose key is unset is logged and returns ``False``, and the
startup warning in ``main.run`` scrolls past at the top of a 39-layer log.
DATA_GOV_IN_API_KEY was absent for weeks without anyone noticing.

The catalog is ``config.API_KEY_CATALOG``; this module only reads it and the
environment. A key's *presence* is all it ever reads — ``bool(os.getenv)`` —
so there is no code path through which a value could reach the output, and
``tests/test_doctor.py`` sets sentinel values to prove it.

Exit status is the deploy contract: 1 when any ``required_in_ci`` key is
missing, 0 otherwise. A missing optional key (MARS) is reported in full but
is not fatal, because the layer it gates is designed to withhold with a
reason rather than break (LAYERS.md → API keys).
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TextIO

from config import API_KEY_CATALOG, ENV_FILE, ENV_FILE_LOADED, LAYER_NUMBERS, ApiKeySpec

__all__ = ["KeyReport", "diagnose", "render", "run_doctor"]

# Pattern borrowed from God's Eye View's `npm run doctor` (live-tested
# 2026-10-05): one line per dependency, a verdict word in a fixed column,
# the consequence on the same line, the signup pointer under it.
_MISSING = "MISSING"
_SET = "set"


@dataclass(frozen=True)
class KeyReport:
    """One catalog entry against the environment. Carries no value, by construction."""

    name: str
    present: bool
    spec: ApiKeySpec


def diagnose(environ: Mapping[str, str] | None = None) -> list[KeyReport]:
    """Every catalogued key, in catalog order, set or missing.

    An empty string is missing: ``os.getenv`` returns it for an exported-but-
    blank variable, and every fetcher's own check is ``if not KEY``, so the
    doctor must agree with them.
    """
    env = os.environ if environ is None else environ
    return [
        KeyReport(name=name, present=bool(env.get(name)), spec=spec)
        for name, spec in API_KEY_CATALOG.items()
    ]


def _layer_line(keys: tuple[str, ...]) -> str:
    return ", ".join(f"Layer {LAYER_NUMBERS[key]} ({key})" for key in keys)


def _consequence(spec: ApiKeySpec) -> list[str]:
    """What a missing key does, one line per distinct outcome — never merged.

    ``skipped`` and ``failed`` are two states (invariant 1): a skipped layer
    never ran and writes no freshness row; a degraded one runs, fetches
    nothing, and is graded as failed. The reader needs to know which rows
    to expect on the health page.
    """
    lines: list[str] = []
    if spec.skips:
        lines.append(f"skipped, no freshness row: {_layer_line(spec.skips)}")
    if spec.degrades:
        lines.append(f"runs and grades as failed: {_layer_line(spec.degrades)}")
    if spec.also:
        lines.append(f"also: {spec.also}")
    return lines


def render(reports: list[KeyReport]) -> str:
    missing = [r for r in reports if not r.present]
    fatal = [r for r in missing if r.spec.required_in_ci]
    width = max(len(r.name) for r in reports)

    env_line = (
        f"env file: loaded {ENV_FILE}" if ENV_FILE_LOADED
        else f"env file: none at {ENV_FILE} (keys come from the environment only)"
    )
    out = ["Mirror Market setup doctor — API keys", env_line, ""]
    for r in reports:
        verdict = _SET if r.present else _MISSING
        scope = "CI-required" if r.spec.required_in_ci else "optional   "
        out.append(f"  {r.name:<{width}}  {verdict:<7}  {scope}  {r.spec.unlocks}")
        if r.present:
            continue
        for line in _consequence(r.spec):
            out.append(f"  {'':<{width}}           {line}")
        out.append(f"  {'':<{width}}           get one: {r.spec.signup}")
    out.append("")
    out.append(
        f"{len(reports) - len(missing)} of {len(reports)} keys set; "
        f"{len(missing)} missing, {len(fatal)} of them CI-required."
    )
    if fatal:
        out.append(
            f"The daily deploy cannot run complete without: {', '.join(r.name for r in fatal)}. "
            "Set them in the environment or in .env (never committed)."
        )
    return "\n".join(out) + "\n"


def run_doctor(stream: TextIO | None = None) -> int:
    """Print the report; return 1 if a CI-required key is missing, else 0."""
    reports = diagnose()
    (sys.stdout if stream is None else stream).write(render(reports))
    return 1 if any(not r.present and r.spec.required_in_ci for r in reports) else 0
