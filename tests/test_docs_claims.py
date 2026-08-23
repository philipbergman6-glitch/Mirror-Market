"""Contract tests for the claims the project documents make about itself (A0 #297).

Two claims drifted far enough to be findings in `AUDIT-2026-08-22.md`:

1. **The mission.** `CLAUDE.md` promised a "Bloomberg-terminal-style" platform —
   a claim the audit graded against and failed, because the product is not an
   execution terminal and does not intend to become one. Map #296 narrowed it.
   The narrowed claim is canonical, so it is pinned here verbatim: a session
   that reads `CLAUDE.md` to orient must not orient to the rejected claim.

2. **The layer count.** Docs said 29, `config.PRODUCTION_LAYERS` said 31, and
   nothing connected the two, so the prose drifted every time a layer landed.
   These tests derive both counts from the registry, which makes the registry
   the single authority the ticket asked for — the docs cannot lag it silently
   again.

These are cheap greps, not a doc linter. They pin the two specific numbers and
the one specific sentence that were wrong; they do not police prose.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import config

REPO = Path(__file__).resolve().parent.parent

# The canonical mission sentence (map #296 Destination). Verbatim — if the
# claim is ever renarrowed, this constant and CLAUDE.md move together.
CANONICAL_MISSION = (
    "Public-data soy intelligence and private desk decision support for "
    "physical buyers making daily cargo, basis, origin, and hedge decisions."
)

# Docs that make counted claims about the layer roster.
COUNTED_DOCS = ("CLAUDE.md", "LAYERS.md", "LATENCY.md", "DESIGN.md", "README.md")


def _operational_layer_count() -> int:
    return len(config.PRODUCTION_LAYERS)


def _numbered_group_count() -> int:
    """Distinct numbered groups: ``15`` and ``15b`` are one group, two layers."""
    return len({re.sub(r"[a-z]+$", "", number) for _, number, *_ in config.PRODUCTION_LAYERS})


def _read(name: str) -> str:
    return (REPO / name).read_text(encoding="utf-8")


def test_claude_md_states_the_canonical_mission() -> None:
    assert CANONICAL_MISSION in _read("CLAUDE.md")


def test_claude_md_does_not_claim_a_bloomberg_terminal() -> None:
    """The rejected claim may be *named* as rejected, never asserted.

    `CLAUDE.md` is the orientation document; the expansion path lives in
    `ROADMAP.md`, where the Bloomberg-class ambition is allowed to appear
    because it is explicitly labelled as a commercially-triggered future.
    """
    assert "Bloomberg-terminal-style" not in _read("CLAUDE.md")


def test_claude_md_disclaims_what_the_product_is_not() -> None:
    claude = _read("CLAUDE.md").lower()
    for disclaimed in ("execution terminal", "real-time", "ctrm", "settlement"):
        assert disclaimed in claude, f"CLAUDE.md no longer disclaims {disclaimed!r}"


@pytest.mark.parametrize("doc", COUNTED_DOCS)
def test_no_doc_states_a_stale_layer_count(doc: str) -> None:
    """Any claim about the *whole roster* must be the registry's count.

    Catches the exact drift the audit found (29 against a 31-layer registry)
    without forbidding the sentence itself — and without catching the honest
    subset counts, which are the reason this is not a bare "\\d+ layers" grep:
    ``LATENCY.md`` legitimately says "Fast pipeline (3 layers...)". A roster
    claim announces itself with "all", "operational" or "data".
    """
    expected = _operational_layer_count()
    roster_claim = re.compile(r"\b(?:all\s+)?(\d+)\s+(?:operational\s+)?(?:data\s+)?layers\b")
    for match in roster_claim.finditer(_read(doc)):
        if not re.match(r"all\s|\d+\s+(?:operational|data)\s", match.group(0)):
            continue  # a subset count ("3 layers"), not a claim about the roster
        assert int(match.group(1)) == expected, (
            f"{doc} claims {match.group(0)!r}; "
            f"config.PRODUCTION_LAYERS holds {expected} layers"
        )


@pytest.mark.parametrize("doc", COUNTED_DOCS)
def test_no_doc_states_a_stale_group_count(doc: str) -> None:
    expected = _numbered_group_count()
    for match in re.finditer(r"(\d+)\s+numbered\s+(?:source\s+)?groups", _read(doc)):
        assert int(match.group(1)) == expected, (
            f"{doc} claims {match.group(0)!r}; "
            f"config.PRODUCTION_LAYERS holds {expected} numbered groups"
        )


def test_registry_counts_are_what_the_docs_were_corrected_to() -> None:
    """A tripwire on the corrected numbers themselves.

    The parametrized tests above only prove docs and registry agree; if a layer
    were deleted they would agree on a new number and stay green while every
    doc silently rewrote itself. This one fails loudly on a roster change, so
    the change is a deliberate doc edit rather than a passive one.
    """
    assert _operational_layer_count() == 31
    assert _numbered_group_count() == 28
