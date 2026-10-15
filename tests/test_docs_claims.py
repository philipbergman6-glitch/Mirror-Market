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
def test_no_doc_states_a_stale_hyphenated_layer_count(doc: str) -> None:
    """The adjective form ("consistent 27-layer counts") is a roster claim too.

    B18 #458: the plural-noun grep above never matched it, so `README.md`
    carried a 27 against a 39-layer registry. Every hyphenated count in these
    docs describes the whole roster; a subset says "N layers" instead.
    """
    expected = _operational_layer_count()
    for match in re.finditer(r"\b(\d+)-layer\b", _read(doc)):
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
    assert _operational_layer_count() == 39
    assert _numbered_group_count() == 35


# A16 #459: AGENTS.md is the model-neutral door to CLAUDE.md, and the
# capability-status table is the one status file. Both are made of pointers;
# a pointer to a moved or deleted file is the exact rot the table exists to stop.
_REPO_POINTER = re.compile(r"`([\w.-]+(?:/[\w.*-]+)*(?:\.(?:py|md|yml)|/))`")


def _capability_table() -> str:
    text = _read("ARCHITECTURE.md")
    start = text.index("## Capability status")
    return text[start : text.index("\n## ", start + 1)]


def _repo_pointers(text: str) -> set[str]:
    return {m.group(1) for m in _REPO_POINTER.finditer(text) if "*" not in m.group(1)}


def test_agents_md_states_the_canonical_mission() -> None:
    assert CANONICAL_MISSION in _read("AGENTS.md")


@pytest.mark.parametrize("text", ["AGENTS.md", "capability table"])
def test_status_pointers_resolve(text: str) -> None:
    body = _read("AGENTS.md") if text == "AGENTS.md" else _capability_table()
    pointers = _repo_pointers(body)
    assert pointers, f"no repository pointers found in {text}"
    missing = sorted(p for p in pointers if not (REPO / p).exists())
    assert not missing, f"{text} points at paths that do not exist: {missing}"


def test_capability_rows_name_a_known_rightmost_column() -> None:
    columns = {"specified", "implemented", "tested", "deployed", "operationally verified"}
    rows = [
        line
        for line in _capability_table().splitlines()
        if line.startswith("| ") and not line.startswith("| Capability")
    ]
    assert rows
    for row in rows:
        rightmost = row.rstrip(" |").rsplit("|", 1)[1].strip()
        assert rightmost in columns, f"unknown rightmost column {rightmost!r} in: {row[:60]}"


def test_readme_source_table_lists_every_production_layer() -> None:
    """The README's source table says it is checked against the roster.

    Make that true: the layer numbers in the table are exactly the numbers in
    ``config.PRODUCTION_LAYERS`` — no layer missing, no retired layer lingering.
    """
    table_numbers = set(re.findall(r"^\| (\d+[a-z]?) \|", _read("README.md"), flags=re.M))
    roster_numbers = {number for _, number, *_ in config.PRODUCTION_LAYERS}
    assert table_numbers == roster_numbers, {
        "missing_from_readme": sorted(roster_numbers - table_numbers),
        "not_in_roster": sorted(table_numbers - roster_numbers),
    }
