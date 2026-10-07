"""Wayfinding pass (2026-10-07): the "How to read" key and the "deeper" pointers.

Two claims under test.

1. The key is the code's own vocabulary, not a retyped copy. Every quote-kind
   label, every ledger state, every block state and every tier the site can
   render must appear in the key — a label the code can emit and the key does
   not explain is a parallel vocabulary (invariant 3).
2. A "deeper" pointer never links to an anchor the target page does not
   render: a brief drops the ledger and news blocks, a stub renders no blocks
   at all.
"""

from __future__ import annotations

import pytest

from app import wayfinding
from app.block_builders import (
    LEDGER_STATE_DARK,
    LEDGER_STATE_NO_PRINT,
    LEDGER_STATE_OUT_OF_CADENCE,
    LEDGER_STATE_REPRICED,
)
from app.blocks import BLOCK_IDS, BRIEF_BLOCK_IDS, STATES
from app.markets import TIER_BRIEF, TIER_PAGE, TIER_STUB
from pricing.semantics import QUOTE_KIND_LABELS
from scripts.generate_html import SECTIONS


# ---------------------------------------------------------------------------
# How to read
# ---------------------------------------------------------------------------
def _flatten(vocab: dict) -> set[str]:
    out: set[str] = set()
    for group in vocab.values():
        for entry in group["entries"]:
            out.add(entry["key"])
    return out


def test_key_explains_every_quote_kind_the_code_can_emit():
    keys = _flatten(wayfinding.how_to_read_vocabulary())
    for kind in QUOTE_KIND_LABELS:
        assert kind in keys, f"quote kind {kind!r} has no entry in the key"


def test_key_explains_every_ledger_state():
    keys = _flatten(wayfinding.how_to_read_vocabulary())
    for state in (LEDGER_STATE_REPRICED, LEDGER_STATE_NO_PRINT,
                  LEDGER_STATE_DARK, LEDGER_STATE_OUT_OF_CADENCE):
        assert state in keys


def test_key_explains_every_block_state_and_tier():
    keys = _flatten(wayfinding.how_to_read_vocabulary())
    for state in STATES:
        assert state in keys
    for tier in (TIER_PAGE, TIER_BRIEF, TIER_STUB):
        assert tier in keys


def test_every_key_entry_has_a_label_and_a_meaning():
    for group in wayfinding.how_to_read_vocabulary().values():
        assert group["title"]
        for entry in group["entries"]:
            assert entry["label"].strip()
            assert entry["meaning"].strip()


# ---------------------------------------------------------------------------
# Deeper pointers
# ---------------------------------------------------------------------------
def _nav(**tiers: str) -> list[dict]:
    return [
        {"slug": slug, "name": slug.upper(), "href": f"markets/{slug}.html", "tier": tier}
        for slug, tier in tiers.items()
    ]


def test_every_headline_section_declares_its_pointers_explicitly():
    """No section may be silently missing: an empty list is a decision."""
    for section in SECTIONS:
        assert section["id"] in wayfinding.DEEPER, section["id"]


def test_unknown_section_is_rejected():
    with pytest.raises(KeyError):
        wayfinding.deeper_links("no-such-section", _nav(cbot=TIER_PAGE))


def test_pointer_block_ids_are_real_blocks():
    for section_id, targets in wayfinding.DEEPER.items():
        for target in targets:
            if target.kind == "block":
                assert target.block in BLOCK_IDS, (section_id, target.block)


def test_page_tier_gets_the_ledger_but_a_brief_does_not():
    nav = _nav(cbot=TIER_PAGE, india=TIER_BRIEF)
    groups = wayfinding.deeper_links("propagation", nav)
    assert len(groups) == 1
    markets = [m["name"] for m in groups[0]["markets"]]
    assert markets == ["CBOT"]
    assert groups[0]["markets"][0]["href"] == "markets/cbot.html#block-ledger"


def test_brief_blocks_are_linked_on_a_brief():
    nav = _nav(india=TIER_BRIEF)
    groups = wayfinding.deeper_links("supply-demand", nav)
    assert "supply_demand" in BRIEF_BLOCK_IDS
    assert groups[0]["markets"][0]["href"] == "markets/india.html#block-supply_demand"


def test_a_stub_is_never_linked():
    nav = _nav(nigeria=TIER_STUB)
    assert wayfinding.deeper_links("supply-demand", nav) == []


def test_scoped_targets_only_link_the_markets_named():
    nav = _nav(cbot=TIER_PAGE, brazil=TIER_PAGE, nigeria=TIER_PAGE)
    groups = wayfinding.deeper_links("crush-board", nav)
    names = {m["name"] for g in groups for m in g["markets"]}
    assert "NIGERIA" not in names
    assert {"CBOT", "BRAZIL"} <= names


def test_block_pointer_carries_number_and_title_from_block_specs():
    groups = wayfinding.deeper_links("crush-board", _nav(cbot=TIER_PAGE))
    assert groups[0]["no"] == "03"
    assert groups[0]["title"] == "Crush margin"


def test_page_pointer_is_rooted():
    groups = wayfinding.deeper_links("forward-curves", [], root="../")
    assert groups[0]["kind"] == "page"
    assert groups[0]["href"] == "../workstation.html"


def test_briefing_section_points_at_the_briefing_page():
    groups = wayfinding.deeper_links("briefing", [])
    assert any(g["href"] == "briefing.html" for g in groups)


# ---------------------------------------------------------------------------
# The one environment factory
# ---------------------------------------------------------------------------
def test_every_production_renderer_uses_the_shared_environment():
    """_base.html.j2 guards the key behind `how_to_read is defined` so a bare
    Environment in a focused test still renders. That guard is only safe if
    no production page is rendered through a bare Environment — one that is
    would ship without the key and nothing would say so."""
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    factory = root / "app" / "templating.py"
    # Any Jinja environment class (Environment, SandboxedEnvironment, …),
    # with or without a space before the paren, and an aliased import.
    pattern = re.compile(r"\w*Environment\s*\(|import\s+Environment\s+as\b")
    offenders = []
    for folder in ("app", "scripts", "trust"):
        for path in (root / folder).rglob("*.py"):
            if path == factory:
                continue
            if pattern.search(path.read_text(encoding="utf-8")):
                offenders.append(str(path.relative_to(root)))
    assert offenders == [], offenders


def test_shared_environment_installs_the_key_and_refuses_autoescape_override():
    from app.templating import site_environment

    env = site_environment()
    assert env.autoescape is True
    assert env.globals["how_to_read"] is wayfinding.how_to_read_vocabulary
    with pytest.raises(ValueError):
        site_environment(autoescape=False)
