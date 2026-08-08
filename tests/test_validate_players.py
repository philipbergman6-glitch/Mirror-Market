"""Tests for scripts/validate_players.py (issue #122 — players data model)."""

from pathlib import Path

import pytest
import yaml

from scripts.validate_players import PLAYERS_DIR, validate_players

VALID_ENTRY = {
    "name": "Test Trading Co",
    "scope": "global",
    "side": "seller",
    "roles": ["exporter"],
    "products": ["beans", "meal"],
    "confidence": "observed",
    "as_of": "2026-08-08",
    "citations": [{"url": "https://example.com", "accessed": "2026-08-08", "supports": "all"}],
    "tier": 1,
    "destinations_structured": [
        {"code": "CN", "products": ["beans"], "note": "dominant buyer"},
        {"code": "EU", "products": ["meal"]},
    ],
    "contacts": {
        "website": "https://example.com",
        "contact_url": "https://example.com/contact",
        "offices": ["Rosario", "Geneva"],
        "trade_desks": [
            {"label": "Oilseeds export desk", "email": "x@example.com", "office": "Rosario"}
        ],
        "accessed": "2026-08-08",
    },
    "activity": [
        {
            "date": "2026-07-02",
            "category": "capacity",
            "headline": "New crush plant",
            "detail": "Doubled capacity.",
            "citation": {"url": "https://example.com/news", "accessed": "2026-08-08"},
        }
    ],
}


def write_players(tmp_path: Path, entries: list) -> Path:
    (tmp_path / "test.yml").write_text(yaml.safe_dump(entries), encoding="utf-8")
    return tmp_path


def errors_for(tmp_path: Path, entry: dict) -> list[str]:
    return validate_players(write_players(tmp_path, [entry]))


def test_valid_entry_passes(tmp_path):
    assert errors_for(tmp_path, VALID_ENTRY) == []


def test_minimal_entry_passes(tmp_path):
    # destinations_structured / tier / contacts / activity are all optional
    assert errors_for(tmp_path, {"name": "Minimal Co"}) == []


@pytest.mark.parametrize(
    "dest",
    [
        {"code": "XX", "products": ["beans"]},  # unassigned ISO code
        {"code": "ASIA-PACIFIC", "products": ["beans"]},  # not in region enum
        {"code": "cn", "products": ["beans"]},  # lowercase not accepted
        {"code": "CN", "products": ["palm oil"]},  # non-soy product
        {"code": "CN", "products": []},  # empty products
        {"code": "CN"},  # products missing
        {"code": "CN", "products": ["beans"], "share": "57%"},  # unknown key
        "CN",  # not a mapping
    ],
)
def test_bad_destination_fails(tmp_path, dest):
    entry = {**VALID_ENTRY, "destinations_structured": [dest]}
    assert errors_for(tmp_path, entry)


def test_tier_must_be_one(tmp_path):
    assert errors_for(tmp_path, {**VALID_ENTRY, "tier": 2})
    assert errors_for(tmp_path, {**VALID_ENTRY, "tier": "1"})
    assert errors_for(tmp_path, {**VALID_ENTRY, "tier": True})  # YAML `tier: true`; True == 1


def test_duplicate_destination_code_fails(tmp_path):
    entry = {
        **VALID_ENTRY,
        "destinations_structured": [
            {"code": "CN", "products": ["beans"]},
            {"code": "CN", "products": ["meal"]},
        ],
    }
    assert errors_for(tmp_path, entry)


def test_duplicate_products_in_destination_fail(tmp_path):
    entry = {**VALID_ENTRY, "destinations_structured": [{"code": "CN", "products": ["beans", "beans"]}]}
    assert errors_for(tmp_path, entry)


def test_stray_yaml_extension_fails(tmp_path):
    (tmp_path / "test.yml").write_text(yaml.safe_dump([{"name": "A Co"}]), encoding="utf-8")
    (tmp_path / "oops.yaml").write_text(yaml.safe_dump([{"name": "B Co"}]), encoding="utf-8")
    assert validate_players(tmp_path)


@pytest.mark.parametrize(
    "activity",
    [
        [{"date": "2026-07-02", "category": "ipo", "headline": "x",
          "citation": {"url": "u", "accessed": "2026-08-08"}}],  # bad category
        [{"date": "2026-07-02", "category": "trade", "headline": "x"}],  # missing citation
        [{"date": "2026-07-02", "category": "trade", "headline": "x",
          "citation": {"url": "u"}}],  # citation missing accessed
        [{"date": "2026-07-02", "category": "trade", "headline": "x",
          "citation": {"accessed": "2026-08-08"}}],  # citation missing url
        [{"date": "07/02/2026", "category": "trade", "headline": "x",
          "citation": {"url": "u", "accessed": "2026-08-08"}}],  # malformed date
        [{"date": "2026-07-02", "category": "trade",
          "citation": {"url": "u", "accessed": "2026-08-08"}}],  # missing headline
        [{"date": "2026-07-02", "category": "trade", "headline": "x", "source": "press",
          "citation": {"url": "u", "accessed": "2026-08-08"}}],  # unknown key
    ],
)
def test_bad_activity_fails(tmp_path, activity):
    entry = {**VALID_ENTRY, "activity": activity}
    assert errors_for(tmp_path, entry)


@pytest.mark.parametrize(
    "contacts",
    [
        {"website": "https://x", "accessed": "August 2026"},  # malformed date
        {"website": "https://x", "linkedin": "https://x"},  # unknown key
        {"trade_desks": [{"email": "x@example.com"}]},  # desk missing label
        {"trade_desks": [{"label": "Desk", "person": "Jane"}]},  # unknown desk key
    ],
)
def test_bad_contacts_fails(tmp_path, contacts):
    entry = {**VALID_ENTRY, "contacts": contacts}
    assert errors_for(tmp_path, entry)


def test_unquoted_yaml_dates_accepted(tmp_path):
    # YAML parses bare 2026-08-08 to datetime.date — must not be rejected
    raw = """
- name: Bare Date Co
  activity:
    - date: 2026-07-02
      category: trade
      headline: x
      citation: {url: u, accessed: 2026-08-08}
"""
    (tmp_path / "test.yml").write_text(raw, encoding="utf-8")
    assert validate_players(tmp_path) == []


def test_missing_name_fails(tmp_path):
    assert errors_for(tmp_path, {"scope": "global"})


def test_file_not_a_list_fails(tmp_path):
    (tmp_path / "test.yml").write_text("name: not-a-list\n", encoding="utf-8")
    assert validate_players(tmp_path)


def test_duplicate_name_same_scope_fails(tmp_path):
    (tmp_path / "a.yml").write_text(yaml.safe_dump([{"name": "Dup Co", "scope": "us"}]), encoding="utf-8")
    (tmp_path / "b.yml").write_text(yaml.safe_dump([{"name": "Dup Co", "scope": "us"}]), encoding="utf-8")
    assert validate_players(tmp_path)


def test_cross_listing_under_different_scopes_passes(tmp_path):
    # e.g. CHS Inc.: global house view in global.yml + US assets in us.yml
    (tmp_path / "a.yml").write_text(yaml.safe_dump([{"name": "Dup Co", "scope": "global"}]), encoding="utf-8")
    (tmp_path / "b.yml").write_text(yaml.safe_dump([{"name": "Dup Co", "scope": "us"}]), encoding="utf-8")
    assert validate_players(tmp_path) == []


def test_error_message_names_file_and_entry(tmp_path):
    entry = {**VALID_ENTRY, "destinations_structured": [{"code": "XX", "products": ["beans"]}]}
    errors = errors_for(tmp_path, entry)
    assert any("test.yml" in e and "Test Trading Co" in e for e in errors)


def test_real_dataset_validates():
    assert validate_players(PLAYERS_DIR) == []
