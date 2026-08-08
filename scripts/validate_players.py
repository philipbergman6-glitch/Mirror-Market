"""Build-time validation for the players knowledge base (issue #122).

Validates every ``data/reference/players/*.yml`` entry against the Phase 2
data model (spec: research/2026-08-08-issue-111-players-phase2-spec.md):
destination vocabulary (ISO-3166 alpha-2 + region enum), ``tier`` flags,
``contacts`` and ``activity`` schemas. Hard-fails on any violation — a typo
in a country code must break the build, not render as a dead filter.

Usage:
    python scripts/validate_players.py        # exit 1 + messages on violations
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PLAYERS_DIR = PROJECT_ROOT / "data" / "reference" / "players"

# Region enum for destinations sources name only as a bloc (spec decision #11).
# EU covers prose "EU"/"Europe"; extend deliberately, never ad hoc.
REGION_CODES = frozenset({"EU", "MENA", "SE-ASIA", "ASIA", "LATAM", "AFRICA", "GLOBAL"})

# ISO 3166-1 alpha-2, officially assigned (249 codes).
_ISO_TABLE = """
AD AE AF AG AI AL AM AO AQ AR AS AT AU AW AX AZ
BA BB BD BE BF BG BH BI BJ BL BM BN BO BQ BR BS BT BV BW BY BZ
CA CC CD CF CG CH CI CK CL CM CN CO CR CU CV CW CX CY CZ
DE DJ DK DM DO DZ
EC EE EG EH ER ES ET
FI FJ FK FM FO FR
GA GB GD GE GF GG GH GI GL GM GN GP GQ GR GS GT GU GW GY
HK HM HN HR HT HU
ID IE IL IM IN IO IQ IR IS IT
JE JM JO JP
KE KG KH KI KM KN KP KR KW KY KZ
LA LB LC LI LK LR LS LT LU LV LY
MA MC MD ME MF MG MH MK ML MM MN MO MP MQ MR MS MT MU MV MW MX MY MZ
NA NC NE NF NG NI NL NO NP NR NU NZ
OM
PA PE PF PG PH PK PL PM PN PR PS PT PW PY
QA
RE RO RS RU RW
SA SB SC SD SE SG SH SI SJ SK SL SM SN SO SR SS ST SV SX SY SZ
TC TD TF TG TH TJ TK TL TM TN TO TR TT TV TW TZ
UA UG UM US UY UZ
VA VC VE VG VI VN VU
WF WS
YE YT
ZA ZM ZW
"""
ISO_ALPHA2 = frozenset(_ISO_TABLE.split())

DESTINATION_CODES = ISO_ALPHA2 | REGION_CODES
SOY_PRODUCTS = frozenset({"beans", "meal", "oil"})
ACTIVITY_CATEGORIES = frozenset({"ma", "capacity", "trade", "policy", "distress"})

DESTINATION_KEYS = frozenset({"code", "products", "note"})
CONTACTS_KEYS = frozenset({"website", "contact_url", "offices", "trade_desks", "accessed"})
TRADE_DESK_KEYS = frozenset({"label", "email", "phone", "office"})
ACTIVITY_KEYS = frozenset({"date", "category", "headline", "detail", "citation"})
CITATION_KEYS = frozenset({"url", "accessed"})


def _bad_date(value: object) -> bool:
    """True unless value is a YAML date or a strict YYYY-MM-DD string."""
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        return False
    if isinstance(value, str):
        try:
            dt.date.fromisoformat(value)
        except ValueError:
            return True
        return len(value) != 10  # fromisoformat also accepts e.g. 20260808
    return True


def _check_keys(obj: dict, allowed: frozenset[str], what: str) -> list[str]:
    unknown = sorted(set(obj) - allowed)
    return [f"{what} has unknown key(s) {unknown} — allowed: {sorted(allowed)}"] if unknown else []


def _validate_destinations(dests: object) -> list[str]:
    if not isinstance(dests, list):
        return ["destinations_structured must be a list"]
    errors: list[str] = []
    codes = [d.get("code") for d in dests if isinstance(d, dict)]
    for dup in sorted({c for c in codes if codes.count(c) > 1 and isinstance(c, str)}):
        errors.append(f"destinations_structured has duplicate code {dup!r} — merge the rows")
    for i, d in enumerate(dests):
        what = f"destinations_structured[{i}]"
        if not isinstance(d, dict):
            errors.append(f"{what} must be a mapping, got {type(d).__name__}")
            continue
        errors += _check_keys(d, DESTINATION_KEYS, what)
        code = d.get("code")
        if code not in DESTINATION_CODES:
            errors.append(
                f"{what}.code {code!r} is not an ISO-3166 alpha-2 code or region enum "
                f"{sorted(REGION_CODES)}"
            )
        products = d.get("products")
        if (
            not isinstance(products, list)
            or not products
            or not set(products) <= SOY_PRODUCTS
            or len(set(products)) != len(products)
        ):
            errors.append(
                f"{what}.products must be a non-empty, duplicate-free subset of "
                f"{sorted(SOY_PRODUCTS)}, got {products!r}"
            )
        if "note" in d and not isinstance(d["note"], str):
            errors.append(f"{what}.note must be a string")
    return errors


def _validate_contacts(contacts: object) -> list[str]:
    if not isinstance(contacts, dict):
        return ["contacts must be a mapping"]
    errors = _check_keys(contacts, CONTACTS_KEYS, "contacts")
    if "accessed" in contacts and _bad_date(contacts["accessed"]):
        errors.append(f"contacts.accessed {contacts['accessed']!r} is not a YYYY-MM-DD date")
    desks = contacts.get("trade_desks", [])
    if not isinstance(desks, list):
        return errors + ["contacts.trade_desks must be a list"]
    for i, desk in enumerate(desks):
        what = f"contacts.trade_desks[{i}]"
        if not isinstance(desk, dict):
            errors.append(f"{what} must be a mapping")
            continue
        errors += _check_keys(desk, TRADE_DESK_KEYS, what)
        if not isinstance(desk.get("label"), str) or not desk.get("label"):
            errors.append(f"{what}.label is required (never named individuals — spec decision #5)")
    return errors


def _validate_activity(activity: object) -> list[str]:
    if not isinstance(activity, list):
        return ["activity must be a list"]
    errors: list[str] = []
    for i, item in enumerate(activity):
        what = f"activity[{i}]"
        if not isinstance(item, dict):
            errors.append(f"{what} must be a mapping")
            continue
        errors += _check_keys(item, ACTIVITY_KEYS, what)
        if _bad_date(item.get("date")):
            errors.append(f"{what}.date {item.get('date')!r} is not a YYYY-MM-DD date")
        if item.get("category") not in ACTIVITY_CATEGORIES:
            errors.append(f"{what}.category {item.get('category')!r} not in {sorted(ACTIVITY_CATEGORIES)}")
        if not isinstance(item.get("headline"), str) or not item.get("headline"):
            errors.append(f"{what}.headline is required")
        citation = item.get("citation")
        if not isinstance(citation, dict):
            errors.append(f"{what}.citation {{url, accessed}} is required")
            continue
        errors += _check_keys(citation, CITATION_KEYS, f"{what}.citation")
        if not isinstance(citation.get("url"), str) or not citation.get("url"):
            errors.append(f"{what}.citation.url is required")
        if _bad_date(citation.get("accessed")):
            errors.append(f"{what}.citation.accessed {citation.get('accessed')!r} is not a YYYY-MM-DD date")
    return errors


def validate_entry(entry: dict) -> list[str]:
    """Return all Phase 2 schema violations for one player entry."""
    errors: list[str] = []
    # isinstance(bool) guard: YAML `tier: true` would otherwise pass (True == 1)
    if "tier" in entry and (isinstance(entry["tier"], bool) or entry["tier"] != 1):
        errors.append(f"tier must be the integer 1 (absent = tier 2), got {entry['tier']!r}")
    if "destinations_structured" in entry:
        errors += _validate_destinations(entry["destinations_structured"])
    if "contacts" in entry:
        errors += _validate_contacts(entry["contacts"])
    if "activity" in entry:
        errors += _validate_activity(entry["activity"])
    return errors


def validate_players(players_dir: Path = PLAYERS_DIR) -> list[str]:
    """Validate every players YAML file; return all violations found."""
    errors: list[str] = []
    # Cross-listing the same company under two scopes is deliberate (e.g. CHS:
    # global house view + US assets) — only a duplicate (name, scope) pair fails.
    seen: dict[tuple[str, str], str] = {}
    stray = sorted(p.name for p in players_dir.glob("*.yaml"))
    if stray:
        errors.append(f"mis-extensioned file(s) {stray} — players files must use .yml")
    for path in sorted(players_dir.glob("*.yml")):
        try:
            entries = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:
            errors.append(f"{path.name} :: not parseable as YAML: {exc}")
            continue
        if not isinstance(entries, list):
            errors.append(f"{path.name} :: must be a YAML list of player entries")
            continue
        for i, entry in enumerate(entries):
            if not isinstance(entry, dict) or not isinstance(entry.get("name"), str) or not entry.get("name"):
                errors.append(f"{path.name} :: entry [{i}] has no name")
                continue
            name = entry["name"]
            key = (name, str(entry.get("scope", "")))
            if key in seen:
                errors.append(f"{path.name} :: {name} :: duplicate of same-scope entry in {seen[key]}")
            seen[key] = path.name
            errors += [f"{path.name} :: {name} :: {e}" for e in validate_entry(entry)]
    return errors


def main() -> int:
    errors = validate_players()
    if errors:
        print(f"players validation FAILED — {len(errors)} violation(s):", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        return 1
    total, tier1 = 0, 0
    for path in sorted(PLAYERS_DIR.glob("*.yml")):
        entries = yaml.safe_load(path.read_text(encoding="utf-8"))
        total += len(entries)
        tier1 += sum(1 for e in entries if e.get("tier") == 1)
    print(f"players validation OK — {total} entries, {tier1} tier-1")
    return 0


if __name__ == "__main__":
    sys.exit(main())
