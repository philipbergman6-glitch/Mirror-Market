"""Enter, list and validate landed-cost assumptions.

The human end of the manual-assumption workflow. Writing YAML by hand works and
is documented, but it is also where an ad-valorem rate gets typed as USD/MT and
an expiry gets left off; this validates before it writes, so a bad entry fails
at the keyboard rather than on the page.

Fully non-interactive — every input is a flag, so it works in a script, in CI
and over SSH.

    # add one
    python scripts/enter_assumption.py \
      --component ocean_freight --value 52.0 --unit usd_per_mt \
      --origin us_gulf --destination cn_north \
      --window 2026-10-01:2026-10-31 \
      --basis "Panamax 60kt USG-N.China, broker indication" \
      --entered-by you@example.com --expires 2026-09-17

    # what is live, what lapses soon, what is already dead
    python scripts/enter_assumption.py --list

    # validate every file; exit 1 on any error (wire this into CI)
    python scripts/enter_assumption.py --check

    # what the origin page still needs before it can rank
    python scripts/enter_assumption.py --gaps
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402
from analysis.origins.assumptions import (  # noqa: E402
    REQUIRED_UNIT,
    AssumptionError,
    load_assumptions,
    parse_assumption,
)
from analysis.origins.domain import Confidence, CostComponent, ShipmentWindow  # noqa: E402

# Where a component's entries are written when --file is not given. Keeping
# policy rates out of the freight file matters: they have different lifetimes
# and different provenance, and a reviewer should be able to see the freight
# file's whole contents at once.
DEFAULT_FILE_BY_COMPONENT = {
    CostComponent.IMPORT_DUTY: "china_import_policy.yml",
    CostComponent.IMPORT_VAT: "china_import_policy.yml",
    CostComponent.PROCESSING_COST: "crush_plant.yml",
    CostComponent.ENERGY_COST: "crush_plant.yml",
    CostComponent.PLANT_FREIGHT_IN: "crush_plant.yml",
    CostComponent.WORKING_CAPITAL: "crush_plant.yml",
}
FALLBACK_FILE = "freight_and_logistics.yml"

# Assumptions lapsing inside this many days are called out by --list. Two weeks
# is about how long it takes to get a fresh freight indication, which is the
# point of warning rather than waiting for the page to block.
EXPIRY_WARNING_DAYS = 14


def _today() -> date:
    return datetime.now(timezone.utc).date()


def _parse_window(text: str | None) -> ShipmentWindow | None:
    if not text:
        return None
    try:
        start, end = text.split(":", 1)
        return ShipmentWindow(date.fromisoformat(start), date.fromisoformat(end))
    except ValueError as exc:
        raise SystemExit(f"--window must be START:END in ISO dates, got {text!r}") from exc


def _slug(component: CostComponent, origin: str | None, destination: str | None, window) -> str:
    parts = [component.value, origin or "any", destination or "any"]
    parts.append(window.start.strftime("%Y-%m") if window else "always")
    return ".".join(parts)


def _target_file(component: CostComponent, explicit: str | None) -> Path:
    name = explicit or DEFAULT_FILE_BY_COMPONENT.get(component, FALLBACK_FILE)
    return Path(config.ASSUMPTIONS_DIR) / name


def _yaml_block(entry: dict) -> str:
    """Render one entry as YAML by hand, to keep the files reviewable.

    ``yaml.safe_dump`` would rewrite the whole file and lose every comment in
    it — and those comments are the part that explains why the file is empty.
    Appending a formatted block keeps the header intact.
    """
    import yaml

    lines = ["", yaml.safe_dump([entry], sort_keys=False, allow_unicode=True, width=100).rstrip()]
    return "\n".join(lines) + "\n"


def add(args: argparse.Namespace) -> int:
    component = CostComponent(args.component)
    window = _parse_window(args.window)
    expected_unit = REQUIRED_UNIT[component]
    unit = args.unit or expected_unit

    entry = {
        "id": args.id or _slug(component, args.origin, args.destination, window),
        "component": component.value,
        "value": args.value,
        "unit": unit,
        "basis": args.basis,
        "source": args.source,
        "entered_by": args.entered_by,
        "entered_at": (args.entered_at or _today()).isoformat(),
        "expires_on": args.expires.isoformat(),
        "confidence": args.confidence,
    }
    for key, value in (
        ("origin", args.origin),
        ("destination", args.destination),
        ("days", args.days),
        ("citation", args.citation),
    ):
        if value is not None:
            entry[key] = value
    if window is not None:
        entry["window"] = {"start": window.start.isoformat(), "end": window.end.isoformat()}

    # Validate before writing. A file that fails to load blocks every route,
    # not just the one being entered, so a bad entry must never reach disk.
    try:
        parsed = parse_assumption(entry, where="<new entry>")
    except AssumptionError as exc:
        print(f"REJECTED: {exc}", file=sys.stderr)
        return 1

    existing = load_assumptions()
    if any(item.id == parsed.id for item in existing.assumptions):
        print(
            f"REJECTED: an assumption with id {parsed.id!r} already exists. "
            "Retire it or pass --id with a different name — two answers to one "
            "question means one of them is stale.",
            file=sys.stderr,
        )
        return 1

    path = _target_file(component, args.file)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text("[]\n", encoding="utf-8")
    text = path.read_text(encoding="utf-8")
    # A file whose only content is the empty-list marker needs that marker
    # removed, or PyYAML reads a list followed by a list.
    stripped = "\n".join(
        line for line in text.splitlines() if line.strip() != "[]"
    ).rstrip()
    path.write_text(stripped + "\n" + _yaml_block(entry), encoding="utf-8")

    # A MIRROR_ASSUMPTIONS_DIR override can point outside the repo (the dev
    # loop does exactly that), so the path is only shortened when it is inside.
    try:
        shown = path.relative_to(PROJECT_ROOT)
    except ValueError:
        shown = path
    print(f"WROTE {parsed.id} -> {shown}")
    print(f"  {parsed.value} {parsed.unit}, expires {parsed.expires_on.isoformat()}")
    # Re-load the whole set: the single most useful thing this command can do
    # after writing is prove the file it just touched still parses.
    reloaded = load_assumptions()
    print(f"  set now holds {len(reloaded.assumptions)} assumption(s), id {reloaded.set_id}")
    return 0


def listing(_: argparse.Namespace) -> int:
    today = _today()
    assumptions = load_assumptions()
    if not assumptions.assumptions:
        print("No assumptions entered. Every costed leg will block — see")
        print("data/reference/assumptions/README.md")
        return 0
    horizon = today + timedelta(days=EXPIRY_WARNING_DAYS)
    for assumption in sorted(assumptions.assumptions, key=lambda a: (a.component.value, a.id)):
        if not assumption.is_live(today):
            state = "EXPIRED"
        elif assumption.expires_on <= horizon:
            state = "LAPSING"
        else:
            state = "live   "
        route = f"{assumption.origin or '*'} -> {assumption.destination or '*'}"
        window = assumption.window.describe() if assumption.window else "any window"
        print(
            f"{state}  {assumption.id:<44} {assumption.value:>10.4g} {assumption.unit:<15} "
            f"{route:<26} {window:<26} expires {assumption.expires_on.isoformat()} "
            f"({assumption.entered_by})"
        )
    print(f"\nset {assumptions.set_id} from {', '.join(assumptions.loaded_from)}")
    return 0


def check(_: argparse.Namespace) -> int:
    try:
        assumptions = load_assumptions()
    except AssumptionError as exc:
        print(f"INVALID: {exc}", file=sys.stderr)
        return 1
    today = _today()
    expired = [a for a in assumptions.assumptions if not a.is_live(today)]
    print(f"OK: {len(assumptions.assumptions)} assumption(s) parse, set {assumptions.set_id}")
    for assumption in expired:
        print(f"  note: {assumption.id} expired {assumption.expires_on.isoformat()}")
    return 0


def gaps(_: argparse.Namespace) -> int:
    """What the origin page still needs before it can publish a ranking."""
    import sqlite3

    from analysis.origins.comparison import build_ranking, default_window

    today = _today()
    window = default_window(today)
    conn = sqlite3.connect(config.DB_PATH) if Path(config.DB_PATH).exists() else None
    try:
        for destination in config.DESTINATION_PORTS:
            ranking = build_ranking(
                conn, destination_key=destination, window=window, today=today
            )
            print(f"\n{destination} · {window.describe()}")
            if not ranking.rows:
                print("  no origin quotes at all — the ingest, not the assumptions")
            for row in ranking.rows:
                status = "RANKABLE" if row.is_rankable else row.comparability.value
                print(f"  {row.quote.origin.key:<14} {status}")
                for blocker in row.blockers:
                    print(f"      - {blocker.message}")
                    if blocker.remedy:
                        print(f"        remedy: {blocker.remedy}")
            for unavailable in ranking.unavailable:
                print(f"  {unavailable.port.key:<14} no source: {unavailable.reason}")
    finally:
        if conn is not None:
            conn.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--list", action="store_true", help="show every assumption and its state")
    mode.add_argument("--check", action="store_true", help="validate every file; exit 1 on error")
    mode.add_argument("--gaps", action="store_true", help="what the page still needs to rank")

    parser.add_argument("--component", choices=[c.value for c in CostComponent])
    parser.add_argument("--value", type=float)
    parser.add_argument("--unit", help="defaults to the component's required unit")
    parser.add_argument("--days", type=int, help="carry days; required for rate_per_annum")
    parser.add_argument("--origin", help="ORIGIN_PORTS key, or a market slug for crush costs")
    parser.add_argument("--destination", help="DESTINATION_PORTS key")
    parser.add_argument("--window", help="START:END in ISO dates")
    parser.add_argument("--basis", help="what the number is and where it came from")
    parser.add_argument("--source", default="manual")
    parser.add_argument("--entered-by", dest="entered_by")
    parser.add_argument("--entered-at", dest="entered_at", type=date.fromisoformat)
    parser.add_argument("--expires", type=date.fromisoformat)
    parser.add_argument(
        "--confidence",
        default=Confidence.INDICATIVE.value,
        choices=[c.value for c in Confidence if c is not Confidence.EXECUTABLE],
    )
    parser.add_argument("--citation")
    parser.add_argument("--id", help="defaults to component.origin.destination.window")
    parser.add_argument("--file", help="target YAML file name inside the assumptions directory")

    args = parser.parse_args(argv)
    if args.list:
        return listing(args)
    if args.check:
        return check(args)
    if args.gaps:
        return gaps(args)

    missing = [
        name for name, value in (
            ("--component", args.component),
            ("--value", args.value),
            ("--basis", args.basis),
            ("--entered-by", args.entered_by),
            ("--expires", args.expires),
        ) if value is None
    ]
    if missing:
        parser.error(
            f"adding an assumption needs {', '.join(missing)} — an entry with no owner, "
            "reason or expiry is indistinguishable from a guess"
        )
    return add(args)


if __name__ == "__main__":
    raise SystemExit(main())
