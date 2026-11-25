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

    # what the origin page still needs before it can rank (reads the database)
    python scripts/enter_assumption.py --gaps

    # what a trader must enter before each route is comparable (no database)
    python scripts/enter_assumption.py --onboarding --window 2026-10-01:2026-10-31

    # the renewal queue: what lapses, when, and which routes go dark with it
    python scripts/enter_assumption.py --review --horizon 30

    # re-sign a lapsed or lapsing entry (same number unless --value is given);
    # the old row is retired in place, the file is the audit trail
    python scripts/enter_assumption.py --renew ocean_freight.us_gulf.cn_north.2026-12 \
      --expires 2026-12-09 --entered-by you@example.com [--value 55.0]

    # --expires may be omitted when adding: the component's default lifetime
    # (config.ASSUMPTION_DEFAULT_LIFETIME_DAYS) is proposed and printed
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import yaml  # noqa: E402

import config  # noqa: E402
from analysis.futures.privacy import AUDIENCE_PRIVATE  # noqa: E402
from analysis.origins.assumptions import (  # noqa: E402
    DESK_COMPONENTS,
    PRIVATE_SUBDIR,
    REQUIRED_UNIT,
    AssumptionError,
    AssumptionSet,
    default_expiry,
    load_assumptions,
    parse_assumption,
    private_assumptions_dir,
)
from analysis.origins.domain import Confidence, CostComponent, ShipmentWindow  # noqa: E402

# Where a component's entries are written when --file is not given. Two
# things are decided here. Policy rates stay out of the freight file because
# they have different lifetimes and provenance, and a reviewer should see the
# freight file's whole contents at once. And every DESK component is routed
# under private/ — the gitignored tier — because an entry carries the owner's
# email and the broker in `basis`, and a tracked file would publish both on
# the next push (B12 #409). `_target_file` refuses to route a desk component
# anywhere else, --file included.
DEFAULT_FILE_BY_COMPONENT = {
    CostComponent.IMPORT_DUTY: "china_import_policy.yml",
    CostComponent.IMPORT_VAT: "china_import_policy.yml",
    CostComponent.PROCESSING_COST: f"{PRIVATE_SUBDIR}/crush_plant.yml",
    CostComponent.ENERGY_COST: f"{PRIVATE_SUBDIR}/crush_plant.yml",
    CostComponent.PLANT_FREIGHT_IN: f"{PRIVATE_SUBDIR}/crush_plant.yml",
    CostComponent.WORKING_CAPITAL: f"{PRIVATE_SUBDIR}/crush_plant.yml",
}
FALLBACK_FILE = f"{PRIVATE_SUBDIR}/freight_and_logistics.yml"


class DeskComponentInCommittedTier(ValueError):
    """A desk assumption was about to be written where git could see it."""


def _load() -> AssumptionSet:
    """The desk's whole set: committed policy rates plus the private tier.

    The CLI is the one reader that always wants both — it is how the private
    tier is written. Every page builder calls the loader bare and gets the
    public tier only.
    """
    return load_assumptions(audience=AUDIENCE_PRIVATE)

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
    """The file an entry is appended to. A desk component may only land under private/."""
    name = explicit or DEFAULT_FILE_BY_COMPONENT.get(component, FALLBACK_FILE)
    target = (Path(config.ASSUMPTIONS_DIR) / name).resolve()
    private_root = private_assumptions_dir().resolve()
    if component in DESK_COMPONENTS and target.parent != private_root:
        raise DeskComponentInCommittedTier(
            f"{component.value} is a desk assumption and may only be written under "
            f"{PRIVATE_SUBDIR}/ (gitignored) — {name!r} is in the committed tier, and the "
            "next push would publish the owner, the basis and the number. Drop --file or "
            f"pass --file {PRIVATE_SUBDIR}/{Path(name).name}."
        )
    return target


def _yaml_block(entry: dict) -> str:
    """Render one entry as YAML by hand, to keep the files reviewable.

    ``yaml.safe_dump`` would rewrite the whole file and lose every comment in
    it — and those comments are the part that explains why the file is empty.
    Appending a formatted block keeps the header intact.
    """
    import yaml

    lines = ["", yaml.safe_dump([entry], sort_keys=False, allow_unicode=True, width=100).rstrip()]
    return "\n".join(lines) + "\n"


def _default_note(component: CostComponent, entered_at: date, expires_on: date) -> str:
    """The line printed when --expires was proposed rather than given (#410)."""
    if component.value in config.ASSUMPTION_LIFETIME_ENDS_WITH_WINDOW:
        rule = "the end of its shipment window"
    else:
        rule = f"{config.ASSUMPTION_DEFAULT_LIFETIME_DAYS[component.value]} days"
    return (
        f"expiry proposed from the {component.value} default lifetime ({rule}): "
        f"{expires_on.isoformat()}. Pass --expires YYYY-MM-DD to choose another date; "
        "past it the row blocks until renewed."
    )


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

    try:
        path = _target_file(component, args.file)
    except DeskComponentInCommittedTier as exc:
        print(f"REJECTED: {exc}", file=sys.stderr)
        return 1

    existing = _load()
    if any(item.id == parsed.id for item in existing.assumptions):
        print(
            f"REJECTED: an assumption with id {parsed.id!r} already exists. "
            "Retire it or pass --id with a different name — two answers to one "
            "question means one of them is stale.",
            file=sys.stderr,
        )
        return 1

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
    print(f"WROTE {parsed.id} -> {_shown(path)}")
    print(f"  {parsed.value} {parsed.unit}, expires {parsed.expires_on.isoformat()}")
    if getattr(args, "expires_proposed", False):
        print(f"  {_default_note(parsed.component, parsed.entered_at, parsed.expires_on)}")
    # Re-load the whole set: the single most useful thing this command can do
    # after writing is prove the file it just touched still parses.
    reloaded = _load()
    print(f"  set now holds {len(reloaded.assumptions)} assumption(s), id {reloaded.set_id}")
    return 0


class CannotRetireInPlace(ValueError):
    """The outgoing row could not be located safely in its file's text."""


# A renewal's id is the root id plus the day it was signed. A renewal of a
# renewal replaces the suffix rather than stacking another one.
_RENEWED_SUFFIX = re.compile(r"\.renewed-\d{4}-\d{2}-\d{2}$")


def renewal_id(old_id: str, entered_at: date) -> str:
    return f"{_RENEWED_SUFFIX.sub('', old_id)}.renewed-{entered_at.isoformat()}"


def _id_line(line: str) -> str | None:
    """The id on a block-style ``- id: ...`` line, or ``None`` if it is not one."""
    match = re.match(r"^-\s+id:\s*(.+?)\s*$", line)
    if not match:
        return None
    try:
        value = yaml.safe_load(match.group(1))
    except yaml.YAMLError:
        return None
    return str(value) if value is not None else None


def retire_in_place(text: str, entry_id: str, *, retired_on: date, superseded_by: str) -> str:
    """``text`` with ``retired_on`` and ``superseded_by`` added to one entry.

    A text edit, not a re-dump: ``yaml.safe_dump`` would rewrite the file and
    drop every comment in it. The edit is then proven by re-parsing — every
    other entry must be unchanged and this one must differ by exactly the two
    keys — and anything the edit cannot prove raises instead of guessing.
    """
    lines = text.splitlines()
    starts = [index for index, line in enumerate(lines) if _id_line(line) == entry_id]
    if len(starts) != 1:
        raise CannotRetireInPlace(
            f"{entry_id} is not written as one block-style `- id:` entry, so it cannot be "
            "retired in place safely — add `retired_on:` and `superseded_by:` to it by hand"
        )
    start = starts[0]
    end = next(
        (index for index in range(start + 1, len(lines))
         if lines[index] and not lines[index][0].isspace()),
        len(lines),
    )
    last = max(index for index in range(start, end) if lines[index].strip())
    indent = " " * (len(lines[start]) - len(lines[start].lstrip("- ")))
    # JSON strings are valid YAML double-quoted scalars: no id can break the file.
    added = [
        f"{indent}retired_on: {retired_on.isoformat()}",
        f"{indent}superseded_by: {json.dumps(superseded_by)}",
    ]
    edited = "\n".join(lines[: last + 1] + added + lines[last + 1:]) + "\n"

    before, after = yaml.safe_load(text) or [], yaml.safe_load(edited) or []
    expected = [
        {**entry, "retired_on": retired_on, "superseded_by": superseded_by}
        if isinstance(entry, dict) and str(entry.get("id")) == entry_id else entry
        for entry in before
    ]
    if after != expected:
        raise CannotRetireInPlace(
            f"retiring {entry_id} in place would change more than its own two keys — add "
            "`retired_on:` and `superseded_by:` to it by hand"
        )
    return edited


def _file_holding(entry_id: str) -> Path:
    """The assumption file whose list carries ``entry_id`` (both tiers)."""
    roots = [Path(config.ASSUMPTIONS_DIR), private_assumptions_dir()]
    for root in roots:
        if not root.is_dir():
            continue
        for path in sorted(root.glob("*.y*ml")):
            document = yaml.safe_load(path.read_text(encoding="utf-8")) or []
            if any(isinstance(e, dict) and str(e.get("id")) == entry_id for e in document):
                return path
    raise LookupError(entry_id)


def renew(args: argparse.Namespace) -> int:
    """Re-sign one entry (#410): retire the old row in place, append the new one.

    The new row keeps the old one's scope, unit, basis and confidence; it gets a
    new ``entered_at`` (today, or ``--entered-at``), the given ``expires_on``,
    the owner from ``--entered-by`` and — only if ``--value`` is given — a new
    number. Nothing is written unless the renewed set loads.
    """
    existing = _load()
    old = next((item for item in existing.assumptions if item.id == args.renew), None)
    if old is None:
        print(
            f"REJECTED: no assumption with id {args.renew!r} — `--list` shows every id",
            file=sys.stderr,
        )
        return 1
    if old.retired_on is not None:
        print(
            f"REJECTED: {old.id} was already retired on {old.retired_on.isoformat()}"
            + (f" and is superseded by {old.superseded_by} — renew that one instead"
               if old.superseded_by else ""),
            file=sys.stderr,
        )
        return 1

    entered_at = args.entered_at or _today()
    entry = {
        key: value for key, value in old.to_dict().items()
        if value is not None and key not in ("retired_on", "superseded_by")
    }
    if entry.get("window"):
        entry["window"] = {k: v for k, v in entry["window"].items() if v is not None}
    entry.update({
        "id": args.id or renewal_id(old.id, entered_at),
        "value": args.value if args.value is not None else old.value,
        "basis": args.basis or old.basis,
        "entered_by": args.entered_by,
        "entered_at": entered_at.isoformat(),
        "expires_on": args.expires.isoformat(),
    })
    try:
        parsed = parse_assumption(entry, where="<renewal>")
    except AssumptionError as exc:
        print(f"REJECTED: {exc}", file=sys.stderr)
        return 1
    if any(item.id == parsed.id for item in existing.assumptions):
        print(
            f"REJECTED: {parsed.id} already exists — it was renewed once today already; "
            "pass --id to name this one",
            file=sys.stderr,
        )
        return 1

    path = _file_holding(old.id)
    original = path.read_text(encoding="utf-8")
    try:
        edited = retire_in_place(
            original, old.id, retired_on=entered_at, superseded_by=parsed.id
        )
    except CannotRetireInPlace as exc:
        print(f"REJECTED: {exc} ({path})", file=sys.stderr)
        return 1
    path.write_text(edited.rstrip() + "\n" + _yaml_block(entry), encoding="utf-8")
    try:
        reloaded = _load()
    except AssumptionError as exc:
        path.write_text(original, encoding="utf-8")
        print(f"REJECTED (file restored): the renewed set does not load — {exc}",
              file=sys.stderr)
        return 1

    print(f"RENEWED {old.id} -> {parsed.id} ({_shown(path)})")
    print(f"  retired {old.id} in place from {entered_at.isoformat()} "
          f"(was {old.value} {old.unit}, expired {old.expires_on.isoformat()})")
    print(f"  {parsed.value} {parsed.unit}, signed by {parsed.entered_by}, "
          f"expires {parsed.expires_on.isoformat()}")
    print(f"  set now holds {len(reloaded.assumptions)} assumption(s), id {reloaded.set_id}")
    return 0


def _shown(path: Path) -> Path:
    try:
        return path.relative_to(PROJECT_ROOT)
    except ValueError:
        return path


def listing(_: argparse.Namespace) -> int:
    today = _today()
    assumptions = _load()
    if not assumptions.assumptions:
        print("No assumptions entered. Every costed leg will block — see")
        print(f"data/reference/assumptions/README.md and {PRIVATE_SUBDIR}/README.md")
        return 0
    horizon = today + timedelta(days=EXPIRY_WARNING_DAYS)
    for assumption in sorted(assumptions.assumptions, key=lambda a: (a.component.value, a.id)):
        if assumption.is_retired(today):
            state = "retired"
        elif not assumption.is_live(today):
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


def check(args: argparse.Namespace) -> int:
    """Validate every file. Exit 1 on a fault in the files, 0 on a fault in the world.

    The split is the same one ``analysis/origins/validation.py`` draws: an
    ambiguous pair or a freight with no origin is a fault in the *files* and
    fails the command, so CI can hold the line. An expired entry is a fault in
    the *world* — it is reported, and renewing it is a decision with a person's
    name on it, not a build break.
    """
    from analysis.origins.validation import Severity, validate_set

    try:
        assumptions = _load()
    except AssumptionError as exc:
        # Includes a desk component found in a committed-tier file: the loader
        # refuses that at parse, so it fails here before anything else is said.
        print(f"INVALID: {exc}", file=sys.stderr)
        return 1
    today = _today()
    issues = validate_set(
        assumptions, on=today, expiry_horizon_days=args.horizon or EXPIRY_WARNING_DAYS
    )
    print(f"OK: {len(assumptions.assumptions)} assumption(s) parse, set {assumptions.set_id}")
    failed = False
    for issue in issues:
        marker = "ERROR" if issue.severity is Severity.ERROR else "note "
        print(f"  {marker} {issue}")
        if issue.remedy:
            print(f"        remedy: {issue.remedy}")
        failed = failed or issue.severity is Severity.ERROR
    # The private tier must be invisible to git. A tracked file under it means
    # somebody force-added one, or the ignore rule was edited; either way the
    # desk's numbers are one push from public.
    tracked = _tracked_private_files(private_assumptions_dir())
    if tracked is None:
        print("  note  git unavailable or the assumptions directory is outside the repo; "
              "tracked-file check skipped")
    elif tracked:
        for name in tracked:
            print(f"  ERROR {name} is TRACKED by git — a desk assumption file must never be")
        print(f"        remedy: git rm --cached -- {' '.join(tracked)}  (the ignore rule is "
              f"data/reference/assumptions/{PRIVATE_SUBDIR}/*.yml)")
        failed = True
    return 1 if failed else 0


def _tracked_private_files(private_dir: Path) -> list[str] | None:
    """Files under the private tier that git tracks. ``None`` when git cannot answer."""
    import subprocess

    try:
        relative = private_dir.resolve().relative_to(PROJECT_ROOT)
    except ValueError:
        return None
    try:
        out = subprocess.run(
            ["git", "ls-files", "--", str(relative)],
            cwd=PROJECT_ROOT, capture_output=True, text=True, check=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return None
    return [name for name in out.split("\n") if name and not name.endswith("README.md")]


def _window_arg(args: argparse.Namespace):
    """The window a report is run for: the one given, or the first forward month."""
    from analysis.origins.sources import offered_windows

    explicit = _parse_window(args.window)
    if explicit is not None:
        return explicit
    windows = offered_windows(_today())
    return windows[1] if len(windows) > 1 else windows[0]


def onboarding(args: argparse.Namespace) -> int:
    """The route checklist: what must be entered before each route is comparable.

    Reads no database. What a route requires is a property of its delivery term
    and the landed stack, so this answers on a fresh clone — which is exactly
    when it is needed.
    """
    from analysis.origins.readiness import assess_routes

    today = _today()
    window = _window_arg(args)
    destination = args.destination or next(iter(config.DESTINATION_PORTS))
    assumptions = _load()

    print(f"Route readiness · {config.DESTINATION_PORTS[destination]['name']} · "
          f"{window.describe()} · as of {today.isoformat()}")
    print(f"assumption set {assumptions.set_id}\n")

    ready = 0
    for route in assess_routes(
        assumptions, window=window, today=today, destination_key=destination
    ):
        if route.unavailable_reason:
            print(f"{route.label}\n  NOT COMPARABLE — {route.unavailable_reason}\n")
            continue
        if route.is_ready:
            ready += 1
        state = "READY" if route.is_ready else f"BLOCKED on {len(route.blocking)}"
        print(f"{route.label}  [{state}]")
        for requirement in route.requirements:
            mark = {
                "satisfied": "  ok  ",
                "expiring": " soon ",
                "expired": "LAPSED",
                "missing": "MISSING",
            }.get(requirement.status, requirement.status)
            detail = (
                f"{requirement.assumption_id} expires {requirement.expires_on} "
                f"({requirement.entered_by})"
                if requirement.assumption_id else requirement.unit
            )
            print(f"  {mark:<8} {requirement.component.value:<24} {detail}")
            if requirement.is_blocking:
                print(f"           {requirement.guidance}")
                print("           " + requirement.command.replace("\n", "\n           "))
        print()

    print(f"{ready} route(s) costable to {destination} for {window.describe()}.")
    print(
        "A route is 'ready' when every input it needs is entered and live. Being ready "
        "is not being ranked: an origin quoting a different shipment window, or none at "
        "all, is still not compared — that is a fact about the market, and it is stated "
        "on the comparison itself."
    )
    return 0


def review(args: argparse.Namespace) -> int:
    """The renewal queue. Exit 1 when something has already lapsed."""
    from analysis.origins.readiness import expiry_review, renewal_command

    today = _today()
    horizon = args.horizon or 30
    assumptions = _load()
    commands = {
        item.id: renewal_command(item, today=today) for item in assumptions.assumptions
    }
    result = expiry_review(
        assumptions,
        today=today,
        horizon_days=horizon,
        window=_window_arg(args),
        destination_key=args.destination,
    )
    print(f"Assumption review · as of {result['as_of']} · horizon {horizon}d · "
          f"routes judged for {result['window']} into {result['destination']}")
    if result["nothing_due"]:
        print("Nothing expired, nothing lapsing inside the horizon.")
        return 0
    for label, rows in (("EXPIRED", result["expired"]), ("LAPSING", result["expiring"])):
        for row in rows:
            blocked = ", ".join(row["routes_blocked"]) or "no route (shadowed or unused)"
            print(
                f"{label:<8} {row['id']:<44} {row['expires_on']} "
                f"({row['days_to_expiry']:+d}d) {row['entered_by']}\n"
                f"         blocks: {blocked}"
            )
            command = commands[row["id"]]
            print(f"         renew:  {command}" if command is not None else
                  "         renew:  — its shipment window has ended; enter the next window as a new entry")
    print(f"\n{result['note']}")
    return 1 if result["expired"] else 0


def gaps(_: argparse.Namespace) -> int:
    """What the origin page still needs before it can publish a ranking."""
    import sqlite3

    from analysis.origins.comparison import build_ranking, default_window

    today = _today()
    window = default_window(today)
    assumptions = _load()
    conn = sqlite3.connect(config.DB_PATH) if Path(config.DB_PATH).exists() else None
    try:
        for destination in config.DESTINATION_PORTS:
            ranking = build_ranking(
                conn, destination_key=destination, window=window, today=today,
                assumptions=assumptions,
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


# Add-only flags: `renew` never reads them (it copies these from the old row).
RENEW_IGNORES = (
    "--component", "--unit", "--days", "--origin", "--destination", "--window",
    "--source", "--confidence", "--citation", "--file",
)


def main(argv: list[str] | None = None) -> int:
    # allow_abbrev=False: argparse would otherwise expand `--orig` to `--origin`,
    # past the exact-token RENEW_IGNORES guard below, and the flag would be
    # dropped without a word (#618 Bugbot). A prefix is an unrecognised argument.
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter, allow_abbrev=False,
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--list", action="store_true", help="show every assumption and its state")
    mode.add_argument("--check", action="store_true", help="validate every file; exit 1 on error")
    mode.add_argument("--gaps", action="store_true", help="what the page still needs to rank")
    mode.add_argument(
        "--onboarding", action="store_true",
        help="per-route checklist: what must be entered before each route is comparable",
    )
    mode.add_argument(
        "--review", action="store_true",
        help="the renewal queue: what lapses, when, and which routes it takes down",
    )
    mode.add_argument(
        "--renew", metavar="ID",
        help="re-sign entry ID with --expires and --entered-by (and --value if it changed); "
             "the old row is retired in place",
    )

    parser.add_argument("--component", choices=[c.value for c in CostComponent])
    parser.add_argument("--value", type=float)
    parser.add_argument("--unit", help="defaults to the component's required unit")
    parser.add_argument("--days", type=int, help="carry days; required for rate_per_annum")
    parser.add_argument(
        "--horizon", type=int,
        help="how many days ahead --review and --check call out a lapse (review: 30, check: 14)",
    )
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
    parser.add_argument(
        "--file",
        help="target YAML file name inside the assumptions directory; a desk component "
             f"must be under {PRIVATE_SUBDIR}/",
    )

    args = parser.parse_args(argv)
    if args.list:
        return listing(args)
    if args.check:
        return check(args)
    if args.gaps:
        return gaps(args)
    if args.onboarding:
        return onboarding(args)
    if args.review:
        return review(args)
    if args.renew:
        # --renew keeps the old row's scope, unit, source, confidence and
        # citation; an add-only flag beside it would be dropped without a word,
        # and the renewal would quietly not be the one asked for.
        given = argv if argv is not None else sys.argv[1:]
        ignored = [
            flag for flag in RENEW_IGNORES
            if any(token == flag or token.startswith(flag + "=") for token in given)
        ]
        if ignored:
            parser.error(
                f"--renew re-signs the entry as it stands and cannot change {', '.join(ignored)} — "
                "enter a new assumption for a different scope"
            )
        missing_for_renewal = [
            name for name, value in (
                ("--expires", args.expires), ("--entered-by", args.entered_by),
            ) if value is None
        ]
        if missing_for_renewal:
            parser.error(
                f"--renew needs {', '.join(missing_for_renewal)} — a renewal is a new "
                "expiry with a person's name on it, not an automatic extension"
            )
        return renew(args)

    missing = [
        name for name, value in (
            ("--component", args.component),
            ("--value", args.value),
            ("--basis", args.basis),
            ("--entered-by", args.entered_by),
        ) if value is None
    ]
    # An omitted --expires is proposed from the component's default lifetime
    # (#410) — and only where config has one. A component outside the table,
    # or a window-bound one entered with no window, still needs the date given.
    args.expires_proposed = False
    if args.expires is None and args.component is not None:
        proposed = default_expiry(
            CostComponent(args.component),
            entered_at=args.entered_at or _today(),
            window=_parse_window(args.window),
        )
        if proposed is None:
            missing.append("--expires")
        else:
            args.expires = proposed
            args.expires_proposed = True
    if missing:
        parser.error(
            f"adding an assumption needs {', '.join(missing)} — an entry with no owner, "
            "reason or expiry is indistinguishable from a guess"
            + (
                f" ({args.component} has no default lifetime in "
                "config.ASSUMPTION_DEFAULT_LIFETIME_DAYS, so its expiry must be given)"
                if "--expires" in missing else ""
            )
        )
    return add(args)


if __name__ == "__main__":
    raise SystemExit(main())
