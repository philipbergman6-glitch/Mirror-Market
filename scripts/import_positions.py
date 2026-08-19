"""Import a broker, clearing or ERP export into the position book — dry run first.

The human end of :mod:`analysis.futures.imports`. **Nothing is written unless
``--apply`` is passed**, and even then a report with rejected rows is refused
unless ``--allow-partial`` says so out loud. That is not caution for its own
sake: a clean parse of the wrong thing is this workflow's real failure mode,
and reading the dry run is the only step that catches it.

Fully non-interactive — every input is a flag.

    # what profiles exist
    python scripts/import_positions.py --list-profiles

    # the dry run: what would be imported, what would be rejected, and why
    python scripts/import_positions.py --file exports/2026-08-19.csv --profile my-broker

    # write it into the book, as YAML, under data/reference/positions/
    python scripts/import_positions.py --file exports/2026-08-19.csv --profile my-broker \
      --apply --out data/reference/positions/imported-2026-08-19.yml

    # a partial import, having read why each row was dropped
    python scripts/import_positions.py --file ... --profile ... --apply --allow-partial

Exit codes: 0 clean, 1 anything was rejected (so CI can gate on it), 2 the file
or the profile could not be read at all.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402
from analysis.futures.imports import (  # noqa: E402
    ImportError_,
    ImportReport,
    apply_import,
    load_profile,
    load_profiles,
    read_import,
)
from analysis.futures.privacy import assert_private_path  # noqa: E402

RULE = "─" * 78


def _list_profiles(directory: str | None) -> int:
    profiles = load_profiles(directory)
    if not profiles:
        print(
            f"No import profiles in {directory or config.IMPORT_PROFILE_DIR}.\n"
            "A profile is a small YAML file mapping your export's column names onto this "
            "project's fields — see data/reference/import_profiles/README.md."
        )
        return 0
    for profile in profiles:
        print(f"{profile.name:24} {profile.kind:9} {profile.source}")
        print(f"{'':24} sign: {profile.quantity_sign}, dates: {profile.date_format}")
        print(f"{'':24} maps: {', '.join(f'{k}<-{v}' for k, v in profile.columns.items())}")
    return 0


def _print_report(report: ImportReport) -> None:
    print(RULE)
    print(f"  {report.path}")
    print(f"  profile {report.profile_name!r} from {report.source}")
    print(f"  sha256  {report.sha256}")
    print(RULE)
    print(f"  accepted {report.accepted_count}    rejected {report.rejected_count}")
    print()

    for row in report.accepted:
        subject = row.contract or row.commodity
        print(
            f"  ok   {row.reference}  {row.trade_date}  {subject:12} "
            f"{row.side:5} {row.quantity:>12,.2f} @ {row.price:>10,.4f} {row.unit}"
        )
    for bad in report.rejected:
        print(f"  DROP row {bad.row_number}: {bad.reason}")

    if report.unmapped_columns:
        print()
        print(f"  columns not claimed by the profile: {', '.join(report.unmapped_columns)}")
    for note in report.notes:
        print(f"  note: {note}")
    print(RULE)


def _to_yaml(book, report: ImportReport) -> str:
    """Render an imported book as a positions document.

    Written by hand rather than through ``yaml.safe_dump`` so the provenance
    comment survives — a file that cannot say where it came from is a file
    nobody can check against the statement it came from.
    """
    lines = [
        f"# imported from {report.path}",
        f"# profile: {report.profile_name} ({report.source})",
        f"# sha256: {report.sha256}",
        f"# accepted {report.accepted_count} row(s), rejected {report.rejected_count}",
        "# Review this before relying on it. Nothing here was inferred; every value",
        "# came from the export named above.",
        "",
    ]
    if book.futures:
        lines.append("futures:")
        for position in book.futures:
            lines.append(f"  - contract: {position.contract.symbol}")
            if position.account:
                lines.append(f"    account: {position.account}")
            lines.append("    fills:")
            for fill in position.fills:
                lines.append(
                    f"      - {{date: {fill.trade_date}, side: {fill.side.value}, "
                    f"quantity: {fill.quantity:g}, price: {fill.price:g}, "
                    f"reference: {fill.reference}}}"
                )
    if book.physical:
        lines.append("physical:")
        for position in book.physical:
            lines.append(f"  - commodity: {position.commodity}")
            lines.append(f"    quantity: {position.quantity:g}")
            lines.append(f"    unit: {position.unit.value}")
            lines.append(f"    side: {position.side.value}")
            if position.average_cost_usd_mt is not None:
                lines.append(f"    average_cost_usd_mt: {position.average_cost_usd_mt:g}")
            lines.append(f"    currency: {position.currency}")
            if position.location:
                lines.append(f"    location: {position.location}")
            lines.append(f"    note: {position.note}")
            lines.append(
                "    # pricing: state one of unpriced | basis_over_futures | "
                "formula_priced | flat_price"
            )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--file", help="the export to read")
    parser.add_argument("--profile", help="name of the import profile to read it with")
    parser.add_argument("--profile-dir", default=None, help="where profiles live")
    parser.add_argument("--list-profiles", action="store_true")
    parser.add_argument(
        "--apply", action="store_true",
        help="write the accepted rows out as a positions document (default: dry run only)",
    )
    parser.add_argument(
        "--allow-partial", action="store_true",
        help="apply even though rows were rejected — read the dry run first",
    )
    parser.add_argument("--out", default=None, help="where to write when --apply is given")
    args = parser.parse_args(argv)

    if args.list_profiles:
        return _list_profiles(args.profile_dir)

    if not args.file or not args.profile:
        parser.error("--file and --profile are both required (or use --list-profiles)")

    try:
        profile = load_profile(args.profile, directory=args.profile_dir)
        report = read_import(args.file, profile)
    except (ImportError_, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    _print_report(report)

    if not args.apply:
        print("  DRY RUN — nothing was written. Re-run with --apply to import.")
        return 0 if report.is_clean else 1

    try:
        book = apply_import(report, allow_partial=args.allow_partial)
    except ImportError_ as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    target = Path(args.out or (Path(config.POSITIONS_DIR) / f"imported-{report.sha256[:8]}.yml"))
    # The book is a client record. It may not be written anywhere the deploy
    # would upload, and that is checked rather than trusted to the default.
    assert_private_path(target, where="imported position book")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(_to_yaml(book, report), encoding="utf-8")
    print(f"  wrote {target}")
    print("  Review it — the pricing convention on each physical row is not in the export")
    print("  and has to be stated by you; until it is, tonnes are counted at their most")
    print("  exposed reading.")
    return 0 if report.is_clean else 1


if __name__ == "__main__":
    raise SystemExit(main())
