"""Compare a private cargo case and save a versioned decision under data/workspace/cargo."""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
from contextlib import closing
from pathlib import Path
from tempfile import NamedTemporaryFile

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402
from analysis.futures.privacy import AUDIENCE_PRIVATE, assert_private_path  # noqa: E402
from analysis.futures.providers import SqliteQuoteProvider  # noqa: E402
from analysis.origins.assumptions import load_assumptions  # noqa: E402
from analysis.origins.cargo import CARGO_WORKSPACE, evaluate_case  # noqa: E402

WORKSPACE = CARGO_WORKSPACE


def _private_file(*parts: str) -> Path:
    path = WORKSPACE.joinpath(*parts)
    if assert_private_path(path, where="cargo decision") != path:
        raise ValueError("cargo files must remain inside the private workspace without symlinks")
    return path


def _save_decision(target: Path, document: str) -> None:
    target.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    with NamedTemporaryFile(dir=target.parent, mode="w", encoding="utf-8") as temporary:
        temporary.write(document)
        temporary.flush()
        os.fsync(temporary.fileno())
        # Exclusive link publishes a complete snapshot. An existing id is an
        # earlier decision and must survive concurrent runs and retries.
        os.link(temporary.name, target)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("case_id", help="private input file data/workspace/cargo/<case-id>.json")
    parser.add_argument("--save", metavar="DECISION_ID", help="save inputs and result under decisions/")
    args = parser.parse_args(argv)
    for name in (args.case_id, args.save):
        if name is not None and not re.fullmatch(r"[a-zA-Z0-9_-]{1,80}", name):
            parser.error("case and decision ids must be 1–80 letters, digits, underscores or hyphens")

    try:
        case = json.loads(_private_file(f"{args.case_id}.json").read_text(encoding="utf-8"))
        if not isinstance(case, dict):
            raise ValueError("cargo case file must contain a JSON object")
        db_uri = Path(config.DB_PATH).resolve().as_uri() + "?mode=ro"
        assumptions = load_assumptions(audience=AUDIENCE_PRIVATE)
        if Path(config.DB_PATH).exists():
            with closing(sqlite3.connect(db_uri, uri=True)) as conn:
                provider = SqliteQuoteProvider(conn)
                result = evaluate_case(
                    case, assumptions, lambda contract, as_of: provider.quote(contract, as_of=as_of)
                )
        else:
            result = evaluate_case(case, assumptions, lambda contract, as_of: None)
        document = json.dumps(
            {"schema_version": 1, "case_id": args.case_id, "case": case, "result": result},
            ensure_ascii=False, indent=2, allow_nan=False,
        ) + "\n"
        if args.save:
            target = _private_file("decisions", f"{args.save}.json")
            _save_decision(target, document)
            print(f"Saved {target}")
        else:
            sys.stdout.write(document)
        return 0
    except (OSError, RuntimeError, TypeError, ValueError, KeyError, sqlite3.Error) as exc:
        parser.exit(1, f"Cargo case error: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
