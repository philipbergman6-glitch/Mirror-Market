#!/usr/bin/env python
"""Write the paste-in daily edition for a trial participant whose chat client cannot speak MCP.

    python scripts/trial_package.py                      # today's DB → data/workspace/trial/package/<date>/
    python scripts/trial_package.py --db path.db --out DIR --built-at 2026-11-06T19:00:00+00:00

Every number is a ``mirror_mcp`` tool envelope (see ``mirror_mcp.package``). The
output directory is refused if it would be published (``docs/``). A database no
run has ever stamped is refused rather than packaged as an empty edition.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import config  # noqa: E402
from mirror_mcp.package import build_package  # noqa: E402
from mirror_mcp.settings import Settings  # noqa: E402

# The package never serves a URL; the token only satisfies Settings' startup check.
_NO_URL = "paste-in-edition-no-url-" + "0" * 16  # public-sample-key: not a secret


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=Path(config.DB_PATH),
                        help="edition database (default: config.DB_PATH)")
    parser.add_argument("--out", type=Path, default=None,
                        help="output directory (default: config.TRIAL_PRIVATE_OUTPUT_DIR/package/<today>)")
    parser.add_argument("--built-at", default=None, help="ISO stamp of the DB build (default: now, UTC)")
    args = parser.parse_args(argv)

    now = datetime.now(timezone.utc)
    out = args.out or Path(config.TRIAL_PRIVATE_OUTPUT_DIR) / "package" / now.date().isoformat()
    settings = Settings(
        db_path=args.db, path_token=_NO_URL, log_path=out / "question_log.jsonl",
        db_built_at=args.built_at or now.isoformat(timespec="seconds"),
    ).validated()
    upload = build_package(settings, out)
    print(f"wrote {upload} ({upload.stat().st_size // 1024} KB) and {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
