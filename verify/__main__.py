"""``python -m verify <check> …`` — one subcommand per scripted check."""

from __future__ import annotations

import sys

from verify import fx_cny

CHECKS = {"fx-cny": fx_cny.main}


def main(argv: list[str]) -> int:
    if not argv or argv[0] not in CHECKS:
        print(f"usage: python -m verify {{{','.join(CHECKS)}}} [options]", file=sys.stderr)
        return 2
    return CHECKS[argv[0]](argv[1:])


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
