"""The fast refresh: reprice the board, rebuild the site, prove it is not a regression.

    python scripts/refresh_prices.py --output-dir /tmp/candidate \
        --public-manifest https://<pages-host>/manifest.json

Three steps, and the third is the reason this is a script rather than three
workflow steps.

1. ``main.run(FAST_REFRESH_LAYERS, FAST_REFRESH_HISTORY_PERIOD)`` — prices,
   FX and the forward curve over a short window. Same code as the daily
   build with two arguments different, so the settlement guard, the cleaners
   and the freshness grading cannot diverge between the two paths.
   ``--layers`` overrides the layer set for schedule-specific runs — the
   08:00 UTC slot passes ``dce``, because Dalian closes 15:00 CST (07:00
   UTC) and an evening fetch is 13-17h late against a 6h objective.
2. Generate the whole site into a private candidate directory.
3. Gate it twice: the existing structural contract
   (``verify_site_candidate``), and then
   ``verify_refresh_is_not_a_regression`` against the manifest of the edition
   currently published.

Gate 3 is what makes a fast refresh safe to run unattended. A fast refresh
fetches three layers; everything else on the site comes from whatever the
database already holds. On a fresh CI runner that database is seeded only
from the committed history CSVs, so a fast refresh with no restored database
would render a complete, structurally valid, internally consistent site that
has silently dropped PSD, weather, COT, crop progress and every other layer
the CSVs do not carry. Nothing in the HTML says so. Only the comparison
against the live edition does, and it refuses.

The failure mode is a **no-op**, deliberately: the last trusted edition stays
up untouched. A fast refresh that cannot prove it is an improvement does not
get to publish, and nothing about the currently published site changes.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from app.manifest import MANIFEST_FILENAME  # noqa: E402
from config import (  # noqa: E402
    FAST_REFRESH_HISTORY_PERIOD,
    FAST_REFRESH_LAYERS,
    setup_logging,
)
from trust.site_promotion import (  # noqa: E402
    verify_refresh_is_not_a_regression,
    verify_site_candidate,
)

log = logging.getLogger("refresh_prices")

EXIT_OK = 0
EXIT_PIPELINE_FAILED = 1
EXIT_GENERATION_FAILED = 2
EXIT_CONTRACT_FAILED = 3
EXIT_REGRESSION = 4


def _fetch_public_manifest(source: str | None) -> dict | None:
    """Read the published edition's manifest, or None if there is not one.

    A 404 is a legal answer — the first fast refresh after this ships runs
    against a site that predates the manifest — and is distinguished in the
    log from a transport failure, which is *not* treated as "no manifest".
    Both return None, because there is genuinely nothing to compare against
    either way; the difference is whether an operator should investigate.
    """
    if not source:
        return None
    try:
        if source.startswith(("http://", "https://")):
            with urlopen(source, timeout=30) as response:  # noqa: S310 — explicit target
                return json.loads(response.read().decode("utf-8"))
        return json.loads(Path(source).read_text(encoding="utf-8"))
    except HTTPError as exc:
        if exc.code == 404:
            log.warning(
                "no published manifest at %s (HTTP 404) — the regression gate has "
                "nothing to compare against and will pass this build",
                source,
            )
        else:
            log.error("could not read the published manifest at %s: HTTP %s", source, exc.code)
        return None
    except (URLError, OSError, ValueError):
        log.error("could not read the published manifest at %s", source, exc_info=True)
        return None


def _load_candidate_pages(root: Path) -> dict[str, str]:
    from trust.site_promotion import expected_site_paths

    return {
        path: (root / path).read_text(encoding="utf-8")
        for path in expected_site_paths()
        if (root / path).is_file()
    }


def refresh(
    *,
    output_dir: Path,
    public_manifest_source: str | None,
    skip_pipeline: bool = False,
    layers: tuple[str, ...] | None = None,
) -> int:
    setup_logging()

    selected = tuple(layers) if layers else FAST_REFRESH_LAYERS
    if not skip_pipeline:
        import main as pipeline_main

        log.info("fast refresh: %s over %s", ", ".join(selected),
                 FAST_REFRESH_HISTORY_PERIOD)
        code = pipeline_main.run(
            layer_keys=selected,
            history_period=FAST_REFRESH_HISTORY_PERIOD,
        )
        if code != 0:
            log.error("fast pipeline exited %s — not generating a candidate", code)
            return EXIT_PIPELINE_FAILED

    from scripts.generate_site import generate_site

    try:
        results = generate_site(output_dir=output_dir)
    except Exception:
        log.exception("candidate generation failed")
        return EXIT_GENERATION_FAILED

    tombstoned = [r for r in results if not r.ok]
    if tombstoned:
        # The daily build tolerates a tombstone in the candidate as a
        # diagnostic and then refuses to promote it. A fast refresh has no
        # such intermediate use — nobody reads its artifact — so it stops here.
        log.error(
            "candidate carries %d tombstone(s): %s",
            len(tombstoned), ", ".join(r.name for r in tombstoned),
        )
        return EXIT_GENERATION_FAILED

    verdict = verify_site_candidate(_load_candidate_pages(output_dir))
    if not verdict.verified:
        for failure in verdict.failures:
            log.error("promotion contract: %s", failure)
        return EXIT_CONTRACT_FAILED

    candidate_manifest_path = output_dir / MANIFEST_FILENAME
    try:
        candidate_manifest = json.loads(candidate_manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        log.error("candidate manifest is missing or unreadable at %s", candidate_manifest_path)
        return EXIT_CONTRACT_FAILED

    refresh_verdict = verify_refresh_is_not_a_regression(
        candidate_manifest, _fetch_public_manifest(public_manifest_source)
    )
    if not refresh_verdict.promotable:
        log.error(
            "REFUSING to promote — this candidate knows less than the edition it "
            "would replace. The published site is unchanged."
        )
        for failure in refresh_verdict.failures:
            log.error("regression: %s", failure)
        return EXIT_REGRESSION

    log.info("candidate at %s is promotable", output_dir)
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the price-only refresh.")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--public-manifest",
        help=(
            "URL or path of the currently published manifest.json. Omitted, the "
            "regression gate has nothing to compare against and passes."
        ),
    )
    parser.add_argument(
        "--skip-pipeline",
        action="store_true",
        help="generate and gate only, against the database as it stands",
    )
    parser.add_argument(
        "--layers",
        help=(
            "comma-separated layer keys to fetch instead of "
            "config.FAST_REFRESH_LAYERS (e.g. 'dce' for the morning Dalian "
            "run). Unknown keys hard-fail in main.run, never run short."
        ),
    )
    args = parser.parse_args(argv)
    layers = tuple(k.strip() for k in args.layers.split(",") if k.strip()) if args.layers else None
    return refresh(
        output_dir=args.output_dir,
        public_manifest_source=args.public_manifest,
        skip_pipeline=args.skip_pipeline,
        layers=layers,
    )


if __name__ == "__main__":
    raise SystemExit(main())
