"""Render a durable trust edition into static candidate output."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.generate_html import static_site_candidate_renderer, static_site_deployer  # noqa: E402
from trust import (  # noqa: E402
    CandidateEditionRender,
    Edition,
    EditionStatus,
    TemporaryDirectoryTrustRepository,
    TrustRepository,
    build_query_cache,
    edition_public_trust_state,
)

GeneratedArtifactPaths = Mapping[str, str | Path] | tuple[Path, ...]
TrustedStaticRenderer = Callable[[Path, Path, Edition], GeneratedArtifactPaths]
TrustedStaticDeployer = Callable[[Edition, CandidateEditionRender], Iterable[str] | None]


@dataclass(frozen=True)
class TrustedStaticPublishResult:
    """Evidence from rendering, and optionally deploying, one trusted edition."""

    edition_id: str
    candidate_output_dir: Path
    cache_path: Path
    generated_artifact_paths: Mapping[str, Path]
    deployed: bool = False
    deployment_evidence: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "edition_id": self.edition_id,
            "candidate_output_dir": str(self.candidate_output_dir),
            "cache_path": str(self.cache_path),
            "generated_artifact_paths": {
                key: str(path) for key, path in sorted(self.generated_artifact_paths.items())
            },
            "deployed": self.deployed,
            "deployment_evidence": list(self.deployment_evidence),
        }


def publish_trusted_static_edition(
    *,
    trust_repository_path: str | Path,
    edition_id: str,
    candidate_root: str | Path,
    deploy: bool = False,
    public_dir: str | Path | None = None,
    include_players: bool = False,
    repository_factory: Callable[[str | Path], TrustRepository] = TemporaryDirectoryTrustRepository,
    renderer_factory: Callable[..., TrustedStaticRenderer] | None = None,
    deployer_factory: Callable[..., TrustedStaticDeployer] | None = None,
) -> TrustedStaticPublishResult:
    """Render a candidate or verified edition without mutating public output by default."""

    repository = repository_factory(trust_repository_path)
    edition = repository.read(Edition, edition_id)
    if edition is None:
        raise ValueError(f"edition {edition_id} is not durable")
    if edition.status not in {EditionStatus.CANDIDATE, EditionStatus.VERIFIED}:
        raise ValueError("trusted static publish requires a candidate or verified edition")

    trust_state = edition_public_trust_state(repository, edition.edition_id)
    renderer_factory = renderer_factory or static_site_candidate_renderer
    renderer = renderer_factory(public_trust_state=trust_state, include_players=include_players)
    render = _render_trusted_static_edition(
        repository,
        edition,
        candidate_root,
        renderer,
    )

    deployment_evidence: tuple[str, ...] = ()
    if deploy:
        deployer_factory = deployer_factory or static_site_deployer
        deployer = deployer_factory(public_dir=public_dir) if public_dir is not None else deployer_factory()
        deployment_evidence = tuple(deployer(edition, render) or ())

    return TrustedStaticPublishResult(
        edition_id=edition.edition_id,
        candidate_output_dir=render.output_dir,
        cache_path=render.cache_build.cache_path,
        generated_artifact_paths=render.generated_artifact_paths,
        deployed=deploy,
        deployment_evidence=deployment_evidence,
    )


def _render_trusted_static_edition(
    repository: TrustRepository,
    edition: Edition,
    candidate_root: str | Path,
    renderer: TrustedStaticRenderer,
) -> CandidateEditionRender:
    output_dir = Path(candidate_root) / edition.edition_id
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_build = build_query_cache(
        repository,
        output_dir / "trusted-query-cache.sqlite",
        mode="edition",
        edition_id=edition.edition_id,
    )
    generated_paths = _generated_artifact_paths(renderer(cache_build.cache_path, output_dir, edition))
    return CandidateEditionRender(
        edition_id=edition.edition_id,
        output_dir=output_dir,
        cache_build=cache_build,
        generated_artifact_paths=generated_paths,
    )


def _generated_artifact_paths(paths: GeneratedArtifactPaths) -> dict[str, Path]:
    if isinstance(paths, Mapping):
        return {str(key): Path(path) for key, path in paths.items()}
    return {path.stem: path for path in paths}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trust-repository", required=True, help="Path to the durable trust repository directory")
    parser.add_argument("--edition-id", required=True, help="Candidate or verified edition identifier to render")
    parser.add_argument("--candidate-root", required=True, help="Root directory for candidate static output")
    parser.add_argument("--deploy", action="store_true", help="Copy rendered artifacts to the public output directory")
    parser.add_argument("--public-dir", help="Public output directory used only with --deploy")
    parser.add_argument("--include-players", action="store_true", help="Render the players page with the dashboard")
    args = parser.parse_args(argv)

    result = publish_trusted_static_edition(
        trust_repository_path=args.trust_repository,
        edition_id=args.edition_id,
        candidate_root=args.candidate_root,
        deploy=args.deploy,
        public_dir=args.public_dir,
        include_players=args.include_players,
    )
    print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
