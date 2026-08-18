"""Rebuildable SQLite query cache for trusted observation revisions."""

from __future__ import annotations

import os
import sqlite3
import tempfile
from collections.abc import Iterable
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from trust.domain import Edition, ObservationRevision, QualityState
from trust.repository import RepositoryFormatError, TrustRepository

CacheMode = Literal["accepted", "edition"]


class TrustQueryCacheError(Exception):
    """Base error for SQLite query-cache operations."""


class TrustQueryCacheSelectionError(TrustQueryCacheError):
    """The requested cache selection cannot be resolved from durable records."""


@dataclass(frozen=True)
class QueryCacheBuild:
    cache_path: Path
    mode: CacheMode
    revision_count: int
    edition_id: str | None = None
    include_legacy: bool = False


def build_query_cache(
    repository: TrustRepository,
    cache_path: str | os.PathLike[str],
    *,
    mode: CacheMode = "accepted",
    edition_id: str | None = None,
    include_legacy: bool = False,
) -> QueryCacheBuild:
    """Atomically rebuild ``cache_path`` from durable trust records.

    Git records remain authoritative.  The SQLite file is deleted and replaced
    on each build, making the cache safe to discard and recreate.
    """

    path = Path(cache_path)
    if mode == "accepted":
        if edition_id is not None:
            raise TrustQueryCacheSelectionError("accepted cache builds cannot specify an edition_id")
        revisions = _accepted_cache_revisions(repository, include_legacy=include_legacy)
        selected_edition_id = None
    elif mode == "edition":
        if edition_id is None:
            raise TrustQueryCacheSelectionError("edition cache builds require an edition_id")
        revisions = _edition_cache_revisions(repository, edition_id, include_legacy=include_legacy)
        selected_edition_id = edition_id
    else:
        raise TrustQueryCacheSelectionError(f"unsupported cache mode: {mode!r}")

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    try:
        _write_cache(
            temporary_path,
            revisions,
            mode=mode,
            edition_id=selected_edition_id,
            include_legacy=include_legacy,
        )
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)
    return QueryCacheBuild(
        cache_path=path,
        mode=mode,
        revision_count=len(revisions),
        edition_id=selected_edition_id,
        include_legacy=include_legacy,
    )


def _accepted_cache_revisions(
    repository: TrustRepository,
    *,
    include_legacy: bool,
) -> tuple[ObservationRevision, ...]:
    by_observation: dict[str, list[ObservationRevision]] = {}
    legacy: list[ObservationRevision] = []
    for revision in repository.all_observation_revisions():
        if revision.quality_state is QualityState.LEGACY:
            if include_legacy:
                legacy.append(revision)
            continue
        by_observation.setdefault(revision.identity.observation_id, []).append(revision)

    selected = [
        revision
        for revisions in by_observation.values()
        if (revision := _accepted_head(tuple(revisions))) is not None
    ]
    return _stable_revision_tuple((*selected, *legacy))


def _edition_cache_revisions(
    repository: TrustRepository,
    edition_id: str,
    *,
    include_legacy: bool,
) -> tuple[ObservationRevision, ...]:
    edition = repository.read(Edition, edition_id)
    if edition is None:
        raise TrustQueryCacheSelectionError(f"edition {edition_id} is not durable")
    revision_ids = (*edition.revision_ids, *edition.derived_revision_ids)
    revisions: list[ObservationRevision] = []
    for revision_id in revision_ids:
        revision = repository.read(ObservationRevision, revision_id)
        if revision is None:
            raise RepositoryFormatError(f"edition {edition_id} references missing revision {revision_id}")
        if _cache_eligible(revision, include_legacy=include_legacy):
            revisions.append(revision)
    return _stable_revision_tuple(revisions)


def _accepted_head(revisions: tuple[ObservationRevision, ...]) -> ObservationRevision | None:
    superseded_ids = {
        revision.supersedes_revision_id
        for revision in revisions
        if revision.quality_state is QualityState.ACCEPTED
        and revision.public_eligible
        and revision.supersedes_revision_id is not None
    }
    eligible = [
        revision
        for revision in revisions
        if revision.quality_state is QualityState.ACCEPTED
        and revision.public_eligible
        and revision.revision_id not in superseded_ids
    ]
    return sorted(eligible, key=lambda revision: (revision.ingested_at, revision.revision_id))[-1] if eligible else None


def _cache_eligible(revision: ObservationRevision, *, include_legacy: bool) -> bool:
    if revision.quality_state is QualityState.ACCEPTED and revision.public_eligible:
        return True
    return include_legacy and revision.quality_state is QualityState.LEGACY


def _stable_revision_tuple(revisions: Iterable[ObservationRevision]) -> tuple[ObservationRevision, ...]:
    unique = {revision.revision_id: revision for revision in revisions}
    return tuple(sorted(unique.values(), key=lambda revision: revision.revision_id))


def _write_cache(
    cache_path: Path,
    revisions: tuple[ObservationRevision, ...],
    *,
    mode: CacheMode,
    edition_id: str | None,
    include_legacy: bool,
) -> None:
    with closing(sqlite3.connect(cache_path)) as conn, conn:
        _populate_cache(
            conn,
            revisions,
            mode=mode,
            edition_id=edition_id,
            include_legacy=include_legacy,
        )


def _populate_cache(
    conn: sqlite3.Connection,
    revisions: tuple[ObservationRevision, ...],
    *,
    mode: CacheMode,
    edition_id: str | None,
    include_legacy: bool,
) -> None:
    """Populate an already-owned cache connection inside its transaction."""
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        """CREATE TABLE cache_metadata (
               key TEXT PRIMARY KEY,
               value TEXT NOT NULL
           )"""
    )
    conn.executemany(
        "INSERT INTO cache_metadata (key, value) VALUES (?, ?)",
        (
            ("mode", mode),
            ("edition_id", edition_id or ""),
            ("include_legacy", "1" if include_legacy else "0"),
            ("revision_count", str(len(revisions))),
        ),
    )
    conn.execute(
        """CREATE TABLE trusted_observations (
               revision_id TEXT PRIMARY KEY,
               observation_id TEXT NOT NULL,
               source_id TEXT NOT NULL,
               dataset_id TEXT NOT NULL,
               dataset_key TEXT NOT NULL,
               effective_date TEXT NOT NULL,
               commodity TEXT NOT NULL,
               product_form TEXT NOT NULL,
               venue TEXT,
               location TEXT,
               price_type TEXT NOT NULL,
               currency TEXT NOT NULL,
               unit TEXT NOT NULL,
               value TEXT NOT NULL,
               open_value TEXT,
               high_value TEXT,
               low_value TEXT,
               close_value TEXT,
               volume TEXT,
               quality_state TEXT NOT NULL,
               public_eligible INTEGER NOT NULL,
               ingested_at TEXT NOT NULL,
               artifact_id TEXT,
               parser_version TEXT,
               source_record_id TEXT,
               supersedes_revision_id TEXT
           )"""
    )
    conn.executemany(
        """INSERT INTO trusted_observations (
               revision_id, observation_id, source_id, dataset_id, dataset_key,
               effective_date, commodity, product_form, venue, location, price_type,
               currency, unit, value, open_value, high_value, low_value, close_value,
               volume, quality_state, public_eligible, ingested_at, artifact_id,
               parser_version, source_record_id, supersedes_revision_id
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        tuple(_revision_row(revision) for revision in revisions),
    )
    conn.execute("CREATE INDEX idx_trusted_observations_identity ON trusted_observations (observation_id)")
    conn.execute(
        "CREATE INDEX idx_trusted_observations_dataset_date "
        "ON trusted_observations (dataset_id, effective_date)"
    )


def _revision_row(revision: ObservationRevision) -> tuple[object, ...]:
    identity = revision.identity
    payload = revision.to_dict()
    return (
        revision.revision_id,
        identity.observation_id,
        identity.source_id,
        identity.dataset_id,
        identity.dataset_key,
        identity.effective_date.isoformat(),
        identity.commodity,
        identity.product_form,
        identity.venue,
        identity.location,
        identity.price_type,
        identity.currency,
        identity.unit,
        payload["value"],
        payload["open_value"],
        payload["high_value"],
        payload["low_value"],
        payload["close_value"],
        payload["volume"],
        revision.quality_state.value,
        1 if revision.public_eligible else 0,
        revision.ingested_at.isoformat(),
        revision.artifact.artifact_id if revision.artifact else None,
        revision.parser_version,
        identity.source_record_id,
        revision.supersedes_revision_id,
    )


__all__ = [
    "QueryCacheBuild",
    "TrustQueryCacheError",
    "TrustQueryCacheSelectionError",
    "build_query_cache",
]
