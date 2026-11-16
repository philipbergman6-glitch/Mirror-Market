"""Durable database snapshots in Cloudflare R2 — the run's *seed*, not its store.

Before this module the daily build started from an empty SQLite file and
re-downloaded every source, so the full database only ever lived in an
evictable Actions cache. Now a run whose candidate passed the promotion
contract pushes its database here, and the next daily build starts from it.

Two authorities, deliberately split (invariant 6 still holds — one backend,
SQLite; R2 holds a *file*, never a queried database):

* ``data/history/*.csv`` stays the authority for every ``HISTORY_TABLES``
  table. A restored seed has those tables **emptied** before the pipeline's
  ``import_history()`` runs, because import only inserts/replaces: a row a
  later run deliberately removed (``layer_partial_streak`` treats an absent
  row as the state itself) would otherwise be resurrected from the seed.
* The seed supplies everything else — the self-healing tables (prices,
  weather, PSD, COT, ...) that an empty runner previously had to rebuild.

States are distinct, never collapsed (invariant 1):

* ``unconfigured`` — no R2 environment at all. The run behaves exactly as it
  did before this module (CSV-only rebuild). Said out loud by the caller.
* ``no_seed`` — configured, but nothing has been pushed yet (first run).
* ``restored`` — a verified, checksummed, integrity-checked seed is in place.

Anything else — a partial configuration, a checksum mismatch, a corrupt
file, a database already present at the target — raises ``SnapshotError``
and places nothing.
"""

from __future__ import annotations

import gzip
import hashlib
import logging
import os
import shutil
import sqlite3
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Protocol

from pipeline.history import HISTORY_TABLES

logger = logging.getLogger(__name__)

ENV_ACCOUNT_ID = "R2_ACCOUNT_ID"
ENV_ACCESS_KEY_ID = "R2_ACCESS_KEY_ID"
ENV_SECRET_ACCESS_KEY = "R2_SECRET_ACCESS_KEY"
ENV_BUCKET = "R2_BUCKET"
REQUIRED_ENV = (ENV_ACCOUNT_ID, ENV_ACCESS_KEY_ID, ENV_SECRET_ACCESS_KEY, ENV_BUCKET)

# The newest verified seed. Overwritten on every push, always *after* the
# dated copy has landed, so it never points at a half-written object.
LATEST_KEY = "verified/latest.db.gz"

# Object metadata keys (S3 user metadata is lower-cased by the service).
META_SHA256 = "sha256"
META_RUN_ID = "run-id"
META_BUILT_AT = "built-at"
META_DB_BYTES = "db-bytes"

_CHUNK = 1024 * 1024


class SnapshotError(RuntimeError):
    """The snapshot could not be pushed or restored; nothing was placed."""


@dataclass(frozen=True)
class R2Config:
    account_id: str
    access_key_id: str
    secret_access_key: str = field(repr=False)
    bucket: str

    @property
    def endpoint(self) -> str:
        return f"https://{self.account_id}.r2.cloudflarestorage.com"


def config_from_env(env: Mapping[str, str] | None = None) -> R2Config | None:
    """R2 settings from the environment; ``None`` when none are set.

    All four or none: a partial set is a misconfigured secret, not a reason
    to quietly fall back to the CSV-only rebuild.
    """
    source = os.environ if env is None else env
    present = {k: source.get(k, "").strip() for k in REQUIRED_ENV}
    if not any(present.values()):
        return None
    missing = [k for k, v in present.items() if not v]
    if missing:
        raise SnapshotError(f"partial R2 configuration — missing: {', '.join(missing)}")
    return R2Config(
        account_id=present[ENV_ACCOUNT_ID],
        access_key_id=present[ENV_ACCESS_KEY_ID],
        secret_access_key=present[ENV_SECRET_ACCESS_KEY],
        bucket=present[ENV_BUCKET],
    )


class ObjectStore(Protocol):
    """The two operations a snapshot needs. ``get`` returns ``None`` when absent."""

    def put(self, key: str, path: Path, metadata: dict[str, str]) -> None: ...

    def get(self, key: str, path: Path) -> dict[str, str] | None: ...


class R2Store:
    """``ObjectStore`` over R2's S3-compatible API (boto3, imported lazily)."""

    def __init__(self, config: R2Config) -> None:
        import boto3
        from botocore.config import Config

        self._bucket = config.bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=config.endpoint,
            aws_access_key_id=config.access_key_id,
            aws_secret_access_key=config.secret_access_key,
            region_name="auto",
            config=Config(
                # R2 rejected boto3 >= 1.36's default integrity headers when
                # they shipped; "when_required" sends only what R2 accepts.
                # Integrity here is our own sha256 in the object metadata.
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
                retries={"max_attempts": 5, "mode": "standard"},
            ),
        )

    def put(self, key: str, path: Path, metadata: dict[str, str]) -> None:
        self._client.upload_file(
            str(path),
            self._bucket,
            key,
            ExtraArgs={"Metadata": metadata, "ContentType": "application/gzip"},
        )

    def get(self, key: str, path: Path) -> dict[str, str] | None:
        from botocore.exceptions import ClientError

        try:
            head = self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        self._client.download_file(self._bucket, key, str(path))
        return {str(k).lower(): str(v) for k, v in head.get("Metadata", {}).items()}


def daily_key(built_at: datetime, run_id: str) -> str:
    """Dated archive key: one object per pushing run, never overwritten."""
    if built_at.tzinfo is None:
        raise SnapshotError("built_at must be timezone-aware")
    day = built_at.astimezone(timezone.utc).strftime("%Y-%m-%d")
    return f"daily/{day}/run-{run_id}.db.gz"


def verify_sqlite(path: Path) -> int:
    """Hard-fail unless ``path`` is an intact SQLite database with tables.

    Returns the table count. An empty file is a valid SQLite database to
    ``sqlite3`` — exactly the stray 0-byte file this project once grew — so
    "no tables" is refused as firmly as a failed integrity check.
    """
    if not path.is_file():
        raise SnapshotError(f"{path}: no database file")
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            result = conn.execute("PRAGMA integrity_check").fetchall()
            tables = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'"
            ).fetchone()[0]
        finally:
            conn.close()
    except sqlite3.DatabaseError as exc:
        raise SnapshotError(f"{path}: not a SQLite database ({exc})") from exc
    if result != [("ok",)]:
        raise SnapshotError(f"{path}: integrity_check failed: {result[:5]}")
    if tables == 0:
        raise SnapshotError(f"{path}: database has no tables")
    return int(tables)


def clear_history_tables(path: Path) -> list[str]:
    """Empty every HISTORY_TABLES table present in ``path``; return their names.

    The committed CSVs are the authority for these tables, and
    ``import_history()`` only inserts — so the seed's copy must be gone
    before it runs, or a row a later run removed comes back.
    """
    conn = sqlite3.connect(str(path))
    try:
        present = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        cleared = sorted(t for t in HISTORY_TABLES if t in present)
        with conn:
            for table in cleared:
                conn.execute(f'DELETE FROM "{table}"')
    finally:
        conn.close()
    return cleared


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class PushResult:
    keys: tuple[str, ...]
    sha256: str
    gz_bytes: int


def push(db_path: Path, store: ObjectStore, *, run_id: str, built_at: datetime) -> PushResult:
    """Upload ``db_path`` as a dated archive, then as the latest verified seed."""
    if not run_id.strip():
        raise SnapshotError("run_id is required")
    dated = daily_key(built_at, run_id)
    verify_sqlite(db_path)
    with tempfile.TemporaryDirectory() as tmp:
        gz = Path(tmp) / "snapshot.db.gz"
        with db_path.open("rb") as src, gzip.open(gz, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst, _CHUNK)
        sha = _sha256(gz)
        metadata = {
            META_SHA256: sha,
            META_RUN_ID: run_id,
            META_BUILT_AT: built_at.astimezone(timezone.utc).isoformat(),
            META_DB_BYTES: str(db_path.stat().st_size),
        }
        store.put(dated, gz, metadata)
        store.put(LATEST_KEY, gz, metadata)
        size = gz.stat().st_size
    logger.info("Snapshot pushed: %s and %s (%d bytes gz, sha256 %s)", dated, LATEST_KEY, size, sha)
    return PushResult(keys=(dated, LATEST_KEY), sha256=sha, gz_bytes=size)


@dataclass(frozen=True)
class PullResult:
    state: Literal["no_seed", "restored"]
    metadata: dict[str, str]
    cleared_tables: tuple[str, ...] = ()


def pull(db_path: Path, store: ObjectStore, *, key: str = LATEST_KEY) -> PullResult:
    """Restore the seed at ``key`` to ``db_path``, history tables emptied.

    Refuses to overwrite an existing database: on a fresh runner there is
    none, and anywhere else a silent replace would destroy local state.
    """
    if db_path.exists():
        raise SnapshotError(f"{db_path} already exists — refusing to overwrite it with a seed")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=db_path.parent) as tmp:
        gz = Path(tmp) / "seed.db.gz"
        metadata = store.get(key, gz)
        if metadata is None:
            logger.info("Snapshot: no object at %s — first run, no seed", key)
            return PullResult(state="no_seed", metadata={})
        expected = metadata.get(META_SHA256, "")
        if not expected:
            raise SnapshotError(f"{key}: object carries no sha256 metadata")
        actual = _sha256(gz)
        if actual != expected:
            raise SnapshotError(f"{key}: sha256 mismatch (metadata {expected}, file {actual})")
        staged = Path(tmp) / "seed.db"
        try:
            with gzip.open(gz, "rb") as src, staged.open("wb") as dst:
                shutil.copyfileobj(src, dst, _CHUNK)
        except (OSError, EOFError) as exc:
            raise SnapshotError(f"{key}: cannot decompress ({exc})") from exc
        verify_sqlite(staged)
        cleared = clear_history_tables(staged)
        os.replace(staged, db_path)
    logger.info(
        "Snapshot restored from %s (run %s, built %s); %d history tables left to the CSVs",
        key,
        metadata.get(META_RUN_ID, "?"),
        metadata.get(META_BUILT_AT, "?"),
        len(cleared),
    )
    return PullResult(state="restored", metadata=metadata, cleared_tables=tuple(cleared))
