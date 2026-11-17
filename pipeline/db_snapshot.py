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
* ``stale_seed`` — only when the caller requires a fresh seed (the quick
  refresh): the seed is older than ``SEED_SHORT_WINDOW_MAX_AGE_DAYS`` and was
  not placed, exactly as if there were none.

**The history window.** A restored seed no older than
``SEED_SHORT_WINDOW_MAX_AGE_DAYS`` already holds the price and FX history a
daily build would otherwise re-download over ``DEFAULT_HISTORY_PERIOD``, so
``history_period_for`` grants the short ``FAST_REFRESH_HISTORY_PERIOD``
window. Every other start keeps the full download. ``assert_short_window_safe``
is the backstop: the short window is refused on a database that does not
actually hold older price history.

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
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Literal, Protocol

from config import (
    DEFAULT_HISTORY_PERIOD,
    FAST_REFRESH_HISTORY_PERIOD,
    SEED_SHORT_WINDOW_MAX_AGE_DAYS,
)
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
    state: Literal["no_seed", "restored", "stale_seed"]
    metadata: dict[str, str]
    cleared_tables: tuple[str, ...] = ()
    age_days: float | None = None


def seed_age_days(metadata: Mapping[str, str], now: datetime) -> float:
    """Days between the seed's built-at stamp and ``now``; hard-fails on a bad stamp."""
    raw = metadata.get(META_BUILT_AT, "")
    try:
        built = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise SnapshotError(f"seed built-at is not ISO 8601: {raw!r}") from exc
    if built.tzinfo is None:
        raise SnapshotError(f"seed built-at carries no timezone: {raw!r}")
    age = (now - built).total_seconds() / 86400
    if age < -1 / 24:
        raise SnapshotError(f"seed built-at {raw} is in the future of {now.isoformat()}")
    return max(age, 0.0)


def history_period_for(result: PullResult | None) -> tuple[str, str]:
    """(yfinance history period, the reason) for a daily build started from ``result``.

    ``None`` means no restore was attempted or it failed. Only a restored seed
    inside the age limit earns the short window.
    """
    if result is None or result.state != "restored":
        state = "no restore" if result is None else result.state
        return DEFAULT_HISTORY_PERIOD, f"{state} — full {DEFAULT_HISTORY_PERIOD} history download"
    if result.age_days is None:
        raise SnapshotError("restored seed has no age")
    if result.age_days > SEED_SHORT_WINDOW_MAX_AGE_DAYS:
        return DEFAULT_HISTORY_PERIOD, (
            f"seed is {result.age_days:.1f} days old (limit {SEED_SHORT_WINDOW_MAX_AGE_DAYS}) "
            f"— full {DEFAULT_HISTORY_PERIOD} history download"
        )
    return FAST_REFRESH_HISTORY_PERIOD, (
        f"seed is {result.age_days:.1f} days old — fetching {FAST_REFRESH_HISTORY_PERIOD} "
        "of prices/FX on top of its stored history"
    )


def assert_short_window_safe(db_path: Path, today: date, *, min_history_days: int = 60) -> None:
    """Hard-fail unless ``db_path`` holds price history older than the short window.

    The short window adds a month to stored history; on a database without
    that history it would silently leave the prices table one month deep.
    """
    cutoff = (today - timedelta(days=min_history_days)).isoformat()
    if not db_path.is_file():
        raise SnapshotError(f"short history window requested but {db_path} does not exist")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        try:
            older = conn.execute(
                "SELECT COUNT(*) FROM prices WHERE Date < ?", (cutoff,)
            ).fetchone()[0]
        except sqlite3.OperationalError as exc:
            raise SnapshotError(f"short history window requested but {db_path} has no prices table") from exc
    finally:
        conn.close()
    if older == 0:
        raise SnapshotError(
            f"short history window requested but {db_path} holds no price rows before {cutoff}"
        )


def pull(
    db_path: Path,
    store: ObjectStore,
    *,
    key: str = LATEST_KEY,
    now: datetime | None = None,
    max_age_days: float | None = None,
) -> PullResult:
    """Restore the seed at ``key`` to ``db_path``, history tables emptied.

    Refuses to overwrite an existing database: on a fresh runner there is
    none, and anywhere else a silent replace would destroy local state.
    With ``max_age_days``, a seed older than that is not placed and the
    result is ``stale_seed``.
    """
    when = now or datetime.now(timezone.utc)
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
        age = seed_age_days(metadata, when)
        if max_age_days is not None and age > max_age_days:
            logger.warning("Snapshot at %s is %.1f days old (limit %s) — not placed", key, age, max_age_days)
            return PullResult(state="stale_seed", metadata=metadata, age_days=age)
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
    return PullResult(
        state="restored", metadata=metadata, cleared_tables=tuple(cleared), age_days=age
    )
