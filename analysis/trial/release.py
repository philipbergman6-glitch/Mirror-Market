"""What produced a trial result, and whether it can be produced again (Phase 5).

Requirement 6: every captured result carries a release/edition id so feedback
can be reproduced against the exact data *and* code version. Before this module
the repository had neither half of that. There is no ``__version__``, no
``CODE_VERSION`` and no git-sha capture anywhere — the only code-provenance
concept is ``trust.domain.Run.code_revision``, a free-text field whose one
production value is a hardcoded string in ``trust/magyp_fob.py``. And the static
site the traders will actually use is not built through ``trust.edition`` at
all, so it has no edition id to inherit.

So a stamp is assembled from what genuinely exists, and says so:

**The code half is the git commit**, with a ``dirty`` flag when the working tree
carries uncommitted changes. Dirty is recorded rather than refused — a trial
session run against a local hotfix is a legitimate session — but
``ReleaseStamp.is_reproducible`` is ``False`` for it, and the weekly review
reports how many findings arrived on unreproducible builds. That number rising
is itself a finding.

**The data half is a fingerprint of ``data_freshness``**, hashed over every
layer's ``(layer_name, last_success, status, rows_fetched)``. This is the right
table to hash because it is the one the pipeline grades on: two editions built
from one commit but different data differ here, and two runs of the same
pipeline over an unchanged upstream do not. Hashing the price rows themselves
would be finer-grained and worse — an upstream revising one historical bar would
read as a different edition of the whole product.

**The edition half is optional and honest.** Where a ``trust`` edition exists,
its id is carried. Where it does not — which is today, for the static site —
``edition_id`` is ``None`` rather than a fabricated identifier, and
``describe()`` says which of the two it is.

Reproduction is then a comparison, not a rebuild. :func:`reproduce` takes a
stamp captured weeks ago and reports what has moved since: the code, the data,
or neither. It deliberately does not check the code *out* — that is the
operator's decision, and a script that silently changed the working tree of a
repository mid-trial would be a worse idea than the problem it solved.
"""

from __future__ import annotations

import hashlib
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from analysis.trial.domain import ReleaseStamp, TrialError, utc_now

log = logging.getLogger(__name__)

__all__ = [
    "ReproductionCheck",
    "capture_release_stamp",
    "data_fingerprint",
    "git_code_revision",
    "reproduce",
]

#: What a stamp records when git is unavailable — no repository, no binary, a
#: source tarball. It is a legal state and must not crash a trial session, but
#: it is never confusable with a real commit, and it can never be reproducible.
UNKNOWN_REVISION = "unknown"


def _git(args: list[str], *, repo: Path) -> str | None:
    """Run a read-only git command. ``None`` on any failure, never an exception.

    A trader mid-session must not lose a record because git is missing. The
    absence propagates into the stamp as ``UNKNOWN_REVISION``, which is visible,
    rather than into a traceback, which is not.
    """
    try:
        completed = subprocess.run(  # noqa: S603 - fixed argv, no shell
            ["git", *args],
            cwd=str(repo),
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        log.warning("git %s failed: %s", " ".join(args), exc)
        return None
    if completed.returncode != 0:
        log.warning("git %s exited %d: %s", " ".join(args), completed.returncode, completed.stderr.strip())
        return None
    return completed.stdout.strip()


def git_code_revision(repo: str | Path | None = None) -> tuple[str, bool]:
    """``(commit_sha, dirty)`` for the working tree.

    ``dirty`` is true when ``git status --porcelain`` reports anything at all,
    tracked or untracked. Untracked files count deliberately: an untracked
    module that a local run imported is exactly the kind of change that makes a
    finding unreproducible and leaves no trace in the commit.
    """
    root = Path(repo) if repo is not None else Path(__file__).resolve().parents[2]
    sha = _git(["rev-parse", "HEAD"], repo=root)
    if sha is None:
        return UNKNOWN_REVISION, True
    status = _git(["status", "--porcelain"], repo=root)
    # A failed status check is treated as dirty. Guessing "clean" here would
    # stamp a finding as reproducible on no evidence, which is the one direction
    # this call must not be wrong in.
    dirty = status is None or bool(status.strip())
    return sha, dirty


def data_fingerprint(freshness: Any | None = None) -> tuple[str, int]:
    """``(sha256_hex, layer_count)`` over the layer freshness table.

    Pass ``freshness`` (a ``data_freshness`` DataFrame) to fingerprint a frame
    you already hold; omit it and the current database is read. An empty or
    unreadable table fingerprints as the hash of the empty string with a count
    of 0 — an honest "no data was behind this", which then differs from every
    real edition and so can never be mistaken for one.

    The hashed tuple is ``(layer_name, last_success, status, rows_fetched)``,
    sorted by layer. ``last_attempt`` is deliberately excluded: it changes on
    every run whether or not anything was fetched, and including it would make
    two identical editions fingerprint differently.
    """
    if freshness is None:
        from pipeline.query import read_freshness

        freshness = read_freshness()

    if freshness is None or getattr(freshness, "empty", True):
        return hashlib.sha256(b"").hexdigest(), 0

    rows: list[str] = []
    for record in freshness.to_dict(orient="records"):
        rows.append(
            "|".join(
                str(record.get(column, ""))
                for column in ("layer_name", "last_success", "status", "rows_fetched")
            )
        )
    rows.sort()
    digest = hashlib.sha256("\n".join(rows).encode("utf-8")).hexdigest()
    return digest, len(rows)


def capture_release_stamp(
    *,
    repo: str | Path | None = None,
    freshness: Any | None = None,
    edition_id: str | None = None,
) -> ReleaseStamp:
    """Stamp what is running right now. Called once per trial session.

    Every argument exists so a test — or a replay of an archived edition — can
    supply the state rather than the live machine. Called with no arguments it
    reads the checkout it lives in and the database that checkout is configured
    for, which is what a trader's session does.
    """
    sha, dirty = git_code_revision(repo)
    fingerprint, layer_count = data_fingerprint(freshness)
    return ReleaseStamp(
        code_revision=sha,
        data_fingerprint=fingerprint,
        captured_at=utc_now(),
        edition_id=edition_id,
        dirty=dirty,
        layer_count=layer_count,
    )


@dataclass(frozen=True)
class ReproductionCheck:
    """Whether a stamped result can be reproduced, and what has moved if not.

    Three separate answers, because the remedies are different. Code drift is
    fixed by a checkout. Data drift is not fixable at all for most layers —
    ``india_domestic`` serves the current day only, so an edition whose mandi
    rows have rolled off is gone — and the honest report is that the finding can
    be re-read but not re-run. A stamp that was dirty when taken cannot be
    reproduced by any mechanism, and saying so is the whole value of having
    recorded it.
    """

    stamp: ReleaseStamp
    current: ReleaseStamp
    code_matches: bool
    data_matches: bool

    @property
    def reproducible(self) -> bool:
        return self.stamp.is_reproducible and self.code_matches and self.data_matches

    @property
    def verdict(self) -> str:
        if not self.stamp.is_reproducible:
            return "not-reproducible"
        if self.reproducible:
            return "reproducible"
        return "drifted"

    @property
    def reason(self) -> str:
        if not self.stamp.is_reproducible:
            return (
                "the stamp was taken against a dirty working tree, so no commit "
                "describes the code that produced this result; it can be re-read "
                "but not re-run"
            )
        if self.reproducible:
            return "code and data are unchanged since the stamp was taken"
        moved = [
            name
            for name, same in (("code", self.code_matches), ("data", self.data_matches))
            if not same
        ]
        return (
            f"{' and '.join(moved)} moved since the stamp was taken; "
            f"check out {self.stamp.short_code} and restore the edition's database "
            "to re-run it"
        )

    @property
    def replay_command(self) -> str:
        """The one command the operator would run. Printed, never executed."""
        return f"git checkout {self.stamp.code_revision}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict,
            "reason": self.reason,
            "reproducible": self.reproducible,
            "code_matches": self.code_matches,
            "data_matches": self.data_matches,
            "replay_command": self.replay_command,
            "stamp": self.stamp.to_dict(),
            "current": self.current.to_dict(),
        }


def reproduce(
    stamp: ReleaseStamp,
    *,
    repo: str | Path | None = None,
    freshness: Any | None = None,
    current: ReleaseStamp | None = None,
) -> ReproductionCheck:
    """Can this stamped result be produced again? Reports; never changes anything.

    Deliberately read-only. Checking out the recorded commit is a decision with
    consequences for whatever else is in the working tree, and a verification
    routine that mutates a repository mid-trial would be a worse failure than the
    unreproducible finding it was investigating. The command to run is returned
    as text.
    """
    if not isinstance(stamp, ReleaseStamp):
        raise TrialError(f"reproduce() needs a ReleaseStamp, got {type(stamp).__name__}")
    now = current if current is not None else capture_release_stamp(repo=repo, freshness=freshness)
    return ReproductionCheck(
        stamp=stamp,
        current=now,
        code_matches=stamp.code_revision == now.code_revision,
        data_matches=stamp.data_fingerprint == now.data_fingerprint,
    )
