"""The privacy boundary, made checkable (Phase 5).

The trial's binding constraint is that trader names, positions, counterparties,
contact notes and commercial decisions are never published, that sensitive
records live outside the GitHub Pages artifact, and that what *is* shared is
sanitised aggregate. Every object in this package already implements that by
construction: ``to_dict(audience=AUDIENCE_AGGREGATE)`` does not *filter* the
private keys, it never builds them.

This module is the second line, and exists because construction-time correctness
is only as good as the last person who added a field. Three guards, each cheap
enough to run on every write:

**Key guard.** :func:`assert_sanitized` walks a payload to any depth and raises
if a key from ``PRIVATE_FIELD_NAMES`` appears anywhere. Recursive, because the
realistic regression is not a top-level ``trader`` key — nobody adds one of
those by accident — but a private key surviving three levels down inside a
nested backlog item that a new caller passed through unchanged.

**Value guard.** :func:`assert_no_identifiers` takes the trader identifiers the
records actually contain and searches every string value for them. This catches
the leak a key guard cannot: a name interpolated into free text, which is how the
real disclosure happens ("alice could not price the Nov cargo"). It requires the
caller to pass the identifiers, because guessing what is a name is not something
this module can do and pretending otherwise would be worse than not checking.

**Path guard.** :func:`assert_private_path` refuses a write target inside
``docs/`` or on the promotion contract's path list. The private edition of the
opportunity board established this pattern in Phase 4; the trial's records are
strictly more sensitive, so they get the same guard applied at the write.

A guard that only runs in tests is a guard that documents an intention. These
run at every write, so the failure mode is a raised exception on the desk rather
than a trader's name on a public page.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from analysis.trial.domain import (
    AUDIENCE_AGGREGATE,
    AUDIENCE_PRIVATE,
    PRIVATE_FIELD_NAMES,
    TrialError,
)

__all__ = [
    "PrivacyLeak",
    "aggregate",
    "assert_no_identifiers",
    "assert_private_path",
    "assert_sanitized",
    "private_output_dir",
    "record_dir",
    "write_private_json",
]


class PrivacyLeak(TrialError):
    """A private field or identifier reached a payload bound for sharing.

    Its own type, not a bare ``TrialError``, so a caller can catch a leak
    specifically and so a stack trace names what went wrong in one word. Never
    caught inside this package: a leak is not a degraded state to report, it is a
    stop.
    """


#: Exact JSON paths exempted from the key guard. **Empty, and meant to stay that
#: way.** It exists so that the one day an exemption is genuinely needed, it is
#: made by path — ``$.drills[0].notes`` — rather than by deleting a name from
#: ``PRIVATE_FIELD_NAMES`` and dropping the guard everywhere at once. An entry
#: here is a decision about one field in one payload; an entry removed from the
#: name set is a decision about every payload forever.
_ALLOWED_PATHS: frozenset[str] = frozenset()


def _walk(payload: Any, path: str = "$", key: str | None = None) -> Iterable[tuple[str, str | None, Any]]:
    """Yield ``(path, key, value)`` for *every node* at any depth, not just mappings.

    Visiting every node rather than every mapping key is the difference between
    a guard that works and one that looks like it does. The aggregate payloads
    here carry bare strings inside lists — ``worked``, ``recommendations`` — and
    a walk that only descended through mappings never scanned a single one of
    them, which is precisely where an interpolated trader name would sit.

    ``key`` is ``None`` for a list element: the element inherits no name of its
    own, and reusing the parent's would report one private key once per item.
    """
    yield path, key, payload
    if isinstance(payload, Mapping):
        for child_key, value in payload.items():
            yield from _walk(value, f"{path}.{child_key}", str(child_key))
    elif isinstance(payload, (list, tuple)):
        for index, value in enumerate(payload):
            yield from _walk(value, f"{path}[{index}]", None)


def assert_sanitized(payload: Any, *, where: str = "payload") -> None:
    """Raise :class:`PrivacyLeak` if any private field name appears at any depth.

    Checks keys, not values — a value guard needs to know what a name looks like,
    which is :func:`assert_no_identifiers`. Between them they cover the two ways
    a record leaks: as a field that should not have been built, and as a string
    that should not have been quoted.
    """
    for path, key, _ in _walk(payload):
        if key is not None and key in PRIVATE_FIELD_NAMES and path not in _ALLOWED_PATHS:
            raise PrivacyLeak(
                f"{where}: private field {key!r} present at {path}. This payload is bound for "
                "sharing and must be built with audience=AUDIENCE_AGGREGATE, which does not "
                "construct that key at all."
            )


def assert_no_identifiers(payload: Any, identifiers: Iterable[str], *, where: str = "payload") -> None:
    """Raise if any trader identifier appears in any string value, at any depth.

    Matching is case-insensitive and substring-based, which over-matches on
    purpose: a trader initialled ``jd`` will trip on the word "adjusted", and the
    right response to that is to pick an identifier that is not a substring of
    ordinary English, not to loosen the check. Over-matching costs a rename;
    under-matching costs a disclosure.

    Identifiers shorter than three characters are refused for the same reason —
    a one-letter identifier makes this guard useless while appearing to run.
    """
    wanted = [identifier.strip().lower() for identifier in identifiers if identifier and identifier.strip()]
    short = [identifier for identifier in wanted if len(identifier) < 3]
    if short:
        raise TrialError(
            f"{where}: identifier(s) {short} are shorter than three characters, which would "
            "match almost any text and make this guard meaningless. Use a longer handle."
        )
    if not wanted:
        return
    for path, key, value in _walk(payload):
        for text in (key or "", value if isinstance(value, str) else ""):
            lowered = text.lower()
            for identifier in wanted:
                if identifier in lowered:
                    raise PrivacyLeak(
                        f"{where}: trader identifier {identifier!r} appears at {path}. "
                        "Aggregate output must not name a participant, including inside free text."
                    )


def assert_private_path(path: str | Path, *, where: str = "output") -> Path:
    """Refuse a write target that would end up published. Returns the path.

    Two independent checks, because they fail differently: a path under ``docs/``
    is picked up by the Pages deploy whether or not the promotion contract knows
    about it, and a path on the contract's own list is uploaded by name. Either
    alone would let one class of mistake through.
    """
    target = Path(path).resolve()
    repo = Path(__file__).resolve().parents[2]
    docs = (repo / "docs").resolve()
    if target == docs or docs in target.parents:
        raise PrivacyLeak(
            f"{where}: {target} is inside docs/, which is deployed to GitHub Pages. Trial "
            "records and private trial output must be written to config.TRIAL_PRIVATE_OUTPUT_DIR."
        )
    try:
        from trust.site_promotion import expected_site_paths

        published = {(docs / url).resolve() for url in expected_site_paths()}
    except Exception:  # pragma: no cover - contract unavailable is not a licence to write
        published = set()
    if target in published:
        raise PrivacyLeak(f"{where}: {target} is on the site promotion contract's path list")
    return target


def aggregate(obj: Any, *, identifiers: Iterable[str] = (), where: str = "payload") -> dict[str, Any]:
    """Project an object to its aggregate form and prove the projection held.

    The single call every reporting path should use. Building the aggregate dict
    and checking it are one operation here precisely so a caller cannot do the
    first and forget the second.
    """
    to_dict = getattr(obj, "to_dict", None)
    if to_dict is None:
        raise TrialError(f"{where}: {type(obj).__name__} has no to_dict(); cannot project it")
    try:
        payload = to_dict(audience=AUDIENCE_AGGREGATE)
    except TypeError as exc:  # a to_dict that takes no audience cannot promise anything
        raise TrialError(
            f"{where}: {type(obj).__name__}.to_dict() does not accept an audience, so it cannot "
            "produce an aggregate projection"
        ) from exc
    if not isinstance(payload, dict):
        raise TrialError(f"{where}: aggregate projection of {type(obj).__name__} is not a dict")
    assert_sanitized(payload, where=where)
    assert_no_identifiers(payload, identifiers, where=where)
    return payload


def write_private_json(
    path: str | Path,
    payload: Any,
    *,
    audience: str = AUDIENCE_PRIVATE,
    identifiers: Iterable[str] = (),
) -> Path:
    """Write a trial payload to disk, guarding the destination and the content.

    ``audience`` decides which guards apply. A private write is checked for its
    *destination* only — it is allowed to contain everything, that is what makes
    it private — while an aggregate write is checked for destination and content
    both. The path guard runs either way, because the thing that must never
    happen is a trial record inside ``docs/`` regardless of how sanitised it is.
    """
    target = assert_private_path(path, where="trial output")
    if audience == AUDIENCE_AGGREGATE:
        assert_sanitized(payload, where=str(target))
        assert_no_identifiers(payload, identifiers, where=str(target))
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    return target


def private_output_dir() -> Path:
    """``config.TRIAL_PRIVATE_OUTPUT_DIR``, verified to be outside ``docs/``."""
    import config

    return assert_private_path(config.TRIAL_PRIVATE_OUTPUT_DIR, where="TRIAL_PRIVATE_OUTPUT_DIR")


def record_dir() -> Path:
    """``config.TRIAL_RECORD_DIR``, verified to be outside ``docs/``."""
    import config

    return assert_private_path(config.TRIAL_RECORD_DIR, where="TRIAL_RECORD_DIR")
