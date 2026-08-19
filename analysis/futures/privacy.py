"""The client-record boundary.

Four things in this project can only come from the client's own desk: a
position, a fill, a clearing statement and an option quote. They are also the
four things that would do the most damage if published — a book is a trading
intention, and a published one is front-runnable. Every other number here is
somebody else's public data; these are the client's.

The boundary is **structural**, on the same terms Phase 4 established for the
opportunity board and Phase 5 for the trial records:

1. the public projection never *builds* a client key (``redact_for_public``);
2. a key guard walks the public payload for one anyway
   (:func:`assert_no_client_records`);
3. a value guard searches free text for the record directories a book is loaded
   from, because provenance is how the leak actually happens — no client key
   anywhere and ``data/reference/positions/house.yml`` printed as a source;
4. a path guard refuses any write the Pages deploy would pick up
   (:func:`assert_private_path`).

Any one of the four could be defeated by a plausible refactor. All four
together is the design.

The records themselves are YAML files rather than tables for the reason the
trial records are: every table in this project round-trips through
``data/history/*.csv``, which is committed to a **public** repository, so a
positions table would publish the book by construction.
"""

from __future__ import annotations

import copy
import os
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

AUDIENCE_PUBLIC = "public"
AUDIENCE_PRIVATE = "private"
AUDIENCES = (AUDIENCE_PUBLIC, AUDIENCE_PRIVATE)


class ClientDataLeak(Exception):
    """A client's own record reached, or was about to reach, public output."""


#: Field names that can only be describing somebody's book. Broad on purpose:
#: this guard runs over the *public workstation payload*, where none of these
#: has any business appearing, so a false positive costs a rename and a false
#: negative costs a publication.
#:
#: ``exposure`` is deliberately **not** here, and the exception is worth stating
#: because it is the shape of every future one. A hedge proposal carries an
#: ``exposure`` key whether it was sized from a client's tonnage or from the
#: public 1,000 MT reference example, so the word alone says nothing about
#: whose it is. What makes an exposure private is the section it sits in, and
#: that is enforced by :data:`PRIVATE_SECTION_IDS` instead. A key belongs in
#: this set only when *no* public payload could honestly contain it.
CLIENT_RECORD_FIELDS: frozenset[str] = frozenset({
    "account",
    "account_id",
    "average_cost",
    "book",
    "breaches",
    "broker",
    "clearing",
    "clearing_account",
    "clearing_member",
    "counterparty",
    "entered_from",
    "fills",
    "limits",
    "loaded_from",
    "marked_positions",
    "net_mt_by_commodity",
    "realised_usd",
    "statement_id",
    "statement_ref",
    "total_realised_usd",
    "total_unrealised_usd",
    "unrealised_usd",
    "valuation",
})

#: Sections of the workstation view that exist only because a client entered
#: something. The public edition renders them ``absent`` with this reason
#: rather than empty: "nothing entered" and "not shown to you" are different
#: states and a public reader is owed the second one.
PRIVATE_SECTION_IDS: tuple[str, ...] = ("book", "exposure", "limits", "clearing", "options_entered")

PRIVATE_SECTION_REASON = (
    "entered positions, limits, clearing statements and option quotes are the client's own "
    "records and are private by construction. They are rendered only to the private edition, "
    "which is written outside docs/ and is never uploaded."
)

#: Path fragments that identify a client record directory wherever they appear
#: in a string — relative or absolute, in provenance, a note or an error.
_RECORD_DIR_MARKERS = (
    "reference/positions",
    "reference/options",
    "reference/clearing",
    "reference/import_profiles",
    "reference\\positions",
    "reference\\options",
    "reference\\clearing",
)


def client_record_dirs() -> tuple[Path, ...]:
    """Every directory a client record can be read from."""
    import config

    return tuple(
        Path(str(getattr(config, name)))
        for name in ("POSITIONS_DIR", "OPTIONS_DIR", "CLEARING_DIR", "IMPORT_PROFILE_DIR")
        if getattr(config, name, None)
    )


def private_output_dir() -> Path:
    """Where a private render may be written. Outside ``docs/`` by construction."""
    import config

    return Path(str(config.OPPORTUNITY_PRIVATE_OUTPUT_DIR))


# ---------------------------------------------------------------------------
# The key and value guards
# ---------------------------------------------------------------------------


def assert_no_client_records(payload: Any, *, where: str = "payload") -> None:
    """Raise if a public payload carries a client key or names a record file.

    Walks mappings, sequences and bare strings alike. A guard that only
    descended through mappings would miss the case that actually happens: a
    provenance string inside a list.
    """
    _walk(payload, path="$", where=where)


def _walk(node: Any, *, path: str, where: str) -> None:
    if isinstance(node, Mapping):
        for key, value in node.items():
            text = str(key)
            if text.lower() in CLIENT_RECORD_FIELDS:
                raise ClientDataLeak(
                    f"{where}: client record field {text!r} at {path}.{text} — this payload is "
                    "public and a book may not reach it"
                )
            _check_text(text, path=f"{path}.{text}", where=where)
            _walk(value, path=f"{path}.{text}", where=where)
        return
    if isinstance(node, str):
        _check_text(node, path=path, where=where)
        return
    if isinstance(node, Sequence) and not isinstance(node, (bytes, bytearray)):
        for index, item in enumerate(node):
            _walk(item, path=f"{path}[{index}]", where=where)


def _check_text(text: str, *, path: str, where: str) -> None:
    lowered = text.replace(os.sep, "/").lower()
    for marker in _RECORD_DIR_MARKERS:
        if marker.replace("\\", "/") in lowered:
            raise ClientDataLeak(
                f"{where}: {path} names a client record directory ({marker!r}). Provenance is "
                "how a book leaks without a single client key being present"
            )


# ---------------------------------------------------------------------------
# The path guard
# ---------------------------------------------------------------------------


def assert_private_path(path: str | os.PathLike[str], *, where: str = "write") -> Path:
    """Refuse any destination the Pages deploy would upload. Returns the path."""
    from trust.site_promotion import expected_site_paths

    resolved = Path(path).expanduser().resolve()
    docs = (Path(__file__).resolve().parents[2] / "docs").resolve()
    if resolved == docs or docs in resolved.parents:
        raise ClientDataLeak(
            f"{where}: {resolved} is inside docs/ — that directory is the published artifact"
        )
    published = {name.rsplit("/", 1)[-1] for name in expected_site_paths()}
    if resolved.name in published and resolved.parent == docs:
        raise ClientDataLeak(f"{where}: {resolved.name} is on the promotion contract")
    return resolved


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


def redact_for_public(
    view: Mapping[str, Any], *, section_ids: Iterable[str] = PRIVATE_SECTION_IDS
) -> dict[str, Any]:
    """Return a copy of a page view with every client section emptied.

    The section is kept, not dropped: a reader of the public edition should see
    that the workstation *has* a book section and that it is not for them,
    rather than a page that silently has one fewer part than the private one.
    """
    private_ids = set(section_ids)
    public = copy.deepcopy(dict(view))
    sections = public.get("sections")
    if isinstance(sections, list):
        public["sections"] = [
            {
                **section,
                "state": "absent",
                "reason": PRIVATE_SECTION_REASON,
                "data": None,
            }
            if isinstance(section, Mapping) and section.get("id") in private_ids
            else section
            for section in sections
        ]
    return public


__all__ = [
    "AUDIENCES",
    "AUDIENCE_PRIVATE",
    "AUDIENCE_PUBLIC",
    "CLIENT_RECORD_FIELDS",
    "PRIVATE_SECTION_IDS",
    "PRIVATE_SECTION_REASON",
    "ClientDataLeak",
    "assert_no_client_records",
    "assert_private_path",
    "client_record_dirs",
    "private_output_dir",
    "redact_for_public",
]
