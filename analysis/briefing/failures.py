"""What of a failure is archived and printed (#558).

A briefing block that raised is named — in the snapshot's ``failed_blocks``
and on the FOB board's withheld line — and both are published: the snapshot
through the committed ``data/history/briefings.csv``, the text on the site.
So the reason keeps the exception type and the first line of its message,
cut before the first path-like token (a local or runner path says nothing a
reader can act on) and capped at ``MAX_REASON_CHARS``.
"""

from __future__ import annotations

import re

MAX_REASON_CHARS = 120

# A path starts at "/", "~/", "./", "../" not glued to a word ("USD/MT" is a
# unit, not a path), or at a drive letter ("C:\").
_PATH_START = re.compile(r"(?<![\w.])(?:~|\.\.?)?/|\b[A-Za-z]:\\")


def failure_reason(exc: BaseException) -> str:
    """``"ValueError: two rows for band 2026-09 …"`` — type plus a cleaned first line."""
    lines = str(exc).strip().splitlines()
    message = lines[0] if lines else ""
    path = _PATH_START.search(message)
    if path:
        message = message[: path.start()]
    message = message.rstrip(" :'\"(=").strip()
    if len(message) > MAX_REASON_CHARS:
        message = message[: MAX_REASON_CHARS - 1] + "…"
    name = type(exc).__name__
    return f"{name}: {message}" if message else name
