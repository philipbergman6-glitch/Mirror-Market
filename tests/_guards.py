"""Test-isolation guards: the suite may not write history, may not dial out.

Two leaks this suite has actually suffered, both silent:

* A test escaped its temp history dir and ran `export_history()` against the
  working tree — `data/history/safex_prices.csv` gained a live row and a new
  column, and the only symptom was a dirty `git status` after the run (#84).
* A monkeypatch that missed a fetcher let `main.run()` walk into Layers 14-21
  and make ~15 live HTTP calls. The test *passed*; the only symptom was a
  34-second wall clock nobody was watching (#84 comment, from #56/PR #172).

Both are now hard failures. The write guard wraps the filesystem entry points
and raises on any write under the repo's real `data/history/`; the network
guard raises on any outbound socket connect. Neither is a lint: they fire
during the offending call, so the traceback names the test and the line.

Opting out of the network guard is per-test and explicit:

    @pytest.mark.network
    def test_that_really_needs_the_internet(): ...

There is no opt-out for the history guard. Tests that exercise import/export
point `pipeline.history.HISTORY_DIR` at `tmp_path` — see tests/test_history.py.
"""

from __future__ import annotations

import builtins
import io
import os
import socket
from collections.abc import Iterator
from contextlib import contextmanager

from config import HISTORY_DIR

# Resolved once: the guard compares against the real directory, so a test that
# monkeypatches HISTORY_DIR to a tmp_path is untouched by it.
_PROTECTED_DIR = os.path.realpath(HISTORY_DIR)

# Loopback traffic is not "the network" — a local test server or a unix socket
# is nobody's live fetch.
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0", ""}

_network_allowed = False
_history_guard_installed = False
_network_guard_installed = False


class HistoryWriteBlocked(RuntimeError):
    """A test tried to write inside the repo's committed data/history/."""


class NetworkBlocked(RuntimeError):
    """A test tried to open an outbound connection."""


# --------------------------------------------------------------------------
# History write guard
# --------------------------------------------------------------------------


def _is_protected(path: object) -> bool:
    """True if `path` names something inside the real data/history/.

    File descriptors (ints) and objects with no filesystem path answer False:
    an already-open fd was opened through a call this guard saw.
    """
    if isinstance(path, int):
        return False
    try:
        raw = os.fspath(path)  # type: ignore[arg-type]
    except TypeError:
        return False
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "replace")
    resolved = os.path.realpath(raw)
    return resolved == _PROTECTED_DIR or resolved.startswith(_PROTECTED_DIR + os.sep)


def _refuse(operation: str, path: object) -> None:
    raise HistoryWriteBlocked(
        f"test tried to {operation} {path!r} inside {_PROTECTED_DIR}. "
        "data/history/ is committed state — only the deploy workflow writes it. "
        "Point pipeline.history.HISTORY_DIR at tmp_path instead."
    )


def _guard_write(operation: str, path: object) -> None:
    if _is_protected(path):
        _refuse(operation, path)


@contextmanager
def block_history_writes() -> Iterator[None]:
    """Patch the filesystem entry points that could write data/history/.

    `builtins.open` and `io.open` are the same function object, but both
    module attributes are rebound: pathlib calls `io.open` by attribute
    lookup, so patching only builtins would miss `Path.write_text`. pandas'
    `to_csv` goes through `builtins.open` and is covered there.
    """
    originals = {
        (builtins, "open"): builtins.open,
        (io, "open"): io.open,
        (os, "open"): os.open,
        (os, "replace"): os.replace,
        (os, "rename"): os.rename,
        (os, "remove"): os.remove,
        (os, "unlink"): os.unlink,
        (os, "rmdir"): os.rmdir,
        (os, "mkdir"): os.mkdir,
        (os, "makedirs"): os.makedirs,
    }
    real_open = builtins.open
    real_os_open = os.open

    def guarded_open(file, mode="r", *args, **kwargs):
        if any(flag in mode for flag in ("w", "a", "x", "+")):
            _guard_write("open for writing", file)
        return real_open(file, mode, *args, **kwargs)

    def guarded_os_open(path, flags, *args, **kwargs):
        if flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_APPEND | os.O_TRUNC):
            _guard_write("open for writing", path)
        return real_os_open(path, flags, *args, **kwargs)

    def _wrap_one_path(name: str, func):
        def guarded(path, *args, **kwargs):
            _guard_write(name, path)
            return func(path, *args, **kwargs)

        return guarded

    def _wrap_two_paths(name: str, func):
        def guarded(src, dst, *args, **kwargs):
            _guard_write(name, src)
            _guard_write(name, dst)
            return func(src, dst, *args, **kwargs)

        return guarded

    builtins.open = guarded_open  # type: ignore[assignment]
    io.open = guarded_open  # type: ignore[assignment]
    os.open = guarded_os_open  # type: ignore[assignment]
    for name in ("replace", "rename"):
        setattr(os, name, _wrap_two_paths(name, originals[(os, name)]))
    for name in ("remove", "unlink", "rmdir", "mkdir", "makedirs"):
        setattr(os, name, _wrap_one_path(name, originals[(os, name)]))
    global _history_guard_installed
    _history_guard_installed = True
    try:
        yield
    finally:
        _history_guard_installed = False
        for (module, name), func in originals.items():
            setattr(module, name, func)


def history_guard_installed() -> bool:
    """Whether the write patches are live.

    tests/test_isolation_guards.py checks this before it attempts a real
    write: without the guard those attempts would *succeed* and dirty the
    working tree — the very leak the test exists to prevent.
    """
    return _history_guard_installed


# --------------------------------------------------------------------------
# Network guard
# --------------------------------------------------------------------------


def network_allowed() -> bool:
    """Whether the test currently running carries `@pytest.mark.network`."""
    return _network_allowed


def network_guard_installed() -> bool:
    """Whether the socket patches are live — checked before a test dials out."""
    return _network_guard_installed


def set_network_allowed(allowed: bool) -> None:
    global _network_allowed
    _network_allowed = allowed


def _is_local(address: object) -> bool:
    if isinstance(address, (str, bytes)):  # AF_UNIX path
        return True
    if isinstance(address, tuple) and address:
        host = address[0]
        return isinstance(host, str) and host in _LOCAL_HOSTS
    return False


def _refuse_network(target: object) -> None:
    raise NetworkBlocked(
        f"test tried to reach {target!r}. Live calls make the suite slow, flaky "
        "and dependent on somebody else's uptime — stub the fetcher, or mark the "
        "test @pytest.mark.network if it genuinely needs the internet."
    )


@contextmanager
def block_network() -> Iterator[None]:
    """Raise on any outbound socket connect or DNS lookup off-loopback."""
    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex
    real_create_connection = socket.create_connection
    real_getaddrinfo = socket.getaddrinfo

    def guarded_connect(self, address, *args, **kwargs):
        if not _network_allowed and not _is_local(address):
            _refuse_network(address)
        return real_connect(self, address, *args, **kwargs)

    def guarded_connect_ex(self, address, *args, **kwargs):
        if not _network_allowed and not _is_local(address):
            _refuse_network(address)
        return real_connect_ex(self, address, *args, **kwargs)

    def guarded_create_connection(address, *args, **kwargs):
        if not _network_allowed and not _is_local(address):
            _refuse_network(address)
        return real_create_connection(address, *args, **kwargs)

    def guarded_getaddrinfo(host, *args, **kwargs):
        if not _network_allowed and not _is_local((host, None)):
            _refuse_network(host)
        return real_getaddrinfo(host, *args, **kwargs)

    socket.socket.connect = guarded_connect  # type: ignore[method-assign]
    socket.socket.connect_ex = guarded_connect_ex  # type: ignore[method-assign]
    socket.create_connection = guarded_create_connection  # type: ignore[assignment]
    socket.getaddrinfo = guarded_getaddrinfo  # type: ignore[assignment]
    global _network_guard_installed
    _network_guard_installed = True
    try:
        yield
    finally:
        _network_guard_installed = False
        socket.socket.connect = real_connect  # type: ignore[method-assign]
        socket.socket.connect_ex = real_connect_ex  # type: ignore[method-assign]
        socket.create_connection = real_create_connection  # type: ignore[assignment]
        socket.getaddrinfo = real_getaddrinfo  # type: ignore[assignment]
