"""The guards that keep the suite off the working tree and off the network.

Ticket #84. Both leaks these cover were silent in their own way — a dirty
`git status` nobody read, a slow test nobody timed — so the guards themselves
need tests that fail loudly if a future refactor unhooks them.
"""

from __future__ import annotations

import os
import socket
from pathlib import Path

import pandas as pd
import pytest

from config import HISTORY_DIR
from tests import _guards

REAL_CSV = Path(HISTORY_DIR) / "safex_prices.csv"


class TestHistoryIsProtected:
    """No test may write inside the committed data/history/.

    Every test here aims a real write at the real directory, so each one
    first refuses to run unless the guard is live — otherwise a broken
    guard would be reported by a test that had already caused the damage.
    """

    @pytest.fixture(autouse=True)
    def _refuse_to_run_unguarded(self):
        assert _guards.history_guard_installed(), (
            "the history guard is not installed — these tests would write the "
            "working tree. Check the _isolation_guards fixture in conftest."
        )

    @pytest.mark.parametrize("mode", ["w", "a", "r+", "x"])
    def test_opening_a_history_csv_for_writing_raises(self, mode: str):
        with pytest.raises(_guards.HistoryWriteBlocked), open(REAL_CSV, mode):
            pass  # pragma: no cover — the open never returns

    def test_pandas_to_csv_raises(self):
        frame = pd.DataFrame({"Date": ["2026-08-22"], "commodity": ["soybeans"]})
        with pytest.raises(_guards.HistoryWriteBlocked):
            frame.to_csv(Path(HISTORY_DIR) / "invented.csv", index=False)

    def test_pathlib_write_text_raises(self):
        with pytest.raises(_guards.HistoryWriteBlocked):
            (Path(HISTORY_DIR) / "invented.csv").write_text("nope")

    def test_the_atomic_rename_export_uses_raises(self, tmp_path: Path):
        staged = tmp_path / "staged.csv"
        staged.write_text("Date,commodity\n")
        with pytest.raises(_guards.HistoryWriteBlocked):
            os.replace(staged, REAL_CSV)

    def test_deleting_a_history_csv_raises(self):
        with pytest.raises(_guards.HistoryWriteBlocked):
            os.remove(REAL_CSV)

    def test_export_history_against_the_real_dir_raises(self, patched_db: Path):
        """The exact #84 leak: an unpatched HISTORY_DIR at export time."""
        from pipeline import history

        with pytest.raises(_guards.HistoryWriteBlocked):
            history.export_history()

    def test_reading_history_is_still_allowed(self):
        assert REAL_CSV.exists(), "fixture assumption: the committed CSV is there"
        with open(REAL_CSV, encoding="utf-8") as fh:
            assert fh.readline().strip(), "the real CSV still reads"

    def test_writing_elsewhere_is_untouched(self, tmp_path: Path):
        target = tmp_path / "history" / "safex_prices.csv"
        target.parent.mkdir()
        target.write_text("Date,commodity\n")
        assert target.read_text() == "Date,commodity\n"


class TestNetworkIsBlocked:
    """An unstubbed fetcher raises instead of quietly costing 30 seconds.

    Same precaution as above: these tests aim at a real host, so an
    uninstalled guard must stop them before they reach it.
    """

    @pytest.fixture(autouse=True)
    def _refuse_to_run_unguarded(self):
        assert _guards.network_guard_installed(), (
            "the network guard is not installed — these tests would dial out. "
            "Check the _isolation_guards fixture in conftest."
        )

    def test_a_socket_connect_raises(self):
        sock = socket.socket()
        try:
            with pytest.raises(_guards.NetworkBlocked):
                sock.connect(("example.com", 80))
        finally:
            sock.close()

    def test_create_connection_raises(self):
        with pytest.raises(_guards.NetworkBlocked):
            socket.create_connection(("example.com", 80), timeout=0.1)

    def test_a_dns_lookup_raises(self):
        with pytest.raises(_guards.NetworkBlocked):
            socket.getaddrinfo("example.com", 80)

    def test_requests_raises_through_the_same_guard(self):
        requests = pytest.importorskip("requests")
        with pytest.raises(Exception) as excinfo:
            requests.get("https://example.com", timeout=0.1)
        # requests wraps the socket error; the guard's message survives.
        assert "stub the fetcher" in str(excinfo.value)

    def test_loopback_is_not_the_network(self):
        """A local server or unix socket is nobody's live fetch."""
        with socket.socket() as server:
            server.bind(("127.0.0.1", 0))
            server.listen(1)
            with socket.create_connection(server.getsockname(), timeout=1) as client:
                assert client.getpeername()[0] == "127.0.0.1"

    @pytest.mark.network
    def test_the_marker_opens_the_gate(self):
        assert _guards.network_allowed(), "@pytest.mark.network should exempt the test"

    def test_and_closes_it_again_afterwards(self):
        assert not _guards.network_allowed(), "the exemption is per-test, not sticky"
