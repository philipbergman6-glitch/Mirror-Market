"""The seed-aware history window (pipeline.db_snapshot + main.py --history-period).

A daily build started from a fresh R2 seed fetches FAST_REFRESH_HISTORY_PERIOD
of prices/FX instead of DEFAULT_HISTORY_PERIOD; every other start keeps the
full download, and the short window is refused on a DB without older history.
"""

from __future__ import annotations

import importlib.util
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

import config
from config import (
    DEFAULT_HISTORY_PERIOD,
    FAST_REFRESH_HISTORY_PERIOD,
    SEED_SHORT_WINDOW_MAX_AGE_DAYS,
)
from pipeline.db_snapshot import (
    LATEST_KEY,
    PullResult,
    SnapshotError,
    assert_short_window_safe,
    history_period_for,
    pull,
    push,
    seed_age_days,
)
from tests.test_db_snapshot import BUILT_AT, MemoryStore, _make_db

NOW = BUILT_AT + timedelta(days=1)


def _restored(age: float) -> PullResult:
    return PullResult(state="restored", metadata={}, age_days=age)


# ── the window decision ──────────────────────────────────────────────


def test_limit_leaves_overlap_inside_the_short_window() -> None:
    # "1mo" reaches ~30 calendar days back; the age limit must leave a margin.
    assert FAST_REFRESH_HISTORY_PERIOD == "1mo"
    assert SEED_SHORT_WINDOW_MAX_AGE_DAYS + 7 <= 30


def test_fresh_seed_earns_the_short_window() -> None:
    period, reason = history_period_for(_restored(1.0))
    assert period == FAST_REFRESH_HISTORY_PERIOD
    assert "1.0 days old" in reason


def test_seed_at_the_limit_still_earns_it() -> None:
    assert history_period_for(_restored(SEED_SHORT_WINDOW_MAX_AGE_DAYS))[0] == FAST_REFRESH_HISTORY_PERIOD


def test_old_seed_keeps_the_full_download() -> None:
    period, reason = history_period_for(_restored(SEED_SHORT_WINDOW_MAX_AGE_DAYS + 0.5))
    assert period == DEFAULT_HISTORY_PERIOD
    assert "limit" in reason


@pytest.mark.parametrize(
    "result",
    [None, PullResult(state="no_seed", metadata={}), PullResult(state="stale_seed", metadata={}, age_days=40)],
)
def test_anything_but_a_restore_keeps_the_full_download(result: PullResult | None) -> None:
    assert history_period_for(result)[0] == DEFAULT_HISTORY_PERIOD


def test_restored_without_an_age_is_an_error() -> None:
    with pytest.raises(SnapshotError):
        history_period_for(PullResult(state="restored", metadata={}))


# ── seed age ─────────────────────────────────────────────────────────


def test_seed_age_from_metadata() -> None:
    assert seed_age_days({"built-at": BUILT_AT.isoformat()}, NOW) == pytest.approx(1.0)


@pytest.mark.parametrize("stamp", ["", "yesterday", "2026-11-16T19:42:00"])
def test_bad_or_naive_stamp_is_refused(stamp: str) -> None:
    with pytest.raises(SnapshotError):
        seed_age_days({"built-at": stamp}, NOW)


def test_future_stamp_is_refused() -> None:
    with pytest.raises(SnapshotError, match="future"):
        seed_age_days({"built-at": (NOW + timedelta(hours=3)).isoformat()}, NOW)


def test_pull_reports_age(tmp_path: Path) -> None:
    store = MemoryStore()
    push(_make_db(tmp_path / "src.db"), store, run_id="42", built_at=BUILT_AT)
    result = pull(tmp_path / "out.db", store, now=NOW)
    assert result.state == "restored"
    assert result.age_days == pytest.approx(1.0)


def test_require_fresh_refuses_an_old_seed_and_places_nothing(tmp_path: Path) -> None:
    store = MemoryStore()
    push(_make_db(tmp_path / "src.db"), store, run_id="42", built_at=BUILT_AT)
    target = tmp_path / "out.db"
    later = BUILT_AT + timedelta(days=SEED_SHORT_WINDOW_MAX_AGE_DAYS + 1)
    result = pull(target, store, now=later, max_age_days=SEED_SHORT_WINDOW_MAX_AGE_DAYS)
    assert result.state == "stale_seed"
    assert not target.exists()


def test_without_require_fresh_an_old_seed_is_still_placed(tmp_path: Path) -> None:
    # The daily build keeps an old seed (its self-healing tables are still
    # worth having) and only loses the short window.
    store = MemoryStore()
    push(_make_db(tmp_path / "src.db"), store, run_id="42", built_at=BUILT_AT)
    target = tmp_path / "out.db"
    result = pull(target, store, now=BUILT_AT + timedelta(days=60))
    assert result.state == "restored"
    assert target.exists()
    assert history_period_for(result)[0] == DEFAULT_HISTORY_PERIOD


# ── the backstop on main.py --history-period ─────────────────────────


def _prices_db(path: Path, dates: list[str]) -> Path:
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute("CREATE TABLE prices (commodity TEXT, Date TEXT, Close REAL)")
        conn.executemany("INSERT INTO prices VALUES ('Soybeans', ?, 1000)", [(d,) for d in dates])
    conn.close()
    return path


def test_short_window_allowed_with_older_history(tmp_path: Path) -> None:
    db = _prices_db(tmp_path / "a.db", ["2026-01-02", "2026-11-13"])
    assert_short_window_safe(db, date(2026, 11, 17))


def test_short_window_refused_on_shallow_history(tmp_path: Path) -> None:
    db = _prices_db(tmp_path / "a.db", ["2026-11-02", "2026-11-13"])
    with pytest.raises(SnapshotError, match="no price rows before"):
        assert_short_window_safe(db, date(2026, 11, 17))


def test_short_window_refused_without_a_db_or_table(tmp_path: Path) -> None:
    with pytest.raises(SnapshotError, match="does not exist"):
        assert_short_window_safe(tmp_path / "absent.db", date(2026, 11, 17))
    empty = tmp_path / "empty.db"
    sqlite3.connect(str(empty)).execute("CREATE TABLE other (x)").connection.close()
    with pytest.raises(SnapshotError, match="no prices table"):
        assert_short_window_safe(empty, date(2026, 11, 17))


@pytest.fixture
def main_module(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    import main

    calls: list[dict] = []
    monkeypatch.setattr(main, "run", lambda **kw: calls.append(kw) or 0)
    monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "mirror_market.db"))
    return main, calls, tmp_path


def test_main_defaults_to_the_full_window(main_module) -> None:
    main, calls, _ = main_module
    assert main.main([]) == 0
    assert calls == [{"history_period": DEFAULT_HISTORY_PERIOD}]


def test_main_short_window_runs_on_a_seeded_db(main_module) -> None:
    main, calls, tmp_path = main_module
    old = (datetime.now(timezone.utc).date() - timedelta(days=400)).isoformat()
    _prices_db(tmp_path / "mirror_market.db", [old])
    assert main.main(["--history-period", FAST_REFRESH_HISTORY_PERIOD]) == 0
    assert calls == [{"history_period": FAST_REFRESH_HISTORY_PERIOD}]


def test_main_short_window_refused_on_an_empty_runner(main_module) -> None:
    main, calls, _ = main_module
    assert main.main(["--history-period", FAST_REFRESH_HISTORY_PERIOD]) == 1
    assert calls == []


def test_main_rejects_unknown_periods_and_fast_combination(main_module) -> None:
    main, _, _ = main_module
    with pytest.raises(SystemExit):
        main.main(["--history-period", "5y"])
    with pytest.raises(SystemExit):
        main.main(["--fast", "--history-period", FAST_REFRESH_HISTORY_PERIOD])


# ── CLI outputs for the workflow ─────────────────────────────────────


def _cli():
    path = Path(__file__).resolve().parent.parent / "scripts" / "db_snapshot.py"
    spec = importlib.util.spec_from_file_location("db_snapshot_cli_window", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _outputs(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    return dict(line.split("=", 1) for line in path.read_text().splitlines())


@pytest.fixture
def configured(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    from pipeline import db_snapshot
    from tests.test_db_snapshot import FULL_ENV

    for key, value in FULL_ENV.items():
        monkeypatch.setenv(key, value)
    out = tmp_path / "github_output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(out))
    store = MemoryStore()
    monkeypatch.setattr(db_snapshot, "R2Store", lambda cfg: store)
    return store, out


def test_cli_fresh_seed_outputs_the_short_window(configured, tmp_path: Path) -> None:
    store, out = configured
    push(_make_db(tmp_path / "src.db"), store, run_id="9", built_at=datetime.now(timezone.utc))
    assert _cli().main(["--db", str(tmp_path / "x.db"), "pull"]) == 0
    assert _outputs(out) == {"state": "restored", "history_period": FAST_REFRESH_HISTORY_PERIOD}


def test_cli_old_seed_outputs_no_window(configured, tmp_path: Path) -> None:
    store, out = configured
    old = datetime.now(timezone.utc) - timedelta(days=SEED_SHORT_WINDOW_MAX_AGE_DAYS + 5)
    push(_make_db(tmp_path / "src.db"), store, run_id="9", built_at=old)
    assert _cli().main(["--db", str(tmp_path / "x.db"), "pull"]) == 0
    assert _outputs(out) == {"state": "restored"}


def test_cli_require_fresh_reports_stale(configured, tmp_path: Path, capsys) -> None:
    store, out = configured
    old = datetime.now(timezone.utc) - timedelta(days=SEED_SHORT_WINDOW_MAX_AGE_DAYS + 5)
    push(_make_db(tmp_path / "src.db"), store, run_id="9", built_at=old)
    target = tmp_path / "x.db"
    assert _cli().main(["--db", str(target), "pull", "--require-fresh"]) == 0
    assert _outputs(out) == {"state": "stale_seed"}
    assert not target.exists()
    assert "::warning title=DB seed too old::" in capsys.readouterr().out


def test_cli_no_seed_outputs_no_window(configured, tmp_path: Path) -> None:
    _, out = configured
    assert _cli().main(["--db", str(tmp_path / "x.db"), "pull"]) == 0
    assert _outputs(out) == {"state": "no_seed"}
    assert LATEST_KEY  # nothing pushed, nothing at the key
