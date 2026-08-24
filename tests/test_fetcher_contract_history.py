"""Tests for Layer 11b — named-contract daily bar capture (A4 #301, B4 #310).

The measured facts this layer is built on (probed live 2026-08-23):
Yahoo serves full per-contract history while a contract is listed, keeps
serving *some* expired contracts (ZSX25: 1007 rows nine months after
expiry) and 404s others (ZSN26: gone one month after expiry) — retention
is not an age window, so coverage is established by asking, and what
answers today may not answer tomorrow. Hence: capture, persist, and never
re-probe an expired symbol already banked.

The clock is injected — nothing here depends on when it runs.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from fetchers import contract_history as ch
from pipeline.clean import clean_contract_bars
from pipeline.query import captured_contract_tickers, read_contract_bars
from pipeline.store import save_contract_bars

NOW = date(2026, 8, 23)


def _bars(dates: list[str], closes: list[float]) -> pd.DataFrame:
    idx = pd.DatetimeIndex(pd.to_datetime(dates), name="Date")
    return pd.DataFrame({"Close": closes, "Volume": [100.0] * len(dates)}, index=idx)


# ── Roster construction ─────────────────────────────────────────────

def test_expired_candidates_walk_back_only_traded_months():
    # Soybeans trade [1,3,5,7,8,9,11]. From Aug 2026, six months back is
    # Feb..Jul 2026 — of which Mar, May, Jul are traded.
    out = ch._expired_candidates("ZS", "CBT", [1, 3, 5, 7, 8, 9, 11], today=NOW)
    assert [c["ticker"] for c in out] == ["ZSN26.CBT", "ZSK26.CBT", "ZSH26.CBT"]


def test_expired_candidates_cross_year_boundary():
    out = ch._expired_candidates("ZS", "CBT", [11], today=date(2026, 2, 10))
    assert [c["ticker"] for c in out] == ["ZSX25.CBT"]


def test_current_month_is_not_an_expired_candidate():
    # Aug 2026 trades (month 8 in the list) but is the *listed* front — it
    # must not appear in the expired roster.
    out = ch._expired_candidates("ZS", "CBT", [1, 3, 5, 7, 8, 9, 11], today=NOW)
    assert "ZSQ26.CBT" not in [c["ticker"] for c in out]


# ── Fetch behavior ──────────────────────────────────────────────────

def _run_fetch(monkeypatch, answers: dict[str, pd.DataFrame], *, already=frozenset()):
    """Drive fetch_contract_bars with a stubbed fetch_one, recording calls."""
    calls: list[tuple[str, int]] = []

    def fake_fetch_one(ticker, period="max", rule=None, max_retries=3):
        calls.append((ticker, max_retries))
        return answers.get(ticker, pd.DataFrame())

    monkeypatch.setattr(ch, "fetch_one", fake_fetch_one)
    df = ch.fetch_contract_bars("Soybeans", already_captured=already, today=NOW)
    return df, calls


def test_listed_and_expired_bars_are_captured(monkeypatch):
    answers = {
        "ZSQ26.CBT": _bars(["2026-08-20", "2026-08-21"], [1147.0, 1150.0]),
        "ZSN26.CBT": _bars(["2026-06-01", "2026-07-10"], [1100.0, 1120.0]),
    }
    df, _ = _run_fetch(monkeypatch, answers)
    assert set(df["ticker"]) == {"ZSQ26.CBT", "ZSN26.CBT"}
    assert len(df) == 4
    assert set(df.columns) == {
        "commodity", "ticker", "contract_month", "Date",
        "Open", "High", "Low", "Close", "Volume",
    }
    q = df[df["ticker"] == "ZSQ26.CBT"].iloc[0]
    assert q["contract_month"] == "2026-08-01"
    assert q["Date"] == "2026-08-20"


def test_expired_probe_uses_single_attempt(monkeypatch):
    _, calls = _run_fetch(monkeypatch, {})
    budgets = dict(calls)
    assert budgets["ZSN26.CBT"] == 1          # expired: one attempt
    assert budgets["ZSQ26.CBT"] == 3          # listed: full budget


def test_already_captured_expired_symbols_are_not_reprobed(monkeypatch):
    _, calls = _run_fetch(
        monkeypatch, {}, already=frozenset({"ZSN26.CBT", "ZSK26.CBT", "ZSH26.CBT"})
    )
    probed = {t for t, _ in calls}
    assert probed.isdisjoint({"ZSN26.CBT", "ZSK26.CBT", "ZSH26.CBT"})


def test_listed_contracts_are_refetched_even_when_captured(monkeypatch):
    # A listed contract's history grows daily — "already captured" must
    # never freeze it.
    _, calls = _run_fetch(monkeypatch, {}, already=frozenset({"ZSQ26.CBT"}))
    assert "ZSQ26.CBT" in {t for t, _ in calls}


def test_nan_close_bars_are_dropped(monkeypatch):
    answers = {"ZSQ26.CBT": _bars(["2026-08-20", "2026-08-21"], [float("nan"), 1150.0])}
    df, _ = _run_fetch(monkeypatch, answers)
    assert list(df["Date"]) == ["2026-08-21"]


def test_unconfigured_commodity_returns_empty():
    assert ch.fetch_contract_bars("Plutonium", today=NOW).empty


def test_a_full_bar_ships_and_a_missing_one_stays_absent(monkeypatch):
    """Yahoo's OHLC ride along when served (#332's candle chart); a frame
    without them yields None, never zeros — absence stays absence
    (invariant 2)."""
    with_ohlc = _bars(["2026-08-20"], [1150.25])
    with_ohlc["Open"] = [1148.00]
    with_ohlc["High"] = [1151.50]
    with_ohlc["Low"] = [1147.25]
    answers = {
        "ZSQ26.CBT": with_ohlc,
        "ZSU26.CBT": _bars(["2026-08-20"], [1149.00]),  # Close/Volume only
    }
    df, _ = _run_fetch(monkeypatch, answers)
    aug = df[df["ticker"] == "ZSQ26.CBT"].iloc[0]
    assert (aug["Open"], aug["High"], aug["Low"]) == (1148.00, 1151.50, 1147.25)
    sep = df[df["ticker"] == "ZSU26.CBT"].iloc[0]
    # NaN in the frame, NULL once stored — same convention as Volume above.
    assert pd.isna(sep["Open"]) and pd.isna(sep["High"]) and pd.isna(sep["Low"])


def test_the_fetch_path_is_the_settlement_guarded_one():
    """The stubs above replace ``ch.fetch_one`` by name, which would stay
    green if the module quietly switched to the unguarded ``download_bars``
    — and an unfinished bar would land as a close (invariant 11). Pin the
    binding itself."""
    import fetchers.yfinance

    assert ch.fetch_one is fetchers.yfinance.fetch_one


# ── Cleaner ─────────────────────────────────────────────────────────

def test_clean_drops_nonpositive_and_dedupes():
    df = pd.DataFrame({
        "ticker": ["ZSQ26.CBT"] * 3 + ["ZSU26.CBT"],
        "Date": ["2026-08-20", "2026-08-20", "2026-08-21", "2026-08-21"],
        "Close": [0.0, 1147.0, 1150.0, 1152.0],
        "Volume": [None, 10.0, 11.0, 12.0],
    })
    out = clean_contract_bars(df)
    assert len(out) == 3
    assert (out["Close"] > 0).all()
    # The zero-close duplicate lost to the real print on the same key.
    kept = out[(out["ticker"] == "ZSQ26.CBT") & (out["Date"] == "2026-08-20")]
    assert kept["Close"].tolist() == [1147.0]


def test_clean_withholds_an_incoherent_candle_but_keeps_its_close():
    """A bar whose high sits below its low (or whose open is missing) is an
    impossible or partial candle: the Open/High/Low trio is withheld as a
    unit, the validated close survives — a partial candle drawn anyway
    would be an invented one (invariant 2, #332)."""
    df = pd.DataFrame({
        "ticker": ["ZSX26.CBT"] * 3,
        "Date": ["2026-08-19", "2026-08-20", "2026-08-21"],
        "Open": [1148.0, 1160.0, None],
        "High": [1151.0, 1155.0, 1160.0],   # 08-20: high < low
        "Low": [1147.0, 1158.0, 1150.0],    # 08-21: open missing
        "Close": [1150.0, 1160.0, 1155.0],
        "Volume": [100.0, 200.0, 300.0],
    })
    out = clean_contract_bars(df)
    assert list(out["Close"]) == [1150.0, 1160.0, 1155.0]
    good = out[out["Date"] == "2026-08-19"].iloc[0]
    assert (good["Open"], good["High"], good["Low"]) == (1148.0, 1151.0, 1147.0)
    for session in ("2026-08-20", "2026-08-21"):
        row = out[out["Date"] == session].iloc[0]
        assert pd.isna(row["Open"]) and pd.isna(row["High"]) and pd.isna(row["Low"])


def test_clean_preserves_null_volume():
    df = pd.DataFrame({
        "ticker": ["ZSQ26.CBT"], "Date": ["2026-08-20"],
        "Close": [1147.0], "Volume": [None],
    })
    out = clean_contract_bars(df)
    assert out["Volume"].isna().all()


# ── Store round-trip ────────────────────────────────────────────────

@pytest.fixture
def bars_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "ticker": ["ZSQ26.CBT", "ZSQ26.CBT", "ZSN26.CBT"],
        "contract_month": ["2026-08-01", "2026-08-01", "2026-07-01"],
        "Date": ["2026-08-20", "2026-08-21", "2026-07-10"],
        "Close": [1147.0, 1150.0, 1120.0],
        "Volume": [10.0, None, 5.0],
    })


def test_save_and_read_roundtrip(patched_db, bars_frame):
    save_contract_bars("Soybeans", bars_frame)
    out = read_contract_bars("Soybeans")
    assert len(out) == 3
    assert set(out["ticker"]) == {"ZSQ26.CBT", "ZSN26.CBT"}
    row = out[out["Date"] == pd.Timestamp("2026-08-21")].iloc[0]
    assert row["Close"] == 1150.0
    assert pd.isna(row["Volume"])  # NULL survived — never became 0


def test_save_is_an_upsert_not_a_snapshot_replace(patched_db, bars_frame):
    save_contract_bars("Soybeans", bars_frame)
    # A later run re-fetches the listed contract only; the expired
    # contract's rows must survive untouched.
    later = pd.DataFrame({
        "ticker": ["ZSQ26.CBT"], "contract_month": ["2026-08-01"],
        "Date": ["2026-08-22"], "Close": [1155.0], "Volume": [9.0],
    })
    save_contract_bars("Soybeans", later)
    out = read_contract_bars("Soybeans")
    assert len(out) == 4
    assert "ZSN26.CBT" in set(out["ticker"])


def test_captured_tickers_reflects_store(patched_db, bars_frame):
    assert captured_contract_tickers() == frozenset()
    save_contract_bars("Soybeans", bars_frame)
    assert captured_contract_tickers() == frozenset({"ZSQ26.CBT", "ZSN26.CBT"})
