"""Layer 11b — per-contract close history (#332).

What these pin: the fetcher reuses Layer 11's ticker grammar and the guarded
``fetch_one`` path, an expired (empty) contract drops out silently rather than
erroring the layer, and the long-frame shape is exactly what the cleaner and
store expect. No network anywhere — ``fetch_one`` is stubbed per ticker.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

import fetchers.contract_history as ch
from pipeline.clean import clean_contract_history

NOW = date(2026, 8, 12)


def _bars(dates: list[str], closes: list[float]) -> pd.DataFrame:
    """A yfinance-shaped frame: OHLCV columns, DatetimeIndex."""
    idx = pd.DatetimeIndex(pd.to_datetime(dates), name="Date")
    return pd.DataFrame({"Close": closes, "Volume": [100.0] * len(closes)}, index=idx)


def _patch_fetch(monkeypatch, prices: dict[str, pd.DataFrame]) -> list[str]:
    asked: list[str] = []

    def fake_fetch_one(ticker: str, period: str = "2y", **_):
        asked.append(ticker)
        return prices.get(ticker, pd.DataFrame())

    monkeypatch.setattr(ch, "fetch_one", fake_fetch_one)
    return asked


def test_each_contract_contributes_its_own_sessions(monkeypatch):
    _patch_fetch(monkeypatch, {
        "ZSQ26.CBT": _bars(["2026-08-10", "2026-08-11"], [1147.25, 1149.00]),
        "ZSU26.CBT": _bars(["2026-08-11"], [1150.25]),
    })
    df = ch.fetch_contract_history("Soybeans", today=NOW)
    assert list(df.columns) == ["ticker", "contract_month", "label", "date", "close", "volume"]
    assert len(df) == 3
    aug = df[df["ticker"] == "ZSQ26.CBT"]
    assert list(aug["date"]) == ["2026-08-10", "2026-08-11"]
    assert list(aug["contract_month"].unique()) == ["2026-08-01"]
    assert list(aug["label"].unique()) == ["Aug 2026"]


def test_the_tickers_are_layer_elevens_own_grammar(monkeypatch):
    asked = _patch_fetch(monkeypatch, {})
    ch.fetch_contract_history("Soybeans", today=NOW)
    # Six upcoming Soybeans months from 2026-08-12, current month included —
    # identical to the forward-curve builder, because it IS that builder.
    assert asked == [
        "ZSQ26.CBT", "ZSU26.CBT", "ZSX26.CBT",
        "ZSF27.CBT", "ZSH27.CBT", "ZSK27.CBT",
    ]


def test_an_expired_contract_answers_empty_and_drops_out(monkeypatch):
    _patch_fetch(monkeypatch, {
        # ZSQ26 delisted: empty frame, exactly what Yahoo serves post-expiry.
        "ZSU26.CBT": _bars(["2026-08-11"], [1150.25]),
    })
    df = ch.fetch_contract_history("Soybeans", today=NOW)
    assert set(df["ticker"]) == {"ZSU26.CBT"}


def test_nothing_at_all_is_an_empty_frame_not_an_error(monkeypatch):
    _patch_fetch(monkeypatch, {})
    df = ch.fetch_contract_history("Soybeans", today=NOW)
    assert df.empty


def test_fetch_all_iterates_the_config_roster_and_skips_empty(monkeypatch):
    _patch_fetch(monkeypatch, {
        "ZSU26.CBT": _bars(["2026-08-11"], [1150.25]),
    })
    monkeypatch.setattr(ch, "CONTRACT_HISTORY_COMMODITIES", ("Soybeans", "Soybean Oil"))
    out = ch.fetch_all_contract_history()
    # Soybean Oil answered nothing on any ticker → absent, not an empty frame:
    # keys_returned must reflect what actually came back.
    assert list(out) == ["Soybeans"]


def test_clean_drops_nonpositive_closes_and_duplicate_sessions():
    df = pd.DataFrame({
        "ticker": ["ZSX26.CBT"] * 4,
        "contract_month": ["2026-11-01"] * 4,
        "label": ["Nov 2026"] * 4,
        "date": ["2026-08-10", "2026-08-11", "2026-08-11", "2026-08-12"],
        "close": [1150.0, 1160.0, 1167.75, -1.0],
        "volume": [100, 200, 250, 300],
    })
    cleaned = clean_contract_history(df)
    assert list(cleaned["date"]) == ["2026-08-10", "2026-08-11"]
    # The later duplicate wins — same outcome INSERT OR REPLACE would produce.
    assert cleaned.loc[cleaned["date"] == "2026-08-11", "close"].item() == 1167.75


def test_clean_returns_a_copy():
    df = pd.DataFrame({
        "ticker": ["ZSX26.CBT"], "contract_month": ["2026-11-01"],
        "label": ["Nov 2026"], "date": ["2026-08-10"],
        "close": [1150.0], "volume": [100],
    })
    cleaned = clean_contract_history(df)
    cleaned.loc[0, "close"] = 0.0
    assert df.loc[0, "close"] == 1150.0


def test_the_layer_is_registered_everywhere_a_layer_must_be():
    import config

    assert "contract_history" in config.PRODUCTION_LAYER_KEYS
    assert config.LAYER_MAX_DATA_AGE_DAYS["contract_history"] == 7
    assert config.layer_expected_keys("contract_history") == len(
        config.CONTRACT_HISTORY_COMMODITIES
    )
    # Deliberately NOT in the fast refresh (twenty 2y pulls) and NOT in the
    # history CSVs (self-healing, like prices).
    assert "contract_history" not in config.FAST_REFRESH_LAYERS
    from pipeline.history import HISTORY_TABLES

    assert "contract_history" not in HISTORY_TABLES
    from pipeline.divergence import GUARDED_TABLES

    assert GUARDED_TABLES["contract_history"] == ("close", ("ticker", "date"))


@pytest.mark.parametrize("commodity", ["Soybeans", "Soybean Oil", "Soybean Meal"])
def test_the_roster_is_the_soy_complex(commodity):
    import config

    assert commodity in config.CONTRACT_HISTORY_COMMODITIES
