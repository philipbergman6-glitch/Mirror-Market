"""B25 (#495): the CNY leg is the ECB reference cross, not Yahoo CNYUSD=X.

Fixture values are the ECB's own published reference rates for
2026-10-12 → 2026-10-16 (``EXR D.USD+CNY.EUR.SP00.A``, read 2026-10-18).
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd
import pytest
import requests

import fetchers.ecb_fx as ecb_fx
import fetchers.yfinance as yf_module
from fetchers._settlement import FX_SESSION

ECB_CSV = """KEY,FREQ,CURRENCY,CURRENCY_DENOM,EXR_TYPE,EXR_SUFFIX,TIME_PERIOD,OBS_VALUE
EXR.D.CNY.EUR.SP00.A,D,CNY,EUR,SP00,A,2026-10-12,7.4832
EXR.D.CNY.EUR.SP00.A,D,CNY,EUR,SP00,A,2026-10-13,7.489
EXR.D.CNY.EUR.SP00.A,D,CNY,EUR,SP00,A,2026-10-14,7.4925
EXR.D.CNY.EUR.SP00.A,D,CNY,EUR,SP00,A,2026-10-15,7.4993
EXR.D.CNY.EUR.SP00.A,D,CNY,EUR,SP00,A,2026-10-16,7.524
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2026-10-12,1.1181
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2026-10-13,1.1188
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2026-10-14,1.1195
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2026-10-15,1.1208
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2026-10-16,1.1253
"""


class _Response:
    def __init__(self, text: str, status: int = 200) -> None:
        self.text = text
        self.status_code = status

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")


def _serve(monkeypatch, text: str, seen: list[dict] | None = None) -> None:
    def fake_get(url, params=None, timeout=None, **_):
        if seen is not None:
            seen.append({"url": url, **(params or {})})
        return _Response(text)

    monkeypatch.setattr(ecb_fx.requests, "get", fake_get)


def test_cross_is_usd_per_cny_from_the_two_euro_legs(monkeypatch) -> None:
    seen: list[dict] = []
    _serve(monkeypatch, ECB_CSV, seen)
    frame = ecb_fx.download_ecb_cross("CNY", period="5d", today=date(2026, 10, 18))

    assert list(frame.index.strftime("%Y-%m-%d")) == [
        "2026-10-12", "2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16",
    ]
    assert frame.index.name == "Date"
    # 1.1253 / 7.524 — the ECB 2026-10-16 cross, 0.149561 in the #495 table.
    assert frame.loc["2026-10-16", "Close"] == pytest.approx(0.1495614, abs=5e-8)
    # A reference rate is one print: no range to invent.
    assert (frame["Open"] == frame["Close"]).all()
    assert (frame["High"] == frame["Close"]).all() and (frame["Low"] == frame["Close"]).all()
    assert seen[0]["url"].endswith("/EXR/D.USD+CNY.EUR.SP00.A")
    assert seen[0]["startPeriod"] == "2026-10-13"


def test_a_day_only_one_leg_printed_is_not_a_cross(monkeypatch) -> None:
    """Invariant 8: both legs from the same day, or no number."""
    lines = [line for line in ECB_CSV.splitlines() if not line.startswith("EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2026-10-16")]
    _serve(monkeypatch, "\n".join(lines) + "\n")
    frame = ecb_fx.download_ecb_cross("CNY", period="5d", today=date(2026, 10, 18))
    assert frame.index.max() == pd.Timestamp("2026-10-15")


def test_shape_drift_raises_rather_than_returning_nothing(monkeypatch) -> None:
    _serve(monkeypatch, "KEY,TIME_PERIOD,VALUE\nEXR.D.CNY.EUR.SP00.A,2026-10-16,7.524\n")
    with pytest.raises(ecb_fx.EcbShapeError, match="OBS_VALUE"):
        ecb_fx.download_ecb_cross("CNY", period="5d", today=date(2026, 10, 18))


def test_an_empty_200_body_raises_rather_than_escaping_as_a_pandas_error(monkeypatch) -> None:
    _serve(monkeypatch, "")
    with pytest.raises(ecb_fx.EcbShapeError, match="empty"):
        ecb_fx.download_ecb_cross("CNY", period="5d", today=date(2026, 10, 18))


def test_an_empty_body_withholds_cny_and_leaves_the_other_pairs_standing(monkeypatch, caplog) -> None:
    monkeypatch.setattr(yf_module, "fetch_one", lambda ticker, period=None, rule=None: pd.DataFrame())
    _serve(monkeypatch, "")
    frames = yf_module.fetch_currencies(period="5d")
    assert frames["CNY/USD"].empty and len(frames) == 10
    assert "empty response body" in caplog.text


def test_a_duplicated_leg_on_one_day_raises_rather_than_picking_one(monkeypatch) -> None:
    _serve(monkeypatch, ECB_CSV + "EXR.D.CNY.EUR.SP00.A,D,CNY,EUR,SP00,A,2026-10-16,7.530\n")
    with pytest.raises(ecb_fx.EcbShapeError, match="duplicated"):
        ecb_fx.download_ecb_cross("CNY", period="5d", today=date(2026, 10, 18))


def test_a_non_positive_rate_raises(monkeypatch) -> None:
    _serve(monkeypatch, ECB_CSV.replace("2026-10-16,7.524", "2026-10-16,0"))
    with pytest.raises(ecb_fx.EcbShapeError, match="positive"):
        ecb_fx.download_ecb_cross("CNY", period="5d", today=date(2026, 10, 18))


def test_transport_failure_after_retries_is_an_empty_frame(monkeypatch) -> None:
    def dead(*_, **__):
        raise requests.ConnectionError("down")

    monkeypatch.setattr(ecb_fx.requests, "get", dead)
    monkeypatch.setattr(ecb_fx, "retry_sleep", lambda attempt: None)
    assert ecb_fx.download_ecb_cross("CNY", period="5d", today=date(2026, 10, 18)).empty


@pytest.mark.parametrize(
    ("period", "start"),
    [("5d", "2026-10-13"), ("1mo", "2026-09-18"), ("15y", "2011-10-18"), ("max", None)],
)
def test_period_maps_to_a_start_date(period: str, start: str | None) -> None:
    assert ecb_fx.period_start(period, date(2026, 10, 18)) == (date.fromisoformat(start) if start else None)


def test_an_unknown_period_raises() -> None:
    with pytest.raises(ValueError, match="period"):
        ecb_fx.period_start("ytd", date(2026, 10, 18))


# ---------------------------------------------------------------------------
# Layer 7 wiring
# ---------------------------------------------------------------------------


def test_layer_7_reads_cny_from_the_ecb_cross_never_from_yahoo(monkeypatch) -> None:
    asked: list[str] = []

    def fake_fetch_one(ticker, period=None, rule=None):
        asked.append(ticker)
        return pd.DataFrame()

    monkeypatch.setattr(yf_module, "fetch_one", fake_fetch_one)
    _serve(monkeypatch, ECB_CSV)
    monkeypatch.setattr(
        yf_module, "_utc_now", lambda: datetime(2026, 10, 18, 8, 0, tzinfo=timezone.utc)
    )
    frames = yf_module.fetch_currencies(period="5d")

    assert "CNYUSD=X" not in asked
    assert frames["CNY/USD"]["Close"].iloc[-1] == pytest.approx(0.1495614, abs=5e-8)


def test_the_ecb_cross_passes_the_same_fx_guard(monkeypatch) -> None:
    """At 22:00 UTC on 10-16 the London day 10-16 is still open: its row waits."""
    monkeypatch.setattr(yf_module, "fetch_one", lambda ticker, period=None, rule=None: pd.DataFrame())
    _serve(monkeypatch, ECB_CSV)
    monkeypatch.setattr(
        yf_module, "_utc_now", lambda: datetime(2026, 10, 16, 22, 0, tzinfo=timezone.utc)
    )
    frames = yf_module.fetch_currencies(period="5d")
    assert frames["CNY/USD"].index.max() == pd.Timestamp("2026-10-15")
    assert FX_SESSION.last_settled_session(datetime(2026, 10, 16, 22, 0, tzinfo=timezone.utc)) == date(2026, 10, 15)


def test_an_ecb_outage_leaves_cny_empty_with_no_yahoo_fallback(monkeypatch, caplog) -> None:
    asked: list[str] = []

    def fake_fetch_one(ticker, period=None, rule=None):
        asked.append(ticker)
        return pd.DataFrame()

    def dead(*_, **__):
        raise requests.ConnectionError("down")

    monkeypatch.setattr(yf_module, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(ecb_fx.requests, "get", dead)
    monkeypatch.setattr(ecb_fx, "retry_sleep", lambda attempt: None)
    frames = yf_module.fetch_currencies(period="5d")

    assert frames["CNY/USD"].empty
    assert "CNYUSD=X" not in asked
    assert "CNY/USD" in caplog.text


def test_ecb_shape_drift_withholds_cny_and_says_why(monkeypatch, caplog) -> None:
    monkeypatch.setattr(yf_module, "fetch_one", lambda ticker, period=None, rule=None: pd.DataFrame())
    _serve(monkeypatch, "KEY,TIME_PERIOD,VALUE\n")
    frames = yf_module.fetch_currencies(period="5d")
    assert frames["CNY/USD"].empty
    assert "OBS_VALUE" in caplog.text
