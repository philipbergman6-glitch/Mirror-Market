"""Parser-level tests for the AFEX Nigeria feed (Layer 22).

No network. These exercise the JSON → NGN/MT transform, the schema-change
and unit-change guards, and the flat-run detector that distinguishes a
quiet market from a stalled feed.

The unit assertion matters more here than in the other spot layers: the
feed publishes NGN/kg, we store NGN/MT, and the difference is a factor of
1000 — exactly the kind of silent error that would put a plausible-looking
but wrong Nigerian price on the dashboard.
"""

from __future__ import annotations

import pandas as pd
import pytest

from fetchers.afex import _parse_series, _trailing_flat_days
from pipeline.clean import clean_nigeria_spot
from pipeline.results import ScraperShapeError

NAME = "Soybean (AFEX Nigeria)"


def _rows(pairs: list[tuple[str, float | None]], key: str = "SSBS") -> list[dict]:
    """Build a minimal feed payload: (date, NGN/kg price) pairs."""
    return [{"date": d, key: v, "SMAZ": 336.99} for d, v in pairs]


# ── Unit conversion ─────────────────────────────────────────────────────────

def test_ngn_per_kg_is_converted_to_ngn_per_mt() -> None:
    """681.25 NGN/kg is 681,250 NGN/MT — the 1000x that must not go wrong."""
    df = _parse_series(_rows([("2026-08-09", 681.25)]), "SSBS", NAME)
    assert df["price_ngn_mt"].iloc[0] == pytest.approx(681_250.0)
    assert df["Unit"].iloc[0] == "NGN/MT"


def test_rows_are_sorted_and_deduped_by_date() -> None:
    payload = _rows([("2026-08-09", 681.25), ("2026-08-07", 690.0), ("2026-08-09", 700.0)])
    df = _parse_series(payload, "SSBS", NAME)
    assert list(df["Date"].dt.strftime("%Y-%m-%d")) == ["2026-08-07", "2026-08-09"]
    # Later duplicate wins, so a same-day correction upstream is respected.
    assert df["price_ngn_mt"].iloc[-1] == pytest.approx(700_000.0)


def test_missing_and_nonpositive_prices_are_skipped() -> None:
    payload = _rows([("2026-08-05", None), ("2026-08-06", 0), ("2026-08-07", 681.25)])
    df = _parse_series(payload, "SSBS", NAME)
    assert len(df) == 1


# ── Shape guards ────────────────────────────────────────────────────────────

def test_absent_key_raises_shape_error() -> None:
    """The feed already renamed its keys once (SBS → SSBS in 2022)."""
    payload = [{"date": "2026-08-09", "SMAZ": 336.99}]
    with pytest.raises(ScraperShapeError, match="absent from every row"):
        _parse_series(payload, "SSBS", NAME)


def test_unit_change_raises_rather_than_storing_a_wrong_level() -> None:
    """If the feed ever serves NGN/MT in the S key, every value lands far
    below the sanity band — that must fail loudly, not store 0.68 NGN/MT."""
    payload = _rows([("2026-08-09", 0.68125), ("2026-08-08", 0.690)])
    with pytest.raises(ScraperShapeError, match="unit appears to have changed"):
        _parse_series(payload, "SSBS", NAME)


def test_partial_out_of_band_rows_are_dropped_not_fatal() -> None:
    payload = _rows([("2026-08-08", 681.25), ("2026-08-09", 0.0001)])
    df = _parse_series(payload, "SSBS", NAME)
    assert len(df) == 1
    assert df["price_ngn_mt"].iloc[0] == pytest.approx(681_250.0)


# ── Flat-run detection ──────────────────────────────────────────────────────

def test_trailing_flat_days_counts_the_constant_tail() -> None:
    assert _trailing_flat_days(pd.Series([1.0, 2.0, 3.0, 3.0, 3.0])) == 3
    assert _trailing_flat_days(pd.Series([1.0, 2.0, 3.0])) == 1
    assert _trailing_flat_days(pd.Series([], dtype=float)) == 0


def test_long_plateau_is_kept_not_discarded(caplog) -> None:
    """A 14-day plateau is normal for this feed (4 such runs historically,
    26-day max). It must warn at most — never silently drop the rows."""
    dates = pd.date_range("2026-07-27", periods=14, freq="D").strftime("%Y-%m-%d")
    df = _parse_series(_rows([(d, 681.25) for d in dates]), "SSBS", NAME)
    assert len(df) == 14


def test_plateau_beyond_historical_max_logs_an_error(caplog) -> None:
    dates = pd.date_range("2026-06-01", periods=40, freq="D").strftime("%Y-%m-%d")
    with caplog.at_level("ERROR"):
        _parse_series(_rows([(d, 681.25) for d in dates]), "SSBS", NAME)
    assert any("may be stalled" in r.message for r in caplog.records)


# ── Cleaner ─────────────────────────────────────────────────────────────────

def test_clean_nigeria_spot_is_nondestructive_and_sorts() -> None:
    raw = pd.DataFrame({
        "Date": ["2026-08-09", "2026-08-07"],
        "price_ngn_mt": [681_250.0, 690_000.0],
        "Unit": ["NGN/MT", "NGN/MT"],
    })
    before = raw.copy()
    out = clean_nigeria_spot(raw)
    pd.testing.assert_frame_equal(raw, before)
    assert list(out["Date"].dt.strftime("%Y-%m-%d")) == ["2026-08-07", "2026-08-09"]


def test_clean_nigeria_spot_handles_empty() -> None:
    assert clean_nigeria_spot(pd.DataFrame()).empty
