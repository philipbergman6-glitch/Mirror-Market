"""Layer 4 (CFTC COT) fetcher — transport failures and market-name drift.

Fixtures are real CFTC artifacts (see tests/fixtures/README.md):
  - ``cot_annualof_2026_sample.txt`` — current annual Legacy futures+options
    file, truncated to 4 real rows per tracked market.
  - ``cot_annualof_2010_sample.txt`` — the same report for 2010, when the
    SRW contract was still published as ``WHEAT - CHICAGO BOARD OF TRADE``
    (today: ``WHEAT-SRW - CHICAGO BOARD OF TRADE``) and canola was absent.
    That is a *real* CFTC market rename, not a hand-built one.
  - ``cftc_landing_200.html`` — cftc.gov answering HTTP 200 with HTML where
    the pipeline expects a zip; feeding those bytes to ``zipfile.ZipFile``
    raises a genuine ``BadZipFile``.
"""

from __future__ import annotations

import io
import urllib.error
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from config import COT_COMMODITIES, MAX_RETRIES
from fetchers import cot as cot_fetcher
from pipeline.results import ScraperShapeError

FIXTURES = Path(__file__).parent / "fixtures"


def _load(name: str) -> pd.DataFrame:
    return pd.read_csv(FIXTURES / name, low_memory=False)


def _real_bad_zip_error() -> zipfile.BadZipFile:
    """The exception CFTC's HTML-instead-of-zip response actually produces."""
    html = (FIXTURES / "cftc_landing_200.html").read_bytes()
    try:
        zipfile.ZipFile(io.BytesIO(html))
    except zipfile.BadZipFile as exc:
        return exc
    raise AssertionError("fixture is a valid zip — it must not be")


# ── market-name drift ───────────────────────────────────────────────


def test_current_cftc_file_matches_config(monkeypatch):
    """Sanity guard: today's real market names all match config.py."""
    monkeypatch.setattr(
        cot_fetcher, "_download_cot_year",
        lambda year: _load("cot_annualof_2026_sample.txt"),
    )
    df = cot_fetcher.fetch_cot_year(2026)
    assert set(df["market_name"].unique()) == set(COT_COMMODITIES.values())


def test_renamed_market_hard_fails(monkeypatch):
    """A CFTC rename must be a visible failure, not an empty commodity."""
    monkeypatch.setattr(
        cot_fetcher, "_download_cot_year",
        lambda year: _load("cot_annualof_2010_sample.txt"),
    )
    with pytest.raises(ScraperShapeError) as exc:
        cot_fetcher.fetch_cot_year(2010)

    message = str(exc.value)
    assert "WHEAT-SRW - CHICAGO BOARD OF TRADE" in message
    assert "CANOLA - ICE FUTURES U.S." in message


def test_renamed_market_hard_fails_through_fetch_cot_recent(monkeypatch):
    """The rename must not be swallowed on the way out of the layer either."""
    monkeypatch.setattr(
        cot_fetcher, "_download_cot_year",
        lambda year: _load("cot_annualof_2010_sample.txt"),
    )
    with pytest.raises(ScraperShapeError):
        cot_fetcher.fetch_cot_recent(years_back=1)


def test_shape_error_is_not_retried(monkeypatch):
    """A rename is not transient — retrying it just delays the failure."""
    calls = []

    def _boom(year: int) -> pd.DataFrame:
        calls.append(year)
        return _load("cot_annualof_2010_sample.txt")

    monkeypatch.setattr(cot_fetcher, "_download_cot_year", _boom)
    with pytest.raises(ScraperShapeError):
        cot_fetcher.fetch_cot_year(2010)
    assert len(calls) == 1


# ── transport failures ──────────────────────────────────────────────


@pytest.mark.parametrize(
    "exc_factory",
    [
        _real_bad_zip_error,
        lambda: urllib.error.URLError("nodename nor servname provided"),
    ],
    ids=["badzipfile", "urlerror"],
)
def test_transport_failures_are_retried_not_raised(monkeypatch, exc_factory):
    """BadZipFile / URLError are transport failures: retry, then degrade."""
    calls = []

    def _raise(year: int) -> pd.DataFrame:
        calls.append(year)
        raise exc_factory()

    monkeypatch.setattr(cot_fetcher, "_download_cot_year", _raise)
    monkeypatch.setattr(cot_fetcher, "retry_sleep", lambda attempt: None)

    df = cot_fetcher.fetch_cot_year(2026)

    assert df.empty
    assert len(calls) == MAX_RETRIES
