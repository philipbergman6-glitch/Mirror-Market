"""Layer 15 — CONAB historical-series parser tests (ticket #63).

Runs against the real upstream artifact committed under ``tests/fixtures/``:

    tests/fixtures/conab_serie_historica_graos.txt.gz
        gzip of https://portaldeinformacoes.conab.gov.br/downloads/arquivos/
        SerieHistoricaGraos.txt — live download 2026-08-11 (bytes verbatim).
    tests/fixtures/conab_serie_historica_graos.headers.txt
        the response headers of that same download (carries Last-Modified).

The missing-state case is derived by deleting the real Mato Grosso rows
from a copy of the real file — real layout, one state removed.
"""

from __future__ import annotations

import gzip
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import pandas as pd
import pytest

from fetchers import conab
from fetchers.conab import _aggregate_national, _parse_csv, fetch_conab_estimates
from pipeline.results import ScraperShapeError

FIXTURES = Path(__file__).parent / "fixtures"
FIXTURE_GZ = FIXTURES / "conab_serie_historica_graos.txt.gz"
FIXTURE_HEADERS = FIXTURES / "conab_serie_historica_graos.headers.txt"

# CONAB serves text/plain with no charset, so requests decodes as latin-1.
_ENCODING = "latin-1"


def _fixture_text() -> str:
    return gzip.decompress(FIXTURE_GZ.read_bytes()).decode(_ENCODING)


def _fixture_last_modified() -> str:
    for line in FIXTURE_HEADERS.read_text(encoding="utf-8").splitlines():
        if line.lower().startswith("last-modified:"):
            return line.split(":", 1)[1].strip()
    raise AssertionError("fixture headers carry no Last-Modified")


def _fixture_frame() -> pd.DataFrame:
    df = _parse_csv(_fixture_text())
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def _drop_mato_grosso(df: pd.DataFrame) -> pd.DataFrame:
    """Delete every Mato Grosso row — the single largest soy/corn state."""
    return df[df["uf"].astype(str).str.strip() != "MT"].reset_index(drop=True)


class _StubResponse:
    def __init__(self, text: str, headers: dict[str, str]) -> None:
        self.status_code = 200
        self.text = text
        self.headers = headers


@pytest.fixture()
def stub_get(monkeypatch: pytest.MonkeyPatch):
    """Serve the committed fixture in place of the live CONAB download."""

    def _install(text: str | None = None, headers: dict[str, str] | None = None):
        payload = _fixture_text() if text is None else text
        hdrs = {"Last-Modified": _fixture_last_modified()} if headers is None else headers

        def _fake_get(url: str, timeout: int | float = 0, **kwargs: object):
            return _StubResponse(payload, hdrs)

        monkeypatch.setattr(conab.requests, "get", _fake_get)

    return _install


# ── The real file parses, and Mato Grosso dominates the national sum ────────

def test_real_fixture_aggregates_national_soybeans() -> None:
    grouped = _aggregate_national(_fixture_frame())
    soy = grouped[(grouped["produto_norm"] == "soja") & (grouped["ano_agricola"] == "2025/26")]
    assert len(soy) == 1
    # MT alone is ~51.6 Mt of the ~180 Mt national soybean crop.
    assert soy["production"].iloc[0] > 150_000.0


# ── AC #1: missing Mato Grosso aborts the national sum ──────────────────────

def test_missing_mato_grosso_aborts_national_sum() -> None:
    """A zeroed/absent MT must hard-fail, not silently shrink the total."""
    with pytest.raises(ScraperShapeError, match="MT"):
        _aggregate_national(_drop_mato_grosso(_fixture_frame()))


def test_unparseable_production_aborts_national_sum() -> None:
    """A non-numeric production cell must hard-fail, not fillna(0)."""
    df = _fixture_frame()
    mask = (
        (df["uf"].astype(str).str.strip() == "MT")
        & (df["produto"].astype(str).str.strip() == "SOJA")
        & (df["ano_agricola"].astype(str).str.strip() == "2025/26")
    )
    assert mask.any()
    df.loc[mask, "producao_mil_t"] = "n/d"
    with pytest.raises(ScraperShapeError, match="numeric"):
        _aggregate_national(df)


# ── AC #2: revision history reflects actual publication dates ───────────────

def test_report_date_is_publication_date_not_today(
    stub_get, monkeypatch: pytest.MonkeyPatch
) -> None:
    """report_date comes from the file's Last-Modified, never the clock.

    The wall clock is pushed forward so a today()-derived stamp cannot
    coincide with the fixture's real publication date.
    """

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz: object = None) -> datetime:  # type: ignore[override]
            return datetime(2027, 3, 2, tzinfo=timezone.utc)

    monkeypatch.setattr(conab, "datetime", _FrozenDatetime, raising=False)

    stub_get()
    result = fetch_conab_estimates()

    published = parsedate_to_datetime(_fixture_last_modified()).astimezone(
        timezone.utc
    ).strftime("%Y-%m-%d")
    assert not result.empty
    assert set(result["report_date"]) == {published}
    assert published != "2027-03-02"


def test_missing_last_modified_hard_fails(stub_get) -> None:
    """No publication date upstream = no invented one."""
    stub_get(headers={})
    with pytest.raises(ScraperShapeError, match="[Ll]ast-[Mm]odified"):
        fetch_conab_estimates()
