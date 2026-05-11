"""Verify USDA fetchers use a dynamic year_end (not a hardcoded year).

Catches the regression where year_end=2026 silently stops returning new
data after the marketing year ends.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest


@pytest.fixture
def stub_usda(monkeypatch):
    """Pretend USDA_API_KEY is set and stub requests.get to capture params."""
    import fetchers.usda as usda_mod
    monkeypatch.setattr(usda_mod, "USDA_API_KEY", "test-key")

    captured: dict = {}

    class FakeResp:
        status_code = 200
        text = ""

        def json(self):
            return {"data": []}

    def fake_get(url, params=None, timeout=None):
        captured["params"] = params
        return FakeResp()

    monkeypatch.setattr(usda_mod.requests, "get", fake_get)
    return usda_mod, captured


def test_fetch_usda_uses_dynamic_year_end(stub_usda):
    usda_mod, captured = stub_usda
    usda_mod.fetch_usda("SOYBEANS")

    expected = str(datetime.now(timezone.utc).year + 1)
    assert captured["params"]["year__LE"] == expected


def test_fetch_crop_progress_uses_dynamic_year_end(stub_usda):
    usda_mod, captured = stub_usda
    usda_mod.fetch_crop_progress("SOYBEANS")

    expected = str(datetime.now(timezone.utc).year + 1)
    assert captured["params"]["year__LE"] == expected


def test_fetch_usda_respects_explicit_year_end(stub_usda):
    usda_mod, captured = stub_usda
    usda_mod.fetch_usda("SOYBEANS", year_end=2030)

    assert captured["params"]["year__LE"] == "2030"
