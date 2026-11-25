"""#637 — Layer 5 through our own Cloudflare Worker, so the quota is ours.

Open-Meteo's free quota is shared per outgoing IP, and GitHub runners share
IPs: on 2026-11-25 another tenant spent it before our first request. The
owner's decision (2026-11-25, evening) is a free Cloudflare Worker that
forwards to api.open-meteo.com — Open-Meteo counts each Worker on its own —
deployed by the owner with ``scripts/setup_openmeteo_worker.sh``.

The fetcher's contract, pinned here against recorded responses:

* both ``OPENMETEO_PROXY_URL`` and ``OPENMETEO_PROXY_TOKEN`` set → the one
  batched request goes to the Worker, with the token in a header;
* neither set → direct to Open-Meteo, as before;
* exactly one set → hard failure before any request (invariant 1: half a
  configuration is a mistake, not a fallback);
* the run log says which path it took, and never carries the token;
* the Worker's own refusal (bad token, path not allowed) is a configuration
  error — raised, never retried, never quietly answered direct.
"""

from __future__ import annotations

import json
import logging
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

import config
import fetchers.weather as weather
from pipeline.results import UpstreamRateLimited

FIXTURES = Path(__file__).parent / "fixtures"
BATCH = json.loads((FIXTURES / "openmeteo_forecast_batch_2026-11-25.json").read_text())
DAILY_429 = json.loads((FIXTURES / "openmeteo_429_daily_2026-11-25.json").read_text())
NOW = pd.Timestamp("2026-11-25 20:49", tz="UTC")

PROXY = "https://mirror-market-openmeteo.example.workers.dev"
# A value that could only ever reach a log line or an error if it leaked.
TOKEN = f"tok-{uuid.uuid4().hex}-never-log-me"


def _resp(status: int, payload, headers: dict | None = None) -> MagicMock:
    resp = MagicMock(status_code=status, headers=headers or {})
    resp.json.return_value = payload
    resp.text = json.dumps(payload)
    return resp


def _relayed(status: int, payload, headers: dict | None = None) -> MagicMock:
    """An Open-Meteo answer as the Worker relays it: unchanged, plus its marker."""
    return _resp(status, payload, {**(headers or {}), weather.PROXY_UPSTREAM_HEADER: "api.open-meteo.com"})


def _cloudflare_page(status: int, html: str) -> MagicMock:
    """Cloudflare answering instead of the Worker (e.g. Error 1027): HTML, no marker."""
    resp = MagicMock(status_code=status, headers={"Content-Type": "text/html"})
    resp.json.side_effect = json.JSONDecodeError("Expecting value", html, 0)
    resp.text = html
    return resp


OVER_QUOTA_403 = {"error": True, "reason": "Your API key is over quota."}
CF_1027 = (
    "<!DOCTYPE html><html><head><title>Worker exceeded resource limits | "
    "workers.dev | Cloudflare</title></head><body>Error 1027</body></html>"
)


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr(weather.time, "sleep", lambda s: None)
    monkeypatch.setattr(weather, "retry_sleep", lambda attempt: None)


@pytest.fixture
def http(monkeypatch):
    calls: list[dict] = []
    queue: list = []

    def fake_get(url, params=None, timeout=None, headers=None):
        calls.append({"url": url, "params": params, "headers": dict(headers or {})})
        item = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(weather.requests, "get", fake_get)
    return calls, queue


@pytest.fixture
def proxied(monkeypatch):
    monkeypatch.setenv("OPENMETEO_PROXY_URL", PROXY)
    monkeypatch.setenv("OPENMETEO_PROXY_TOKEN", TOKEN)


# ── routing ─────────────────────────────────────────────────────────────────


def test_with_both_set_the_batch_goes_to_the_worker_with_the_token_header(http, no_sleep, proxied):
    calls, queue = http
    queue.append(_relayed(200, BATCH))

    results = weather.fetch_all_regions(now=NOW)

    assert len(calls) == 1
    assert calls[0]["url"] == f"{PROXY}/v1/forecast"
    assert calls[0]["headers"][weather.PROXY_TOKEN_HEADER] == TOKEN
    assert all(len(df) == 37 for df in results.values())


def test_the_worker_gets_exactly_the_params_open_meteo_would(http, no_sleep, monkeypatch):
    calls, queue = http
    queue.append(_resp(200, BATCH))
    weather.fetch_all_regions(now=NOW)
    direct_params = calls[-1]["params"]

    monkeypatch.setenv("OPENMETEO_PROXY_URL", PROXY + "/")  # a trailing slash is the same Worker
    monkeypatch.setenv("OPENMETEO_PROXY_TOKEN", TOKEN)
    queue[:] = [_relayed(200, BATCH)]
    weather.fetch_all_regions(now=NOW)

    assert calls[-1]["url"] == f"{PROXY}/v1/forecast"
    assert calls[-1]["params"] == direct_params


def test_with_neither_set_it_goes_direct_and_sends_no_token(http, no_sleep):
    calls, queue = http
    queue.append(_resp(200, BATCH))

    weather.fetch_all_regions(now=NOW)

    assert calls[0]["url"] == config.OPENMETEO_FORECAST_URL == "https://api.open-meteo.com/v1/forecast"
    assert weather.PROXY_TOKEN_HEADER not in calls[0]["headers"]


@pytest.mark.parametrize("only", ["OPENMETEO_PROXY_URL", "OPENMETEO_PROXY_TOKEN"])
def test_half_a_proxy_configuration_hard_fails_before_any_request(http, no_sleep, monkeypatch, only):
    calls, queue = http
    queue.append(_resp(200, BATCH))
    monkeypatch.setenv(only, PROXY if only.endswith("URL") else TOKEN)

    with pytest.raises(weather.OpenMeteoProxyError, match="both"):
        weather.fetch_all_regions(now=NOW)
    assert calls == []


def test_a_plain_http_proxy_url_hard_fails_rather_than_send_the_token_in_clear(http, no_sleep, monkeypatch):
    calls, queue = http
    queue.append(_resp(200, BATCH))
    monkeypatch.setenv("OPENMETEO_PROXY_URL", "http://mirror-market-openmeteo.example.workers.dev")
    monkeypatch.setenv("OPENMETEO_PROXY_TOKEN", TOKEN)

    with pytest.raises(weather.OpenMeteoProxyError, match="https"):
        weather.fetch_all_regions(now=NOW)
    assert calls == []


# ── the log names the path, never the token ─────────────────────────────────


def test_the_log_names_the_worker_path_and_never_the_token(http, no_sleep, proxied, caplog):
    _calls, queue = http
    queue.append(_relayed(200, BATCH, {"X-RateLimit-Cost": "24.0", "X-RateLimit-Remaining": "9976"}))

    with caplog.at_level(logging.DEBUG, logger="fetchers.weather"):
        weather.fetch_all_regions(now=NOW)

    assert "via Worker mirror-market-openmeteo.example.workers.dev" in caplog.text
    assert TOKEN not in caplog.text


def test_the_log_names_the_direct_path_and_its_shared_quota(http, no_sleep, caplog):
    _calls, queue = http
    queue.append(_resp(200, BATCH))

    with caplog.at_level(logging.INFO, logger="fetchers.weather"):
        weather.fetch_all_regions(now=NOW)

    assert "direct to api.open-meteo.com" in caplog.text
    assert "shared" in caplog.text


def test_no_failure_path_leaks_the_token(http, no_sleep, proxied, caplog):
    _calls, queue = http
    queue.extend([_relayed(502, {"error": True, "reason": "bad gateway"})] * 3)

    with caplog.at_level(logging.DEBUG, logger="fetchers.weather"):
        results = weather.fetch_all_regions(now=NOW)

    assert all(df.empty for df in results.values())
    assert TOKEN not in caplog.text


# ── what a refusal means on each path ───────────────────────────────────────


def test_a_quota_429_through_the_worker_names_the_workers_own_quota(http, no_sleep, proxied):
    calls, queue = http
    queue.append(_relayed(429, DAILY_429, {"Retry-After": "33000"}))

    with pytest.raises(UpstreamRateLimited) as info:
        weather.fetch_all_regions(now=NOW)

    assert len(calls) == 1
    reason = info.value.reason
    assert "Daily API request limit exceeded" in reason
    assert "Retry-After 33000 s" in reason
    assert "Worker" in reason
    assert "shared per outgoing IP" not in reason
    assert TOKEN not in reason


def test_a_quota_429_direct_still_names_the_shared_ip(http, no_sleep):
    _calls, queue = http
    queue.append(_resp(429, DAILY_429))

    with pytest.raises(UpstreamRateLimited) as info:
        weather.fetch_all_regions(now=NOW)

    assert "shared per outgoing IP" in info.value.reason


@pytest.mark.parametrize(
    ("status", "code"), [(401, "unauthorized"), (404, "path_not_allowed"), (500, "misconfigured")],
)
def test_the_workers_own_refusal_is_a_config_error_never_retried(http, no_sleep, proxied, status, code):
    calls, queue = http
    queue.append(_resp(status, {"error": True, "reason": code}, {weather.PROXY_ERROR_HEADER: code}))

    with pytest.raises(weather.OpenMeteoProxyError, match=code) as info:
        weather.fetch_all_regions(now=NOW)

    assert len(calls) == 1
    assert TOKEN not in str(info.value)


def test_an_upstream_5xx_relayed_by_the_worker_is_still_transient(http, no_sleep, proxied):
    # A 5xx without the Worker's own marker is Open-Meteo's, forwarded
    # unchanged: retried like any transport failure, then the second pass.
    calls, queue = http
    queue.extend([_relayed(503, {"error": True, "reason": "upstream"}), _relayed(200, BATCH)])

    results = weather.fetch_all_regions(now=NOW)

    assert len(calls) == 2
    assert all(len(df) == 37 for df in results.values())


# ── review of #658 (0818e5c): the Worker PR's list ──────────────────────────


def test_the_direct_url_is_read_from_the_module_at_call_time(http, no_sleep, monkeypatch):
    """The URL seam: a test, or the later paid customer host, patches one name."""
    calls, queue = http
    queue.append(_resp(200, BATCH))
    monkeypatch.setattr(weather, "OPENMETEO_FORECAST_URL", "https://customer-api.open-meteo.com/v1/forecast")

    weather.fetch_all_regions(now=NOW)

    assert calls[0]["url"] == "https://customer-api.open-meteo.com/v1/forecast"


def test_a_cloudflare_429_page_is_not_read_as_an_open_meteo_quota(http, no_sleep, proxied):
    # Cloudflare answering for the Worker (its own limit, Error 1027) carries
    # no relay marker and an HTML body. It is a rate limit — Cloudflare's —
    # and the reason must say so, never quote Open-Meteo's quota.
    calls, queue = http
    queue.append(_cloudflare_page(429, CF_1027))

    with pytest.raises(UpstreamRateLimited) as info:
        weather.fetch_all_regions(now=NOW)

    assert len(calls) == 1
    reason = info.value.reason
    assert reason.startswith("Cloudflare HTTP 429")
    assert "Worker exceeded resource limits" in reason
    assert "<" not in reason
    assert "Open-Meteo HTTP 429" not in reason


def test_an_unmarked_200_through_the_worker_hard_fails(http, no_sleep, proxied):
    # A 200 the Worker did not relay is something else's page (a parked
    # domain, a captive portal): never parsed as weather.
    calls, queue = http
    queue.append(_resp(200, BATCH))

    with pytest.raises(weather.OpenMeteoProxyError, match="not relayed by the Worker"):
        weather.fetch_all_regions(now=NOW)
    assert len(calls) == 1


def test_an_unmarked_cloudflare_5xx_is_transient(http, no_sleep, proxied):
    calls, queue = http
    queue.extend([_cloudflare_page(502, "<html><title>Bad gateway</title></html>"), _relayed(200, BATCH)])

    results = weather.fetch_all_regions(now=NOW)

    assert len(calls) == 2
    assert all(len(df) == 37 for df in results.values())


@pytest.mark.parametrize("via_worker", [False, True])
def test_a_paid_key_over_quota_403_is_rate_limited_not_retried(http, no_sleep, monkeypatch, via_worker):
    # open-meteo docs/api-rate-limiting.md @ aeab72e L134: "the response is
    # HTTP `403 Forbidden` with the reason `Your API key is over quota.`
    # Review your usage and subscription rather than repeatedly retrying".
    calls, queue = http
    if via_worker:
        monkeypatch.setenv("OPENMETEO_PROXY_URL", PROXY)
        monkeypatch.setenv("OPENMETEO_PROXY_TOKEN", TOKEN)
        queue.append(_relayed(403, OVER_QUOTA_403))
    else:
        queue.append(_resp(403, OVER_QUOTA_403))

    with pytest.raises(UpstreamRateLimited) as info:
        weather.fetch_all_regions(now=NOW)

    assert len(calls) == 1
    assert "Open-Meteo HTTP 403: Your API key is over quota." in info.value.reason


def test_any_other_403_is_not_a_quota(http, no_sleep):
    calls, queue = http
    queue.extend([_resp(403, {"error": True, "reason": "Forbidden"})] * 3)

    results = weather.fetch_all_regions(now=NOW)

    assert all(df.empty for df in results.values())
    assert len(calls) == 6  # 3 attempts × 2 passes, as any non-quota refusal
