"""#637 — Layer 5 against Open-Meteo's shared free quota.

Open-Meteo's free API limits *weighted* calls per outgoing IP (10,000/day,
5,000/h, 600/min), and GitHub runners share IPs — so the layer can be
refused when its own usage is low (open-meteo/open-meteo
docs/api-rate-limiting.md). Two things follow, both pinned here against
recorded responses (no live calls):

* One HTTP request carries every pin (comma-separated coordinates), so a
  run makes 1 request instead of 24, and a refused run makes 1 instead of
  144 (24 pins × 3 retries × 2 passes).
* A quota 429 is its own named state, ``rate_limited``, carrying the
  upstream's reason — never retried, never a silent empty, never a
  ``last_success`` stamp.

Fixtures: ``openmeteo_forecast_batch_2026-11-25.json`` is one real
24-location response recorded from a workstation on 2026-11-25;
``openmeteo_429_daily_2026-11-25.json`` is the body every request of
deploy run 45635134090 received from 14:42:31 UTC that day.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

import config
import fetchers.weather as weather
import main
from pipeline.results import UpstreamRateLimited

FIXTURES = Path(__file__).parent / "fixtures"
BATCH = json.loads((FIXTURES / "openmeteo_forecast_batch_2026-11-25.json").read_text())
DAILY_429 = json.loads((FIXTURES / "openmeteo_429_daily_2026-11-25.json").read_text())
NOW = pd.Timestamp("2026-11-25 20:49", tz="UTC")


def _resp(status: int, payload, headers: dict | None = None) -> MagicMock:
    resp = MagicMock(status_code=status, headers=headers or {})
    resp.json.return_value = payload
    resp.text = json.dumps(payload)
    return resp


@pytest.fixture
def no_sleep(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr(weather.time, "sleep", sleeps.append)
    monkeypatch.setattr(weather, "retry_sleep", lambda attempt: sleeps.append(-attempt))
    return sleeps


@pytest.fixture
def http(monkeypatch):
    """Queue of responses; records every request's params."""
    calls: list[dict] = []
    queue: list = []

    def fake_get(url, params=None, timeout=None, headers=None, allow_redirects=True):
        calls.append({"url": url, "params": params})
        item = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(weather.requests, "get", fake_get)
    return calls, queue


# ---------------------------------------------------------------------------
# One request carries every pin
# ---------------------------------------------------------------------------


def test_a_full_run_is_one_http_request_for_all_pins(http, no_sleep):
    calls, queue = http
    queue.append(_resp(200, BATCH))

    results = weather.fetch_all_regions(now=NOW)

    assert len(calls) == 1
    params = calls[0]["params"]
    regions = list(config.GROWING_REGIONS.values())
    assert params["latitude"] == ",".join(str(c["lat"]) for c in regions)
    assert params["longitude"] == ",".join(str(c["lon"]) for c in regions)
    assert params["timezone"] == "auto"
    assert list(results) == list(config.GROWING_REGIONS)
    assert all(len(df) == 37 for df in results.values())
    assert no_sleep == []


def test_each_pin_keeps_its_own_day_boundary_inside_the_batch(http, no_sleep):
    # 20:49 UTC on 11-25 is already 11-26 in Jakarta and Shanghai, still
    # 11-25 in Iowa: the batch must place "today" per location, by the
    # timezone that location's own entry names.
    _calls, queue = http
    queue.append(_resp(200, BATCH))

    results = weather.fetch_all_regions(now=NOW)

    iowa = results["US Midwest (Iowa)"]
    riau = results[list(config.GROWING_REGIONS)[15]]
    assert BATCH[0]["timezone"] == "America/Chicago"
    assert BATCH[15]["timezone"] == "Asia/Jakarta"
    first_forecast = lambda df: df.loc[df["is_forecast"] == 1, "Date"].iloc[0]  # noqa: E731
    assert first_forecast(iowa) == "2026-11-25"
    assert first_forecast(riau) == "2026-11-26"


def test_the_batch_values_land_on_the_pin_that_asked_for_them(http, no_sleep):
    _calls, queue = http
    queue.append(_resp(200, BATCH))

    results = weather.fetch_all_regions(now=NOW)

    for i, name in enumerate(config.GROWING_REGIONS):
        assert list(results[name]["temp_max"]) == BATCH[i]["daily"]["temperature_2m_max"]


def test_a_batch_answering_fewer_locations_than_asked_hard_fails(http, no_sleep):
    _calls, queue = http
    queue.append(_resp(200, BATCH[:-1]))
    with pytest.raises(ValueError, match="24"):
        weather.fetch_all_regions(now=NOW)


def test_a_batch_whose_location_ids_are_out_of_order_hard_fails(http, no_sleep):
    shuffled = [dict(entry) for entry in BATCH]
    shuffled[1]["location_id"], shuffled[2]["location_id"] = 2, 1
    _calls, queue = http
    queue.append(_resp(200, shuffled))
    with pytest.raises(ValueError, match="location_id"):
        weather.fetch_all_regions(now=NOW)


def test_a_reordered_batch_with_no_location_ids_hard_fails_on_coordinates(http, no_sleep):
    # Review of #658: Open-Meteo documents `location_id` for CSV/XLSX only, and
    # entry 0 never carries one. Without ids, order is checked by coordinates:
    # each entry's grid cell must sit by the pin it is matched to.
    swapped = [{k: v for k, v in entry.items() if k != "location_id"} for entry in BATCH]
    swapped[0], swapped[1] = swapped[1], swapped[0]
    _calls, queue = http
    queue.append(_resp(200, swapped))
    with pytest.raises(ValueError, match="grid cell"):
        weather.fetch_all_regions(now=NOW)


def test_an_entry_whose_grid_cell_is_far_from_its_pin_hard_fails(http, no_sleep):
    moved = [dict(entry) for entry in BATCH]
    moved[5]["latitude"] = moved[5]["latitude"] + 1.0
    _calls, queue = http
    queue.append(_resp(200, moved))
    with pytest.raises(ValueError, match="grid cell"):
        weather.fetch_all_regions(now=NOW)


def test_the_grid_tolerance_tolerates_snapping_and_cannot_fit_two_pins():
    # Recorded snap (pin → returned grid-cell centre) is at most ~0.05°; the
    # tolerance must admit that, and be under half the closest pin spacing so
    # no grid cell can match two pins.
    tol = config.OPENMETEO_GRID_MATCH_MAX_DEG
    pins = list(config.GROWING_REGIONS.values())
    snap = max(max(abs(e["latitude"] - p["lat"]), abs(e["longitude"] - p["lon"])) for e, p in zip(BATCH, pins, strict=True))
    spacing = min(
        max(abs(a["lat"] - b["lat"]), abs(a["lon"] - b["lon"]))
        for i, a in enumerate(pins) for b in pins[i + 1:]
    )
    assert snap < tol < spacing / 2


def test_a_transient_miss_is_retried_as_one_batch_after_the_pause(http, no_sleep):
    """#577 still holds: a lost first pass gets one batched second pass."""
    import requests

    calls, queue = http
    queue.extend([requests.Timeout("t")] * config.MAX_RETRIES + [_resp(200, BATCH)])

    results = weather.fetch_all_regions(now=NOW)

    assert len(calls) == config.MAX_RETRIES + 1
    assert all(not df.empty for df in results.values())
    assert config.WEATHER_SECOND_PASS_DELAY_S in no_sleep


# ---------------------------------------------------------------------------
# A quota 429 is rate_limited: one request, a reason, no retries
# ---------------------------------------------------------------------------


def test_a_daily_quota_429_stops_after_one_request_with_the_upstream_reason(http, no_sleep):
    calls, queue = http
    queue.append(_resp(429, DAILY_429, {"Retry-After": "33600"}))

    with pytest.raises(UpstreamRateLimited) as info:
        weather.fetch_all_regions(now=NOW)

    assert len(calls) == 1
    assert no_sleep == []  # no backoff, no second-pass pause
    reason = info.value.reason
    assert "Daily API request limit exceeded" in reason
    assert "HTTP 429" in reason
    assert "33600" in reason
    assert "shared" in reason  # names the per-IP quota, so nobody hunts our own usage
    assert info.value.data == {}


def test_a_429_without_a_json_reason_is_still_rate_limited(http, no_sleep):
    resp = _resp(429, None)
    resp.json.side_effect = json.JSONDecodeError("x", "", 0)
    resp.text = "<html>Too Many Requests</html>"
    _calls, queue = http
    queue.append(resp)
    with pytest.raises(UpstreamRateLimited, match="HTTP 429"):
        weather.fetch_all_regions(now=NOW)


def test_a_concurrency_429_is_transient_not_a_quota(http, no_sleep):
    """'Too many concurrent requests' is queue pressure, uncharged: retry it."""
    calls, queue = http
    busy = _resp(429, {"error": True, "reason": "Too many concurrent requests"})
    queue.extend([busy, _resp(200, BATCH)])

    results = weather.fetch_all_regions(now=NOW)

    assert len(calls) == 2
    assert all(not df.empty for df in results.values())


def test_a_quota_429_on_the_second_pass_carries_the_first_pass_rows(monkeypatch):
    """Save first, grade second: rows that did arrive travel with the verdict."""
    row = pd.DataFrame({"Date": ["2026-11-24"], "temp_max": [30.0]})
    first = {name: (pd.DataFrame() if i == 0 else row) for i, name in enumerate(config.GROWING_REGIONS)}
    passes: list[list[str]] = []

    def fake_batch(regions, now):
        passes.append(list(regions))
        if len(passes) == 1:
            return first
        raise UpstreamRateLimited("weather", "Open-Meteo HTTP 429: quota")

    monkeypatch.setattr(weather, "_fetch_batch", fake_batch)
    monkeypatch.setattr(weather.time, "sleep", lambda s: None)
    with pytest.raises(UpstreamRateLimited) as info:
        weather.fetch_all_regions(now=NOW)

    assert passes[1] == [list(config.GROWING_REGIONS)[0]]
    assert len(info.value.data) == len(config.GROWING_REGIONS) - 1


# ---------------------------------------------------------------------------
# The pipeline names it, keeps last_success, and saves what arrived
# ---------------------------------------------------------------------------


@pytest.fixture
def captured(monkeypatch):
    calls: list[dict] = []

    def _capture(layer_name, rows_fetched=0, status="success", keys_returned=None,
                 keys_expected=None, missing_keys=None):
        calls.append({"layer": layer_name, "rows": rows_fetched, "status": status,
                      "keys_returned": keys_returned, "keys_expected": keys_expected})

    monkeypatch.setattr(main, "save_freshness", _capture)
    sets = (main._HARD_FAILURES, main._STALE_LAST_KNOWN_GOOD, main._RATE_LIMITED,
            main._INCOMPLETE_KEY_COVERAGE, main._USABLE_PARTIAL, main._NO_PUBLICATION)
    for s in sets:
        s.clear()
    yield calls
    for s in sets:
        s.clear()


def _layer(fetch, saved: list) -> main.DictLayer:
    return main.DictLayer(
        "weather", "Layer 5", "weather for growing regions",
        fetch=fetch, save=lambda n, d: saved.append(n), clean=lambda n, d: d,
    )


def test_a_rate_limited_layer_records_failed_and_never_stamps_last_success(captured):
    def fetch():
        raise UpstreamRateLimited("weather", "Open-Meteo HTTP 429: Daily API request limit exceeded")

    saved: list = []
    ok = main._run_dict_layer(_layer(fetch, saved))

    assert ok is False
    assert [c["status"] for c in captured] == ["failed"]  # preserves last_success
    assert main._RATE_LIMITED == {"weather": "Open-Meteo HTTP 429: Daily API request limit exceeded"}
    assert "weather" in main._HARD_FAILURES
    assert saved == []


def test_rows_that_arrived_before_the_429_are_saved_then_graded(captured):
    row = pd.DataFrame({"Date": ["2026-11-24"], "temp_max": [30.0]})

    def fetch():
        raise UpstreamRateLimited("weather", "quota", data={"US Illinois": row})

    saved: list = []
    main._run_dict_layer(_layer(fetch, saved))

    assert saved == ["US Illinois"]
    assert captured[-1]["status"] == "failed"
    assert captured[-1]["rows"] == 1
    assert captured[-1]["keys_returned"] == 1
    assert captured[-1]["keys_expected"] == len(config.GROWING_REGIONS)


def test_the_run_summary_classifies_rate_limited_apart_from_upstream_failure(captured):
    main._mark_failed("cot")
    main._mark_rate_limited("weather", "Open-Meteo HTTP 429: quota", {})

    classes = main._outcome_classes()

    assert classes["rate_limited"] == ["weather"]
    assert classes["upstream_failure"] == ["cot"]
    assert classes["rate_limited_detail"] == {"weather": "Open-Meteo HTTP 429: quota"}
    assert classes["degraded"] == ["cot", "weather"]


def test_the_ci_alert_names_the_rate_limited_layer_with_its_reason():
    from scripts import ci_layer_alert

    body = ci_layer_alert.build_alert_body({
        "classifications": {"rate_limited": ["weather"], "upstream_failure": []},
        "rate_limited_detail": {"weather": "Open-Meteo HTTP 429: Daily API request limit exceeded"},
    })
    assert "Rate-limited by upstream quota" in body
    assert "- `weather` — Open-Meteo HTTP 429: Daily API request limit exceeded" in body
    assert "weather" in ci_layer_alert.named_layers({"classifications": {"rate_limited": ["weather"]}})
