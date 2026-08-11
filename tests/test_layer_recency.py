"""T8 / audit F3 + F3a (issue #56): "success" must require recency.

F3  — `_finalize_layer` stamped `last_success` whenever the frames came back
      non-empty. Rows arriving is not the same as *new* rows arriving: an
      upstream that answers 200 OK with last month's file every day cleared
      every gate and stayed green forever.
F3a — `fetchers/worldbank.py` *detected* a >100-day-stale Pink Sheet (the
      documented GUID-rotation trap) and returned it anyway.

The mechanism under test: a stale layer is recorded with status='failed',
which `save_freshness` treats as "preserve the previous last_success". The
timestamp stops advancing, so the layer ages out of its
FRESHNESS_WARNING_DAYS_BY_LAYER window on its own and every surface that
already reads freshness reports it stale. Nothing downstream needs to know
this check exists.

No network, no DB — `save_freshness` is captured and the Pink Sheet download
is stubbed.
"""

from __future__ import annotations

import pandas as pd
import pytest

import main
from config import (
    FRESHNESS_WARNING_DAYS,
    FRESHNESS_WARNING_DAYS_BY_LAYER,
    LAYER_MAX_DATA_AGE_DAYS,
    LAYER_MIN_KEYS,
)


@pytest.fixture
def freshness_calls(monkeypatch):
    """Capture every save_freshness call main.py makes, without touching a DB."""
    calls: list[dict] = []

    def _capture(layer_name, rows_fetched=0, status="success"):
        calls.append(
            {"layer": layer_name, "rows": rows_fetched, "status": status}
        )

    monkeypatch.setattr(main, "save_freshness", _capture)
    main._HARD_FAILURES.clear()
    yield calls
    main._HARD_FAILURES.clear()


def _dated_frame(days_ago: float, rows: int = 5, col: str = "Date") -> pd.DataFrame:
    end = pd.Timestamp.now().normalize() - pd.Timedelta(days=days_ago)
    dates = pd.date_range(end=end, periods=rows, freq="D")
    return pd.DataFrame({col: dates, "value": range(rows)})


def _indexed_frame(days_ago: float, rows: int = 5) -> pd.DataFrame:
    end = pd.Timestamp.now().normalize() - pd.Timedelta(days=days_ago)
    dates = pd.date_range(end=end, periods=rows, freq="D")
    return pd.DataFrame({"Close": range(rows)}, index=pd.DatetimeIndex(dates, name="Date"))


def _price_layer(days_ago: float) -> dict:
    """A `prices`-shaped payload that clears the LAYER_MIN_KEYS floor."""
    return {f"C{i}": _indexed_frame(days_ago) for i in range(LAYER_MIN_KEYS["prices"])}


# ── The core F3 behaviour ──────────────────────────────────────────────────


def test_fresh_layer_stamps_success(freshness_calls):
    assert main._finalize_layer("prices", _price_layer(days_ago=1)) is True

    assert freshness_calls == [
        {"layer": "prices", "rows": 5 * LAYER_MIN_KEYS["prices"], "status": "success"}
    ]
    assert not main._HARD_FAILURES


def test_frozen_upstream_does_not_stamp_success(freshness_calls):
    """The F3 case: full-shaped payload, every key populated, all of it old.

    Before the fix this returned True and stamped a fresh last_success —
    which is precisely how a frozen source stayed green indefinitely.
    """
    stale_by = LAYER_MAX_DATA_AGE_DAYS["prices"] + 1

    assert main._finalize_layer("prices", _price_layer(days_ago=stale_by)) is False

    assert len(freshness_calls) == 1
    assert freshness_calls[0]["status"] == "failed"
    assert freshness_calls[0]["layer"] == "prices"


def test_stale_layer_counts_as_a_hard_failure(freshness_calls):
    """A frozen upstream is an outage, so the CI alerter must see it.

    `_HARD_FAILURES` is what lands in pipeline_status.json's `hard_failures`,
    which scripts/ci_layer_alert.py turns into a GitHub issue.
    """
    main._finalize_layer("prices", _price_layer(days_ago=90))

    assert "prices" in main._HARD_FAILURES


def test_boundary_exactly_at_budget_still_passes(freshness_calls):
    """The budget is inclusive — `age > budget` fails, `age == budget` does not.

    Pinned because an off-by-one here fires on the last good day of every
    exchange holiday.
    """
    at_budget = LAYER_MAX_DATA_AGE_DAYS["prices"]

    assert main._finalize_layer("prices", _price_layer(days_ago=at_budget)) is True
    assert freshness_calls[0]["status"] == "success"


def test_unlisted_layer_is_not_recency_checked(freshness_calls):
    """NOT LISTED = NOT CHECKED.

    psd/wasde/usda are keyed by marketing year and carry no date column at
    all; forward_curve is dated by contract month. They must pass through
    untouched rather than tripping the "no date found" branch.
    """
    assert "psd" not in LAYER_MAX_DATA_AGE_DAYS

    payload = {
        f"c{i}": pd.DataFrame({"year": [2019], "value": [1.0]})
        for i in range(LAYER_MIN_KEYS["psd"])
    }

    assert main._finalize_layer("psd", payload) is True
    assert freshness_calls[0]["status"] == "success"


def test_configured_layer_with_no_date_column_is_not_certified_fresh(freshness_calls):
    """A shape change is not a clean bill of health.

    If `prices` ever stops carrying a datetime index we cannot verify
    recency — and "cannot verify" must never resolve to "fresh".
    """
    payload = {
        f"C{i}": pd.DataFrame({"Close": [1.0, 2.0]})
        for i in range(LAYER_MIN_KEYS["prices"])
    }

    assert main._finalize_layer("prices", payload) is False
    assert freshness_calls[0]["status"] == "failed"


# ── Shape coverage: every layer's real post-clean frame ────────────────────


@pytest.mark.parametrize(
    "layer, make_frame",
    [
        # clean_ohlcv -> DataFrame with a DatetimeIndex named "Date"
        ("prices", _indexed_frame),
        ("currencies", _indexed_frame),
        # clean_cot / clean_weather / clean_dce_futures / clean_eia -> "Date" column
        ("cot", _dated_frame),
        ("weather", _dated_frame),
        ("dce", _dated_frame),
        ("eia", _dated_frame),
        # clean_export_sales -> "week_ending" column
        ("export_sales", lambda d: _dated_frame(d, col="week_ending")),
    ],
)
def test_recency_check_reads_each_layers_real_frame_shape(
    layer, make_frame, freshness_calls
):
    """Guards the highest-risk failure mode of this change.

    If the extractor misses a layer's date, that layer fails *every* run —
    and for `prices`/`fred` that is a CRITICAL_LAYERS exit-1 that blocks the
    deploy daily. Each shape here is the one pipeline/clean.py actually
    produces for that layer.
    """
    keys = LAYER_MIN_KEYS.get(layer, 1)
    fresh = {f"k{i}": make_frame(0) for i in range(keys)}
    stale = {
        f"k{i}": make_frame(LAYER_MAX_DATA_AGE_DAYS[layer] + 5) for i in range(keys)
    }

    assert main._finalize_layer(layer, fresh) is True, f"{layer}: fresh read as stale"

    freshness_calls.clear()
    main._HARD_FAILURES.clear()

    assert main._finalize_layer(layer, stale) is False, f"{layer}: stale read as fresh"


def test_fred_series_shape_is_datable(freshness_calls):
    """`clean_fred_series` returns a **Series**, not a DataFrame.

    It has a DatetimeIndex but no `.columns`, so a DataFrame-only extractor
    would find no date and fail FRED — a critical layer — on every run.
    """
    end = pd.Timestamp.now().normalize()
    fresh = {
        f"s{i}": pd.Series(
            range(5), index=pd.date_range(end=end, periods=5, freq="D")
        )
        for i in range(LAYER_MIN_KEYS["fred"])
    }

    assert main._finalize_layer("fred", fresh) is True
    assert freshness_calls[0]["status"] == "success"


def test_newest_frame_in_the_layer_decides(freshness_calls):
    """A layer is stale only when *everything* in it is stale.

    FRED bundles daily Treasury yields with monthly CPI; the monthly series
    is always weeks behind and must not drag the layer under.
    """
    payload = {f"C{i}": _indexed_frame(0) for i in range(LAYER_MIN_KEYS["prices"])}
    payload["C0"] = _indexed_frame(365)

    assert main._finalize_layer("prices", payload) is True


def test_tz_aware_and_naive_frames_compare_without_raising(freshness_calls):
    """Mixed tz-awareness across frames must not blow up the comparison."""
    end = pd.Timestamp.now(tz="UTC").normalize()
    payload = {
        f"C{i}": pd.DataFrame(
            {"Close": range(3)},
            index=pd.date_range(end=end, periods=3, freq="D"),
        )
        for i in range(LAYER_MIN_KEYS["prices"])
    }
    payload["C0"] = _indexed_frame(0)  # naive

    assert main._finalize_layer("prices", payload) is True


# ── Acceptance: stale flips the layer within its per-layer window ──────────


def test_frozen_upstream_flips_the_layer_to_stale_end_to_end(patched_db):
    """The acceptance criterion, run against a real DB.

    Simulates a source that freezes on day 0 and then answers 200 OK with
    the same file every day. Asserts the whole chain through the real
    `_finalize_layer`: the recency gate stops stamping,
    `save_freshness('failed')` preserves the old last_success (pinned
    separately in test_store.py:530), the displayed age therefore climbs,
    and the layer crosses its own freshness window.

    "N days after the freeze" is modelled by ageing the *data* rather than
    moving the clock — the two are the same thing to the gate, and this way
    no time is faked.
    """
    from pipeline.query import read_freshness

    budget = LAYER_MAX_DATA_AGE_DAYS["prices"]
    window = FRESHNESS_WARNING_DAYS_BY_LAYER.get("prices", FRESHNESS_WARNING_DAYS)

    def _run_on(day: int) -> None:
        """One pipeline run `day` days after the freeze, same stale file."""
        main._HARD_FAILURES.clear()
        main._finalize_layer("prices", _price_layer(days_ago=day))

    # Day 0: everything normal.
    _run_on(0)
    first = read_freshness().set_index("layer_name").loc["prices", "last_success"]
    assert pd.notna(first)

    # Last day the data is still inside budget — still stamping.
    _run_on(budget)
    still_stamping = (
        read_freshness().set_index("layer_name").loc["prices", "last_success"]
    )
    assert pd.notna(still_stamping)

    # One day past budget: the gate closes and last_success freezes.
    _run_on(budget + 1)
    row = read_freshness().set_index("layer_name").loc["prices"]
    assert row["status"] == "failed"
    assert row["last_success"] == still_stamping, (
        "last_success advanced on a run that delivered no new data — "
        "this is the F3 bug"
    )

    # Every later run repeats it, so the displayed age keeps climbing and
    # crosses the layer's freshness window.
    for day in range(budget + 2, budget + window + 3):
        _run_on(day)
    final = read_freshness().set_index("layer_name").loc["prices"]
    assert final["status"] == "failed"
    assert final["last_success"] == still_stamping


def test_worldbank_budget_exceeds_its_display_window():
    """worldbank is the one layer whose recency budget (100d, matching the
    fetcher's own GUID guard) is larger than its 42d display window.

    That ordering is deliberate: the dashboard shows it stale well before
    the fetcher starts discarding files, so a human sees the amber before
    the data disappears.
    """
    assert (
        LAYER_MAX_DATA_AGE_DAYS["worldbank"]
        > FRESHNESS_WARNING_DAYS_BY_LAYER["worldbank"]
    )


def test_every_budget_exceeds_the_layers_publication_cadence():
    """A threshold that cries wolf gets ignored — that is the failure mode
    this whole block exists to avoid. Weekly layers must tolerate a full
    week plus publication lag."""
    for layer in ("cot", "export_sales", "eia"):
        assert LAYER_MAX_DATA_AGE_DAYS[layer] >= 14, layer
    for layer in ("prices", "currencies"):
        assert LAYER_MAX_DATA_AGE_DAYS[layer] >= 4, layer  # Easter/Christmas breaks


# ── F3a: World Bank must not ship data it knows is stale ───────────────────


def _pink_sheet(monkeypatch, latest_month: pd.Timestamp):
    """Stub the download+parse so no network is touched."""
    from fetchers import worldbank

    dates = pd.date_range(end=latest_month, periods=24, freq="MS")
    parsed = {
        "Palm Oil": pd.DataFrame(
            {"Date": dates, "price": range(len(dates)), "unit": "$/mt"}
        )
    }
    monkeypatch.setattr(worldbank, "_download_pink_sheet", lambda: b"xlsx-bytes")
    monkeypatch.setattr(worldbank, "_parse_pink_sheet", lambda _b: parsed)
    return worldbank


def test_worldbank_returns_fresh_data_normally(monkeypatch):
    worldbank = _pink_sheet(
        monkeypatch, pd.Timestamp.today().normalize() - pd.Timedelta(days=35)
    )

    result = worldbank.fetch_worldbank_prices()

    assert "Palm Oil" in result
    assert not result["Palm Oil"].empty


def test_worldbank_discards_a_stale_file_instead_of_shipping_it(monkeypatch):
    """F3a proper: the guard existed and logged; it just did not act.

    A rotated-away CMO GUID keeps serving an old file with HTTP 200.
    Detecting that and storing it anyway is worse than not detecting it —
    the only evidence was a log line, and nothing reads the logs.
    """
    stale_days = LAYER_MAX_DATA_AGE_DAYS["worldbank"] + 30
    worldbank = _pink_sheet(
        monkeypatch, pd.Timestamp.today().normalize() - pd.Timedelta(days=stale_days)
    )

    assert worldbank.fetch_worldbank_prices() == {}


def test_worldbank_stale_threshold_comes_from_config(monkeypatch):
    """The fetcher guard and the layer budget must be the same number.

    Two independently-drifting staleness thresholds for one source is how
    you end up storing data the pipeline then flags as stale.
    """
    import config
    from fetchers import worldbank

    monkeypatch.setattr(
        config, "LAYER_MAX_DATA_AGE_DAYS", {**LAYER_MAX_DATA_AGE_DAYS, "worldbank": 5}
    )
    monkeypatch.setattr(
        worldbank, "LAYER_MAX_DATA_AGE_DAYS", {"worldbank": 5}
    )
    _pink_sheet(monkeypatch, pd.Timestamp.today().normalize() - pd.Timedelta(days=20))

    assert worldbank.fetch_worldbank_prices() == {}


def test_empty_worldbank_never_records_an_empty_success(freshness_calls):
    """The other half of F3a.

    `_mark_empty` stamps status='success', i.e. a fresh last_success. For a
    layer whose empty result means "download failed" or "we threw the file
    away", that fabricates exactly the freshness the discard was protecting
    against.
    """
    assert main._finalize_layer("worldbank", {}, empty_fails=True) is False

    assert freshness_calls[0]["status"] == "failed"
    assert "worldbank" in main._HARD_FAILURES


class _StopAfterDictLayers(BaseException):
    """Abort run() the moment the dict-layer table has been walked.

    Deliberately a BaseException: run()'s per-layer guards catch Exception,
    so anything narrower would be swallowed and the run would carry on into
    the live scraper layers (Layers 14-21) and hit the network.
    """


def test_worldbank_layer_is_wired_with_empty_fails(monkeypatch):
    """Pin the wiring, not just the helper.

    `empty_fails` defaults to False, so a dropped keyword argument silently
    restores the old empty-success behaviour with no test failing anywhere
    else.
    """
    monkeypatch.setattr(main, "init_database", lambda: None)
    monkeypatch.setattr(main, "import_history", lambda: None)

    captured: list[main.DictLayer] = []
    monkeypatch.setattr(
        main, "_run_dict_layer", lambda layer: captured.append(layer) or False
    )
    # Layer 14 is the first thing after the dict-layer loop and is a live
    # USDA call — stop the run here.
    def _stop():
        raise _StopAfterDictLayers

    monkeypatch.setattr(main, "fetch_crush_data", _stop)

    with pytest.raises(_StopAfterDictLayers):
        main.run()

    by_key = {layer.key: layer for layer in captured}
    assert by_key["worldbank"].empty_fails is True
    assert by_key["prices"].empty_fails is False


# ── Non-regression: the existing gates still work ──────────────────────────


def test_partial_layer_still_fails_the_min_keys_floor(freshness_calls):
    payload = {"C0": _indexed_frame(0)}  # 1 key vs a floor of 8

    assert main._finalize_layer("prices", payload) is False
    assert freshness_calls[0]["status"] == "failed"


def test_empty_non_critical_layer_still_records_empty_success(freshness_calls):
    """Unchanged default: a layer that ran fine with nothing to publish."""
    assert main._finalize_layer("safex", {}) is False

    assert freshness_calls[0]["status"] == "success"
    assert freshness_calls[0]["rows"] == 0
