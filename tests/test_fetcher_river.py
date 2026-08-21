"""Layers 27 / 28 — river gauges (M25 #272, M26 #273).

River water is the freight leg of a cash bid, and the number is a stage
reading rather than a price. That makes the failure modes different from every
other layer here, and each one below parses cleanly and lies quietly if
unguarded:

1.  **NWPS prints its "no value" sentinel in the data field.** `-999` and
    `-9999` arrive in the same key as a real reading; 15 of them arrived in
    one St. Louis payload on 2026-08-21. On a gauge whose record low is
    -10.81 ft, a stored sentinel is not an outlier — it is the deepest
    low-water event in history, every day.

2.  **A forecast row overwriting an observation.** NWPS issues its trace the
    previous afternoon, so it *starts before* the newest observation. Into a
    `(gauge, Date)` upsert, yesterday's model output replaces yesterday's
    measurement, and nothing about the stored row says so.

3.  **A partial day stored as the day.** NWPS samples hourly; the current
    day's last reading is not yet the day's reading. A forecast trace is not
    an aggregate and must *not* be cut the same way — the guard applies to
    observations only.

4.  **Metres read as feet.** The two rivers share one table and one shape by
    design, and 2.97 is a plausible number in either unit. The unit rides on
    every row and the store refuses a row without one.

Plus the two structural facts the site depends on: a river renders inside
block 06 rather than as a tenth block, and a *configured* gauge with no rows
says so instead of vanishing.

No network: every payload here is a monkeypatched literal shaped like the
live one (probed 2026-08-21).
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

import config
import fetchers.river as river
import main
from app import markets as markets_mod
from app.block_builders import SiteContext, build_blocks
from app.markets import load_markets
from config import LAYER_MAX_DATA_AGE_DAYS, LAYER_MIN_KEYS
from pipeline import schema
from pipeline.clean import clean_river_levels
from pipeline.history import HISTORY_TABLES

MEMPHIS = "Mississippi at Memphis"
ST_LOUIS = "Mississippi at St. Louis"
ROSARIO = "Paraná at Rosario"

TODAY = date(2026, 8, 21)


def _spec(gauge: str) -> dict:
    return config.RIVER_GAUGES[gauge]


def _nwps_payload(observed: list[tuple[str, float]], forecast: list[tuple[str, float]],
                  *, units: str = "ft") -> dict:
    """A stageflow payload shaped like the live one."""
    def block(rows):
        return {
            "primaryName": "Stage",
            "primaryUnits": units,
            "secondaryName": "Flow",
            "secondaryUnits": "kcfs",
            "data": [
                {"validTime": when, "generatedTime": when, "primary": value, "secondary": -999}
                for when, value in rows
            ],
        }
    return {"observed": block(observed), "forecast": block(forecast)}


def _ina_payload(rows: list[tuple[str, float]]) -> list[dict]:
    return [
        {
            "id": 1, "tipo": "puntual", "series_id": 34,
            "timestart": when, "timeend": when,
            "nombre": "upsertObservacionesPuntual", "valor": value, "stats": None,
        }
        for when, value in rows
    ]


@pytest.fixture
def frozen_today(monkeypatch: pytest.MonkeyPatch):
    """Pin 'now' so the partial-day guard is exercised on a known date.

    2026-08-21 18:00 UTC is 13:00 in Chicago and 15:00 in Buenos Aires, so
    both gauges are mid-day in their own zone — the case the guard is for.
    """
    class _Now:
        @staticmethod
        def now(tz=None):
            stamp = pd.Timestamp("2026-08-21T18:00:00Z").to_pydatetime()
            return stamp if tz is None else stamp.astimezone(tz)

    monkeypatch.setattr(river, "datetime", _Now)
    return TODAY


def _patch_payload(monkeypatch: pytest.MonkeyPatch, payload):
    monkeypatch.setattr(river, "_get_json", lambda url, params, label: payload)


# ---------------------------------------------------------------------------
# Trap 1 — the sentinel that reads as a record low
# ---------------------------------------------------------------------------
def test_the_no_value_sentinel_never_becomes_a_stage(frozen_today, monkeypatch, caplog):
    _patch_payload(monkeypatch, _nwps_payload(
        observed=[
            ("2026-08-18T12:00:00Z", 11.4),
            ("2026-08-19T12:00:00Z", -9999),
            ("2026-08-20T12:00:00Z", 11.2),
        ],
        forecast=[],
    ))
    frame = river.fetch_nwps_gauge(MEMPHIS, _spec(MEMPHIS))

    assert frame["stage"].min() > 0
    # The sentinel day is dropped entirely rather than carried at a
    # neighbour's value: nobody measured it.
    assert "2026-08-19" not in {d.isoformat() for d in frame["Date"]}


def test_dropped_sentinels_are_reported_once_per_block_not_once_per_row(
    frozen_today, monkeypatch, caplog
):
    """Fifteen of them arrived in one live St. Louis payload."""
    _patch_payload(monkeypatch, _nwps_payload(
        observed=(
            [(f"2026-08-{day:02d}T0{hour}:00:00Z", -9999)
             for day in (18, 19) for hour in range(1, 8)]
            + [("2026-08-20T12:00:00Z", 8.4)]
        ),
        forecast=[],
    ))
    with caplog.at_level("WARNING"):
        river.fetch_nwps_gauge(ST_LOUIS, _spec(ST_LOUIS))

    dropped = [r for r in caplog.records if "outside the" in r.getMessage()]
    assert len(dropped) == 1
    assert "dropped 14 value(s)" in dropped[0].getMessage()


def test_a_block_quoted_in_the_wrong_unit_is_discarded_whole(frozen_today, monkeypatch):
    """NWPS carries stage and flow in one payload. A kcfs discharge read as a
    stage is the sentinel trap at a larger scale, and no per-row band catches
    it — a discharge in kcfs is a perfectly plausible number of feet."""
    _patch_payload(monkeypatch, _nwps_payload(
        observed=[("2026-08-20T12:00:00Z", 204.0)], forecast=[], units="kcfs",
    ))
    assert river.fetch_nwps_gauge(MEMPHIS, _spec(MEMPHIS)).empty


# ---------------------------------------------------------------------------
# Trap 2 — a forecast row must never date an observation
# ---------------------------------------------------------------------------
def test_the_forecast_trace_is_cut_to_dates_after_the_newest_observation(
    frozen_today, monkeypatch
):
    """The live trace is issued the previous afternoon and overlaps by a day."""
    _patch_payload(monkeypatch, _nwps_payload(
        observed=[
            ("2026-08-19T12:00:00Z", 11.4),
            ("2026-08-20T12:00:00Z", 11.3),
        ],
        forecast=[
            ("2026-08-20T18:00:00Z", 10.6),   # overlaps the observation
            ("2026-08-21T12:00:00Z", 9.8),
            ("2026-08-22T12:00:00Z", 8.9),
        ],
    ))
    frame = river.fetch_nwps_gauge(MEMPHIS, _spec(MEMPHIS))

    forecast_dates = {d.isoformat() for d in frame.loc[frame["is_forecast"] == 1, "Date"]}
    assert forecast_dates == {"2026-08-21", "2026-08-22"}
    # And the observation for the overlapped day survives untouched.
    observed = frame[frame["is_forecast"] == 0].set_index("Date")["stage"]
    assert observed[date(2026, 8, 20)] == 11.3
    # One row per date, so the (gauge, Date) upsert has nothing to resolve.
    assert not frame["Date"].duplicated().any()


def test_a_stored_observation_survives_a_later_run_that_only_has_a_forecast(patched_db):
    """The upsert direction that matters. Yesterday's forecast for today is
    replaced by today's observation — never the other way round."""
    from pipeline import query, store

    forecast = pd.DataFrame({
        "Date": [pd.Timestamp("2026-08-21")], "stage": [9.8], "unit": ["ft"],
        "is_forecast": [1], "source": ["nwps"], "attribution": ["NOAA"],
    })
    store.save_river_levels(MEMPHIS, forecast)
    store.save_river_levels(MEMPHIS, forecast.assign(stage=10.4, is_forecast=0))

    stored = query.read_river_levels(MEMPHIS)
    assert len(stored) == 1
    assert stored.loc[0, "stage"] == 10.4
    assert stored.loc[0, "is_forecast"] == 0


def test_a_forecast_row_never_dates_the_gauge_in_commodity_freshness(patched_db):
    """Trap 3 in its third guise. `MAX(Date)` over a table holding a 14-day
    forecast trace reports the gauge as fresh into next week."""
    from pipeline import store

    store.init_database()
    store.save_river_levels(MEMPHIS, pd.DataFrame({
        "Date": [pd.Timestamp("2026-08-20"), pd.Timestamp("2026-09-03")],
        "stage": [-6.2, -7.5], "unit": ["ft", "ft"], "is_forecast": [0, 1],
        "source": ["nwps", "nwps"], "attribution": ["NOAA", "NOAA"],
    }))
    store.update_commodity_freshness()

    conn = sqlite3.connect(patched_db)
    try:
        last_date, rows_total = conn.execute(
            "SELECT last_date_in_db, rows_total FROM commodity_freshness "
            "WHERE table_name = 'river_levels' AND commodity = ?",
            (MEMPHIS,),
        ).fetchone()
    finally:
        conn.close()
    assert last_date == "2026-08-20"
    assert rows_total == 1  # the observed row; the forecast is not a reading


# ---------------------------------------------------------------------------
# Trap 3 — a partial day is not the day, but a forecast is not an aggregate
# ---------------------------------------------------------------------------
def test_the_current_day_is_dropped_from_an_hourly_observed_series(
    frozen_today, monkeypatch
):
    _patch_payload(monkeypatch, _nwps_payload(
        observed=[
            ("2026-08-20T06:00:00Z", 11.5),
            ("2026-08-20T18:00:00Z", 11.3),
            ("2026-08-21T06:00:00Z", 11.1),   # today, still accumulating
            ("2026-08-21T17:00:00Z", 11.0),
        ],
        forecast=[],
    ))
    frame = river.fetch_nwps_gauge(MEMPHIS, _spec(MEMPHIS))
    observed = frame[frame["is_forecast"] == 0]

    assert observed["Date"].max() == date(2026, 8, 20)
    # The day that IS complete keeps its *last* reading, not its first.
    assert observed.set_index("Date")["stage"][date(2026, 8, 20)] == 11.3


def test_the_forecast_for_today_is_kept(frozen_today, monkeypatch):
    """A forecast is not an aggregate of anything: its value for today is the
    latest issued value for today, and it is replaced by the observation when
    the day closes."""
    _patch_payload(monkeypatch, _nwps_payload(
        observed=[("2026-08-20T12:00:00Z", 11.3)],
        forecast=[("2026-08-21T12:00:00Z", 10.9), ("2026-08-22T12:00:00Z", 10.1)],
    ))
    frame = river.fetch_nwps_gauge(MEMPHIS, _spec(MEMPHIS))

    forecast_dates = {d.isoformat() for d in frame.loc[frame["is_forecast"] == 1, "Date"]}
    assert "2026-08-21" in forecast_dates


def test_a_once_daily_source_keeps_todays_reading(frozen_today, monkeypatch):
    """INA publishes one point per day at 00:00 local. There is nothing to
    aggregate, so the guard that protects an hourly series must not fire."""
    _patch_payload(monkeypatch, _ina_payload([
        ("2026-08-19T03:00:00.000Z", 2.91),
        ("2026-08-20T03:00:00.000Z", 2.97),
        ("2026-08-21T03:00:00.000Z", 2.99),
    ]))
    frame = river.fetch_ina_gauge(ROSARIO, _spec(ROSARIO))

    assert frame["Date"].max() == date(2026, 8, 21)
    assert frame["stage"].iloc[-1] == 2.99


def test_a_reading_is_filed_under_the_river_day_not_the_utc_day(
    frozen_today, monkeypatch
):
    """22:00 in Memphis is 03:00Z the next morning. Bucketing by UTC date
    would file every evening reading under tomorrow — and on the newest day,
    under a day that has not happened."""
    _patch_payload(monkeypatch, _nwps_payload(
        observed=[
            ("2026-08-19T12:00:00Z", 11.4),
            ("2026-08-20T03:00:00Z", 11.9),   # 22:00 local on the 19th
        ],
        forecast=[],
    ))
    frame = river.fetch_nwps_gauge(MEMPHIS, _spec(MEMPHIS))
    observed = frame[frame["is_forecast"] == 0].set_index("Date")["stage"]

    assert observed.index.max() == date(2026, 8, 19)
    assert observed[date(2026, 8, 19)] == 11.9


# ---------------------------------------------------------------------------
# Trap 4 — metres and feet share a table
# ---------------------------------------------------------------------------
def test_each_river_stamps_its_own_unit_on_every_row(frozen_today, monkeypatch):
    _patch_payload(monkeypatch, _nwps_payload(
        observed=[("2026-08-20T12:00:00Z", 11.3)], forecast=[]))
    assert set(river.fetch_nwps_gauge(MEMPHIS, _spec(MEMPHIS))["unit"]) == {"ft"}

    _patch_payload(monkeypatch, _ina_payload([("2026-08-20T03:00:00.000Z", 2.97)]))
    assert set(river.fetch_ina_gauge(ROSARIO, _spec(ROSARIO))["unit"]) == {"m"}


def test_the_store_refuses_a_row_that_cannot_say_its_unit(patched_db):
    from pipeline import store

    unitless = pd.DataFrame({
        "Date": [pd.Timestamp("2026-08-20")], "stage": [2.97],
        "is_forecast": [0], "source": ["ina"], "attribution": ["INA"],
    })
    with pytest.raises(ValueError, match="must carry its unit"):
        store.save_river_levels(ROSARIO, unitless)


def test_the_two_rivers_are_measured_in_different_units():
    """The registry fact the whole shared shape rests on."""
    assert _spec(MEMPHIS)["unit"] == "ft"
    assert _spec(ROSARIO)["unit"] == "m"
    assert set(config.RIVER_STAGE_BOUNDS) == {"ft", "m"}


# ---------------------------------------------------------------------------
# Failure states — never answered is not the same as nothing to report
# ---------------------------------------------------------------------------
def test_a_gauge_that_never_answered_is_absent_from_the_result(
    frozen_today, monkeypatch
):
    """Absent, not present-and-empty: LAYER_MIN_KEYS is what sees the outage,
    and it counts keys."""
    _patch_payload(monkeypatch, None)
    assert river.fetch_nwps_gauges() == {}
    assert river.fetch_ina_gauges() == {}


def test_the_ina_error_object_served_under_http_200_is_a_failure_not_a_frame(
    frozen_today, monkeypatch
):
    """The a5 API answers an error with a JSON *object* and status 200 —
    probed live: `{"title": "Mensaje de error", ...}`."""
    _patch_payload(monkeypatch, {"title": "Mensaje de error", "mensaje": "Argumento faltante."})
    assert river.fetch_ina_gauge(ROSARIO, _spec(ROSARIO)).empty


# ---------------------------------------------------------------------------
# Cleaning — a river gap is not a weather gap
# ---------------------------------------------------------------------------
def test_a_missing_stage_is_never_forward_filled():
    """clean_weather forward-fills small gaps; this must not. The days a gauge
    goes dark — a frozen sensor in a low-water event — are exactly the days a
    carried-forward level would be wrong in the direction that matters."""
    frame = pd.DataFrame({
        "Date": pd.to_datetime(["2026-08-18", "2026-08-19", "2026-08-20"]),
        "stage": [11.4, None, 11.0],
        "unit": ["ft"] * 3, "is_forecast": [0] * 3,
        "source": ["nwps"] * 3, "attribution": ["NOAA"] * 3,
    })
    cleaned = clean_river_levels(frame)

    assert pd.isna(cleaned.loc[1, "stage"])
    # And the cleaner returns a copy — the original is untouched.
    assert len(frame) == 3


def test_the_cleaner_drops_a_row_that_cannot_be_keyed_or_read():
    frame = pd.DataFrame({
        "Date": ["2026-08-20", "not a date", "2026-08-19"],
        "stage": [11.0, 11.1, 11.2],
        "unit": ["ft", "ft", ""],
        "is_forecast": [0, 0, 0],
    })
    cleaned = clean_river_levels(frame)

    assert list(cleaned["Date"]) == [pd.Timestamp("2026-08-20")]


# ---------------------------------------------------------------------------
# Pipeline wiring
# ---------------------------------------------------------------------------
def test_both_layers_are_in_the_production_inventory():
    keys = {row[0] for row in config.PRODUCTION_LAYERS}

    assert {"river_us", "river_ar"} <= keys


@pytest.mark.parametrize("layer", ["river_us", "river_ar"])
def test_both_layers_are_wired_into_the_dict_layer_table(layer):
    assert layer in {entry.key for entry in main._build_dict_layers()}


@pytest.mark.parametrize("layer", ["river_us", "river_ar"])
def test_neither_layer_can_freeze_and_stay_green(layer):
    """Both are fixed-URL sources: nothing rotates, so a feed that stops being
    refreshed answers 200 forever and only the age budget catches it."""
    assert layer in LAYER_MAX_DATA_AGE_DAYS


def test_the_us_layer_demands_every_gauge():
    """Both come out of one API on one run, and a river has a level every day.
    A floor of 1 would let Memphis — the gauge the basis trades off — go dark
    behind a green St. Louis."""
    assert LAYER_MIN_KEYS["river_us"] == len(config.LAYER_KEY_CATALOGS["river_us"]) == 2


def test_the_single_gauge_layer_treats_empty_as_failure():
    """No floor to derive from, and INA serves this series back to 1884: an
    empty return means the request or the parse broke."""
    entry = next(e for e in main._build_dict_layers() if e.key == "river_ar")
    assert entry.empty_fails is True


def test_forecast_rows_never_date_the_layer():
    """A dead observed feed would otherwise pass its recency budget on a
    14-day forecast trace alone."""
    frame = pd.DataFrame({
        "Date": pd.to_datetime(["2026-08-20", "2026-09-04"]),
        "stage": [11.3, 4.2],
        "unit": ["ft", "ft"],
        "is_forecast": [0, 1],
    })
    assert main._latest_observation_date({MEMPHIS: frame}) == pd.Timestamp("2026-08-20")


def test_river_levels_round_trips_through_git_history():
    """NWPS serves a rolling ~30-day observed window and nothing older, so on
    an ephemeral CI database the low-water episodes the series exists for
    would never accumulate."""
    assert HISTORY_TABLES["river_levels"] == ("gauge", "Date")


# ---------------------------------------------------------------------------
# The site — block 06, not a tenth block
# ---------------------------------------------------------------------------
def _seed_site(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rows):
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "river.db"
    conn = sqlite3.connect(str(db_path))
    for ddl in schema.ALL_SCHEMAS:
        conn.execute(ddl)
    conn.executemany(
        "INSERT INTO river_levels (gauge, Date, stage, unit, is_forecast, source, attribution) "
        "VALUES (?,?,?,?,?,?,?)", rows,
    )
    # One weather row so block 06 has its regions too — the river is a line
    # inside that block, not a replacement for it.
    for region in ("US Midwest (Iowa)", "Argentina Pampas", "Brazil Mato Grosso"):
        conn.execute(
            "INSERT INTO weather (region, Date, temp_max, temp_min, precipitation) "
            "VALUES (?,?,?,?,?)", (region, TODAY.isoformat(), 28.0, 15.0, 4.0),
        )
    conn.commit()
    monkeypatch.setattr(markets_mod, "get_connection", lambda: sqlite3.connect(str(db_path)))
    monkeypatch.setattr(markets_mod, "is_cloud", lambda: False)
    monkeypatch.setattr(config, "DB_PATH", str(db_path))
    return conn


def _row(gauge, offset, stage, unit, is_forecast=0):
    when = (TODAY - timedelta(days=offset)).isoformat()
    return (gauge, when, stage, unit, is_forecast, "test", "Test attribution")


def _weather_block(slug, conn, registry):
    blocks = build_blocks(registry[slug], None, SiteContext(conn=conn, today=TODAY),
                          markets=registry)
    return next(b for b in blocks if b.id == "weather")


@pytest.fixture
def registry():
    return load_markets()


def test_the_river_renders_inside_block_06_and_adds_no_tenth_block(
    tmp_path, monkeypatch, registry
):
    conn = _seed_site(tmp_path, monkeypatch, [
        _row(MEMPHIS, 1, 11.3, "ft"), _row(MEMPHIS, 8, 12.9, "ft"),
        _row(ST_LOUIS, 1, 8.4, "ft"),
    ])
    blocks = build_blocks(registry["cbot"], None, SiteContext(conn=conn, today=TODAY),
                          markets=registry)
    conn.close()

    assert [b.no for b in blocks] == [f"{n:02d}" for n in range(1, 10)]
    rivers = next(b for b in blocks if b.id == "weather").data["rivers"]
    assert [r["gauge"] for r in rivers] == [MEMPHIS, ST_LOUIS]


def test_the_memphis_line_carries_its_level_direction_and_outlook(
    tmp_path, monkeypatch, registry
):
    conn = _seed_site(tmp_path, monkeypatch, [
        _row(MEMPHIS, 8, 12.9, "ft"),
        _row(MEMPHIS, 1, 11.3, "ft"),
        _row(MEMPHIS, -14, 4.2, "ft", is_forecast=1),
        _row(ST_LOUIS, 1, 8.4, "ft"),
    ])
    memphis = _weather_block("cbot", conn, registry).data["rivers"][0]
    conn.close()

    assert memphis["state"] == "ok"
    assert memphis["stage"] == 11.3
    assert memphis["unit"] == "ft"
    assert memphis["change_7d"] == pytest.approx(-1.6)
    assert memphis["forecast_stage"] == 4.2
    assert memphis["forecast_date"] == (TODAY + timedelta(days=14)).isoformat()


def test_the_seven_day_change_is_withheld_rather_than_struck_on_the_oldest_row(
    tmp_path, monkeypatch, registry
):
    """On a fresh database the oldest row is today. A "7-day change" computed
    against it is a zero that reads as a flat river."""
    conn = _seed_site(tmp_path, monkeypatch, [
        _row(MEMPHIS, 1, 11.3, "ft"), _row(MEMPHIS, 2, 11.4, "ft"),
        _row(ST_LOUIS, 1, 8.4, "ft"),
    ])
    memphis = _weather_block("cbot", conn, registry).data["rivers"][0]
    conn.close()

    assert memphis["change_7d"] is None


def test_a_low_water_breach_flags_only_where_a_threshold_is_declared(
    tmp_path, monkeypatch, registry
):
    """St. Louis is carried as the barge-rate reference and no threshold for
    it is sourced. Inventing one to fill the line is what invariant 2
    forbids — so it renders its level and raises no flag, at any stage."""
    conn = _seed_site(tmp_path, monkeypatch, [
        _row(MEMPHIS, 1, -6.2, "ft"),
        _row(ST_LOUIS, 1, -20.0, "ft"),
    ])
    block = _weather_block("cbot", conn, registry)
    conn.close()

    memphis, st_louis = block.data["rivers"]
    assert memphis["low_water_breach"] is True
    assert memphis["low_water"] == -5.0
    assert memphis["low_water_basis"]
    assert st_louis["low_water"] is None
    assert st_louis["low_water_breach"] is False
    assert [r["gauge"] for r in block.data["river_alerts"]] == [MEMPHIS]


def test_a_forecast_into_low_water_is_flagged_separately_from_a_breach_today(
    tmp_path, monkeypatch, registry
):
    conn = _seed_site(tmp_path, monkeypatch, [
        _row(MEMPHIS, 1, 2.0, "ft"),
        _row(MEMPHIS, -10, -6.0, "ft", is_forecast=1),
        _row(ST_LOUIS, 1, 8.4, "ft"),
    ])
    memphis = _weather_block("cbot", conn, registry).data["rivers"][0]
    conn.close()

    assert memphis["low_water_breach"] is False
    assert memphis["low_water_outlook_breach"] is True


def test_the_parana_renders_in_metres_and_claims_no_forecast(
    tmp_path, monkeypatch, registry
):
    """INA's forecast trace for this station answered empty on every probe.
    The asymmetry with the Mississippi leg is a fact about the publisher and
    is rendered as one, never filled in."""
    conn = _seed_site(tmp_path, monkeypatch, [_row(ROSARIO, 1, 2.97, "m")])
    rosario = _weather_block("argentina", conn, registry).data["rivers"][0]
    conn.close()

    assert rosario["unit"] == "m"
    assert rosario["stage"] == 2.97
    assert rosario["forecast_stage"] is None
    assert rosario["low_water"] == 1.64


def test_a_configured_gauge_with_no_rows_says_so_instead_of_vanishing(
    tmp_path, monkeypatch, registry
):
    conn = _seed_site(tmp_path, monkeypatch, [_row(MEMPHIS, 1, 11.3, "ft")])
    block = _weather_block("cbot", conn, registry)
    conn.close()

    st_louis = block.data["rivers"][1]
    assert st_louis["state"] == "empty"
    assert ST_LOUIS in st_louis["reason"]
    # The weather block itself is still ok — one dark gauge is not an outage
    # of the block it renders inside.
    assert block.is_ok


def test_a_market_with_no_river_renders_no_river_line(tmp_path, monkeypatch, registry):
    """Six of eight markets have no river pricing their freight. That is a
    fact about the registry, not an outage, so it gets no line at all — which
    is why this is not a tenth block."""
    conn = _seed_site(tmp_path, monkeypatch, [_row(MEMPHIS, 1, 11.3, "ft")])
    block = _weather_block("brazil", conn, registry)
    conn.close()

    assert registry["brazil"].river_gauges == ()
    assert block.data["rivers"] == []


def test_only_the_two_markets_with_a_priced_river_declare_one(registry):
    declared = {slug: m.river_gauges for slug, m in registry.items() if m.river_gauges}
    assert set(declared) == {"cbot", "argentina"}


def test_a_market_naming_an_unknown_gauge_fails_at_load(monkeypatch):
    """The same load-time gate weather regions get: a typo would otherwise
    render a silent blank where a freight leg should be."""
    broken = {
        slug: (raw | {"river_gauges": ["Danube at Nowhere"]} if slug == "cbot" else raw)
        for slug, raw in config.MARKETS.items()
    }
    monkeypatch.setattr(config, "MARKETS", broken)
    with pytest.raises(ValueError, match="not in RIVER_GAUGES"):
        load_markets()


# ---------------------------------------------------------------------------
# The markup — the block partial is the last place a number can go wrong
# ---------------------------------------------------------------------------
def _render_block_06(block):
    from jinja2 import Environment, FileSystemLoader

    env = Environment(
        loader=FileSystemLoader(str(Path("app/templates"))), autoescape=False
    )
    return env.get_template("blocks/06_weather.html.j2").render(block=block)


def test_the_markup_prints_the_stage_with_its_own_unit_and_its_own_flag(
    tmp_path, monkeypatch, registry
):
    """Feet and metres render off the row, never off a template constant."""
    conn = _seed_site(tmp_path, monkeypatch, [
        _row(MEMPHIS, 8, 12.9, "ft"),
        _row(MEMPHIS, 1, -6.2, "ft"),
        _row(MEMPHIS, -14, -7.5, "ft", is_forecast=1),
        _row(ST_LOUIS, 1, 8.4, "ft"),
    ])
    cbot = _render_block_06(_weather_block("cbot", conn, registry))
    conn.close()

    conn = _seed_site(tmp_path / "ar", monkeypatch, [_row(ROSARIO, 1, 2.97, "m")])
    argentina = _render_block_06(_weather_block("argentina", conn, registry))
    conn.close()

    assert "-6.20 ft" in cbot
    # The 7-day direction, signed and coloured — 12.9 ft a week ago to -6.2.
    assert '<span class="down">-19.10 ft</span>' in cbot
    assert "forecast -7.50 ft" in cbot
    assert "low water" in cbot             # Memphis has a declared threshold
    assert "Test attribution" in cbot      # the row's own credit, not the registry's
    assert "2.97 m" in argentina
    assert " ft" not in argentina          # no unit leaks across the two rivers
    assert "low water" not in argentina    # 2.97 m is well above 1.64 m


def test_the_markup_names_a_dark_gauge_rather_than_dropping_the_line(
    tmp_path, monkeypatch, registry
):
    conn = _seed_site(tmp_path, monkeypatch, [_row(MEMPHIS, 1, 11.3, "ft")])
    html = _render_block_06(_weather_block("cbot", conn, registry))
    conn.close()

    assert ST_LOUIS in html
    assert "no reading" in html
