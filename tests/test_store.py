"""Round-trip tests for pipeline.store.

For each save_* function: empty-input early return, single-row
persistence, and (representatively) INSERT OR REPLACE upsert. Reads are
validated either via raw SQL against the patched temp DB or via the
matching read_* function in pipeline.query.

Failures should be treated as findings against the store function, not
as test bugs.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from pipeline import query, store

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fetchall(db_path: Path, sql: str, params: tuple = ()) -> list[tuple]:
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def _datetime_index(dates: list[str]) -> pd.DatetimeIndex:
    idx = pd.to_datetime(dates)
    idx.name = "Date"
    return idx


def _price_df(dates: list[str], closes: list[float] | None = None) -> pd.DataFrame:
    if closes is None:
        closes = [100.0] * len(dates)
    return pd.DataFrame(
        {
            "Open": closes,
            "High": [c + 1 for c in closes],
            "Low": [c - 1 for c in closes],
            "Close": closes,
            "Volume": [1000.0] * len(dates),
        },
        index=_datetime_index(dates),
    )


def _currency_df(dates: list[str], close: float = 1.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Open": [close] * len(dates),
            "High": [close + 0.01] * len(dates),
            "Low": [close - 0.01] * len(dates),
            "Close": [close] * len(dates),
        },
        index=_datetime_index(dates),
    )


# ---------------------------------------------------------------------------
# Round-trip scenarios
# ---------------------------------------------------------------------------


def _scenario_prices(_: Path) -> None:
    store.save_price_data("Soybeans", _price_df(["2026-01-01", "2026-01-02"], [1200.0, 1210.0]))
    out = query.read_prices("Soybeans")
    assert len(out) == 2
    assert sorted(out["Close"].tolist()) == [1200.0, 1210.0]
    # Filter for an absent commodity returns empty.
    assert query.read_prices("Corn").empty


def _scenario_fred(_: Path) -> None:
    series = pd.Series(
        [0.0525, 0.0530],
        index=pd.DatetimeIndex(["2026-01-01", "2026-01-02"], name="Date"),
    )
    store.save_fred_data("FED_FUNDS", series)
    out = query.read_economic("FED_FUNDS")
    assert len(out) == 2
    assert sorted(out["value"].tolist()) == [0.0525, 0.0530]


def _scenario_usda(_: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "year": "2025",
                "short_desc": "SOYBEANS - PRODUCTION, MEASURED IN BU",
                "Value": "4500000000",
                "unit_desc": "BU",
                "state_name": "US TOTAL",
                "reference_period_desc": "YEAR",
            }
        ]
    )
    store.save_usda_data(df, "PRODUCTION")
    out = query.read_usda("PRODUCTION")
    assert len(out) == 1
    assert out["Value"].iloc[0] == "4500000000"


def _scenario_crop_progress(_: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "week_ending": "2025-09-15",
                "year": "2025",
                "short_desc": "SOYBEANS - CONDITION, MEASURED IN PCT GOOD",
                "Value": "62",
                "unit_desc": "PCT",
                "statisticcat_desc": "CONDITION",
            }
        ]
    )
    store.save_crop_progress("Soybeans", df)
    out = query.read_crop_progress("Soybeans")
    assert len(out) == 1
    assert out["Value"].iloc[0] == "62"


def _scenario_cot(_: Path) -> None:
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2026-01-07", "2026-01-14"]),
            "commercial_long": [100000.0, 105000.0],
            "commercial_short": [80000.0, 82000.0],
            "commercial_net": [20000.0, 23000.0],
            "noncommercial_long": [200000.0, 210000.0],
            "noncommercial_short": [150000.0, 155000.0],
            "noncommercial_net": [50000.0, 55000.0],
            "total_open_interest": [600000.0, 620000.0],
        }
    )
    store.save_cot_data("Soybeans", df)
    out = query.read_cot("Soybeans")
    assert len(out) == 2
    assert out["commercial_net"].sum() == pytest.approx(43000.0)


def _scenario_weather(_: Path) -> None:
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "temp_max": [30.0, 31.5],
            "temp_min": [18.0, 19.0],
            "precipitation": [0.0, 5.5],
        }
    )
    store.save_weather_data("Mato Grosso", df)
    out = query.read_weather("Mato Grosso")
    assert len(out) == 2
    assert out["precipitation"].max() == 5.5


def _scenario_psd(_: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "commodity": "Soybeans",
                "country": "Brazil",
                "year": 2025,
                "attribute": "Production",
                "value": 169000.0,
                "unit": "1000 MT",
            },
            {
                "commodity": "Soybeans",
                "country": "United States",
                "year": 2025,
                "attribute": "Production",
                "value": 122000.0,
                "unit": "1000 MT",
            },
        ]
    )
    store.save_psd_data("Soybeans", df)
    out = query.read_psd("Soybeans")
    assert len(out) == 2
    assert set(out["country"]) == {"Brazil", "United States"}


def _scenario_currencies(_: Path) -> None:
    store.save_currency_data("USDBRL", _currency_df(["2026-01-01"], close=5.10))
    out = query.read_currencies("USDBRL")
    assert len(out) == 1
    assert out["Close"].iloc[0] == pytest.approx(5.10)


def _scenario_worldbank(_: Path) -> None:
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2025-12-01"]),
            "price": [950.5],
            "unit": ["$/mt"],
        }
    )
    store.save_worldbank_data("Palm oil", df)
    out = query.read_worldbank_prices("Palm oil")
    assert len(out) == 1
    assert out["price"].iloc[0] == 950.5


def _scenario_dce(_: Path) -> None:
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2026-01-02"]),
            "Open": [4500.0],
            "High": [4520.0],
            "Low": [4480.0],
            "Close": [4510.0],
            "Volume": [50000.0],
            "Open_Interest": [120000.0],
            "Settle": [4505.0],
        }
    )
    store.save_dce_futures_data("DCE Soybean Meal", df)
    out = query.read_dce_futures("DCE Soybean Meal")
    assert len(out) == 1
    assert out["Settle"].iloc[0] == 4505.0


def _scenario_export_sales(_: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "week_ending": "2026-01-08",
                "country": "China",
                "net_sales": 500000.0,
                "weekly_exports": 400000.0,
                "accumulated_exports": 30000000.0,
                "outstanding_sales": 10000000.0,
            }
        ]
    )
    store.save_export_sales("Soybeans", df)
    out = query.read_export_sales("Soybeans")
    assert len(out) == 1
    assert out["country"].iloc[0] == "China"


def _scenario_forward_curve(_: Path) -> None:
    df = pd.DataFrame(
        [
            {"contract_month": "2026-03", "label": "Mar 26", "ticker": "ZSH26.CBT",
             "close": 1205.0, "observation_date": "2026-02-27"},
            {"contract_month": "2026-05", "label": "May 26", "ticker": "ZSK26.CBT",
             "close": 1215.0, "observation_date": "2026-02-27"},
        ]
    )
    store.save_forward_curve("Soybeans", df)
    out = query.read_forward_curve("Soybeans")
    assert len(out) == 2
    assert sorted(out["close"].tolist()) == [1205.0, 1215.0]
    # The session the curve was read at survives the round trip — it is
    # what the dashboard dates the curve by, not the pipeline's run date.
    assert set(out["observation_date"]) == {"2026-02-27"}


def _scenario_wasde(_: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "commodity_desc": "SOYBEANS",
                "statisticcat_desc": "PRODUCTION",
                "year": "2025",
                "Value": 4500.0,
                "unit_desc": "MIL BU",
                "reference_period_desc": "MARKETING YEAR",
            }
        ]
    )
    store.save_wasde("SOYBEANS/PRODUCTION", df)
    out = query.read_wasde("SOYBEANS")
    assert len(out) == 1
    assert out["value"].iloc[0] == 4500.0


def _scenario_inspections(_: Path) -> None:
    df = pd.DataFrame(
        [{"week_ending": "2026-01-08", "inspections_mt": 1250000.0}]
    )
    store.save_inspections("Soybeans", df)
    out = query.read_inspections("Soybeans")
    assert len(out) == 1
    assert out["inspections_mt"].iloc[0] == 1250000.0


def _scenario_eia(_: Path) -> None:
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2026-01-01"]),
            "value": [1.05],
            "unit": ["mil bbl/day"],
        }
    )
    store.save_eia_data("ETHANOL_PRODUCTION", df)
    out = query.read_eia_data("ETHANOL_PRODUCTION")
    assert len(out) == 1
    assert out["value"].iloc[0] == 1.05


def _scenario_brazil_estimates(_: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "source": "CONAB",
                "commodity": "Soybeans",
                "crop_year": "2025/26",
                "attribute": "Production",
                "value": 172500.0,
                "unit": "1000 MT",
                "report_date": "2026-01-15",
            }
        ]
    )
    store.save_brazil_estimates(df)
    out = query.read_brazil_estimates("Soybeans")
    assert len(out) == 1
    assert out["value"].iloc[0] == 172500.0


def _scenario_india_domestic(_: Path) -> None:
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2026-01-02"]),
            "Open": [45000.0],
            "High": [45500.0],
            "Low": [44800.0],
            "Close": [45200.0],
            "Volume": [10000.0],
            "Unit": ["INR/MT"],
        }
    )
    store.save_india_domestic("Soybeans", df)
    out = query.read_india_domestic("Soybeans")
    assert len(out) == 1
    assert out["Close"].iloc[0] == 45200.0


def _scenario_brazil_spot(_: Path) -> None:
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2026-01-02"]),
            "price_brl_mt": [2500.0],
            "Unit": ["BRL/MT"],
        }
    )
    store.save_brazil_spot("Soybeans", df)
    out = query.read_brazil_spot("Soybeans")
    assert len(out) == 1
    assert out["price_brl"].iloc[0] == 2500.0


def _scenario_briefings(_: Path) -> None:
    signals = [{"severity": "alert", "commodity": "Soybeans", "message": "RSI overbought"}]
    snapshot = {
        "prices": {"Soybeans": {"close": 1234.5, "rsi": 72.0}},
        "crush_spread": {"value_cents": 100.0, "value_usd_mt": 36.7},
        "cot": {},
    }
    store.save_briefing(
        briefing_date="2026-01-15",
        text="=== Mirror Market Daily Briefing — 2026-01-15 ===\nPRICES: ...",
        signals=signals,
        snapshot=snapshot,
    )
    out = query.read_briefing("2026-01-15")
    assert out is not None
    assert out["briefing_date"] == "2026-01-15"
    assert "Mirror Market" in out["text"]
    assert out["signals"] == signals
    assert out["snapshot"]["prices"]["Soybeans"]["rsi"] == 72.0

    # Range query
    df = query.read_briefings(start_date="2026-01-01", end_date="2026-01-31")
    assert len(df) == 1
    assert df.iloc[0]["briefing_date"] == "2026-01-15"


def _scenario_safex(_: Path) -> None:
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2026-01-02"]),
            "Close": [8500.0],
            "Volume": [500.0],
            "Contract": ["MAR26"],
            "Unit": ["ZAR/MT"],
        }
    )
    store.save_safex("White Maize", df)
    out = query.read_safex("White Maize")
    assert len(out) == 1
    assert out["Close"].iloc[0] == 8500.0
    assert out["contract"].iloc[0] == "MAR26"


@pytest.mark.parametrize(
    "scenario",
    [
        _scenario_prices,
        _scenario_fred,
        _scenario_usda,
        _scenario_crop_progress,
        _scenario_cot,
        _scenario_weather,
        _scenario_psd,
        _scenario_currencies,
        _scenario_worldbank,
        _scenario_dce,
        _scenario_export_sales,
        _scenario_forward_curve,
        _scenario_wasde,
        _scenario_inspections,
        _scenario_eia,
        _scenario_brazil_estimates,
        _scenario_india_domestic,
        _scenario_brazil_spot,
        _scenario_safex,
        _scenario_briefings,
    ],
    ids=lambda fn: fn.__name__.removeprefix("_scenario_"),
)
def test_save_roundtrip(patched_db, scenario):
    scenario(patched_db)


def test_save_briefing_requires_date_and_text(patched_db):
    with pytest.raises(ValueError):
        store.save_briefing(briefing_date="", text="x")
    with pytest.raises(ValueError):
        store.save_briefing(briefing_date="2026-01-15", text="")


def test_save_briefing_insert_or_replace_updates_existing(patched_db):
    store.save_briefing("2026-01-15", text="first", signals=[{"x": 1}])
    store.save_briefing("2026-01-15", text="second", signals=[{"x": 2}])

    out = query.read_briefing("2026-01-15")
    assert out is not None
    assert out["text"] == "second"
    assert out["signals"] == [{"x": 2}]


def test_read_briefing_missing_returns_none(patched_db):
    assert query.read_briefing("1999-01-01") is None


# ---------------------------------------------------------------------------
# Empty-input early return
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "save_call",
    [
        pytest.param(lambda: store.save_price_data("X", pd.DataFrame()), id="prices"),
        pytest.param(lambda: store.save_fred_data("X", pd.Series(dtype=float)), id="fred"),
        pytest.param(lambda: store.save_usda_data(pd.DataFrame(), "X"), id="usda"),
        pytest.param(lambda: store.save_crop_progress("X", pd.DataFrame()), id="crop_progress"),
        pytest.param(lambda: store.save_cot_data("X", pd.DataFrame()), id="cot"),
        pytest.param(lambda: store.save_weather_data("X", pd.DataFrame()), id="weather"),
        pytest.param(lambda: store.save_psd_data("X", pd.DataFrame()), id="psd"),
        pytest.param(lambda: store.save_currency_data("X", pd.DataFrame()), id="currencies"),
        pytest.param(lambda: store.save_worldbank_data("X", pd.DataFrame()), id="worldbank"),
        pytest.param(lambda: store.save_dce_futures_data("X", pd.DataFrame()), id="dce"),
        pytest.param(lambda: store.save_export_sales("X", pd.DataFrame()), id="export_sales"),
        pytest.param(lambda: store.save_forward_curve("X", pd.DataFrame()), id="forward_curve"),
        pytest.param(lambda: store.save_wasde("X/Y", pd.DataFrame()), id="wasde"),
        pytest.param(lambda: store.save_inspections("X", pd.DataFrame()), id="inspections"),
        pytest.param(lambda: store.save_eia_data("X", pd.DataFrame()), id="eia"),
        pytest.param(lambda: store.save_brazil_estimates(pd.DataFrame()), id="brazil_estimates"),
        pytest.param(lambda: store.save_india_domestic("X", pd.DataFrame()), id="india_domestic"),
        pytest.param(lambda: store.save_brazil_spot("X", pd.DataFrame()), id="brazil_spot"),
        pytest.param(lambda: store.save_safex("X", pd.DataFrame()), id="safex"),
    ],
)
def test_save_empty_returns_silently(patched_db, save_call):
    # No exception, and no row written anywhere we can observe.
    save_call()
    counts = _fetchall(
        patched_db,
        "SELECT COUNT(*) FROM prices UNION ALL SELECT COUNT(*) FROM economic",
    )
    assert all(c[0] == 0 for c in counts)


# ---------------------------------------------------------------------------
# Upsert (INSERT OR REPLACE) — representative test for save_price_data
# ---------------------------------------------------------------------------


def test_save_price_data_insert_or_replace_updates_existing_row(patched_db):
    # The revision is deliberately inside SAME_PK_DIVERGENCE_QUARANTINE_THRESHOLD
    # (T19 #67): past it the store layer holds the row back instead, which is
    # its own scenario in tests/test_store_quarantine.py. This test is about the
    # upsert, so it revises by an amount a source actually publishes.
    store.save_price_data("Soybeans", _price_df(["2026-01-01"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-01"], [1260.0]))

    out = query.read_prices("Soybeans")
    assert len(out) == 1
    assert out["Close"].iloc[0] == 1260.0


# ---------------------------------------------------------------------------
# save_freshness
# ---------------------------------------------------------------------------


def test_save_freshness_success_stamps_last_success(patched_db):
    store.save_freshness("yfinance", rows_fetched=42, status="success")

    rows = _fetchall(
        patched_db,
        "SELECT layer_name, last_success, last_attempt, rows_fetched, status "
        "FROM data_freshness WHERE layer_name = ?",
        ("yfinance",),
    )
    assert len(rows) == 1
    name, last_success, last_attempt, rows_fetched, status = rows[0]
    assert name == "yfinance"
    assert last_success is not None
    assert last_attempt is not None
    assert last_success == last_attempt  # same UTC stamp
    assert rows_fetched == 42
    assert status == "success"


def test_save_freshness_failed_preserves_prior_last_success(patched_db):
    store.save_freshness("usda", rows_fetched=10, status="success")
    prior = _fetchall(
        patched_db,
        "SELECT last_success FROM data_freshness WHERE layer_name = ?",
        ("usda",),
    )[0][0]

    store.save_freshness("usda", rows_fetched=0, status="failed")
    rows = _fetchall(
        patched_db,
        "SELECT last_success, last_attempt, status FROM data_freshness WHERE layer_name = ?",
        ("usda",),
    )
    last_success, last_attempt, status = rows[0]
    assert last_success == prior  # preserved
    assert last_attempt is not None
    assert status == "failed"


def test_save_freshness_failed_with_no_prior_row_leaves_last_success_null(patched_db):
    store.save_freshness("brand_new_layer", status="failed")

    row = _fetchall(
        patched_db,
        "SELECT last_success, status FROM data_freshness WHERE layer_name = ?",
        ("brand_new_layer",),
    )[0]
    assert row[0] is None
    assert row[1] == "failed"


@pytest.mark.parametrize("status", ["no_publication", "stale", "incomplete"])
def test_non_success_freshness_states_preserve_prior_success(patched_db, status):
    store.save_freshness("layer", rows_fetched=4, status="success")
    prior = _fetchall(
        patched_db,
        "SELECT last_success FROM data_freshness WHERE layer_name = 'layer'",
    )[0][0]

    store.save_freshness("layer", rows_fetched=0, status=status)

    row = _fetchall(
        patched_db,
        "SELECT last_success, status FROM data_freshness WHERE layer_name = 'layer'",
    )[0]
    assert row == (prior, status)


def test_save_freshness_invalid_status_raises(patched_db):
    with pytest.raises(ValueError, match="status must be"):
        store.save_freshness("x", status="bogus")


# ---------------------------------------------------------------------------
# init_database + clear_database + update_commodity_freshness
# ---------------------------------------------------------------------------


def test_init_database_is_idempotent(patched_db):
    # Tables already exist (fixture created them); a second init must not
    # raise and must keep existing data intact.
    store.save_price_data("Soybeans", _price_df(["2026-01-01"], [1200.0]))

    store.init_database()  # second time
    store.init_database()  # third time

    out = query.read_prices("Soybeans")
    assert len(out) == 1


def test_clear_database_drops_all_tables(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-01"], [1200.0]))
    store.clear_database()

    tables = _fetchall(
        patched_db,
        "SELECT name FROM sqlite_master WHERE type='table' AND name='prices'",
    )
    assert tables == []  # prices was dropped


def test_update_commodity_freshness_records_per_commodity_rows(patched_db):
    # Populate two tables that update_commodity_freshness scans.
    store.save_price_data("Soybeans", _price_df(["2026-01-01", "2026-01-02"], [1200.0, 1210.0]))
    store.save_price_data("Corn", _price_df(["2026-01-01"], [500.0]))

    store.update_commodity_freshness()

    rows = _fetchall(
        patched_db,
        "SELECT commodity, table_name, last_date_in_db, rows_total "
        "FROM commodity_freshness WHERE table_name = 'prices' ORDER BY commodity",
    )
    assert len(rows) == 2
    by_commodity = {r[0]: r for r in rows}
    assert by_commodity["Soybeans"][2] == "2026-01-02"
    assert by_commodity["Soybeans"][3] == 2
    assert by_commodity["Corn"][2] == "2026-01-01"
    assert by_commodity["Corn"][3] == 1


# ---------------------------------------------------------------------------
# gulf_bids.price_change (#190)
# ---------------------------------------------------------------------------


def _gulf_bid_row(price_change: str | None = "DN 0.1025-DN 0.1075") -> pd.DataFrame:
    return pd.DataFrame([{
        "report_date": "2026-08-11",
        "commodity": "Soybeans",
        "location": "Gulf Coast Ports",
        "delivery": "Current",
        "sale_type": "Bid",
        "basis_low": 95.0,
        "basis_high": 100.0,
        "futures_month": 8,
        "futures_month_high": 11,
        "basis_change": "UNCH",
        "price_change": price_change,
        "price_low": 12.4250,
        "price_high": 12.6875,
        "average": 12.5563,
        "year_ago": 10.9775,
        "freight": "CIF-B",
    }])


def test_save_gulf_bids_persists_ranged_price_change(patched_db):
    store.save_gulf_bids(_gulf_bid_row())

    rows = _fetchall(patched_db, "SELECT basis_change, price_change FROM gulf_bids")
    assert rows == [("UNCH", "DN 0.1025-DN 0.1075")]


def test_save_gulf_bids_accepts_blank_change(patched_db):
    store.save_gulf_bids(_gulf_bid_row(price_change=None))

    rows = _fetchall(patched_db, "SELECT price_change FROM gulf_bids")
    assert rows == [(None,)]


def test_init_database_adds_price_change_to_legacy_gulf_bids(patched_db):
    """A DB predating #190 gains the column instead of failing every insert."""
    conn = sqlite3.connect(str(patched_db))
    try:
        conn.execute("DROP TABLE gulf_bids")
        conn.execute(
            """CREATE TABLE gulf_bids (
                   report_date   TEXT NOT NULL,
                   commodity     TEXT NOT NULL,
                   location      TEXT NOT NULL,
                   delivery      TEXT NOT NULL,
                   sale_type     TEXT,
                   basis_low     REAL,
                   basis_high    REAL,
                   futures_month INTEGER,
                   basis_change  TEXT,
                   price_low     REAL,
                   price_high    REAL,
                   average       REAL,
                   year_ago      REAL,
                   freight       TEXT,
                   PRIMARY KEY (report_date, commodity, location, delivery)
               )"""
        )
        conn.commit()
    finally:
        conn.close()

    store.init_database()
    store.save_gulf_bids(_gulf_bid_row())

    assert _fetchall(patched_db, "SELECT price_change FROM gulf_bids") == [
        ("DN 0.1025-DN 0.1075",)
    ]


# ---------------------------------------------------------------------------
# gulf_bids.futures_month_high (#196)
# ---------------------------------------------------------------------------


def test_save_gulf_bids_persists_split_contract_months(patched_db):
    store.save_gulf_bids(_gulf_bid_row())

    assert _fetchall(
        patched_db, "SELECT futures_month, futures_month_high FROM gulf_bids"
    ) == [(8, 11)]


def test_save_gulf_bids_defaults_high_month_to_low(patched_db):
    """A frame predating the column quotes both legs on one contract."""
    df = _gulf_bid_row().drop(columns=["futures_month_high"])
    store.save_gulf_bids(df)

    assert _fetchall(
        patched_db, "SELECT futures_month, futures_month_high FROM gulf_bids"
    ) == [(8, 8)]


def test_init_database_adds_futures_month_high_to_legacy_gulf_bids(patched_db):
    """A DB predating #196 gains the column instead of failing every insert."""
    conn = sqlite3.connect(str(patched_db))
    try:
        conn.execute("DROP TABLE gulf_bids")
        conn.execute(
            """CREATE TABLE gulf_bids (
                   report_date   TEXT NOT NULL,
                   commodity     TEXT NOT NULL,
                   location      TEXT NOT NULL,
                   delivery      TEXT NOT NULL,
                   sale_type     TEXT,
                   basis_low     REAL,
                   basis_high    REAL,
                   futures_month INTEGER,
                   basis_change  TEXT,
                   price_change  TEXT,
                   price_low     REAL,
                   price_high    REAL,
                   average       REAL,
                   year_ago      REAL,
                   freight       TEXT,
                   PRIMARY KEY (report_date, commodity, location, delivery)
               )"""
        )
        conn.commit()
    finally:
        conn.close()

    store.init_database()
    store.save_gulf_bids(_gulf_bid_row())

    assert _fetchall(patched_db, "SELECT futures_month_high FROM gulf_bids") == [(11,)]
