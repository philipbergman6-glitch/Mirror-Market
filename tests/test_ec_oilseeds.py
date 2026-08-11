"""Layer 22 — EC Oilseeds Observatory weekly EU rapeseed (issue #163).

This layer exists because Euronext MATIF settlements are licensed and this
project publishes (#148). The Commission's weekly physical FOB assessment is
the licence-clean substitute, and these tests pin the three ways it can lie
to us quietly.

1.  **The stale GUID.** The xlsx sits behind an opaque CIRCABC link. A link
    that has rotated away still answers HTTP 200 with a frozen workbook, so
    "the fetch succeeded" proves nothing. Two defences are pinned: the URL is
    re-resolved from the landing page by *link text* each run, and a workbook
    older than the recency budget is discarded rather than stored.

2.  **`n.q.` is a published value, not a gap in transmission.** The EUR block
    carries the string "n.q." for weeks the Commission did not convert. Parsed
    naively that is either a crash or — worse — a coerced number.

3.  **EUR is derived from USD, and is currently inconsistent.** Verified over
    all 398 rows on 2026-08-11: EUR = USD / the row's ECB rate for 391 of the
    393 rows carrying one, but the two newest rows convert at a rate two weeks
    stale (1.3% off). So USD is authoritative and EUR is stored verbatim —
    never recomputed, never back-filled.

No network: the workbook bytes are built in-memory.
"""

from __future__ import annotations

import io

import pandas as pd
import pytest

import fetchers.ec_oilseeds as ec
from config import (
    EC_OILSEEDS_CADENCE,
    EC_OILSEEDS_QUOTE_KIND,
    EC_OILSEEDS_SERIES,
    LAYER_MAX_DATA_AGE_DAYS,
)
from pipeline.clean import clean_ec_oilseeds

SERIES_COL = "Rapeseed - EU Moselle"
SERIES_LABEL = EC_OILSEEDS_SERIES[SERIES_COL]


def _workbook(rows: list[tuple], *, eur_block: bool = True) -> bytes:
    """Build a workbook shaped like the real one.

    Layout mirrors the published file: two title rows, a header row, then the
    USD block, a spacer, the ECB rate, a spacer, and the EUR block repeating
    the same series names (which is how pandas ends up appending ".1").
    """
    header = [
        "Wednesday", SERIES_COL, "Rapeseed - Canada",
        "Unnamed: 3", "Exchange rate: Spot, ECB reference - U.S. Dollar/Euro",
        "Unnamed: 5",
    ]
    if eur_block:
        header += [SERIES_COL, "Rapeseed - Canada"]

    data = []
    for date, usd, eur in rows:
        row = [date, usd, 579.41, None, 1.1554, None]
        if eur_block:
            row += [eur, 507.9]
        data.append(row)

    frame = pd.DataFrame(
        [[None] * len(header), [None] * len(header), header] + data
    )
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        frame.to_excel(writer, sheet_name="Data", header=False, index=False)
    return buf.getvalue()


def _fresh_rows(n: int = 4) -> list[tuple]:
    """n weekly Wednesday rows ending today — inside any recency budget."""
    end = pd.Timestamp.today().normalize()
    return [
        (end - pd.Timedelta(weeks=i), 605.52 - i, 530.78 - i) for i in range(n)
    ]


@pytest.fixture
def no_network(monkeypatch):
    """Fail loudly if a test reaches for the network."""
    def _boom(*a, **k):
        raise AssertionError("test attempted a real HTTP request")
    monkeypatch.setattr(ec.requests, "get", _boom)


# ---------------------------------------------------------------------------
# Link resolution — the stale-GUID trap
# ---------------------------------------------------------------------------


def test_resolver_matches_by_link_text_not_filename(monkeypatch):
    """The href is an opaque GUID and the published filename is misspelled
    ("oliseeds"). Anchoring on the link text survives both a GUID rotation
    and an upstream spelling fix."""
    rotated = "https://circabc.europa.eu/sd/a/NEW-GUID-9999/world-prices.xlsx"
    html = (
        '<p><a href="https://circabc.europa.eu/sd/a/AAA/production.xlsx">'
        "EU oilseeds production</a></p>"
        f'<p><a href="{rotated}">World oilseed prices</a></p>'
    )

    class _Resp:
        text = html
        def raise_for_status(self): pass

    monkeypatch.setattr(ec.requests, "get", lambda *a, **k: _Resp())
    assert ec._resolve_world_prices_url() == rotated


def test_resolver_falls_back_to_pin_when_landing_page_is_down(monkeypatch):
    def _boom(*a, **k):
        raise ec.requests.RequestException("dns")
    monkeypatch.setattr(ec.requests, "get", _boom)
    assert ec._resolve_world_prices_url() == ec.EC_OILSEEDS_WORLD_PRICES_URL


def test_resolver_falls_back_when_the_link_is_gone(monkeypatch):
    """A restructured landing page must not silently yield no URL."""
    class _Resp:
        text = "<p>nothing here</p>"
        def raise_for_status(self): pass

    monkeypatch.setattr(ec.requests, "get", lambda *a, **k: _Resp())
    assert ec._resolve_world_prices_url() == ec.EC_OILSEEDS_WORLD_PRICES_URL


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def test_parses_usd_and_eur(no_network):
    out = ec._parse_world_prices(_workbook(_fresh_rows()))
    df = out[SERIES_LABEL]
    assert list(df.columns) == ["Date", "price_usd", "price_eur"]
    assert len(df) == 4
    assert df["Date"].is_monotonic_increasing
    assert df["price_usd"].iloc[-1] == pytest.approx(605.52)


@pytest.mark.parametrize("sentinel", ["n.q.", "-", ""])
def test_not_quoted_sentinels_become_null_not_numbers(no_network, sentinel):
    """`n.q.` is the Commission saying "no assessment this week". It must land
    as NULL — never coerced, never dropped along with the USD row that is
    perfectly good."""
    rows = _fresh_rows(3)
    rows[1] = (rows[1][0], 616.87, sentinel)
    df = ec._parse_world_prices(_workbook(rows))[SERIES_LABEL]

    assert len(df) == 3, "a missing EUR conversion must not drop the USD row"
    assert df["price_eur"].isna().sum() == 1
    assert df["price_usd"].notna().all()


def test_row_with_no_usd_price_is_dropped(no_network):
    rows = _fresh_rows(3)
    rows[0] = (rows[0][0], "n.q.", "n.q.")
    df = ec._parse_world_prices(_workbook(rows))[SERIES_LABEL]
    assert len(df) == 2


def test_missing_eur_block_still_yields_usd(no_network):
    """USD is the authoritative column; losing the derived block is a
    degradation, not an outage."""
    df = ec._parse_world_prices(
        _workbook(_fresh_rows(3), eur_block=False)
    )[SERIES_LABEL]
    assert len(df) == 3
    assert df["price_eur"].isna().all()


def test_renamed_series_column_yields_nothing(no_network, monkeypatch):
    """A silently renamed column would amputate the Europe page's only leg,
    so it must produce an empty result (graded as a failure) rather than a
    partial one."""
    monkeypatch.setitem(EC_OILSEEDS_SERIES, "Rapeseed - Somewhere Else", "X")
    monkeypatch.delitem(EC_OILSEEDS_SERIES, SERIES_COL)
    assert ec._parse_world_prices(_workbook(_fresh_rows())) == {}


def test_unreadable_workbook_returns_empty(no_network):
    assert ec._parse_world_prices(b"not a zip file at all") == {}


# ---------------------------------------------------------------------------
# Stale-file guard
# ---------------------------------------------------------------------------


def test_stale_workbook_is_discarded_not_stored(monkeypatch, caplog):
    """The F3a lesson from the Pink Sheet: detecting the trap and walking into
    it anyway is worse than not detecting it. The payload is dropped, which
    empty_fails=True then grades as a layer failure."""
    budget = LAYER_MAX_DATA_AGE_DAYS["ec_oilseeds"]
    old = pd.Timestamp.today().normalize() - pd.Timedelta(days=budget + 7)
    rows = [(old - pd.Timedelta(weeks=i), 605.52, 530.78) for i in range(3)]

    monkeypatch.setattr(ec, "_download_world_prices", lambda: _workbook(rows))
    with caplog.at_level("ERROR"):
        assert ec.fetch_ec_oilseed_prices() == {}
    assert "stale GUID" in caplog.text


def test_fresh_workbook_passes_the_guard(monkeypatch):
    monkeypatch.setattr(
        ec, "_download_world_prices", lambda: _workbook(_fresh_rows())
    )
    assert SERIES_LABEL in ec.fetch_ec_oilseed_prices()


def test_failed_download_returns_empty(monkeypatch):
    monkeypatch.setattr(ec, "_download_world_prices", lambda: b"")
    assert ec.fetch_ec_oilseed_prices() == {}


# ---------------------------------------------------------------------------
# Cleaning
# ---------------------------------------------------------------------------


def test_clean_preserves_eur_nulls():
    """Filling an `n.q.` — by carrying the last value forward or by dividing
    USD by a rate ourselves — would publish our arithmetic as the
    Commission's."""
    df = pd.DataFrame({
        "Date": pd.to_datetime(["2026-07-15", "2026-07-22", "2026-07-29"]),
        "price_usd": [606.06, 616.87, 604.94],
        "price_eur": [None, None, 530.37],
    })
    out = clean_ec_oilseeds(df)
    assert len(out) == 3
    assert out["price_eur"].isna().sum() == 2


def test_clean_sorts_dedupes_and_drops_unpriced_rows():
    df = pd.DataFrame({
        "Date": pd.to_datetime(
            ["2026-07-29", "2026-07-15", "2026-07-29", "2026-07-22"]
        ),
        "price_usd": [604.94, 606.06, 604.94, None],
        "price_eur": [530.37, None, 530.37, None],
    })
    out = clean_ec_oilseeds(df)
    assert list(out["Date"].dt.strftime("%Y-%m-%d")) == ["2026-07-15", "2026-07-29"]


def test_clean_does_not_mutate_the_original():
    df = pd.DataFrame({
        "Date": pd.to_datetime(["2026-07-29", "2026-07-15"]),
        "price_usd": [604.94, 606.06],
        "price_eur": [530.37, None],
    })
    before = df.copy()
    clean_ec_oilseeds(df)
    pd.testing.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# Storage — the label must travel with the rows
# ---------------------------------------------------------------------------


def test_saved_rows_carry_cadence_and_quote_kind(tmp_path, monkeypatch):
    """Map #142 risk: board / physical / administered / assessment quotes all
    share a USD/MT axis and are trivially collapsed into one "price" line. A
    consumer that forgets to look the label up cannot lose it if it is on the
    row."""
    import sqlite3

    from pipeline import connection, store

    db = tmp_path / "t.db"
    monkeypatch.setattr(connection, "DB_PATH", db)
    monkeypatch.setattr(store, "DB_PATH", db, raising=False)
    monkeypatch.setattr(connection, "get_connection", lambda: sqlite3.connect(db))
    store.init_database()

    store.save_ec_oilseed_prices(SERIES_LABEL, pd.DataFrame({
        "Date": pd.to_datetime(["2026-07-29", "2026-08-05"]),
        "price_usd": [604.94, 605.52],
        "price_eur": [530.37, 530.79],
    }))

    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT series, Date, price_usd, price_eur, cadence, quote_kind "
            "FROM ec_oilseed_prices ORDER BY Date"
        ).fetchall()

    assert len(rows) == 2
    assert rows[0][0] == SERIES_LABEL
    assert rows[-1][2] == pytest.approx(605.52)
    assert {r[4] for r in rows} == {EC_OILSEEDS_CADENCE}
    assert {r[5] for r in rows} == {EC_OILSEEDS_QUOTE_KIND}


def test_null_eur_survives_the_round_trip(tmp_path, monkeypatch):
    import sqlite3

    from pipeline import connection, store

    db = tmp_path / "t.db"
    monkeypatch.setattr(connection, "DB_PATH", db)
    monkeypatch.setattr(connection, "get_connection", lambda: sqlite3.connect(db))
    store.init_database()

    store.save_ec_oilseed_prices(SERIES_LABEL, pd.DataFrame({
        "Date": pd.to_datetime(["2026-07-22"]),
        "price_usd": [616.87],
        "price_eur": [None],
    }))

    with sqlite3.connect(db) as conn:
        (eur,) = conn.execute(
            "SELECT price_eur FROM ec_oilseed_prices"
        ).fetchone()
    assert eur is None


# ---------------------------------------------------------------------------
# Pipeline wiring
# ---------------------------------------------------------------------------


def test_layer_is_registered_and_empty_fails():
    """A single-series layer has no LAYER_MIN_KEYS floor to derive grading
    from, so empty_fails must be set explicitly — otherwise an empty return
    would stamp a fresh last_success against a table that got no rows."""
    import main

    layers = {layer.key: layer for layer in main._build_dict_layers()}
    assert "ec_oilseeds" in layers
    assert layers["ec_oilseeds"].empty_fails is True
    assert layers["ec_oilseeds"].run_if is None


def test_recency_budget_is_configured():
    """Not listed in LAYER_MAX_DATA_AGE_DAYS = not checked. A weekly source
    behind a rotating link is precisely the shape that must be checked."""
    assert LAYER_MAX_DATA_AGE_DAYS["ec_oilseeds"] >= 14
