"""Layer 29 — EPA RFS (#353): RIN generation, RIN prices, RVOs.

No network: every test feeds the parsers payloads shaped like the ones probed
live on 2026-10-05, or stubs the two fetch halves.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

import config
import main
from fetchers import epa_rfs
from fetchers.epa_rfs import EpaRfsError

# ---------------------------------------------------------------------------
# rin_generation — link resolution (invariant 10)
# ---------------------------------------------------------------------------
_LANDING = """
<a href="https://www.epa.gov/sites/default/files/2015-12/rindata_sept2015.csv">Sep</a>
<a href="https://www.epa.gov/sites/default/files/2016-08/rindata_june2016.csv">Jun</a>
<a href="https://www.epa.gov/system/files/other-files/2026-08/rindata_jul2026.csv">Jul</a>
<a href="/system/files/other-files/2026-09/rindata_aug2026.csv">Aug</a>
<a href="https://www.epa.gov/system/files/other-files/2026-09/fuelproduction_aug2026.csv">x</a>
"""


def test_the_newest_data_month_wins_and_a_relative_link_is_made_absolute():
    url, month = epa_rfs._resolve_generation_url(_LANDING)
    assert url == "https://www.epa.gov/system/files/other-files/2026-09/rindata_aug2026.csv"
    assert month == pd.Timestamp("2026-08-01")


def test_irregular_month_tokens_are_read_by_their_first_three_letters():
    _, month = epa_rfs._resolve_generation_url(_LANDING.split("2026-08")[0])
    assert month == pd.Timestamp("2016-06-01")


def test_a_landing_page_without_a_file_is_a_changed_page_not_a_quiet_month():
    with pytest.raises(EpaRfsError, match="no rindata"):
        epa_rfs._resolve_generation_url("<html>nothing here</html>")


# ---------------------------------------------------------------------------
# rin_generation — parse
# ---------------------------------------------------------------------------
_CSV = (
    "FUEL_CODE,RIN_YEAR,Production Month,RIN_QUANTITY,BATCH_VOLUME\n"
    "4,2026,7,795645395,490000000\n"
    "6,2026,7,1300000000,1300000000\n"
    "4,2026,8,693031207,424843525\n"
    "6,2026,8,1289993450,1289993450\n"
)


def test_generation_splits_by_d_code_and_flags_only_the_newest_month_preliminary():
    out = epa_rfs._parse_generation(_CSV.encode(), pd.Timestamp("2026-08-01"))
    assert set(out) == {"D4", "D6"}
    d4 = out["D4"]
    assert list(d4["Date"]) == [pd.Timestamp("2026-07-01"), pd.Timestamp("2026-08-01")]
    assert list(d4["preliminary"]) == [0, 1]
    assert d4["rins"].iloc[-1] == 693031207


def test_a_file_whose_rows_end_on_another_month_is_rejected():
    # The filename is the only statement of which month is preliminary.
    with pytest.raises(EpaRfsError, match="names 2026-09"):
        epa_rfs._parse_generation(_CSV.encode(), pd.Timestamp("2026-09-01"))


def test_a_changed_header_is_rejected():
    bad = _CSV.replace("RIN_QUANTITY", "RINS")
    with pytest.raises(EpaRfsError, match="header changed"):
        epa_rfs._parse_generation(bad.encode(), pd.Timestamp("2026-08-01"))


def test_an_unknown_d_code_is_rejected():
    bad = _CSV + "9,2026,8,1,1\n"
    with pytest.raises(EpaRfsError, match="unknown D-code D9"):
        epa_rfs._parse_generation(bad.encode(), pd.Timestamp("2026-08-01"))


# ---------------------------------------------------------------------------
# rin_prices — EPA's own measure, stored in USD/RIN
# ---------------------------------------------------------------------------
def _serial(day: str) -> int:
    return (pd.Timestamp(day) - pd.Timestamp("1899-12-30")).days


def _cell(text=None, num=None) -> dict:
    return {"qText": text, "qNum": num if num is not None else "NaN"}


def _price_rows(*rows):
    return [
        [_cell(day, _serial(day)), _cell(code), _cell(num=price), _cell(num=volume)]
        for day, code, price, volume in rows
    ]


def test_price_cells_become_dated_usd_per_rin_rows_per_d_code():
    out = epa_rfs._parse_price_rows(_price_rows(
        ("2026-08-17", "4", 2.104222, 270754210),
        ("2026-08-24", "4", 1.976853, 292467731),
        ("2026-08-24", "6", 1.767660, 335171799),
    ))
    assert set(out) == {"D4", "D6"}
    assert list(out["D4"]["Date"]) == [pd.Timestamp("2026-08-17"), pd.Timestamp("2026-08-24")]
    assert out["D6"]["price_usd_per_rin"].iloc[0] == pytest.approx(1.76766)


def test_a_week_without_a_qualifying_trade_has_no_row_not_a_zero():
    rows = _price_rows(("2026-08-24", "4", 1.97, 1e8))
    rows.append([_cell("8/24/2026", _serial("2026-08-24")), _cell("5"), _cell("-"), _cell(num=0)])
    out = epa_rfs._parse_price_rows(rows)
    assert set(out) == {"D4"}


def test_a_price_outside_epas_own_filter_band_fails_the_key():
    # 1,970 "USD/RIN" is a cents-for-dollars or field change, never a trade.
    with pytest.raises(EpaRfsError, match="outside"):
        epa_rfs._parse_price_rows(_price_rows(("2026-08-24", "4", 1970.0, 1e8)))


# ---------------------------------------------------------------------------
# rvo — final rule only, cross-checked against EPA
# ---------------------------------------------------------------------------
def _epa_rvo(override: dict | None = None) -> pd.DataFrame:
    rows = []
    for year, cats in config.EPA_RFS_RVO_REFERENCE.items():
        values = {
            col: f"{cats[cat][2]:.0f}" for col, cat in config.EPA_RFS_QLIK_RVO_COLUMNS.items()
        }
        values.update((override or {}).get(year, {}))
        rows.append({"RVO_T2_parameter": "Projected Volume Obligation",
                     "RVO_T2_year": str(year), **values})
        rows.append({"RVO_T2_parameter": "Total RVO", "RVO_T2_year": str(year),
                     **{c: "-" for c in config.EPA_RFS_QLIK_RVO_COLUMNS}})
    return pd.DataFrame(rows)


def test_reference_rvos_carry_their_final_rule_citation_in_rins():
    out = epa_rfs._build_rvo(_epa_rvo())
    assert len(out) == 8
    assert set(out["rule_status"]) == {"final"}
    assert set(out["rule_citation"]) == {"91 FR 16388"}
    assert set(out["unit"]) == {"RINs"}
    bbd = out[(out["compliance_year"] == 2026) & (out["category"] == "Biomass-based diesel")].iloc[0]
    assert bbd["total_rins"] == pytest.approx(9.07e9)
    assert bbd["base_rins"] + bbd["sre_reallocation_rins"] == pytest.approx(bbd["total_rins"])


def test_reference_totals_are_base_plus_reallocation():
    for year, cats in config.EPA_RFS_RVO_REFERENCE.items():
        for category, (base, realloc, total) in cats.items():
            assert base + realloc == pytest.approx(total), (year, category)


def test_a_moved_epa_obligation_fails_the_key_rather_than_showing_a_superseded_one():
    moved = _epa_rvo({2026: {"RVO_T2_BD": "9500000000"}})
    with pytest.raises(EpaRfsError, match="2026 Biomass-based diesel"):
        epa_rfs._build_rvo(moved)


def test_epa_missing_a_reference_year_fails_the_key():
    epa = _epa_rvo()
    with pytest.raises(EpaRfsError, match="2027"):
        epa_rfs._build_rvo(epa[epa["RVO_T2_year"] != "2027"])


def test_rounding_inside_half_a_hundredth_of_a_billion_is_not_a_new_rule():
    near = _epa_rvo({2026: {"RVO_T2_BD": "9072000000"}})
    assert len(epa_rfs._build_rvo(near)) == 8


# ---------------------------------------------------------------------------
# Layer entry point — per-key budgets and attribution
# ---------------------------------------------------------------------------
def _frames(gen_last: str, price_last: str) -> tuple[pd.DataFrame, dict]:
    gen = pd.DataFrame({
        "Date": pd.to_datetime([gen_last]), "rins": [1.0], "batch_volume_gal": [1.0],
        "preliminary": [1], "d_code": ["D4"],
    })
    prices = pd.DataFrame({
        "Date": pd.to_datetime([price_last]), "price_usd_per_rin": [2.0],
        "rins_in_average": [1.0], "d_code": ["D4"],
    })
    return gen, {"rin_prices": prices, "rvo": epa_rfs._build_rvo(_epa_rvo())}


def test_a_stale_price_feed_cannot_hide_behind_a_fresh_generation_file(monkeypatch):
    today = pd.Timestamp.today().normalize()
    gen, qlik = _frames(
        (today - pd.Timedelta(days=40)).strftime("%Y-%m-%d"),
        (today - pd.Timedelta(days=200)).strftime("%Y-%m-%d"),
    )
    monkeypatch.setattr(epa_rfs, "fetch_rin_generation", lambda: gen)
    monkeypatch.setattr(epa_rfs, "fetch_qlik_keys", lambda: dict(qlik))
    out = epa_rfs.fetch_epa_rfs()
    assert set(out) == {"rin_generation", "rvo"}
    assert len(out) < config.LAYER_MIN_KEYS["epa_rfs"]  # → incomplete, not green


def test_every_key_is_stamped_with_epa_attribution(monkeypatch):
    today = pd.Timestamp.today().normalize()
    gen, qlik = _frames(
        (today - pd.Timedelta(days=40)).strftime("%Y-%m-%d"),
        (today - pd.Timedelta(days=30)).strftime("%Y-%m-%d"),
    )
    monkeypatch.setattr(epa_rfs, "fetch_rin_generation", lambda: gen)
    monkeypatch.setattr(epa_rfs, "fetch_qlik_keys", lambda: dict(qlik))
    out = epa_rfs.fetch_epa_rfs()
    assert set(out) == {"rin_generation", "rin_prices", "rvo"}
    for frame in out.values():
        assert set(frame["attribution"]) == {config.EPA_RFS_ATTRIBUTION}


def test_budget_boundary_is_inclusive():
    frame = pd.DataFrame({"Date": [pd.Timestamp("2026-08-24")]})
    budget = config.EPA_RFS_KEY_MAX_AGE_DAYS["rin_prices"]
    edge = (pd.Timestamp("2026-08-24") + pd.Timedelta(days=budget)).date()
    assert epa_rfs._within_budget("rin_prices", frame, today=edge)
    assert not epa_rfs._within_budget("rin_prices", frame, today=date.fromordinal(edge.toordinal() + 1))


# ---------------------------------------------------------------------------
# Wiring
# ---------------------------------------------------------------------------
def test_layer_is_in_the_production_inventory_and_the_dict_layer_table():
    assert "epa_rfs" in {row[0] for row in config.PRODUCTION_LAYERS}
    assert "epa_rfs" in {layer.key for layer in main._build_dict_layers()}


def test_layer_cannot_freeze_and_stay_green():
    assert config.LAYER_MAX_DATA_AGE_DAYS["epa_rfs"] == max(config.EPA_RFS_KEY_MAX_AGE_DAYS.values())
    assert config.LAYER_MIN_KEYS["epa_rfs"] == len(config.EPA_RFS_KEYS)


def test_no_epa_table_round_trips_through_git_history():
    # Every key re-serves its whole history (or comes from config), so none
    # needs a data/history/ CSV — and none may appear in a PR.
    from pipeline.history import HISTORY_TABLES

    assert not {"rin_generation", "rin_prices", "rfs_rvo"} & set(HISTORY_TABLES)


# ---------------------------------------------------------------------------
# Store → read, analyst, section, markup
# ---------------------------------------------------------------------------
def _seed(gen_months: dict[str, float], prices: list[tuple[str, str, float]]):
    from pipeline.store import save_epa_rfs

    gen = pd.DataFrame({
        "d_code": "D4",
        "Date": pd.to_datetime(list(gen_months)),
        "rins": list(gen_months.values()),
        "batch_volume_gal": list(gen_months.values()),
        "preliminary": [0] * (len(gen_months) - 1) + [1],
        "attribution": config.EPA_RFS_ATTRIBUTION,
    })
    save_epa_rfs("rin_generation", gen)
    save_epa_rfs("rin_prices", pd.DataFrame({
        "d_code": [p[1] for p in prices],
        "Date": pd.to_datetime([p[0] for p in prices]),
        "price_usd_per_rin": [p[2] for p in prices],
        "rins_in_average": 1e8,
        "attribution": config.EPA_RFS_ATTRIBUTION,
    }))
    save_epa_rfs("rvo", epa_rfs._build_rvo(_epa_rvo()).assign(attribution=config.EPA_RFS_ATTRIBUTION))


_PRICES = [
    ("2026-07-27", "D4", 2.04), ("2026-07-27", "D6", 2.27),
    ("2026-08-17", "D4", 2.10), ("2026-08-17", "D6", 2.06),
    ("2026-08-24", "D4", 1.98), ("2026-08-24", "D6", 1.77),
    ("2026-08-31", "D4", 1.90),  # D6 did not print: not the spread's week
]


def test_rin_prices_are_stored_in_usd_per_rin(patched_db):
    import sqlite3

    _seed({"2026-08-01": 1.0}, _PRICES)
    units = sqlite3.connect(patched_db).execute("SELECT DISTINCT unit FROM rin_prices").fetchall()
    assert units == [("USD/RIN",)]


def test_d4_pace_against_the_final_bbd_obligation(patched_db):
    from analysis.rfs import rfs_policy_analysis

    _seed({"2025-08-01": 500e6, "2026-01-01": 2e9, "2026-02-01": 1.0235e9}, _PRICES)
    g = rfs_policy_analysis()["generation"]
    assert g["year"] == 2026 and g["months"] == 2
    assert g["ytd_rins"] == pytest.approx(3.0235e9)
    assert g["pace_rins"] == pytest.approx(9.07e9 * 2 / 12)
    assert g["pace_pct"] == pytest.approx(3.0235e9 / (9.07e9 * 2 / 12) * 100)
    assert g["latest_preliminary"] is True
    assert "91 FR 16388" in g["citation"]


def test_a_year_without_a_final_rin_denominated_obligation_withholds_the_pace(patched_db):
    from analysis.rfs import rfs_policy_analysis

    # 2025's BBD requirement was gallons — no reference row, so no pace.
    _seed({"2025-07-01": 1e9, "2025-08-01": 1e9}, _PRICES)
    g = rfs_policy_analysis()["generation"]
    assert g["pace_pct"] is None and g["pace_rins"] is None
    assert "no final 2025" in g["pace_reason"]


def test_the_spread_is_struck_on_the_newest_week_both_legs_printed(patched_db):
    from analysis.rfs import rfs_policy_analysis

    _seed({"2026-08-01": 1.0}, _PRICES)
    p = rfs_policy_analysis()["prices"]
    assert p["week"] == "2026-08-24"
    assert p["spread"] == pytest.approx(1.98 - 1.77)
    assert p["d4_chg"] == pytest.approx(1.98 - 2.04)  # vs 2026-07-27, four weeks back


def test_an_empty_database_is_an_empty_section_with_a_reason(patched_db):
    from analysis.rfs import rfs_policy_analysis
    from app.sections import rfs_section

    assert rfs_policy_analysis() is None
    env = rfs_section(None)
    assert env["state"] == "empty" and env["reason"]


def test_markup_labels_rin_prices_per_rin_and_never_per_tonne(patched_db):
    from jinja2 import Environment, FileSystemLoader

    from analysis.rfs import rfs_policy_analysis
    from app.sections import rfs_section

    _seed({"2026-07-01": 1e9, "2026-08-01": 1e9}, _PRICES)
    env = rfs_section(rfs_policy_analysis())
    html = Environment(
        loader=FileSystemLoader(str(Path("app/templates"))), autoescape=True
    ).get_template("sections/rfs.html.j2").render(s=env["data"])
    assert "USD per RIN" in html
    assert "USD/MT" not in html
    assert "91 FR 16388" in html
    assert "Aug 2026 preliminary" in html
