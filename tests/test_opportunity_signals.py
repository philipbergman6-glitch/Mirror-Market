"""Detector guards — requirement 9's "does not generate from bad inputs" half.

Every test here is a case where the arithmetic would happily produce a number
and the number would be wrong or meaningless.
"""

from __future__ import annotations

from datetime import date, timedelta

from opportunity_fixtures import (
    board_crush_result,
    crush_result,
    seed_currency,
    seed_export_sales,
    seed_freshness,
    seed_weekly_flow,
    stub_crush_stack,
)

import config
from analysis.opportunities import signals as signals_mod

TODAY = date(2026, 8, 18)


# ---------------------------------------------------------------------------
# Flow / commitment
# ---------------------------------------------------------------------------
def test_flow_shift_fires_on_a_share_spike(tmp_db):
    seed_weekly_flow(tmp_db)
    found = signals_mod.flow_shift_detections(tmp_db, today=TODAY)
    assert [d.destination.country_iso for d in found] == ["CN"]
    detection = found[0]
    assert detection.dislocation.z_score is not None
    assert detection.dislocation.value > detection.dislocation.baseline
    # The tonnage carried is the week's own published flow, and it is labelled
    # as such rather than as an offerable cargo.
    assert detection.volume is not None
    assert "not an estimate" in detection.volume.basis


def test_flow_shift_needs_enough_history(tmp_db):
    """Eight weeks is not a baseline. One vessel would set the mean."""
    seed_weekly_flow(tmp_db, weeks=8)
    assert signals_mod.flow_shift_detections(tmp_db, today=TODAY) == []


def test_flow_shift_ignores_a_destination_below_the_share_floor(tmp_db):
    settings = config.OPPORTUNITY_RULES["destination_flow_shift"]
    # A 2% share that triples is still 2% — one Panamax into a small buyer.
    seed_weekly_flow(tmp_db, baseline_share=0.005, spike_share=settings["min_share"] / 2)
    found = signals_mod.flow_shift_detections(tmp_db, today=TODAY)
    assert all(d.destination.country_iso != "CN" for d in found)


def test_flow_shift_refuses_a_flat_baseline(tmp_db):
    """A zero standard deviation makes every z-score infinite, not significant."""
    rows = []
    for index in range(30):
        week = date(2026, 8, 16) - timedelta(weeks=29 - index)
        share = 0.30 if index < 29 else 0.75
        rows.append((week.isoformat(), "fixture", "CHINA", "Soybeans", 1e6 * share))
        rows.append((week.isoformat(), "fixture", "MEXICO", "Soybeans", 1e6 * (1 - share)))
    tmp_db.executemany(
        "INSERT INTO inspection_destinations "
        "(week_ending, region, country, commodity, inspections_mt) VALUES (?,?,?,?,?)",
        rows,
    )
    tmp_db.commit()
    found = signals_mod.flow_shift_detections(tmp_db, today=TODAY)
    # A perfectly flat baseline has stdev 0; _zscore returns None and nothing fires.
    assert [d.destination.country_iso for d in found] == []


def test_an_unmapped_destination_is_skipped_not_guessed(tmp_db):
    seed_weekly_flow(tmp_db, country="REPUBLIC OF NOWHERE")
    assert signals_mod.flow_shift_detections(tmp_db, today=TODAY) == []


def test_commitment_uses_outstanding_sales_not_inspections(tmp_db):
    seed_export_sales(tmp_db)
    found = signals_mod.commitment_detections(tmp_db, today=TODAY)
    assert [d.rule_id for d in found] == ["commitment_shift"]
    assert found[0].signal.evidence[0].source.table == "export_sales"
    # And the shipped-flow rule must not see the same rows.
    assert signals_mod.flow_shift_detections(tmp_db, today=TODAY) == []


def test_flow_regions_are_summed_not_treated_as_separate_buyers(tmp_db):
    """A cargo moving Gulf → PNW is a logistics story, not a new buyer."""
    rows = []
    for index in range(30):
        week = date(2026, 8, 16) - timedelta(weeks=29 - index)
        china = 750_000 if index == 29 else 300_000 + (index % 3) * 10_000
        rows.extend([
            (week.isoformat(), "Gulf", "CHINA", "Soybeans", china / 2),
            (week.isoformat(), "PNW", "CHINA", "Soybeans", china / 2),
            (week.isoformat(), "Gulf", "MEXICO", "Soybeans", 1_000_000 - china),
        ])
    tmp_db.executemany(
        "INSERT INTO inspection_destinations "
        "(week_ending, region, country, commodity, inspections_mt) VALUES (?,?,?,?,?)",
        rows,
    )
    tmp_db.commit()
    found = signals_mod.flow_shift_detections(tmp_db, today=TODAY)
    assert len(found) == 1
    assert found[0].volume.low_mt == 750_000.0


# ---------------------------------------------------------------------------
# Currency
# ---------------------------------------------------------------------------
def test_a_weaker_origin_currency_calls_for_a_seller(tmp_db):
    """BRL/USD is USD per BRL, so a FALL is a weaker real and a keener farmer."""
    seed_currency(tmp_db, pair="BRL/USD", start=0.20, end=0.18)
    found = signals_mod.currency_detections(tmp_db, today=TODAY)
    brazil = [d for d in found if d.origin.country_iso == "BR"]
    assert len(brazil) == 1
    detection = brazil[0]
    # (0.18 - 0.20) / 0.20 = -10.0%
    assert detection.dislocation.value == -10.0
    assert detection.role_wanted.value == "seller"
    assert "weaker" in detection.why_now
    assert "paid more per dollar cargo" in detection.why_now


def test_a_stronger_origin_currency_flips_the_side(tmp_db):
    seed_currency(tmp_db, pair="BRL/USD", start=0.18, end=0.20)
    detection = [
        d for d in signals_mod.currency_detections(tmp_db, today=TODAY)
        if d.origin.country_iso == "BR"
    ][0]
    assert detection.dislocation.value == round(2 / 18 * 100, 2)  # +11.11%
    assert detection.role_wanted.value == "buyer"
    assert "slows farmer selling" in detection.why_now


def test_a_small_currency_move_does_not_fire(tmp_db):
    seed_currency(tmp_db, pair="BRL/USD", start=0.200, end=0.198)  # -1%
    assert signals_mod.currency_detections(tmp_db, today=TODAY) == []


def test_currency_needs_a_full_lookback(tmp_db):
    seed_currency(tmp_db, pair="BRL/USD", sessions=5, start=0.20, end=0.10)
    assert signals_mod.currency_detections(tmp_db, today=TODAY) == []


# ---------------------------------------------------------------------------
# PSD dating
# ---------------------------------------------------------------------------
def test_psd_signals_are_dated_by_our_own_ingest_not_by_today(tmp_db):
    """PSD carries no observation date. Stamping it with today would render a
    marketing-year balance sheet as this morning's news."""
    rows = []
    for year, stocks in ((2021, 900), (2022, 850), (2023, 880), (2024, 870), (2025, 400)):
        rows.extend([
            ("Soybeans", "China", year, "Ending Stocks", stocks, "1000 MT"),
            ("Soybeans", "China", year, "Domestic Consumption", 10_000, "1000 MT"),
            ("Soybeans", "China", year, "Exports", 100, "1000 MT"),
        ])
    tmp_db.executemany(
        "INSERT INTO psd (commodity, country, year, attribute, value, unit) "
        "VALUES (?,?,?,?,?,?)", rows,
    )
    ingest_date = TODAY - timedelta(days=12)
    seed_freshness(tmp_db, "psd", last_success=ingest_date)

    found = signals_mod.supply_deficit_detections(tmp_db, today=TODAY)
    china = [d for d in found if d.destination.country_iso == "CN"]
    assert len(china) == 1
    evidence = china[0].signal.evidence[0]
    assert evidence.observed_on == ingest_date
    assert "publishes no observation date" in evidence.note


def test_psd_signal_is_withheld_when_we_cannot_date_it(tmp_db):
    tmp_db.executemany(
        "INSERT INTO psd (commodity, country, year, attribute, value, unit) VALUES (?,?,?,?,?,?)",
        [("Soybeans", "China", 2025, "Ending Stocks", 1, "1000 MT")],
    )
    tmp_db.commit()
    # No data_freshness row for psd: we cannot say when we learned this.
    assert signals_mod.supply_deficit_detections(tmp_db, today=TODAY) == []


def test_exporting_countries_are_not_scanned_for_a_buyer_deficit(tmp_db):
    """A tight balance sheet in Brazil is a supply story, not a buyer with a hole."""
    rows = []
    for year, stocks in ((2021, 900), (2022, 850), (2023, 880), (2024, 870), (2025, 10)):
        rows.extend([
            ("Soybeans", "Brazil", year, "Ending Stocks", stocks, "1000 MT"),
            ("Soybeans", "Brazil", year, "Domestic Consumption", 50_000, "1000 MT"),
            ("Soybeans", "Brazil", year, "Exports", 90_000, "1000 MT"),
        ])
    tmp_db.executemany(
        "INSERT INTO psd (commodity, country, year, attribute, value, unit) VALUES (?,?,?,?,?,?)",
        rows,
    )
    seed_freshness(tmp_db, "psd")
    found = signals_mod.supply_deficit_detections(tmp_db, today=TODAY)
    assert all(d.destination.country_iso != "BR" for d in found)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def test_a_failing_detector_is_reported_not_swallowed(tmp_db, monkeypatch):
    def boom(conn, *, today):
        raise RuntimeError("upstream is on fire")

    monkeypatch.setattr(
        signals_mod, "DETECTORS",
        (("destination_flow_shift", boom, False),),
    )
    run = signals_mod.detect_all(tmp_db, today=TODAY)
    assert run.detections == ()
    assert len(run.failed) == 1
    assert "upstream is on fire" in run.failed[0]["error"]


def test_an_empty_database_produces_no_detections_and_no_errors(tmp_db):
    run = signals_mod.detect_all(tmp_db, today=TODAY)
    assert run.detections == ()
    assert all(entry["ran"] for entry in run.coverage)
    assert {entry["rule_id"] for entry in run.coverage} == set(config.OPPORTUNITY_RULES)


# ---------------------------------------------------------------------------
# Crush margin: the prose carries the legs' price level (#405)
# ---------------------------------------------------------------------------
def _only(detections, slug):
    found = [d for d in detections if d.context["market"] == slug]
    assert len(found) == 1, [d.signal.headline for d in detections]
    return found[0]


def test_administered_legs_are_not_called_a_gross_physical_crush(monkeypatch):
    stub_crush_stack(monkeypatch, {"argentina": crush_result("argentina", "administered")})

    det = _only(signals_mod.crush_margin_detections(None, today=TODAY, assumptions={}), "argentina")

    for text in (det.signal.headline, det.why_now, det.dislocation.label, det.economics.method):
        assert "administered" in text.lower(), text
        assert "gross physical" not in text.lower(), text
    assert "room to bid" not in det.why_now
    # The detail is the shared caveat for the level, not "cash bean against cash oil".
    assert "decree" in det.signal.detail
    assert "cash bean" not in det.signal.detail.lower()
    assert det.context["level"] == "gross_physical"


def test_traded_legs_keep_the_gross_physical_wording(monkeypatch):
    stub_crush_stack(monkeypatch, {"cbot": crush_result("cbot", "physical")})

    det = _only(signals_mod.crush_margin_detections(None, today=TODAY, assumptions={}), "cbot")

    assert det.signal.headline.startswith("CBOT gross physical crush at 24.10 USD/MT")
    assert "gross physical crush" in det.why_now
    assert "room to bid up for beans" in det.why_now
    assert "administered" not in det.why_now.lower()
    assert det.signal.detail == det.economics.note
    assert "Cash bean against cash oil and meal" in det.signal.detail


# ---------------------------------------------------------------------------
# A pricing venue is not a delivery destination (#672)
# ---------------------------------------------------------------------------
def test_a_board_crush_names_no_destination_and_says_why(monkeypatch):
    """The live 83.9 row: destination "CBOT" — the venue had become the berth."""
    stub_crush_stack(monkeypatch, {"cbot": board_crush_result("cbot")})

    det = _only(signals_mod.crush_margin_detections(None, today=TODAY, assumptions={}), "cbot")

    assert det.origin is None
    assert det.destination is None
    assert det.lane_absent_reason.startswith("no physical destination")
    assert "crusher" in det.lane_absent_reason
    # The number itself, its venue and its contracts are untouched.
    assert det.economics.per_mt.amount == 68.30
    assert det.signal.subject == "US"
    assert det.signal.headline.startswith("CBOT board crush")
    assert det.context["contracts"] == ["ZSF27", "ZLF27", "ZMF27"]


def test_no_crush_names_a_pricing_venue_as_its_destination(monkeypatch):
    """Every crush market, every level: a destination is a country, never a venue."""
    venues = {str(market["name"]) for market in config.MARKETS.values()} - {
        str(market["psd_country"]) for market in config.MARKETS.values()
    }
    assert {"CBOT", "Dalian"} <= venues
    for kind in ("physical", "administered", "board"):
        results = {
            slug: (board_crush_result(slug) if kind == "board" else crush_result(slug, kind))
            for slug in config.PHYSICAL_CRUSH
        }
        stub_crush_stack(monkeypatch, results)
        for det in signals_mod.crush_margin_detections(None, today=TODAY, assumptions={}):
            if det.destination is not None:
                assert det.destination.name not in venues, (kind, det.destination)


def test_a_physical_crush_names_the_crushers_country_not_the_venue(monkeypatch):
    stub_crush_stack(monkeypatch, {"dalian": crush_result("dalian", "physical")})

    det = _only(signals_mod.crush_margin_detections(None, today=TODAY, assumptions={}), "dalian")

    assert det.destination is not None
    assert det.destination.name == "China"
    assert det.destination.country_iso == "CN"
    assert det.lane_absent_reason == ""


def test_an_administered_crush_names_no_destination(monkeypatch):
    """No plant buys or sells at a decree, so no crusher is shown to earn it."""
    stub_crush_stack(monkeypatch, {"argentina": crush_result("argentina", "administered")})

    det = _only(signals_mod.crush_margin_detections(None, today=TODAY, assumptions={}), "argentina")

    assert det.destination is None
    assert det.lane_absent_reason.startswith("no physical destination")


def test_a_board_crush_does_not_claim_a_crusher_earns_it(monkeypatch):
    """The lead sentence must agree with the blocker: a paper margin, not room to bid."""
    stub_crush_stack(monkeypatch, {"cbot": board_crush_result("cbot")})

    det = _only(signals_mod.crush_margin_detections(None, today=TODAY, assumptions={}), "cbot")

    assert "room to bid" not in det.why_now
    assert "paper margin" in det.why_now
    assert det.why_now.startswith("CBOT's board crush")
