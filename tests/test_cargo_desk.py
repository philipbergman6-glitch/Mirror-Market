"""Synthetic cargo decision using the private CLI, SQLite provider and real engines."""

import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from analysis.futures.privacy import AUDIENCE_PRIVATE
from analysis.origins.assumptions import load_assumptions
from analysis.origins.cargo import evaluate_case
from scripts import cargo_desk

FIXTURE_ASSUMPTIONS = Path(__file__).parent / "fixtures" / "assumptions_complete"


def _case():
    common = {
        "carrier": "vessel",
        "incoterm": "FOB",
        "grade": "Synthetic soybean specification",
        "observed_at": "2026-08-18T07:00:00-05:00",
        "expires_at": "2026-08-18T10:00:00-05:00",
        "loading_start": "2026-09-01",
        "loading_end": "2026-09-30",
        "available_mt": 25000,
        "unpriced_mt": 25000,
        "source": "Synthetic offer",
        "contract": "ZSX26",
        "pricing_end": "2026-09-15",
    }
    return {
        "decision_at": "2026-08-18T08:00:00-05:00",
        "destination": "cn_north",
        "quantity_mt": 25000,
        "loading_start": "2026-09-01",
        "loading_end": "2026-09-30",
        "offers": [
            {**common, "id": "gulf", "origin": "us_gulf", "price_usd_mt": 420,
             "carrier": "barge", "incoterm": "CIF", "duty_condition_confirmed": True},
            {**common, "id": "brazil", "origin": "br_paranagua", "price_usd_mt": 458},
        ],
    }


def test_private_cargo_case_to_saved_named_hedge(tmp_path, monkeypatch):
    workspace = tmp_path / "private"
    workspace.mkdir()
    (workspace / "synthetic.json").write_text(json.dumps(_case()), encoding="utf-8")
    db = tmp_path / "market.db"
    with closing(sqlite3.connect(db)) as conn:
        conn.execute(
            "CREATE TABLE forward_curve (commodity TEXT, contract_month TEXT, label TEXT, ticker TEXT, "
            "close REAL, observation_date TEXT, fetched_date TEXT)"
        )
        conn.execute(
            "INSERT INTO forward_curve VALUES ('Soybeans','2026-11-01','Nov 2026','ZSX26.CBT',"
            "1167.75,'2026-08-17','2026-08-17')"
        )
        conn.commit()
    monkeypatch.setattr(cargo_desk.config, "DB_PATH", db)
    monkeypatch.setattr(cargo_desk, "WORKSPACE", workspace)
    monkeypatch.setattr(
        cargo_desk, "load_assumptions",
        lambda *, audience: load_assumptions(FIXTURE_ASSUMPTIONS, audience=audience),
    )
    assert cargo_desk.main(["synthetic", "--save", "decision-01"]) == 0
    target = workspace / "decisions" / "decision-01.json"
    original = target.read_text(encoding="utf-8")
    saved = json.loads(original)
    assert saved["case"] == _case()
    result = saved["result"]
    assert result["winner"] == "brazil"
    assert result["hedge"]["symbol"] == "ZSX26"
    assert result["hedge"]["contracts"] == 184
    assert result["hedge"]["side"] == "long"
    assert result["freight_breakeven"]["move_usd_mt"] > 0
    with pytest.raises(SystemExit) as taken:
        cargo_desk.main(["synthetic", "--save", "decision-01"])
    assert taken.value.code == 1
    assert target.read_text(encoding="utf-8") == original


def test_conditional_duty_and_missing_named_month_fail_closed():
    case = _case()
    assumptions = load_assumptions(FIXTURE_ASSUMPTIONS, audience=AUDIENCE_PRIVATE)
    case["offers"][0]["duty_condition_confirmed"] = False
    blocked = evaluate_case(case, assumptions, lambda contract, as_of: None)
    assert blocked["winner"] is None
    assert "confirm the applicable duty" in blocked["offers"][0]["reason"]
    case["offers"][0]["duty_condition_confirmed"] = True
    case["offers"][1]["contract"] = None
    comparison = evaluate_case(case, assumptions, lambda contract, as_of: None)
    assert comparison["winner"] == "brazil"
    assert comparison["hedge"]["state"] == "blocked"
    assert "named contract" in comparison["hedge"]["reason"]


def test_cutoff_uses_cbot_calendar_and_missing_prior_quote_blocks():
    case = _case()
    # The buyer's local date has rolled to August 19. Chicago remains on the
    # 18th, so the delayed quote proved available before their cutoff is the 17th.
    case["decision_at"] = "2026-08-19T02:00:00+14:00"
    for offer in case["offers"]:
        offer["observed_at"] = "2026-08-18T06:00:00-05:00"
        offer["expires_at"] = "2026-08-18T09:00:00-05:00"
    assumptions = load_assumptions(FIXTURE_ASSUMPTIONS, audience=AUDIENCE_PRIVATE)
    requested = []

    def no_quote(contract, as_of):
        requested.append((contract.symbol, as_of.isoformat()))
        return None

    result = evaluate_case(case, assumptions, no_quote)
    assert result["winner"] == "brazil"
    assert requested == [("ZSX26", "2026-08-17")]
    assert result["hedge"] == {
        "state": "blocked", "reason": "no recent prior close for the exact named contract",
    }
