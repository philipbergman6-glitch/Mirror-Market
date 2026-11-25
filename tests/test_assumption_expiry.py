"""#410: per-component expiry defaults, lapsed-input display, renewal.

The hard-stop expiry already exists and already blocks (``AssumptionSet.lookup``).
This is the policy on top of it, so a buyer in the trial can keep a freight
quote alive without knowing the repository:

1. **Defaults.** ``enter_assumption.py`` proposes a per-component lifetime from
   ``config`` when ``--expires`` is omitted, and prints it; ``--check`` warns on
   a freight that lives longer than a quote does.
2. **Renewal.** ``--renew <id> --expires <date> [--value <v>]`` re-signs an
   entry: the old row is retired in place (the file is the audit trail), a new
   row carries the new owner, date and expiry.
3. **Lapsed display, private edition only.** The dead value struck through,
   with its entry date, expiry date, owner and the exact renewal command —
   never in arithmetic, and none of it in the public edition.

Every value here is invented and every file is written under ``tmp_path``.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
import yaml

import config
from analysis.futures.privacy import AUDIENCE_PRIVATE, AUDIENCE_PUBLIC
from analysis.origins.assumptions import (
    PRIVATE_SUBDIR,
    default_expiry,
    load_assumptions,
)
from analysis.origins.domain import CostComponent, ShipmentWindow

TODAY = date(2026, 11, 25)
NOVEMBER = ShipmentWindow(date(2026, 12, 1), date(2026, 12, 31))


# ---------------------------------------------------------------------------
# 1. Default lifetimes
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("component", "expected"),
    [
        (CostComponent.OCEAN_FREIGHT, date(2026, 12, 2)),        # 7 days
        (CostComponent.ELEVATION, date(2026, 12, 25)),           # 30 days
        (CostComponent.DESTINATION_PORT_COSTS, date(2027, 2, 23)),  # 90 days
        (CostComponent.ORIGIN_PORT_COSTS, date(2027, 2, 23)),
        (CostComponent.INLAND_TRANSPORT, date(2027, 2, 23)),
        (CostComponent.FINANCING, date(2027, 2, 23)),
        (CostComponent.WORKING_CAPITAL, date(2027, 2, 23)),
        (CostComponent.IMPORT_DUTY, date(2027, 11, 25)),         # 365 days
        (CostComponent.IMPORT_VAT, date(2027, 11, 25)),
    ],
)
def test_the_default_lifetime_is_the_ticket_table(component, expected):
    assert default_expiry(component, entered_at=TODAY, window=None) == expected


def test_a_quality_adjustment_lives_until_its_shipment_window_ends():
    assert default_expiry(
        CostComponent.QUALITY_ADJUSTMENT, entered_at=TODAY, window=NOVEMBER
    ) == date(2026, 12, 31)


def test_a_windowless_quality_adjustment_has_no_default():
    """No window, nothing for it to end with — the owner must say when."""
    assert default_expiry(CostComponent.QUALITY_ADJUSTMENT, entered_at=TODAY, window=None) is None


@pytest.mark.parametrize(
    "component",
    [
        CostComponent.MARINE_INSURANCE,
        CostComponent.PROCESSING_COST,
        CostComponent.ENERGY_COST,
        CostComponent.PLANT_FREIGHT_IN,
    ],
)
def test_a_component_outside_the_table_has_no_default(component):
    """Absence never becomes an assumption: no table row, no proposed lifetime."""
    assert default_expiry(component, entered_at=TODAY, window=None) is None


def test_every_configured_lifetime_names_a_real_component():
    known = {c.value for c in CostComponent}
    assert set(config.ASSUMPTION_DEFAULT_LIFETIME_DAYS) <= known
    assert set(config.ASSUMPTION_LIFETIME_ENDS_WITH_WINDOW) <= known
    assert set(config.ASSUMPTION_LIFETIME_WARN_DAYS) <= known
    assert not set(config.ASSUMPTION_DEFAULT_LIFETIME_DAYS) & set(
        config.ASSUMPTION_LIFETIME_ENDS_WITH_WINDOW
    )


# ---------------------------------------------------------------------------
# 1b. The CLI proposes the default, prints it, and --check polices freight
# ---------------------------------------------------------------------------
@pytest.fixture
def cli_root(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "ASSUMPTIONS_DIR", str(tmp_path))
    return tmp_path


def _cli(args: list[str]) -> int:
    from scripts.enter_assumption import main

    return main(args)


def _freight_args(*extra: str) -> list[str]:
    return [
        "--component", "ocean_freight", "--value", "52.0",
        "--origin", "us_gulf", "--destination", "cn_north",
        "--window", "2026-12-01:2026-12-31",
        "--basis", "TEST — invented, not a quote",
        "--entered-by", "desk@example.com", "--entered-at", "2026-11-25",
        *extra,
    ]


def _private_entries(root: Path) -> list[dict]:
    path = root / PRIVATE_SUBDIR / "freight_and_logistics.yml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_an_omitted_expiry_is_proposed_from_the_component_default_and_printed(cli_root, capsys):
    assert _cli(_freight_args()) == 0, capsys.readouterr().err
    out = capsys.readouterr().out
    [entry] = _private_entries(cli_root)
    assert str(entry["expires_on"]) == "2026-12-02"
    assert "2026-12-02" in out
    assert "7" in out and "default" in out.lower()
    assert "--expires" in out, "the printout must say how to override the proposal"


def test_an_explicit_expiry_is_never_overridden_by_the_default(cli_root, capsys):
    assert _cli(_freight_args("--expires", "2026-11-28")) == 0, capsys.readouterr().err
    [entry] = _private_entries(cli_root)
    assert str(entry["expires_on"]) == "2026-11-28"
    assert "default" not in capsys.readouterr().out.lower()


def test_a_component_with_no_default_still_needs_an_explicit_expiry(cli_root, capsys):
    with pytest.raises(SystemExit) as stopped:
        _cli([
            "--component", "processing_cost", "--value", "22.0", "--origin", "argentina",
            "--basis", "TEST — invented", "--entered-by", "desk@example.com",
        ])
    assert stopped.value.code == 2
    assert "--expires" in capsys.readouterr().err
    assert list(cli_root.rglob("*.yml")) == []


def test_check_warns_on_a_freight_living_longer_than_two_weeks(cli_root, capsys):
    assert _cli(_freight_args("--expires", "2026-12-25")) == 0
    capsys.readouterr()
    assert _cli(["--check"]) == 0, "a long freight lifetime is a note, never a build break"
    out = capsys.readouterr().out
    assert "lifetime" in out.lower() and "30" in out and "14" in out


def test_check_is_quiet_about_a_freight_at_the_two_week_limit(cli_root, capsys):
    assert _cli(_freight_args("--expires", "2026-12-09")) == 0   # exactly 14 days
    capsys.readouterr()
    assert _cli(["--check"]) == 0
    assert "lifetime" not in capsys.readouterr().out.lower()


# ---------------------------------------------------------------------------
# 2a. A retired row stays in the file and is never selected after retirement
# ---------------------------------------------------------------------------
def _entry(**overrides) -> dict:
    base = {
        "id": "test.freight.old",
        "component": "ocean_freight",
        "value": 52.0,
        "unit": "usd_per_mt",
        "origin": "us_gulf",
        "destination": "cn_north",
        "basis": "TEST — invented, not a quote",
        "source": "test",
        "entered_by": "desk@example.com",
        "entered_at": "2026-11-01",
        "expires_on": "2026-11-30",
        "confidence": "indicative",
    }
    base.update(overrides)
    return base


def _write_private(root: Path, entries: list[dict]) -> None:
    path = root / PRIVATE_SUBDIR / "freight_and_logistics.yml"
    path.parent.mkdir(parents=True, exist_ok=True)
    clean = [{k: v for k, v in entry.items() if v is not None} for entry in entries]
    path.write_text(yaml.safe_dump(clean, sort_keys=False), encoding="utf-8")


def _freight(found):
    return found.lookup(
        CostComponent.OCEAN_FREIGHT, origin="us_gulf", destination="cn_north",
        window=NOVEMBER, on=TODAY,
    )


def test_a_renewal_chain_loads_and_the_successor_answers(tmp_path):
    """Old row retired the day the new one is signed: no overlap, one answer."""
    _write_private(tmp_path, [
        _entry(retired_on="2026-11-25", superseded_by="test.freight.new"),
        _entry(id="test.freight.new", value=55.0, entered_at="2026-11-25",
               expires_on="2026-12-02"),
    ])
    found = load_assumptions(tmp_path, audience=AUDIENCE_PRIVATE)
    assert _freight(found).id == "test.freight.new"


def test_a_retired_row_still_answers_for_the_days_before_it_was_retired(tmp_path):
    """The file is the audit trail: a ranking replayed for 20 Nov sees the old row."""
    _write_private(tmp_path, [
        _entry(retired_on="2026-11-25", superseded_by="test.freight.new"),
        _entry(id="test.freight.new", value=55.0, entered_at="2026-11-25",
               expires_on="2026-12-02"),
    ])
    found = load_assumptions(tmp_path, audience=AUDIENCE_PRIVATE)
    earlier = found.lookup(
        CostComponent.OCEAN_FREIGHT, origin="us_gulf", destination="cn_north",
        window=NOVEMBER, on=date(2026, 11, 20),
    )
    assert earlier.id == "test.freight.old"


def test_a_withdrawn_route_entry_is_not_replaced_by_a_wider_one(tmp_path):
    """Retired with no successor is a miss — never a fallback to a wider rate."""
    from analysis.origins.assumptions import AssumptionMiss

    _write_private(tmp_path, [
        _entry(id="test.elevation.to_cn", component="elevation", value=14.0,
               retired_on="2026-11-20"),
        _entry(id="test.elevation.any", component="elevation", value=9.0,
               destination=None, expires_on="2026-12-31"),
    ])
    miss = load_assumptions(tmp_path, audience=AUDIENCE_PRIVATE).lookup(
        CostComponent.ELEVATION, origin="us_gulf", destination="cn_north",
        window=NOVEMBER, on=TODAY,
    )
    assert isinstance(miss, AssumptionMiss)
    assert "retired" in miss.reason


def test_a_retirement_before_entry_is_a_load_error(tmp_path):
    from analysis.origins.assumptions import AssumptionError

    _write_private(tmp_path, [_entry(retired_on="2026-10-01")])
    with pytest.raises(AssumptionError, match="retired_on"):
        load_assumptions(tmp_path, audience=AUDIENCE_PRIVATE)


def test_a_successor_that_does_not_exist_is_a_load_error(tmp_path):
    from analysis.origins.assumptions import AssumptionError

    _write_private(tmp_path, [
        _entry(retired_on="2026-11-25", superseded_by="test.freight.nowhere"),
    ])
    with pytest.raises(AssumptionError, match="test.freight.nowhere"):
        load_assumptions(tmp_path, audience=AUDIENCE_PRIVATE)


def test_an_unretired_entry_keeps_the_set_id_it_always_had(tmp_path):
    """Retirement keys appear only when present, so stored rankings do not move."""
    from analysis.origins.assumptions import parse_assumption

    assert "retired_on" not in parse_assumption(_entry(), where="t").to_dict()
    assert "superseded_by" not in parse_assumption(_entry(), where="t").to_dict()


# ---------------------------------------------------------------------------
# 2b. --renew re-signs an entry; the old row is retired in place
# ---------------------------------------------------------------------------
HEADER = "# desk freight — TEST FILE, every number invented\n"


@pytest.fixture
def lapsed_freight(cli_root, monkeypatch):
    """One freight entered 1 Nov, lapsed 8 Nov; today is 25 Nov."""
    from scripts import enter_assumption

    monkeypatch.setattr(enter_assumption, "_today", lambda: TODAY)
    path = cli_root / PRIVATE_SUBDIR / "freight_and_logistics.yml"
    path.parent.mkdir(parents=True)
    body = yaml.safe_dump(
        [_entry(expires_on="2026-11-08", window={"start": "2026-12-01", "end": "2026-12-31"})],
        sort_keys=False,
    )
    path.write_text(HEADER + "\n" + body, encoding="utf-8")
    return path


def test_renew_retires_the_old_row_in_place_and_appends_the_re_signed_one(lapsed_freight, capsys):
    code = _cli([
        "--renew", "test.freight.old", "--expires", "2026-12-02",
        "--entered-by", "buyer@example.com",
    ])
    assert code == 0, capsys.readouterr().err
    text = lapsed_freight.read_text(encoding="utf-8")
    assert text.startswith(HEADER), "the file's own header must survive a renewal"
    old, new = yaml.safe_load(text)

    assert old["id"] == "test.freight.old"
    assert str(old["retired_on"]) == "2026-11-25"
    assert old["superseded_by"] == new["id"]
    assert old["value"] == 52.0 and str(old["expires_on"]) == "2026-11-08", "history untouched"

    assert new["id"] != old["id"]
    assert new["value"] == 52.0, "no --value re-signs the same number"
    assert str(new["entered_at"]) == "2026-11-25"
    assert str(new["expires_on"]) == "2026-12-02"
    assert new["entered_by"] == "buyer@example.com"
    for kept in ("component", "unit", "origin", "destination", "basis", "confidence"):
        assert new[kept] == old[kept], kept
    assert {k: str(v) for k, v in new["window"].items()} == {
        k: str(v) for k, v in old["window"].items()
    }

    found = load_assumptions(lapsed_freight.parents[1], audience=AUDIENCE_PRIVATE)
    assert _freight(found).id == new["id"]


def test_renew_with_a_value_re_signs_a_new_number(lapsed_freight, capsys):
    code = _cli([
        "--renew", "test.freight.old", "--expires", "2026-12-02", "--value", "58.5",
        "--entered-by", "buyer@example.com",
    ])
    assert code == 0, capsys.readouterr().err
    old, new = yaml.safe_load(lapsed_freight.read_text(encoding="utf-8"))
    assert old["value"] == 52.0
    assert new["value"] == 58.5


@pytest.mark.parametrize("dropped", ["--expires", "--entered-by"])
def test_renew_needs_an_expiry_and_a_name(lapsed_freight, capsys, dropped):
    args = {"--expires": "2026-12-02", "--entered-by": "buyer@example.com"}
    del args[dropped]
    before = lapsed_freight.read_text(encoding="utf-8")
    with pytest.raises(SystemExit) as stopped:
        _cli(["--renew", "test.freight.old", *[x for kv in args.items() for x in kv]])
    assert stopped.value.code == 2
    assert dropped in capsys.readouterr().err
    assert lapsed_freight.read_text(encoding="utf-8") == before


def test_renew_of_an_unknown_id_is_refused(lapsed_freight, capsys):
    before = lapsed_freight.read_text(encoding="utf-8")
    code = _cli([
        "--renew", "test.freight.nope", "--expires", "2026-12-02",
        "--entered-by", "buyer@example.com",
    ])
    assert code == 1
    assert "test.freight.nope" in capsys.readouterr().err
    assert lapsed_freight.read_text(encoding="utf-8") == before


def test_renew_of_a_retired_row_points_at_its_successor(lapsed_freight, capsys):
    renew = ["--renew", "test.freight.old", "--expires", "2026-12-02",
             "--entered-by", "buyer@example.com"]
    assert _cli(renew) == 0
    successor = yaml.safe_load(lapsed_freight.read_text(encoding="utf-8"))[1]["id"]
    before = lapsed_freight.read_text(encoding="utf-8")
    capsys.readouterr()
    assert _cli(renew) == 1
    assert successor in capsys.readouterr().err
    assert lapsed_freight.read_text(encoding="utf-8") == before


def test_a_renewal_of_a_renewal_does_not_grow_the_id(lapsed_freight, capsys, monkeypatch):
    from scripts import enter_assumption

    assert _cli(["--renew", "test.freight.old", "--expires", "2026-12-02",
                 "--entered-by", "buyer@example.com"]) == 0
    first = yaml.safe_load(lapsed_freight.read_text(encoding="utf-8"))[1]["id"]
    monkeypatch.setattr(enter_assumption, "_today", lambda: date(2026, 12, 2))
    assert _cli(["--renew", first, "--expires", "2026-12-09",
                 "--entered-by", "buyer@example.com"]) == 0
    second = yaml.safe_load(lapsed_freight.read_text(encoding="utf-8"))[2]["id"]
    assert second.count("renewed") == 1
    assert second.startswith("test.freight.old.")


def test_renew_refuses_an_entry_it_cannot_retire_in_place(cli_root, monkeypatch, capsys):
    """A hand-written flow-style row cannot be edited safely: refuse, change nothing."""
    from scripts import enter_assumption

    monkeypatch.setattr(enter_assumption, "_today", lambda: TODAY)
    path = cli_root / PRIVATE_SUBDIR / "freight_and_logistics.yml"
    path.parent.mkdir(parents=True)
    path.write_text(
        yaml.safe_dump([_entry(expires_on="2026-11-08")], default_flow_style=True),
        encoding="utf-8",
    )
    before = path.read_text(encoding="utf-8")
    code = _cli(["--renew", "test.freight.old", "--expires", "2026-12-02",
                 "--entered-by", "buyer@example.com"])
    assert code == 1
    assert "retired_on" in capsys.readouterr().err, "say what to add by hand"
    assert path.read_text(encoding="utf-8") == before


# ---------------------------------------------------------------------------
# 3. Lapsed display: private edition only, never in arithmetic
# ---------------------------------------------------------------------------
# Distinctive invented numbers, so a leak is findable in rendered HTML.
DEAD_FREIGHT = 61.37
DEAD_DUTY = 0.0313
OWNER = "lapsed-owner@example.com"


@pytest.fixture
def lapsed_root(tmp_path, monkeypatch):
    """A committed tier with a lapsed duty and a private tier with a lapsed freight."""
    monkeypatch.setattr(config, "ASSUMPTIONS_DIR", str(tmp_path))
    duty = _entry(
        id="test.duty.lapsed", component="import_duty", value=DEAD_DUTY, unit="fraction",
        origin=None, entered_by="policy desk", entered_at="2025-11-01",
        expires_on="2026-11-01",
    )
    (tmp_path / "policy.yml").write_text(
        yaml.safe_dump([{k: v for k, v in duty.items() if v is not None}], sort_keys=False),
        encoding="utf-8",
    )
    _write_private(tmp_path, [
        _entry(id="test.freight.lapsed", value=DEAD_FREIGHT, entered_by=OWNER,
               entered_at="2026-11-02", expires_on="2026-11-09"),
    ])
    return tmp_path


def _render(view: dict) -> str:
    from datetime import datetime, timezone

    from scripts.generate_site import _env

    now = datetime(2026, 11, 25, 21, 0, tzinfo=timezone.utc)
    return _env().get_template("origins.html.j2").render(
        origins=view, root="", market_nav=[], current_page="origins",
        current_market=None, day_line="WEDNESDAY 25 NOVEMBER 2026",
        generated_at=now.strftime("%Y-%m-%d %H:%M UTC"), generated_at_iso=now.isoformat(),
    )


def test_the_private_edition_shows_the_dead_value_its_dates_owner_and_renewal(lapsed_root):
    from app import origins_page

    view = origins_page.build_view(None, today=TODAY, audience=AUDIENCE_PRIVATE)
    lapsed = view["lapsed_inputs"]["test.freight.lapsed"]
    assert lapsed["value"] == DEAD_FREIGHT
    assert lapsed["entered_at"] == "2026-11-02"
    assert lapsed["expires_on"] == "2026-11-09"
    assert lapsed["entered_by"] == OWNER
    assert lapsed["renew_command"] == (
        "python scripts/enter_assumption.py --renew test.freight.lapsed "
        "--expires 2026-12-02 --entered-by '<YOUR-NAME>'"
    )

    html = _render(view)
    assert f"<s>{DEAD_FREIGHT:g} usd_per_mt</s>" in html
    assert "--renew test.freight.lapsed" in html
    assert "2026-11-02" in html and "2026-11-09" in html and OWNER in html


def test_a_lapsed_input_is_never_in_the_arithmetic(lapsed_root):
    """Still withheld and still named, exactly as before #410."""
    from app import origins_page

    view = origins_page.build_view(None, today=TODAY, audience=AUDIENCE_PRIVATE)
    for ranking in view["rankings"]:
        for row in ranking.rows:
            assert all(step.assumption_id != "test.freight.lapsed" for step in row.steps)
    for window in view["views"]:
        gulf = next(r for r in window["readiness"]["routes"] if r["origin_key"] == "us_gulf")
        freight = next(q for q in gulf["requirements"] if q["component"] == "ocean_freight")
        assert freight["status"] == "expired"
        assert freight["is_blocking"]


def test_the_public_edition_renders_no_lapsed_value_and_no_renewal(lapsed_root):
    from app import origins_page

    view = origins_page.build_view(None, today=TODAY, audience=AUDIENCE_PUBLIC)
    assert view["lapsed_inputs"] == {}
    html = _render(view)
    assert "<s>" not in html
    assert "--renew" not in html
    assert f"{DEAD_FREIGHT:g}" not in html
    assert f"{DEAD_DUTY:g}" not in html, "a lapsed policy rate's dead value is private too"
    assert OWNER not in html
    assert "test.freight.lapsed" not in html


def test_a_retired_row_is_history_not_a_lapse_to_renew(lapsed_root, capsys, monkeypatch):
    """After --renew, the old row leaves the lapsed list and the renewal queue."""
    from analysis.origins.readiness import expiry_review
    from app import origins_page
    from scripts import enter_assumption

    monkeypatch.setattr(enter_assumption, "_today", lambda: TODAY)
    assert _cli(["--renew", "test.freight.lapsed", "--expires", "2026-12-02",
                 "--entered-by", OWNER]) == 0, capsys.readouterr().err
    view = origins_page.build_view(None, today=TODAY, audience=AUDIENCE_PRIVATE)
    assert "test.freight.lapsed" not in view["lapsed_inputs"]
    review = expiry_review(
        load_assumptions(lapsed_root, audience=AUDIENCE_PRIVATE), today=TODAY
    )
    assert "test.freight.lapsed" not in {row["id"] for row in review["expired"]}


def test_the_review_queue_prints_the_renewal_command_for_a_lapsed_entry(
    lapsed_root, capsys, monkeypatch
):
    from scripts import enter_assumption

    monkeypatch.setattr(enter_assumption, "_today", lambda: TODAY)
    assert _cli(["--review"]) == 1, "something has lapsed: the queue exits 1, as before"
    out = capsys.readouterr().out
    assert (
        "python scripts/enter_assumption.py --renew test.freight.lapsed "
        "--expires 2026-12-02 --entered-by '<YOUR-NAME>'"
    ) in out


# --- #618 review: the renewal command must be pasteable and signed by whoever pastes it ---


def _one(entry: dict):
    from analysis.origins.assumptions import parse_assumption

    return parse_assumption({k: v for k, v in entry.items() if v is not None}, where="<test>")


def test_the_renewal_command_survives_a_shell_and_leaves_the_signer_blank():
    import shlex

    from analysis.origins.readiness import RENEWAL_SIGNER_PLACEHOLDER, renewal_command

    # The committed policy rates carry this exact owner: '#' starts a bash
    # comment and '(' is a zsh syntax error if pasted unquoted.
    owner = "mirror-market B22 #485 (published policy rate, not a trader entry)"
    item = _one(_entry(id="test.freight.it's", entered_by=owner,
                       window={"start": "2026-12-01", "end": "2026-12-31"}))

    command = renewal_command(item, today=TODAY)

    assert command is not None
    argv = shlex.split(command)
    assert argv[argv.index("--renew") + 1] == "test.freight.it's"
    assert argv[argv.index("--entered-by") + 1] == RENEWAL_SIGNER_PLACEHOLDER
    assert owner not in command, "pasting must never re-sign under the previous owner"


def test_a_sailed_window_is_history_not_a_renewal(cli_root):
    from analysis.origins.readiness import lapsed_inputs, renewal_command

    sailed = _entry(id="test.freight.october", entered_at="2026-09-20", expires_on="2026-09-27",
                    window={"start": "2026-10-01", "end": "2026-10-31"})
    _write_private(cli_root, [sailed])

    found = load_assumptions(cli_root, audience=AUDIENCE_PRIVATE)
    item = next(a for a in found.assumptions if a.id == "test.freight.october")

    assert renewal_command(item, today=TODAY) is None, (
        "--renew would be refused (window_already_sailed): printing it is a dead command"
    )
    assert [x.assumption.id for x in lapsed_inputs(found, today=TODAY)] == []


@pytest.mark.parametrize("flag", [
    ["--origin", "br_paranagua"], ["--confidence", "provisional"], ["--citation", "x"],
    ["--window", "2027-01-01:2027-01-31"], ["--unit", "usd_per_mt"], ["--days", "30"],
    ["--component", "ocean_freight"], ["--destination", "cn_north"], ["--source", "broker"],
    ["--file", "x.yml"],
])
def test_renew_refuses_a_flag_it_would_silently_ignore(lapsed_freight, capsys, flag):
    before = lapsed_freight.read_text(encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        _cli(["--renew", "test.freight.old", "--expires", "2026-12-02",
              "--entered-by", "buyer@example.com", *flag])

    assert exc.value.code == 2
    assert flag[0] in capsys.readouterr().err
    assert lapsed_freight.read_text(encoding="utf-8") == before
