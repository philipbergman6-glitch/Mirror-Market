"""B12 (#409): desk assumptions are gitignored files, policy rates are committed.

The first real broker freight indication entered through
``scripts/enter_assumption.py`` carries the owner's email and the broker in
``basis``. Before this split the only costable path wrote it to a tracked file,
so the next push published it. Invariant 4: client records are gitignored
files, never anything git can see.

Three things are pinned here, each independently:

1. the **loader** draws the tier line — a desk component in a public-tier file
   is a load error, the public audience never sees ``private/``;
2. the **CLI** routes every desk component to ``private/`` and refuses an
   explicit target outside it;
3. the **repository** matches — ``private/*.yml`` is gitignored, nothing under
   it is tracked, and the public origins page build never reads it.
"""

from __future__ import annotations

import subprocess
from datetime import date
from pathlib import Path

import pytest
import yaml

import config
from analysis.futures.privacy import AUDIENCE_PRIVATE, AUDIENCE_PUBLIC, ClientDataLeak
from analysis.origins.assumptions import (
    DESK_COMPONENTS,
    POLICY_COMPONENTS,
    PRIVATE_SUBDIR,
    AssumptionError,
    load_assumptions,
    private_assumptions_dir,
)
from analysis.origins.domain import CostComponent

REPO = Path(__file__).resolve().parents[1]
SHIPPED = REPO / "data" / "reference" / "assumptions"


def _entry(component: str, **overrides) -> dict:
    base = {
        "id": f"test.{component}",
        "component": component,
        "value": 44.0,
        "unit": "usd_per_mt",
        "origin": "br_paranagua",
        "destination": "cn_north",
        "basis": "test — invented",
        "source": "test",
        "entered_by": "tests@example.com",
        "entered_at": "2026-08-01",
        "expires_on": "2027-12-31",
        "confidence": "indicative",
    }
    base.update(overrides)
    return base


def _duty() -> dict:
    return _entry(
        "import_duty", id="test.duty", value=0.03, unit="fraction", origin=None
    ) | {"origin": None}


def _write(path: Path, entries: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    clean = [{k: v for k, v in e.items() if v is not None} for e in entries]
    path.write_text(yaml.safe_dump(clean, sort_keys=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# 1. The tier line is drawn by the loader
# ---------------------------------------------------------------------------
def test_every_component_is_either_policy_or_desk_and_never_both():
    """A new component defaults to desk — the private tier — never to committed."""
    assert frozenset() == POLICY_COMPONENTS & DESK_COMPONENTS
    assert frozenset(CostComponent) - {
        CostComponent.ORIGIN_PRICE
    } == POLICY_COMPONENTS | DESK_COMPONENTS
    assert {CostComponent.IMPORT_DUTY, CostComponent.IMPORT_VAT} == POLICY_COMPONENTS
    for named in (
        "ocean_freight", "elevation", "inland_transport", "origin_port_costs",
        "destination_port_costs", "financing", "working_capital", "quality_adjustment",
        "processing_cost", "energy_cost", "plant_freight_in", "marine_insurance",
    ):
        assert CostComponent(named) in DESK_COMPONENTS, named


def test_a_desk_component_in_a_public_tier_file_is_a_load_error(tmp_path):
    _write(tmp_path / "freight.yml", [_entry("ocean_freight")])
    with pytest.raises(AssumptionError, match="private"):
        load_assumptions(tmp_path, audience=AUDIENCE_PRIVATE)
    with pytest.raises(AssumptionError, match="private"):
        load_assumptions(tmp_path, audience=AUDIENCE_PUBLIC)


def test_the_public_audience_never_reads_the_private_tier(tmp_path):
    _write(tmp_path / "policy.yml", [_duty()])
    _write(tmp_path / PRIVATE_SUBDIR / "freight.yml", [_entry("ocean_freight")])

    public = load_assumptions(tmp_path, audience=AUDIENCE_PUBLIC)
    assert [a.component for a in public.assumptions] == [CostComponent.IMPORT_DUTY]
    assert all(not name.startswith(PRIVATE_SUBDIR) for name in public.loaded_from)

    private = load_assumptions(tmp_path, audience=AUDIENCE_PRIVATE)
    assert {a.component for a in private.assumptions} == {
        CostComponent.IMPORT_DUTY, CostComponent.OCEAN_FREIGHT
    }
    assert f"{PRIVATE_SUBDIR}/freight.yml" in private.loaded_from


def test_the_default_audience_is_public():
    """Every caller that does not opt in gets the committed tier only.

    The leak path is the default: five builders call ``load_assumptions()``
    bare, and two of them render into ``docs/``.
    """
    import inspect

    assert inspect.signature(load_assumptions).parameters["audience"].default == AUDIENCE_PUBLIC


def test_an_unknown_audience_is_refused_not_defaulted(tmp_path):
    with pytest.raises(ValueError, match="audience"):
        load_assumptions(tmp_path, audience="desk")


def test_a_policy_rate_may_sit_in_either_tier(tmp_path):
    """An origin-scoped retaliatory duty is a desk's own entry; it may stay private."""
    _write(tmp_path / PRIVATE_SUBDIR / "duty.yml", [_duty()])
    private = load_assumptions(tmp_path, audience=AUDIENCE_PRIVATE)
    assert len(private.assumptions) == 1
    assert load_assumptions(tmp_path, audience=AUDIENCE_PUBLIC).assumptions == ()


def test_private_assumptions_dir_is_the_subdirectory_of_the_configured_root(tmp_path):
    assert private_assumptions_dir(tmp_path) == tmp_path / PRIVATE_SUBDIR


# ---------------------------------------------------------------------------
# 2. The CLI routes desk components to the private tier
# ---------------------------------------------------------------------------
@pytest.fixture
def cli_root(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "ASSUMPTIONS_DIR", str(tmp_path))
    return tmp_path


def _add(args: list[str]) -> int:
    from scripts.enter_assumption import main

    return main(args)


def test_a_freight_entry_lands_in_the_private_tier(cli_root, capsys):
    code = _add([
        "--component", "ocean_freight", "--value", "52.0",
        "--origin", "us_gulf", "--destination", "cn_north",
        "--window", "2026-11-01:2026-11-30",
        "--basis", "Panamax 60kt USG-N.China, broker indication",
        "--entered-by", "desk@example.com", "--expires", "2026-11-07",
    ])
    assert code == 0, capsys.readouterr().err
    written = sorted(p.relative_to(cli_root).as_posix() for p in cli_root.rglob("*.yml"))
    assert written == [f"{PRIVATE_SUBDIR}/freight_and_logistics.yml"]
    assert "desk@example.com" not in "".join(
        p.read_text(encoding="utf-8") for p in cli_root.glob("*.yml")
    )
    loaded = load_assumptions(cli_root, audience=AUDIENCE_PRIVATE)
    assert [a.component for a in loaded.assumptions] == [CostComponent.OCEAN_FREIGHT]
    assert load_assumptions(cli_root, audience=AUDIENCE_PUBLIC).assumptions == ()


def test_every_desk_component_defaults_to_a_private_file():
    from scripts.enter_assumption import DEFAULT_FILE_BY_COMPONENT, FALLBACK_FILE, _target_file

    for component in DESK_COMPONENTS:
        target = _target_file(component, None)
        assert target.parent == Path(config.ASSUMPTIONS_DIR) / PRIVATE_SUBDIR, component
    for component in POLICY_COMPONENTS:
        target = _target_file(component, None)
        assert target.parent == Path(config.ASSUMPTIONS_DIR), component
    assert FALLBACK_FILE.startswith(f"{PRIVATE_SUBDIR}/")
    assert all(
        name.startswith(f"{PRIVATE_SUBDIR}/")
        for component, name in DEFAULT_FILE_BY_COMPONENT.items()
        if component in DESK_COMPONENTS
    )


def test_an_explicit_public_target_for_a_desk_component_is_refused(cli_root, capsys):
    code = _add([
        "--component", "financing", "--value", "0.065", "--days", "45",
        "--destination", "cn_north",
        "--basis", "own cost of funds", "--entered-by", "desk@example.com",
        "--expires", "2027-01-31", "--file", "china_import_policy.yml",
    ])
    assert code == 1
    assert "private" in capsys.readouterr().err
    assert list(cli_root.rglob("*.yml")) == []


def test_a_policy_rate_still_lands_in_the_committed_file(cli_root, capsys):
    code = _add([
        "--component", "import_duty", "--value", "0.28", "--unit", "fraction",
        "--origin", "us_gulf", "--destination", "cn_north",
        "--basis", "MFN 3% + retaliatory 25%", "--entered-by", "desk@example.com",
        "--expires", "2026-12-31",
    ])
    assert code == 0, capsys.readouterr().err
    assert (cli_root / "china_import_policy.yml").exists()
    assert not (cli_root / PRIVATE_SUBDIR).exists()


def test_check_refuses_a_desk_component_found_in_a_committed_file(cli_root, capsys):
    _write(cli_root / "freight_and_logistics.yml", [_entry("ocean_freight")])
    assert _add(["--check"]) == 1
    assert "private" in capsys.readouterr().err


def test_check_fails_when_a_private_file_is_tracked_by_git(cli_root, monkeypatch, capsys):
    """The done criterion: a real entry on a fresh clone cannot be committed."""
    from scripts import enter_assumption

    _write(cli_root / PRIVATE_SUBDIR / "freight.yml", [_entry("ocean_freight")])
    monkeypatch.setattr(
        enter_assumption, "_tracked_private_files",
        lambda _dir: [f"{PRIVATE_SUBDIR}/freight.yml"],
    )
    assert _add(["--check"]) == 1
    assert "tracked" in capsys.readouterr().out.lower()


# ---------------------------------------------------------------------------
# 3. The repository matches the contract
# ---------------------------------------------------------------------------
def test_the_private_tier_is_gitignored_in_the_same_style_as_the_book():
    ignored = (REPO / ".gitignore").read_text(encoding="utf-8")
    assert "data/reference/assumptions/private/*.yml" in ignored
    assert "data/reference/assumptions/private/*.yaml" in ignored
    probe = subprocess.run(
        ["git", "check-ignore", "-q", "data/reference/assumptions/private/freight.yml"],
        cwd=REPO, capture_output=True,
    )
    assert probe.returncode == 0, "git does not ignore a private assumption file"


def test_nothing_under_the_private_tier_is_tracked_and_its_readme_is():
    tracked = subprocess.run(
        ["git", "ls-files", "--", "data/reference/assumptions/private"],
        cwd=REPO, capture_output=True, text=True, check=True,
    ).stdout.split()
    assert tracked == ["data/reference/assumptions/private/README.md"]


def test_the_shipped_public_tier_holds_policy_rates_only():
    """No committed file may carry a desk component, empty header files included."""
    assert not (SHIPPED / "freight_and_logistics.yml").exists()
    assert not (SHIPPED / "crush_plant.yml").exists()
    shipped = load_assumptions(SHIPPED, audience=AUDIENCE_PUBLIC)
    assert shipped.assumptions, "the China policy rates must still ship"
    assert {a.component for a in shipped.assumptions} <= POLICY_COMPONENTS
    for entry in shipped.assumptions:
        assert "@" not in entry.entered_by, "a committed policy rate must not carry an email"


def test_the_private_tier_counts_as_a_client_record_directory():
    """The value guard: provenance naming the private dir must not reach a public payload."""
    from analysis.futures.privacy import assert_no_client_records

    with pytest.raises(ClientDataLeak):
        assert_no_client_records(
            {"files": ["data/reference/assumptions/private/freight_and_logistics.yml"]}
        )


# ---------------------------------------------------------------------------
# The page: public withholds, private renders, and the private writer is guarded
# ---------------------------------------------------------------------------
def test_the_public_origins_build_reads_only_the_committed_tier(tmp_path, monkeypatch):
    """Even on a desk machine with freight entered, docs/origins.html stays blocked."""
    from app import origins_page

    _write(tmp_path / "policy.yml", [_duty()])
    _write(tmp_path / PRIVATE_SUBDIR / "freight.yml", [_entry("ocean_freight")])
    monkeypatch.setattr(config, "ASSUMPTIONS_DIR", str(tmp_path))

    seen: dict[str, object] = {}
    real_ranking = origins_page.build_ranking

    def fake_ranking(conn, *, destination_key, window, today, assumptions):
        seen["components"] = {a.component for a in assumptions.assumptions}
        return real_ranking(
            conn, destination_key=destination_key, window=window, today=today,
            assumptions=assumptions,
        )

    monkeypatch.setattr(origins_page, "build_ranking", fake_ranking)
    public = origins_page.build_view(None, today=date(2026, 8, 18))
    assert seen["components"] == {CostComponent.IMPORT_DUTY}
    assert public["is_private"] is False
    assert public["audience"] == AUDIENCE_PUBLIC

    private = origins_page.build_view(None, today=date(2026, 8, 18), audience=AUDIENCE_PRIVATE)
    assert seen["components"] == {CostComponent.IMPORT_DUTY, CostComponent.OCEAN_FREIGHT}
    assert private["is_private"] is True


def test_the_private_origins_edition_cannot_be_written_into_docs():
    from app.origins_page import private_origins_target

    with pytest.raises(ClientDataLeak):
        private_origins_target(REPO / "docs")
    with pytest.raises(ClientDataLeak):
        private_origins_target(REPO / "docs" / "markets")
    target = private_origins_target()
    assert target.name == "origins.html"
    assert (REPO / "docs").resolve() not in target.resolve().parents
    assert target.resolve().parent == Path(config.OPPORTUNITY_PRIVATE_OUTPUT_DIR).resolve()


def test_the_rendered_private_edition_is_labelled_and_the_public_one_is_not(tmp_path, monkeypatch):
    from datetime import datetime, timezone

    from app import origins_page
    from scripts.generate_site import _env

    _write(tmp_path / "policy.yml", [_duty()])
    monkeypatch.setattr(config, "ASSUMPTIONS_DIR", str(tmp_path))
    now = datetime(2026, 8, 18, 21, 0, tzinfo=timezone.utc)

    def render(view: dict) -> str:
        return _env().get_template("origins.html.j2").render(
            origins=view, root="", market_nav=[], current_page="origins",
            current_market=None, day_line="TUESDAY 18 AUGUST 2026",
            generated_at=now.strftime("%Y-%m-%d %H:%M UTC"), generated_at_iso=now.isoformat(),
        )

    public = render(origins_page.build_view(None, today=date(2026, 8, 18)))
    private = render(
        origins_page.build_view(None, today=date(2026, 8, 18), audience=AUDIENCE_PRIVATE)
    )
    assert "PRIVATE EDITION" not in public
    assert "PRIVATE EDITION" in private
