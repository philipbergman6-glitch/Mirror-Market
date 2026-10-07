"""Briefing STORMS section (S2 #374 §9, slice 5 #391).

The six line rules in §9, exercised against the Francine regression fixture
and synthetic storms, plus the parity rule: the flag sentence the briefing
prints is the one ``analysis.hazards.flag_sentence`` builds, so block 06 and
the briefing cannot disagree for the same stored rows.

Readers are stubbed at the section module's seam; nothing here touches the
database or the clock.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import pytest

import config
from analysis import hazards
from analysis.briefing import orchestrator
from analysis.briefing.sections import storms as storms_section
from tests.test_hazards import (
    NHC_OK,
    _iso,
    _status,
    _status_row,
    _storms,
    _synthetic_storm,
    _track,
    _utc,
    francine,
)

NOT_COVERED_LINE = (
    "  Not covered: Paranaguá, Rosario (up-river) — South Atlantic — no publishable cyclone source. "
    "Absence of a flag is not a clear reading."
)


def _freshness(states: dict[str, str | None], last_success: dict[str, str] | None = None) -> pd.DataFrame:
    rows = []
    for layer, state in states.items():
        rows.append({
            "layer_name": layer,
            "status": state,
            "last_success": pd.Timestamp((last_success or {}).get(layer)) if (last_success or {}).get(layer) else pd.NaT,
        })
    return pd.DataFrame(rows, columns=["layer_name", "status", "last_success"])


def _stub(monkeypatch, *, status, storms, track, freshness, now: datetime) -> None:
    monkeypatch.setattr(storms_section, "read_cyclone_status", lambda: status)
    monkeypatch.setattr(storms_section, "read_cyclone_storms", lambda: storms)
    monkeypatch.setattr(storms_section, "read_cyclone_track", lambda: track)
    monkeypatch.setattr(storms_section, "read_freshness", lambda: freshness)
    monkeypatch.setattr(storms_section, "_now", lambda: now)


def _francine_day(monkeypatch, advisory: str = "010", *, nhc_state: str = "success", now_offset_h: int = 2):
    """Francine ``advisory`` from NHC, JTWC answering with zero storms, both layers as given."""
    status, storms, track = francine(advisory)
    checked = _utc(status.iloc[0]["checked_at"])
    jtwc = _status(_status_row("JTWC", checked, 0))
    now = checked + timedelta(hours=now_offset_h)
    _stub(
        monkeypatch,
        status=pd.concat([status, jtwc], ignore_index=True), storms=storms, track=track,
        freshness=_freshness({"cyclones_nhc": nhc_state, "cyclones_jtwc": "success"},
                             {"cyclones_nhc": "2024-09-10 04:00:00", "cyclones_jtwc": _iso(checked)}),
        now=now,
    )
    return status, storms, track, now


# ---------------------------------------------------------------------------
# Rule 6 — no rows at all
# ---------------------------------------------------------------------------
def test_no_rows_at_all_is_no_data(monkeypatch):
    empty = pd.DataFrame()
    _stub(monkeypatch, status=empty, storms=empty, track=empty, freshness=pd.DataFrame(),
          now=_utc("2026-10-07T20:00:00Z"))
    assert storms_section.format() == "STORMS: No data"


# ---------------------------------------------------------------------------
# Rules 1, 2, 4, 5 and the header — the Francine regression fixture
# ---------------------------------------------------------------------------
def test_francine_section_lines(monkeypatch):
    status, storms, track, now = _francine_day(monkeypatch)
    nola = hazards.assess_places(hazards.active_places(config.PLACES, now.date()), config.CYCLONE_BASINS,
                                 status, storms, track, NHC_OK, now)["P-NOLA"]

    text = storms_section.format()
    lines = text.splitlines()

    assert lines[0] == "STORMS (NHC + JTWC, checked 04:00Z 11 Sep):"
    # Rule 1 + parity: the flag line IS the shared flag sentence.
    assert lines[1] == "  " + hazards.flag_sentence(nola)
    assert lines[1].startswith("  New Orleans: Francine (NHC adv 10, Hurricane, category 1, 65 kt) — "
                               "tropical-storm-force winds forecast from Thu 12 Sep 00:00Z")
    assert lines[2] == "    Legs: CBOT board (ZS front), US Gulf CIF (NOLA barge), Dalian No.2 (crush bean)"
    # Rule 2
    assert lines[3] == "  No active storm threatens: North China (Qingdao), Durban"
    # Rule 4
    assert lines[4] == NOT_COVERED_LINE
    # Rule 5
    assert lines[5] == ("  Active storms: Francine (NHC, 65 kt). "
                        "Nearest to a covered port: Francine, 107 km from New Orleans.")
    assert len(lines) == 6
    assert "NOT ASSESSED" not in text


def test_times_are_absolute_utc_never_bare_lead_times(monkeypatch):
    _francine_day(monkeypatch)
    flag_line = storms_section.format().splitlines()[1]
    assert "Z" in flag_line
    assert "+24 h from the" in flag_line  # the lead time is anchored to the advisory, never alone


# ---------------------------------------------------------------------------
# Rule 1 — a watch gets its own line and its legs
# ---------------------------------------------------------------------------
def test_watch_place_prints_the_watch_sentence_and_legs(monkeypatch):
    now = _utc("2026-10-07T20:00:00Z")
    checked = now - timedelta(hours=1)
    # 400 km south of New Orleans at tropical-storm strength, no radii published → watch.
    storm, rows = _synthetic_storm(
        "NHC", "al052026", [{"tau": t, "lat": 26.34, "lon": -90.0619, "vmax": 40} for t in (0, 12, 24)],
        issued_at=checked - timedelta(hours=2), checked_at=checked, name="Watcher",
    )
    _stub(
        monkeypatch,
        status=_status(_status_row("NHC", checked, 1), _status_row("JTWC", checked, 0)),
        storms=_storms(storm), track=_track(*rows),
        freshness=_freshness(NHC_OK), now=now,
    )
    lines = storms_section.format().splitlines()
    watch = hazards.assess_places(hazards.active_places(config.PLACES, now.date()), config.CYCLONE_BASINS,
                                  _status(_status_row("NHC", checked, 1), _status_row("JTWC", checked, 0)),
                                  _storms(storm), _track(*rows), NHC_OK, now)["P-NOLA"]
    assert watch.state == "watch"
    assert lines[1] == "  " + hazards.watch_sentence(watch)
    assert "outside its forecast wind radii" in lines[1]
    assert lines[2] == "    Legs: CBOT board (ZS front), US Gulf CIF (NOLA barge), Dalian No.2 (crush bean)"
    assert lines[3] == "  No active storm threatens: North China (Qingdao), Durban"


# ---------------------------------------------------------------------------
# Rules 2, 4, 5 — a quiet day: both sources answered, nothing listed
# ---------------------------------------------------------------------------
def test_quiet_day_prints_the_clear_line_not_covered_and_none_listed(monkeypatch):
    now = _utc("2026-10-07T20:00:00Z")
    checked = now - timedelta(hours=1)
    _stub(
        monkeypatch,
        status=_status(_status_row("NHC", checked, 0), _status_row("JTWC", checked, 0)),
        storms=_storms(), track=_track(), freshness=_freshness(NHC_OK), now=now,
    )
    lines = storms_section.format().splitlines()
    assert lines == [
        "STORMS (NHC + JTWC, checked 19:00Z 7 Oct):",
        "  No active storm threatens: New Orleans, North China (Qingdao), Durban",
        NOT_COVERED_LINE,
        "  Active storms: none listed.",
    ]


# ---------------------------------------------------------------------------
# Rule 3 — NOT ASSESSED for failed and stale
# ---------------------------------------------------------------------------
def test_failed_layer_is_not_assessed_and_never_clear(monkeypatch):
    _francine_day(monkeypatch, nhc_state="failed")
    text = storms_section.format()
    lines = text.splitlines()
    assert lines[0] == "STORMS (NHC + JTWC, checked 04:00Z 11 Sep):"
    assert "  NOT ASSESSED: New Orleans — NHC layer cyclones_nhc is 'failed', not success; last success 2024-09-10 04:00:00" in lines
    assert "  No active storm threatens: North China (Qingdao), Durban" in lines
    assert "New Orleans:" not in text.replace("NOT ASSESSED: New Orleans", "")
    # NHC is not current, so its storms are not current storms (§2.1).
    assert "  Active storms: none listed." in lines


def test_old_check_is_not_assessed(monkeypatch):
    _francine_day(monkeypatch, now_offset_h=37)
    lines = storms_section.format().splitlines()
    not_assessed = [line for line in lines if line.startswith("  NOT ASSESSED: ")]
    assert len(not_assessed) == 3, lines
    assert all("— last checked Wed 11 Sep 04:00Z" in line for line in not_assessed)
    assert not any(line.startswith("  No active storm threatens") for line in lines)
    assert NOT_COVERED_LINE in lines


def test_absent_track_is_not_assessed_with_the_source_named(monkeypatch):
    now = _utc("2026-10-07T20:00:00Z")
    checked = now - timedelta(hours=1)
    storm, rows = _synthetic_storm("NHC", "al052026", [{"tau": 0, "lat": 30.0, "lon": -90.0, "vmax": 50}],
                                   issued_at=checked - timedelta(hours=2), checked_at=checked,
                                   name="Ghost", track_state="absent")
    _stub(
        monkeypatch,
        status=_status(_status_row("NHC", checked, 1), _status_row("JTWC", checked, 0)),
        storms=_storms(storm), track=_track(*rows), freshness=_freshness(NHC_OK), now=now,
    )
    lines = storms_section.format().splitlines()
    assert "  NOT ASSESSED: New Orleans — NHC lists al052026 but its track product is absent" in lines
    assert "  Active storms: Ghost (NHC, track absent)." in lines


# ---------------------------------------------------------------------------
# Header — two sources checked at different times are both stamped
# ---------------------------------------------------------------------------
def test_header_names_each_check_time_when_they_differ(monkeypatch):
    now = _utc("2026-10-07T20:00:00Z")
    _stub(
        monkeypatch,
        status=_status(_status_row("NHC", now - timedelta(hours=1), 0),
                       _status_row("JTWC", now - timedelta(hours=5), 0)),
        storms=_storms(), track=_track(), freshness=_freshness(NHC_OK), now=now,
    )
    assert storms_section.format().splitlines()[0] == "STORMS (NHC checked 19:00Z 7 Oct, JTWC checked 15:00Z 7 Oct):"


# ---------------------------------------------------------------------------
# Orchestrator wiring (§9)
# ---------------------------------------------------------------------------
def test_storms_follows_weather_and_always_prints():
    order = orchestrator._SECTION_ORDER
    assert order.index("storms") == order.index("weather") + 1
    assert "storms" not in orchestrator._SKIP_WHEN_EMPTY


@pytest.mark.parametrize("line", ["STORMS"])
def test_section_header_is_present_on_an_empty_db(patched_db, line):
    from analysis import loaders
    from analysis.briefing import generate_briefing_data

    loaders.clear_loader_cache()
    data = generate_briefing_data(archive=False)
    assert data.section("storms") == "STORMS: No data"
    assert line in data.text
