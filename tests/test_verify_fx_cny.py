"""AL1 #640: the verifier tracer, CNY/USD vs the ECB cross, at its CLI seam.

Given a saved edition (a rendered page) and a saved ECB document, the CLI
writes these records. The expected values are worked by hand from the fixture:
the ECB printed USD 1.0963 and CNY 7.3063 per EUR for 2026-11-24, so the cross
is 0.1500486 USD per CNY (6.66451 CNY per USD). The page's legs are synthetic:
DCE No.2 3,800, oil 8,600, meal 3,000 CNY/MT.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from verify import fx_cny, record, tolerances

FIXTURES = Path(__file__).parent / "fixtures" / "verify"
ECB_XML = FIXTURES / "ecb_eurofxref_2026-11-10_25.xml"
NOW = datetime(2026, 11, 25, 20, 0, 0, tzinfo=timezone.utc)
REPO = Path(__file__).resolve().parents[1]


def _edition(tmp_path: Path, usd_per_cny: str, cny_per_usd: str, as_of: str) -> Path:
    edition = tmp_path / "edition"
    (edition / "markets").mkdir(parents=True)
    html = (FIXTURES / "dalian_edition.html").read_text(encoding="utf-8")
    html = html.replace("@USD_PER_CNY@", usd_per_cny).replace("@CNY_PER_USD@", cny_per_usd).replace("@FX_AS_OF@", as_of)
    (edition / "markets" / "dalian.html").write_text(html, encoding="utf-8")
    return edition


def _run(tmp_path: Path, edition: Path, *extra: str, now: datetime = NOW) -> tuple[int, list[dict]]:
    out = tmp_path / "records"
    argv = ["--edition", str(edition), "--out", str(out), *extra]
    if "--ecb-url" not in extra and "--ecb-xml" not in extra:
        argv += ["--ecb-xml", str(ECB_XML)]
    code = fx_cny.main(argv, now=now)
    files = sorted(out.rglob("*.jsonl"))
    rows = [row for path in files for row in record.read(path)]
    return code, rows


def _by_unit(rows: list[dict]) -> dict[str, dict]:
    return {row["displayed_unit"]: row for row in rows}


# ---------------------------------------------------------------------------
# One case per grade the CNY path can reach
# ---------------------------------------------------------------------------
def test_a_matching_rate_passes_with_every_field_stated(tmp_path):
    code, rows = _run(tmp_path, _edition(tmp_path, "0.1500", "6.66", "2026-11-24"))
    assert code == 0
    assert len(rows) == 2  # one record per displayed CNY/USD number
    usd = _by_unit(rows)["USD per CNY"]
    assert usd["result"] == "pass"
    assert usd["tier"] == "1" and usd["kind"] == "exact"
    assert usd["transport"] == "independent document"
    assert usd["fx_date"] == usd["observation_date"] == "2026-11-24"
    assert usd["site_edition"] == "2026-11-24T19:24:28+00:00"
    assert usd["page"] == "markets/dalian.html" and len(usd["page_sha256"]) == 64
    assert usd["source_value"] == pytest.approx(0.15004859)  # ECB permits showing it: a derived cross
    assert usd["delta_native"] == pytest.approx(-0.0000486, abs=1e-7)
    # -4.86e-5 USD/CNY on the oil leg's 8,600 CNY/MT is -0.42 USD/MT: against
    # oil's 1.5 that binds less than No.2's -0.18 against 0.5.
    assert usd["delta_usd_mt"] == pytest.approx(-0.1846, abs=1e-3)
    assert usd["threshold"] == 0.5
    assert usd["material"] is False and usd["disposition"] is None
    assert usd["source_vintage"] == "reference date 2026-11-24; publication time unknown"
    assert re.fullmatch(r"verify/fx_cny\.py@[0-9a-f]{40}(\+dirty)?", usd["verifier"])
    assert usd["verified_at"] == "2026-11-25T20:00:00Z"
    assert re.fullmatch(r"v1 sha256:[0-9a-f]{12}", usd["tolerance_version"])
    assert usd["source_url"] == f"saved copy: {ECB_XML.name}"
    assert _by_unit(rows)["CNY per USD"]["result"] == "pass"


def test_a_rate_just_outside_display_rounding_is_an_immaterial_discrepancy(tmp_path):
    # 0.1501 - 0.1500486 = 5.14e-5 > half of 0.0001; on 3,800 CNY/MT that is
    # 0.195 USD/MT, under the 0.5 threshold.
    code, rows = _run(tmp_path, _edition(tmp_path, "0.1501", "6.66", "2026-11-24"))
    assert code == 0
    usd = _by_unit(rows)["USD per CNY"]
    assert usd["result"] == "discrepancy"
    assert usd["material"] is False
    assert usd["disposition"] == "unexplained"
    assert usd["delta_usd_mt"] == pytest.approx(0.1954, abs=1e-3)


def test_a_rate_off_by_a_tenth_of_a_cent_is_a_material_discrepancy(tmp_path):
    # 0.1510 - 0.1500486 = 9.51e-4 USD/CNY x 3,800 CNY/MT = 3.62 USD/MT > 0.5.
    code, rows = _run(tmp_path, _edition(tmp_path, "0.1510", "6.66", "2026-11-24"))
    assert code == 0  # a discrepancy is a graded result, not a crash
    usd = _by_unit(rows)["USD per CNY"]
    assert usd["result"] == "discrepancy"
    assert usd["material"] is True
    assert usd["disposition"] == "unexplained"
    assert usd["delta_usd_mt"] == pytest.approx(3.6154, abs=1e-3)
    assert "DCE Soybean No.2" in usd["conversion"]


def test_another_days_rate_under_this_days_date_is_a_metadata_fail(tmp_path):
    # 0.1498 / 6.68 is the 2026-11-20 (and 2026-11-25) fix, shown as 2026-11-24.
    code, rows = _run(tmp_path, _edition(tmp_path, "0.1498", "6.68", "2026-11-24"))
    assert code == 0
    for row in rows:
        assert row["result"] == "metadata fail"
        assert row["material"] is True  # at any delta
        assert row["disposition"] == "unexplained"
        assert "2026-11-20" in row["explanation"] and "2026-11-25" in row["explanation"]


def test_a_date_the_ecb_published_no_fix_for_is_a_metadata_fail(tmp_path):
    # 2026-11-21 is a Saturday: no TARGET day, so no reference rate exists.
    _code, rows = _run(tmp_path, _edition(tmp_path, "0.1500", "6.66", "2026-11-21"))
    for row in rows:
        assert row["result"] == "metadata fail" and row["material"] is True
        assert "carries no reference date 2026-11-21" in row["explanation"]
        assert row["delta_native"] is None  # nothing to subtract: never a zero


def test_a_date_after_the_document_is_not_yet_published(tmp_path):
    code, rows = _run(tmp_path, _edition(tmp_path, "0.1500", "6.66", "2026-11-26"))
    assert code == 0  # a later run can still resolve it
    assert {row["result"] for row in rows} == {"not yet published"}


def test_a_source_that_is_down_writes_not_checked_and_exits_non_zero(tmp_path):
    missing = (tmp_path / "nowhere" / "eurofxref.xml").as_uri()
    code, rows = _run(tmp_path, _edition(tmp_path, "0.1500", "6.66", "2026-11-24"), "--ecb-url", missing)
    assert code == 1
    assert len(rows) == 2
    for row in rows:
        assert row["result"] == "not checked"
        assert row["explanation"].startswith("source down:")
        assert row["material"] is None and row["delta_native"] is None
        assert row["displayed_value"] in ("0.1500", "6.66")  # what the reader saw is still recorded
        assert row["source_url"] == missing


def test_a_page_without_the_located_block_writes_not_checked(tmp_path):
    edition = _edition(tmp_path, "0.1500", "6.66", "2026-11-24")
    page = edition / "markets" / "dalian.html"
    page.write_text(page.read_text(encoding="utf-8").replace('data-verify-figure="fx-rate"', ""), encoding="utf-8")
    code, rows = _run(tmp_path, edition)
    assert code == 1
    (row,) = rows
    assert row["result"] == "not checked"
    assert "no located CNY/USD rate" in row["explanation"]


def test_hidden_and_script_text_is_never_read_as_the_displayed_number(tmp_path):
    # The fixture's <script> carries "as of 1999-01-01 0.9999 USD per CNY".
    _code, rows = _run(tmp_path, _edition(tmp_path, "0.1500", "6.66", "2026-11-24"))
    assert {row["observation_date"] for row in rows} == {"2026-11-24"}


def test_a_located_leg_whose_label_names_no_product_is_not_checked(tmp_path):
    # Owner 2026-11-26 (#663): a threshold is per product, so a leg the verifier
    # cannot classify is refused, never graded on the default.
    edition = _edition(tmp_path, "0.1500", "6.66", "2026-11-24")
    page_path = edition / "markets" / "dalian.html"
    page_path.write_text(page_path.read_text(encoding="utf-8").replace("DCE Soybean Meal", "DCE Corn"),
                         encoding="utf-8")
    code, rows = _run(tmp_path, edition)
    assert code == 1
    assert {row["result"] for row in rows} == {"not checked"}
    assert all("DCE Corn" in row["explanation"] for row in rows)


# ---------------------------------------------------------------------------
# Records are append-only and never re-graded
# ---------------------------------------------------------------------------
def test_changing_the_tolerance_file_does_not_regrade_existing_records(tmp_path):
    edition = _edition(tmp_path, "0.1501", "6.66", "2026-11-24")
    v1 = tmp_path / "tol-v1.json"
    shutil.copy(tolerances.DEFAULT_PATH, v1)
    _run(tmp_path, edition, "--tolerances", str(v1))
    (first,) = sorted((tmp_path / "records").rglob("*.jsonl"))
    before = first.read_bytes()
    assert _by_unit(record.read(first))["USD per CNY"]["material"] is False

    data = json.loads(v1.read_text(encoding="utf-8"))
    data["version"] = 2
    data["strata"]["6"]["threshold_usd_mt"]["default"] = 0.1
    v2 = tmp_path / "tol-v2.json"
    v2.write_text(json.dumps(data), encoding="utf-8")
    later = NOW.replace(hour=21)
    _run(tmp_path, edition, "--tolerances", str(v2), now=later)

    assert first.read_bytes() == before  # the old record is untouched
    second = next(p for p in (tmp_path / "records").rglob("*.jsonl") if p != first)
    new = _by_unit(record.read(second))["USD per CNY"]
    assert new["tolerance_version"].startswith("v2 ")
    assert new["material"] is True  # the new file grades new records only


def test_a_record_file_is_never_overwritten(tmp_path):
    edition = _edition(tmp_path, "0.1500", "6.66", "2026-11-24")
    _run(tmp_path, edition)
    with pytest.raises(FileExistsError):
        _run(tmp_path, edition)  # the same instant names the same file


def test_a_tolerance_file_missing_a_basket_stratum_is_refused(tmp_path):
    data = json.loads(tolerances.DEFAULT_PATH.read_text(encoding="utf-8"))
    del data["strata"]["10"]
    broken = tmp_path / "tol.json"
    broken.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(tolerances.ToleranceFileError, match="10"):
        tolerances.load(broken)
    code = fx_cny.main(["--edition", str(_edition(tmp_path, "0.1500", "6.66", "2026-11-24")),
                        "--ecb-xml", str(ECB_XML), "--tolerances", str(broken),
                        "--out", str(tmp_path / "records")], now=NOW)
    assert code == 2
    assert not (tmp_path / "records").exists()  # a setup error writes nothing


def test_tolerance_file_v1_carries_the_owner_numbers():
    tol = tolerances.load()
    assert tol.version == 1
    # Owner #477 Q1: 0.5 USD/MT bean-scale and derived, 1.5 oil — in every block that grades.
    for key in ("1", "1b", "2", "3", "4", "5", "6", "7", "8", "9", "10"):
        block = tol.stratum(key)["threshold_usd_mt"]
        values = block.values() if isinstance(block, dict) else [block]
        assert {v for v in values if isinstance(v, (int, float))} <= {0.5, 1.5}, key
    fx = tol.for_series("6", "CNY/USD")
    assert tol.threshold_usd_mt(fx, tol.product_of("DCE Soybean Oil")) == 1.5
    assert tol.threshold_usd_mt(fx, tol.product_of("DCE Soybean Meal")) == 0.5
    assert tol.native_allowance(fx, 4) == pytest.approx(0.00005)
    cbot = tol.stratum("1")
    assert cbot["kind"] == "exact" and cbot["native_tolerance"]["value"] == 0
    assert cbot["native_tolerance"]["unit"] == "ticks"
    assert tol.stratum("3u")["kind"] == "unverifiable"
    assert tol.for_series("6", "CNY/USD")["kind"] == "exact"
    assert tol.for_series("6", "BRL/USD")["kind"] == "band"


def test_the_module_entry_point_runs_the_check(tmp_path):
    edition = _edition(tmp_path, "0.1500", "6.66", "2026-11-24")
    out = tmp_path / "records"
    done = subprocess.run(
        [sys.executable, "-m", "verify", "fx-cny", "--edition", str(edition), "--ecb-xml", str(ECB_XML),
         "--out", str(out)],
        cwd=REPO, capture_output=True, text=True, check=False,
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout.count("pass") == 2
    assert len(list(out.rglob("*.jsonl"))) == 1


@pytest.mark.network
def test_live_ecb_document_parses_and_carries_cny(tmp_path):
    from verify import ecb

    rates = ecb.parse(ecb.fetch())
    assert "CNY" in rates.by_date[rates.last] and "USD" in rates.by_date[rates.last]


def test_the_grade_reads_the_tolerance_file_not_a_constant(tmp_path):
    # Widening stratum 6's native tolerance by 0.001 USD per CNY (an edit
    # nobody should make without evidence) turns the material 0.1510 into a pass:
    # proof that the file, not the code, decides.
    data = json.loads(tolerances.DEFAULT_PATH.read_text(encoding="utf-8"))
    data["strata"]["6"]["native_tolerance"]["value"] = 0.001
    wide = tmp_path / "tol.json"
    wide.write_text(json.dumps(data), encoding="utf-8")
    _code, rows = _run(tmp_path, _edition(tmp_path, "0.1510", "6.66", "2026-11-24"), "--tolerances", str(wide))
    assert _by_unit(rows)["USD per CNY"]["result"] == "pass"


def test_a_page_locating_the_rate_under_another_stratum_is_not_checked(tmp_path):
    edition = _edition(tmp_path, "0.1500", "6.66", "2026-11-24")
    page = edition / "markets" / "dalian.html"
    page.write_text(page.read_text(encoding="utf-8").replace('data-verify-stratum="6"', 'data-verify-stratum="2"'),
                    encoding="utf-8")
    code, rows = _run(tmp_path, edition)
    assert code == 1
    assert {row["result"] for row in rows} == {"not checked"}
    assert "under stratum 2" in rows[0]["explanation"]


# ---------------------------------------------------------------------------
# Review of #663 (wave 6b): a wrong FX date is proved, never inferred from
# display rounding; a stale document is not an unpublished fix
# ---------------------------------------------------------------------------


def test_a_two_dp_rounding_miss_on_a_date_the_block_confirms_is_a_discrepancy_not_a_wrong_date(tmp_path):
    # 0.1500 matches the 2026-11-24 cross (0.1500486) at 4 dp, so the block's one
    # "as of" date is right. 6.67 misses 6.66451 by 0.0055 — just past half a cent —
    # and at 2 dp also lies within rounding of the 11-17/18/19 fixes. That is not
    # evidence of another date: one caption dates both cards, and the other card
    # proves it. Graded on economics: −0.47 USD/MT on DCE No.2, under 0.5.
    code, rows = _run(tmp_path, _edition(tmp_path, "0.1500", "6.67", "2026-11-24"))
    by_unit = _by_unit(rows)
    assert by_unit["USD per CNY"]["result"] == "pass"
    cny = by_unit["CNY per USD"]
    assert cny["result"] == "discrepancy"
    assert cny["material"] is False
    assert cny["delta_usd_mt"] == pytest.approx(-0.4695, abs=1e-3)
    assert "not a wrong FX date" in cny["explanation"]
    assert code == 0


def _ecb_ending(tmp_path: Path, last: str) -> Path:
    """The fixture document with every reference date after ``last`` removed."""
    lines = ECB_XML.read_text(encoding="utf-8").splitlines(keepends=True)
    kept = [line for line in lines if not (m := re.match(r'<Cube time="(\d{4}-\d{2}-\d{2})">', line)) or m.group(1) <= last]
    text = "".join(kept)
    assert max(re.findall(r'<Cube time="(\d{4}-\d{2}-\d{2})">', text)) == last
    path = tmp_path / f"ecb_ending_{last}.xml"
    path.write_text(text, encoding="utf-8")
    return path


def test_a_document_ending_before_a_fix_that_was_due_is_stale_not_unpublished(tmp_path):
    # The ECB posts each TARGET day's fix around 16:00 CET. At 2026-11-25 20:00 UTC
    # the 2026-11-24 fix is a day overdue, so a document ending 2026-11-20 is a
    # stale copy (or a cached one), not a publisher that has not posted yet.
    stale = _ecb_ending(tmp_path, "2026-11-20")
    code, rows = _run(tmp_path, _edition(tmp_path, "0.1500", "6.66", "2026-11-24"), "--ecb-xml", str(stale))
    assert {row["result"] for row in rows} == {"not checked"}
    assert all("stale" in row["explanation"] for row in rows)
    assert code == 1  # a stale source is loud, like a source that is down


def test_a_fix_not_yet_due_is_still_not_yet_published(tmp_path):
    # The same document, read before 2026-11-24's fix was due (11-24 12:00 UTC).
    stale = _ecb_ending(tmp_path, "2026-11-20")
    code, rows = _run(
        tmp_path, _edition(tmp_path, "0.1500", "6.66", "2026-11-24"), "--ecb-xml", str(stale),
        now=datetime(2026, 11, 24, 12, 0, 0, tzinfo=timezone.utc),
    )
    assert {row["result"] for row in rows} == {"not yet published"}
    assert code == 0
