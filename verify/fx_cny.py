"""Stratum 6, CNY: the displayed CNY/USD rate vs the ECB reference-rate cross (AL1 #640).

Tier 1, exact. Since B25 #495 the ECB is CNY's publisher of record, so the
displayed rate and the ECB cross for the displayed date share one definition.
The only allowance is our display rounding (half a unit in the last place
shown). A delta outside it is graded on the legs the rate moves — every price
on the page whose visible home print is in CNY/MT — against each leg's
economic threshold (0.5 USD/MT, oil 1.5).

A displayed value that misses its own date's fix but matches another day's is a
**metadata fail** (wrong FX date), material at any delta, and so is a date the
ECB published no fix for.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from verify import ecb, page, record, tolerances
from verify.grade import Impact, grade_exact, within

SCRIPT = "verify/fx_cny.py"
STRATUM = "6"
SERIES = "CNY/USD"
FOREIGN = "CNY"
PUBLISHER = "ecb"
DEFAULT_PAGE = "markets/dalian.html"
DEFAULT_OUT = Path("verification/records")
# How far either side of the displayed date a matching fix is looked for when
# naming a wrong FX date. A1's prior-close cap is 3 calendar days; a week
# covers it across a weekend.
MATCH_WINDOW_DAYS = 7
TRANSPORT = "independent document"
SOURCE_IDENTITY = (
    f"{ecb.IDENTITY}: USD and CNY per EUR on one reference date, derived cross "
    "(the product's fetcher reads the ECB SDMX API, a different document)"
)


class SetupError(RuntimeError):
    """Nothing can be graded: the run itself is misconfigured (exit 2, no record)."""


@dataclass(frozen=True)
class _Context:
    page: str
    page_sha256: str
    source_url: str
    tolerances: tolerances.Tolerances
    verifier: str
    verified_at: str
    ledger_edition: str | None


def verifier_identity() -> str:
    """``verify/fx_cny.py@<commit>`` — ``+dirty`` when the verifier differs from that commit."""
    here = Path(__file__).resolve().parent
    try:
        sha = subprocess.run(
            ["git", "-C", str(here), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "-C", str(here), "status", "--porcelain", "--", str(here)],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        sha, dirty = os.environ.get("GITHUB_SHA", "").strip(), ""
    if not sha:
        raise SetupError("cannot name the verifier's commit (no git, no GITHUB_SHA): a record must be re-checkable")
    return f"{SCRIPT}@{sha}{'+dirty' if dirty else ''}"


def _base(ctx: _Context, reading: page.FxBlockReading | None, figure: page.Figure | None, **fields) -> record.Record:
    defaults: dict = {
        "ledger_edition": ctx.ledger_edition,
        "site_edition": reading.site_edition if reading else None,
        "page": ctx.page,
        "page_sha256": ctx.page_sha256,
        "stratum": STRATUM,
        "series": SERIES,
        "displayed_value": figure.text if figure else None,
        "displayed_unit": figure.unit if figure else None,
        "observation_date": reading.observation_date if reading else None,
        "contract_or_window": None,
        "fx_date": reading.observation_date if reading else None,
        "event_class": "ordinary",
        "tier": "1",
        "kind": "exact",
        "source_identity": SOURCE_IDENTITY,
        "source_url": ctx.source_url,
        "source_vintage": None,
        "transport": TRANSPORT,
        "source_value": None,
        "source_unit": figure.unit if figure else None,
        "conversion": None,
        "delta_native": None,
        "delta_usd_mt": None,
        "tolerance": None,
        "threshold": None,
        "band": None,
        "tolerance_version": ctx.tolerances.stamp,
        "result": "not checked",
        "material": None,
        "disposition": None,
        "explanation": None,
        "owner": None,
        "issue": None,
        "verifier": ctx.verifier,
        "verified_at": ctx.verified_at,
        "publisher": PUBLISHER,
    }
    defaults.update(fields)
    return record.Record(**defaults)


def _not_checked(ctx, reading, figures, reason: str) -> list[record.Record]:
    targets = figures or [None]
    return [_base(ctx, reading, figure, result="not checked", explanation=reason) for figure in targets]


def _cross(rates: ecb.ReferenceRates, day: date, figure: page.Figure) -> tuple[float, str] | None:
    legs = rates.by_date.get(day)
    if legs is None or "USD" not in legs or FOREIGN not in legs:
        return None
    usd, cny = legs["USD"], legs[FOREIGN]
    if figure.unit == f"USD per {FOREIGN}":
        return usd / cny, f"ECB USD {usd:g} ÷ {FOREIGN} {cny:g} per EUR ({day.isoformat()})"
    return cny / usd, f"ECB {FOREIGN} {cny:g} ÷ USD {usd:g} per EUR ({day.isoformat()})"


def _usd_per_foreign(figure: page.Figure, value: float) -> float:
    return value if figure.unit == f"USD per {FOREIGN}" else 1.0 / value


def _matching_days(rates, figure, around: date, tolerance: float) -> list[str]:
    days = []
    for offset in range(-MATCH_WINDOW_DAYS, MATCH_WINDOW_DAYS + 1):
        day = around + timedelta(days=offset)
        cross = _cross(rates, day, figure) if day != around else None
        if cross is not None and within(figure.value - cross[0], tolerance):
            days.append(day.isoformat())
    return days


def _grade_figure(ctx, reading: page.FxBlockReading, figure: page.Figure, rates, legs) -> record.Record:
    shown = date.fromisoformat(reading.observation_date)
    half_step = 0.5 * 10 ** -figure.decimals
    tolerance_text = (
        f"±{half_step:g} {figure.unit} (display rounding at {figure.decimals} dp; native 0: "
        "the product strikes the same ECB cross)"
    )
    graded: dict[str, object] = {"tolerance": tolerance_text}

    if shown > rates.last:
        return _base(ctx, reading, figure, **graded, result="not yet published",
                     source_vintage=f"document ends at reference date {rates.last}; {ecb.VINTAGE_NOTE}",
                     explanation=f"the ECB document ends at {rates.last}; the page shows a rate dated {shown}")
    if shown < rates.first:
        return _base(ctx, reading, figure, **graded, result="not checked",
                     explanation=f"the ECB document covers {rates.first}..{rates.last}, not {shown}")

    cross = _cross(rates, shown, figure)
    if cross is None:
        elsewhere = _matching_days(rates, figure, shown, half_step)
        found = f"; the displayed value matches the ECB fix of {', '.join(elsewhere)}" if elsewhere else ""
        return _base(ctx, reading, figure, **graded, result="metadata fail", material=True,
                     disposition="unexplained",
                     source_vintage=f"no reference date {shown} in the document; {ecb.VINTAGE_NOTE}",
                     explanation=f"wrong FX date: the ECB published no USD/{FOREIGN} reference rate for {shown} "
                                 f"(not a TARGET day){found}")

    source, legs_text = cross
    delta = figure.value - source
    shift = _usd_per_foreign(figure, figure.value) - _usd_per_foreign(figure, source)
    impacts = [
        Impact(leg=leg.label, delta_usd_mt=leg.home_value * shift,
               threshold_usd_mt=ctx.tolerances.threshold_usd_mt(leg.label))
        for leg in legs
    ]
    grade = grade_exact(delta, half_step, impacts)
    graded.update(
        source_value=round(source, 8),
        source_vintage=f"reference date {shown}; {ecb.VINTAGE_NOTE}",
        conversion=(f"{legs_text}; USD/MT effect = Δ USD per {FOREIGN} × the displayed {FOREIGN}/MT print of "
                    f"{grade.binding.leg} (the leg it moves furthest against its threshold)"),
        delta_native=round(delta, 10),
        delta_usd_mt=round(grade.binding.delta_usd_mt, 4),
        threshold=grade.binding.threshold_usd_mt,
    )
    if grade.result == "pass":
        return _base(ctx, reading, figure, **graded, result="pass", material=False)

    elsewhere = _matching_days(rates, figure, shown, half_step)
    if elsewhere:
        return _base(ctx, reading, figure, **graded, result="metadata fail", material=True,
                     disposition="unexplained",
                     explanation=f"wrong FX date: the displayed value misses the ECB fix of {shown}, the date "
                                 f"the page shows, and matches the fix of {', '.join(elsewhere)}")
    where = "above" if grade.material else "within"
    return _base(ctx, reading, figure, **graded, result="discrepancy", material=grade.material,
                 disposition="unexplained",
                 explanation=f"outside display rounding; {where} the economic threshold on {grade.binding.leg}")


def check(
    edition: Path,
    page_name: str,
    *,
    ecb_document: bytes | None,
    ecb_url: str,
    tolerances_path: Path,
    now: datetime,
    ledger_edition: str | None = None,
) -> list[record.Record]:
    """Grade every displayed CNY/USD rate on one page of a built edition."""
    page_path = edition / page_name
    if not page_path.is_file():
        raise SetupError(f"{page_path} is not a file: point --edition at a built site")
    try:
        tol = tolerances.load(tolerances_path)
        tol.for_series(STRATUM, SERIES)
    except (OSError, tolerances.ToleranceFileError) as exc:
        raise SetupError(f"tolerance file: {exc}") from exc
    html = page_path.read_bytes()
    ctx = _Context(
        page=page_name,
        page_sha256=hashlib.sha256(html).hexdigest(),
        source_url=ecb_url,
        tolerances=tol,
        verifier=verifier_identity(),
        verified_at=now.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        ledger_edition=ledger_edition,
    )

    try:
        reading = page.read_fx_block(html.decode("utf-8"), series=SERIES)
    except (page.PageReadError, UnicodeDecodeError) as exc:
        return _not_checked(ctx, None, [], f"page could not be read: {exc}")
    figures = list(reading.figures)
    bad_units = [f.unit for f in figures if f.unit not in (f"USD per {FOREIGN}", f"{FOREIGN} per USD")]
    if bad_units:
        return _not_checked(ctx, reading, figures, f"the {SERIES} block states units {bad_units}, not CNY/USD")

    legs = [leg for leg in reading.legs if leg.home_unit == f"{FOREIGN}/MT"]
    if not legs:
        return _not_checked(ctx, reading, figures,
                            f"no leg on the page shows a {FOREIGN}/MT print, so the USD/MT effect of a delta "
                            "cannot be computed")
    try:
        document = ecb_document if ecb_document is not None else ecb.fetch(ecb_url)
        rates = ecb.parse(document)
    except ecb.SourceUnavailable as exc:
        return _not_checked(ctx, reading, figures, f"source down: {exc}")
    except ecb.EcbDocumentError as exc:
        return _not_checked(ctx, reading, figures, f"source unreadable: {exc}")
    return [_grade_figure(ctx, reading, figure, rates, legs) for figure in figures]


def main(argv: Sequence[str] | None = None, *, now: datetime | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m verify fx-cny", description=__doc__.splitlines()[0])
    parser.add_argument("--edition", type=Path, required=True, help="root of a built site (e.g. docs/)")
    parser.add_argument("--page", default=DEFAULT_PAGE, help=f"page under the edition (default {DEFAULT_PAGE})")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--ecb-xml", type=Path, help="a saved eurofxref XML instead of the live document")
    source.add_argument("--ecb-url", default=ecb.HIST_90D_URL, help="the ECB document to fetch")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help=f"records directory (default {DEFAULT_OUT})")
    parser.add_argument("--tolerances", type=Path, default=tolerances.DEFAULT_PATH)
    parser.add_argument("--ledger-edition", default=None, help="L-YYYY-MM-DD-n, when run inside a ledger pass")
    args = parser.parse_args(argv)

    now = now or datetime.now(timezone.utc)
    try:
        document = args.ecb_xml.read_bytes() if args.ecb_xml else None
        url = f"saved copy: {args.ecb_xml.name}" if args.ecb_xml else args.ecb_url
        records = check(args.edition, args.page, ecb_document=document, ecb_url=url,
                        tolerances_path=args.tolerances, now=now, ledger_edition=args.ledger_edition)
    except (SetupError, OSError) as exc:
        print(f"verify fx-cny: {exc}", file=sys.stderr)
        return 2
    stamp = now.astimezone(timezone.utc)
    path = record.write(records, args.out / stamp.strftime("%Y-%m-%d"), f"{stamp:%Y%m%dT%H%M%S%fZ}-fx-cny")
    for row in record.read(path):
        print(f"{row['result']:<17} {row['displayed_value'] or '—':>8} {row['displayed_unit'] or ''} "
              f"vs ECB {row['source_value'] if row['source_value'] is not None else '—'} "
              f"Δ {row['delta_native']} · {row['delta_usd_mt']} USD/MT"
              f"{' · ' + row['explanation'] if row['explanation'] else ''}")
    print(f"wrote {path}")
    return 1 if any(r.result == "not checked" for r in records) else 0
