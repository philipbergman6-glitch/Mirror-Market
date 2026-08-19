"""A worked example of the whole desk workflow, on a synthetic position.

Everything printed below is computed by the same modules the workstation page
calls — not by a separate demo path — against the synthetic desk in
``tests/book_fixtures.py``. The fixture backs both, so this cannot drift away
from what the tests actually verify, and every account string in it carries the
marker ``SYNTHETIC`` so nothing here can be mistaken for somebody's book.

    python scripts/worked_book_example.py

It walks the seven questions in order:

    1  the position          what was entered, and how it is priced
    2  hedge sizing          tonnes to lots, in a named month, with the residual
    3  the marked book       average cost, mark, realised and unrealised P&L
    4  exposure              flat price, basis, crush, FX, month, notice, residual
    5  desk limits           every line, its headroom, and what is crossed
    6  scenario P&L          futures, basis and FX moved together
    7  clearing              the official figure beside ours, never merged
    8  option Greeks         a broker's quote, valued, with the model's limits

Nothing here is routed anywhere, and the numbers are marks on delayed closes —
a management estimate, not a margin call.
"""

from __future__ import annotations

import sqlite3
import sys
from datetime import timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "tests"))

from book_fixtures import (  # noqa: E402
    ACCOUNT,
    BROKER,
    PRICES,
    TODAY,
    fx_for,
    quote_for,
    synthetic_book,
)

from analysis.futures import scenarios as scenarios_mod  # noqa: E402
from analysis.futures.clearing import ClearingLine, ClearingStatement, reconcile  # noqa: E402
from analysis.futures.curve import analyse_curve  # noqa: E402
from analysis.futures.domain import Side, named_contract  # noqa: E402
from analysis.futures.exposure import build_exposure  # noqa: E402
from analysis.futures.hedge import (  # noqa: E402
    BasisConvention,
    PhysicalExposure,
    PhysicalUnit,
    Rounding,
    fx_exposure_from_rate,
    propose_hedge,
)
from analysis.futures.options import (  # noqa: E402
    BLACK76_LIMITATIONS,
    ManualLadder,
    ManualQuote,
    OptionContract,
    OptionRight,
    OptionStyle,
    value_manual_ladder,
)
from analysis.futures.positions import BookKind, value_book  # noqa: E402
from analysis.futures.providers import open_provider  # noqa: E402
from pipeline import schema  # noqa: E402

RULE = "═" * 78
THIN = "─" * 78
OPTION_RATE = 0.04


def head(number: int, title: str) -> None:
    print()
    print(RULE)
    print(f"  {number}.  {title.upper()}")
    print(RULE)


def curve_db() -> sqlite3.Connection:
    """One session of the three soy legs, so the real provider path is used."""
    conn = sqlite3.connect(":memory:")
    for ddl in schema.ALL_SCHEMAS:
        conn.execute(ddl)
    conn.executemany(
        "INSERT INTO forward_curve (commodity, contract_month, label, ticker, close, "
        "observation_date, volume, open_interest, fetched_date) "
        f"VALUES (?,?,?,?,?,'{TODAY}',4210,NULL,'{TODAY}')",
        [
            ("Soybeans", "2026-11-01", "Nov 2026", "ZSX26.CBT", PRICES["ZSX26"]),
            ("Soybeans", "2027-01-01", "Jan 2027", "ZSF27.CBT", PRICES["ZSF27"]),
            ("Soybean Meal", "2026-12-01", "Dec 2026", "ZMZ26.CBT", PRICES["ZMZ26"]),
            ("Soybean Oil", "2026-12-01", "Dec 2026", "ZLZ26.CBT", PRICES["ZLZ26"]),
        ],
    )
    conn.commit()
    return conn


def main() -> int:
    conn = curve_db()
    provider = open_provider(conn)
    book = synthetic_book()
    valuation = value_book(book, as_of=TODAY, quote_for=quote_for(), fx_for=fx_for())

    print(RULE)
    print("  MIRROR MARKET — WORKED SYNTHETIC POSITION")
    print(f"  as of {TODAY} · account {ACCOUNT} · all figures synthetic")
    print(RULE)

    # ------------------------------------------------------------------ 1 --
    head(1, "the position, as entered")
    for position in book.physical:
        print(
            f"  physical  {position.commodity:14} {position.side.value:5} "
            f"{position.quantity:>10,.0f} {position.unit.value:3} @ "
            f"{position.average_cost_usd_mt:>8,.2f} {position.currency}/MT "
            f"· {position.basis_convention.value} · {position.location}"
        )
    for position in book.futures:
        lot = position.lot
        source = "from fills" if lot.derived else "stated, not derived"
        print(
            f"  futures   {position.contract.symbol:14} "
            f"{lot.net_quantity:>+10,.0f} lots @ "
            f"{(lot.average_cost or 0):>8,.2f} · {source}"
        )

    # ------------------------------------------------------------------ 2 --
    head(2, "hedge sizing")
    exposure = PhysicalExposure(
        commodity="Soybeans",
        side=Side.LONG,
        quantity=12_000.0,
        unit=PhysicalUnit.METRIC_TON,
        pricing_start=TODAY,
        pricing_end=TODAY + timedelta(days=75),
        basis_convention=BasisConvention.BASIS_OVER_FUTURES,
        basis_usd_per_mt=-12.5,
        basis_source="Paranagua FOB indication",
        note="SYNTHETIC worked example",
    )
    analysis = analyse_curve(provider.curve("Soybeans", as_of=TODAY), as_of=TODAY)
    proposal = propose_hedge(
        exposure, analysis, as_of=TODAY,
        fx=fx_exposure_from_rate(exposure, analysis.front_price_usd_mt, None),
        rounding=Rounding.NEAREST,
    )
    payload = proposal.to_dict()
    for leg in payload["legs"]:
        print(
            f"  {leg['side']:5} {abs(leg['contracts']):>4,.0f} × {leg['symbol']:8} "
            f"({leg['label']}) = {leg['covered_physical_mt']:>10,.1f} MT covered "
            f"@ {leg['reference_price']:,.2f} ({leg['reference_price_label']})"
        )
        print(
            f"        {leg['mt_per_contract']:,.4f} MT/contract · last trade "
            f"{leg['last_trade'] or 'rule not encoded'} · first notice "
            f"{leg['first_notice'] or 'rule not encoded'}"
        )
    print(f"  exposure                     {payload['exposure']['quantity_mt']:>10,.1f} MT")
    print(f"  coverage                     {payload['coverage_pct']:>10,.1f} %")
    print(f"  residual (unhedged)          {payload['residual_mt']:>10,.1f} MT")
    print(THIN)
    print("  A hedge does not remove risk; it exchanges flat price for basis. Section 4")
    print("  shows where those tonnes went.")

    # ------------------------------------------------------------------ 3 --
    head(3, "the marked book — management basis")
    print(
        f"  {'position':28} {'net':>10} {'avg cost':>10} {'mark':>10} "
        f"{'unrealised':>12} {'realised':>11}"
    )
    for marked in valuation.positions:
        print(
            f"  {marked.key:28} {marked.net_quantity:>+10,.0f} "
            f"{(marked.average_cost or 0):>10,.2f} {(marked.mark or 0):>10,.2f} "
            f"{(marked.unrealised_usd or 0):>+12,.0f} {marked.realised_usd:>+11,.0f}"
        )
    print(THIN)
    print(f"  total unrealised {valuation.total_unrealised_usd:>+15,.0f} USD")
    print(f"  total realised   {valuation.total_realised_usd:>+15,.0f} USD")
    print(f"  {valuation.mark_note}")

    # ------------------------------------------------------------------ 4 --
    head(4, "exposure")
    report = build_exposure(book, valuation, as_of=TODAY)
    print(
        f"  {'view':16} {'scope':22} {'MT':>10} {'lots':>6} {'USD/unit move':>14}  per"
    )
    for line in report.lines:
        per = (
            "unmeasured" if line.usd_per_unit_move is None
            else f"{line.usd_per_unit_move:+,.0f}"
        )
        mt = "—" if line.quantity_mt is None else f"{line.quantity_mt:+,.0f}"
        lots = "—" if line.contracts is None else f"{line.contracts:+,.0f}"
        print(
            f"  {line.view.value:16} {line.key:22} {mt:>10} {lots:>6} {per:>14}  "
            f"{line.unit_move_label}"
        )
    for line in report.lines:
        for warning in line.warnings:
            print(f"    ! {line.key}: {warning}")

    # ------------------------------------------------------------------ 5 --
    head(5, "desk limits — reported, never enforced")
    print(f"  {'limit':22} {'scope':12} {'observed':>12} {'maximum':>12} {'headroom':>12}  status")
    for check in valuation.limit_checks:
        print(
            f"  {check.limit.key:22} {check.scope_key:12} {check.observed:>+12,.0f} "
            f"{check.limit.maximum:>12,.0f} {check.headroom:>+12,.0f}  {check.status.value}"
        )
    if valuation.breaches:
        print(THIN)
        for breach in valuation.breaches:
            print(
                f"  BREACH  {breach.limit.key} for {breach.scope_key}: over by "
                f"{breach.excess:,.0f} {breach.limit.unit}"
            )
    else:
        print("  no limit crossed")

    # ------------------------------------------------------------------ 6 --
    head(6, "scenario P&L")
    results = scenarios_mod.run_panel(proposal, scenarios_mod.default_panel_for(proposal))
    print(
        f"  {'scenario':30} {'physical':>13} {'futures':>13} {'net':>13} {'eff %':>7}"
    )
    for result in results:
        row = result.to_dict()
        effectiveness = row.get("hedge_effectiveness_pct")
        eff = "—" if effectiveness is None else f"{effectiveness:,.1f}"
        print(
            f"  {row['scenario']['name'][:30]:30} {row['physical_pnl_usd']:>+13,.0f} "
            f"{row['futures_pnl_usd']:>+13,.0f} {row['net_pnl_usd']:>+13,.0f} {eff:>7}"
        )

    # ------------------------------------------------------------------ 7 --
    head(7, "clearing reconciliation — two bases, never merged")
    futures_marks = [p for p in valuation.positions if p.kind is BookKind.FUTURES]
    statement = ClearingStatement(
        account=ACCOUNT,
        broker=BROKER,
        statement_date=TODAY,
        lines=tuple(
            ClearingLine(
                symbol=marked.key,
                description=f"{marked.commodity.upper()} {marked.key}",
                quantity=marked.net_quantity,
                settlement_price=(marked.mark or 0.0) + 0.25,
                realised_usd=marked.realised_usd,
                # The clearer settled a quarter-cent away from our delayed
                # close. That difference is the whole point of the section.
                unrealised_usd=(marked.unrealised_usd or 0.0) - 850.0,
            )
            for marked in futures_marks
        ),
    )
    result = reconcile(valuation, statement)
    print(f"  {'contract':10} {'their lots':>11} {'ours':>7} {'their P&L':>13} {'ours':>13} {'diff':>11}  agrees")
    for row in result.rows:
        diff = "—" if row.difference_usd is None else f"{row.difference_usd:+,.0f}"
        agrees = "unknown" if row.agrees is None else ("yes" if row.agrees else "NO")
        print(
            f"  {row.key:10} {(row.official_quantity or 0):>+11,.0f} "
            f"{(row.management_quantity or 0):>+7,.0f} "
            f"{(row.official_unrealised_usd or 0):>+13,.0f} "
            f"{(row.management_unrealised_usd or 0):>+13,.0f} {diff:>11}  {agrees}"
        )
    print(THIN)
    print(f"  {result.summary}")
    for note in result.notes:
        print(f"  {note}")

    # ------------------------------------------------------------------ 8 --
    head(8, "option Greeks — a broker's quote, valued")
    ladder = ManualLadder(quotes=(
        ManualQuote(
            contract=OptionContract(
                underlying=named_contract("Soybeans", 2026, 11),
                right=OptionRight.CALL,
                strike=1200.0,
                expiry=TODAY + timedelta(days=65),
                style=OptionStyle.AMERICAN,
            ),
            source=f"{BROKER} desk quote",
            quoted_on=TODAY,
            implied_volatility=0.185,
        ),
    ))
    rows = value_manual_ladder(
        ladder, as_of=TODAY, forwards={"ZSX26": PRICES["ZSX26"]}, rate=OPTION_RATE,
    )
    for row in rows:
        if not row.get("valued"):
            print(f"  not valued — {row['reason']}")
            continue
        greeks = row["greeks"]
        print(f"  {row['contract']['symbol']}  strike {row['contract']['strike']:g}  "
              f"expiry {row['contract']['expiry']}")
        print(f"    forward          {row['forward']:>12,.2f}")
        print(f"    volatility       {row['volatility'] * 100:>11,.1f} %  ({row['volatility_derived_from']})")
        print(f"    premium          {row['premium']:>12,.3f}  = {row['premium_usd']:>10,.0f} USD/contract")
        print(f"    delta            {greeks['delta']:>+12,.4f}")
        print(f"    gamma            {greeks['gamma']:>+12,.6f}")
        print(f"    vega / vol pt    {greeks['vega_per_vol_point']:>+12,.4f}")
        print(f"    theta / day      {greeks['theta_per_day']:>+12,.4f}")
        print(f"    rho / rate pt    {greeks['rho_per_rate_point']:>+12,.4f}")
    print(THIN)
    print("  Black-76 is wrong about this option in stated ways:")
    for limit in BLACK76_LIMITATIONS[:3]:
        print(f"    · [{limit.direction}] {limit.why}")

    print()
    print(RULE)
    print("  PROPOSAL — NOT ROUTED. This project has no connection to any venue,")
    print("  broker or clearing system, and every figure above is synthetic.")
    print(RULE)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
