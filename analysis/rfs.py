"""
The RFS read for the soy-oil demand block (Layer 29, #353).

Two questions, both about the policy lever behind soybean oil's share of
crush value:

1.  **Is D4 generation running ahead of the biomass-based-diesel obligation?**
    Year-to-date D4 RINs against the final BBD volume for the same compliance
    year, pro-rated straight-line by months elapsed. The yardstick is ours,
    not EPA's — RINs are not generated evenly across a year — and it is
    labelled as such. It is computed only for a year whose BBD obligation is
    stored in RINs (2026 on: see config.EPA_RFS_RVO_REFERENCE); for any other
    year the pace is withheld with a reason, never approximated.

2.  **What is a D4 credit worth against a D6 one?** Both on the newest
    transfer week where *both* printed (one week's number — invariant 8), and
    their spread. In USD per RIN: there is no USD/MT here.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from pipeline.query import read_rfs_rvo, read_rin_generation, read_rin_prices

BBD = "Biomass-based diesel"
# Weeks back for the "vs 4 weeks ago" read; the row must exist on exactly
# that week for both legs, or the change is withheld.
_LOOKBACK_WEEKS = 4
# Weeks of D4/D6 history handed to the chart.
CHART_WEEKS = 104


def _generation(rvo: pd.DataFrame) -> dict[str, Any] | None:
    gen = read_rin_generation("D4")
    if gen.empty:
        return None
    gen = gen.sort_values("Date")
    latest = gen.iloc[-1]
    year = latest["Date"].year
    months = latest["Date"].month
    ytd = float(gen.loc[gen["Date"].dt.year == year, "rins"].sum())

    prior = gen[gen["Date"] == latest["Date"] - pd.DateOffset(years=1)]
    yoy = None
    if not prior.empty and prior.iloc[0]["rins"]:
        yoy = float((latest["rins"] / prior.iloc[0]["rins"] - 1) * 100)

    out: dict[str, Any] = {
        "year": year,
        "through": latest["Date"].strftime("%b %Y"),
        "months": months,
        "latest_month_rins": float(latest["rins"]),
        "latest_month_yoy_pct": yoy,
        "latest_preliminary": bool(latest["preliminary"]),
        "ytd_rins": ytd,
        "obligation_rins": None,
        "pace_rins": None,
        "pace_pct": None,
        "pace_reason": "",
        "citation": None,
    }

    bbd = rvo[(rvo["compliance_year"] == year) & (rvo["category"] == BBD)] if not rvo.empty else rvo
    if bbd.empty or bbd.iloc[0]["unit"] != "RINs" or bbd.iloc[0]["rule_status"] != "final":
        out["pace_reason"] = (
            f"no final {year} biomass-based-diesel obligation in RINs is stored, "
            "so D4 generation has nothing like-for-like to be paced against"
        )
        return out
    row = bbd.iloc[0]
    obligation = float(row["total_rins"])
    pace = obligation * months / 12
    out.update(
        obligation_rins=obligation,
        pace_rins=pace,
        pace_pct=ytd / pace * 100,
        citation=f"{row['rule_citation']} (final, effective {row['rule_effective']})",
    )
    return out


def _prices() -> dict[str, Any] | None:
    prices = read_rin_prices()
    if prices.empty:
        return None
    wide = prices.pivot_table(
        index="Date", columns="d_code", values="price_usd_per_rin", aggfunc="first"
    ).sort_index()
    if not {"D4", "D6"} <= set(wide.columns):
        return None
    both = wide[["D4", "D6"]].dropna()
    if both.empty:
        return None
    week = both.index[-1]
    d4, d6 = float(both.loc[week, "D4"]), float(both.loc[week, "D6"])

    back = week - pd.Timedelta(weeks=_LOOKBACK_WEEKS)
    then = both.loc[back] if back in both.index else None
    return {
        "week": week.strftime("%Y-%m-%d"),
        "d4": d4,
        "d6": d6,
        "spread": d4 - d6,
        "d4_chg": None if then is None else d4 - float(then["D4"]),
        "d6_chg": None if then is None else d6 - float(then["D6"]),
        "lookback_weeks": _LOOKBACK_WEEKS,
        "series": both.tail(CHART_WEEKS),
    }


def _rvo_rows(rvo: pd.DataFrame) -> list[dict[str, Any]]:
    if rvo.empty:
        return []
    rows = []
    for _, r in rvo[rvo["rule_status"] == "final"].sort_values(
        ["compliance_year", "category"]
    ).iterrows():
        rows.append({
            "year": int(r["compliance_year"]),
            "category": r["category"],
            "base_bn": r["base_rins"] / 1e9,
            "realloc_bn": r["sre_reallocation_rins"] / 1e9,
            "total_bn": r["total_rins"] / 1e9,
            "citation": r["rule_citation"],
        })
    return rows


def rfs_policy_analysis() -> dict[str, Any] | None:
    """The RFS block's numbers, or None when Layer 29 has never landed."""
    rvo = read_rfs_rvo()
    generation = _generation(rvo)
    prices = _prices()
    rows = _rvo_rows(rvo)
    if generation is None and prices is None and not rows:
        return None
    return {"generation": generation, "prices": prices, "rvo": rows}
