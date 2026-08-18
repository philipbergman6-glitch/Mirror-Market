"""Measurements (Phase 4) — the only module here that knows SQL exists.

Everything above this file works on :class:`~analysis.opportunities.domain.MarketSignal`
values that already carry their own evidence, their own observation date and
their own validity horizon. Same seam ``analysis/origins/sources.py`` keeps for
Phase 2 and ``analysis/futures/providers.py`` for Phase 3: one substitution
point, and no SQL anywhere else in the package.

A :class:`Detection` is a measurement *plus the lane it happened on*. It is not
an opportunity: it has no counterparty, no ladder rung and no next action, and
several of them will never become one. ``rules.py`` decides that.

Three rules run through every detector here.

**A statistic needs a baseline it earned.** Every z-score in this module refuses
to fire without ``min_weeks`` of history, and every share test refuses below
``min_share``. One Panamax into a small destination is a 300% week-on-week move
and means nothing; the guard is what separates a flow shift from a vessel.

**An observation date is the market's, not ours.** Freshness is judged on the
week the flow happened or the session the price printed, never on when the
pipeline ran. The two differ on every run that lands before settlement.

**A source with no observation date says so.** PSD is keyed by marketing year
and carries no date column at all (which is why ``config.LAYER_MAX_DATA_AGE_DAYS``
deliberately omits it). Rather than stamping it with today's date — which would
render a six-month-old balance sheet as this morning's — a PSD-derived signal is
dated by our own ``data_freshness.last_success`` for that layer, and the evidence
note says exactly that.
"""

from __future__ import annotations

import logging
import sqlite3
import statistics
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import config
from analysis.opportunities.domain import (
    Blocker,
    BlockerCode,
    Blockers,
    Confidence,
    Dislocation,
    Economics,
    Evidence,
    MarketSignal,
    PartyRole,
    SignalKind,
    Volume,
)
from analysis.origins.domain import Grade, Incoterm, Money, Port, ShipmentWindow, SourceRef

log = logging.getLogger(__name__)

__all__ = [
    "DETECTORS",
    "Detection",
    "DetectionRun",
    "country_port",
    "crush_margin_detections",
    "currency_detections",
    "detect_all",
    "flow_shift_detections",
    "commitment_detections",
    "landed_advantage_detections",
    "supply_deficit_detections",
]

#: Which soy product each detector speaks about. The engine is bean-first
#: because that is what the ingested physical legs price; a meal-only detector
#: would need a meal FOB series this stack does not have.
BEANS = "beans"
MEAL = "meal"
OIL = "oil"

#: PSD names countries in full; the players base and the market registry key on
#: ISO alpha-2. One mapping, here, rather than a lookup in each detector.
_PSD_COUNTRY_ISO = {
    "China": "CN",
    "European Union": "EU",
    "India": "IN",
    "Mexico": "MX",
    "Japan": "JP",
    "Egypt": "EG",
    "Indonesia": "ID",
    "Vietnam": "VN",
    "Thailand": "TH",
    "Korea, South": "KR",
    "Taiwan": "TW",
    "Turkey": "TR",
    "Bangladesh": "BD",
    "Pakistan": "PK",
    "Iran": "IR",
    "Nigeria": "NG",
    "South Africa": "ZA",
    "Brazil": "BR",
    "Argentina": "AR",
    "United States": "US",
    "Canada": "CA",
    "Paraguay": "PY",
    "Russia": "RU",
    "Ukraine": "UA",
}

#: AMS inspection and FAS export-sales destination names, which are neither PSD
#: names nor ISO codes. Only destinations this project's own history actually
#: contains are listed — an unmapped country is skipped with a debug line
#: rather than guessed at, because a wrong ISO puts the right tonnage against
#: the wrong country's players.
_TRADE_COUNTRY_ISO = {
    "CHINA": "CN",
    "CHINA, PEOPLES REPUBLIC OF": "CN",
    "MEXICO": "MX",
    "JAPAN": "JP",
    "EGYPT": "EG",
    "INDONESIA": "ID",
    "TAIWAN": "TW",
    "KOREA, SOUTH": "KR",
    "SOUTH KOREA": "KR",
    "NETHERLANDS": "NL",
    "SPAIN": "ES",
    "GERMANY": "DE",
    "ITALY": "IT",
    "TURKEY": "TR",
    "THAILAND": "TH",
    "VIETNAM": "VN",
    "BANGLADESH": "BD",
    "PAKISTAN": "PK",
    "COLOMBIA": "CO",
    "PERU": "PE",
    "CHILE": "CL",
    "ECUADOR": "EC",
    "GUATEMALA": "GT",
    "CANADA": "CA",
    "NIGERIA": "NG",
    "MOROCCO": "MA",
    "ALGERIA": "DZ",
    "SAUDI ARABIA": "SA",
    "UNITED KINGDOM": "GB",
    "FRANCE": "FR",
    "BELGIUM": "BE",
    "PORTUGAL": "PT",
    "PHILIPPINES": "PH",
    "MALAYSIA": "MY",
    "INDIA": "IN",
}


def trade_country_iso(name: str | None) -> str | None:
    """ISO alpha-2 for an AMS/FAS destination name, or ``None`` if unmapped."""
    if not name:
        return None
    return _TRADE_COUNTRY_ISO.get(str(name).strip().upper())


def psd_country_iso(name: str | None) -> str | None:
    return _PSD_COUNTRY_ISO.get(str(name or "").strip())


def country_port(iso: str, name: str, *, role: str = "destination") -> Port:
    """A country as a pricing location, when no berth is configured for it.

    ``key`` is prefixed ``country:`` so it can never collide with a
    ``config.DESTINATION_PORTS`` key and can never be handed to a freight
    assumption as a route: a country is not a discharge port, and an assumption
    entered against one would be a rate for a place that does not exist.
    """
    return Port(key=f"country:{iso}", name=name, country=name, country_iso=iso, role=role)


@dataclass(frozen=True)
class Detection:
    """A measurement on a lane. The input to a rule, not an opportunity.

    ``blockers`` here are the ones the *detector* already knows about — a
    policy-blocked basis, an unbridged window, a stale quote. ``rules.py`` adds
    the ones that only become visible once counterparties are attached.
    """

    signal: MarketSignal
    rule_id: str
    product: str
    role_wanted: PartyRole
    origin: Port | None
    destination: Port | None
    window: ShipmentWindow | None = None
    incoterm: Incoterm | None = None
    grade: Grade | None = None
    dislocation: Dislocation | None = None
    economics: Economics | None = None
    volume: Volume | None = None
    blockers: Blockers = ()
    missing: tuple[str, ...] = ()
    why_now: str = ""
    context: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Small SQL helpers
# ---------------------------------------------------------------------------
def _rows(conn, sql: str, params: tuple = ()) -> list[tuple]:
    """Query, treating a missing table as no data rather than a crash.

    Deliberately soft, and only here. A market page that dies because one
    optional table has not been migrated is worse than a screen that says the
    signal could not be measured — and every caller turns an empty result into
    a named absence rather than into silence.
    """
    if conn is None:
        return []
    try:
        return list(conn.execute(sql, params).fetchall())
    except sqlite3.Error as exc:
        log.debug("opportunity read failed: %s (%s)", sql.split()[3] if len(sql.split()) > 3 else sql, exc)
        return []


def _as_date(raw: Any) -> date | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    try:
        return datetime.fromisoformat(str(raw)[:19]).date()
    except ValueError:
        return None


def layer_budget(layer: str, *, default: int) -> int:
    """The layer's own recency budget, or a stated default for layers with none.

    ``config.LAYER_MAX_DATA_AGE_DAYS`` omits sources with no observation date
    on purpose (CLAUDE.md: "not listed = not checked"). A signal still has to
    put *some* horizon on its evidence, so the default is passed in by the
    detector that knows the source's cadence rather than assumed globally.
    """
    return int(config.LAYER_MAX_DATA_AGE_DAYS.get(layer, default))


def layer_last_success(conn, layer: str) -> date | None:
    """When our ingest of ``layer`` last succeeded. The honest date for a
    source that publishes none of its own."""
    rows = _rows(
        conn,
        "SELECT last_success FROM data_freshness WHERE layer_name = ?",
        (layer,),
    )
    return _as_date(rows[0][0]) if rows and rows[0][0] else None


def _zscore(current: float, history: list[float]) -> tuple[float | None, float, float]:
    """``(z, mean, stdev)``. ``z`` is ``None`` where the sample cannot support one.

    Two samples have a standard deviation and it is meaningless; a flat series
    has one of zero and dividing by it manufactures infinity. Both return
    ``None``, which every caller treats as "no signal", never as "zero".
    """
    if len(history) < 3:
        return None, (statistics.fmean(history) if history else 0.0), 0.0
    mean = statistics.fmean(history)
    stdev = statistics.pstdev(history)
    if stdev <= 1e-12:
        return None, mean, stdev
    return (current - mean) / stdev, mean, stdev


# ---------------------------------------------------------------------------
# 1. Origin landed advantage
# ---------------------------------------------------------------------------
def landed_advantage_detections(
    conn,
    *,
    today: date,
    assumptions=None,
) -> list[Detection]:
    """The Phase 2 ranking, read as a business question rather than an analysis.

    Reuses ``analysis.origins.comparison.build_ranking`` verbatim — the landed
    cost is not recomputed here, because two implementations of one waterfall
    is exactly how a screen and a page come to disagree about which origin is
    cheapest.

    A ranking that is not decisive produces no detection at all rather than a
    blocked one: "cheapest of one" is the sentence Phase 2 already refuses to
    print, and wrapping it in an opportunity would print it anyway.
    """
    from analysis.origins.assumptions import load_assumptions
    from analysis.origins.comparison import build_ranking, default_window

    settings = config.OPPORTUNITY_RULES["landed_advantage"]
    assumptions = assumptions if assumptions is not None else load_assumptions()
    out: list[Detection] = []

    for destination_key in config.DESTINATION_PORTS:
        window = default_window(today)
        try:
            ranking = build_ranking(
                conn,
                destination_key=destination_key,
                window=window,
                today=today,
                assumptions=assumptions,
            )
        except Exception:  # noqa: BLE001 — one destination must not cost the screen
            log.warning("landed-advantage detection failed for %s", destination_key, exc_info=True)
            continue

        cheapest = ranking.cheapest
        advantage = ranking.advantage_usd_mt
        if cheapest is None or advantage is None:
            continue
        if advantage < settings["min_advantage_usd_mt"]:
            continue

        runner_up = ranking.rankable[1]
        quote = cheapest.quote
        evidence = (
            Evidence(
                label=f"{quote.origin.name} landed {destination_key}",
                value=cheapest.landed_usd_mt,
                unit="usd_per_mt",
                observed_on=quote.observation_date,
                source=quote.source,
                max_age_days=quote.max_age_days,
                quote_kind=quote.quote_kind.value,
                confidence=cheapest.confidence,
                note=f"landed-cost method v{cheapest.method_version}",
            ),
            Evidence(
                label=f"{runner_up.quote.origin.name} landed {destination_key}",
                value=runner_up.landed_usd_mt,
                unit="usd_per_mt",
                observed_on=runner_up.quote.observation_date,
                source=runner_up.quote.source,
                max_age_days=runner_up.quote.max_age_days,
                quote_kind=runner_up.quote.quote_kind.value,
                confidence=runner_up.confidence,
                note="the origin this advantage is measured against",
            ),
        )
        signal = MarketSignal(
            signal_id=f"landed:{destination_key}:{quote.origin.key}:{window.start.isoformat()}",
            kind=SignalKind.LANDED_ADVANTAGE,
            headline=(
                f"{quote.origin.name} lands {advantage:,.2f} USD/MT under "
                f"{runner_up.quote.origin.name} into {ranking.destination.name}"
            ),
            detail=(
                f"{len(ranking.rankable)} of {len(ranking.rows)} declared origins were "
                f"comparable for {window.describe()}. The advantage is against the second "
                "cheapest comparable origin, never against a blocked or differently-windowed "
                "one."
            ),
            observed_on=max(row.quote.observation_date for row in ranking.rankable),
            evidence=evidence,
            validity_days=settings["validity_days"],
            magnitude=advantage,
            magnitude_unit="usd_per_mt",
            subject=f"{quote.origin.country_iso}->{ranking.destination.country_iso}",
        )
        out.append(Detection(
            signal=signal,
            rule_id="landed_advantage",
            product=BEANS,
            role_wanted=PartyRole.SELLER,
            origin=quote.origin,
            destination=ranking.destination,
            window=window,
            incoterm=quote.incoterm,
            grade=quote.grade,
            dislocation=Dislocation(
                kind="landed_advantage",
                label=f"vs {runner_up.quote.origin.name} landed",
                value=advantage,
                unit="usd_per_mt",
                baseline=runner_up.landed_usd_mt,
                baseline_label=f"{runner_up.quote.origin.name} landed cost",
            ),
            economics=Economics(
                per_mt=Money(advantage),
                method="cheapest comparable landed cost less the second cheapest",
                method_version=config.LANDED_COST_METHOD_VERSION,
                struck_on=today,
                components=(
                    (f"{quote.origin.name} landed", cheapest.landed_usd_mt or 0.0),
                    (f"{runner_up.quote.origin.name} landed", runner_up.landed_usd_mt or 0.0),
                ),
                note=(
                    "An advantage, not a margin. It says which origin is cheaper delivered; "
                    "what a buyer pays is a negotiation this number does not observe."
                ),
            ),
            missing=tuple(
                sorted({
                    component.value
                    for row in ranking.rows
                    for component in row.missing_inputs
                })
            ),
            why_now=(
                f"{quote.origin.name} is the cheapest comparable origin delivered "
                f"{ranking.destination.name} for {window.describe()}, by "
                f"{advantage:,.2f} USD/MT over {runner_up.quote.origin.name}. Both legs were "
                f"observed within {config.ORIGIN_MAX_OBSERVATION_SPREAD_DAYS} days of each other, "
                "so the gap is competitiveness rather than calendar."
            ),
            context={
                "ranking": ranking,
                "destination_key": destination_key,
                "advantage_pct": ranking.advantage_pct,
            },
        ))
    return out


# ---------------------------------------------------------------------------
# 2. Destination flow shift (shipped) and 3. commitment shift (sold, unshipped)
# ---------------------------------------------------------------------------
def _weekly_shares(
    conn,
    *,
    table: str,
    value_column: str,
    date_column: str,
    commodity: str,
) -> dict[date, dict[str, float]]:
    """``{week: {country: tonnes}}`` for one commodity, regions summed.

    Regions are summed rather than kept apart on purpose: a cargo that moved
    from the Gulf to the PNW is a US logistics story, not a change in who is
    buying, and this rule is about the buyer.
    """
    rows = _rows(
        conn,
        f"SELECT {date_column}, country, SUM({value_column}) "  # noqa: S608 — literal identifiers
        f"FROM {table} WHERE commodity = ? AND {value_column} IS NOT NULL "
        f"GROUP BY {date_column}, country",
        (commodity,),
    )
    weeks: dict[date, dict[str, float]] = {}
    for raw_week, country, total in rows:
        week = _as_date(raw_week)
        if week is None or total is None:
            continue
        weeks.setdefault(week, {})[str(country)] = float(total)
    return weeks


def _flow_like_detections(
    conn,
    *,
    today: date,
    rule_id: str,
    table: str,
    value_column: str,
    date_column: str,
    layer: str,
    layer_default_age: int,
    kind: SignalKind,
    unit_label: str,
    what: str,
    commodity: str = "Soybeans",
) -> list[Detection]:
    """Shared body for the two flow rules — shipped tonnage and forward sales.

    They are separate *rules* because they answer different questions (who took
    delivery vs who bought and has not shipped) but they are the same
    *measurement*: one destination's share of a weekly US total, against its own
    trailing mean. Writing the statistic twice would be two chances for the
    guard rails to drift apart.
    """
    settings = config.OPPORTUNITY_RULES[rule_id]
    weeks = _weekly_shares(
        conn, table=table, value_column=value_column, date_column=date_column, commodity=commodity
    )
    if len(weeks) < settings["min_weeks"] + 1:
        return []

    ordered = sorted(weeks)
    latest = ordered[-1]
    baseline_weeks = ordered[max(0, len(ordered) - 1 - settings["baseline_weeks"]):-1]
    if len(baseline_weeks) < settings["min_weeks"]:
        return []

    latest_total = sum(weeks[latest].values())
    if latest_total <= 0:
        return []

    origin = country_port("US", "United States", role="origin")
    max_age = layer_budget(layer, default=layer_default_age)
    out: list[Detection] = []

    for country, tonnes in sorted(weeks[latest].items(), key=lambda item: -item[1]):
        share = tonnes / latest_total
        if share < settings["min_share"]:
            continue
        iso = trade_country_iso(country)
        if iso is None:
            log.debug("%s: destination %r has no ISO mapping — skipped", rule_id, country)
            continue
        history = [
            weeks[week].get(country, 0.0) / total
            for week in baseline_weeks
            if (total := sum(weeks[week].values())) > 0
        ]
        z, mean_share, _ = _zscore(share, history)
        if z is None or z < settings["min_z"]:
            continue

        title = country.title()
        source = SourceRef(
            layer=layer,
            table=table,
            key=f"{commodity}/{country}",
            detail=f"week ending {latest.isoformat()}",
            href="index.html",
        )
        evidence = (
            Evidence(
                label=f"{title} {what}, week ending {latest.isoformat()}",
                value=round(tonnes, 1),
                unit="mt",
                observed_on=latest,
                source=source,
                max_age_days=max_age,
                quote_kind="observation",
                confidence=Confidence.INDICATIVE,
                note=(
                    f"{share * 100:.1f}% of that week's {latest_total:,.0f} MT total, against a "
                    f"{len(history)}-week mean of {mean_share * 100:.1f}%"
                ),
            ),
        )
        signal = MarketSignal(
            signal_id=f"{rule_id}:{iso}:{latest.isoformat()}",
            kind=kind,
            headline=(
                f"{title} took {share * 100:.1f}% of US {commodity.lower()} {what} — "
                f"{z:.1f} sigma over its {len(history)}-week mean"
            ),
            detail=(
                f"{tonnes:,.0f} MT in the week ending {latest.isoformat()}. The test is on "
                "SHARE, not tonnage: a heavy export week lifts every destination and would "
                "fire on all of them at once."
            ),
            observed_on=latest,
            evidence=evidence,
            validity_days=settings["validity_days"],
            magnitude=z,
            magnitude_unit="sigma",
            subject=iso,
        )
        out.append(Detection(
            signal=signal,
            rule_id=rule_id,
            product=BEANS,
            role_wanted=PartyRole.BUYER,
            origin=origin,
            destination=country_port(iso, title),
            dislocation=Dislocation(
                kind="flow",
                label=f"{title} share of US {what}",
                value=round(share * 100, 2),
                unit="pct",
                baseline=round(mean_share * 100, 2),
                baseline_label=f"{len(history)}-week mean share",
                z_score=round(z, 2),
            ),
            volume=Volume(
                low_mt=round(tonnes, 1),
                high_mt=round(tonnes, 1),
                basis=(
                    f"the week's own {unit_label} to this destination — a published flow, "
                    "not an estimate of what a next cargo would be"
                ),
                source=source,
            ),
            blockers=(
                Blocker(
                    code=BlockerCode.SIZE_UNKNOWN,
                    message=(
                        "The tonnage shown is last week's flow, not the size of any cargo "
                        "on offer."
                    ),
                    remedy=(
                        "Size it from the counterparty's own programme; this project ingests "
                        "no line-up, manifest or contract."
                    ),
                ),
            ),
            why_now=(
                f"{title} took {share * 100:.1f}% of US {commodity.lower()} {what} in the week "
                f"ending {latest.isoformat()}, {z:.1f} standard deviations above its own "
                f"{len(history)}-week mean of {mean_share * 100:.1f}%. Something changed in that "
                "buyer's programme, and it changed recently enough to still be worth a call."
            ),
            context={"share": share, "z": z, "tonnes": tonnes, "week": latest},
        ))
    return out


def flow_shift_detections(conn, *, today: date) -> list[Detection]:
    """Destinations taking an unusual share of *shipped* US soybeans."""
    return _flow_like_detections(
        conn,
        today=today,
        rule_id="destination_flow_shift",
        table="inspection_destinations",
        value_column="inspections_mt",
        date_column="week_ending",
        layer="crush_inspections",
        layer_default_age=21,
        kind=SignalKind.FLOW_SHIFT,
        unit_label="inspected tonnage",
        what="export inspections",
    )


def commitment_detections(conn, *, today: date) -> list[Detection]:
    """Destinations holding an unusual share of *sold but unshipped* US soybeans.

    Outstanding sales rather than net sales: a week of net sales is a flow and
    is already noisy enough to need its own baseline, while outstanding sales is
    the standing book — the thing that says a buyer is committed rather than
    that they were busy on Tuesday.
    """
    return _flow_like_detections(
        conn,
        today=today,
        rule_id="commitment_shift",
        table="export_sales",
        value_column="outstanding_sales",
        date_column="week_ending",
        layer="export_sales",
        layer_default_age=21,
        kind=SignalKind.COMMITMENT_SHIFT,
        unit_label="outstanding sales",
        what="outstanding sales",
    )


# ---------------------------------------------------------------------------
# 4. Buyer-region supply deficit / tight stocks-to-use
# ---------------------------------------------------------------------------
def supply_deficit_detections(conn, *, today: date) -> list[Detection]:
    """Importing regions whose stocks-to-use fell below their own prior low.

    Reuses ``analysis.stocks_to_use`` rather than restating the ratio: PSD's
    "Total Distribution" equals total *supply*, and a second implementation is
    one more place to pick the wrong denominator.

    PSD publishes no observation date, so the signal is dated by our own last
    successful PSD ingest and the evidence says so. Stamping it with today would
    render a marketing-year balance sheet as this morning's news.
    """
    import pandas as pd

    from analysis.stocks_to_use import compute_stocks_to_use, detect_tight_supply

    settings = config.OPPORTUNITY_RULES["supply_deficit"]
    rows = _rows(
        conn,
        "SELECT commodity, country, year, attribute, value, unit FROM psd "
        "WHERE commodity = 'Oilseed, Soybean'",
    )
    if not rows:
        return []
    frame = pd.DataFrame(
        rows, columns=["commodity", "country", "year", "attribute", "value", "unit"]
    )

    observed_on = layer_last_success(conn, "psd")
    if observed_on is None:
        log.debug("supply_deficit: no psd freshness row — cannot date the evidence")
        return []

    # Importers only. A tight balance sheet in an exporting country is a
    # supply story, not a buyer with a hole to fill, and the two call for
    # opposite trades.
    importers = {
        iso: name
        for name, iso in _PSD_COUNTRY_ISO.items()
        if iso not in {"US", "BR", "AR", "PY", "UA", "RU", "CA"}
    }
    out: list[Detection] = []

    for country_name, iso in sorted(
        ((name, _PSD_COUNTRY_ISO[name]) for name in _PSD_COUNTRY_ISO),
        key=lambda pair: pair[1],
    ):
        if iso not in importers:
            continue
        ratios = compute_stocks_to_use(frame, country=country_name)
        signals = detect_tight_supply(ratios, today=today.isoformat())
        if not signals or ratios.empty:
            continue
        latest = ratios.sort_values("year").iloc[-1]
        source = SourceRef(
            layer="psd",
            table="psd",
            key=f"Oilseed, Soybean/{country_name}",
            detail=f"marketing year {int(latest['year'])}",
            href="index.html",
        )
        evidence = (
            Evidence(
                label=f"{country_name} soybean stocks-to-use, MY {int(latest['year'])}",
                value=round(float(latest["ratio"]) * 100, 2),
                unit="pct",
                observed_on=observed_on,
                source=source,
                # PSD is a monthly release keyed by marketing year. Ninety days
                # is two releases plus slack: past that our own copy is behind,
                # whatever the balance sheet says.
                max_age_days=90,
                quote_kind="observation",
                confidence=Confidence.INDICATIVE,
                note=(
                    "PSD is keyed by marketing year and publishes no observation date, so "
                    "this is dated by our own last successful ingest of Layer 6, not by "
                    "the market."
                ),
            ),
        )
        signal = MarketSignal(
            signal_id=f"deficit:{iso}:{int(latest['year'])}",
            kind=SignalKind.SUPPLY_DEFICIT,
            headline=signals[0]["description"].replace("Oilseed, Soybean", country_name),
            detail=(
                "Ending stocks over total use (domestic consumption plus exports), below "
                "the prior five-year low for this country. A tight importer bids for cargo; "
                "it does not tell you what they will pay."
            ),
            observed_on=observed_on,
            evidence=evidence,
            validity_days=settings["validity_days"],
            magnitude=round(float(latest["ratio"]) * 100, 2),
            magnitude_unit="pct",
            subject=iso,
        )
        out.append(Detection(
            signal=signal,
            rule_id="supply_deficit",
            product=BEANS,
            role_wanted=PartyRole.BUYER,
            origin=None,
            destination=country_port(iso, country_name),
            dislocation=Dislocation(
                kind="balance_sheet",
                label=f"{country_name} stocks-to-use",
                value=round(float(latest["ratio"]) * 100, 2),
                unit="pct",
                baseline_label="prior five-year low",
            ),
            blockers=(
                Blocker(
                    code=BlockerCode.SIZE_UNKNOWN,
                    message="A balance sheet says nothing about the size of any one cargo.",
                    remedy="Size it from the buyer's own programme.",
                ),
            ),
            why_now=(
                f"{country_name}'s soybean stocks-to-use has fallen below its own prior "
                "five-year low. A buyer running its balance sheet that thin has to cover, "
                "and the cover has to come from somewhere."
            ),
            context={"ratio": float(latest["ratio"]), "year": int(latest["year"])},
        ))
    return out


# ---------------------------------------------------------------------------
# 5. Crush margin
# ---------------------------------------------------------------------------
def crush_margin_detections(conn, *, today: date, assumptions=None) -> list[Detection]:
    """Crushers earning enough to bid up for beans.

    Uses ``analysis.origins.crush`` and takes the *gross physical* level where
    it exists, falling back to the board. Both are labelled on the opportunity,
    because they are not the same number: a board crush is three futures
    settlements and is not what any plant earns.
    """
    from analysis.origins.assumptions import load_assumptions
    from analysis.origins.crush import CrushLevel, crush_stack

    settings = config.OPPORTUNITY_RULES["crush_margin"]
    assumptions = assumptions if assumptions is not None else load_assumptions()
    out: list[Detection] = []

    for slug in sorted(config.PHYSICAL_CRUSH):
        market = config.MARKETS.get(slug) or {}
        try:
            board, gross, _net = crush_stack(conn, slug, assumptions, today=today)
        except Exception:  # noqa: BLE001 — one market must not cost the screen
            log.warning("crush detection failed for %s", slug, exc_info=True)
            continue

        chosen = gross if gross.is_ok else board
        if not chosen.is_ok or chosen.as_of is None:
            continue
        margin = chosen.margin_usd_mt
        if margin is None or margin < settings["min_margin_usd_mt"]:
            continue

        iso = market.get("players_country")
        if not iso:
            continue
        evidence = tuple(
            Evidence(
                label=f"{slug} {leg.name} leg",
                value=leg.price.amount,
                unit="usd_per_mt",
                observed_on=chosen.as_of,
                source=leg.source,
                max_age_days=layer_budget(leg.source.layer, default=14),
                quote_kind=leg.quote_kind.value,
                confidence=chosen.confidence,
            )
            for leg in chosen.legs
        )
        if not evidence:
            continue

        is_board = chosen.level is CrushLevel.BOARD
        signal = MarketSignal(
            signal_id=f"crush:{slug}:{chosen.level.value}:{chosen.as_of.isoformat()}",
            kind=SignalKind.CRUSH_MARGIN,
            headline=(
                f"{market.get('name', slug)} {chosen.label.lower()} at "
                f"{margin:,.2f} USD/MT"
            ),
            detail=chosen.meaning,
            observed_on=chosen.as_of,
            evidence=evidence,
            validity_days=settings["validity_days"],
            magnitude=margin,
            magnitude_unit="usd_per_mt",
            subject=iso,
        )
        blockers: list[Blocker] = [
            Blocker(
                code=BlockerCode.SIZE_UNKNOWN,
                message="A margin per tonne says nothing about how many tonnes a plant needs.",
                remedy="Size it from the plant's own capacity and run rate.",
            )
        ]
        if is_board:
            blockers.append(Blocker(
                code=BlockerCode.LIQUIDITY_UNPROVEN,
                message=(
                    "This is the BOARD crush — three futures settlements, not a plant's own "
                    "buy and sell. No physical leg for this market is ingested."
                ),
                remedy=(
                    "Treat it as a paper margin. A physical crush needs cash oil and meal "
                    "assessments this stack does not have for this market."
                ),
            ))
        out.append(Detection(
            signal=signal,
            rule_id="crush_margin",
            product=BEANS,
            role_wanted=PartyRole.BUYER,
            origin=None,
            destination=country_port(iso, market.get("name", slug)),
            dislocation=Dislocation(
                kind="margin",
                label=chosen.label,
                value=margin,
                unit="usd_per_mt",
            ),
            economics=Economics(
                per_mt=Money(margin),
                method=chosen.label,
                method_version=config.LANDED_COST_METHOD_VERSION,
                struck_on=chosen.as_of,
                components=tuple(
                    (leg.name, leg.price.amount) for leg in chosen.legs
                ),
                note=chosen.meaning,
            ),
            blockers=tuple(blockers),
            why_now=(
                f"{market.get('name', slug)}'s {chosen.label.lower()} is "
                f"{margin:,.2f} USD/MT on {chosen.as_of.isoformat()}, above the "
                f"{settings['min_margin_usd_mt']:,.0f} USD/MT this engine treats as worth a "
                "call. A crusher earning that has room to bid up for beans."
            ),
            context={"level": chosen.level.value, "market": slug},
        ))
    return out


# ---------------------------------------------------------------------------
# 6. Currency move changes origin competitiveness
# ---------------------------------------------------------------------------
#: Which origins have a home currency whose move changes a farmer's incentive to
#: sell. Keyed by market slug so the FX pair and the country come from the
#: registry rather than being restated here.
_FX_ORIGIN_MARKETS = ("brazil", "argentina")


def currency_detections(conn, *, today: date) -> list[Detection]:
    """An origin currency move that repriced a local seller's incentive.

    Direction matters and is stated rather than left to the sign. Every series
    in this stack is ``<CCY>/USD`` meaning **USD per one unit of local
    currency**, so a *fall* is a weaker local currency: the farmer receives more
    of their own money for the same dollar cargo, and the origin gets more
    competitive. Reading that backwards produces a confident recommendation to
    buy from the origin that just got dearer.
    """
    settings = config.OPPORTUNITY_RULES["currency_shift"]
    lookback = int(settings["lookback_sessions"])
    out: list[Detection] = []

    for slug in _FX_ORIGIN_MARKETS:
        market = config.MARKETS.get(slug) or {}
        pair = market.get("currency_pair")
        iso = market.get("players_country")
        if not pair or not iso:
            continue
        rows = _rows(
            conn,
            "SELECT Date, Close FROM currencies WHERE pair = ? AND Close IS NOT NULL "
            "ORDER BY Date DESC LIMIT ?",
            (pair, lookback + 1),
        )
        if len(rows) < lookback + 1:
            continue
        latest_date = _as_date(rows[0][0])
        earlier_date = _as_date(rows[-1][0])
        if latest_date is None or earlier_date is None:
            continue
        latest, earlier = float(rows[0][1]), float(rows[-1][1])
        if earlier <= 0:
            continue
        move_pct = (latest - earlier) / earlier * 100.0
        if abs(move_pct) < settings["min_move_pct"]:
            continue

        weaker = move_pct < 0
        source = SourceRef(
            layer="currencies",
            table="currencies",
            key=pair,
            detail=f"{earlier_date.isoformat()} → {latest_date.isoformat()}",
            href=f"markets/{slug}.html",
        )
        evidence = (
            Evidence(
                label=f"{pair} ({lookback}-session move)",
                value=round(move_pct, 2),
                unit="pct",
                observed_on=latest_date,
                source=source,
                max_age_days=layer_budget("currencies", default=7),
                quote_kind="board",
                confidence=Confidence.EXECUTABLE,
                note=(
                    f"{earlier:.6f} → {latest:.6f} USD per unit of "
                    f"{market.get('home_currency', '?')}"
                ),
            ),
        )
        direction = "weaker" if weaker else "stronger"
        signal = MarketSignal(
            signal_id=f"fx:{pair}:{latest_date.isoformat()}",
            kind=SignalKind.CURRENCY_SHIFT,
            headline=(
                f"{market.get('home_currency', pair)} {abs(move_pct):.1f}% {direction} over "
                f"{lookback} sessions"
            ),
            detail=(
                "A weaker origin currency pays the local seller more of their own money for "
                "the same dollar cargo, which pulls supply forward; a stronger one does the "
                "reverse. It changes the seller's incentive, not the USD price we observe."
            ),
            observed_on=latest_date,
            evidence=evidence,
            validity_days=settings["validity_days"],
            magnitude=round(move_pct, 2),
            magnitude_unit="pct",
            subject=iso,
        )
        out.append(Detection(
            signal=signal,
            rule_id="currency_shift",
            product=BEANS,
            role_wanted=PartyRole.SELLER if weaker else PartyRole.BUYER,
            origin=country_port(iso, market.get("name", slug), role="origin"),
            destination=None,
            dislocation=Dislocation(
                kind="fx",
                label=f"{pair} over {lookback} sessions",
                value=round(move_pct, 2),
                unit="pct",
            ),
            blockers=(
                Blocker(
                    code=BlockerCode.LIQUIDITY_UNPROVEN,
                    message=(
                        "An FX move changes a seller's incentive. It is not itself a price "
                        "difference and there is no spread here to capture."
                    ),
                    remedy=(
                        "Pair it with a landed comparison for the lane before treating it as "
                        "an edge."
                    ),
                ),
                Blocker(
                    code=BlockerCode.SIZE_UNKNOWN,
                    message="No tonnage is implied by an exchange rate.",
                    remedy="Size it from the counterparty's own programme.",
                ),
            ),
            why_now=(
                f"{market.get('home_currency', pair)} is {abs(move_pct):.1f}% {direction} "
                f"against the dollar over {lookback} sessions "
                f"({earlier_date.isoformat()} → {latest_date.isoformat()}). "
                + (
                    "Local sellers are being paid more per dollar cargo, which historically "
                    "pulls farmer selling forward."
                    if weaker else
                    "Local sellers are being paid less per dollar cargo, which historically "
                    "slows farmer selling."
                )
            ),
            context={"pair": pair, "move_pct": move_pct, "weaker": weaker},
        ))
    return out


# ---------------------------------------------------------------------------
# Everything, in one call
# ---------------------------------------------------------------------------
#: Detector order is the order the page ranks ties in, and it is deliberate:
#: a costed landed advantage outranks a statistical flow shift, which outranks
#: a balance-sheet observation.
#: ``(rule_id, callable, needs_assumptions)``. The rule id is on the entry
#: rather than derived from the function name: coverage reporting keys on it,
#: and a rename that silently broke the join would produce a screen that says a
#: rule found nothing when it was never asked.
DETECTORS: tuple[tuple[str, Any, bool], ...] = (
    ("landed_advantage", landed_advantage_detections, True),
    ("destination_flow_shift", flow_shift_detections, False),
    ("commitment_shift", commitment_detections, False),
    ("supply_deficit", supply_deficit_detections, False),
    ("crush_margin", crush_margin_detections, True),
    ("currency_shift", currency_detections, False),
)


@dataclass(frozen=True)
class DetectionRun:
    """Every detection, plus what each detector did.

    Coverage is returned alongside rather than recomputed, because recomputing
    it would mean running every detector twice — and a screen with three items
    looks the same whether three rules found one each or one rule found three
    and two crashed. This is how the page tells those apart.
    """

    detections: tuple[Detection, ...]
    coverage: tuple[dict[str, Any], ...]

    @property
    def failed(self) -> tuple[dict[str, Any], ...]:
        return tuple(entry for entry in self.coverage if not entry["ran"])


def detect_all(conn, *, today: date, assumptions=None) -> DetectionRun:
    """Run every detector once, isolating failures.

    One detector raising must not empty the screen — the same failure-isolation
    policy ``scripts/generate_site.py`` applies to pages and ``app/blocks.py``
    to blocks. The failure is logged *and reported*: silently absorbing it would
    render "no opportunities today" over a crash.
    """
    detections: list[Detection] = []
    coverage: list[dict[str, Any]] = []
    for rule_id, detector, needs_assumptions in DETECTORS:
        settings = config.OPPORTUNITY_RULES.get(rule_id, {})
        entry = {
            "rule_id": rule_id,
            "label": settings.get("label", rule_id),
            "question": settings.get("question", ""),
            "ran": True,
            "found": 0,
            "error": None,
        }
        try:
            found = (
                detector(conn, today=today, assumptions=assumptions)
                if needs_assumptions else detector(conn, today=today)
            )
            detections.extend(found)
            entry["found"] = len(found)
        except Exception as exc:  # noqa: BLE001 — isolation is the point
            log.warning("detector %s failed", rule_id, exc_info=True)
            entry["ran"] = False
            entry["error"] = f"{type(exc).__name__}: {exc}"
        coverage.append(entry)
    return DetectionRun(detections=tuple(detections), coverage=tuple(coverage))
