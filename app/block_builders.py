"""The nine block builders (M18 #214) — data only, never markup.

M17 #213 shipped the envelope and the page skeleton; every block rendered a
"not built yet" empty state. This module fills them.

Two rules run through all of it:

**The market is a parameter, never a code path.** There is no ``if market ==
"india"`` anywhere below, and adding one is the drift the whole contract exists
to prevent (M8 #150). Everything that varies between markets — which table,
which column, which unit, which FX pair, why a block is absent — is read from
the registry descriptor. That is why these builders are generic SQL over
``Source`` rather than nine hand-written per-market queries: a tenth market is
a ``config.MARKETS`` entry and no code.

**Builders return data; templates own markup.** Before M18 five headline
sections assembled ~600 lines of HTML inside f-strings, which cannot enforce
"same block, same treatment, eight markets" — a per-market tweak was one
f-string away and invisible in review. Every function here returns a ``Block``
whose ``data`` is numbers and labels; ``app/templates/blocks/NN_<id>.html.j2``
decides how it looks.

Failure isolation (M8): ``build_blocks`` catches per block. A builder that
raises becomes an ``empty`` block carrying ``GENERATION_ERROR`` — the same
*shape* as a missing source, a deliberately different *reason* — so one bad
market block cannot tombstone a page.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from typing import Any

import pandas as pd

import config
from analysis import hazards, weather_alerts
from analysis.futures.domain import NamedContract, named_contract
from analysis.hazards import LegHazard, PlaceHazard
from analysis.origins.domain import LoadingMonth, ShipmentWindow
from analysis.origins.sources import nearest_window_key
from app.blocks import (
    BLOCK_IDS,
    GENERATION_ERROR,
    STATE_ABSENT,
    STATE_EMPTY,
    STATE_OK,
    Block,
    absent_reason,
    make_block,
)
from app.markets import Market, Source, TierResult, _ingest_status
from pipeline.units import kg_to_metric_tons, kg_to_million_metric_tons
from pricing.fx_alignment import FxResolution, align_fx
from pricing.semantics import quote_kind_label

log = logging.getLogger(__name__)

# How much history each cadence reads. This is M8's chart budget applied at the
# source: a block that renders one number and a change has no business pulling
# 15 years of rows, and `docs/index.html` is 7 MB today precisely because
# nothing bounded the series a surface reads.
LOOKBACK_DAYS_BY_CADENCE = {"daily": 400, "weekly": 1100, "monthly": 1900}

# Sessions back for the "change" figure on a price leg. One observation, not a
# calendar day: a market that did not print yesterday must compare against the
# last time it *did*, or a holiday reads as a flat market.
CHANGE_LOOKBACK_OBS = (1, 5, 21)

# Sessions block 01's signal chips need: MA_50 and the slow MACD are NaN on
# fewer. On a windowed source they are counted on one window (#513).
SIGNAL_MIN_OBS = 60

# PSD attributes the supply & demand block renders, in balance-sheet order.
PSD_ATTRIBUTES = ("Production", "Imports", "Exports", "Ending Stocks")

# The market whose board every basis is struck against. Named once here rather
# than read from each `Source.reference`, which is checked against it.
REFERENCE_MARKET = "cbot"

# M3 #145's state pills. Three, not four: a venue that re-dates a carried price
# has not repriced, which `no_print_since` already says — see `leg_prints`.
# `out_of_cadence` exists only on the headline ledger, whose rows are markets
# rather than legs and therefore include Europe's weekly leg.
LEDGER_STATE_REPRICED = "repriced"
LEDGER_STATE_NO_PRINT = "no_print_since"
LEDGER_STATE_DARK = "dark"
LEDGER_STATE_OUT_OF_CADENCE = "out_of_cadence"

# Sessions a leg needs before the headline crush board puts a mean and a range
# around its margin. Twenty is the floor the headline basis panel already uses:
# below it an "average" is a fortnight of weather wearing a year's label.
CRUSH_RANGE_MIN_OBS = 20

# How far the USD and home-currency moves must diverge before the row is tagged
# `FX`. A tenth of a percentage point: below that the rate is rounding, above it
# the currency is doing work the reader would otherwise credit to the market.
FX_DIVERGENCE_PP = 0.1

# ---- the row drill-down (M21 #250) ----------------------------------------
# Its own budget, declared beside the others. `app/sections.clip()` does NOT fit
# here: it is `frame.tail(504)` on a DataFrame, sized for the headline's Plotly
# figures, and these series are lists of tuples over a 90-day default window.
# Reusing it would either raise on a list or ship 504 points per leg.
LEDGER_DRILLDOWN_MAX_OBS = 260

# 30d / 90d / 1y — never "all". A control labelled "all" beside a series capped
# at 260 observations is a lie about the data, and what we do and do not know is
# this block's entire subject.
LEDGER_DRILLDOWN_WINDOW_DAYS = (30, 90, 365)
LEDGER_DRILLDOWN_DEFAULT_DAYS = 90

# The sparse-history rule, by OBSERVATION COUNT on the leg and never by age of
# the source: the snapshot legs are dense-but-short while a weekly leg is
# sparse-but-long, and only the count decides whether a shape can be read.
#
#   >= 8   line, plus a dot per observation
#   3 – 7  dots only — a connecting line asserts a path between prints that was
#          never observed, and on a source publishing one number a day with
#          holes that assertion is the whole error
#   < 3    no chart at all; the pane states the level and where history starts
LEDGER_LINE_MIN_OBS = 8
LEDGER_CHART_MIN_OBS = 3


# ---- shipment windows (B30 #504) -------------------------------------------
@dataclass(frozen=True)
class WindowQuote:
    """One shipment window's print on one session.

    ``quotes`` counts stored rows *inside this one window* (two AMS barge
    locations quoting the same slot) — a mean over places, never over windows.
    """

    window: ShipmentWindow
    value: float
    quotes: int

    @property
    def label(self) -> str:
        return self.window.describe()


@dataclass(frozen=True)
class SessionWindows:
    """Every window a windowed source printed on one session, nearest first.

    ``chosen`` is what the session's print *is* on every surface: the nearest
    window still open on that date (B30 #504). A venue printing Oct-26,
    Nov–Apr and May–Sep on one circular has published three prices, and their
    mean — 483.0 on 2026-10-16 — is none of them.
    """

    printed: tuple[WindowQuote, ...]

    @property
    def chosen(self) -> WindowQuote:
        return self.printed[0]

    @property
    def identity(self) -> LoadingMonth:
        """Which cargo, across sessions: the chosen band's loading month (A20
        #536, B39 #540). Every comparison across sessions keys on this — moves,
        rolls, signal runs, ranges, the drill-down's breaks — so a band change
        inside a month (``Sep`` ↔ ``Sep–Oct``) is like-for-like and only a
        change of month is a roll. The one selector; no surface branches on
        the venue."""
        return self.chosen.window.loading_month

    @property
    def band(self) -> tuple[str, date]:
        """Which printed band, across sessions — what a band-change note names.
        ``(label, end)`` and not ``start``: AMS's Current slot starts on its own
        report date, so its start moves every day while the band it names
        stays the month's balance."""
        return self.chosen.label, self.chosen.window.end


@dataclass(frozen=True)
class _Prints:
    series: dict[str, list[tuple[date, float, int]]]
    # Empty for a source that prints one window (or none) per session.
    windows: dict[str, dict[date, SessionWindows]]


def _same_identity(windows: dict[date, SessionWindows], now: date, prior: date) -> bool:
    """Whether two sessions' prints are one loading month's — a move is only a
    move inside one. Across a roll (Sep → Oct loading) it is a calendar spread
    wearing a session-change label. A source with no windows never rolls."""
    if not windows:
        return True
    return windows[now].identity == windows[prior].identity


def _roll_reason(before: LoadingMonth, after: LoadingMonth) -> str:
    """B39 #540's reason text for a change withheld across a loading-month roll."""
    return f"loading month rolled {before.name} → {after.name} — no like-for-like change"


def _roll_note(windows: dict[date, SessionWindows], now: date, prior: date, move: str) -> str:
    """Why a move is blank across a loading-month roll — never a bare dash."""
    return f"{move} withheld: {_roll_reason(windows[prior].identity, windows[now].identity)}"


def _band_note(windows: dict[date, SessionWindows], now: date, prior: date) -> str | None:
    """The inline note on a move shown across a band change inside one loading
    month (B39 #540) — like-for-like, but the band is named. None where the
    band did not change, the month did, or the source has no windows."""
    if not windows or not _same_identity(windows, now, prior):
        return None
    before, after = windows[prior], windows[now]
    if before.band == after.band:
        return None
    return (
        f"band {before.chosen.window.band_name(year=False)} → "
        f"{after.chosen.window.band_name(year=False)}, same loading month"
    )


def _window_fields(windows: dict[date, SessionWindows], when: date) -> dict:
    """The window a rendered print names, and the session's other windows.

    Same keys on every row, windowed or not, so templates never branch on
    their presence. ``loading`` is the label a print is rendered under —
    ``Sep loading (band Sep–Oct 2026)`` (B39 #540).
    """
    session = windows.get(when)
    if session is None:
        return {"window": None, "loading": None, "windows_printed": None, "window_rows": []}
    return {
        "window": session.chosen.label,
        "loading": session.chosen.window.loading_label(),
        "windows_printed": len(session.printed),
        "window_rows": [
            {
                "window": quote.label,
                "home_value": quote.value,
                "quotes": quote.quotes,
                "is_chosen": index == 0,
            }
            for index, quote in enumerate(session.printed)
        ],
    }


# ---- window rolls across sessions (#513, B39 #540) ---------------------------
# A statistic over many sessions of a windowed source reads the nearest band
# of each session, so it crosses every roll in between — and a roll is a
# calendar-spread step, Sep cargo to Oct cargo, not a market move. Measured
# 2026-10-19 over data/history/argentina_fob.csv (462 sessions): MAGyP's exact
# nearest band holds at most 21 sessions in a row, median 2, because the
# circular re-cuts its curve daily and flips Sep ↔ Sep–Oct (58 flips a year on
# beans). Keyed on the loading month instead (A20 #536), beans, oil and meal
# roll 11.8 times a year, median run ≈ 20 sessions; AMS's Current slot rolls
# monthly by either key. Ranges and the drill-down chart *label* the loading
# months they span and break only at rolls — a band change is a tick, not a
# break; the signal chips, which a label cannot repair, read the current
# loading month alone (`_current_window_run`).
@dataclass(frozen=True)
class WindowSpan:
    """Which loading months a run of sessions' prints were for, where it
    rolled, and where the band changed inside a month."""

    months: tuple[LoadingMonth, ...]
    # (the session the new loading month was first printed, from, to), oldest first
    rolls: tuple[tuple[date, LoadingMonth, LoadingMonth], ...]
    # Sessions whose band differs from the session before, same loading month.
    band_changes: tuple[date, ...]
    # Sessions since the last roll (all of them where there is none).
    last_run: int

    def run(self) -> str:
        """``Sep loading · 20 sessions, 3 band changes`` — the newest month's run."""
        since = self.rolls[-1][0] if self.rolls else None
        changes = len([day for day in self.band_changes if since is None or day >= since])
        return (
            f"{self.months[-1].name} loading · {self.last_run} "
            f"session{'' if self.last_run == 1 else 's'}, {changes} "
            f"band change{'' if changes == 1 else 's'}"
        )

    def describe(self) -> str:
        if not self.rolls:
            return self.run()
        count = len(self.rolls)
        when, before, after = self.rolls[-1]
        return (
            f"spans {len(self.months)} loading months, rolled "
            f"{'once' if count == 1 else f'{count} times'} — last on "
            f"{_short_date(when)} ({before.name} → {after.name}); {self.run()}"
        )


def _window_span(windows: dict[date, SessionWindows], sessions) -> WindowSpan | None:
    """The loading months a set of sessions spans; None for an unwindowed
    source (or no sessions), which is what keeps every other leg's output
    unchanged."""
    days = sorted(sessions)
    if not windows or not days:
        return None
    rolls = tuple(
        (day, windows[prior].identity, windows[day].identity)
        for prior, day in zip(days[:-1], days[1:], strict=True)
        if windows[prior].identity != windows[day].identity
    )
    band_changes = tuple(
        day
        for prior, day in zip(days[:-1], days[1:], strict=True)
        if windows[prior].identity == windows[day].identity
        and windows[prior].band != windows[day].band
    )
    last_run = len([day for day in days if not rolls or day >= rolls[-1][0]])
    months = tuple(dict.fromkeys(windows[day].identity for day in days))
    return WindowSpan(months, rolls, band_changes, last_run)


def _current_window_run(
    rows: list[tuple[date, float, int]], windows: dict[date, SessionWindows]
) -> list[tuple[date, float, int]]:
    """The newest sessions printed for the newest session's loading month, unbroken.

    Unbroken rather than every session of that month: a circular that skips
    the month for a session and returns to it has rolled twice, and an
    indicator stitched over the gap is still stitched. Band changes inside
    the month do not cut the run — they are like-for-like (B39 #540). An
    unwindowed series is returned as it came — never rolls.
    """
    if not windows or not rows:
        return rows
    identity = windows[rows[-1][0]].identity
    start = len(rows)
    while start and windows[rows[start - 1][0]].identity == identity:
        start -= 1
    return rows[start:]


# ---------------------------------------------------------------------------
# Shared reads
# ---------------------------------------------------------------------------
@dataclass
class SiteContext:
    """Everything the nine builders share, loaded once for the whole site.

    Eight market pages × nine blocks is 72 builder calls; without this the
    CBOT reference series alone would be read eight times.
    """

    conn: Any
    today: date
    _cache: dict = field(default_factory=dict)
    _owned: Any = None
    # The generation instant the page is stamped with — what a stored cyclone
    # check is aged against (spec §7.1: passed in, never read inside the
    # assessor). ``open`` sets it; a context built without one is graded at
    # the end of its own ``today``, so an un-stamped render is deterministic
    # and never calls a check fresher than the clock would.
    now: datetime | None = None

    @classmethod
    def open(cls, *, today: date | None = None, now: datetime | None = None) -> SiteContext:
        """One connection for the whole site, or none at all.

        A missing DB is not an error here: ``compute_tiers`` already stubs
        every market in that case, and every read below returns nothing, so
        each block states its own honest empty reason rather than the run
        dying on a fresh clone or a docs-only CI job.
        """
        # Via app.markets rather than pipeline.connection directly: the tier
        # probe already resolves the connection there, and two import sites
        # would mean the tiers and the blocks could read different databases.
        import os

        from app.markets import get_connection

        now = now or datetime.now(timezone.utc)
        today = today or now.date()

        if not os.path.exists(config.DB_PATH):
            log.warning("No database at %s — every block renders an empty state", config.DB_PATH)
            return cls(conn=None, today=today, now=now)
        conn = get_connection()
        return cls(conn=conn, today=today, _owned=conn, now=now)

    def close(self) -> None:
        if self._owned is not None:
            try:
                self._owned.close()
            except Exception:  # noqa: BLE001 — closing must never fail a render
                log.debug("site context close failed", exc_info=True)
            self._owned = None

    def cached(self, key, produce: Callable[[], Any]):
        if key not in self._cache:
            self._cache[key] = produce()
        return self._cache[key]

    # -- generic source reads ------------------------------------------------
    def series(self, source: Source) -> dict[str, list[tuple[date, float, int]]]:
        """``{key: [(date, value, n_rows), ...]}`` oldest-first, per key.

        ``n_rows`` is how many stored rows one key/date collapsed into, and
        the value is their mean — the count travels with it, to be rendered
        rather than hidden. A source that prints several *shipment windows*
        per session (AMS 3147, MAGyP) declares a ``window_scheme`` and is never
        averaged across them: its value is the nearest window's, named by
        ``session_windows`` (B30 #504). Only rows inside that one window — two
        barge locations quoting it — are still collapsed into a mean.
        """
        return self._prints(source).series

    def _prints(self, source: Source, proof_column: str | None = None) -> _Prints:
        return self.cached(
            ("prints", source, proof_column),
            lambda: _read_prints(self.conn, source, self.today, proof_column=proof_column),
        )

    def session_windows(self, source: Source, key: str) -> dict[date, SessionWindows]:
        """``{session: SessionWindows}`` for one key of a windowed source (B30
        #504); empty where the source prints one window, or none, a session."""
        return self._prints(source).windows.get(key) or {}

    def leg_windows(self, leg) -> dict[date, SessionWindows]:
        """``session_windows`` for a ledger leg's own read (its proof column)."""
        return self._prints(leg.source, leg.trade_proof_column).windows.get(leg.key) or {}

    def latest(self, source: Source, key: str) -> tuple[date, float, int] | None:
        rows = self.series(source).get(key) or []
        return rows[-1] if rows else None

    def leg_prints(self, leg) -> list[tuple[date, float, int]]:
        """One ledger leg's *prints*, oldest-first — trade-proved where it can be.

        Deliberately not ``series()``: a leg whose venue re-dates a
        carry-forward row (SAFEX, #157) has rows that are not prints, and the
        ledger's whole claim is about which markets have repriced. Where the
        registry names a ``trade_proof_column`` the read enforces it here too,
        rather than trusting the fetcher to have done it — the invariant is
        cheap to state at both ends and expensive to discover at neither.
        """
        return self._prints(leg.source, leg.trade_proof_column).series.get(leg.key) or []

    # -- cyclone hazards (S2 #374 slice 4, #390) --------------------------------
    @property
    def generated_at(self) -> datetime:
        """The instant stored checks are aged against (see ``now``)."""
        if self.now is not None:
            return self.now.astimezone(timezone.utc)
        return datetime.combine(self.today, time(23, 59, 59), tzinfo=timezone.utc)

    def _hazard_read(self) -> tuple[dict[str, PlaceHazard], dict[str, str | None]]:
        return self.cached(("hazards",), lambda: _read_place_hazards(self.conn, self.today, self.generated_at))

    def place_hazards(self) -> dict[str, PlaceHazard]:
        """Every exposed place graded once for the whole site (spec §7.1)."""
        return self._hazard_read()[0]

    def cyclone_checked_at(self) -> dict[str, str | None]:
        """Each source's newest ``checked_at``; ``None`` where it never answered."""
        return self._hazard_read()[1]

    def leg_hazard(self, leg) -> LegHazard | None:
        return self.cached(
            ("leg_hazard", leg.leg_id),
            lambda: hazards.leg_hazard(leg.place_ids, self.place_hazards()),
        )

    def port_rain(self, place_id: str) -> dict | None:
        """The port-rain caption slot [P1 #7] — ``None`` until footprint weather
        (S1) owns the ECMWF fetch and fills ``{"tp15_mm", "wet_days", "stamp"}``.
        ``None`` is correct, not a blank to fill: there is no second observation
        yet, so the row renders without the caption."""
        return None

    # -- FX ------------------------------------------------------------------
    def fx_series(self, pair: str | None) -> list[tuple[date, float]]:
        """``<CCY>/USD`` closes, oldest-first. Empty when the pair is unset."""
        if not pair:
            return []
        return self.cached(("fx", pair), lambda: _read_fx(self.conn, pair, self.today))

    def fx(self, pair: str | None) -> tuple[date, float] | None:
        rows = self.fx_series(pair)
        return rows[-1] if rows else None

    def fx_rate(self, pair: str | None) -> float | None:
        latest = self.fx(pair)
        return latest[1] if latest else None

    def fx_on(self, pair: str | None, when: date) -> FxResolution:
        """The rate a print dated ``when`` converts at, under A1 (#298).

        Own date's close, else the newest prior close at most
        ``config.FX_ALIGNMENT_MAX_GAP_DAYS`` older (carried with both dates so
        the render can label the substitution), else withheld with a reason —
        and never a later-dated rate. The old ``fallback_to_oldest`` converted
        a print at a rate from its own future; A1 killed it, and dropping the
        session is now the only behaviour on every surface.

        Pass ``.rate`` to ``Source.to_usd_mt``; spread the rest onto the row
        with ``_fx_fields``.
        """
        return align_fx(pair, self.fx_series(pair), when)

    # -- the reference board -------------------------------------------------
    def reference_series(self, markets: dict[str, Market]) -> list[tuple[date, float]]:
        """CBOT's headline leg in USD/MT, oldest-first — every basis's other leg."""
        def build() -> list[tuple[date, float]]:
            ref = markets.get(REFERENCE_MARKET)
            if ref is None or ref.price is None:
                return []
            key = ref.price.headline_key or ref.price.keys[0]
            rows = self.series(ref.price).get(key) or []
            out = []
            for day, value, _ in rows:
                usd = ref.price.to_usd_mt(value, key, None)
                if usd is not None:
                    out.append((day, usd))
            return out
        return self.cached("reference_series", build)


def _fx_fields(source: Source, fx: FxResolution) -> dict[str, Any]:
    """The A1 annotation a rendered USD figure carries (#298 §3).

    Only a ``home_per_mt`` leg was converted at a rate at all, so only there
    can a date have been substituted or a figure withheld for want of one;
    every other unit gets the same keys, empty, so templates never branch on
    their presence.
    """
    if source.unit != "home_per_mt":
        return {
            "fx_observed_on": None, "fx_gap_days": None, "fx_aligned": None,
            "fx_label": None, "fx_reason": None,
        }
    aligned = fx.alignment
    return {
        "fx_observed_on": aligned.observed_on.isoformat() if aligned else None,
        "fx_gap_days": aligned.gap_days if aligned else None,
        "fx_aligned": aligned.aligned if aligned else None,
        "fx_label": aligned.label if aligned else None,
        "fx_reason": fx.reason,
    }


# ---------------------------------------------------------------------------
# Cyclone hazards — the one read, the one view (S2 #374 slice 4, #390)
#
# The assessment itself lives in analysis/hazards.py and is shared with the
# briefing; this file only reads the three cyclone tables plus the two
# data_freshness rows, hands them to the assessor, and shapes the result for
# the templates. ``now`` is the page-generation instant, passed in.
# ---------------------------------------------------------------------------
_CYCLONE_LAYERS: tuple[str, ...] = tuple(
    sorted({str(b["layer"]) for b in config.CYCLONE_BASINS.values() if b.get("layer")})
)


def _frame(conn, sql: str) -> pd.DataFrame:
    cursor = conn.execute(sql)
    columns = [column[0] for column in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def _read_place_hazards(
    conn, today: date, now: datetime
) -> tuple[dict[str, PlaceHazard], dict[str, str | None]]:
    """Grade every exposed place from the stored tables.

    With no connection every leg hazard is ``None`` — the same "no database"
    behaviour every other read has. A database with no cyclone tables at all
    is treated the same way and logged: the schema creates them on every
    pipeline run, so on a deployed site that branch never fires.
    """
    sources = {str(b["source"]) for b in config.CYCLONE_BASINS.values() if b.get("source")}
    none_checked: dict[str, str | None] = dict.fromkeys(sorted(sources))
    if conn is None:
        return {}, none_checked
    try:
        # Plain cursors, not pandas.read_sql_query: on an error pandas rolls the
        # connection back, which would discard a caller's uncommitted writes.
        status = _frame(conn, "SELECT * FROM cyclone_source_status")
        storms = _frame(conn, "SELECT * FROM cyclone_storms")
        track = _frame(conn, "SELECT * FROM cyclone_track_points")
        placeholders = ",".join("?" for _ in _CYCLONE_LAYERS)
        freshness = conn.execute(
            f"SELECT layer_name, status, last_success FROM data_freshness "  # noqa: S608 — placeholders
            f"WHERE layer_name IN ({placeholders})",
            _CYCLONE_LAYERS,
        ).fetchall()
    except Exception as exc:  # noqa: BLE001 — a missing table is "no database", not a crash
        log.warning("cyclone hazards: tables unavailable, nothing assessed (%s)", exc)
        return {}, none_checked
    layer_states: dict[str, str | None] = dict.fromkeys(_CYCLONE_LAYERS)
    last_success: dict[str, str | None] = dict.fromkeys(_CYCLONE_LAYERS)
    for layer, state, last in freshness:
        layer_states[str(layer)] = None if state is None else str(state)
        last_success[str(layer)] = None if last is None else str(last)
    graded = hazards.assess_places(
        hazards.active_places(config.PLACES, today),
        config.CYCLONE_BASINS,
        status, storms, track, layer_states, now,
        layer_last_success=last_success,
    )
    newest = hazards.newest_status(status)
    checked = {s: (str(newest[s]["checked_at"]) if s in newest else None) for s in none_checked}
    return graded, checked


# §8.2 — the State-column chip, by leg state and severity. ``None`` renders nothing.
def _hazard_chip(state: str, severity: str | None, partial: bool) -> tuple[str | None, str | None]:
    if state == "flag":
        if severity == "alert":
            return "storm", "hz-alert"
        if severity == "warning":
            return "storm", "hz-warning"
        return "storm: 3–5 days", "hz-quiet"
    if state == "watch":
        return "storm watch", "hz-quiet"
    if state == "failed":
        return "storm: no reading", "hz-quiet"
    if state == "stale":
        return "storm: stale", "hz-quiet"
    if state == "not_covered":
        return "storm: not covered", "hz-quiet"
    if state == "clear":
        return ("storm: part covered", "hz-quiet") if partial else (None, None)
    raise ValueError(f"_hazard_chip: unknown hazard state {state!r}")


_HAZARD_LEVELS = {34: "ts_force", 50: "50kt", 64: "hurricane_force"}


def _hazard_view(leg_hazard: LegHazard | None, anchor: str) -> dict[str, Any] | None:
    """The row's hazard (spec §7.2): ``None`` when the leg has no exposed place."""
    if leg_hazard is None:
        return None
    primary = leg_hazard.primary
    chip, chip_class = _hazard_chip(leg_hazard.state, leg_hazard.severity, leg_hazard.partial)
    storm = primary if leg_hazard.state in ("flag", "watch") and primary is not None else None
    return {
        "state": leg_hazard.state,
        "severity": leg_hazard.severity,
        "partial": leg_hazard.partial,
        "chip": chip,
        "chip_class": chip_class,
        "text": leg_hazard.text,
        "level": _HAZARD_LEVELS.get(storm.band_kt) if storm is not None and storm.band_kt else None,
        "storm": storm.storm_name if storm is not None else None,
        "storm_id": storm.storm_id if storm is not None else None,
        "source": storm.source if storm is not None else None,
        "first_arrival_h": storm.first_arrival_tau_h if storm is not None else None,
        "first_arrival_at": storm.first_arrival_at if storm is not None else None,
        "reason": (
            primary.reason
            if primary is not None and leg_hazard.state in ("not_covered", "stale", "failed")
            else None
        ),
        "uncovered": [
            p.short for place_id in leg_hazard.uncovered
            for p in leg_hazard.places if p.place_id == place_id
        ],
        "anchor": anchor,
    }


def _hazard_anchor(leg) -> str:
    # Block 06 of the leg's OWNING page — the block id is `weather` (app/blocks.py).
    return f"{leg.href}#block-weather"


def ledger_hazard_views(markets: dict[str, Market], ctx: SiteContext) -> dict[str, dict[str, Any] | None]:
    """``{leg_id: hazard view}`` for every ledger leg, from the run's one context.

    The origins page's seam (spec §7.3): it never builds a ledger row, so the
    generator computes the views once here and hands them over.
    """
    out: dict[str, dict[str, Any] | None] = {}
    for market in markets.values():
        if market.ledger is None:
            continue
        for leg in market.ledger.legs:
            if leg.leg_id not in out:
                out[leg.leg_id] = _hazard_view(ctx.leg_hazard(leg), _hazard_anchor(leg))
    return out


def hazard_flag_rows(ctx: SiteContext, markets: dict[str, Market], *, run_date: date) -> list[dict[str, Any]]:
    """What this edition published, as ``hazard_flags`` rows (spec §7.4).

    One row per place in ``flag`` or ``watch``; ``legs`` is every ledger leg
    whose ``place_ids`` include it, in registry order. The registry
    coordinates are frozen into the row so a later edit cannot rewrite what
    was said. Nothing else is archived — a clear or uncovered place was not
    published as a hazard.
    """
    legs_by_place: dict[str, list[str]] = {}
    for market in markets.values():
        if market.ledger is None:
            continue
        for leg in market.ledger.legs:
            for place_id in leg.place_ids:
                carried = legs_by_place.setdefault(place_id, [])
                if leg.leg_id not in carried:
                    carried.append(leg.leg_id)
    rows: list[dict[str, Any]] = []
    for place_id, p in ctx.place_hazards().items():
        if p.state not in ("flag", "watch"):
            continue
        place = config.PLACES[place_id]
        rows.append({
            "run_date": run_date.isoformat(),
            "place_id": place_id,
            "source": p.source,
            "storm_id": p.storm_id,
            "state": p.state,
            "severity": p.severity,
            "band_kt": p.band_kt,
            "storm_name": p.storm_name,
            "advisory": p.advisory,
            "issued_at": p.issued_at,
            "first_arrival_tau_h": p.first_arrival_tau_h,
            "first_arrival_at": p.first_arrival_at,
            "closest_km": p.closest_km,
            "closest_tau_h": p.closest_tau_h,
            "place_lat": float(place["lat"]),
            "place_lon": float(place["lon"]),
            "legs": ",".join(legs_by_place.get(place_id, [])),
        })
    return rows


def _read_prints(
    conn,
    source: Source,
    today: date,
    *,
    proof_column: str | None = None,
) -> _Prints:
    if source.window_scheme is None:
        return _Prints(_read_series(conn, source, today, proof_column=proof_column), {})
    return _read_windowed(conn, source, today, proof_column=proof_column)


def _proof_clause(proof_column: str | None) -> str:
    # M12/#157: where a venue publishes a field that proves a trade happened,
    # a row failing it is not a print. `<= 0` is proof of no trade and the row
    # goes; NULL is proof of nothing and the row stays — dropping it would
    # invent an outage, which is the one thing a "who has repriced" surface
    # must never do.
    return f"AND ({proof_column} IS NULL OR {proof_column} > 0) " if proof_column else ""


def _read_windowed(
    conn,
    source: Source,
    today: date,
    *,
    proof_column: str | None = None,
) -> _Prints:
    """A windowed source's prints: one named window per key per session.

    The SQL ``AVG`` the unwindowed read uses is exactly the defect here (B30
    #504) — MAGyP's circular and AMS 3147 each print a forward curve per
    session, and their mean is a cargo nobody can ship. The nearest window
    still open on the session is the print; the rest ride along so a page can
    list them. Rows sharing one window (two barge locations) are still
    averaged, and counted, because that is a mean over places for one cargo.

    A row whose window cannot be read raises rather than being skipped: the
    nearest window cannot be named while one row's window is unknown — that
    row may be the nearest.
    """
    series: dict[str, list[tuple[date, float, int]]] = {key: [] for key in source.keys}
    windows: dict[str, dict[date, SessionWindows]] = {}
    if conn is None:
        return _Prints(series, windows)
    lookback = LOOKBACK_DAYS_BY_CADENCE.get(source.cadence, 400)
    cutoff = (today - _days(lookback)).isoformat()
    placeholders = ",".join("?" for _ in source.keys)
    window_select = ", ".join(source.window_columns)
    sql = (
        f"SELECT {source.key_column}, {source.date_column}, "  # noqa: S608 — identifiers come from the registry
        f"{source.value_column}, {window_select} "
        f"FROM {source.table} "
        f"WHERE {source.key_column} IN ({placeholders}) "
        f"AND {source.date_column} >= ? AND {source.value_column} IS NOT NULL "
        f"{_proof_clause(proof_column)}"
        f"ORDER BY {source.date_column}"
    )
    try:
        rows = conn.execute(sql, (*source.keys, cutoff)).fetchall()
    except sqlite3.OperationalError as exc:
        # A missing table is data (a fresh clone, a docs-only job). Anything
        # else — a mistyped `window_columns` above all — is a registry bug
        # that would otherwise render as a quiet "no data".
        if "no such table" not in str(exc):
            raise
        log.debug("block read: %s unavailable (%s)", source.table, exc)
        return _Prints(series, windows)

    sessions: dict[tuple[str, date], dict[ShipmentWindow, list[float]]] = {}
    for key, raw_date, value, *raw_window in rows:
        day = _parse_date(raw_date)
        if day is None:
            continue
        window = source.parse_window(tuple(raw_window), day)
        if window is None:
            raise ValueError(
                f"{source.table}: {key} on {day.isoformat()} carries an unreadable "
                f"shipment window {tuple(raw_window)!r} ({source.window_scheme}) — the "
                "nearest window cannot be named while one row's window is unknown"
            )
        sessions.setdefault((str(key), day), {}).setdefault(window, []).append(float(value))

    for (key, day), by_window in sorted(sessions.items(), key=lambda item: item[0][1]):
        printed = tuple(
            WindowQuote(window=window, value=sum(values) / len(values), quotes=len(values))
            # The origins domain's one nearest-window rule, shared with the
            # briefing: open windows first, a sailed one only if all have.
            for window, values in sorted(
                by_window.items(), key=lambda item: nearest_window_key(item[0], day)
            )
        )
        session = SessionWindows(printed)
        windows.setdefault(key, {})[day] = session
        series.setdefault(key, []).append((day, session.chosen.value, session.chosen.quotes))
    return _Prints(series, windows)


def _read_series(
    conn,
    source: Source,
    today: date,
    *,
    proof_column: str | None = None,
) -> dict[str, list[tuple[date, float, int]]]:
    if conn is None:
        return {key: [] for key in source.keys}
    lookback = LOOKBACK_DAYS_BY_CADENCE.get(source.cadence, 400)
    cutoff = (today - _days(lookback)).isoformat()
    placeholders = ",".join("?" for _ in source.keys)
    proof_clause = _proof_clause(proof_column)
    sql = (
        f"SELECT {source.key_column}, {source.date_column}, "  # noqa: S608 — identifiers come from the registry
        f"AVG({source.value_column}), COUNT({source.value_column}) "
        f"FROM {source.table} "
        f"WHERE {source.key_column} IN ({placeholders}) "
        f"AND {source.date_column} >= ? AND {source.value_column} IS NOT NULL "
        f"{proof_clause}"
        f"GROUP BY {source.key_column}, {source.date_column} "
        f"ORDER BY {source.date_column}"
    )
    out: dict[str, list[tuple[date, float, int]]] = {key: [] for key in source.keys}
    try:
        rows = conn.execute(sql, (*source.keys, cutoff)).fetchall()
    except Exception as exc:  # noqa: BLE001 — a missing table is data, not a crash
        log.debug("block read: %s unavailable (%s)", source.table, exc)
        return out
    for key, raw_date, value, count in rows:
        parsed = _parse_date(raw_date)
        if parsed is None or value is None:
            continue
        out.setdefault(str(key), []).append((parsed, float(value), int(count or 1)))
    return out


def _read_fx(conn, pair: str, today: date) -> list[tuple[date, float]]:
    if conn is None:
        return []
    cutoff = (today - _days(LOOKBACK_DAYS_BY_CADENCE["daily"])).isoformat()
    try:
        rows = conn.execute(
            "SELECT Date, Close FROM currencies WHERE pair = ? AND Date >= ? "
            "AND Close IS NOT NULL ORDER BY Date",
            (pair, cutoff),
        ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug("block read: currencies unavailable (%s)", exc)
        return []
    out = []
    for raw_date, close in rows:
        parsed = _parse_date(raw_date)
        if parsed is not None:
            out.append((parsed, float(close)))
    return out


def _days(n: int):
    from datetime import timedelta
    return timedelta(days=n)


def _parse_date(raw) -> date | None:
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


def _pct_change(current: float | None, prior: float | None) -> float | None:
    if current is None or not prior:
        return None
    return (current - prior) / abs(prior) * 100.0


def _age_days(today: date, when: date | None) -> int | None:
    return None if when is None else (today - when).days


# ---------------------------------------------------------------------------
# 01 Price
# ---------------------------------------------------------------------------
def price_block(market: Market, ctx: SiteContext, **_) -> tuple[str, str, dict]:
    # Narrowing the Optional descriptor: build_blocks only dispatches a
    # builder when absent_reason() found a descriptor, so None here is a
    # dispatch bug — and the assert turns it into M8's generation-error
    # envelope rather than an AttributeError three lines later.
    source = market.price
    assert source is not None, f"price_block dispatched for {market.slug} with no price source"
    rows_by_key = ctx.series(source)
    legs: list[dict] = []
    ordered = _headline_first(source)

    for key in ordered:
        rows = rows_by_key.get(key) or []
        if not rows:
            continue
        when, value, n_rows = rows[-1]
        fx = ctx.fx_on(market.currency_pair, when)
        windows = ctx.session_windows(source, key)
        changes = {}
        move_notes: list[str] = []
        for back in CHANGE_LOOKBACK_OBS:
            prior = rows[-1 - back] if len(rows) > back else None
            # B30 #504 / B39 #540: a move is only a move inside one loading
            # month; withheld across a roll it says so rather than rendering
            # blank, and shown across a band change inside the month it names
            # the bands.
            move = "session move" if back == 1 else f"{back}-obs move"
            if prior is not None and not _same_identity(windows, when, prior[0]):
                move_notes.append(_roll_note(windows, when, prior[0], move))
                prior = None
            elif prior is not None and (band := _band_note(windows, when, prior[0])):
                move_notes.append(f"{move}: {band}")
            changes[f"chg_{back}_pct"] = _pct_change(value, prior[1] if prior else None)
        window = _window_fields(windows, when)
        for window_row in window["window_rows"]:
            window_row["usd_mt"] = source.to_usd_mt(window_row["home_value"], key, fx.rate)
        legs.append({
            "key": key,
            "label": source.key_label(key),
            "is_headline": key == (source.headline_key or ordered[0]),
            "home_value": value,
            # #230: the venue's own number gets the venue's own unit, and it is
            # only a *second* quote where the venue does not already publish
            # USD/MT. Argentina's FOB is dollars; labelling it ARS stated a
            # figure wrong by three orders of magnitude. Same answer the ledger
            # gives (`_ledger_row`), by the same registry call.
            "home_unit": _home_unit(source, key, market.home_currency),
            "has_home_quote": source.has_native_quote,
            "usd_mt": source.to_usd_mt(value, key, fx.rate),
            "as_of": when.isoformat(),
            "age_days": _age_days(ctx.today, when),
            # Several quotes on one date are averaged (see SiteContext.series);
            # say so rather than presenting a mean as a single print. On a
            # windowed source these are places quoting one window, never
            # several windows (B30 #504).
            "quotes": n_rows,
            # B30 #504: the shipment window this print is for, and every
            # window the venue printed that session for the per-window table.
            **window,
            "move_notes": move_notes,
            # A1 #298: which day's rate the USD figure was struck at, or why
            # there is none. Rendered wherever it differs from `as_of`.
            **_fx_fields(source, fx),
            **changes,
        })

    if not legs:
        return STATE_EMPTY, (
            f"{source.table} holds no {source.cadence} observation inside its "
            f"{source.max_age_days}-day budget"
        ), {}

    headline = next(leg for leg in legs if leg["is_headline"]) if any(
        leg["is_headline"] for leg in legs) else legs[0]
    signals, signals_note = _windowed_signals(
        rows_by_key.get(headline["key"]) or [],
        ctx.session_windows(source, headline["key"]),
        headline["key"],
    )
    return STATE_OK, "", {
        "legs": legs,
        "headline": headline,
        "quote_kind": source.quote_kind,
        "cadence": source.cadence,
        "venue": market.venue,
        # No block-level `home_currency`: the unit belongs to the leg, not the
        # market (#230). One table can hold three units; a page-wide currency
        # label is the assumption that produced "ARS 451.67/MT" for dollars.
        "signals": signals,
        # #513: on a windowed source, which window the chips were read on, or
        # why there are none. None on every other source.
        "signals_note": signals_note,
        # M3 #145: the stack stores no time of day, so a page can honestly show
        # the date a venue printed and never a timestamp.
        "no_timestamp_note": "dates only — this stack stores no time of day",
    }


def _headline_first(source: Source) -> list[str]:
    keys = list(source.keys)
    if source.headline_key and source.headline_key in keys:
        keys.remove(source.headline_key)
        keys.insert(0, source.headline_key)
    return keys


def _windowed_signals(
    rows: list[tuple[date, float, int]], windows: dict[date, SessionWindows], key: str
) -> tuple[list[dict], str | None]:
    """The chips, read on the current loading month alone where the source has windows.

    A4 #301's rule for futures rolls, applied to loading-month rolls (#513,
    B39 #540): an MA crossover or MACD turn a roll manufactured is a wrong
    signal, and a label beside it would not make it a right one. So the chips
    see only the newest loading month's unbroken run — band changes inside it
    are like-for-like — and until that run holds ``SIGNAL_MIN_OBS`` sessions
    they are withheld, with the reason, never a silent blank.
    """
    run = _current_window_run(rows, windows)
    if run is rows:
        return _signal_chips(rows, key), None
    current = windows[run[-1][0]].identity
    name = f"{current.name} loading"
    since = _short_date(run[0][0])
    if len(run) == len(rows):
        return _signal_chips(run, key), f"signals read on {name} alone, since {since}"
    previous = windows[rows[-len(run) - 1][0]].identity
    rolled = f"loading month rolled {previous.name} → {current.name} on {since}"
    if len(run) < SIGNAL_MIN_OBS:
        return [], (
            f"signals withheld — {rolled}, and {name} has {len(run)} "
            f"session{'' if len(run) == 1 else 's'} of the {SIGNAL_MIN_OBS} they need; "
            "read across the roll they would call a calendar-spread step a move"
        )
    return _signal_chips(run, key), f"signals read on {name} alone — {rolled}"


def _signal_chips(rows: list[tuple[date, float, int]], key: str) -> list[dict]:
    """M2 #144: signals ride inside block 01 as severity chips, not a tenth block.

    Computed from the market's own close series, so every market gets the same
    treatment — the price-only detectors, since a generic source has no volume
    column and a volume signal we cannot compute must be absent, not faked.
    """
    if len(rows) < SIGNAL_MIN_OBS:
        return []
    try:
        from analysis.signals import (
            detect_ma_crossovers,
            detect_macd_crossover,
            detect_rsi_extremes,
            suppress_near_roll_signals,
        )
        from analysis.technical import compute_all_technicals

        frame = pd.DataFrame(
            {"Close": [value for _, value, _ in rows]},
            index=pd.to_datetime([day for day, _, _ in rows]),
        )
        frame = compute_all_technicals(frame)
        signals: list[dict] = []
        for detect in (detect_ma_crossovers, detect_rsi_extremes, detect_macd_crossover):
            signals.extend(detect(frame, key))
        # Chips are computed from the market's own close series, which for a
        # curve commodity is the provider front-month — near an estimated
        # roll the chip is withheld, not footnoted (A4 #301).
        signals = suppress_near_roll_signals(signals)
    except Exception:  # noqa: BLE001 — chips are decoration; the price is the block
        log.warning("signal chips failed for %s", key, exc_info=True)
        return []
    return [
        {
            "severity": sig.get("severity", "info"),
            "message": sig.get("description") or sig.get("message", ""),
        }
        for sig in signals
    ]


# ---------------------------------------------------------------------------
# 02 Propagation ledger
# ---------------------------------------------------------------------------
def ledger_block(market: Market, ctx: SiteContext, **_) -> tuple[str, str, dict]:
    """The propagation ledger: who has repriced, and who has not printed.

    Shape from M3 #145, ordering from M20 #236 — rows render in the registry's
    declared order (row position is role in the trade, never recency; the state
    pill and the leading-edge caption carry recency), each dual-quoting USD/MT
    over its home print, both moves with an ``FX`` tag when they diverge, and a
    state pill so silence can never read as flat. Counterpart set from M12
    #161, read from the registry (``config.LEDGERS``) rather than chosen here.

    Two things this builder is careful about, both of which would otherwise
    produce a confident wrong number:

    **A row stamp is not a print.** Where a venue publishes a field proving a
    trade, ``leg_prints`` enforces it — Grain SA re-dates a carried SAFEX price
    with Volume 0 (#157), and a ledger that read that as a reprice would be
    lying in exactly the place it claims to be authoritative.

    **A spread is one session's number.** The legs print on different days —
    that is the point of the surface — so the spread against the pinned leg is
    struck on the most recent session *both* printed, and its date is rendered
    whenever that is not the row's own. Subtracting two different days would
    manufacture an arbitrage out of a calendar gap.
    """
    ledger = market.ledger
    assert ledger is not None, f"ledger_block dispatched for {market.slug} with no ledger"
    own_prints = ctx.leg_prints(ledger.own)
    if not own_prints:
        return STATE_EMPTY, (
            f"{ledger.own.label} has no trade-proved print inside its "
            f"{ledger.own.source.max_age_days}-day budget — with no pinned leg there is "
            "nothing for the counterparts to be a spread against"
        ), {}

    rows = [
        _ledger_row(
            leg,
            ctx,
            is_own=(index == 0),
            is_reference=leg.leg_id in ledger.reference_leg_ids,
            own_prints=own_prints if index else None,
            own_leg=ledger.own,
            drilldown=True,
        )
        for index, leg in enumerate(ledger.legs)
    ]

    # Declared order (M20 #236): row position is role in the trade, never
    # recency. The own leg leads because the registry declares it first, and a
    # reference row rides last because the load-time gate in app/markets.py
    # refuses any other declaration. Recency is carried entirely by the state
    # pill and the leading-edge caption.
    ordered = rows

    dated = [row["as_of"] for row in ordered if row["as_of"]]
    leading_edge = max(dated) if dated else None
    for row in ordered:
        row["state"], row["state_detail"], row["overdue"] = _ledger_state(
            row, leading_edge=leading_edge, today=ctx.today
        )

    # M21 #250: the dashed overlay is drawn only where the ledger names a leg
    # from another market. That is a structural read of the registry, not a
    # per-market branch — India's ledger is two of its own state medians
    # because no foreign bean is connected to a mandi by trade (#206), so there
    # is no foreign leg to draw. Drawing one anyway would render the
    # `policy_blocked` +66% as a gap that closes, the read #222 refused on the
    # basis block. The *reason* is the ledger's own note and is not restated
    # here: another single-market ledger could arrive for a different reason
    # entirely, and this sentence would then be a false causal claim.
    foreign = [leg for leg in ledger.legs if leg.market.slug != market.slug]
    reference = _reference_payload(ledger.own, ctx, own_prints) if foreign else None
    reference_note = None if foreign else (
        f"No reference line: every leg on this ledger is {market.name}'s own, so there "
        "is no second market to draw against — see the note below this table for why "
        "this ledger names none."
    )
    fx_series = {}
    for row in ordered:
        pair = (row["drill"] or {}).get("fx_pair")
        if pair and pair not in fx_series:
            payload = _fx_payload(pair, ctx)
            if payload is not None:
                fx_series[pair] = payload

    return STATE_OK, "", {
        "rule": ledger.rule,
        "rule_statement": ledger.rule_statement,
        "note": ledger.note,
        "rows": ordered,
        "leading_edge": leading_edge,
        "has_drilldown": True,
        # S2 #374 §7.2: the chip renders on market-page ledgers only. An
        # explicit flag from the builder, never inferred — the has_drilldown
        # pattern exactly.
        "has_hazard": True,
        "reference": reference,
        "reference_note": reference_note,
        "fx_series": fx_series,
        "today": ctx.today.isoformat(),
        "drilldown_default_days": LEDGER_DRILLDOWN_DEFAULT_DAYS,
        "line_min_obs": LEDGER_LINE_MIN_OBS,
        "chart_min_obs": LEDGER_CHART_MIN_OBS,
        # M20 #236: a caption date the reader has to attribute by scanning is
        # half a fact — name the leg(s) that produced it. Plural because the
        # stack stores no time of day, so two legs on the leading date are
        # genuinely joint holders.
        "leading_edge_legs": [
            row["label"] for row in ordered if row["as_of"] == leading_edge
        ] if leading_edge else [],
        "has_spread": True,
        "commodity": "Soybean",
        # M12 decision 2 — one commodity per ledger, named on the block. In a
        # single USD/MT column a per-row good label is not strong enough to stop
        # five numbers reading as one price in five places.
        "commodity_note": (
            "Every row is the soybean. Meal and oil are a different good and would "
            "be a second ledger, not more rows here."
        ),
        # M20 #236 wording, M3 #145 / M4 #146 constraint: position states role,
        # and the stack stores no time of day, so the page must not imply
        # anything finer than the date.
        "no_timestamp_note": (
            "ordered as declared in the registry — role in the trade. This stack "
            "stores no time of day, so no finer ordering than the date exists"
        ),
    }


def headline_ledger(markets: dict[str, Market], ctx: SiteContext) -> tuple[str, str, dict]:
    """The headline page's eight-row ledger (M2 #144 / M12 #161).

    Rows are **markets** here, not legs, and the market cell is the link — this
    is the headline's sole per-market presence, so a reader scans it to decide
    which page to open. Three consequences, all of them M12's:

    * **No spread column.** There is no pinned own leg on the headline, so
      there is nothing for a spread to be against. Levels and moves only.
    * **Europe carries no value.** Its only leg is the EC's weekly assessment —
      out of the ledger's cadence, not missing — so it renders
      ``out_of_cadence`` rather than a value that would invite comparison
      against seven daily prints, and rather than a ``dark`` pill that would
      report an outage that is not happening.
    * **Nigeria is dark**, with the registry's own reason: no price leg of any
      kind is ingested.
    """
    rows: list[dict] = []
    for market in markets.values():
        if market.ledger is not None:
            leg = market.ledger.own
            row = _ledger_row(
                leg, ctx, is_own=False, is_reference=False, own_prints=None, own_leg=leg
            )
            row["forced_state"] = None
        else:
            row = _headline_placeholder(market)
        # On the headline the row IS the market, so the market name leads and
        # the leg label rides underneath as the thing actually quoted.
        row["market_label"] = market.name
        row["href"] = market.url
        rows.append(row)

    dated = [row["as_of"] for row in rows if row["as_of"]]
    leading_edge = max(dated) if dated else None
    for row in rows:
        forced = row.pop("forced_state", None)
        if forced:
            row["state"], row["state_detail"], row["overdue"] = forced
        else:
            row["state"], row["state_detail"], row["overdue"] = _ledger_state(
                row, leading_edge=leading_edge, today=ctx.today
            )

    if not dated:
        return STATE_EMPTY, "no market on the ledger has a print in its window", {}
    return STATE_OK, "", {
        "rows": rows,
        "leading_edge": leading_edge,
        # M21 #250 left the headline alone: no pinned leg to overlay, and the
        # market cell is already this row's affordance.
        "has_drilldown": False,
        # [P1 #2] / spec §14.3: nothing on the headline in v1. Its rows are
        # markets, so a port flag would be attributed to a whole market.
        "has_hazard": False,
        # On the headline the row is the market, so the market names the edge.
        "leading_edge_legs": [
            row["market_label"] for row in rows if row["as_of"] == leading_edge
        ] if leading_edge else [],
        "has_spread": False,
        "commodity": "Soybean",
        "commodity_note": (
            "Every row is the soybean — Europe's row names its own good, which is "
            "why it carries no value here."
        ),
        "no_timestamp_note": (
            "ordered as declared in the registry — role in the trade. This stack "
            "stores no time of day, so no finer ordering than the date exists."
        ),
    }


def _headline_placeholder(market: Market) -> dict:
    """A row for a market with no ledger — valueless, and it says which kind."""
    source = market.price
    row: dict[str, Any] = {
        "leg_id": None,
        "label": source.label if source is not None else market.venue,
        "market_slug": market.slug,
        "market_name": market.name,
        "href": market.url,
        "kind": source.quote_kind if source is not None else None,
        "kind_label": (
            quote_kind_label(source.quote_kind)
            if source is not None and source.quote_kind else None
        ),
        "is_own": False,
        "is_reference": False,
        "expected_gap_days": None,
        "budget_days": None,
        "trade_proved": False,
        "as_of": None,
        "age_days": None,
        "usd_mt": None,
        "home_value": None,
        "home_unit": "",
        "has_home_quote": False,
        "usd_chg_pct": None,
        "home_chg_pct": None,
        "fx_tag": False,
        "fx_note": None,
        "quotes": None,
        "spread_usd_mt": None,
        "spread_as_of": None,
        "spread_note": None,
        "drill": None,
        "band": None,
        "hazard": None,
    }
    if source is None:
        row["forced_state"] = (LEDGER_STATE_DARK, market.absent_reason("price"), True)
    else:
        row["label"] = source.label or (source.headline_key or source.keys[0])
        row["forced_state"] = (
            LEDGER_STATE_OUT_OF_CADENCE,
            f"{source.cadence} assessment — outside the ledger's daily cadence, "
            "so no value is shown here (M10 #151)",
            False,
        )
    return row


def _ledger_row(
    leg,
    ctx: SiteContext,
    *,
    is_own: bool,
    is_reference: bool,
    own_prints: list[tuple[date, float, int]] | None,
    own_leg,
    drilldown: bool = False,
) -> dict:
    """One ledger row, before its state pill is set (that needs the whole set).

    ``drilldown`` is an explicit flag the caller passes, never inferred from
    ``has_spread``: that is a statement about the spread column, and tying the
    two together would couple two unrelated decisions. The headline passes
    False — its rows are markets, the market cell is already the link, and an
    expansion would compete with the one affordance that row exists to offer.
    """
    source = leg.source
    owner = leg.market
    prints = ctx.leg_prints(leg)

    row = {
        "leg_id": leg.leg_id,
        "label": leg.label,
        "market_slug": owner.slug,
        "market_name": owner.name,
        "href": leg.href,
        "kind": source.quote_kind,
        # The venue plus the price type. One USD/MT column carrying a delayed
        # board close, a cash assessment and a decreed minimum needs each row to
        # say which it is; "board" alone reads as a settlement this stack does
        # not hold.
        "kind_label": quote_kind_label(source.quote_kind) if source.quote_kind else None,
        "is_own": is_own,
        "is_reference": is_reference,
        "expected_gap_days": leg.expected_gap_days,
        # The same recency answer main.py grades this layer on — one number in
        # the codebase, so the site cannot call a source alive that the pipeline
        # has already failed.
        "budget_days": source.max_age_days,
        "trade_proved": leg.trade_proof_column is not None,
        "as_of": None,
        "age_days": None,
        "usd_mt": None,
        "home_value": None,
        "home_unit": _home_unit(source, leg.key, owner.home_currency),
        "has_home_quote": source.has_native_quote,
        "usd_chg_pct": None,
        "home_chg_pct": None,
        "fx_tag": False,
        "fx_note": None,
        "quotes": None,
        # B30 #504: the shipment window the row's print is for, where the
        # venue prints several. None on an unwindowed leg.
        "window": None,
        "loading": None,
        "windows_printed": None,
        "move_note": None,
        "spread_usd_mt": None,
        "spread_as_of": None,
        "spread_note": None,
        # M21 #250 — both None on the headline, by the flag above.
        "drill": None,
        "band": None,
        # S2 #374 §7.2 — set for every row whether or not it has prints, so
        # every ledger that lists the leg inherits its port flag with no
        # per-page code. None when the leg has no exposed place.
        "hazard": _hazard_view(ctx.leg_hazard(leg), _hazard_anchor(leg)),
    }
    if not prints:
        return row

    when, value, quotes = prints[-1]
    fx = ctx.fx_on(owner.currency_pair, when)
    windows = ctx.leg_windows(leg)
    window = _window_fields(windows, when)
    row.update({
        "as_of": when.isoformat(),
        "age_days": _age_days(ctx.today, when),
        "usd_mt": source.to_usd_mt(value, leg.key, fx.rate),
        "home_value": value,
        "quotes": quotes,
        "window": window["window"],
        "loading": window["loading"],
        "windows_printed": window["windows_printed"],
        **_fx_fields(source, fx),
    })

    # B30 #504 / B39 #540: across a loading-month roll the "move" is a calendar
    # spread, so it is withheld — with its reason, like spread_note — never
    # credited to the market. Across a band change inside the month it is
    # like-for-like: shown, with the bands named.
    if len(prints) > 1 and not _same_identity(windows, when, prints[-2][0]):
        row["move_note"] = _roll_note(windows, when, prints[-2][0], "move")
    elif len(prints) > 1:
        prior_when, prior_value, _ = prints[-2]
        row["move_note"] = _band_note(windows, when, prior_when)
        row["home_chg_pct"] = _pct_change(value, prior_value)
        prior_usd = source.to_usd_mt(prior_value, leg.key, ctx.fx_on(owner.currency_pair, prior_when).rate)
        row["usd_chg_pct"] = _pct_change(row["usd_mt"], prior_usd)
        # M3 #145: show both moves, and say so when the currency did the work.
        # A leg quoted in its own currency can be up at home and down in USD;
        # printing one number would attribute an FX move to the market.
        if (
            source.unit == "home_per_mt"
            and row["usd_chg_pct"] is not None
            and row["home_chg_pct"] is not None
            and abs(row["usd_chg_pct"] - row["home_chg_pct"]) >= FX_DIVERGENCE_PP
        ):
            row["fx_tag"] = True
            row["fx_note"] = (
                f"{owner.currency_pair} moved between prints — the USD and "
                f"{owner.home_currency} moves are not the same number"
            )

    if own_prints is not None:
        row["spread_usd_mt"], row["spread_as_of"], row["spread_note"] = _spread(
            leg, prints, own_leg, own_prints, ctx
        )

    if drilldown:
        row["drill"] = _leg_drilldown(leg, ctx, prints)
        row["band"] = _range_band(leg, ctx, prints, row["usd_mt"])
    return row


def _short_date(when: date | None) -> str | None:
    """``29 Jul`` — the ledger's own date vocabulary, no year, no leading zero."""
    return None if when is None else when.strftime("%d %b").lstrip("0")


def _drilldown_treatment(count: int) -> str:
    """Which of the three sparse-history bands a count of *drawn points* falls in.

    Drawn, not fetched: a print that could not be converted to USD/MT is not a
    dot, so counting it would let an 8-dot "line" window render five dots — the
    caption asserting a density the picture does not have.
    """
    if count >= LEDGER_LINE_MIN_OBS:
        return "line"
    if count >= LEDGER_CHART_MIN_OBS:
        return "dots"
    return "none"


def _leg_points(leg, ctx: SiteContext, prints: list[tuple[date, float, int]]):
    """One leg's prints, clipped to the widest window and then to the cap.

    Returns ``(kept, total_inside_window)``. Clipping happens here, at build
    time, because the payload ships inline in the page: the prototype went
    26 KB → 89 KB carrying five legs unclipped.

    Bounded at *both* ends. A future-dated row — a venue stamping a forward
    date, a timezone slip in a fetcher — would otherwise be counted server-side
    and dropped client-side, so the caption would claim one more observation
    than the chart draws.
    """
    cutoff = ctx.today - _days(max(LEDGER_DRILLDOWN_WINDOW_DAYS))
    inside = [point for point in prints if cutoff <= point[0] <= ctx.today]
    return inside[-LEDGER_DRILLDOWN_MAX_OBS:], len(inside)


def _rate_at_or_before(pair: str | None, rates: list[tuple[date, float]], when: date) -> float | None:
    """A1's rate for one chart point — **None** where there is none.

    The same ``align_fx`` the level uses, on the series the pane already holds:
    a later-dated rate is never used, and a prior one only inside the cap. A
    chart multiplies any laxer rule — the same wrong rate applied to a run of
    old prints draws a stretch of movement that is the currency's, attributed
    to the venue, on a picture that reads as evidence. Blank, and the pane
    says how many were withheld.
    """
    return align_fx(pair, rates, when).rate


def _usd_at_each_date(
    leg, ctx: SiteContext, points, *, places: int | None = 2
) -> list[float | None]:
    """Every observation converted at **its own date's** rate, else None.

    Converting a historical series at today's rate produces a chart in which the
    venue appears to have moved on days it did not — which is the exact error
    the separate FX panel exists to expose, so making it here would be
    self-defeating.

    Only a ``home_per_mt`` leg needs a rate at all: ``to_usd_mt`` ignores the
    ``fx`` argument for ``usd_per_bushel`` and ``native_exchange``, which are
    unit conversions inside one currency.

    ``places`` rounds the *payload*, which ships inline and where the third
    decimal of a USD/MT price is bytes rather than information. The range band
    passes ``None``: it is compared against the row's own unrounded level, and a
    band rounded inwards can end a hair below the number sitting in it.
    """
    needs_rate = leg.source.unit == "home_per_mt"
    rates = ctx.fx_series(leg.market.currency_pair) if needs_rate else []
    out: list[float | None] = []
    for day, value, _ in points:
        rate = _rate_at_or_before(leg.market.currency_pair, rates, day) if needs_rate else None
        if needs_rate and rate is None:
            out.append(None)
            continue
        usd = leg.source.to_usd_mt(value, leg.key, rate)
        out.append(None if usd is None else (usd if places is None else round(usd, places)))
    return out


def _leg_drilldown(leg, ctx: SiteContext, prints: list[tuple[date, float, int]]) -> dict | None:
    """The inline expansion's payload for one leg — prints, never rows.

    Sourced from ``leg_prints`` like the row above it, so a leg cannot end up
    with a chart and a row that disagree. Points are **parallel arrays** keyed
    off a single origin date rather than an array of objects: same numbers, a
    third of the bytes, and the page ships this inline.
    """
    kept, _total = _leg_points(leg, ctx, prints)
    if not kept:
        return None

    origin = kept[0][0]
    usd = _usd_at_each_date(leg, ctx, kept)
    leg_windows = ctx.leg_windows(leg)
    home_unit = _home_unit(leg.source, leg.key, leg.market.home_currency)
    # Two different questions, and collapsing them was a defect. Whether the
    # venue's own number is a *second view* is `has_native_quote` — #230's
    # shared answer, asked here rather than restated. Whether it is a second
    # *currency* is narrower, and only that case has an FX component a panel
    # could separate: CBOT's cents/bu is a second view in the same currency.
    has_home_quote = leg.source.has_native_quote
    has_second_currency = leg.source.unit == "home_per_mt"

    windows = []
    for days in LEDGER_DRILLDOWN_WINDOW_DAYS:
        start = ctx.today - _days(days)
        drawn = [
            day for (day, _, _), value in zip(kept, usd, strict=True)
            if day >= start and value is not None
        ]
        # Per window, never per leg: the cap bites the 1y window and leaves 30d
        # untouched, so a leg-level note would print "the last 260 of 320"
        # beside a count of 31 — two populations in one sentence.
        in_window = len([point for point in prints if start <= point[0] <= ctx.today])
        shown = len([day for day, _, _ in kept if day >= start])
        withheld = shown - len(drawn)
        entry = {
            "days": days,
            "label": "1y" if days >= 365 else f"{days}d",
            "count": len(drawn),
            "treatment": _drilldown_treatment(len(drawn)),
            # Where this window's drawing starts — NOT where the leg's history
            # starts, which the cap and the window would both misreport.
            "starts": drawn[0].isoformat() if drawn else None,
            "starts_label": _short_date(drawn[0]) if drawn else None,
            "truncated": in_window > shown,
            "truncation_note": (
                f"showing the last {shown} of {in_window} observations"
                if in_window > shown else None
            ),
            "withheld_note": (
                f"{withheld} print{'' if withheld == 1 else 's'} withheld: no "
                f"{leg.market.currency_pair} rate on or before their date, and a "
                "print converted at a later rate is the currency's move wearing "
                "the venue's name"
                if withheld else None
            ),
        }
        if leg_windows:
            # #513: only a windowed leg carries the key — the payload ships
            # inline, and a null on every other leg would change its page.
            entry["roll_note"] = _drill_roll_note(_window_span(leg_windows, drawn))
        windows.append(entry)

    payload = {
        "leg_id": leg.leg_id,
        "label": leg.label,
        "origin": origin.isoformat(),
        "d": [(day - origin).days for day, _, _ in kept],
        "usd": usd,
        # The venue's own print is NOT shipped. Both panes are drawn from `usd`
        # and the rate; a home-currency array would be up to 260 numbers per leg
        # inlined on the page and never drawn, which is the exact weight the
        # build-time clip exists to keep off it. The unit still travels, because
        # the FX pane's sentence names it.
        "home_unit": home_unit,
        "has_home_quote": has_home_quote,
        "fx_pair": leg.market.currency_pair if has_second_currency else None,
        "single_currency_note": None if has_second_currency else (
            f"{leg.label} is quoted in USD/MT. There is no second currency between "
            "the venue's print and the number in the row, so there is no FX "
            "component to separate out — one quote, not two."
            if leg.source.unit == "usd_per_mt" else
            f"{leg.label} is quoted in {home_unit}, already a US dollar unit. The "
            "USD/MT figure is a unit conversion, not a currency one, so there is "
            "no FX component to separate out."
        ),
        # The oldest print this page reads at all — bounded by the layer's
        # lookback, so it is stated as what we hold rather than as when the
        # market began. Carries its year: this one reaches past the turn of the
        # year by construction, and "17 Jul" beside a 90-day window reads as
        # this year's.
        "history_from": prints[0][0].isoformat() if prints else None,
        "history_from_label": (
            prints[0][0].strftime("%d %b %Y").lstrip("0") if prints else None
        ),
        "windows": windows,
        "default_days": LEDGER_DRILLDOWN_DEFAULT_DAYS,
    }
    if leg_windows:
        payload["breaks"] = _roll_breaks(leg_windows, kept, origin)
        payload["ticks"] = _band_ticks(leg_windows, kept, origin)
    return payload


def _drill_roll_note(span: WindowSpan | None) -> str | None:
    """A drill window's note on the loading months it spans (#513, B39 #540),
    or None where it holds one month on one band."""
    if span is None or not (span.rolls or span.band_changes):
        return None
    ticks = (
        "; band changes inside a loading month are ticks on the line, not breaks"
        if span.band_changes else ""
    )
    if not span.rolls:
        return f"{span.describe()}{ticks}"
    return (
        f"nearest band each print, compared by loading month — {span.describe()}; "
        f"the line breaks at each roll, a calendar-spread step rather than a move{ticks}"
    )


def _roll_breaks(windows: dict[date, SessionWindows], kept, origin: date) -> list[int]:
    """Day offsets (from ``origin``) of each print that opens a new loading
    month — where the drawn line restarts rather than joining a roll's
    calendar-spread step to the market's moves (#513, B39 #540)."""
    return [
        (day - origin).days
        for (prior, _, _), (day, _, _) in zip(kept[:-1], kept[1:], strict=True)
        if windows[prior].identity != windows[day].identity
    ]


def _band_ticks(windows: dict[date, SessionWindows], kept, origin: date) -> list[int]:
    """Day offsets of each print whose band changed inside its loading month —
    drawn as a tick on an unbroken line: like-for-like, but visible (B39 #540)."""
    return [
        (day - origin).days
        for (prior, _, _), (day, _, _) in zip(kept[:-1], kept[1:], strict=True)
        if windows[prior].identity == windows[day].identity
        and windows[prior].band != windows[day].band
    ]


def _range_band(leg, ctx: SiteContext, prints, current_usd: float | None) -> dict:
    """The collapsed row's range band — ``352–378 since 29 Jul``.

    Variant C's band, kept: for a buyer scanning five legs, "high in its own
    range" is often the entire question, so it must not need a click. Rendered
    server-side with the rest of the row (the expansion is the only client-side
    part) — the page must not depend on script running to state a number.

    Under ``LEDGER_LINE_MIN_OBS`` prints it reads ``4 obs since 06 Aug — no
    range yet``: two points are not a range, and a band drawn across them
    invents one.
    """
    start = ctx.today - _days(LEDGER_DRILLDOWN_DEFAULT_DAYS)
    inside = [point for point in prints if start <= point[0] <= ctx.today]
    # Count and date describe the same population: a print that could not be
    # converted is not in the range, so it must not date the range either.
    priced = [
        (point[0], usd)
        for point, usd in zip(
            inside, _usd_at_each_date(leg, ctx, inside, places=None), strict=True
        )
        if usd is not None
    ]
    values = [usd for _, usd in priced]
    since = priced[0][0] if priced else None
    band = {
        "count": len(values),
        "since": since.isoformat() if since else None,
        "since_label": _short_date(since),
        "has_range": False,
        "low": None,
        "high": None,
        "position": None,
        # #513 / B39 #540: how many loading months a windowed leg's range
        # spans (None inside one), and its note — the months, the rolls, the
        # newest month's run and band changes. Both None on every other leg.
        "loading_months": None,
        "window_note": None,
    }
    if len(values) < LEDGER_LINE_MIN_OBS:
        return band
    span = _window_span(ctx.leg_windows(leg), [day for day, _ in priced])
    if span is not None and span.rolls:
        band["loading_months"] = len(span.months)
        band["window_note"] = (
            f"nearest band each print, compared by loading month — {span.describe()}; "
            "a roll is a calendar-spread step, not a market move"
        )
    elif span is not None:
        band["window_note"] = span.describe()
    low, high = min(values), max(values)
    latest = current_usd if current_usd is not None else values[-1]
    band.update({
        "has_range": True,
        "low": low,
        "high": high,
        # Where today's print sits in the band, clamped: the row's level is
        # struck on the leg's own newest print, which is inside this window by
        # construction, but a clamp is cheaper than a tick off the end of a bar.
        "position": 0.5 if high == low else min(1.0, max(0.0, (latest - low) / (high - low))),
    })
    return band


def _reference_payload(own_leg, ctx: SiteContext, own_prints) -> dict | None:
    """The page's own pinned leg, for the dashed overlay under every other row.

    CEPEA on Brazil, No.2 on Dalian — never CBOT everywhere, which is the
    assumption M4 found wrong and M12 encoded against. Emitted once for the
    whole block rather than copied onto each row: four copies of the same 260
    points is four times the payload for one series.
    """
    kept, total = _leg_points(own_leg, ctx, own_prints)
    if not kept:
        # The pinned leg printed inside its recency budget but outside the
        # drill-down's widest window. No overlay rather than an invented one.
        return None
    origin = kept[0][0]
    payload = {
        "leg_id": own_leg.leg_id,
        "label": own_leg.label,
        "origin": origin.isoformat(),
        "d": [(day - origin).days for day, _, _ in kept],
        "usd": _usd_at_each_date(own_leg, ctx, kept),
        # An overlay that stops mid-chart because of the cap must say so, or it
        # reads as the pinned leg having no earlier history.
        "truncated": total > len(kept),
        "truncation_note": (
            f"the overlay shows the last {len(kept)} of {total} of its own observations"
            if total > len(kept) else None
        ),
    }
    own_windows = ctx.leg_windows(own_leg)
    if own_windows:
        # #513: the overlay breaks at the pinned leg's own rolls, as its row does.
        payload["breaks"] = _roll_breaks(own_windows, kept, origin)
    return payload


def _fx_payload(pair: str, ctx: SiteContext) -> dict | None:
    """One currency pair's own series, over the drill-down's widest window.

    This is what earns the drill-down. SAFEX moved +0.03% locally and +0.44% in
    USD on one session; a single USD line cannot say which of the two moved.
    """
    widest = max(LEDGER_DRILLDOWN_WINDOW_DAYS)
    cutoff = ctx.today - _days(widest)
    inside = [row for row in ctx.fx_series(pair) if cutoff <= row[0] <= ctx.today]
    # Its own cap, not the legs'. A daily rate prints ~261 weekdays a year, so
    # the leg cap of 260 would truncate every 1y FX pane by a day or two and
    # start it three months late for no saving worth having. One series per
    # pair, shared by every row on the page.
    rows = inside[-widest:]
    if not rows:
        return None
    origin = rows[0][0]
    return {
        "pair": pair,
        "origin": origin.isoformat(),
        "d": [(day - origin).days for day, _ in rows],
        "v": [round(rate, 6) for _, rate in rows],
        "truncated": len(inside) > len(rows),
        "truncation_note": (
            f"showing the last {len(rows)} of {len(inside)} rates"
            if len(inside) > len(rows) else None
        ),
    }


def _spread(leg, prints, own_leg, own_prints, ctx: SiteContext):
    """This leg minus the pinned leg, struck on the newest session both printed.

    Never a cross-day subtraction: two legs from different days differ by the
    calendar as much as by the arbitrage, and the whole reason this surface
    exists is that they *do* print on different days.
    """
    own_by_date = {day: (value, day) for day, value, _ in own_prints}
    for day, value, _ in reversed(prints):
        if day not in own_by_date:
            continue
        mine = leg.source.to_usd_mt(value, leg.key, ctx.fx_on(leg.market.currency_pair, day).rate)
        theirs = own_leg.source.to_usd_mt(
            own_by_date[day][0], own_leg.key, ctx.fx_on(own_leg.market.currency_pair, day).rate
        )
        if mine is None or theirs is None:
            continue
        return mine - theirs, day.isoformat(), None
    return None, None, (
        f"no session in the window where both {leg.label} and {own_leg.label} printed — "
        "a spread across two dates is a calendar artefact, not an arbitrage"
    )


def _ledger_state(row: dict, *, leading_edge: str | None, today: date):
    """M3 #145's three pills, plus whether being behind is *abnormal*.

    ``dark`` is the leg's own recency budget — the same number ``main.py``
    grades the layer on, so the site and the pipeline cannot disagree about
    whether a source is alive. ``overdue`` is M4 section 3.4 trap 5: a daily leg
    six days silent is well inside ``FRESHNESS_WARNING_DAYS = 7`` and still
    plainly wrong, so the ledger carries each leg's own expected gap instead.
    """
    if row["as_of"] is None:
        return LEDGER_STATE_DARK, (
            "no trade-proved print in the window"
            if row["trade_proved"] else "no print in the window"
        ), True
    if row["age_days"] is not None and row["age_days"] > row["budget_days"]:
        return LEDGER_STATE_DARK, (
            f"newest print {row['as_of']} is {row['age_days']}d old, past this layer's "
            f"{row['budget_days']}d budget"
        ), True
    if leading_edge is not None and row["as_of"] == leading_edge:
        return LEDGER_STATE_REPRICED, f"printed {row['as_of']}, the newest on this ledger", False
    overdue = (row["age_days"] or 0) > row["expected_gap_days"]
    detail = f"last printed {row['as_of']}"
    if overdue:
        detail += (
            f" — {row['age_days']}d ago, past the {row['expected_gap_days']}d gap that is "
            "normal for this leg"
        )
    return LEDGER_STATE_NO_PRINT, detail, overdue


def _home_unit(source: Source, key: str, home_currency: str) -> str:
    """The venue's own quote unit. One vocabulary — see `Source.quote_unit`."""
    return source.quote_unit(key, home_currency)


# ---------------------------------------------------------------------------
# 03 Crush
# ---------------------------------------------------------------------------
def _crush_sessions(crush, rows_by_key: dict) -> set[date]:
    """The sessions every one of the three legs printed on.

    One engine, every market (M7 #149), and one session for all three legs: a
    leg's own latest print taken alone would silently mix sessions and move the
    margin on a day that leg did not trade. Shared by the level and its history
    so the two cannot come to disagree about which days count.
    """
    common: set[date] | None = None
    for key in crush.legs.values():
        days = {day for day, _, _ in rows_by_key.get(key) or []}
        common = days if common is None else (common & days)
    return common or set()


@dataclass(frozen=True)
class _CrushStrike:
    """One session's three leg values, and the band each was quoted for.

    ``band`` and ``windows`` are None on a source that prints one quote a
    session; on a windowed one (MAGyP, #507) they name the shared band the
    margin is for — the period all three legs' bands cover (B35 #532) — and
    each leg's own band, which contains it. ``loading`` is that shared
    period's loading month — the crush's identity across sessions (A20 #536,
    B39 #540) — and ``loading_note`` names it with the bands it came from.
    """

    values: dict[str, float]
    band: str | None = None
    windows: dict[str, str] | None = None
    loading: LoadingMonth | None = None
    loading_note: str | None = None


def _crush_strike(crush, source: Source, ctx: SiteContext, day: date) -> _CrushStrike | str:
    """The three legs on ``day``, or why that session strikes no margin.

    A windowed source is struck on ONE band all three products share — the
    origins page's rule, ``analysis.origins.crush.shared_crush_band``, read
    here rather than restated (#507). Each leg's mean across its bands is a
    price nobody quoted, and each leg's nearest window taken alone need not be
    one cargo — the products' bands are cut differently from the bean's.
    Shared by the level and its history.
    """
    if source.window_scheme is None:
        values = {}
        for name, key in crush.legs.items():
            values[name] = next(v for d, v, _ in ctx.series(source)[key] if d == day)
        return _CrushStrike(values)

    from analysis.origins.crush import shared_crush_band

    picked = shared_crush_band(
        {
            name: [
                (quote.window, quote.value)
                for quote in ctx.session_windows(source, key)[day].printed
            ]
            for name, key in crush.legs.items()
        },
        day,
    )
    if isinstance(picked, str):
        return picked
    loading = picked.window.loading_month
    return _CrushStrike(
        values=picked.values,
        band=picked.window.describe(),
        windows={name: window.describe() for name, window in picked.windows.items()},
        loading=loading,
        loading_note=(
            f"crush for {loading.name} loading (overlap {picked.window.band_name()} of "
            + " / ".join(
                f"{name} {window.band_name(year=False)}"
                for name, window in picked.windows.items()
            )
            + ")"
        ),
    )


def _crush_margin(legs: dict[str, float], yields: dict[str, float]) -> float:
    """``oil x yield + meal x yield − bean``, in whatever one unit the legs share.

    The convention lives here once. It is the level's arithmetic and its
    history's, and a yield change that reached only one of them would put a
    range around a margin the card is not printing.
    """
    return legs["oil"] * yields["oil"] + legs["meal"] * yields["meal"] - legs["bean"]


def crush_block(market: Market, ctx: SiteContext, **_) -> tuple[str, str, dict]:
    state, reason, data = _board_crush_block(market, ctx)
    if state == STATE_OK:
        # The physical oil and meal legs ride beside the margin as their own
        # envelope (#352) — a cash premium over the board, never folded into
        # the margin and never labelled as one. None where the registry
        # declares no cash leg for this market.
        from analysis.processor_cash import cash_leg_panel

        data["cash"] = cash_leg_panel(ctx.conn, market.slug, today=ctx.today)
    return state, reason, data


def _board_crush_block(market: Market, ctx: SiteContext) -> tuple[str, str, dict]:
    # Same narrowing contract as price_block: absent_reason() gates dispatch.
    crush = market.crush
    assert crush is not None, f"crush_block dispatched for {market.slug} with no crush source"
    if crush.months is not None:
        return _month_crush_block(market, ctx)
    if crush.contracts == "named":
        return _named_crush_block(market, ctx)
    source = crush.as_source()
    rows_by_key = ctx.series(source)

    common = _crush_sessions(crush, rows_by_key)
    if not common:
        return STATE_EMPTY, (
            "the bean, oil and meal legs share no session — a margin across "
            "different days would not be a margin"
        ), {}

    when = max(common)
    # The newest shared session or nothing: falling back to an older one where
    # the bands happen to line up would publish yesterday's margin as today's.
    strike = _crush_strike(crush, source, ctx, when)
    if isinstance(strike, str):
        return STATE_EMPTY, f"{strike} ({when.isoformat()})", {}
    fx = ctx.fx_on(market.currency_pair, when)
    legs: dict[str, dict] = {}
    for leg_name, key in crush.legs.items():
        value = strike.values[leg_name]
        legs[leg_name] = {
            "key": key,
            "home_value": value,
            "usd_mt": source.to_usd_mt(value, key, fx.rate),
            "window": strike.windows[leg_name] if strike.windows else None,
        }

    if any(leg["usd_mt"] is None for leg in legs.values()):
        return STATE_EMPTY, (
            f"the legs quote in {market.home_currency} and cannot be stated in USD/MT "
            f"for {when.isoformat()} — {fx.note}"
        ), {}

    yields = crush.yields
    margin_usd = _crush_margin(
        {name: leg["usd_mt"] for name, leg in legs.items()}, yields
    )
    # A home-currency margin exists only where all three legs are quoted per MT
    # in that one currency. CBOT's legs are cents/bu, cents/lb and USD/short
    # ton — the yields are MT-per-MT factors, so combining them in native units
    # produces a number with no unit at all (it printed "USD -820/MT" beside a
    # +$66.9 margin before this check). USD/MT is the only honest statement of
    # a board crush, which is what M7 #149's one engine means in practice.
    margin_home = _crush_margin(
        {name: leg["home_value"] for name, leg in legs.items()}, yields
    ) if crush.unit == "home_per_mt" else None
    return STATE_OK, "", {
        "kind": crush.kind,
        "legs": legs,
        "yields": yields,
        "margin_usd_mt": margin_usd,
        "margin_home": margin_home,
        "home_currency": market.home_currency,
        "as_of": when.isoformat(),
        "age_days": _age_days(ctx.today, when),
        # #507 / B35 #532: the period all three legs' bands ship (their
        # three-way intersection), the margin's own period.
        # None where the source prints one quote a session.
        "shipment_window": strike.band,
        # B39 #540: "crush for Sep loading (overlap Sep 2026 of bean Sep–Oct /
        # oil Sep / meal Sep–Nov)" — the identity the range is keyed on.
        "loading_note": strike.loading_note,
        # A1 #298: the three legs share one session; the rate may be a labelled
        # prior close. Not a same-session claim about the FX leg (invariant 8).
        **_fx_fields(source, fx),
        "profitable": margin_usd > 0,
        # M7 #149 / M5 #147: a margin off an inferred NCM position code is not
        # a number to trade on, and the caveat rides with the data.
        "provisional": crush.provisional,
        "provisional_note": (
            "provisional — one leg's position code is inferred, not cross-checked "
            "against the labelled series (M5 #147)"
        ) if crush.provisional else None,
        # Structural, not decorative: a margin off continuous main-contract
        # series is arithmetically fine and names no instrument an order can be
        # placed in. The block renders the two differently because they are
        # different claims.
        "contract_basis": crush.contracts,
        "hedgeable": False,
        "contract_note": (
            "Continuous main-contract series: the underlying contract changes on the "
            "provider's own schedule and is not published, so this margin names no "
            "contract and is not one an order can be placed against."
            if crush.contracts == "continuous" else None
        ),
        "legs_named": False,
    }


def _month_crush_block(market: Market, ctx: SiteContext) -> tuple[str, str, dict]:
    """The board crush on ONE named delivery month (B29 #503).

    The month, the session and every refusal come from
    ``analysis.futures.crush.month_board_crush`` — the Origins board level and
    the briefing read the same call. This only converts: each leg at the
    session's own FX rate (A1 #298), through ``Source.to_usd_mt``.
    """
    from analysis.futures.crush import CrushWithheld, month_board_crush, month_label

    if ctx.conn is None:
        return STATE_EMPTY, "no database connection", {}
    crush = market.crush
    assert crush is not None and crush.months is not None
    outcome = month_board_crush(ctx.conn, crush.months, today=ctx.today)
    if isinstance(outcome, CrushWithheld):
        return STATE_EMPTY, outcome.reason, {}

    source = crush.as_source()
    when = outcome.session
    fx = ctx.fx_on(market.currency_pair, when)
    legs: dict[str, dict] = {}
    closes = outcome.closes
    for role, quote in outcome.legs.items():
        commodity = crush.legs[role]
        legs[role] = {
            # `key` is the contract, as on the CBOT named block — the one
            # template renders both, and the commodity rides beside it.
            "key": quote.contract,
            "commodity": commodity,
            "home_value": closes[role],
            "usd_mt": source.to_usd_mt(closes[role], commodity, fx.rate),
            "contract_month": month_label(quote.delivery_month),
            "price_label": outcome.price_type.label,
            "settlement_proven": False,
        }
    if any(leg["usd_mt"] is None for leg in legs.values()):
        return STATE_EMPTY, (
            f"the legs quote in {market.home_currency} and cannot be stated in USD/MT "
            f"for {when.isoformat()} — {fx.note}"
        ), {}

    yields = crush.yields
    margin_usd = _crush_margin({name: leg["usd_mt"] for name, leg in legs.items()}, yields)
    margin_home = _crush_margin(
        {name: leg["home_value"] for name, leg in legs.items()}, yields
    ) if crush.unit == "home_per_mt" else None
    return STATE_OK, "", {
        "kind": crush.kind,
        "legs": legs,
        "yields": yields,
        "margin_usd_mt": margin_usd,
        "margin_home": margin_home,
        "home_currency": market.home_currency,
        "as_of": when.isoformat(),
        "age_days": _age_days(ctx.today, when),
        **_fx_fields(source, fx),
        "profitable": margin_usd > 0,
        "provisional": crush.provisional,
        "provisional_note": None,
        "contract_basis": "named_contract",
        # Never placeable: MONTH_CRUSH_UNPLACEABLE_REASON — the Origins
        # CrushResult carries the same reason, so the two surfaces agree.
        "hedgeable": False,
        # The headline card's inline month label; block 03 renders the
        # period label and caption instead (legs_named).
        "contract_note": (
            f"{outcome.label} — "
            + " / ".join(outcome.legs[role].contract for role in ("bean", "oil", "meal"))
            + f", all traded {when.isoformat()}"
        ),
        "legs_named": True,
        "period_label": outcome.label,
        "delivery_month": outcome.month,
        "convention": crush.months.convention,
        "provider_display": crush.months.provider_display,
    }


def _named_crush_block(market: Market, ctx: SiteContext) -> tuple[str, str, dict]:
    """The board crush on explicit delivery months (``analysis.futures.crush``).

    The whole of the CBOT block's arithmetic lives there and is shared with
    Origins, the Workstation, the Opportunity board and the briefing — so the
    five surfaces cannot print five different crushes, which is what happened
    while each one reached for the continuous series itself.
    """
    from analysis.futures.crush import CrushWithheld, named_board_crush
    from analysis.futures.providers import open_provider

    if ctx.conn is None:
        return STATE_EMPTY, "no database connection", {}
    crush = market.crush
    assert crush is not None, f"named crush requested for {market.slug} with no crush source"
    outcome = named_board_crush(open_provider(ctx.conn), as_of=ctx.today)
    if isinstance(outcome, CrushWithheld):
        return STATE_EMPTY, outcome.reason, {}

    data = outcome.to_dict()
    data.update({
        "kind": crush.kind,
        "home_currency": market.home_currency,
        "as_of": outcome.observation_date.isoformat(),
        "age_days": _age_days(ctx.today, outcome.observation_date),
        # A named-contract board crush is quoted in three different native
        # units (cents/bu, $/short ton, cents/lb), so USD/MT is the only honest
        # statement of it — the same rule the generic engine keeps.
        "margin_home": None,
        "provisional": crush.provisional,
        "provisional_note": None,
        "contract_basis": outcome.contract_basis.value,
        "legs_named": True,
        "contract_note": None,
        # `key` and `usd_mt` are the generic engine's names for the same two
        # facts, kept so one template renders both shapes; the contract fields
        # beside them are what only a named leg can answer.
        "legs": {
            leg.role: {**leg.to_dict(), "key": leg.symbol, "usd_mt": leg.usd_per_mt}
            for leg in outcome.legs
        },
    })
    return STATE_OK, "", data


# ---------------------------------------------------------------------------
# The headline crush board (section 04, M16 #208)
# ---------------------------------------------------------------------------
def headline_crush_board(
    markets: dict[str, Market], ctx: SiteContext
) -> tuple[str, str, dict]:
    """Four markets' crush margins side by side, each labelled by kind.

    The headline's answer to "is processing paying, and where" — a scan, with
    the depth on the market page each card links to. Three things about it are
    load-bearing:

    * **The level is block 03's number, not a second calculation.** Every card
      calls ``crush_block`` for the market it names, so the headline and the
      page cannot disagree about the same margin. That was M7 #149's finding
      turned into code: one engine, and only yields, FX and the kind label
      vary between markets.
    * **The kinds do not collapse.** A board margin, an administered one and a
      physical one are three different claims, and four numbers in one row of
      cards is the easiest place on the site for them to read as one "crush"
      line. Every card states its own kind (M2 #144 constraint 3).
    * **Brazil is on the board without a number.** Its oil and meal legs are an
      unbuilt scrape, so the card carries the registry's reason. Dropping it
      would say something quite different — that Brazil has no crush industry.

    Which markets these are is ``config.CRUSH_BOARD``, not a rule derived here.
    """
    rows = [_crush_board_card(markets[slug], ctx) for slug in config.CRUSH_BOARD]
    if not any(row["margin_usd_mt"] is not None for row in rows):
        return STATE_EMPTY, (
            "no market on the board struck a margin — each leg is either unbuilt or "
            "short of a session its bean, oil and meal all printed"
        ), {}
    return STATE_OK, "", {
        "rows": rows,
        "kind_note": (
            "Board, administered and physical margins are different claims and are "
            "labelled per card — a Ley 21.453 minimum is not a traded price and an "
            "exchange close is not a plant's earnings."
        ),
        "range_note": (
            "A range is struck by the same engine that struck the level, or it is "
            "not struck at all."
        ),
    }


def _crush_board_card(market: Market, ctx: SiteContext) -> dict:
    """One market's card — the margin, or the reason there is none.

    Every card carries the same keys whichever way it goes: the template
    renders one shape, and a key that exists only on the happy path is a
    ``StrictUndefined`` error that tombstones the whole headline (#226).
    """
    card: dict[str, Any] = {
        "market_slug": market.slug,
        "market_name": market.name,
        "venue": market.venue,
        "href": market.url,
        "state": STATE_OK,
        "reason": "",
        "kind": None,
        "kind_label": None,
        "margin_usd_mt": None,
        "profitable": None,
        "margin_home": None,
        "home_currency": market.home_currency,
        "as_of": None,
        "age_days": None,
        "bean_key": None,
        "contract_basis": None,
        "legs_named": None,
        "hedgeable": None,
        "contract_note": None,
        "provisional": None,
        "provisional_note": None,
        "shipment_window": None,
        "range": None,
        "range_note": "",
    }

    if market.crush is None:
        # `absent`, not `empty`: nothing was asked of a source, because there
        # is no source. M1 #143 renders the two differently on purpose — and a
        # non-`ok` card with no reason is the blank the `Block` type raises on,
        # so it raises here too rather than rendering an unexplained gap.
        reason = absent_reason(market, "crush")
        if not (reason or "").strip():
            raise ValueError(
                f"{market.slug} has no crush source and no reason for it — an "
                "unexplained empty card reads as a market with no crush industry"
            )
        card.update(state=STATE_ABSENT, reason=reason)
        return card

    state, reason, data = crush_block(market, ctx)
    card["kind"] = market.crush.kind
    card["kind_label"] = quote_kind_label(market.crush.kind)
    if state != STATE_OK:
        card.update(state=state, reason=reason)
        return card

    band, range_note = _crush_range(market, ctx)
    card.update(
        margin_usd_mt=data["margin_usd_mt"],
        profitable=data["profitable"],
        margin_home=data["margin_home"],
        home_currency=data["home_currency"],
        as_of=data["as_of"],
        age_days=data["age_days"],
        bean_key=data["legs"]["bean"]["key"],
        contract_basis=data["contract_basis"],
        legs_named=data["legs_named"],
        hedgeable=data["hedgeable"],
        contract_note=data["contract_note"],
        provisional=data["provisional"],
        provisional_note=data["provisional_note"],
        shipment_window=data.get("shipment_window"),
        range=band,
        range_note=range_note,
    )
    return card


def _crush_range(market: Market, ctx: SiteContext) -> tuple[dict | None, str]:
    """This leg's own mean and range, or why it has none (#208 sub-question 4).

    **A range is struck by the engine that struck the level.** CBOT's margin is
    ZSU26/ZMU26/ZLU26 out of ``forward_curve``; the continuous front-month
    series beside it in ``prices`` runs back fifteen years and would happily
    produce a mean — around a different number, on legs that name no contract.
    Rendered under a named-contract level it would read as that margin's own
    history, which is the one thing it is not.
    """
    crush = market.crush
    assert crush is not None, f"crush range requested for {market.slug} with no crush source"
    if crush.months is not None:
        history, band = _month_margin_history(market, ctx), None
    elif crush.contracts == "named":
        return None, (
            "no range yet — struck on named delivery months; a mean off the "
            "front-month series would be a range around a different margin"
        )
    else:
        history, band = _crush_margin_history(market, ctx)
    on_band = f" for {band.name} loading" if band else ""
    if len(history) < CRUSH_RANGE_MIN_OBS:
        return None, (
            f"no range yet — {len(history)} session(s) all three legs struck{on_band}, "
            f"short of the {CRUSH_RANGE_MIN_OBS} a mean needs"
        )
    values = [margin for _, margin in history]
    return {
        "n_obs": len(values),
        # Sessions, never "1Y": the read is bounded at 400 days
        # (`LOOKBACK_DAYS_BY_CADENCE`) and a leg's stored depth is its own, so
        # a year is a claim about coverage this cannot make. The count is the
        # honest label and it is also the more useful one.
        "window": f"{len(values)}-session{on_band}",
        "mean": sum(values) / len(values),
        "low": min(values),
        "high": max(values),
        "start": history[0][0].isoformat(),
        "end": history[-1][0].isoformat(),
    }, ""


def _crush_margin_history(
    market: Market, ctx: SiteContext
) -> tuple[list[tuple[date, float]], LoadingMonth | None]:
    """The generic engine's margin restruck for every session it can be, and
    the loading month it is the history of (None on an unwindowed source).

    The two rules the level keeps, kept here too rather than relaxed because
    these are old numbers nobody reads individually:

    * **One session for all three legs.** A day the oil did not print is not a
      day with a margin, and interpolating one would put a shape into the range
      that the market never traded.
    * **The level's own loading month, on a windowed source (#507, B39
      #540).** Each session is struck by the level's shared-band rule (B35
      #532: the period all three legs' bands cover), and only sessions whose
      shared period starts in the month the level's does count — an Oct
      cargo's margin beside a Nov–Apr one is a range around a different
      margin, the run B29 #503 refuses to stitch across Dalian months. A band
      change inside the month (a ``Sep`` bean one day, ``Sep–Oct`` the next)
      is like-for-like, as it is for every other MAGyP surface (A20 #536); a
      session with no shared band is dropped, never restruck on means.
    * **That row's own date's rate, under A1 (#298).** ``ctx.fx_on`` never
      converts at a later-dated rate and only at a prior one inside the cap;
      a session with no such rate is dropped rather than restruck at a rate
      from its own future.
    """
    crush = market.crush
    assert crush is not None, f"crush history requested for {market.slug} with no crush source"
    source = crush.as_source()
    rows_by_key = ctx.series(source)

    common = _crush_sessions(crush, rows_by_key)
    if not common:
        return [], None
    level = _crush_strike(crush, source, ctx, max(common))
    if isinstance(level, str):
        return [], None

    yields = crush.yields
    history: list[tuple[date, float]] = []
    for day in sorted(common):
        strike = _crush_strike(crush, source, ctx, day)
        # A20 #536's identity: the loading month of the three-way intersection.
        if isinstance(strike, str) or strike.loading != level.loading:
            continue
        rate = ctx.fx_on(market.currency_pair, day).rate
        legs: dict[str, float] = {}
        for leg_name, key in crush.legs.items():
            usd = source.to_usd_mt(strike.values[leg_name], key, rate)
            if usd is None:
                break
            legs[leg_name] = usd
        else:
            history.append((day, _crush_margin(legs, yields)))
    return history, level.loading


def _month_margin_history(market: Market, ctx: SiteContext) -> list[tuple[date, float]]:
    """The level's own delivery month, restruck on every session it traded (B29 #503).

    One month's three contracts across their own life, never a run stitched
    across months — that would be a range around a different margin. Each
    session at its own date's rate under A1 (#298), or dropped.
    """
    from analysis.futures.crush import CrushWithheld, month_board_crush, month_crush_history

    crush = market.crush
    assert crush is not None and crush.months is not None
    if ctx.conn is None:
        return []
    level = month_board_crush(ctx.conn, crush.months, today=ctx.today)
    if isinstance(level, CrushWithheld):
        return []
    source = crush.as_source()
    since = ctx.today - _days(LOOKBACK_DAYS_BY_CADENCE["daily"])
    history: list[tuple[date, float]] = []
    for strike in month_crush_history(
        ctx.conn, crush.months, level.month, since=since, until=ctx.today
    ):
        rate = ctx.fx_on(market.currency_pair, strike.session).rate
        legs: dict[str, float] = {}
        for role, close in strike.closes.items():
            usd = source.to_usd_mt(close, crush.legs[role], rate)
            if usd is None:
                break
            legs[role] = usd
        else:
            history.append((strike.session, _crush_margin(legs, crush.yields)))
    return history


# ---------------------------------------------------------------------------
# 04 Basis
# ---------------------------------------------------------------------------
def basis_block(market: Market, ctx: SiteContext, *, markets: dict[str, Market], **_):
    source = market.basis
    assert source is not None, f"basis_block dispatched for {market.slug} with no basis source"
    if source.reference != REFERENCE_MARKET:
        return STATE_EMPTY, f"unknown basis reference {source.reference!r}", {}

    board_market = markets.get(REFERENCE_MARKET)
    board_kind = board_market.price.quote_kind if board_market and board_market.price else None
    # A named-contract basis (#512) does not strike on this series at all, so
    # it does not read it either: `markets.block_reads` states this block's
    # tables and the availability basket is checked against them (B37 #534).
    reference = dict(ctx.reference_series(markets)) if source.contract_board is None else {}
    if not reference and source.contract_board is None:
        return STATE_EMPTY, "the CBOT reference leg has no rows to strike a basis against", {}

    key = source.headline_key or source.keys[0]
    rows = ctx.series(source).get(key) or []
    history: list[tuple[date, float, float, float, FxResolution]] = []
    withheld: FxResolution | None = None
    for when, value, _n in rows:
        board = reference.get(when)
        if board is None and source.contract_board is None:
            continue  # M3: no cross-day joins — a basis is one session's number
        fx = ctx.fx_on(market.currency_pair, when)
        local = source.to_usd_mt(value, key, fx.rate)
        if local is None:
            withheld = fx  # the newest session lost to A1, for the reason below
            continue
        if source.contract_board is not None:
            # #513: the range is struck by the engine that struck the level —
            # each session over the contract its own window names, at that
            # contract's close on the session. A session that cannot be is
            # left out of the range, never padded with the front month.
            struck = _named_board(ctx, source, key, when)
            if isinstance(struck, str):
                continue
            board = struck[1]
        assert board is not None  # narrowed above, on both paths
        history.append((when, local, board, local - board, fx))

    board_contract: str | None = None
    if source.contract_board is not None:
        # #512: the newest print's own window, over the contract that window
        # names, at that contract's close on the same session — or nothing.
        if not rows:
            return STATE_EMPTY, f"{source.label or key} has no rows to strike a basis on", {}
        when, value, _n = rows[-1]
        fx = ctx.fx_on(market.currency_pair, when)
        local = source.to_usd_mt(value, key, fx.rate)
        if local is None:
            return STATE_EMPTY, (
                f"{source.label or key} printed on {when.isoformat()} but cannot be stated "
                f"in USD/MT — {fx.note}"
            ), {}
        struck = _named_board(ctx, source, key, when)
        if isinstance(struck, str):
            return STATE_EMPTY, struck, {}
        board_contract, board = struck
        basis = local - board
    elif not history:
        if withheld is not None and withheld.reason is not None:
            return STATE_EMPTY, (
                f"{source.label or key} printed on a session the board shares, but "
                f"cannot be stated in USD/MT — {withheld.note}"
            ), {}
        return STATE_EMPTY, (
            f"{source.label or key} and the CBOT board share no session — the basis "
            "is only defined where both printed"
        ), {}
    else:
        when, local, board, basis, fx = history[-1]
    spreads = [row[3] for row in history]
    # B30 #504: the local leg is one named window's print, and the session's
    # other windows are listed beside it — as flat prices, never as a basis
    # over this board: AMS quotes its later slots over later contracts.
    window = _window_fields(ctx.session_windows(source, key), when)
    for window_row in window["window_rows"]:
        window_row["usd_mt"] = source.to_usd_mt(window_row["home_value"], key, fx.rate)
    return STATE_OK, "", {
        "label": source.label or key,
        **window,
        "home_unit": source.quote_unit(key, market.home_currency),
        "basis_usd_mt": basis,
        "local_usd_mt": local,
        "board_usd_mt": board,
        # Both legs name their kind, like the ledger's Kind column. This is the
        # other block that prints two USD/MT figures side by side, and they are
        # rarely the same animal — an AMS cash bid or an Argentine decreed
        # minimum against a delayed CBOT close. The block header stamps one kind
        # (block 01's case) and cannot label two, so each card carries its own.
        # The label carries the price type as well as the venue: the CBOT leg is
        # a delayed daily bar, not the exchange's settlement, and the card is
        # where a reader would otherwise assume otherwise.
        "local_quote_kind": source.quote_kind,
        "local_kind_label": quote_kind_label(source.quote_kind) if source.quote_kind else None,
        "board_quote_kind": board_kind,
        # #512: the named contract the board leg is, where the venue names one.
        "board_contract": board_contract,
        "board_kind_label": quote_kind_label(board_kind) if board_kind else None,
        "direction": "premium" if basis > 0 else "discount",
        "as_of": when.isoformat(),
        "age_days": _age_days(ctx.today, when),
        "n_obs": len(history),
        # A1 #298: the local leg's rate may be a labelled prior close; the
        # board leg needs none. Rendered beside the local figure.
        **_fx_fields(source, fx),
        # M19 #222: a spread no cargo can close is not a basis a trader can
        # work, and the difference is invisible in the number. India's mandi
        # bean prints ~+66% over CBOT behind a GM import ban; the caveat rides
        # with the figure rather than living in a comment nobody renders.
        "arbitrage": source.arbitrage,
        "caveat": source.caveat,
        # A level with no range behind it can mislead; where there is too
        # little history to say, the block says that instead of inventing one.
        "avg": sum(spreads) / len(spreads) if len(spreads) >= 20 else None,
        "low": min(spreads) if len(spreads) >= 20 else None,
        "high": max(spreads) if len(spreads) >= 20 else None,
        "history_note": None if len(spreads) >= 20 else (
            f"history building — {len(spreads)} common session(s); range shown at 20"
        ),
        # #513: the mean and range are struck as the level is — over the
        # contract each session's window names — and say so.
        "history_board": (
            "each session struck over the contract its window names"
            if source.contract_board is not None else None
        ),
        # #513 / B39 #540: the local leg is each session's nearest band, so the
        # range crosses every loading-month roll between — labelled, with the
        # newest month's run and band changes (see `_window_span`).
        "history_windows": _history_windows(
            ctx.session_windows(source, key), [row[0] for row in history]
        ),
    }


def _history_windows(windows: dict[date, SessionWindows], sessions: list[date]) -> str | None:
    """Block 04's label for the loading months its mean and range span (#513,
    B39 #540)."""
    span = _window_span(windows, sessions)
    if span is None:
        return None
    if not span.rolls:
        return span.describe()
    return (
        f"nearest band each session, compared by loading month — {span.describe()}: "
        "a roll is a calendar-spread step, not a market move"
    )


def _named_board(ctx: SiteContext, source: Source, key: str, when: date) -> tuple[str, float] | str:
    """The named contract the session's chosen window is quoted over, and its
    close on that session in USD/MT — or the reason there is none (#512).

    The venue prints a month with no year; it is this year's at or after the
    session's month and next year's before it, the rule ``parse_ams_slot``
    applies to the windows themselves. The close is Layer 11b's, read through
    the quote provider, and must be the session's own — invariant 8.
    """
    from analysis.futures.providers import open_provider

    board = source.contract_board
    assert board is not None, f"{source.table} declares no contract board"
    label = source.label or key
    if ctx.conn is None:
        return f"no database to read {label}'s contract months from"
    session = ctx.session_windows(source, key).get(when)
    if session is None:
        raise ValueError(f"{source.table}: {key} on {when.isoformat()} has a print but no window")
    window = session.chosen.window
    columns = ", ".join((*source.window_columns, *source.contract_columns))
    rows = ctx.conn.execute(
        f"SELECT {columns} FROM {source.table} "  # noqa: S608 — identifiers come from the registry
        f"WHERE {source.key_column} = ? AND date({source.date_column}) = ? "
        f"AND {source.value_column} IS NOT NULL",
        (key, when.isoformat()),
    ).fetchall()
    arity = len(source.window_columns)
    months = {
        tuple(row[arity:])
        for row in rows
        if source.parse_window(tuple(row[:arity]), when) == window
    }
    if not months:
        raise ValueError(f"{source.table}: no stored row carries {key}'s chosen window on {when}")
    where = f"{label} {window.describe()} on {when.isoformat()}"
    if any(month is None for pair in months for month in pair):
        return f"{where} names no CBOT contract month — no board to strike its basis against"

    def contract(month) -> NamedContract:
        month = int(month)
        year = when.year if month >= when.month else when.year + 1
        return named_contract(board, year, month)

    contracts = sorted({contract(m) for pair in months for m in pair}, key=lambda c: c.symbol)
    if len(contracts) != 1:
        return (
            f"{where} is quoted over {' and '.join(c.symbol for c in contracts)} — a basis "
            "over more than one contract has no single board, and an average of them is "
            "neither contract's"
        )
    (named,) = contracts
    closes = open_provider(ctx.conn).close_history(named, as_of=when, sessions=1)
    quote = closes[-1] if closes else None
    if quote is None or quote.observation_date != when:
        seen = f"its newest is {quote.observation_date.isoformat()}" if quote else "no close is stored"
        return (
            f"{where} is quoted over {named.symbol}, and no {named.symbol} close is stored "
            f"for that session ({seen}) — basis withheld rather than struck against "
            "another session or the front month"
        )
    if quote.volume is not None and quote.volume <= 0:
        # The board's own trade proof (#157, `_proof_clause`): a close printed
        # against zero volume is not a session the contract traded. NULL is
        # proof of nothing and stays.
        return (
            f"{where} is quoted over {named.symbol}, whose {when.isoformat()} close printed "
            "against zero volume — not a traded session, so the basis is withheld"
        )
    return named.symbol, quote.usd_per_mt


# ---------------------------------------------------------------------------
# 05 Currency
# ---------------------------------------------------------------------------
def currency_block(market: Market, ctx: SiteContext, **_) -> tuple[str, str, dict]:
    rows = ctx.fx_series(market.currency_pair)
    if not rows:
        return STATE_EMPTY, f"no {market.currency_pair} rows in the currencies layer", {}

    when, rate = rows[-1]
    changes = {}
    for back in CHANGE_LOOKBACK_OBS:
        prior = rows[-1 - back][1] if len(rows) > back else None
        changes[f"chg_{back}_pct"] = _pct_change(rate, prior)
    return STATE_OK, "", {
        "pair": market.currency_pair,
        "usd_per_home": rate,
        "home_per_usd": (1.0 / rate) if rate else None,
        "home_currency": market.home_currency,
        "as_of": when.isoformat(),
        "age_days": _age_days(ctx.today, when),
        **changes,
        # M3 #145: the USD figure carries an FX timestamp of its own. A Dalian
        # settle converts at a rate struck hours later — same-day comparable,
        # never simultaneous, and the page says so rather than implying it.
        "conversion_note": (
            f"every USD/MT figure on this page is struck at this rate. The rate and the "
            f"{market.name} print are same-day, not simultaneous — this stack stores no "
            "time of day."
        ),
    }


# ---------------------------------------------------------------------------
# 06 Weather
# ---------------------------------------------------------------------------
def weather_block(market: Market, ctx: SiteContext, **_) -> tuple[str, str, dict]:
    """This market's pins, graded by the shared rule set (#355).

    The card is the latest *observed* day and the alerts are the briefing's
    six rules on the same rows, so the page and the briefing cannot disagree
    about a pin. A rule the stored history cannot answer is withheld with its
    reason rather than read as a clear day.
    """
    regions: list[dict[str, Any]] = []
    alerts: list[dict[str, Any]] = []
    withheld: list[weather_alerts.Withheld] = []
    for region in market.weather_regions:
        assessed = weather_alerts.assess_region(region, _weather_rows(ctx, region))
        if assessed is None:
            continue
        precip_7d = assessed.observed["precipitation"].tail(7).dropna()
        region_alerts = [alert.as_dict() for alert in assessed.alerts]
        regions.append({
            "region": region,
            # Why this pin is on this page (M14 #207) — "import origin",
            # "rapeseed, not soy". Without it Europe's pins read as soy.
            "role": market.weather_roles[region],
            "as_of": assessed.as_of.isoformat(),
            "age_days": _age_days(ctx.today, assessed.as_of),
            "temp_max": assessed.temp_max,
            "temp_min": assessed.temp_min,
            "precip_mm": assessed.precip,
            "precip_7d_mm": float(precip_7d.sum()) if not precip_7d.empty else None,
            "alerts": region_alerts,
            # None when the crop is in the ground. The card renders either way.
            "season_note": out_of_season_note(region, ctx.today),
        })
        alerts.extend(region_alerts)
        withheld.extend(assessed.withheld)
    rivers = _river_rows(market, ctx)
    storms = _storm_rows(market, ctx)
    if not regions and not any(r["state"] == "ok" for r in rivers) and not (storms and storms["ports"]):
        return STATE_EMPTY, (
            "the weather layer holds no observed rows for "
            + ", ".join(market.weather_regions)
        ), {}
    return STATE_OK, "", {
        "regions": regions,
        "alerts": alerts,
        "withheld": weather_alerts.group_withheld(withheld),
        "rivers": rivers,
        "river_alerts": [r for r in rivers if r.get("low_water_breach")],
        # S2 #374 §8.3 — None when the market has no ledger (no port to list).
        "storms": storms,
    }


# ---------------------------------------------------------------------------
# 06 Weather — the river leg (Layers 27/28, M25 #272 / M26 #273)
#
# River water is tradeable weather, so it renders inside block 06 rather than
# as a tenth block: the nine block ids are the contract on every page, and a
# river block would be a permanent unfillable blank on the six markets that
# have no river pricing their freight.
#
# A configured gauge with no rows renders as a named gap rather than vanishing
# (M1 constraint 2). An unconfigured market has no gauges to miss, so it gets
# no line at all — that is a fact about the registry, not an outage.
# ---------------------------------------------------------------------------
def _river_rows(market: Market, ctx: SiteContext) -> list[dict]:
    out: list[dict] = []
    for gauge in market.river_gauges:
        spec = config.RIVER_GAUGES[gauge]
        observed, forecast, stored_attribution = _river_readings(ctx, gauge)
        row = {
            "gauge": gauge,
            "river": spec["river"],
            "unit": spec["unit"],
            "note": spec.get("note"),
            # The row's own attribution, not the registry's. It is stored per
            # row for exactly this reason (the same rule ocean_freight_rates
            # follows): a reading credited to whoever the registry names
            # *today* would re-credit history on the day a publisher changes.
            # The registry is the fallback for legacy rows that predate the
            # column, never the first answer.
            "attribution": stored_attribution or spec["attribution"],
            "url": spec.get("url"),
            "low_water": spec.get("low_water"),
            "low_water_basis": spec.get("low_water_basis"),
        }
        if not observed:
            out.append({
                **row,
                "state": STATE_EMPTY,
                "reason": f"the {spec['provider']} river layer holds no reading for {gauge}",
            })
            continue

        when, stage = observed[-1]
        # The 7-day change is struck against the reading 7 days back if there
        # is one, and withheld otherwise — never against "the oldest row we
        # happen to hold", which on a fresh database is today.
        prior = _river_reading_on_or_before(observed, when - _days(7))
        # Only the far end of the forecast trace is rendered: a trader wants
        # where the river is heading, and every intermediate day is a
        # different model output dressed as a series.
        outlook = forecast[-1] if forecast else None
        low_water = spec.get("low_water")
        breach = low_water is not None and stage <= low_water
        outlook_breach = (
            low_water is not None and outlook is not None and outlook[1] <= low_water
        )
        out.append({
            **row,
            "state": STATE_OK,
            "as_of": when.isoformat(),
            "age_days": _age_days(ctx.today, when),
            "stage": stage,
            "change_7d": None if prior is None else stage - prior[1],
            "forecast_stage": None if outlook is None else outlook[1],
            "forecast_date": None if outlook is None else outlook[0].isoformat(),
            "low_water_breach": breach,
            "low_water_outlook_breach": outlook_breach and not breach,
        })
    return out


# ---------------------------------------------------------------------------
# 06 Weather — "Storms — the port leg" (S2 #374 slice 4, #390)
#
# The exposed and unexposed places of EVERY leg in this market's ledger, in
# declared leg order, each once (§8.3, decision §14.5): a chip on any row of
# the page's ledger has its explanation on the same page. A market with no
# ledger has no storms section — a fact about the registry, like a market
# with no river gauge. The grading is analysis/hazards.py, shared with the
# briefing; nothing here decides a state.
# ---------------------------------------------------------------------------
STORMS_METHOD_CAPTION = (
    "Forecast wind radii (34, 50, 64 kt) out to 120 h, from NOAA National Hurricane "
    "Center and the Joint Typhoon Warning Center. JTWC is US military guidance, not a "
    "national warning. NHC forecasts hurricane-force radii to 72 h only. Ports and "
    "pricing points only — storm rain on a growing area is in its rainfall forecast."
)
_FLAG_LINE_CLASS = {"alert": "alert alert-err", "warning": "alert alert-warn", "info": "alert alert-info"}


def _storm_rows(market: Market, ctx: SiteContext) -> dict[str, Any] | None:
    if market.ledger is None:
        return None
    graded = ctx.place_hazards()
    ordered: list[str] = []
    for leg in market.ledger.legs:
        for place_id in leg.place_ids:
            if place_id not in ordered:
                ordered.append(place_id)

    ports: list[dict[str, Any]] = []
    not_exposed: list[dict[str, Any]] = []
    for place_id in ordered:
        raw = config.PLACES[place_id]
        if raw["basin"] == "none":
            not_exposed.append({"place_id": place_id, "short": raw["short"], "reason": raw["basin_reason"]})
            continue
        p = graded.get(place_id)
        if p is None:
            continue  # not active today (effective dates) — nothing to say
        # §8.3: the value cell carries the chip for flag / watch only; every
        # other state is muted text, so the chip means "something is coming".
        chip, chip_class = _hazard_chip(p.state, p.severity, False) if p.state in ("flag", "watch") else (None, None)
        ports.append({
            "place_id": place_id,
            "short": p.short,
            "state": p.state,
            "severity": p.severity,
            "chip": chip,
            "chip_class": chip_class,
            "reason": p.reason,
            "port_rain": ctx.port_rain(place_id),
        })

    by_id = {row["place_id"]: graded[row["place_id"]] for row in ports}
    flagged = sorted((p for p in by_id.values() if p.state == "flag"), key=hazards._place_rank)
    watches = [p for p in by_id.values() if p.state == "watch"]
    failures = [p for p in by_id.values() if p.state in ("failed", "stale")]
    clear = [p.short for p in by_id.values() if p.state == "clear"]
    not_covered = [
        {"short": p.short, "reason": config.PLACES[p.place_id].get("basin_reason") or p.reason}
        for p in by_id.values() if p.state == "not_covered"
    ]
    # §8.3 line 4: only when nothing on the page is flagged or watched AND at
    # least one port is clear — "no storm threatens a covered port" on a page
    # with no covered port would be an empty reassurance.
    clear_line = (
        f"No active storm threatens {', '.join(clear)}" if clear and not flagged and not watches else None
    )
    checked = ctx.cyclone_checked_at()
    stamps = list(checked.values())
    checked_label = (
        hazards.format_check_time(min(str(c) for c in stamps)) if stamps and all(stamps)
        else hazards.format_check_time(None)
    )
    return {
        "checked_label": checked_label,
        "ports": ports,
        "flagged": [
            {"place_id": p.place_id, "short": p.short, "severity": p.severity,
             "css_class": _FLAG_LINE_CLASS[p.severity or "info"], "text": hazards.flag_sentence(p)}
            for p in flagged
        ],
        "watches": [
            {"place_id": p.place_id, "short": p.short, "css_class": "alert alert-info",
             "text": hazards.watch_sentence(p)}
            for p in watches
        ],
        "failures": [
            {"place_id": p.place_id, "short": p.short, "state": p.state,
             "label": "no reading" if p.state == "failed" else "stale", "reason": p.reason}
            for p in failures
        ],
        "clear_line": clear_line,
        "not_covered": not_covered,
        "not_exposed": not_exposed,
        "method": STORMS_METHOD_CAPTION,
    }


def _river_reading_on_or_before(readings, when):
    """The newest reading at or before ``when``, or None if none is that old."""
    older = [row for row in readings if row[0] <= when]
    return older[-1] if older else None


def _river_readings(ctx: SiteContext, gauge: str):
    """(observed, forecast, attribution) for one gauge; readings oldest-first.

    Observed and forecast are split at read time rather than filtered at write
    time: the forecast rows are the tradeable part of the Mississippi leg, and
    NULL ``is_forecast`` counts as observed — the same convention the store
    writes and the freshness path reads.

    The attribution returned is the newest observed row's, so the credit
    travels with the reading rather than with the registry.
    """
    def build():
        if ctx.conn is None:
            return [], [], None
        cutoff = (ctx.today - _days(400)).isoformat()
        try:
            rows = ctx.conn.execute(
                "SELECT Date, stage, is_forecast, attribution FROM river_levels "
                "WHERE gauge = ? AND Date >= ? ORDER BY Date",
                (gauge, cutoff),
            ).fetchall()
        except Exception as exc:  # noqa: BLE001
            log.debug("block read: river_levels unavailable (%s)", exc)
            return [], [], None
        observed, forecast = [], []
        attribution = None
        for raw_date, stage, is_forecast, credit in rows:
            parsed = _parse_date(raw_date)
            if parsed is None or stage is None:
                continue
            if is_forecast == 1:
                forecast.append((parsed, stage))
            else:
                observed.append((parsed, stage))
                attribution = credit
        return observed, forecast, attribution
    return ctx.cached(("river_levels", gauge), build)


def _weather_rows(ctx: SiteContext, region: str) -> pd.DataFrame:
    """One region's weather rows, forecast included (the assessor drops it),
    reaching back as far as the slowest rule needs."""
    def build():
        empty = pd.DataFrame(columns=["Date", "temp_max", "temp_min", "precipitation", "is_forecast"])
        if ctx.conn is None:
            return empty
        cutoff = (ctx.today - _days(weather_alerts.LOOKBACK_DAYS)).isoformat()
        try:
            rows = ctx.conn.execute(
                "SELECT Date, temp_max, temp_min, precipitation, is_forecast FROM weather "
                "WHERE region = ? AND Date >= ? ORDER BY Date",
                (region, cutoff),
            ).fetchall()
        except Exception as exc:  # noqa: BLE001
            log.debug("block read: weather unavailable (%s)", exc)
            return empty
        return pd.DataFrame(rows, columns=list(empty.columns))
    return ctx.cached(("weather", region), build)


_MONTH_NAMES = (
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
)


def out_of_season_note(region: str, today: date) -> str | None:
    """"out of season — planting ~Oct", or None when the crop is in the ground.

    M14 #207 chose a tag over hiding the card: September Iowa rain is real and
    prices harvest logistics, so the reading stays and the tag prices it. The
    calendar is declared planting-first, so ``months[0]`` names the month.

    A region with no calendar entry returns None rather than guessing a
    season — but nothing should reach that branch: the calendar's keys are
    pinned to GROWING_REGIONS by test.
    """
    months = config.WEATHER_GROWING_SEASON_MONTHS.get(region)
    if not months or today.month in months:
        return None
    return f"out of season — planting ~{_MONTH_NAMES[months[0] - 1]}"


# ---------------------------------------------------------------------------
# 07 Supply & demand
# ---------------------------------------------------------------------------
def supply_demand_block(market: Market, ctx: SiteContext, **_) -> tuple[str, str, dict]:
    rows = _psd_rows(ctx, market.psd_country)
    flows = _flows(market, ctx)
    exports = _customs_exports(market, ctx)
    if not rows and not flows and (exports is None or exports["state"] != STATE_OK):
        return STATE_EMPTY, (
            f"the PSD layer holds no soybean balance sheet for {market.psd_country}"
        ), ({"exports": exports} if exports else {})

    lines = []
    if rows:
        years = sorted({year for _, year, _, _ in rows})
        current, prior = years[-1], (years[-2] if len(years) > 1 else None)
        by_attr = {(attr, year): (value, unit) for attr, year, value, unit in rows}
        for attribute in PSD_ATTRIBUTES:
            now = by_attr.get((attribute, current))
            if now is None:
                continue
            was = by_attr.get((attribute, prior)) if prior else None
            lines.append({
                "attribute": attribute,
                "value": now[0],
                "unit": now[1],
                "yoy_pct": _pct_change(now[0], was[0] if was else None),
            })

    return STATE_OK, "", {
        "country": market.psd_country,
        "marketing_year": (sorted({y for _, y, _, _ in rows})[-1] if rows else None),
        "lines": lines,
        # Annual, and it says so: M10 #151 keeps non-daily series in their own
        # cadence-stamped context, never as a row that reads like an outage.
        "cadence_note": "USDA PSD, annual marketing years — this does not move daily",
        "flows": flows,
        "exports": exports,
    }


def _psd_rows(ctx: SiteContext, country: str | None):
    if not country:
        return []

    def build():
        if ctx.conn is None:
            return []
        try:
            return [
                (attr, int(year), float(value), unit)
                for attr, year, value, unit in ctx.conn.execute(
                    "SELECT attribute, year, value, unit FROM psd "
                    "WHERE country = ? AND commodity = 'Soybeans' AND value IS NOT NULL",
                    (country,),
                ).fetchall()
            ]
        except Exception as exc:  # noqa: BLE001
            log.debug("block read: psd unavailable (%s)", exc)
            return []
    return ctx.cached(("psd", country), build)


def _flows(market: Market, ctx: SiteContext) -> dict | None:
    """The market's own physical-flow leg, where the registry names one.

    South Africa is the case this exists for: SAFEX is licence-capped to a
    last-traded print while SAGIS grants reproduction with acknowledgement, so
    the SA story is tonnage, not price (#157 → #202). The attribution is not
    optional and travels with the numbers.
    """
    source = market.flows
    if source is None:
        return None
    series = ctx.series(source)
    lines = []
    for key in source.keys:
        rows = series.get(key) or []
        if not rows:
            continue
        when, value, _ = rows[-1]
        lines.append({
            "key": key,
            "value": value,
            "as_of": when.isoformat(),
            "age_days": _age_days(ctx.today, when),
        })
    if not lines:
        return None
    return {
        "label": source.label or "Producer deliveries",
        "unit": "MT",
        "cadence": source.cadence,
        "lines": lines,
        "attribution": getattr(config, "SAGIS_ATTRIBUTION", None)
        if source.layer == "sagis" else None,
    }


def _customs_exports(market: Market, ctx: SiteContext) -> dict | None:
    """The newest published month of customs exports, as a sub-envelope.

    Monthly, so it sits in block 07 beside the balance sheet and is stamped
    monthly (M10 #151) — never a ledger row. Per product: tonnes, the focus
    destination against the rest of the world, and the same month a year
    earlier. The newest month is preliminary by the publisher's own rule
    (MDIC revises the current year until February), and the month after it
    is named as unreleased rather than drawn as a zero.

    The customs unit value (FOB ÷ tonnes) is computed only when the feed's
    publish gate is open: it is our arithmetic on figures licensed CC BY-ND,
    and a gated number must not reach the page data at all (#351).
    """
    if market.customs_exports is None:
        return None
    spec = config.CUSTOMS_EXPORTS[market.customs_exports]
    layer, focus = spec["layer"], spec["focus_destination"]
    publish_unit_value = bool(getattr(config, spec["unit_value_gate"]))
    monthly = _customs_monthly(ctx, spec["table"], focus)
    # Our ingest is asked first, rows or no rows: stored rows can outlive the
    # run that wrote them, and drawing them under the "release calendar, not
    # an outage" note below would blame the publisher for our own failure.
    ingest = _ingest_status(ctx.conn, layer) if ctx.conn is not None else None
    if ingest:
        return {"state": STATE_EMPTY, "reason": ingest, "data": {}}
    if not monthly:
        return {
            "state": STATE_EMPTY,
            "reason": f"the {layer} layer holds no {spec['publisher']} export rows",
            "data": {},
        }

    months = sorted({month for month, _ in monthly})
    latest = months[-1]
    as_of = _month_end(latest)
    age_days = _age_days(ctx.today, as_of)
    budget = config.LAYER_MAX_DATA_AGE_DAYS[layer]
    if age_days is not None and age_days > budget:
        return {
            "state": STATE_EMPTY,
            "reason": (
                f"the newest {spec['publisher']} month we hold is {_month_iso(latest)}, "
                f"{age_days} days old — past the layer's {budget}-day budget"
            ),
            "data": {},
        }
    year_ago = latest - 12
    unreleased = latest + 1
    rows = []
    for product in spec["products"]:
        now = monthly.get((latest, product))
        if now is None:
            continue
        was = monthly.get((year_ago, product))
        tonnes = kg_to_metric_tons(now["kg"])
        focus_tonnes = kg_to_metric_tons(now["focus_kg"])
        row = {
            "product": product,
            "tonnes": tonnes,
            "focus_tonnes": focus_tonnes,
            "rest_tonnes": tonnes - focus_tonnes,
            "million_t": kg_to_million_metric_tons(now["kg"]),
            "focus_million_t": kg_to_million_metric_tons(now["focus_kg"]),
            "rest_million_t": kg_to_million_metric_tons(now["kg"] - now["focus_kg"]),
            "focus_share_pct": (100.0 * focus_tonnes / tonnes) if tonnes else None,
            "yoy_pct": _pct_change(now["kg"], was["kg"] if was else None),
            "focus_yoy_pct": _pct_change(now["focus_kg"], was["focus_kg"] if was else None),
        }
        if publish_unit_value:
            row["unit_value_usd_mt"] = _unit_value(now)
        rows.append(row)

    data: dict[str, Any] = {
        "label": spec["label"],
        "publisher": spec["publisher"],
        "cadence": "monthly",
        "month": _month_iso(latest),
        "month_label": _month_label(latest),
        "as_of": as_of.isoformat(),
        "age_days": age_days,
        "year_ago_label": _month_label(year_ago),
        "preliminary": True,
        "unreleased_note": (
            f"{_month_iso(unreleased)} ({_month_label(unreleased)}) not yet published by "
            f"{spec['publisher']} — "
            "the release calendar, not an outage"
        ),
        "focus_destination": focus,
        "rows": rows,
        "attribution": spec["attribution"],
        "revision_note": spec["revision_note"],
    }
    if publish_unit_value:
        window = months[-spec["trend_months"]:]
        data["unit_value_trend"] = {
            product: [
                (_month_iso(month), _unit_value(monthly[(month, product)]))
                for month in window
                if (month, product) in monthly
            ]
            for product in spec["products"]
        }
        data["unit_value_caveat"] = (
            "Customs unit value: FOB USD ÷ net tonnes, an average over every "
            "contract that cleared in the month, struck weeks earlier — not a price."
        )
    else:
        data["withheld_note"] = spec["unit_value_withheld_note"]
    return {"state": STATE_OK, "reason": "", "data": data}


def _month_index(when: date) -> int:
    """A calendar month as one integer, so "a year earlier" is just −12."""
    return when.year * 12 + when.month - 1


def _month_iso(index: int) -> str:
    return f"{index // 12}-{index % 12 + 1:02d}"


def _month_label(index: int) -> str:
    return date(index // 12, index % 12 + 1, 1).strftime("%b %Y")


def _month_end(index: int) -> date:
    following = index + 1
    return date(following // 12, following % 12 + 1, 1) - _days(1)


def _customs_monthly(ctx: SiteContext, table: str, focus: str) -> dict:
    """{(month index, product): {kg, fob_usd, focus_kg}} — sums over every row."""
    def build():
        if ctx.conn is None:
            return {}
        try:
            fetched = ctx.conn.execute(
                f"SELECT month_end, product, SUM(kg), SUM(fob_usd), "  # noqa: S608 — table comes from the registry
                f"SUM(CASE WHEN country = ? THEN kg ELSE 0 END) "
                f"FROM {table} GROUP BY month_end, product",
                (focus,),
            ).fetchall()
        except Exception as exc:  # noqa: BLE001 — a missing table is data, not a crash
            log.debug("block read: %s unavailable (%s)", table, exc)
            return {}
        out = {}
        for month_end, product, kg, fob, focus_kg in fetched:
            parsed = _parse_date(month_end)
            if parsed is None:
                continue
            out[(_month_index(parsed), str(product))] = {
                "kg": int(kg), "fob_usd": int(fob), "focus_kg": int(focus_kg),
            }
        return out
    return ctx.cached(("customs", table, focus), build)


def _unit_value(month: dict) -> float | None:
    """FOB USD over net tonnes — a ratio of the month's sums, never a mean of ratios."""
    tonnes = kg_to_metric_tons(month["kg"])
    return month["fob_usd"] / tonnes if tonnes else None


# ---------------------------------------------------------------------------
# 08 News — reserved
# ---------------------------------------------------------------------------
def news_block(market: Market, ctx: SiteContext, **_) -> tuple[str, str, dict]:
    """Never ``ok``: the slot exists so the layout is right when it is filled.

    Choosing sources, licences, parsing and dedupe is a separate map (out of
    scope on #142). ``skeleton_blocks`` already renders this absent from the
    registry's own reason; this builder exists so the nine-builder set is
    complete and the reason has exactly one home.
    """
    del market, ctx
    return STATE_ABSENT, "reserved — no news layer is ingested (out of scope for map #142)", {}


# ---------------------------------------------------------------------------
# 09 Players
# ---------------------------------------------------------------------------
def players_block(market: Market, ctx: SiteContext, **_) -> tuple[str, str, dict]:
    entries = [
        entry for entry in _players(ctx)
        if entry.get("country") == market.players_country
    ]
    if not entries:
        return STATE_EMPTY, (
            f"the players knowledge base holds no entry for {market.players_country}"
        ), {}

    roles: dict[str, int] = {}
    for entry in entries:
        for role in entry.get("roles") or []:
            roles[role] = roles.get(role, 0) + 1
    return STATE_OK, "", {
        "country": market.players_country,
        "count": len(entries),
        "roles": sorted(roles.items(), key=lambda item: (-item[1], item[0])),
        "names": sorted(entry["name"] for entry in entries if entry.get("name")),
        "href": "players.html",
    }


def _players(ctx: SiteContext) -> list[dict]:
    def build():
        try:
            from scripts.generate_players import load_players
            return load_players()
        except Exception:  # noqa: BLE001
            log.warning("players knowledge base unreadable", exc_info=True)
            return []
    return ctx.cached("players", build)


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------
BUILDERS: dict[str, Callable[..., tuple[str, str, dict]]] = {
    "price": price_block,
    "ledger": ledger_block,
    "crush": crush_block,
    "basis": basis_block,
    "currency": currency_block,
    "weather": weather_block,
    "supply_demand": supply_demand_block,
    "news": news_block,
    "players": players_block,
}


def build_blocks(
    market: Market,
    tier: TierResult | None,
    ctx: SiteContext,
    *,
    markets: dict[str, Market],
    block_ids: tuple[str, ...] = BLOCK_IDS,
) -> list[Block]:
    """Every block for one market, in the contract's fixed order.

    Absent is decided before any builder runs, from the registry's own reason:
    "no source exists for this market" is a fact about the descriptor, not
    something to discover by querying an empty table (M1 #143 constraint 2 —
    the distinction between *no source* and *our ingest is broken* is the whole
    point of the two states).
    """
    blocks: list[Block] = []
    for block_id in block_ids:
        absent = absent_reason(market, block_id)
        if absent:
            blocks.append(make_block(block_id, state=STATE_ABSENT, reason=absent))
            continue
        try:
            state, reason, data = BUILDERS[block_id](market, ctx, markets=markets)
        except Exception:  # noqa: BLE001 — M8's block-level isolation
            log.error("block %s failed for %s", block_id, market.slug, exc_info=True)
            state, reason, data = STATE_EMPTY, GENERATION_ERROR, {}
        blocks.append(make_block(
            block_id,
            state=state,
            reason=reason,
            data=data or None,
            kind=_kind(market, block_id),
        ))
    return blocks


def _kind(market: Market, block_id: str) -> str | None:
    if block_id == "price" and market.price is not None:
        return market.price.quote_kind
    if block_id == "crush" and market.crush is not None:
        return market.crush.kind
    return None


__all__ = ["BUILDERS", "SiteContext", "build_blocks", "headline_ledger"]
