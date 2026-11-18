"""The MCP server: eleven read tools, one health route, one envelope.

Numbers come from the same builders the site renders — the market-page block
builders through a ``SiteContext`` on a read-only connection — so invariant 8
(every leg one session) and block-level isolation are inherited, not
re-implemented.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from fastmcp import FastMCP
from fastmcp.tools import ToolResult
from starlette.requests import Request
from starlette.responses import JSONResponse

import config
from analysis.futures.privacy import AUDIENCE_PUBLIC, assert_no_client_records
from app.block_builders import SiteContext, build_blocks
from app.blocks import STATE_EMPTY, STATE_OK
from app.markets import load_markets
from app.origins_page import build_view
from mirror_mcp import db
from mirror_mcp.envelope import envelope, meta_for
from mirror_mcp.question_log import QuestionLog
from mirror_mcp.settings import Settings
from pipeline.units import native_label
from pricing import policy, semantics

HEALTH_PATH = "/health"
SERVER_NAME = "mirror-market"
# The manifest's own word for a rostered layer with no freshness row
# (app/manifest.py `_coverage`): never learned, which is not a zero.
LAYER_NOT_RUN = "not-run"

INSTRUCTIONS = """Mirror-Market: public-data soy intelligence for physical buyers, as a daily edition.

Rules for every answer:
- Numbers come only from these tools. Never estimate, extrapolate or recall a price.
- Cite the as-of date and the source with every number. Converted block prices say USD/MT explicitly.
- series returns RAW STORAGE values, not converted prices. Use native_price_units or a row's explicit unit.
  If neither establishes the unit, do not quote or convert the value; use price or provenance to establish it.
- "empty" and "absent" are answers, and so is a withheld leg inside a block. Report them with their
  reason; never fill a gap.
- Never claim a price is settled, an authoritative settlement, executable or firm. Nothing here is a settlement.
  Run check_language on any draft that quotes a price before sending it; a "withheld" verdict means rewrite.
- This is a daily edition on honest timestamps, not a live feed. Say how old a number is.
- Retained series rows do not prove a healthy source. Report source_health failures alongside historical values.
- Growing-region weather is model-estimated, not a station observation; retain the row's basis and forecast flag.
- Confidence is derived from the price type in meta; do not upgrade it in prose.
"""


def _text(body: dict[str, Any]) -> str:
    """The human-readable half of a ToolResult: state and reason, never a second copy of the data."""
    if body["state"] == STATE_OK:
        return f"ok — as of {body['meta'].get('as_of') or body['meta'].get('latest_session')}"
    return f"{body['state']}: {body['reason']}"


def _result(body: dict[str, Any]) -> ToolResult:
    assert_no_client_records(body, where="mcp tool payload")  # invariant 4 — a desk key here is a server error
    return ToolResult(content=_text(body), structured_content=body)


def _layer_record(key: str, row: sqlite3.Row | None, now: datetime) -> dict[str, Any]:
    """One rostered layer's freshness, graded against its own budget; ``None`` where nothing was learned."""
    roster = {k: (n, src, cad, scope) for k, n, src, cad, scope in config.PRODUCTION_LAYERS}
    number, source, cadence, scope = roster[key]
    last = row["last_success"] if row else None
    # save_freshness writes UTC without an offset; imported editions may
    # also carry offsets. Preserve the original stamp in the response.
    stamp = datetime.fromisoformat(last) if last else None
    if stamp is not None and stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    age = (now - stamp).days if stamp is not None else None
    budget = config.freshness_limit_days(key)
    return {
        "layer": key, "number": number, "source": source, "cadence": cadence, "scope": scope,
        "status": row["status"] if row else LAYER_NOT_RUN,
        "last_success": last, "age_days": age, "budget_days": budget,
        "age_basis": "time since last successful fetch, not observation age",
        "over_budget": (age > budget) if age is not None else None,
        "rows_fetched": row["rows_fetched"] if row else None,
        **{name: row[name] if row else None for name in (
            "last_attempt", "observed_at", "fetch_started_at", "fetch_completed_at", "stored_at",
        )},
    }


def build_server(settings: Settings, *, clock: Callable[[], datetime] | None = None) -> FastMCP:
    """The server. ``clock`` is the one clock every age judgement uses (UTC now by default)."""
    conn = db.open_readonly(settings.db_path)
    db.assert_public_roster(conn)
    now_utc = clock or (lambda: datetime.now(timezone.utc))
    markets = load_markets()
    layer_keys = set(config.PRODUCTION_LAYER_KEYS)

    mcp = FastMCP(SERVER_NAME, instructions=INSTRUCTIONS, middleware=[QuestionLog(settings.log_path)])

    def base_meta(**extra: Any) -> dict[str, Any]:
        """What every envelope carries: which edition answered, and the price semantics of what it carries."""
        return {"db_built_at": settings.db_built_at, "latest_session": db.latest_session(conn), **extra}

    def market_or_error(slug: str):
        if slug not in markets:
            raise ValueError(f"unknown market {slug!r}; one of: {', '.join(sorted(markets))}")
        return markets[slug]

    def block_envelope(slug: str, block_id: str) -> dict[str, Any]:
        """One site block as an envelope: the builders' state/reason/data, plus price semantics from its kind."""
        market = market_or_error(slug)
        now = now_utc()
        ctx = SiteContext(conn=conn, today=now.date(), now=now)
        (block,) = build_blocks(market, None, ctx, markets=markets, block_ids=(block_id,))
        data = block.data or {}
        headline: dict[str, Any] = data["headline"] if isinstance(data.get("headline"), dict) else {}
        # price prints legs, the ledger prints rows; either way the block's date is its newest printed leg
        leg_dates: list[str] = [
            str(leg["as_of"])
            for key in ("legs", "rows")
            for leg in data.get(key, [])
            if isinstance(leg, dict) and leg.get("as_of")
        ]
        as_of = data.get("as_of") or headline.get("as_of") or (max(leg_dates) if leg_dates else None)
        source = market.price if block_id in ("price", "ledger") else getattr(market, block_id, None)
        meta = base_meta(**meta_for(block.kind), market=slug, block=block_id, as_of=as_of,
                         source_layer=getattr(source, "layer", None), venue=market.venue)
        return envelope(block.state, block.reason, data or None, meta=meta)

    @mcp.custom_route(HEALTH_PATH, methods=["GET"])
    async def health(_: Request) -> JSONResponse:
        return JSONResponse({
            "ok": True,
            "db_built_at": settings.db_built_at,
            "latest_session": db.latest_session(conn),
            "tool_count": len(await mcp.list_tools()),
        })

    @mcp.tool
    def freshness() -> ToolResult:
        """Every data layer: status word, last success, age in days, and whether it is over its freshness budget."""
        rows = {r["layer_name"]: r for r in conn.execute("SELECT * FROM data_freshness")}
        now = now_utc()
        layers = [_layer_record(key, rows.get(key), now) for key in config.PRODUCTION_LAYER_KEYS]
        meta = base_meta(**meta_for(None), as_of=now.date().isoformat())
        return _result(envelope(STATE_OK, data={"layers": layers}, meta=meta))

    @mcp.tool
    def series(
        table: str,
        key: str | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int = db.DEFAULT_LIMIT,
    ) -> ToolResult:
        """RAW rows in native storage units, not converted USD/MT prices. Never assume a unit if not given.
        Newest first, bounded. `key` filters the table's key column (commodity, pair, region, gauge, ...);
        `start`/`end` bound its date column (ISO strings). Use price() for converted market prices."""
        rows = db.bounded_rows(conn, table, key=key, start=start, end=end, limit=limit)
        writers = config.HEALTH_TABLE_WRITER_LAYERS.get(table)
        units = {
            str(row["commodity"]): native_label(str(row["commodity"]))
            for row in rows
            if table in ("prices", "forward_curve", "contract_bars") and native_label(str(row.get("commodity", "")))
        }
        meta = base_meta(
            **meta_for(None), source_table=table, source_layer=list(writers) if writers else None, key=key,
            value_basis="native_storage", native_price_units=units,
            unit_note="Raw storage values, not USD/MT unless explicitly labelled. "
                      "Use native_price_units or the row's unit; unknown units must not be inferred.",
            source_health=[
                _layer_record(
                    layer, conn.execute("SELECT * FROM data_freshness WHERE layer_name = ?", (layer,)).fetchone(),
                    now_utc(),
                )
                for layer in sorted(writers or ()) if layer in layer_keys
            ],
        )
        if not rows:
            reason = f"no rows in {table} for key={key!r} start={start!r} end={end!r}"
            return _result(envelope(STATE_EMPTY, reason, meta=meta))
        meta["as_of"] = str(rows[0][db.ALLOWED[table].date_column])
        return _result(envelope(STATE_OK, data={"rows": rows, "truncated": len(rows) == limit}, meta=meta))

    @mcp.tool
    def price(market: str) -> ToolResult:
        """The market's decision price in USD/MT (legs, headline, quote kind) — the same block the site shows —
        with the currency block it was struck at under data.currency.
        market: one of the registry slugs (cbot, dalian, brazil, argentina, india, europe, south_africa, nigeria)."""
        body = block_envelope(market, "price")
        body["data"] = {**(body["data"] or {}), "currency": block_envelope(market, "currency")}
        return _result(body)

    @mcp.tool
    def crush(market: str) -> ToolResult:
        """Crush margin for one market: board (named contract, one session), physical or administered.
        Empty with a reason when the legs are not one session's numbers."""
        return _result(block_envelope(market, "crush"))

    @mcp.tool
    def spread(market: str) -> ToolResult:
        """Basis vs the board (local minus reference, USD/MT, struck on one session) and the propagation
        ledger (who has repriced). Two named parts, each with its own state and reason; the envelope is ok
        when at least one part printed."""
        parts = {name: block_envelope(market, name) for name in ("basis", "ledger")}
        ok = [n for n, e in parts.items() if e["state"] == STATE_OK]
        state = STATE_OK if ok else STATE_EMPTY
        reason = "" if ok else "; ".join(f"{n}: {e['reason']}" for n, e in parts.items())
        meta = base_meta(**meta_for(None), market=market, as_of=next((parts[n]["meta"]["as_of"] for n in ok), None))
        return _result(envelope(state, reason, parts, meta=meta))

    @mcp.tool
    def supply_demand(market: str) -> ToolResult:
        """Annual balance sheet for the market's country (stocks-to-use, production, use) — does not move daily."""
        return _result(block_envelope(market, "supply_demand"))

    @mcp.tool
    def weather(market: str) -> ToolResult:
        """Growing-region model-estimated weather: regions, rule-based alerts, what was withheld and why,
        river gauges and tropical storms."""
        return _result(block_envelope(market, "weather"))

    @mcp.tool
    def origins(destination: str | None = None) -> ToolResult:
        """Origin comparison for a destination port: landed cost by origin per offered shipment window, the
        cheapest origin and its advantage, each row's comparability and confidence — the public edition only."""
        if destination is not None and destination not in config.DESTINATION_PORTS:
            known = ", ".join(sorted(config.DESTINATION_PORTS))
            raise ValueError(f"unknown destination {destination!r}; one of: {known}")
        now = now_utc()
        view = build_view(conn, today=now.date(), audience=AUDIENCE_PUBLIC, destination_key=destination)
        view = {**view, "rankings": [r.to_dict() for r in view["rankings"]]}
        meta = base_meta(**meta_for(None), audience=AUDIENCE_PUBLIC, destination=view.get("destination"),
                         as_of=view.get("today"))
        if all(r.get("not_comparable_reason") for r in view["rankings"]):
            reasons = sorted({str(r["not_comparable_reason"]) for r in view["rankings"]})
            return _result(envelope(STATE_EMPTY, "every window is unavailable: " + "; ".join(reasons), view, meta=meta))
        return _result(envelope(STATE_OK, data=view, meta=meta))

    @mcp.tool
    def river(gauge: str | None = None, per_gauge: int = 30) -> ToolResult:
        """Observed river stages (forecast rows excluded), newest first, `per_gauge` rows per gauge, one or all."""
        rows = db.river_rows(conn, gauge=gauge, per_gauge=per_gauge)
        meta = base_meta(**meta_for("observation"), source_table="river_levels", gauge=gauge,
                         as_of=max(r["Date"] for r in rows) if rows else None)
        if not rows:
            return _result(envelope(STATE_EMPTY, f"no observed river stages for gauge={gauge!r}", meta=meta))
        return _result(envelope(STATE_OK, data={"rows": rows}, meta=meta))

    @mcp.tool
    def provenance(layer: str) -> ToolResult:
        """Where a number comes from: the layer's source, cadence, scope, freshness budget and current freshness row,
        and which markets quote it with what price type."""
        if layer not in layer_keys:
            raise ValueError(f"unknown layer {layer!r}; one of: {', '.join(sorted(layer_keys))}")
        row = conn.execute("SELECT * FROM data_freshness WHERE layer_name = ?", (layer,)).fetchone()
        record = _layer_record(layer, row, now_utc())
        quoting = [
            {"market": slug, "quote_kind": m.price.quote_kind,
             **{k: v for k, v in meta_for(m.price.quote_kind).items() if k in ("price_type", "confidence")}}
            for slug, m in sorted(markets.items()) if m.price is not None and m.price.layer == layer
        ]
        data = {**record, "freshness": dict(row) if row else None, "markets_quoting_it": quoting,
                "private": layer in config.PRIVATE_LAYERS}
        return _result(envelope(STATE_OK, data=data, meta=base_meta(**meta_for(None), layer=layer)))

    @mcp.tool
    def check_language(text: str, price_types: list[str] | None = None) -> ToolResult:
        """Scan a draft answer for claims the data cannot support ("settlement price", "executable", "firm offer").
        price_types: the price types the draft quotes (e.g. delayed_close); none given means nothing is permitted.
        data.verdict is "clean" or "withheld"; a withheld draft must be rewritten before it is sent."""
        types = tuple(semantics.PriceType(p) for p in (price_types or ()))
        violations = [
            {"kind": v.kind.value, "phrase": v.phrase, "sentence": v.sentence, "why": v.why}
            for v in policy.scan(text, surface="mcp draft", price_types=types)
        ]
        data = {
            "verdict": "withheld" if violations else "clean",
            "violations": violations,
            "permitted_claims": sorted(k.value for k in policy.permitted_claims(types)),
        }
        return _result(envelope(STATE_OK, data=data, meta=base_meta(**meta_for(None))))

    return mcp


__all__ = ["HEALTH_PATH", "INSTRUCTIONS", "LAYER_NOT_RUN", "build_server"]
