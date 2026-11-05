"""The MCP server: eleven read tools, one health route, one envelope.

Numbers come from the same builders the site renders — the market-page block
builders through a ``SiteContext`` on a read-only connection — so invariant 8
(every leg one session) and block-level isolation are inherited, not
re-implemented.
"""

from __future__ import annotations

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
from app.markets import load_markets
from app.origins_page import build_view
from mirror_mcp import db
from mirror_mcp.envelope import envelope, meta_for
from mirror_mcp.question_log import QuestionLog
from mirror_mcp.settings import Settings
from pricing import policy, semantics

HEALTH_PATH = "/health"
SERVER_NAME = "mirror-market"

INSTRUCTIONS = """Mirror-Market: public-data soy intelligence for physical buyers, as a daily edition.

Rules for every answer:
- Numbers come only from these tools. Never estimate, extrapolate or recall a price.
- Cite the as-of date and the source with every number. All prices are USD/MT unless a row says otherwise.
- "withheld", "empty" and "absent" are answers. Report them with their reason; never fill a gap.
- Never write "settled", "settlement", "executable" or "firm". Nothing here is a settlement.
  Run check_language on any draft that quotes a price before sending it.
- This is a daily edition on honest timestamps, not a live feed. Say how old a number is.
- Confidence is derived from the price type in meta; do not upgrade it in prose.
"""


def _text(body: dict[str, Any]) -> str:
    """The human-readable half of a ToolResult: state and reason, never a second copy of the data."""
    if body["state"] == "ok":
        return f"ok — as of {body['meta'].get('as_of') or body['meta'].get('latest_session')}"
    return f"{body['state']}: {body['reason']}"


def _result(body: dict[str, Any]) -> ToolResult:
    assert_no_client_records(body, where="mcp tool payload")  # invariant 4 — a desk key here is a server error
    return ToolResult(content=_text(body), structured_content=body)


def build_server(settings: Settings, *, clock: Callable[[], datetime] | None = None) -> FastMCP:
    """The server. ``clock`` is the one clock every age judgement uses (UTC now by default)."""
    conn = db.open_readonly(settings.db_path)
    db.assert_public_roster(conn)
    now_utc = clock or (lambda: datetime.now(timezone.utc))
    markets = load_markets()

    mcp = FastMCP(SERVER_NAME, instructions=INSTRUCTIONS, middleware=[QuestionLog(settings.log_path)])

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

    def base_meta(**extra: Any) -> dict[str, Any]:
        return {
            "db_built_at": settings.db_built_at,
            "latest_session": db.latest_session(conn),
            **extra,
        }

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
        rows = {r["layer_name"]: dict(r) for r in conn.execute("SELECT * FROM data_freshness")}
        now = datetime.now(timezone.utc)
        layers = []
        for key, number, source, cadence, scope in config.PRODUCTION_LAYERS:
            row = rows.get(key)
            last = row["last_success"] if row else None
            age = None
            if last:
                age = (now - datetime.fromisoformat(last)).days
            budget = config.freshness_limit_days(key)
            layers.append({
                "layer": key, "number": number, "source": source, "cadence": cadence, "scope": scope,
                "status": row["status"] if row else "not-run",
                "last_success": last, "age_days": age, "budget_days": budget,
                "over_budget": (age > budget) if age is not None else None,
                "rows_fetched": row["rows_fetched"] if row else None,
            })
        return _result(envelope("ok", data={"layers": layers}, meta=base_meta(**meta_for(None))))


    @mcp.tool
    def series(
        table: str,
        key: str | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int = db.DEFAULT_LIMIT,
    ) -> ToolResult:
        """Rows from one stored table, newest first, bounded. `key` filters the table's key column
        (commodity, pair, region, gauge, ...); `start`/`end` bound its date column (ISO strings)."""
        rows = db.bounded_rows(conn, table, key=key, start=start, end=end, limit=limit)
        meta = base_meta(**meta_for(None), table=table, key=key)
        if not rows:
            reason = f"no rows in {table} for key={key!r} start={start!r} end={end!r}"
            return _result(envelope("empty", reason, meta=meta))
        meta["as_of"] = str(rows[0][db.ALLOWED[table][1]])
        return _result(envelope("ok", data={"rows": rows, "truncated": len(rows) == limit}, meta=meta))


    @mcp.tool
    def price(market: str) -> ToolResult:
        """The market's decision price in USD/MT (legs, headline, quote kind) — the same block the site shows.
        market: one of the registry slugs (cbot, dalian, brazil, argentina, india, europe, south_africa, nigeria)."""
        return _result(block_envelope(market, "price"))

    @mcp.tool
    def crush(market: str) -> ToolResult:
        """Crush margin for one market: board (named contract, one session), physical or administered.
        Withheld with a reason when the legs are not one session's numbers."""
        return _result(block_envelope(market, "crush"))

    @mcp.tool
    def spread(market: str) -> ToolResult:
        """Basis vs the board (local minus reference, USD/MT, struck on one session) and the propagation
        ledger (who has repriced). Two named parts; each carries its own state and reason."""
        parts = {name: block_envelope(market, name) for name in ("basis", "ledger")}
        ok = [n for n, e in parts.items() if e["state"] == "ok"]
        state = "ok" if ok else "empty"
        reason = "" if ok else "; ".join(f"{n}: {e['reason']}" for n, e in parts.items())
        meta = base_meta(**meta_for(None), market=market, as_of=next((parts[n]["meta"]["as_of"] for n in ok), None))
        return _result(envelope(state, reason, parts, meta=meta))

    @mcp.tool
    def supply_demand(market: str) -> ToolResult:
        """Annual balance sheet for the market's country (stocks-to-use, production, use) — does not move daily."""
        return _result(block_envelope(market, "supply_demand"))

    @mcp.tool
    def weather(market: str) -> ToolResult:
        """Growing-region weather for one market: observed regions, rule-based alerts, what was withheld and why,
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
        rankings = [r.to_dict() if hasattr(r, "to_dict") else r for r in view.get("rankings", [])]
        view = {**view, "rankings": rankings}
        meta = base_meta(**meta_for(None), audience=AUDIENCE_PUBLIC, destination=view.get("destination"),
                         as_of=view.get("today"))
        if rankings and all(r.get("not_comparable_reason") for r in rankings):
            reasons = sorted({str(r["not_comparable_reason"]) for r in rankings})
            return _result(envelope("empty", "every window is unavailable: " + "; ".join(reasons), view, meta=meta))
        return _result(envelope("ok", data=view, meta=meta))

    @mcp.tool
    def river(gauge: str | None = None, limit: int = 60) -> ToolResult:
        """Observed river stages (forecast rows excluded), newest first, for one gauge or all gauges."""
        rows = db.bounded_rows(conn, "river_levels", key=gauge, limit=limit)
        rows = [r for r in rows if not r.get("is_forecast")]
        meta = base_meta(**meta_for("observation"), gauge=gauge, as_of=rows[0]["Date"] if rows else None)
        if not rows:
            return _result(envelope("empty", f"no observed river stages for gauge={gauge!r}", meta=meta))
        return _result(envelope("ok", data={"rows": rows}, meta=meta))

    @mcp.tool
    def provenance(layer: str) -> ToolResult:
        """Where a number comes from: the layer's source, cadence, scope, freshness budget and current freshness row,
        and which markets quote it with what price type."""
        roster = {k: (n, src, cad, scope) for k, n, src, cad, scope in config.PRODUCTION_LAYERS}
        if layer not in roster:
            raise ValueError(f"unknown layer {layer!r}; one of: {', '.join(sorted(roster))}")
        number, source, cadence, scope = roster[layer]
        row = conn.execute("SELECT * FROM data_freshness WHERE layer_name = ?", (layer,)).fetchone()
        quoting = [
            {"market": slug, "quote_kind": m.price.quote_kind, **{k: v for k, v in meta_for(m.price.quote_kind).items()
                                                                 if k in ("price_type", "confidence")}}
            for slug, m in sorted(markets.items()) if m.price is not None and m.price.layer == layer
        ]
        data = {
            "layer": layer, "number": number, "source": source, "cadence": cadence, "scope": scope,
            "budget_days": config.freshness_limit_days(layer),
            "freshness": dict(row) if row else {"status": "not-run", "last_success": None},
            "markets_quoting_it": quoting,
            "private": layer in config.PRIVATE_LAYERS,
        }
        return _result(envelope("ok", data=data, meta=base_meta(**meta_for(None), layer=layer)))

    @mcp.tool
    def check_language(text: str, price_types: list[str] | None = None) -> ToolResult:
        """Scan a draft answer for claims the data cannot support ("settled", "executable", "firm offer", ...).
        price_types: the price types the draft quotes (e.g. delayed_close); none given means nothing is permitted."""
        types = tuple(semantics.PriceType(p) for p in (price_types or ()))
        violations = [
            {"kind": v.kind.value, "phrase": v.phrase, "sentence": v.sentence, "why": v.why}
            for v in policy.scan(text, surface="mcp draft", price_types=types)
        ]
        data = {"violations": violations, "permitted_claims": sorted(k.value for k in policy.permitted_claims(types))}
        meta = base_meta(**meta_for(None))
        if violations:
            return _result(envelope("withheld", f"{len(violations)} claim(s) the data cannot support", data, meta=meta))
        return _result(envelope("ok", data=data, meta=meta))

    mcp._mirror_conn = conn  # type: ignore[attr-defined]
    return mcp


__all__ = ["HEALTH_PATH", "INSTRUCTIONS", "build_server"]
