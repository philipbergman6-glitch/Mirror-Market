"""The paste-in edition: every MCP tool's answer, written once, as files a chat client can upload.

Why this exists
---------------
The hosted server (``mirror_mcp.server``) needs a client that speaks MCP. The
one trial participant's client (Proton Lumo) does not, and the trial question
on #578 — does conversational access help a physical buyer answer useful
questions faster? — does not depend on the transport. So the same eleven tools
are called here in-process, through the same FastMCP client the tests use, and
their envelopes are written to disk: one Markdown file to upload, one JSON
file for the owner's spot-checks, one note for the participant.

What it is not: a second vocabulary. Every number is a tool envelope, with its
state, reason and meta intact. A tool that answered ``empty`` is printed as
empty with its reason, never dropped. The output directory must be private
(``assert_private_path``): the file carries only the public edition, but the
participant's own words will sit beside it once the readout starts.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastmcp import Client

import config
from analysis.trial.sanitize import assert_private_path
from mirror_mcp.server import INSTRUCTIONS, build_server
from mirror_mcp.settings import Settings

MARKET_TOOLS = ("price", "crush", "spread", "supply_demand", "weather")
UPLOAD_STEM = "mirror-market"
TOOLS_FILE = "tools.json"
NOTE_FILE = "HOW-TO-USE.md"


class EmptyEdition(RuntimeError):
    """The database holds no edition to package; writing a file would invent one (invariant 2)."""


def _assert_edition(db_path: Path) -> None:
    """Refuse a database that has never been filled: no freshness rows means no run ever stamped it."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        try:
            layers = conn.execute("SELECT COUNT(*) FROM data_freshness").fetchone()[0]
        except sqlite3.OperationalError as exc:
            raise EmptyEdition(f"{db_path}: no schema ({exc}); run the pipeline or import_history() first") from exc
        if not layers:
            raise EmptyEdition(f"{db_path}: data_freshness is empty — no run has ever stamped this database")
    finally:
        conn.close()


def _latest_briefing(db_path: Path) -> tuple[str, str] | None:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        row = conn.execute("SELECT briefing_date, text FROM briefings ORDER BY briefing_date DESC LIMIT 1").fetchone()
    finally:
        conn.close()
    return (str(row[0]), str(row[1])) if row else None


# The origins view is the page's own render model: `views` and `sections` are
# per-window HTML scaffolding (~640 KB) that repeats what `rankings` says. They
# are dropped from the upload — named in meta so the omission is visible — so
# the file stays inside what a chat client will take as one upload.
ORIGINS_OMITTED = ("views", "sections")


def _slim(key: str, body: dict[str, Any]) -> dict[str, Any]:
    if not key.startswith("origins:") or not isinstance(body.get("data"), dict):
        return body
    data = {k: v for k, v in body["data"].items() if k not in ORIGINS_OMITTED}
    return {**body, "data": data, "meta": {**body["meta"], "omitted_from_package": list(ORIGINS_OMITTED)}}


async def _call_every_tool(settings: Settings, clock: Callable[[], datetime]) -> dict[str, Any]:
    """One envelope per (tool, argument) pair, keyed the way the Markdown names them."""
    out: dict[str, Any] = {}

    async def call(key: str, tool: str, **args: Any) -> None:
        result = await client.call_tool(tool, args)
        body = result.structured_content
        if not isinstance(body, dict) or "state" not in body:
            raise RuntimeError(f"{tool}({args}) returned no envelope: {body!r}")
        out[key] = _slim(key, body)

    async with Client(build_server(settings, clock=clock)) as client:
        await call("freshness", "freshness")
        for market in sorted(config.MARKETS):
            for tool in MARKET_TOOLS:
                await call(f"{tool}:{market}", tool, market=market)
        for destination in sorted(config.DESTINATION_PORTS):
            await call(f"origins:{destination}", "origins", destination=destination)
        await call("river", "river")
    return out


def _fence(body: Any) -> str:
    compact = json.dumps(body, indent=None, separators=(",", ":"), default=str, ensure_ascii=False)
    return "```json\n" + compact + "\n```\n"


def _render_upload(as_of: str, briefing: tuple[str, str] | None, envelopes: dict[str, Any]) -> str:
    lines = [
        f"# Mirror-Market daily edition — {as_of}",
        "",
        "This file is the whole edition. Answer only from it. Each block below is one tool's answer: "
        "`state` ok/empty/absent, `reason` when not ok, `data`, and `meta` (as-of date, source layer, price type, "
        "confidence). Read the rules first.",
        "",
        "## Rules for the assistant",
        "",
        INSTRUCTIONS.strip(),
        "",
        "Tool names below are the same as the hosted edition's, so a question answered here can be re-asked there.",
        "",
    ]
    if briefing:
        lines += [f"## Daily briefing ({briefing[0]})", "", "```text", briefing[1].rstrip(), "```", ""]
    else:
        lines += ["## Daily briefing", "", "absent: no briefing row in this edition.", ""]
    lines += ["## freshness — every layer's age and budget", "", _fence(envelopes["freshness"])]
    for market in sorted(config.MARKETS):
        lines += [f"## market: {market}", ""]
        for tool in MARKET_TOOLS:
            lines += [f"### {tool}({market})", "", _fence(envelopes[f"{tool}:{market}"])]
    for destination in sorted(config.DESTINATION_PORTS):
        lines += [f"## origins({destination}) — landed cost by origin, public edition", "",
                  _fence(envelopes[f"origins:{destination}"])]
    lines += ["## river — observed stages", "", _fence(envelopes["river"])]
    return "\n".join(lines)


def _render_note(as_of: str, upload_name: str) -> str:
    return f"""# How to use the Mirror-Market daily edition in your chat assistant

Built {as_of}. One file a day: `{upload_name}`.

1. Start a new chat. Upload `{upload_name}`.
2. Ask what you would ask the desk: "what is Paranaguá worth today in USD/MT", "which origin is cheapest into
   North China for the January window", "has Argentina repriced", "how old is the Dalian number".
3. The assistant must answer only from the file, cite the as-of date and source with every number, and say
   when a block is empty and why. If it quotes a price without a date, ask it where the number came from.
4. Nothing in the file is a settlement, a firm offer or an executable price. It is a daily edition on
   honest timestamps, not a live feed.

Once a day, send back: the question you asked, the answer (a screenshot is fine), and whether it was faster
than the site. That is the whole trial record — there is no server log in this edition.
"""


def build_package(settings: Settings, out_dir: Path, *, clock: Callable[[], datetime] | None = None) -> Path:
    """Write the upload file, the raw envelopes and the participant note into ``out_dir``; return the upload path."""
    now_utc = clock or (lambda: datetime.now(timezone.utc))
    target = assert_private_path(out_dir, where="trial package")
    _assert_edition(settings.db_path)
    envelopes = asyncio.run(_call_every_tool(settings, now_utc))
    as_of = now_utc().date().isoformat()
    briefing = _latest_briefing(settings.db_path)

    target.mkdir(parents=True, exist_ok=True)
    upload = target / f"{UPLOAD_STEM}-{as_of}.md"
    upload.write_text(_render_upload(as_of, briefing, envelopes), encoding="utf-8")
    (target / TOOLS_FILE).write_text(
        json.dumps({"as_of": as_of, "db_built_at": settings.db_built_at, "envelopes": envelopes},
                   indent=1, default=str, ensure_ascii=False),
        encoding="utf-8",
    )
    (target / NOTE_FILE).write_text(_render_note(as_of, upload.name), encoding="utf-8")
    return upload


__all__ = ["EmptyEdition", "MARKET_TOOLS", "NOTE_FILE", "TOOLS_FILE", "build_package"]
