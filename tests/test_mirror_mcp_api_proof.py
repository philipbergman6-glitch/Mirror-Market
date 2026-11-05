"""The three FastMCP 4.1.3 behaviours the hosted MCP edition (#578) depends on.

Written before any server code: if the installed package disagrees, the server
follows the package, not the docs read while planning.
"""

from __future__ import annotations

import pytest
from fastmcp import Client, FastMCP
from fastmcp.tools import ToolResult
from starlette.testclient import TestClient

pytestmark = pytest.mark.anyio


def _server() -> FastMCP:
    mcp = FastMCP("proof", instructions="numbers only from tools")

    @mcp.tool
    def envelope() -> ToolResult:
        return ToolResult(content="summary", structured_content={"state": "ok", "data": {"n": 1}})

    return mcp


async def test_instructions_reach_the_client_on_initialize():
    async with Client(_server()) as client:
        assert client.instructions == "numbers only from tools"  # era-neutral; initialize_result is None on modern discover


async def test_tool_result_round_trips_content_and_structured_content():
    async with Client(_server()) as client:
        result = await client.call_tool("envelope")
    assert result.structured_content == {"state": "ok", "data": {"n": 1}}
    assert result.content[0].text == "summary"


def test_http_app_serves_only_at_the_token_path():
    app = _server().http_app(path="/mcp/abc123")
    with TestClient(app) as http:
        assert http.get("/mcp").status_code == 404
        # The MCP endpoint wants a JSON-RPC POST; a bare GET must still prove the route exists.
        assert http.get("/mcp/abc123").status_code != 404
