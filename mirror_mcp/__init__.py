"""The hosted MCP edition (#578): a read-only MCP server over the CI-built SQLite.

One named trial buyer, free, daily cadence. Everything here reads; nothing
here writes, orders, or settles. The package is ``mirror_mcp`` because a
top-level ``mcp/`` would shadow the SDK FastMCP depends on.
"""
