"""``python -m mirror_mcp`` — serve the edition over streamable HTTP at the secret path."""

from __future__ import annotations

import sys

from mirror_mcp.server import build_server
from mirror_mcp.settings import Settings, SettingsError


def main() -> int:
    try:
        settings = Settings.from_env().validated()
        server = build_server(settings)
    except SettingsError as exc:
        print(f"mirror_mcp: refusing to start — {exc}", file=sys.stderr)
        return 2
    server.run(
        transport="http",
        host=settings.host,
        port=settings.port,
        path=settings.mcp_path,
        show_banner=False,
        # The secret path would otherwise be printed on every access-log line.
        uvicorn_config={"access_log": False},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
