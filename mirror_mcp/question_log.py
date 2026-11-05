"""One JSON line per tool call — what was asked, never what was answered.

The two-week readout (#581) counts usage days, tools and failures. The answer
text stays out by construction: the record is built from the request and the
outcome class only.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastmcp.server.middleware import Middleware, MiddlewareContext


class QuestionLog(Middleware):
    def __init__(self, path: Path) -> None:
        self.path = path

    async def on_call_tool(self, context: MiddlewareContext, call_next):  # noqa: ANN001
        started = datetime.now(timezone.utc)
        record: dict[str, Any] = {
            "ts": started.isoformat(),
            "tool": context.message.name,
            "args": dict(context.message.arguments or {}),
        }
        try:
            result = await call_next(context)
        except Exception as exc:
            record["status"] = "failed"
            record["error"] = type(exc).__name__
            self._emit(record, started)
            raise
        record["status"] = "error" if getattr(result, "is_error", False) else "completed"
        self._emit(record, started)
        return result

    def _emit(self, record: dict[str, Any], started: datetime) -> None:
        record["duration_ms"] = round((datetime.now(timezone.utc) - started).total_seconds() * 1000, 1)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, sort_keys=True) + "\n")
