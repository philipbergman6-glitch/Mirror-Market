"""One JSON line per tool call — what was asked, never what was answered.

The two-week readout (#581) counts usage days, tools and failures. The answer
text stays out: check_language's draft argument is replaced by its length,
since it can contain the buyer's own commercial information. Other request
arguments and the outcome class use a closed set of keys (``FIELDS``).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastmcp.server.middleware import Middleware, MiddlewareContext

FIELDS = frozenset({"ts", "tool", "args", "status", "duration_ms", "call_id", "error"})


class QuestionLog(Middleware):
    def __init__(self, path: Path) -> None:
        self.path = path

    async def on_call_tool(self, context: MiddlewareContext, call_next):  # noqa: ANN001
        started = datetime.now(timezone.utc)
        ctx = context.fastmcp_context
        arguments = dict(context.message.arguments or {})
        if context.message.name == "check_language":
            draft = arguments.pop("text", "")
            arguments["text_length"] = len(draft) if isinstance(draft, str) else None
        record: dict[str, Any] = {
            "ts": started.isoformat(),
            "tool": context.message.name,
            "args": arguments,
            "call_id": ctx.request_id if ctx is not None and ctx.request_context else None,
        }
        try:
            result = await call_next(context)
        except Exception as exc:
            record["status"] = "failed"
            record["error"] = type(exc).__name__
            raise
        else:
            record["status"] = "error" if getattr(result, "is_error", False) else "completed"
            return result
        finally:
            # After the raise/return is decided, so a log-write failure never
            # masks the tool's own outcome — and is itself loud, not swallowed.
            record["duration_ms"] = round((datetime.now(timezone.utc) - started).total_seconds() * 1000, 1)
            assert set(record) <= FIELDS, set(record) - FIELDS
            with self.path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, sort_keys=True) + "\n")
