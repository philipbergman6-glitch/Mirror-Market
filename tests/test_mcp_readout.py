"""The question-log readout (#581): the trial's usage inputs, and a log it cannot read in full is an error."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "mcp_readout", Path(__file__).resolve().parents[1] / "scripts" / "mcp_readout.py"
)
assert _SPEC is not None and _SPEC.loader is not None
mcp_readout = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(mcp_readout)


def _log(tmp_path: Path, records: list) -> Path:
    path = tmp_path / "questions.jsonl"
    path.write_text("\n".join(json.dumps(r) if not isinstance(r, str) else r for r in records) + "\n")
    return path


def test_readout_counts_days_tools_and_failures(tmp_path):
    path = _log(tmp_path, [
        {"ts": "2026-11-10T07:02:00+00:00", "tool": "price", "args": {"market": "cbot"}, "status": "completed"},
        {"ts": "2026-11-10T07:05:00+00:00", "tool": "crush", "args": {"market": "dalian"}, "status": "completed"},
        {"ts": "2026-11-11T06:58:00+00:00", "tool": "price", "args": {"market": "brazil"}, "status": "completed"},
        {"ts": "2026-11-13T09:10:00+00:00", "tool": "series", "args": {"table": "nope"}, "status": "failed", "error": "ToolError"},
    ])
    summary = mcp_readout.summarise(mcp_readout.read_log(path))
    assert summary["calls"] == 4
    assert summary["usage_days"] == 3
    assert summary["days"] == {"2026-11-10": 2, "2026-11-11": 1, "2026-11-13": 1}
    assert summary["tools"] == {"price": 2, "crush": 1, "series": 1}
    assert summary["failed"] == 1 and summary["failure_rate"] == 0.25
    assert summary["first_call"] == "2026-11-10T07:02:00+00:00"
    assert summary["last_call"] == "2026-11-13T09:10:00+00:00"
    text = mcp_readout.render(summary)
    assert "usage days       3" in text and "price           2" in text


def test_an_empty_log_is_zero_usage_not_an_error(tmp_path):
    path = _log(tmp_path, [])
    summary = mcp_readout.summarise(mcp_readout.read_log(path))
    assert summary["calls"] == 0 and summary["usage_days"] == 0 and summary["failure_rate"] is None
    assert "(none)" in mcp_readout.render(summary)


def test_a_malformed_line_is_an_error(tmp_path):
    path = _log(tmp_path, [{"ts": "2026-11-10T07:02:00+00:00", "tool": "price", "status": "completed"}, "{not json"])
    with pytest.raises(mcp_readout.ReadoutError, match="line 2 is not JSON"):
        mcp_readout.read_log(path)


def test_a_record_without_the_required_fields_is_an_error(tmp_path):
    path = _log(tmp_path, [{"ts": "2026-11-10T07:02:00+00:00", "tool": "price"}])
    with pytest.raises(mcp_readout.ReadoutError, match=r"lacks \['status'\]"):
        mcp_readout.read_log(path)


def test_a_missing_log_is_an_error(tmp_path):
    with pytest.raises(mcp_readout.ReadoutError, match="no log"):
        mcp_readout.read_log(tmp_path / "nope.jsonl")


def test_the_readout_never_needs_answer_text(tmp_path):
    """The server's own log records are enough: the readout reads the closed key set and nothing more."""
    from mirror_mcp.question_log import FIELDS

    assert set(mcp_readout.REQUIRED) <= FIELDS
