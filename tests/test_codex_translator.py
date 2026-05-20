"""Translator tests — every supported Codex item type, in isolation.

Asserts the schema we promise downstream consumers (ingester, UI):

  * deterministic ``external_id`` shape
  * canonical block format
  * dedicated handling of encrypted reasoning, MCP tool calls, and
    the legacy ``exec_command`` header
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from nerve.sources.codex_threads.base import ThreadEvent
from nerve.sources.codex_threads.translator import (
    StoredMessage,
    translate_event,
)


def _evt(type_: str, payload: dict, *, seq: int = 1, thread_id: str = "t1") -> ThreadEvent:
    return ThreadEvent(
        type=type_,                       # type: ignore[arg-type]
        thread_id=thread_id,
        sequence=seq,
        timestamp=datetime(2026, 5, 19, 12, 0, 0, tzinfo=timezone.utc),
        payload=payload,
    )


def test_user_message_becomes_user_role():
    e = _evt("user_message", {"message": "hello"})
    [msg] = translate_event(e)
    assert msg.role == "user"
    assert msg.external_id.startswith("msg:")
    assert msg.content == "hello"
    assert msg.blocks == [{"type": "text", "content": "hello"}]


def test_user_message_preserves_event_id():
    e = _evt("user_message", {"message": "x", "event_id": "evt-42"})
    [msg] = translate_event(e)
    assert msg.external_id == "msg:evt-42"


def test_assistant_message_carries_phase_metadata():
    e = _evt("assistant_message", {"message": "answer", "phase": "final_answer"})
    [msg] = translate_event(e)
    assert msg.role == "assistant"
    assert msg.external_metadata.get("phase") == "final_answer"
    assert msg.blocks == [{"type": "text", "content": "answer"}]


def test_reasoning_block_is_a_thinking_placeholder():
    e = _evt("reasoning", {"encrypted_content": "BLOB", "summary": []})
    [msg] = translate_event(e)
    assert msg.role == "assistant"
    assert msg.external_id.startswith("reasoning:")
    assert msg.blocks[0]["type"] == "thinking"
    assert "encrypted" in msg.blocks[0]["content"].lower()
    assert msg.external_metadata["encrypted_blob_length"] == 4
    assert msg.external_metadata["encrypted_content"] == "BLOB"


def test_reasoning_can_drop_encrypted_blob_when_disabled():
    e = _evt("reasoning", {"encrypted_content": "BLOB", "summary": []})
    [msg] = translate_event(e, store_encrypted_reasoning=False)
    assert "encrypted_content" not in msg.external_metadata
    # Length is still reported so diagnostics can see "we got something".
    assert msg.external_metadata["encrypted_blob_length"] == 4


def test_function_call_translates_to_tool_call_block():
    e = _evt("tool_call", {
        "name": "exec_command",
        "arguments": '{"cmd": "ls"}',
        "call_id": "call_X",
    })
    [msg] = translate_event(e)
    assert msg.role == "assistant"
    assert msg.external_id == "tool_call:call_X"
    assert msg.blocks == [{
        "type": "tool_call",
        "tool": "exec_command",
        "input": {"cmd": "ls"},
        "tool_use_id": "call_X",
    }]


def test_function_call_mcp_namespace_prefixes_tool_name():
    e = _evt("tool_call", {
        "name": "task_list",
        "namespace": "mcp__nerve__",
        "arguments": '{"limit": 3}',
        "call_id": "c1",
    })
    [msg] = translate_event(e)
    assert msg.blocks[0]["tool"] == "mcp__nerve__task_list"


def test_function_call_falls_back_when_arguments_are_not_json():
    e = _evt("tool_call", {
        "name": "weird",
        "arguments": "not-json",
        "call_id": "c2",
    })
    [msg] = translate_event(e)
    assert msg.blocks[0]["input"] == {"raw": "not-json"}


def test_mcp_tool_call_end_emits_call_plus_merge_intent():
    e = _evt("tool_result", {
        "call_id": "mcp_1",
        "invocation": {
            "server": "nerve",
            "tool": "task_list",
            "arguments": {"limit": 3},
        },
        "result": {"Ok": "the result text"},
    })
    messages = translate_event(e)
    assert len(messages) == 2

    call = next(m for m in messages if m.role == "assistant")
    assert call.external_id == "tool_call:mcp_1"
    assert call.blocks == [{
        "type": "tool_call",
        "tool": "nerve.task_list",
        "input": {"limit": 3},
        "tool_use_id": "mcp_1",
    }]
    assert call.merge_into_tool_use_id is None

    merge = next(m for m in messages if m.role == "tool")
    # Merge intent — no standalone block, the ingester folds these into
    # the existing tool_call message's block.
    assert merge.blocks == []
    assert merge.merge_into_tool_use_id == "mcp_1"
    assert merge.merge_result == "the result text"
    assert merge.merge_is_error is False


def test_mcp_tool_call_end_with_err_marks_error():
    e = _evt("tool_result", {
        "call_id": "c",
        "invocation": {"server": "s", "tool": "t", "arguments": {}},
        "result": {"Err": "boom"},
    })
    messages = translate_event(e)
    merge = next(m for m in messages if m.role == "tool")
    assert merge.merge_into_tool_use_id == "c"
    assert merge.merge_result == "boom"
    assert merge.merge_is_error is True


def test_function_call_output_strips_exec_header():
    e = _evt("tool_result", {
        "call_id": "x",
        "output": (
            "Chunk ID: abc\nWall time: 0.5 seconds\nProcess exited with code 0\n"
            "Output:\nthe real content\nline 2\n"
        ),
    })
    [merge] = translate_event(e)
    assert merge.role == "tool"
    assert merge.merge_into_tool_use_id == "x"
    assert merge.merge_result == "the real content\nline 2\n"
    assert merge.merge_is_error is False


def test_function_call_output_without_header_preserves_content():
    e = _evt("tool_result", {"call_id": "x", "output": "plain output"})
    [merge] = translate_event(e)
    assert merge.merge_result == "plain output"


def test_function_call_output_dict_result_serializes():
    e = _evt("tool_result", {
        "call_id": "x",
        "result": {"Ok": [{"a": 1}]},
        "invocation": {"server": "s", "tool": "t", "arguments": {}},
    })
    messages = translate_event(e)
    merge = next(m for m in messages if m.role == "tool")
    # JSON-serialized for storage
    assert "a" in merge.merge_result


def test_tool_result_without_call_id_is_dropped():
    e = _evt("tool_result", {"output": "no call_id"})
    assert translate_event(e) == []


def test_unknown_event_type_returns_empty_list():
    e = _evt("turn_started", {"turn_id": "t"})
    assert translate_event(e) == []


def test_translator_preserves_event_timestamp():
    e = _evt("user_message", {"message": "x"})
    [msg] = translate_event(e)
    assert msg.created_at == datetime(2026, 5, 19, 12, 0, 0, tzinfo=timezone.utc)


def test_storedmessage_is_a_dataclass():
    """Sanity check the export — downstream consumers import it."""
    m = StoredMessage(role="user", external_id="x")
    assert m.role == "user"
    assert m.blocks == []
