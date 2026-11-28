"""Generated artifacts use the originating turn and ordinary file history."""
from base64 import b64decode
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from nerve.agent.backends.codex.backend import CodexClient
from nerve.agent.backends.events import GeneratedImage, TextDelta
from nerve.agent.engine import AgentEngine, _TurnState


def image_engine(tmp_path):
    engine = AgentEngine.__new__(AgentEngine)
    engine.config = SimpleNamespace(workspace=tmp_path)
    engine.agents = SimpleNamespace(
        note_main_tool_use=AsyncMock(), ingest_sidechain=AsyncMock(),
        on_main_tool_result=AsyncMock(),
    )
    engine._mcp_app_for = AsyncMock(return_value=None)
    engine._tool_result_side_effects = AsyncMock()
    return engine


@pytest.fixture
def stream():
    with patch("nerve.agent.engine.broadcaster") as broadcaster:
        broadcaster.broadcast_tool_use = AsyncMock()
        broadcaster.broadcast_tool_result = AsyncMock()
        broadcaster.broadcast_token = AsyncMock()
        broadcaster.broadcast_error = AsyncMock()
        yield broadcaster


@pytest.mark.asyncio
@pytest.mark.parametrize("result", [False, OSError("transport failed")])
async def test_delivery_failure_keeps_live_file_card_and_turn(tmp_path, stream, result):
    engine = image_engine(tmp_path)
    saved = tmp_path / "generated.png"
    sender = AsyncMock(side_effect=[result])
    st = _TurnState(generated_image_sender=sender)
    event = GeneratedImage("call-1", "data:image/png;base64,x")
    with patch("nerve.agent.engine.store_generated_image", return_value=(saved, b"image")):
        await engine._process_agent_event("s1", event, st)
        await engine._process_agent_event("s1", event, st)
        await engine._process_agent_event("s1", TextDelta("The turn continues."), st)
    await engine._settle_image_uploads("s1", st, finish=True)
    sender.assert_awaited_once_with(saved, b"image")
    stream.broadcast_error.assert_not_awaited()
    stream.broadcast_tool_result.assert_awaited_once()
    assert not st.ordered_blocks[0]["is_error"]
    assert st.ordered_blocks[0]["result"].startswith("File ready:")
    assert "attachment delivery failed" in st.ordered_blocks[-1]["content"]
    assert st.full_response_text == "The turn continues."


@pytest.mark.asyncio
async def test_no_turn_sender_keeps_file_in_session(tmp_path, stream):
    engine = image_engine(tmp_path)
    saved = tmp_path / "generated.png"
    engine._active_channel = {"s1": "telegram"}
    engine._router = SimpleNamespace(send_file=AsyncMock())
    st = _TurnState()
    with patch("nerve.agent.engine.store_generated_image", return_value=(saved, b"image")):
        await engine._process_agent_event("s1", GeneratedImage("call-1", "inline"), st)
    engine.router.send_file.assert_not_awaited()
    stream.broadcast_error.assert_not_awaited()
    stream.broadcast_token.assert_not_awaited()
    assert st.ordered_blocks[0]["result"].startswith("File ready:")


@pytest.mark.asyncio
async def test_generated_sidechain_does_not_send_or_persist(tmp_path, stream):
    engine = image_engine(tmp_path)
    sender = AsyncMock()
    st = _TurnState(generated_image_sender=sender)
    event = GeneratedImage("call-1", "inline", parent_tool_use_id="agent-1")
    await engine._process_agent_event("s1", event, st)
    sender.assert_not_awaited()
    engine.agents.ingest_sidechain.assert_awaited_once_with("s1", event)
    assert st == _TurnState(generated_image_sender=sender)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_url", ["", "data:image/png;base64,\ud800"])
async def test_invalid_generated_image_reports_failure_without_ending_turn(tmp_path, stream, invalid_url):
    engine = image_engine(tmp_path)
    sender = AsyncMock()
    st = _TurnState(generated_image_sender=sender)
    await engine._process_agent_event("s1", GeneratedImage("call-1", invalid_url), st)
    await engine._process_agent_event("s1", TextDelta("The turn continues."), st)
    stream.broadcast_error.assert_not_awaited()
    assert stream.broadcast_token.await_count == 2
    assert stream.broadcast_token.await_args_list[0].args[1].endswith("\n\n")
    assert "could not be prepared" in st.ordered_blocks[0]["content"]
    assert st.full_response_text == "The turn continues."
    sender.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_image_delivers_validated_bytes_once_and_persists_card(tmp_path, stream):
    encoded = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAAAAAA6fptVAAAACklEQVR4nGP4DwABAQEAsTj2FAAAAABJRU5ErkJggg=="
    client = CodexClient.__new__(CodexClient)
    client._items = {}
    native = {
        "id": "image-1", "type": "imageGeneration", "status": "completed", "result": encoded,
    }
    started = await client._map_item_started(native)
    completed = await client._map_item_completed(native)
    assert sum(isinstance(event, GeneratedImage) for event in completed) == 1
    expected = b64decode(encoded)

    async def sender(path, content):
        assert path.read_bytes() == expected
        path.write_bytes(b"changed")
        assert content == expected
        return True

    engine = image_engine(tmp_path)
    st = _TurnState(generated_image_sender=AsyncMock(side_effect=sender))
    for event in started + completed + completed:
        await engine._process_agent_event("s1", event, st)
    await engine._settle_image_uploads("s1", st, finish=True)
    st.generated_image_sender.assert_awaited_once()
    block = st.ordered_blocks[-1]
    assert Path(block["input"]["file_path"]).is_relative_to(tmp_path)
    assert block["tool"] == "send_file"
    assert block["result"].startswith("Sent file:")
    assert not block["is_error"]
    assert encoded not in str(st.ordered_blocks)
