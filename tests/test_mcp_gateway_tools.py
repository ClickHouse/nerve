"""Tool calls to a server of the MCP gateway (nerve.mcp_gateway.McpGatewayServer)."""

from __future__ import annotations

import json

import httpx
import pytest

from nerve.mcp_gateway import (
    TOOL_CALL_PROTOCOL_VERSION,
    GatewayCallError,
    GatewayToolResult,
    McpGatewayServer,
)

URL = "http://192.0.2.1:3130/s/docs/mcp"


def _result(id_: int, result: dict) -> dict:
    return {"jsonrpc": "2.0", "id": id_, "result": result}


def _error(id_: int | None, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": id_, "error": {"code": code, "message": message}}


class Recorder:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def body(self, index: int = -1) -> dict:
        return json.loads(self.requests[index].content)


def _server(recorder: Recorder) -> McpGatewayServer:
    return McpGatewayServer(URL, transport=httpx.MockTransport(recorder))


@pytest.mark.asyncio
async def test_a_call_and_its_structured_result() -> None:
    recorder = Recorder(httpx.Response(200, json=_result(1, {
        "content": [{"type": "text", "text": "{\"answer\": 42}"}],
        "structuredContent": {"answer": 42},
    })))
    server = _server(recorder)
    result = await server.call_tool("search", {"query": "q"})
    assert result == GatewayToolResult(is_error=False, structured={"answer": 42}, text='{"answer": 42}')
    request = recorder.requests[0]
    assert str(request.url) == URL and request.method == "POST"
    assert request.headers["Content-Type"] == "application/json"
    assert request.headers["Accept"] == "application/json, text/event-stream"
    assert request.headers["MCP-Protocol-Version"] == TOOL_CALL_PROTOCOL_VERSION
    assert "Authorization" not in request.headers  # the gateway knows the agent
    assert recorder.body() == {
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "search", "arguments": {"query": "q"}},
    }
    await server.close()


@pytest.mark.asyncio
async def test_text_only_results_are_read_as_json_when_they_are() -> None:
    recorder = Recorder(
        httpx.Response(200, json=_result(1, {"content": [{"type": "text", "text": "[1, 2]"}]})),
        httpx.Response(200, json=_result(2, {"content": [{"type": "text", "text": "plain"}]})),
    )
    server = _server(recorder)
    assert (await server.call_tool("a", {})).structured == [1, 2]
    second = await server.call_tool("b", {})
    assert second.structured is None and second.text == "plain"
    assert recorder.body(1)["id"] == 2
    await server.close()


@pytest.mark.asyncio
async def test_a_tool_that_refused_is_a_result() -> None:
    recorder = Recorder(httpx.Response(200, json=_result(1, {
        "isError": True,
        "content": [{"type": "text", "text": "memory.get: invalid arguments."}],
        "structuredContent": {"code": "VALIDATION_FAILED"},
    })))
    result = await _server(recorder).call_tool("memory_get", {})
    assert result.is_error is True and result.structured == {"code": "VALIDATION_FAILED"}


@pytest.mark.asyncio
async def test_an_event_stream_answer_is_read() -> None:
    stream = (
        "event: message\n"
        f"data: {json.dumps(_result(1, {'structuredContent': {'ok': True}}))}\n\n"
    )
    recorder = Recorder(httpx.Response(
        200, text=stream, headers={"Content-Type": "text/event-stream"},
    ))
    assert (await _server(recorder).call_tool("a", {})).structured == {"ok": True}


@pytest.mark.parametrize(("code", "retryable"), [
    (-32001, False),  # not enabled, no credential: a policy decision
    (-32002, True),   # unavailable, a limit, an open circuit: later
    (-32000, True),   # a fault the server reported
    (-32601, False),
    (-32602, False),
])
@pytest.mark.asyncio
async def test_jsonrpc_errors(code, retryable) -> None:
    recorder = Recorder(httpx.Response(200, json=_error(1, code, "MCP server unavailable")))
    with pytest.raises(GatewayCallError) as err:
        await _server(recorder).call_tool("search", {"query": "q"})
    assert err.value.code == code and err.value.retryable is retryable
    assert "MCP server unavailable" in str(err.value)
    assert err.value.rpc_message == "MCP server unavailable"  # as received


@pytest.mark.asyncio
async def test_a_limited_agent_is_told_when_to_come_back() -> None:
    recorder = Recorder(httpx.Response(
        429, json=_error(None, -32002, "too many requests in progress for this agent"),
        headers={"Retry-After": "1"},
    ))
    with pytest.raises(GatewayCallError) as err:
        await _server(recorder).call_tool("search", {})
    assert err.value.retryable is True and err.value.retry_after == 1.0
    assert err.value.status == 429


@pytest.mark.parametrize(("status", "body", "retryable", "reason"), [
    (403, {"reason": "host_not_allowed"}, False, "host_not_allowed"),
    (404, {"reason": "not_found"}, False, "not_found"),
    (503, None, True, ""),
])
@pytest.mark.asyncio
async def test_responses_without_a_jsonrpc_message(status, body, retryable, reason) -> None:
    response = httpx.Response(status, json=body) if body is not None else httpx.Response(status)
    with pytest.raises(GatewayCallError) as err:
        await _server(Recorder(response)).call_tool("search", {})
    assert err.value.status == status and err.value.reason == reason
    assert err.value.retryable is retryable and err.value.code is None


@pytest.mark.parametrize(("failure", "timed_out"), [
    (httpx.ConnectError("refused"), False),
    (httpx.ConnectTimeout("no route"), False),
    (httpx.ReadTimeout("slow"), True),
])
@pytest.mark.asyncio
async def test_transport_failures_are_retryable(failure, timed_out) -> None:
    with pytest.raises(GatewayCallError) as err:
        await _server(Recorder(failure)).call_tool("search", {"query": "secret words"})
    assert err.value.retryable is True and err.value.status is None
    # Sent and unanswered in time, or never sent at all.
    assert err.value.timed_out is timed_out
    assert "secret words" not in str(err.value)  # arguments never in the message


@pytest.mark.asyncio
async def test_a_result_that_is_not_an_object() -> None:
    recorder = Recorder(httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": []}))
    with pytest.raises(GatewayCallError, match="without a result object"):
        await _server(recorder).call_tool("search", {})
