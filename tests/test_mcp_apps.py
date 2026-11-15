"""MCP Apps host: the CLI's host flag, which tool opens which view, the
result a view is handed, and the tool events that carry both
(``nerve/mcp_apps.py``, the Claude client's ``mcp_*`` methods, the engine).

The configuration is in ``test_mcp_apps_config.py``, the routes in
``test_mcp_apps_routes.py`` and the sandbox document in
``test_mcp_apps_sandbox.py``.
"""

from __future__ import annotations

import asyncio
import json
import logging
from types import SimpleNamespace

import pytest
import pytest_asyncio
from claude_agent_sdk import ClaudeAgentOptions, ToolResultBlock, UserMessage
from claude_agent_sdk._errors import CLIConnectionError
from claude_agent_sdk._internal.query import Query

import nerve.agent.backends.claude as claude_module
from nerve import mcp_apps
from nerve.agent.backends import events as ev
from nerve.agent.backends.base import BackendError, SessionSpec, TransportDiedError
from nerve.agent.backends.claude import ClaudeBackend, ClaudeClient, translate_message
from nerve.agent.engine import AgentEngine, _TurnState
from nerve.agent.streaming import broadcaster
from nerve.config import NerveConfig

SID = "s-1234"
VIEW = "ui://example/view.html"


def _status(*servers: dict) -> dict:
    return {"mcpServers": list(servers)}


def _server(name: str, *tools: dict, status: str = "connected", **extra) -> dict:
    return {"name": name, "status": status, "tools": list(tools), **extra}


def _tool(name: str, *, uri: str | None = None, visibility=None, **extra) -> dict:
    ui: dict = {}
    if uri is not None:
        ui["resourceUri"] = uri
    if visibility is not None:
        ui["visibility"] = visibility
    tool = {"name": name, "annotations": {}, **extra}
    if ui:
        tool["_meta"] = {"ui": ui}
    return tool


# --------------------------------------------------------------------------- #
#  The CLI's host flag                                                         #
# --------------------------------------------------------------------------- #


def _cli_env(config: NerveConfig) -> dict[str, str]:
    return ClaudeBackend(SimpleNamespace(config=lambda: config))._build_env()


class TestHostFlag:
    def test_absent_by_default(self):
        assert mcp_apps.HOST_ENV not in _cli_env(NerveConfig.from_dict({}))

    def test_set_when_the_feature_is_on(self):
        config = NerveConfig.from_dict({"mcp_apps": {"enabled": True}})
        assert _cli_env(config)[mcp_apps.HOST_ENV] == "1"

    def test_absent_when_the_flag_is_off(self):
        for section in ({"enabled": False}, {}):
            assert mcp_apps.HOST_ENV not in _cli_env(NerveConfig.from_dict({"mcp_apps": section}))

    def test_absent_in_external_mode(self):
        """Behind a gateway that signs in every request the feature is not
        turned on: it is for local mode only."""
        config = NerveConfig.from_dict({"mcp_apps": {"enabled": True}})
        deps = SimpleNamespace(config=lambda: config, managed_mcp=True)
        assert mcp_apps.HOST_ENV not in ClaudeBackend(deps)._build_env()
        deps.managed_mcp = False
        assert ClaudeBackend(deps)._build_env()[mcp_apps.HOST_ENV] == "1"

    def test_the_variable_is_the_one_the_cli_reads(self):
        assert mcp_apps.HOST_ENV == "CLAUDE_CODE_MCP_APPS_HOST"


# --------------------------------------------------------------------------- #
#  MCP status → tools and views                                                #
# --------------------------------------------------------------------------- #


class TestIndex:
    def test_a_tool_linked_to_a_view(self):
        servers = mcp_apps.index_servers(_status(_server("example-server", _tool("show", uri=VIEW))))
        (tool,) = servers["example-server"].tools
        assert tool == mcp_apps.AppTool(
            server="example-server", name="show",
            full_name="mcp__example-server__show", resource_uri=VIEW,
        )
        assert tool.app() == {"server": "example-server", "tool": "show", "resource_uri": VIEW}
        assert tool.view() == {
            "name": "show", "read_only": False,
            "visibility": ["model", "app"], "resource_uri": VIEW,
        }

    def test_a_tool_without_a_view_opens_nothing(self):
        (tool,) = mcp_apps.index_servers(_status(_server("s", _tool("plain"))))["s"].tools
        assert tool.resource_uri is None and tool.app() is None

    def test_underscores_in_a_server_name_are_not_split(self):
        """``mcp__my_server__get__all`` cannot be split back into its parts;
        the names come from the status."""
        servers = mcp_apps.index_servers(_status(
            _server("my_server", _tool("get__all", uri=VIEW)),
            _server("my", _tool("server__x", uri="ui://other/view.html")),
        ))
        tools = mcp_apps.tools_by_full_name(servers)
        assert tools["mcp__my_server__get__all"].app() == {
            "server": "my_server", "tool": "get__all", "resource_uri": VIEW,
        }
        assert tools["mcp__my__server__x"].server == "my"

    def test_other_characters_become_underscores_in_the_model_name(self):
        servers = mcp_apps.index_servers(_status(
            _server("plugin:docs:reader", _tool("files.read", uri=VIEW)),
        ))
        (tool,) = servers["plugin:docs:reader"].tools
        assert tool.full_name == "mcp__plugin_docs_reader__files_read"
        # The view is asked for by the names the server and Nerve know.
        assert tool.app() == {"server": "plugin:docs:reader", "tool": "files.read", "resource_uri": VIEW}

    def test_nerve_and_in_process_servers_are_left_out(self):
        servers = mcp_apps.index_servers(_status(
            _server("nerve", _tool("task_list", uri=VIEW)),
            _server("in-process", _tool("x", uri=VIEW), config={"type": "sdk"}),
            _server("example-server", _tool("show", uri=VIEW), config={"type": "stdio"}),
        ))
        assert list(servers) == ["example-server"]
        assert not mcp_apps.is_third_party_tool("mcp__nerve__task_list")
        assert mcp_apps.is_third_party_tool("mcp__nerve_tools__x")
        assert not mcp_apps.is_third_party_tool("Bash")

    def test_the_deprecated_flat_key_still_links_a_view(self):
        tool = {"name": "old", "_meta": {"ui/resourceUri": VIEW}}
        assert mcp_apps.index_servers(_status(_server("s", tool)))["s"].tools[0].resource_uri == VIEW
        both = {"name": "both", "_meta": {"ui": {"resourceUri": VIEW}, "ui/resourceUri": "ui://old/x"}}
        assert mcp_apps.index_servers(_status(_server("s", both)))["s"].tools[0].resource_uri == VIEW

    @pytest.mark.parametrize("uri", ["https://example.com/view.html", "", 7, None, ["ui://x"]])
    def test_only_a_ui_uri_is_a_view(self, uri):
        tool = {"name": "t", "_meta": {"ui": {"resourceUri": uri}}}
        assert mcp_apps.index_servers(_status(_server("s", tool)))["s"].tools[0].resource_uri is None

    def test_visibility(self):
        servers = mcp_apps.index_servers(_status(_server(
            "s",
            _tool("default"),
            _tool("app_only", visibility=["app"]),
            _tool("model_only", visibility=["model"]),
            _tool("nobody", visibility=[]),
            _tool("odd", visibility=["app", "everyone", 3]),
            _tool("not_a_list", visibility="app"),
        )))
        seen = {t.name: (t.visibility, t.app_visible) for t in servers["s"].tools}
        assert seen == {
            "default": (("model", "app"), True),
            "app_only": (("app",), True),
            "model_only": (("model",), False),
            "nobody": ((), False),
            "odd": (("app",), True),
            "not_a_list": (("model", "app"), True),
        }

    def test_read_only_is_the_annotation_as_the_cli_or_mcp_spells_it(self):
        servers = mcp_apps.index_servers(_status(_server(
            "s",
            _tool("cli", annotations={"readOnly": True}),
            _tool("mcp", annotations={"readOnlyHint": True}),
            _tool("writes", annotations={"readOnly": False, "destructive": True}),
            _tool("truthy", annotations={"readOnly": "yes"}),
            _tool("none"),
        )))
        assert {t.name: t.read_only for t in servers["s"].tools} == {
            "cli": True, "mcp": True, "writes": False, "truthy": False, "none": False,
        }

    def test_a_server_that_is_not_connected_is_known_without_tools(self):
        servers = mcp_apps.index_servers(_status({"name": "slow", "status": "pending"}))
        assert servers["slow"].connected is False and servers["slow"].tools == ()

    @pytest.mark.parametrize("status", [
        None, "nope", [], {}, {"mcpServers": None}, {"mcpServers": "x"},
        {"mcpServers": [None, 3, {"name": 5}, {"name": ""}, {"status": "connected"}]},
    ])
    def test_a_status_of_another_shape_has_no_servers(self, status):
        assert mcp_apps.index_servers(status) == {}

    def test_tools_of_another_shape_are_skipped(self):
        servers = mcp_apps.index_servers(_status(_server(
            "s", None, "x", {"name": 3}, {"name": ""}, {"name": "ok", "_meta": "x", "annotations": 4},
        )))
        assert [t.name for t in servers["s"].tools] == ["ok"]
        assert mcp_apps.index_servers(_status({"name": "s", "status": "connected", "tools": "x"}))["s"].tools == ()

    def test_a_name_two_tools_share_names_neither(self):
        servers = mcp_apps.index_servers(_status(
            _server("a.b", _tool("show", uri=VIEW)),
            _server("a_b", _tool("show", uri="ui://other/view.html")),
            _server("c", _tool("show", uri=VIEW)),
        ))
        assert list(mcp_apps.tools_by_full_name(servers)) == ["mcp__c__show"]


# --------------------------------------------------------------------------- #
#  The Claude client                                                           #
# --------------------------------------------------------------------------- #


class _FakeSDK:
    """An SDK client whose ``_send_control_request`` answers from a script."""

    def __init__(self, answers: dict | None = None):
        self.requests: list[tuple[dict, float]] = []
        self.answers = answers or {}
        sdk = self

        class _Query:
            async def _send_control_request(self, request, timeout=60.0):
                sdk.requests.append((request, timeout))
                answer = sdk.answers.get(request["subtype"], {})
                if callable(answer):
                    answer = answer(request)
                if isinstance(answer, Exception):
                    raise answer
                return answer

        self._query = _Query()

    def sent(self, subtype: str) -> list[dict]:
        return [r for r, _ in self.requests if r["subtype"] == subtype]


def _client(sdk: _FakeSDK, *, host: bool = True) -> ClaudeClient:
    client = ClaudeClient.__new__(ClaudeClient)
    client._spec = SessionSpec(
        session_id=SID, source="web", model="m", effort="high", system_prompt="", cwd="/tmp",
    )
    client._sdk = sdk
    client._mcp_apps_host = host
    return client


STATUS = _status(
    _server("nerve", _tool("task_list", uri=VIEW)),
    _server(
        "my_server",
        _tool("show_counter", uri=VIEW, annotations={"readOnly": True}),
        _tool("bump", uri=VIEW, visibility=["app"]),
        _tool("plain"),
    ),
)


class TestClientLookup:
    def test_a_client_is_a_host_only_when_the_cli_got_the_flag(self):
        spec = SessionSpec(
            session_id=SID, source="web", model="m", effort="high", system_prompt="", cwd="/tmp",
        )
        host = ClaudeClient(spec, ClaudeAgentOptions(env={mcp_apps.HOST_ENV: "1"}))
        plain = ClaudeClient(spec, ClaudeAgentOptions(env={"OTHER": "1"}))
        assert (host._mcp_apps_host, plain._mcp_apps_host) == (True, False)
        assert ClaudeClient.__new__(ClaudeClient)._mcp_apps_host is False

    @pytest.mark.asyncio
    async def test_a_tool_with_a_view(self):
        sdk = _FakeSDK({"mcp_status": STATUS})
        client = _client(sdk)
        assert await client.mcp_app_for_tool("mcp__my_server__show_counter") == {
            "server": "my_server", "tool": "show_counter", "resource_uri": VIEW,
        }
        assert sdk.requests == [({"subtype": "mcp_status"}, claude_module.MCP_STATUS_TIMEOUT)]

    @pytest.mark.asyncio
    async def test_the_status_is_read_once(self):
        sdk = _FakeSDK({"mcp_status": STATUS})
        client = _client(sdk)
        for _ in range(3):
            assert await client.mcp_app_for_tool("mcp__my_server__show_counter")
            assert await client.mcp_app_for_tool("mcp__my_server__plain") is None
        assert len(sdk.requests) == 1

    @pytest.mark.asyncio
    async def test_no_request_for_a_tool_that_cannot_have_a_view(self):
        sdk = _FakeSDK({"mcp_status": STATUS})
        client = _client(sdk)
        for name in ("Bash", "Read", "mcp__nerve__task_list", "mcp__nerve__ask_user"):
            assert await client.mcp_app_for_tool(name) is None
        assert sdk.requests == []

    @pytest.mark.asyncio
    async def test_no_request_and_no_view_without_the_host_flag(self):
        sdk = _FakeSDK({"mcp_status": STATUS})
        client = _client(sdk, host=False)
        assert await client.mcp_app_for_tool("mcp__my_server__show_counter") is None
        assert sdk.requests == []

    @pytest.mark.asyncio
    async def test_a_name_the_status_lacks_is_looked_up_once_more(self):
        """Its server may have connected since the status was read."""
        later = _status(
            *STATUS["mcpServers"], _server("late", _tool("show", uri="ui://late/view.html")),
        )
        statuses = [STATUS, later]
        sdk = _FakeSDK({"mcp_status": lambda request: statuses.pop(0)})
        client = _client(sdk)
        assert await client.mcp_app_for_tool("mcp__my_server__plain") is None
        assert await client.mcp_app_for_tool("mcp__late__show") == {
            "server": "late", "tool": "show", "resource_uri": "ui://late/view.html",
        }
        assert len(sdk.requests) == 2

    @pytest.mark.asyncio
    async def test_an_unknown_name_is_remembered(self):
        sdk = _FakeSDK({"mcp_status": STATUS})
        client = _client(sdk)
        for _ in range(4):
            assert await client.mcp_app_for_tool("mcp__gone__tool") is None
        # One status for the first lookup; the name is not asked about again.
        assert len(sdk.requests) == 1
        assert await client.mcp_app_for_tool("mcp__other__tool") is None
        assert len(sdk.requests) == 2

    @pytest.mark.asyncio
    async def test_a_failed_status_leaves_no_view_and_is_not_asked_again_at_once(self, caplog):
        sdk = _FakeSDK({"mcp_status": Exception("boom")})
        client = _client(sdk)
        with caplog.at_level(logging.DEBUG, logger="nerve.agent.backends.claude"):
            for _ in range(3):
                assert await client.mcp_app_for_tool("mcp__my_server__show_counter") is None
        assert len(sdk.requests) == 1
        assert "boom" in caplog.text
        assert all(record.levelno == logging.DEBUG for record in caplog.records)

        # After the pause it is asked again, and nothing was remembered as unknown.
        sdk.answers["mcp_status"] = STATUS
        client._mcp_status_retry_at = 0.0
        assert await client.mcp_app_for_tool("mcp__my_server__show_counter")
        assert len(sdk.requests) == 2

    @pytest.mark.asyncio
    async def test_a_late_status_does_not_hold_the_tool_event(self, monkeypatch):
        """The lookup runs on the task that reads the CLI's stream. A full
        stream buffer holds the status answer back until that task reads on,
        so the lookup must give up waiting, and still use the answer later."""
        monkeypatch.setattr(claude_module, "MCP_LOOKUP_WAIT", 0.05)
        release = asyncio.Event()
        sdk = _FakeSDK()

        class _Query:
            async def _send_control_request(self, request, timeout=60.0):
                sdk.requests.append((request, timeout))
                await release.wait()
                return STATUS

        sdk._query = _Query()
        client = _client(sdk)
        started = asyncio.get_running_loop().time()
        assert await client.mcp_app_for_tool("mcp__my_server__show_counter") is None
        assert await client.mcp_app_for_tool("mcp__gone__tool") is None
        assert asyncio.get_running_loop().time() - started < 1.0
        # Both waited for the one request that is under way.
        assert len(sdk.requests) == 1

        release.set()
        await client._mcp_lookup
        # The answer serves what comes after: the view, and the unknown name.
        assert await client.mcp_app_for_tool("mcp__my_server__show_counter") == {
            "server": "my_server", "tool": "show_counter", "resource_uri": VIEW,
        }
        assert await client.mcp_app_for_tool("mcp__gone__tool") is None
        assert len(sdk.requests) == 1

    @pytest.mark.asyncio
    async def test_disconnect_ends_a_status_request_that_is_under_way(self, monkeypatch):
        monkeypatch.setattr(claude_module, "MCP_LOOKUP_WAIT", 0.01)
        sdk = _FakeSDK()

        class _Query:
            transport = None

            async def _send_control_request(self, request, timeout=60.0):
                await asyncio.Event().wait()

        async def _disconnect():
            return None

        sdk._query = _Query()
        sdk.disconnect = _disconnect
        client = _client(sdk)
        assert await client.mcp_app_for_tool("mcp__my_server__show_counter") is None
        lookup = client._mcp_lookup
        assert lookup is not None and not lookup.done()
        await client.disconnect()
        await asyncio.wait({lookup}, timeout=5)
        assert lookup.cancelled()

    @pytest.mark.asyncio
    async def test_servers_are_read_now_and_never_include_nerve(self):
        sdk = _FakeSDK({"mcp_status": STATUS})
        client = _client(sdk)
        for _ in range(2):
            servers = await client.mcp_app_servers()
            assert list(servers) == ["my_server"]
        assert len(sdk.requests) == 2
        # The read also serves the tool lookup.
        assert await client.mcp_app_for_tool("mcp__my_server__bump")
        assert len(sdk.requests) == 2


class TestClientRequests:
    @pytest.mark.asyncio
    async def test_read_resource(self):
        answer = {"contents": [{"uri": VIEW, "mimeType": mcp_apps.RESOURCE_MIME_TYPE, "text": "<p>"}]}
        sdk = _FakeSDK({"mcp_read_resource": answer})
        assert await _client(sdk).mcp_read_resource("my_server", VIEW) == answer
        assert sdk.requests == [(
            {"subtype": "mcp_read_resource", "serverName": "my_server", "uri": VIEW},
            claude_module.MCP_READ_RESOURCE_TIMEOUT,
        )]

    @pytest.mark.asyncio
    async def test_call_tool_asks_for_the_servers_own_result_within_a_time_limit(self):
        answer = {
            "content": [{"type": "text", "text": "counter is 2"}],
            "structuredContent": {"n": 2},
            "staging": {"ok": True, "outputs": [], "inputs_used": []},
        }
        sdk = _FakeSDK({"mcp_call": answer})
        tool = mcp_apps.AppTool(server="dotted.name", name="bump.it", full_name="mcp__dotted_name__bump_it")
        assert await _client(sdk).mcp_call_tool(tool, {"by": 2}) == answer
        ((request, timeout),) = sdk.requests
        assert request == {
            "subtype": "mcp_call",
            "tool": "mcp__dotted_name__bump_it",
            "arguments": {"by": 2},
            "result_not_for_model": True,
            "timeout_ms": 60_000,
        }
        # The CLI's limit ends the tool first; the request outlives it.
        assert timeout > claude_module.MCP_APP_TOOL_TIMEOUT

    @pytest.mark.asyncio
    async def test_a_tool_that_reports_an_error_is_a_result(self):
        sdk = _FakeSDK({"mcp_call": {"staging": {
            "ok": False, "error_code": "tool_error", "detail": "tool call failed: the tool says no",
        }}})
        tool = mcp_apps.AppTool(server="s", name="t", full_name="mcp__s__t")
        assert await _client(sdk).mcp_call_tool(tool, {}) == {
            "content": "the tool says no", "isError": True,
        }

    @pytest.mark.asyncio
    async def test_a_tool_that_runs_too_long_times_out(self):
        sdk = _FakeSDK({"mcp_call": {"staging": {
            "ok": False, "error_code": "timeout", "detail": "tool call exceeded 60000ms",
        }}})
        tool = mcp_apps.AppTool(server="s", name="t", full_name="mcp__s__t")
        with pytest.raises(asyncio.TimeoutError, match="exceeded"):
            await _client(sdk).mcp_call_tool(tool, {})

    @pytest.mark.asyncio
    async def test_another_failed_call_is_a_backend_error(self):
        sdk = _FakeSDK({"mcp_call": {"staging": {
            "ok": False, "error_code": "expired", "detail": "too late",
        }}})
        tool = mcp_apps.AppTool(server="s", name="t", full_name="mcp__s__t")
        with pytest.raises(BackendError, match="too late"):
            await _client(sdk).mcp_call_tool(tool, {})

    @pytest.mark.asyncio
    async def test_the_cli_refusing_a_request_is_a_backend_error(self):
        sdk = _FakeSDK({"mcp_read_resource": Exception("Server not found: nope")})
        with pytest.raises(BackendError, match="Server not found: nope"):
            await _client(sdk).mcp_read_resource("nope", VIEW)

    @pytest.mark.asyncio
    async def test_a_dead_process_is_a_transport_error(self):
        sdk = _FakeSDK({"mcp_status": CLIConnectionError("Not connected")})
        with pytest.raises(TransportDiedError):
            await _client(sdk).mcp_app_servers()

    @pytest.mark.asyncio
    async def test_without_the_seam_the_request_is_refused(self):
        sdk = _FakeSDK()
        sdk._query = None
        with pytest.raises(BackendError, match="not available"):
            await _client(sdk).mcp_read_resource("s", VIEW)
        sdk._query = object()
        with pytest.raises(BackendError, match="not available"):
            await _client(sdk).mcp_app_servers()


class _Pipe:
    """A CLI stdin that the SDK's ``Query`` writes control requests to."""

    def __init__(self, on_write=None):
        self.written: list[dict] = []
        self._on_write = on_write

    async def write(self, data: str) -> None:
        frame = json.loads(data)
        self.written.append(frame)
        if self._on_write is not None:
            self._on_write(frame)


class TestSdkSeam:
    """``_control_request`` goes through the SDK's private
    ``Query._send_control_request``. These pin what it relies on: the frame,
    the answer, and how the SDK reports a refusal and its own timeout."""

    @staticmethod
    def _query(on_write=None) -> tuple[Query, _Pipe, ClaudeClient]:
        pipe = _Pipe(on_write)
        query = Query(transport=pipe, is_streaming_mode=True)
        return query, pipe, _client(SimpleNamespace(_query=query))

    @pytest.mark.asyncio
    async def test_the_request_frame_and_its_answer(self):
        holder: dict = {}

        def _answer(frame):
            query, rid = holder["query"], frame["request_id"]
            query.pending_control_results[rid] = {"response": {"mcpServers": []}}
            query.pending_control_responses[rid].set()

        query, pipe, client = self._query(_answer)
        holder["query"] = query
        assert await client._control_request({"subtype": "mcp_status"}, 5) == {"mcpServers": []}
        (frame,) = pipe.written
        assert frame["type"] == "control_request"
        assert frame["request"] == {"subtype": "mcp_status"}

    @pytest.mark.asyncio
    async def test_a_refusal_carries_the_clis_words(self):
        holder: dict = {}

        def _refuse(frame):
            query, rid = holder["query"], frame["request_id"]
            # What Query._read_messages stores for a control_response of subtype error.
            query.pending_control_results[rid] = Exception("Server not found: nope")
            query.pending_control_responses[rid].set()

        query, _, client = self._query(_refuse)
        holder["query"] = query
        with pytest.raises(BackendError, match="Server not found: nope"):
            await client._control_request({"subtype": "mcp_read_resource"}, 5)

    @pytest.mark.asyncio
    async def test_the_sdks_own_timeout_is_a_timeout_and_leaves_nothing_behind(self):
        query, _, client = self._query()
        with pytest.raises(asyncio.TimeoutError):
            await client._control_request({"subtype": "mcp_call"}, 0.05)
        assert query.pending_control_responses == {}
        assert query.pending_control_results == {}


# --------------------------------------------------------------------------- #
#  Tool results                                                                #
# --------------------------------------------------------------------------- #


class TestResult:
    def test_a_string_becomes_one_text_block(self):
        assert mcp_apps.call_tool_result("done") == {"content": [{"type": "text", "text": "done"}]}

    @pytest.mark.parametrize("content", [None, "", []])
    def test_nothing_is_an_empty_list(self, content):
        assert mcp_apps.call_tool_result(content) == {"content": []}

    def test_mcp_blocks_pass_through(self):
        blocks = [
            {"type": "text", "text": "a", "annotations": {"audience": ["user"]}},
            {"type": "image", "data": "aGk=", "mimeType": "image/png"},
            {"type": "audio", "data": "aGk=", "mimeType": "audio/wav"},
            {"type": "resource", "resource": {"uri": "file:///x", "text": "x"}},
            {"type": "resource_link", "uri": "file:///y", "name": "y"},
        ]
        assert mcp_apps.call_tool_result(blocks)["content"] == blocks

    def test_an_image_in_the_model_apis_shape_becomes_an_mcp_image(self):
        block = {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "aGk="}}
        assert mcp_apps.call_tool_result([block])["content"] == [
            {"type": "image", "data": "aGk=", "mimeType": "image/png"},
        ]

    def test_what_is_not_an_mcp_content_block_is_dropped(self):
        content = [
            "plain", "",
            {"type": "tool_reference", "tool_name": "mcp__s__t"},
            {"type": "text"}, {"type": "text", "text": 3},
            {"type": "image", "source": {"type": "url", "url": "https://example.com/x.png"}},
            {"type": "resource", "resource": {"uri": "file:///x"}},
            {"type": "resource_link", "uri": "file:///y"},
            {"no": "type"}, 7, None,
        ]
        assert mcp_apps.call_tool_result(content)["content"] == [{"type": "text", "text": "plain"}]

    def test_another_value_is_shown_as_json(self):
        assert mcp_apps.call_tool_result({"n": 1})["content"] == [{"type": "text", "text": '{"n": 1}'}]

    def test_structured_content_error_and_meta(self):
        result = mcp_apps.call_tool_result(
            "x", structured={"n": 1}, is_error=True, meta={"example/note": "kept"},
        )
        assert result == {
            "content": [{"type": "text", "text": "x"}],
            "structuredContent": {"n": 1},
            "isError": True,
            "_meta": {"example/note": "kept"},
        }

    def test_only_objects_are_structured_content_or_meta(self):
        result = mcp_apps.call_tool_result("x", structured=[1], meta="m")
        assert result == {"content": [{"type": "text", "text": "x"}]}
        assert "_meta" not in mcp_apps.call_tool_result("x", meta={})

    def test_the_claude_clis_tool_result(self):
        """``UserMessage.tool_use_result`` of an MCP tool with structured content."""
        result = mcp_apps.tool_event_result(
            '{"n":0}', {"content": '{"n":0}', "structuredContent": {"n": 0}}, False,
        )
        assert result == {
            "content": [{"type": "text", "text": '{"n":0}'}],
            "structuredContent": {"n": 0},
        }

    def test_a_tool_result_without_a_structured_part(self):
        blocks = [{"type": "text", "text": "note: hi"}]
        assert mcp_apps.tool_event_result(blocks, None, False) == {"content": blocks}
        # Another tool's structured output is not an MCP result.
        assert mcp_apps.tool_event_result("out", {"stdout": "out"}, False) == {
            "content": [{"type": "text", "text": "out"}],
        }

    def test_a_failed_tool_call(self):
        assert mcp_apps.tool_event_result("the tool says no", None, True) == {
            "content": [{"type": "text", "text": "the tool says no"}], "isError": True,
        }

    def test_unpaired_surrogates_are_replaced(self):
        result = mcp_apps.call_tool_result(
            "cut \ud83d", structured={"k\udc00": ["v\ud800", {"deep": "\udfff"}]},
        )
        assert result["content"][0]["text"] == "cut �"
        assert result["structuredContent"] == {"k�": ["v�", {"deep": "�"}]}
        json.dumps(result, ensure_ascii=False).encode("utf-8")
        # A whole character is left alone.
        assert mcp_apps.call_tool_result("ok \U0001f600")["content"][0]["text"] == "ok \U0001f600"


class TestRejectedCall:
    """What the Claude CLI reports in place of a result (``tool_use_result``)
    for a tool use that the person stopped while it ran, or refused."""

    @staticmethod
    def _results(tool_use_result, *, blocks: int = 1) -> list[ev.ToolResult]:
        message = UserMessage(
            content=[
                ToolResultBlock(tool_use_id=f"toolu_{n}", content="a notice", is_error=True)
                for n in range(blocks)
            ],
            tool_use_result=tool_use_result,
        )
        return [e for e in translate_message(message) if isinstance(e, ev.ToolResult)]

    def test_the_clis_notice_marks_the_result_as_rejected(self):
        (result,) = self._results("User rejected tool use")
        assert result.rejected is True
        assert result.is_error is True and result.structured is None

    @pytest.mark.parametrize("reported", [
        "Error: the tool says no",      # the tool, or its server, failed
        "Error: not allowed here",      # a permission rule refused it
        {"content": "ok"},
        None,
    ])
    def test_a_result_or_an_error_is_not_rejected(self, reported):
        (result,) = self._results(reported)
        assert result.rejected is False

    def test_with_several_results_none_is_marked(self):
        results = self._results("User rejected tool use", blocks=2)
        assert [r.rejected for r in results] == [False, False]


class TestResultCap:
    @staticmethod
    def _size(result: dict) -> int:
        return len(json.dumps(result))

    def test_a_result_within_the_limit_is_untouched(self):
        structured = {"rows": ["x" * 1000] * 1000}
        result = mcp_apps.call_tool_result("ok", structured=structured)
        assert result["structuredContent"] == structured
        assert "_meta" not in result

    def test_beyond_the_limit_the_structured_content_goes(self):
        big = {"rows": ["x" * 1024] * 3000}
        result = mcp_apps.call_tool_result(
            "summary", structured=big, is_error=True, meta={"example/note": "kept"},
        )
        assert result == {
            "content": [{"type": "text", "text": "summary"}],
            "isError": True,
            "_meta": {"example/note": "kept", "nerve/truncated": True},
        }

    def test_a_meta_that_is_too_large_goes_too(self):
        result = mcp_apps.call_tool_result("summary", meta={"blob": "x" * (3 * 1024 * 1024)})
        assert result == {
            "content": [{"type": "text", "text": "summary"}],
            "_meta": {"nerve/truncated": True},
        }

    def test_content_over_the_limit_keeps_the_start_of_its_text(self):
        content = [
            {"type": "image", "data": "A" * (3 * 1024 * 1024), "mimeType": "image/png"},
            {"type": "text", "text": "start " + "y" * (3 * 1024 * 1024)},
        ]
        result = mcp_apps.call_tool_result(content, structured={"n": 1})
        assert self._size(result) <= mcp_apps.MAX_RESULT_BYTES
        assert set(result) == {"content", "_meta"}
        assert result["_meta"] == {"nerve/truncated": True}
        (block,) = result["content"]
        assert block["type"] == "text" and block["text"].startswith("start yyy")
        assert len(block["text"]) > 500_000

    def test_the_limit_counts_escaped_characters(self):
        """Characters that JSON escapes take six bytes each in the stored form."""
        result = mcp_apps.call_tool_result("é" * (1024 * 1024))
        assert self._size(result) <= mcp_apps.MAX_RESULT_BYTES
        assert result["_meta"] == {"nerve/truncated": True}

    def test_only_images_over_the_limit_leave_no_content(self):
        content = [{"type": "image", "data": "A" * (3 * 1024 * 1024), "mimeType": "image/png"}]
        assert mcp_apps.call_tool_result(content) == {
            "content": [], "_meta": {"nerve/truncated": True},
        }


# --------------------------------------------------------------------------- #
#  The engine: tool events and blocks                                          #
# --------------------------------------------------------------------------- #


class _AppClient:
    """A live runtime that knows which tools open a view."""

    def __init__(self, apps: dict[str, dict] | None = None, error: Exception | None = None):
        self.apps = apps or {}
        self.error = error
        self.lookups: list[str] = []

    async def mcp_app_for_tool(self, tool_name: str):
        self.lookups.append(tool_name)
        if self.error is not None:
            raise self.error
        return self.apps.get(tool_name)


APP = {"server": "example-server", "tool": "show", "resource_uri": VIEW}


@pytest_asyncio.fixture
async def turn(db, tmp_path):
    workspace = tmp_path / "ws"
    workspace.mkdir()
    config = NerveConfig.from_dict({"workspace": str(workspace), "agent": {"backend": "claude"}})
    engine = AgentEngine(config, db)
    await engine.sessions.get_or_create(SID, source="web", backend="claude", actor=None)
    events: list[dict] = []

    async def _listen(session_id: str, message: dict) -> None:
        events.append(message)

    await broadcaster.register(SID, "test-mcp-apps", _listen)
    try:
        yield SimpleNamespace(engine=engine, events=events, st=_TurnState())
    finally:
        await broadcaster.unregister(SID, "test-mcp-apps")


class TestEngine:
    @pytest.mark.asyncio
    async def test_a_call_that_opens_a_view_carries_it_and_its_result(self, turn):
        client = _AppClient({"mcp__example-server__show": APP})
        turn.engine.sessions.set_client(SID, client)
        await turn.engine._process_agent_event(SID, ev.ToolUse(
            tool_use_id="toolu_1", name="mcp__example-server__show", input={"q": 1},
        ), turn.st)
        await turn.engine._process_agent_event(SID, ev.ToolResult(
            tool_use_id="toolu_1", content='{"n":0}',
            structured={"content": '{"n":0}', "structuredContent": {"n": 0}},
        ), turn.st)

        result = {"content": [{"type": "text", "text": '{"n":0}'}], "structuredContent": {"n": 0}}
        use, done = turn.events
        assert use == {
            "type": "tool_use", "session_id": SID, "tool": "mcp__example-server__show",
            "input": {"q": 1}, "tool_use_id": "toolu_1", "mcp_app": APP,
        }
        assert done == {
            "type": "tool_result", "session_id": SID, "tool_use_id": "toolu_1",
            "result": '{"n":0}', "is_error": False, "mcp_app_result": result,
        }
        (block,) = turn.st.ordered_blocks
        assert block == {
            "type": "tool_call", "tool": "mcp__example-server__show", "input": {"q": 1},
            "tool_use_id": "toolu_1", "result": '{"n":0}', "is_error": False,
            "mcp_app": APP, "mcp_app_result": result,
        }
        # What the messages table stores and the history loader reads back.
        assert json.loads(json.dumps(turn.st.ordered_blocks)) == [block]

    @pytest.mark.asyncio
    async def test_a_failed_call_of_a_view_tool_still_has_a_result(self, turn):
        turn.engine.sessions.set_client(SID, _AppClient({"mcp__example-server__show": APP}))
        await turn.engine._process_agent_event(SID, ev.ToolUse(
            tool_use_id="toolu_1", name="mcp__example-server__show", input={},
        ), turn.st)
        await turn.engine._process_agent_event(SID, ev.ToolResult(
            tool_use_id="toolu_1", content="the tool says no", is_error=True,
        ), turn.st)
        assert turn.events[1]["mcp_app_result"] == {
            "content": [{"type": "text", "text": "the tool says no"}], "isError": True,
        }

    @pytest.mark.asyncio
    async def test_a_call_that_the_person_stopped_has_no_result_for_its_view(self, turn):
        """What the runtime says to the model in place of a result is not the
        tool's: the view must hear that the call was cancelled."""
        turn.engine.sessions.set_client(SID, _AppClient({"mcp__example-server__show": APP}))
        await turn.engine._process_agent_event(SID, ev.ToolUse(
            tool_use_id="toolu_1", name="mcp__example-server__show", input={},
        ), turn.st)
        notice = "The user doesn't want to proceed with this tool use."
        await turn.engine._process_agent_event(SID, ev.ToolResult(
            tool_use_id="toolu_1", content=notice, is_error=True, rejected=True,
        ), turn.st)

        use, done = turn.events
        assert use["mcp_app"] == APP
        assert done == {
            "type": "tool_result", "session_id": SID, "tool_use_id": "toolu_1",
            "result": notice, "is_error": True,
        }
        assert turn.st.ordered_blocks == [{
            "type": "tool_call", "tool": "mcp__example-server__show", "input": {},
            "tool_use_id": "toolu_1", "result": notice, "is_error": True, "mcp_app": APP,
        }]

    @pytest.mark.asyncio
    async def test_other_tools_carry_neither_field(self, turn):
        client = _AppClient({"mcp__example-server__show": APP})
        turn.engine.sessions.set_client(SID, client)
        for tool_use_id, name in (
            ("toolu_1", "Bash"), ("toolu_2", "mcp__nerve__task_list"),
            ("toolu_3", "mcp__example-server__plain"),
        ):
            await turn.engine._process_agent_event(
                SID, ev.ToolUse(tool_use_id=tool_use_id, name=name, input={}), turn.st,
            )
            await turn.engine._process_agent_event(SID, ev.ToolResult(
                tool_use_id=tool_use_id, content="ok", structured={"content": "ok"},
            ), turn.st)
        # Only a tool of a third-party server is looked up.
        assert client.lookups == ["mcp__example-server__plain"]
        assert not any("mcp_app" in e or "mcp_app_result" in e for e in turn.events)
        assert not any(
            "mcp_app" in b or "mcp_app_result" in b for b in turn.st.ordered_blocks
        )
        assert [e["type"] for e in turn.events] == ["tool_use", "tool_result"] * 3

    @pytest.mark.asyncio
    @pytest.mark.parametrize("client", [
        _AppClient(error=RuntimeError("status timed out")),
        _AppClient({"mcp__example-server__show": "not an object"}),
        object(),       # a runtime without MCP Apps
        None,           # no live client
    ])
    async def test_a_lookup_that_fails_leaves_an_ordinary_tool_call(self, turn, client):
        if client is not None:
            turn.engine.sessions.set_client(SID, client)
        await turn.engine._process_agent_event(SID, ev.ToolUse(
            tool_use_id="toolu_1", name="mcp__example-server__show", input={},
        ), turn.st)
        await turn.engine._process_agent_event(
            SID, ev.ToolResult(tool_use_id="toolu_1", content="ok"), turn.st,
        )
        assert turn.events == [
            {"type": "tool_use", "session_id": SID, "tool": "mcp__example-server__show",
             "input": {}, "tool_use_id": "toolu_1"},
            {"type": "tool_result", "session_id": SID, "tool_use_id": "toolu_1",
             "result": "ok", "is_error": False},
        ]
        assert turn.st.ordered_blocks == [{
            "type": "tool_call", "tool": "mcp__example-server__show", "input": {},
            "tool_use_id": "toolu_1", "result": "ok", "is_error": False,
        }]

    @pytest.mark.asyncio
    async def test_a_sub_agents_call_is_never_looked_up(self, turn):
        client = _AppClient({"mcp__example-server__show": APP})
        turn.engine.sessions.set_client(SID, client)
        await turn.engine._process_agent_event(SID, ev.ToolUse(
            tool_use_id="toolu_9", name="mcp__example-server__show", input={},
            parent_tool_use_id="toolu_agent",
        ), turn.st)
        assert client.lookups == [] and turn.events == [] and turn.st.ordered_blocks == []

    @pytest.mark.asyncio
    async def test_the_events_replay_with_both_fields(self, turn):
        """A client that reconnects mid-turn gets the turn's buffered events."""
        turn.engine.sessions.set_client(SID, _AppClient({"mcp__example-server__show": APP}))
        broadcaster.start_buffering(SID)
        try:
            await turn.engine._process_agent_event(SID, ev.ToolUse(
                tool_use_id="toolu_1", name="mcp__example-server__show", input={},
            ), turn.st)
            await turn.engine._process_agent_event(SID, ev.ToolResult(
                tool_use_id="toolu_1", content="ok", structured={"content": "ok"},
            ), turn.st)
            use, done = broadcaster.get_buffer(SID)
        finally:
            broadcaster.stop_buffering(SID)
        assert use["mcp_app"] == APP
        assert done["mcp_app_result"] == {"content": [{"type": "text", "text": "ok"}]}
