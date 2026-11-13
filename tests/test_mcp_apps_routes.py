"""The MCP Apps routes (``nerve/gateway/routes/mcp_apps.py``) against the
contract the web UI is written to: the JSON shapes and the status codes.

The session's live client is a real ``ClaudeClient`` over an SDK whose
control requests are answered from a script, so each route is exercised down
to the request the CLI would get.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from claude_agent_sdk._errors import CLIConnectionError
from fastapi import FastAPI

from nerve import mcp_apps
from nerve.agent.backends.base import SessionSpec
from nerve.agent.backends.claude import ClaudeClient
from nerve.agent.engine import AgentEngine
from nerve.config import McpServerConfig, NerveConfig

SID = "s-1234"
SERVER = "my_server"
VIEW = "ui://example/view.html"
HTML = "<!doctype html><title>view</title>"

RESOURCE = f"/api/sessions/{SID}/mcp-apps/resource"
TOOLS = f"/api/sessions/{SID}/mcp-apps/tools"
CALL = f"/api/sessions/{SID}/mcp-apps/tool-call"

STATUS = {"mcpServers": [
    {"name": "nerve", "status": "connected", "config": {"type": "sdk"},
     "tools": [{"name": "task_list", "annotations": {}}]},
    {"name": SERVER, "status": "connected", "tools": [
        {"name": "show_counter", "annotations": {"readOnly": True},
         "_meta": {"ui": {"resourceUri": VIEW}}},
        {"name": "bump", "annotations": {},
         "_meta": {"ui": {"resourceUri": VIEW, "visibility": ["app"]}}},
        {"name": "plain", "annotations": {}},
        {"name": "model_only", "annotations": {}, "_meta": {"ui": {"visibility": ["model"]}}},
    ]},
    {"name": "starting", "status": "pending"},
]}

READ = {"contents": [{
    "uri": VIEW, "mimeType": "text/html;profile=mcp-app", "text": HTML,
    "_meta": {"ui": {
        "csp": {"connectDomains": ["https://api.example.com"]},
        "permissions": {"clipboardWrite": {}},
        "prefersBorder": True,
    }},
}]}

DOCUMENT = {
    "uri": VIEW, "mime_type": "text/html;profile=mcp-app", "html": HTML,
    "csp": {"connectDomains": ["https://api.example.com"]},
    "permissions": {"clipboardWrite": {}},
    "prefers_border": True,
}

CALLED = {
    "content": [{"type": "text", "text": "counter is 2"}],
    "structuredContent": {"n": 2},
    "_meta": {"example/note": "kept"},
    "staging": {"ok": True, "outputs": [], "inputs_used": []},
}


class _FakeSDK:
    """The SDK client of a live CLI: control requests answered from a script."""

    def __init__(self) -> None:
        self.requests: list[dict] = []
        self.answers: dict = {"mcp_status": STATUS, "mcp_read_resource": READ, "mcp_call": CALLED}
        self._transport = SimpleNamespace(_process=SimpleNamespace(returncode=None))
        sdk = self

        class _Query:
            async def _send_control_request(self, request, timeout=60.0):
                sdk.requests.append(request)
                answer = sdk.answers[request["subtype"]]
                if isinstance(answer, Exception):
                    raise answer
                return answer

        self._query = _Query()

    def sent(self, subtype: str) -> list[dict]:
        return [r for r in self.requests if r["subtype"] == subtype]


def _timeout(subtype: str) -> Exception:
    """What the SDK raises when the CLI does not answer a control request."""
    try:
        raise TimeoutError()
    except TimeoutError as cause:
        error = Exception(f"Control request timeout: {subtype}")
        error.__cause__ = cause
        return error


@pytest_asyncio.fixture
async def api(db, tmp_path, bypass_auth, monkeypatch):
    from nerve.gateway.routes import _deps as deps_module
    from nerve.gateway.routes.mcp_apps import router

    workspace = tmp_path / "ws"
    workspace.mkdir()
    config = NerveConfig.from_dict({
        "workspace": str(workspace),
        "agent": {"backend": "claude"},
        "mcp_apps": {"enabled": True},
    })
    engine = AgentEngine(config, db)
    # What the configuration has: the server of the tests.
    engine._mcp_servers_cache = [McpServerConfig(name=SERVER, command="example-server")]
    # The gateway decided at startup that it serves the sandbox document.
    deps = deps_module.RouteDeps(engine=engine, db=db, mcp_apps_serving=True)
    monkeypatch.setattr(deps_module, "_deps", deps)
    await engine.sessions.get_or_create(SID, source="web", backend="claude", actor=None)

    sdk = _FakeSDK()
    client = ClaudeClient.__new__(ClaudeClient)
    client._spec = SessionSpec(
        session_id=SID, source="web", model="m", effort="high", system_prompt="", cwd="/tmp",
    )
    client._sdk = sdk
    client._mcp_apps_host = True
    engine.sessions.set_client(SID, client)

    app = FastAPI()
    app.include_router(router)
    bypass_auth(app)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://nerve-test",
    ) as http:
        yield SimpleNamespace(http=http, engine=engine, deps=deps, sdk=sdk, app=app)


def _resource(server: str = SERVER, uri: str = VIEW) -> dict:
    return {"server": server, "uri": uri}


def _call(server=SERVER, tool="bump", arguments=None, **extra) -> dict:
    body = {"server": server, "tool": tool, **extra}
    if arguments is not None:
        body["arguments"] = arguments
    return body


# --------------------------------------------------------------------------- #
#  Config                                                                      #
# --------------------------------------------------------------------------- #


class TestConfig:
    @pytest.mark.asyncio
    async def test_on_while_the_gateway_serves_the_sandbox_document(self, api):
        r = await api.http.get("/api/mcp-apps/config")
        assert r.status_code == 200
        # The flag is the whole answer: the sandbox page is at the address of this API.
        assert r.json() == {"enabled": True}

    @pytest.mark.asyncio
    async def test_off_when_the_gateway_decided_at_startup_not_to_serve_it(self, api):
        """In external mode, for one."""
        api.deps.mcp_apps_serving = False
        assert (await api.http.get("/api/mcp-apps/config")).json() == {"enabled": False}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("section", [{}, {"enabled": False}])
    async def test_off_in_the_configuration(self, api, section):
        api.engine.config = NerveConfig.from_dict({
            "workspace": str(api.engine.config.workspace), "mcp_apps": section,
        })
        assert (await api.http.get("/api/mcp-apps/config")).json() == {"enabled": False}


class TestGate:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("how", ["config", "startup"])
    async def test_while_off_the_session_routes_do_not_exist(self, api, how):
        if how == "config":
            api.engine.config = NerveConfig.from_dict(
                {"workspace": str(api.engine.config.workspace)},
            )
        else:
            api.deps.mcp_apps_serving = False
        for r in (
            await api.http.get(RESOURCE, params=_resource()),
            await api.http.get(TOOLS, params={"server": SERVER}),
            await api.http.post(CALL, json=_call()),
        ):
            assert (r.status_code, r.json()) == (404, {"detail": "mcp_apps_disabled"})
        assert api.sdk.requests == []

    @pytest.mark.asyncio
    async def test_every_route_needs_authentication(self, api):
        from nerve.gateway.auth import pin_jwt_secret, require_auth

        pin_jwt_secret("a-signing-secret-for-this-test")
        api.app.dependency_overrides.pop(require_auth)
        for r in (
            await api.http.get("/api/mcp-apps/config"),
            await api.http.get(RESOURCE, params=_resource()),
            await api.http.get(TOOLS, params={"server": SERVER}),
            await api.http.post(CALL, json=_call()),
        ):
            assert r.status_code == 401, r.text
        assert api.sdk.requests == []


# --------------------------------------------------------------------------- #
#  The view's document                                                         #
# --------------------------------------------------------------------------- #


class TestResource:
    @pytest.mark.asyncio
    async def test_the_document(self, api):
        r = await api.http.get(RESOURCE, params=_resource())
        assert r.status_code == 200
        assert r.json() == {**DOCUMENT, "cached": False}
        assert api.sdk.requests == [
            {"subtype": "mcp_read_resource", "serverName": SERVER, "uri": VIEW},
        ]

    @pytest.mark.asyncio
    async def test_a_document_sent_as_base64_and_without_metadata(self, api):
        api.sdk.answers["mcp_read_resource"] = {"contents": [{
            "uri": VIEW, "mimeType": "text/html; profile=mcp-app",
            "blob": base64.b64encode(HTML.encode()).decode(),
        }]}
        r = await api.http.get(RESOURCE, params=_resource())
        assert r.json() == {
            "uri": VIEW, "mime_type": "text/html;profile=mcp-app", "html": HTML,
            "csp": None, "permissions": None, "prefers_border": None, "cached": False,
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize("params", [
        {"server": SERVER},
        {"server": SERVER, "uri": ""},
        {"server": SERVER, "uri": "https://example.com/view.html"},
        {"server": SERVER, "uri": "file:///etc/hosts"},
        {"server": SERVER, "uri": "UI://example/view.html"},
        {"server": SERVER, "uri": "ui://" + "x" * 3000},
    ])
    async def test_a_uri_that_is_not_a_ui_uri_is_400(self, api, params):
        r = await api.http.get(RESOURCE, params=params)
        assert (r.status_code, r.json()) == (400, {"detail": "bad_uri"})
        assert api.sdk.requests == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("server", [None, "", "   ", "x" * 300])
    async def test_a_bad_server_is_400(self, api, server):
        params = {"uri": VIEW} if server is None else {"uri": VIEW, "server": server}
        r = await api.http.get(RESOURCE, params=params)
        assert (r.status_code, r.json()) == (400, {"detail": "bad_server"})

    @pytest.mark.asyncio
    async def test_an_unknown_session_is_404(self, api):
        r = await api.http.get("/api/sessions/nope/mcp-apps/resource", params=_resource())
        assert (r.status_code, r.json()) == (404, {"detail": "session_not_found"})

    @pytest.mark.asyncio
    async def test_nerves_own_server_is_unknown(self, api):
        r = await api.http.get(RESOURCE, params=_resource(server="nerve"))
        assert (r.status_code, r.json()) == (404, {"detail": "server_not_found"})
        assert api.sdk.requests == []

    @pytest.mark.asyncio
    async def test_a_server_the_runtime_does_not_know_is_404(self, api):
        api.sdk.answers["mcp_read_resource"] = Exception("Server not found: nope")
        r = await api.http.get(RESOURCE, params=_resource(server="nope"))
        assert (r.status_code, r.json()) == (404, {"detail": "server_not_found"})

    @pytest.mark.asyncio
    async def test_a_resource_the_server_does_not_have_is_404(self, api):
        api.sdk.answers["mcp_read_resource"] = Exception(
            "mcp_read_resource: 'unknown resource ui://example/nope.html'",
        )
        r = await api.http.get(RESOURCE, params=_resource(uri="ui://example/nope.html"))
        assert (r.status_code, r.json()) == (404, {"detail": "resource_not_found"})

    @pytest.mark.asyncio
    async def test_a_server_that_fails_is_502_with_its_words(self, api):
        api.sdk.answers["mcp_read_resource"] = Exception(
            "mcp_read_resource: could not read the resource",
        )
        r = await api.http.get(RESOURCE, params=_resource())
        assert (r.status_code, r.json()) == (
            502, {"detail": "mcp_read_resource: could not read the resource"},
        )

    @pytest.mark.asyncio
    async def test_a_runtime_that_does_not_answer_is_502(self, api):
        api.sdk.answers["mcp_read_resource"] = _timeout("mcp_read_resource")
        r = await api.http.get(RESOURCE, params=_resource())
        assert r.status_code == 502
        assert "did not answer in time" in r.json()["detail"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("contents, reason", [
        ([], "one document"),
        ([READ["contents"][0], READ["contents"][0]], "one document"),
        ([{"uri": VIEW, "mimeType": "text/html", "text": HTML}], "not an MCP App document"),
        ([{"uri": VIEW, "text": HTML}], "not an MCP App document"),
        ([{"uri": VIEW, "mimeType": "text/html;profile=mcp-app"}], "no content"),
        ([{"uri": VIEW, "mimeType": "text/html;profile=mcp-app", "blob": "!!!"}], "not base64"),
        ([{"uri": VIEW, "mimeType": "text/html;profile=mcp-app",
           "text": "x" * (5 * 1024 * 1024 + 1)}], "larger than 5 MiB"),
        ([{"uri": VIEW, "mimeType": "text/html;profile=mcp-app",
           "blob": "A" * (7 * 1024 * 1024)}], "larger than 5 MiB"),
    ])
    async def test_what_is_not_one_app_document_is_502(self, api, contents, reason):
        api.sdk.answers["mcp_read_resource"] = {"contents": contents}
        r = await api.http.get(RESOURCE, params=_resource())
        assert r.status_code == 502
        assert reason in r.json()["detail"]
        assert mcp_apps.load_document(SERVER, VIEW) is None

    @pytest.mark.asyncio
    async def test_a_document_of_exactly_the_limit_is_served(self, api):
        api.sdk.answers["mcp_read_resource"] = {"contents": [{
            "uri": VIEW, "mimeType": "text/html;profile=mcp-app", "text": "x" * (5 * 1024 * 1024),
        }]}
        r = await api.http.get(RESOURCE, params=_resource())
        assert r.status_code == 200 and len(r.json()["html"]) == 5 * 1024 * 1024


class TestResourceCache:
    @pytest.mark.asyncio
    async def test_no_live_runtime_and_no_copy_is_409(self, api):
        api.engine.sessions.remove_client(SID)
        r = await api.http.get(RESOURCE, params=_resource())
        assert (r.status_code, r.json()) == (409, {"detail": "session_not_running"})

    @pytest.mark.asyncio
    async def test_a_session_without_a_runtime_gets_the_copy(self, api):
        assert (await api.http.get(RESOURCE, params=_resource())).json()["cached"] is False
        api.engine.sessions.remove_client(SID)
        r = await api.http.get(RESOURCE, params=_resource())
        assert r.status_code == 200
        assert r.json() == {**DOCUMENT, "cached": True}
        assert len(api.sdk.sent("mcp_read_resource")) == 1

    @pytest.mark.asyncio
    async def test_the_copy_is_per_server_and_uri(self, api):
        await api.http.get(RESOURCE, params=_resource())
        api.engine.sessions.remove_client(SID)
        for params in (_resource(uri="ui://example/other.html"), _resource(server="other")):
            r = await api.http.get(RESOURCE, params=params)
            assert (r.status_code, r.json()) == (409, {"detail": "session_not_running"})

    @pytest.mark.asyncio
    @pytest.mark.parametrize("configured", [
        [],                                                         # taken out
        [McpServerConfig(name=SERVER, command="example-server", enabled=False)],
        [McpServerConfig(name="another-server", command="example-server")],
    ])
    async def test_a_server_the_configuration_has_no_more_gets_no_copy(self, api, configured):
        """Taking a server out must stop its views: an old chat does not run
        the HTML that was kept of it."""
        assert (await api.http.get(RESOURCE, params=_resource())).json()["cached"] is False
        assert mcp_apps.load_document(SERVER, VIEW) is not None
        api.engine._mcp_servers_cache = configured

        # A session without a runtime.
        api.engine.sessions.remove_client(SID)
        r = await api.http.get(RESOURCE, params=_resource())
        assert (r.status_code, r.json()) == (409, {"detail": "session_not_running"})

    @pytest.mark.asyncio
    async def test_nor_when_a_running_sessions_read_fails(self, api):
        await api.http.get(RESOURCE, params=_resource())
        api.engine._mcp_servers_cache = []
        api.sdk.answers["mcp_read_resource"] = Exception(
            f"Server '{SERVER}' is not connected (status: pending)",
        )
        r = await api.http.get(RESOURCE, params=_resource())
        assert r.status_code == 502
        assert "html" not in r.json()

    @pytest.mark.asyncio
    async def test_a_server_that_is_configured_again_has_its_copy_again(self, api):
        await api.http.get(RESOURCE, params=_resource())
        api.engine.sessions.remove_client(SID)
        configured, api.engine._mcp_servers_cache = api.engine._mcp_servers_cache, []
        assert (await api.http.get(RESOURCE, params=_resource())).status_code == 409
        api.engine._mcp_servers_cache = configured
        r = await api.http.get(RESOURCE, params=_resource())
        assert (r.status_code, r.json()["cached"]) == (200, True)

    @pytest.mark.asyncio
    async def test_another_session_without_a_runtime_gets_the_copy_too(self, api):
        await api.http.get(RESOURCE, params=_resource())
        await api.engine.sessions.get_or_create("s-other", source="web", backend="claude", actor=None)
        r = await api.http.get("/api/sessions/s-other/mcp-apps/resource", params=_resource())
        assert (r.status_code, r.json()["cached"]) == (200, True)

    @pytest.mark.asyncio
    async def test_a_runtime_whose_process_is_gone_counts_as_none(self, api):
        api.sdk._transport._process.returncode = 1
        r = await api.http.get(RESOURCE, params=_resource())
        assert (r.status_code, r.json()) == (409, {"detail": "session_not_running"})
        assert api.sdk.requests == []

    @pytest.mark.asyncio
    async def test_a_runtime_that_cannot_host_apps_counts_as_none(self, api):
        api.engine.sessions.set_client(SID, SimpleNamespace(is_alive=lambda: True))
        for r in (
            await api.http.get(RESOURCE, params=_resource()),
            await api.http.get(TOOLS, params={"server": SERVER}),
            await api.http.post(CALL, json=_call()),
        ):
            assert (r.status_code, r.json()) == (409, {"detail": "session_not_running"})

    @pytest.mark.asyncio
    async def test_a_failed_read_falls_back_to_the_copy(self, api):
        await api.http.get(RESOURCE, params=_resource())
        api.sdk.answers["mcp_read_resource"] = Exception(
            f"Server '{SERVER}' is not connected (status: pending)",
        )
        r = await api.http.get(RESOURCE, params=_resource())
        assert (r.status_code, r.json()) == (200, {**DOCUMENT, "cached": True})

    @pytest.mark.asyncio
    async def test_a_resource_that_is_gone_does_not_fall_back(self, api):
        await api.http.get(RESOURCE, params=_resource())
        api.sdk.answers["mcp_read_resource"] = Exception("mcp_read_resource: Resource not found")
        r = await api.http.get(RESOURCE, params=_resource())
        assert (r.status_code, r.json()) == (404, {"detail": "resource_not_found"})

    @pytest.mark.asyncio
    async def test_a_new_read_replaces_the_copy(self, api):
        await api.http.get(RESOURCE, params=_resource())
        api.sdk.answers["mcp_read_resource"] = {"contents": [{
            "uri": VIEW, "mimeType": "text/html;profile=mcp-app", "text": "<p>v2</p>",
        }]}
        assert (await api.http.get(RESOURCE, params=_resource())).json()["html"] == "<p>v2</p>"
        api.engine.sessions.remove_client(SID)
        r = await api.http.get(RESOURCE, params=_resource())
        assert (r.json()["html"], r.json()["csp"], r.json()["cached"]) == ("<p>v2</p>", None, True)

    def test_the_cache_keeps_a_bounded_number_of_private_files(self, monkeypatch):
        monkeypatch.setattr(mcp_apps, "MAX_CACHED_DOCUMENTS", 3)
        for i in range(6):
            mcp_apps.store_document(SERVER, f"ui://example/{i}.html", {**DOCUMENT, "html": str(i)})
        files = list(mcp_apps.documents_dir().iterdir())
        assert len(files) == 3
        assert all(f.stat().st_mode & 0o077 == 0 for f in files)
        assert mcp_apps.documents_dir().stat().st_mode & 0o077 == 0
        assert mcp_apps.load_document(SERVER, "ui://example/5.html")["html"] == "5"

    def test_an_unchanged_copy_is_not_written_again(self):
        """Every view on a page reads its document: the same document must
        not be megabytes written each time."""
        mcp_apps.store_document(SERVER, VIEW, DOCUMENT)
        (file,) = mcp_apps.documents_dir().iterdir()
        inode = file.stat().st_ino
        os.utime(file, (1, 1))
        mcp_apps.store_document(SERVER, VIEW, DOCUMENT)
        assert file.stat().st_ino == inode       # the file was left in place
        assert file.stat().st_mtime > 1          # and marked as used
        mcp_apps.store_document(SERVER, VIEW, {**DOCUMENT, "html": "<p>v2</p>"})
        assert file.stat().st_ino != inode
        assert mcp_apps.load_document(SERVER, VIEW)["html"] == "<p>v2</p>"

    def test_a_damaged_copy_is_no_copy(self):
        mcp_apps.store_document(SERVER, VIEW, DOCUMENT)
        (file,) = mcp_apps.documents_dir().iterdir()
        assert mcp_apps.load_document(SERVER, VIEW) == DOCUMENT
        for damaged in ("{", "[]", json.dumps({"server": "other", "uri": VIEW, "document": DOCUMENT}),
                        json.dumps({"server": SERVER, "uri": VIEW, "document": {"html": 3}})):
            file.write_text(damaged)
            assert mcp_apps.load_document(SERVER, VIEW) is None


# --------------------------------------------------------------------------- #
#  Tools                                                                       #
# --------------------------------------------------------------------------- #


class TestTools:
    @pytest.mark.asyncio
    async def test_the_tools_of_a_server(self, api):
        r = await api.http.get(TOOLS, params={"server": SERVER})
        assert r.status_code == 200
        assert r.json() == {"tools": [
            {"name": "show_counter", "read_only": True, "visibility": ["model", "app"],
             "resource_uri": VIEW},
            {"name": "bump", "read_only": False, "visibility": ["app"], "resource_uri": VIEW},
            {"name": "plain", "read_only": False, "visibility": ["model", "app"],
             "resource_uri": None},
            {"name": "model_only", "read_only": False, "visibility": ["model"],
             "resource_uri": None},
        ]}
        assert api.sdk.requests == [{"subtype": "mcp_status"}]

    @pytest.mark.asyncio
    async def test_the_status_is_read_for_each_request(self, api):
        await api.http.get(TOOLS, params={"server": SERVER})
        api.sdk.answers["mcp_status"] = {"mcpServers": [
            {"name": SERVER, "status": "connected", "tools": [{"name": "new_tool"}]},
        ]}
        r = await api.http.get(TOOLS, params={"server": SERVER})
        assert [t["name"] for t in r.json()["tools"]] == ["new_tool"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("server, status, detail", [
        ("nope", 404, "server_not_found"),
        ("nerve", 404, "server_not_found"),
        ("", 400, "bad_server"),
    ])
    async def test_an_unknown_or_bad_server(self, api, server, status, detail):
        r = await api.http.get(TOOLS, params={"server": server})
        assert (r.status_code, r.json()) == (status, {"detail": detail})

    @pytest.mark.asyncio
    async def test_an_unknown_session_is_404(self, api):
        r = await api.http.get("/api/sessions/nope/mcp-apps/tools", params={"server": SERVER})
        assert (r.status_code, r.json()) == (404, {"detail": "session_not_found"})

    @pytest.mark.asyncio
    async def test_no_live_runtime_is_409(self, api):
        api.engine.sessions.remove_client(SID)
        r = await api.http.get(TOOLS, params={"server": SERVER})
        assert (r.status_code, r.json()) == (409, {"detail": "session_not_running"})

    @pytest.mark.asyncio
    async def test_a_server_that_is_not_connected_is_502(self, api):
        r = await api.http.get(TOOLS, params={"server": "starting"})
        assert r.status_code == 502
        assert r.json() == {"detail": "MCP server 'starting' is not connected (pending)"}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("error, status", [
        (Exception("boom"), 502),
        (CLIConnectionError("Not connected"), 502),
        (_timeout("mcp_status"), 504),
    ])
    async def test_a_runtime_that_fails_or_does_not_answer(self, api, error, status):
        api.sdk.answers["mcp_status"] = error
        r = await api.http.get(TOOLS, params={"server": SERVER})
        assert r.status_code == status
        assert isinstance(r.json()["detail"], str) and r.json()["detail"]


# --------------------------------------------------------------------------- #
#  Tool calls                                                                  #
# --------------------------------------------------------------------------- #


class TestToolCall:
    @pytest.mark.asyncio
    async def test_a_call_for_a_view(self, api):
        r = await api.http.post(CALL, json=_call(arguments={"by": 2}))
        assert r.status_code == 200
        assert r.json() == {
            "content": [{"type": "text", "text": "counter is 2"}],
            "structuredContent": {"n": 2},
            "_meta": {"example/note": "kept"},
        }
        assert api.sdk.sent("mcp_call") == [{
            "subtype": "mcp_call",
            "tool": f"mcp__{SERVER}__bump",
            "arguments": {"by": 2},
            "result_not_for_model": True,
            "timeout_ms": 60_000,
        }]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("body", [_call(), _call(arguments={}), {**_call(), "arguments": None}])
    async def test_arguments_may_be_left_out(self, api, body):
        assert (await api.http.post(CALL, json=body)).status_code == 200
        assert api.sdk.sent("mcp_call")[0]["arguments"] == {}

    @pytest.mark.asyncio
    async def test_a_tool_for_the_model_and_the_app(self, api):
        r = await api.http.post(CALL, json=_call(tool="show_counter"))
        assert r.status_code == 200
        assert api.sdk.sent("mcp_call")[0]["tool"] == f"mcp__{SERVER}__show_counter"

    @pytest.mark.asyncio
    async def test_a_plain_string_result_becomes_a_content_list(self, api):
        api.sdk.answers["mcp_call"] = {"content": '{"n":2}', "structuredContent": {"n": 2}}
        r = await api.http.post(CALL, json=_call())
        assert r.json() == {
            "content": [{"type": "text", "text": '{"n":2}'}], "structuredContent": {"n": 2},
        }

    @pytest.mark.asyncio
    async def test_a_tool_that_reports_an_error_is_still_200(self, api):
        api.sdk.answers["mcp_call"] = {"staging": {
            "ok": False, "error_code": "tool_error", "detail": "tool call failed: the tool says no",
        }}
        r = await api.http.post(CALL, json=_call())
        assert (r.status_code, r.json()) == (200, {
            "content": [{"type": "text", "text": "the tool says no"}], "isError": True,
        })

    @pytest.mark.asyncio
    async def test_a_tool_only_the_model_may_call_is_403(self, api):
        r = await api.http.post(CALL, json=_call(tool="model_only"))
        assert (r.status_code, r.json()) == (403, {"detail": "tool_not_app_visible"})
        assert api.sdk.sent("mcp_call") == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("body", [
        _call(server="nerve", tool="task_list"),
        _call(server="nerve", tool="bump"),
    ])
    async def test_nerves_own_tools_are_always_refused(self, api, body):
        r = await api.http.post(CALL, json=body)
        assert (r.status_code, r.json()) == (404, {"detail": "server_not_found"})
        assert api.sdk.requests == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("body, detail", [
        (_call(server="nope"), "server_not_found"),
        (_call(tool="nope"), "tool_not_found"),
        (_call(tool="mcp__my_server__bump"), "tool_not_found"),
    ])
    async def test_an_unknown_server_or_tool_is_404(self, api, body, detail):
        r = await api.http.post(CALL, json=body)
        assert (r.status_code, r.json()) == (404, {"detail": detail})
        assert api.sdk.sent("mcp_call") == []

    @pytest.mark.asyncio
    async def test_an_unknown_session_is_404(self, api):
        r = await api.http.post("/api/sessions/nope/mcp-apps/tool-call", json=_call())
        assert (r.status_code, r.json()) == (404, {"detail": "session_not_found"})

    @pytest.mark.asyncio
    @pytest.mark.parametrize("content, detail", [
        (b"", "bad_json"),
        (b"{not json", "bad_json"),
        (b"[1, 2]", "bad_json"),
        (b'"text"', "bad_json"),
        (json.dumps({"tool": "bump"}).encode(), "bad_server"),
        (json.dumps({"server": 3, "tool": "bump"}).encode(), "bad_server"),
        (json.dumps({"server": SERVER}).encode(), "bad_tool"),
        (json.dumps({"server": SERVER, "tool": ""}).encode(), "bad_tool"),
        (json.dumps({"server": SERVER, "tool": ["bump"]}).encode(), "bad_tool"),
        (json.dumps({"server": SERVER, "tool": "bump", "arguments": [1]}).encode(), "bad_arguments"),
        (json.dumps({"server": SERVER, "tool": "bump", "arguments": "x"}).encode(), "bad_arguments"),
        (json.dumps(_call(arguments={"blob": "x" * (1024 * 1024)})).encode(), "body_too_large"),
    ])
    async def test_a_malformed_request_is_400(self, api, content, detail):
        r = await api.http.post(CALL, content=content, headers={"Content-Type": "application/json"})
        assert (r.status_code, r.json()) == (400, {"detail": detail})
        assert api.sdk.requests == []

    @pytest.mark.asyncio
    async def test_no_live_runtime_is_409(self, api):
        api.engine.sessions.remove_client(SID)
        r = await api.http.post(CALL, json=_call())
        assert (r.status_code, r.json()) == (409, {"detail": "session_not_running"})

    @pytest.mark.asyncio
    async def test_a_server_that_is_not_connected_is_502(self, api):
        r = await api.http.post(CALL, json=_call(server="starting"))
        assert r.status_code == 502
        assert "not connected" in r.json()["detail"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("answer, status, detail", [
        (Exception("MCP server not connected: my_server"), 502, "MCP server not connected: my_server"),
        (CLIConnectionError("Not connected"), 502, "Not connected"),
        ({"staging": {"ok": False, "error_code": "expired", "detail": "too late"}}, 502, "too late"),
        ({"staging": {"ok": False, "error_code": "timeout", "detail": "tool call exceeded 60000ms"}},
         504, "tool call exceeded 60000ms"),
        (_timeout("mcp_call"), 504, "Control request timeout: mcp_call"),
    ])
    async def test_a_call_that_fails_or_runs_too_long(self, api, answer, status, detail):
        api.sdk.answers["mcp_call"] = answer
        r = await api.http.post(CALL, json=_call())
        assert (r.status_code, r.json()) == (status, {"detail": detail})

    @pytest.mark.asyncio
    async def test_a_result_over_the_limit_loses_its_structured_content(self, api):
        api.sdk.answers["mcp_call"] = {
            "content": [{"type": "text", "text": "summary"}],
            "structuredContent": {"rows": ["x" * 1024] * 3000},
        }
        r = await api.http.post(CALL, json=_call())
        assert (r.status_code, r.json()) == (200, {
            "content": [{"type": "text", "text": "summary"}],
            "_meta": {"nerve/truncated": True},
        })

    @pytest.mark.asyncio
    async def test_calls_do_not_wait_for_each_other(self, api):
        """A view may call while another call runs: one slow tool must not
        hold the rest."""
        release = asyncio.Event()
        started: list[str] = []

        class _Query:
            async def _send_control_request(self, request, timeout=60.0):
                if request["subtype"] == "mcp_status":
                    return STATUS
                started.append(request["arguments"]["id"])
                if request["arguments"]["id"] == "slow":
                    await release.wait()
                return {"content": request["arguments"]["id"]}

        api.sdk._query = _Query()
        slow = asyncio.create_task(api.http.post(CALL, json=_call(arguments={"id": "slow"})))
        while "slow" not in started:
            await asyncio.sleep(0.01)
        fast = await api.http.post(CALL, json=_call(arguments={"id": "fast"}))
        assert fast.json() == {"content": [{"type": "text", "text": "fast"}]}
        assert not slow.done()
        release.set()
        assert (await slow).json() == {"content": [{"type": "text", "text": "slow"}]}
