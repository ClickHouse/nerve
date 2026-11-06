"""The catalog reader of the MCP gateway (nerve.mcp_gateway)."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest

from nerve import mcp_gateway
from nerve.mcp_gateway import (
    CatalogError,
    GatewayCatalog,
    GatewayServer,
    McpGatewayCatalog,
    parse_catalog,
)

BASE = "http://192.0.2.1:8080"
CATALOG_URL = BASE + "/catalog"


def _server(server_id: str, **extra) -> dict:
    return {
        "id": server_id,
        "displayName": server_id.title(),
        "description": f"The {server_id} server.",
        "url": f"/s/{server_id}/mcp",
        "revision": f"sha256:{server_id}",
        **extra,
    }


def _document(*server_ids: str, revision: str = "sha256:catalog") -> dict:
    return {"revision": revision, "servers": [_server(s) for s in server_ids]}


class TestParse:
    def test_a_document_with_two_servers(self):
        catalog = parse_catalog(_document("docs", "issues"), CATALOG_URL)
        assert catalog == GatewayCatalog(
            revision="sha256:catalog",
            servers=(
                GatewayServer(
                    id="docs", url=f"{BASE}/s/docs/mcp", revision="sha256:docs",
                ),
                GatewayServer(
                    id="issues", url=f"{BASE}/s/issues/mcp", revision="sha256:issues",
                ),
            ),
        )

    def test_other_members_are_ignored(self):
        document = _document("docs", "issues")
        document.update(schemaVersion=2, generation=8)
        document["servers"][0].update(tools=["search"], displayName=None)
        del document["servers"][1]["description"]
        assert parse_catalog(document, CATALOG_URL) == parse_catalog(
            _document("docs", "issues"), CATALOG_URL,
        )

    def test_no_servers(self):
        assert parse_catalog(_document(), CATALOG_URL).servers == ()

    @pytest.mark.parametrize(("catalog_url", "reference", "expected"), [
        # An absolute path replaces the whole path of the catalog URL.
        ("http://gw.example/base/catalog", "/s/docs/mcp", "http://gw.example/s/docs/mcp"),
        # A relative path resolves next to the catalog path.
        ("http://gw.example/base/catalog", "s/docs/mcp", "http://gw.example/base/s/docs/mcp"),
        # An absolute URL of the same scheme, host and port is accepted.
        (CATALOG_URL, f"{BASE}/other/mcp", f"{BASE}/other/mcp"),
    ])
    def test_url_resolves_against_the_catalog_url(self, catalog_url, reference, expected):
        document = _document("docs")
        document["servers"][0]["url"] = reference
        assert parse_catalog(document, catalog_url).servers[0].url == expected

    @pytest.mark.parametrize("change", [
        lambda d: [d],
        lambda d: d.pop("revision"),
        lambda d: d.pop("servers"),
        lambda d: d.update(revision=""),
        lambda d: d.update(revision=7),
        lambda d: d.update(servers=None),
        lambda d: d["servers"].append("docs"),
        lambda d: d["servers"][0].pop("id"),
        lambda d: d["servers"][0].pop("url"),
        lambda d: d["servers"][0].pop("revision"),
        lambda d: d["servers"][0].update(id="Docs"),
        lambda d: d["servers"][0].update(id="docs-"),
        lambda d: d["servers"][0].update(id="a" * 33),
        lambda d: d["servers"][0].update(id="docs.url"),
        lambda d: d["servers"][0].update(id=1),
        lambda d: d["servers"].reverse(),
        lambda d: d["servers"].append(_server("issues")),
        lambda d: d["servers"][0].update(url=""),
        lambda d: d["servers"][0].update(url=None),
        lambda d: d["servers"][0].update(url="https://192.0.2.1:8080/s/docs/mcp"),
        lambda d: d["servers"][0].update(url="http://192.0.2.2:8080/s/docs/mcp"),
        lambda d: d["servers"][0].update(url="//192.0.2.2:8080/s/docs/mcp"),
        lambda d: d["servers"][0].update(url="http://192.0.2.1:8081/s/docs/mcp"),
        lambda d: d["servers"][0].update(url="http://user@192.0.2.1:8080/s/docs/mcp"),
        lambda d: d["servers"][0].update(revision=""),
        lambda d: d["servers"][0].update(revision=None),
    ])
    def test_a_document_that_is_not_valid_is_refused(self, change):
        document = _document("docs", "issues")
        result = change(document)
        payload = result if isinstance(result, list) else document
        with pytest.raises(CatalogError):
            parse_catalog(payload, CATALOG_URL)

    def test_the_message_never_holds_the_document(self):
        document = _document("docs")
        document["servers"][0]["url"] = "http://203.0.113.9/secret-path"
        with pytest.raises(CatalogError) as caught:
            parse_catalog(document, CATALOG_URL)
        assert "secret-path" not in str(caught.value)
        assert "servers[0].url" in str(caught.value)


class FakeGateway:
    """``GET /catalog`` of an MCP gateway, as an httpx transport."""

    def __init__(self, document: dict | None = None):
        self.document = document if document is not None else _document("docs")
        self.status = 200
        self.body: bytes | None = None
        self.error: Exception | None = None
        self.delay = 0.0
        self.requests: list[httpx.Request] = []

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.error is not None:
            raise self.error
        body = self.body if self.body is not None else json.dumps(self.document).encode()
        return httpx.Response(
            self.status, content=body, headers={"Content-Type": "application/json"},
        )

    def reader(self, base: str = BASE) -> McpGatewayCatalog:
        return McpGatewayCatalog(base, transport=httpx.MockTransport(self))


class TestReader:
    @pytest.mark.asyncio
    async def test_reads_the_catalog_without_a_credential(self):
        gateway = FakeGateway()
        reader = gateway.reader(BASE + "/")
        try:
            catalog = await reader.refresh()
        finally:
            await reader.close()
        assert catalog is reader.latest
        assert [s.url for s in catalog.servers] == [f"{BASE}/s/docs/mcp"]
        [request] = gateway.requests
        assert (request.method, str(request.url)) == ("GET", CATALOG_URL)
        assert request.headers["accept"] == "application/json"
        assert "authorization" not in request.headers
        assert "cookie" not in request.headers

    @pytest.mark.asyncio
    async def test_the_proxy_environment_is_not_used(self, monkeypatch):
        """A real loopback server: httpx reads no proxy for a custom transport."""
        body = json.dumps(_document("docs")).encode()

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):  # noqa: N802 - http.server API
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        for name in ("HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"):
            monkeypatch.setenv(name, "http://127.0.0.1:9")
        for name in ("NO_PROXY", "no_proxy"):
            monkeypatch.delenv(name, raising=False)
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        reader = McpGatewayCatalog(f"http://127.0.0.1:{server.server_port}")
        try:
            catalog = await reader.refresh()
        finally:
            await reader.close()
            server.shutdown()
            server.server_close()
        assert catalog is not None
        assert catalog.servers[0].url == f"http://127.0.0.1:{server.server_port}/s/docs/mcp"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("fail", [
        lambda g: setattr(g, "status", 503),
        lambda g: setattr(g, "status", 302),
        lambda g: setattr(g, "body", b"<html>"),
        lambda g: g.document.update(servers=None),
        lambda g: setattr(g, "error", httpx.ConnectError("refused")),
    ])
    async def test_a_failure_keeps_the_last_catalog(self, fail):
        gateway = FakeGateway()
        reader = gateway.reader()
        try:
            first = await reader.refresh()
            gateway.document = _document("issues", revision="sha256:next")
            fail(gateway)
            assert await reader.refresh() is first
            assert reader.latest is first
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_no_catalog_before_the_first_success(self):
        gateway = FakeGateway()
        gateway.status = 503
        reader = gateway.reader()
        try:
            assert await reader.refresh() is None
            gateway.status = 200
            assert (await reader.refresh()).revision == "sha256:catalog"
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_a_slow_gateway_is_a_failure(self, monkeypatch):
        monkeypatch.setattr(mcp_gateway, "FETCH_TIMEOUT_SECONDS", 0.05)
        gateway = FakeGateway()
        gateway.delay = 5
        reader = gateway.reader()
        try:
            assert await reader.refresh() is None
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_one_warning_for_each_new_problem(self, caplog):
        gateway = FakeGateway()
        gateway.status = 503
        reader = gateway.reader()
        caplog.set_level(logging.DEBUG, logger="nerve.mcp_gateway")
        try:
            await reader.refresh()
            await reader.refresh()
            gateway.status = 429
            await reader.refresh()
        finally:
            await reader.close()
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings == [
            "MCP gateway catalog not read: MCP gateway answered HTTP 503",
            "MCP gateway catalog not read: MCP gateway answered HTTP 429",
        ]

    @pytest.mark.asyncio
    async def test_a_call_that_waits_uses_the_read_in_progress(self):
        gateway = FakeGateway()
        gateway.delay = 0.01
        reader = gateway.reader()
        try:
            results = await asyncio.gather(*(reader.refresh() for _ in range(3)))
            assert len(gateway.requests) == 1
            assert results[0] is not None
            assert all(result is results[0] for result in results)
            # A call after the read reads again.
            await reader.refresh()
            assert len(gateway.requests) == 2
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_callers_wait_for_one_slow_read_only(self, monkeypatch):
        monkeypatch.setattr(mcp_gateway, "FETCH_TIMEOUT_SECONDS", 0.05)
        gateway = FakeGateway()
        gateway.delay = 5
        reader = gateway.reader()
        try:
            results = await asyncio.gather(*(reader.refresh() for _ in range(3)))
        finally:
            await reader.close()
        assert results == [None, None, None]
        assert len(gateway.requests) == 1
