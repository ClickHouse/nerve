"""The MCP gateway catalog: contract checks and the catalog reader.

The reader talks to a fake gateway, a real HTTP server on loopback, so these
tests cover the request, the error statuses, an unreachable gateway and the
background retry.
"""

from __future__ import annotations

import asyncio
import copy
import json
import threading

import pytest

from nerve.mcp_gateway import (
    MAX_CATALOG_BYTES,
    CatalogError,
    GatewayServer,
    McpGatewayCatalog,
    parse_catalog,
)
from tests.fake_mcp_gateway import FakeMcpGateway, catalog_payload

BASE = "http://192.0.2.1:8080"

# The example of the catalog contract.
EXAMPLE = {
    "schemaVersion": 1,
    "generation": 7,
    "digest": "sha256:3f9a0c1d2b4e5f60718293a4b5c6d7e8f9a0b1c2d3e4f5061728394a5b6c7d8e",
    "servers": [
        {
            "id": "docs",
            "displayName": "Docs",
            "description": "Search the product documentation.",
            "path": "/s/docs/mcp",
            "tools": ["fetch_page", "search"],
        },
        {
            "id": "github",
            "displayName": "GitHub",
            "description": "",
            "path": "/s/github/mcp",
            "tools": ["create_issue", "get_issue", "list_issues"],
        },
    ],
}


@pytest.fixture
def gateway():
    fake = FakeMcpGateway().start()
    yield fake
    fake.stop()


async def _until(predicate, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


class TestGatewayUrlSetting:
    @pytest.mark.parametrize("value, expected", [
        (None, ""),
        ("", ""),
        ("  ", ""),
        ("http://192.0.2.1:8080", "http://192.0.2.1:8080"),
        ("http://192.0.2.1:8080/", "http://192.0.2.1:8080"),
        (" https://mcp.example.com/agent/ ", "https://mcp.example.com/agent"),
        ("http://[::1]:8080", "http://[::1]:8080"),
        ("http://gateway.example.:8080", "http://gateway.example.:8080"),
        ("http://bücher.example", "http://bücher.example"),
    ])
    def test_accepted_values(self, value, expected):
        from nerve.config import NerveConfig

        assert NerveConfig.from_dict({"mcp_gateway_url": value}).mcp_gateway_url == expected

    def test_default_is_empty(self):
        from nerve.config import NerveConfig

        assert NerveConfig.from_dict({}).mcp_gateway_url == ""

    @pytest.mark.parametrize("value", [
        "192.0.2.1:8080",
        "ftp://192.0.2.1",
        "http://",
        "http://user:secret@192.0.2.1:8080",
        "http://192.0.2.1:8080?agent=1",
        "http://192.0.2.1:8080#x",
        "http://192.0.2.1:8080?",
        "http://192.0.2.1:99999",
        "http://192.0.2.1:0",
        # Values that the HTTP client cannot use, or that would corrupt a log
        # line or the doctor report.
        "http://192.0.2.1:8080/a\nb",
        "http://192.0.2.1:8080/a b",
        "http://192.0.2.1:8080/a\x00b",
        "http://exa mple.example",
        "http://ex\tample.example",
        "http://\x7fexample.example",
        "http://[::1",
        "http://-bad-.example",
        "http://a..example",
        "http://.example",
        "http://%zz.example",
    ])
    def test_refused_values(self, value):
        from nerve.config import ConfigError, NerveConfig

        with pytest.raises(ConfigError, match="mcp_gateway_url"):
            NerveConfig.from_dict({"mcp_gateway_url": value})

    def test_load_config_reads_the_local_file(self, tmp_path):
        from nerve.config import load_config

        (tmp_path / "config.yaml").write_text(
            f"workspace: {tmp_path / 'ws'}\n", encoding="utf-8",
        )
        (tmp_path / "config.local.yaml").write_text(
            'mcp_gateway_url: "http://192.0.2.1:8080"\n', encoding="utf-8",
        )
        assert load_config(tmp_path).mcp_gateway_url == "http://192.0.2.1:8080"

    def test_a_change_needs_a_restart(self):
        from nerve.config import NerveConfig
        from nerve.config_reload import restart_required

        before = NerveConfig.from_dict({})
        after = NerveConfig.from_dict({"mcp_gateway_url": "http://192.0.2.1:8080"})
        assert restart_required(before, after) == [
            "mcp_gateway_url: '' → 'http://192.0.2.1:8080'",
        ]


class TestParseCatalog:
    def test_contract_example(self):
        catalog = parse_catalog(EXAMPLE, BASE)
        assert catalog.generation == 7
        assert catalog.digest == EXAMPLE["digest"]
        assert catalog.servers == (
            GatewayServer(
                id="docs", url=f"{BASE}/s/docs/mcp",
                tools=("fetch_page", "search"), display_name="Docs",
                description="Search the product documentation.",
            ),
            GatewayServer(
                id="github", url=f"{BASE}/s/github/mcp",
                tools=("create_issue", "get_issue", "list_issues"),
                display_name="GitHub", description="",
            ),
        )

    def test_empty_catalog_of_a_tenant_without_one(self):
        catalog = parse_catalog(
            {"schemaVersion": 1, "generation": 0, "digest": EXAMPLE["digest"],
             "servers": []},
            BASE,
        )
        assert catalog.generation == 0
        assert catalog.servers == ()

    def test_unknown_fields_and_missing_optional_text_are_accepted(self):
        payload = copy.deepcopy(EXAMPLE)
        payload["future"] = {"x": 1}
        payload["servers"][0]["future"] = True
        del payload["servers"][1]["displayName"]
        del payload["servers"][1]["description"]
        catalog = parse_catalog(payload, BASE)
        assert catalog.servers[1].display_name == ""
        assert catalog.servers[1].description == ""

    @pytest.mark.parametrize("change, problem", [
        (lambda c: c.update(schemaVersion=2), "schemaVersion"),
        (lambda c: c.update(schemaVersion=True), "schemaVersion"),
        (lambda c: c.pop("schemaVersion"), "schemaVersion"),
        (lambda c: c.update(generation=-1), "generation"),
        (lambda c: c.update(generation=True), "generation"),
        (lambda c: c.update(generation="7"), "generation"),
        (lambda c: c.update(digest="sha256:ABC"), "digest"),
        (lambda c: c.update(digest=None), "digest"),
        (lambda c: c.update(servers=None), "servers"),
        (lambda c: c.update(servers={}), "servers"),
        (lambda c: c["servers"].reverse(), "order"),
        (lambda c: c["servers"].append(copy.deepcopy(c["servers"][1])), "order"),
        (lambda c: c["servers"][0].update(id="Docs"), "id"),
        (lambda c: c["servers"][0].update(id="my_docs", path="/s/my_docs/mcp"), "id"),
        (lambda c: c["servers"][0].update(id="docs-"), "id"),
        (lambda c: c["servers"][0].update(id="a" * 33), "id"),
        (lambda c: c["servers"][0].update(path="/s/other/mcp"), "path"),
        (lambda c: c["servers"][0].update(path="https://upstream.example/mcp"), "path"),
        (lambda c: c["servers"][0].update(tools=[]), "tools"),
        (lambda c: c["servers"][0].update(tools=None), "tools"),
        (lambda c: c["servers"][0].update(tools=["search", "fetch_page"]), "tools"),
        (lambda c: c["servers"][0].update(tools=["search", "search"]), "tools"),
        (lambda c: c["servers"][0].update(tools=["a b"]), "tools"),
        (lambda c: c["servers"][0].update(tools=["x" * 129]), "tools"),
        (lambda c: c["servers"][0].update(displayName=3), "displayName"),
        (lambda c: c["servers"].append("docs"), r"servers\[2\] is not"),
    ])
    def test_contract_violations_are_refused(self, change, problem):
        payload = copy.deepcopy(EXAMPLE)
        change(payload)
        with pytest.raises(CatalogError, match=problem):
            parse_catalog(payload, BASE)

    def test_not_an_object(self):
        with pytest.raises(CatalogError, match="object"):
            parse_catalog([EXAMPLE], BASE)


class TestCatalogReader:
    @pytest.mark.asyncio
    async def test_start_applies_the_catalog(self, gateway):
        applied = []

        async def on_applied(catalog):
            applied.append(catalog)

        reader = McpGatewayCatalog(gateway.url + "/", on_applied=on_applied)
        try:
            await reader.start()
            assert [server.id for server in reader.servers] == ["docs", "github"]
            assert reader.servers[0].url == f"{gateway.url}/s/docs/mcp"
            assert [catalog.generation for catalog in applied] == [7]
            status = reader.status()
            assert status["url"] == gateway.url
            assert status["generation"] == 7
            assert status["servers"] == ["docs", "github"]
            assert status["error"] is None
            assert status["retrying"] is False
            assert status["applied_at"] and status["checked_at"]
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_a_new_generation_replaces_the_catalog(self, gateway):
        applied = []

        async def on_applied(catalog):
            applied.append(catalog.generation)

        reader = McpGatewayCatalog(gateway.url, on_applied=on_applied)
        try:
            await reader.start()
            first = reader.applied
            await reader.refresh()
            assert reader.applied is first
            assert applied == [7]

            gateway.catalog = catalog_payload(8, {"docs": ["search"]})
            await reader.refresh()
            assert reader.applied.generation == 8
            assert [server.id for server in reader.servers] == ["docs"]
            assert reader.servers[0].tools == ("search",)
            assert applied == [7, 8]
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_unreachable_at_start_retries_in_the_background(self, gateway):
        gateway.stop()
        reader = McpGatewayCatalog(gateway.url, retry_initial=0.05, retry_max=0.05)
        try:
            await reader.start()
            assert reader.applied is None
            assert reader.servers == ()
            status = reader.status()
            assert status["generation"] is None
            assert "cannot reach the MCP gateway" in status["error"]
            assert status["retrying"] is True

            gateway.start()
            await _until(lambda: reader.applied is not None)
            assert reader.applied.generation == 7
            await _until(lambda: not reader.retrying)
            assert reader.status()["error"] is None
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_an_outage_keeps_the_applied_catalog(self, gateway):
        reader = McpGatewayCatalog(gateway.url, retry_initial=30)
        try:
            await reader.start()
            gateway.stop()
            await reader.refresh()
            assert reader.applied.generation == 7
            assert [server.id for server in reader.servers] == ["docs", "github"]
            assert reader.retrying is True
            assert "cannot reach" in reader.status()["error"]

            # A new session reads the catalog again, also while the retry
            # waits, and a success ends the retry.
            gateway.start()
            gateway.catalog = catalog_payload(9, {"docs": ["search"]})
            await reader.refresh()
            assert reader.applied.generation == 9
            assert reader.retrying is False
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_retry_interval_doubles_up_to_the_limit(self, gateway):
        gateway.stop()
        delays: list[float] = []
        enough = asyncio.Event()

        async def sleep(delay):
            delays.append(delay)
            if len(delays) >= 9:
                enough.set()
                await asyncio.Event().wait()
            await asyncio.sleep(0)

        reader = McpGatewayCatalog(gateway.url, sleep=sleep)
        try:
            await reader.start()
            await asyncio.wait_for(enough.wait(), 10)
            assert delays == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0, 60.0]
        finally:
            await reader.close()
        assert reader.retrying is False

    @pytest.mark.asyncio
    async def test_unavailable_catalog_reports_the_reason(self, gateway):
        gateway.catalog_status = 503
        reader = McpGatewayCatalog(gateway.url, retry_initial=30)
        try:
            await reader.start()
            assert reader.applied is None
            assert reader.status()["error"] == (
                "MCP gateway answered HTTP 503 (catalog_unavailable, "
                "request 0192f000-0000-7000-8000-000000000001)"
            )
        finally:
            await reader.close()

    @pytest.mark.parametrize("setup, problem", [
        (lambda g: setattr(g, "catalog", {**g.catalog, "schemaVersion": 2}),
         "schemaVersion"),
        (lambda g: setattr(g, "catalog_body", b"{not json"), "not valid JSON"),
        (lambda g: setattr(g, "content_type", "text/html"), "not application/json"),
        (lambda g: setattr(g, "catalog_body", b" " * (MAX_CATALOG_BYTES + 1)),
         "larger than"),
    ])
    @pytest.mark.asyncio
    async def test_a_bad_catalog_keeps_the_applied_one(self, gateway, setup, problem):
        reader = McpGatewayCatalog(gateway.url, retry_initial=30)
        try:
            await reader.start()
            setup(gateway)
            await reader.refresh()
            assert reader.applied.generation == 7
            assert problem in reader.status()["error"]
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_the_error_holds_no_response_text(self, gateway):
        gateway.catalog_body = json.dumps(
            {**gateway.catalog, "schemaVersion": "secret-value-in-body"},
        ).encode()
        reader = McpGatewayCatalog(gateway.url, retry_initial=30)
        try:
            await reader.start()
            assert "secret-value-in-body" not in reader.status()["error"]
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_a_slow_catalog_counts_as_unavailable(self, gateway):
        """A body that arrives byte by byte stays under each read timeout.

        The whole request has a deadline, so startup and a new session do not
        wait for it without limit. The applied catalog stays and the retry
        runs.
        """
        gateway.trickle = 0.05
        reader = McpGatewayCatalog(gateway.url, retry_initial=30, deadline=0.5)
        loop = asyncio.get_running_loop()
        try:
            started = loop.time()
            await asyncio.wait_for(reader.start(), 5)
            assert loop.time() - started < 2
            assert reader.applied is None
            assert reader.status()["error"] == (
                "MCP gateway catalog not received within 0.5 seconds"
            )
            assert reader.retrying is True

            gateway.trickle = None
            await reader.refresh()
            assert reader.applied.generation == 7

            gateway.catalog = catalog_payload(8, {"docs": ["search"]})
            gateway.trickle = 0.05
            started = loop.time()
            await asyncio.wait_for(reader.refresh(), 5)
            assert loop.time() - started < 2
            assert reader.applied.generation == 7
            assert reader.retrying is True
        finally:
            await reader.close()

    def test_the_default_deadline_is_bounded(self):
        from nerve.mcp_gateway import FETCH_DEADLINE_SECONDS

        assert 0 < FETCH_DEADLINE_SECONDS <= 10
        assert McpGatewayCatalog("http://192.0.2.1:8080")._deadline == FETCH_DEADLINE_SECONDS

    @pytest.mark.asyncio
    async def test_a_failed_handler_runs_again_on_the_next_request(self, gateway):
        calls: list[int] = []

        async def on_applied(catalog):
            calls.append(catalog.generation)
            if len(calls) == 1:
                raise RuntimeError("database is locked")

        reader = McpGatewayCatalog(gateway.url, on_applied=on_applied)
        try:
            await reader.start()
            assert calls == [7]
            # The same catalog: the handler did not complete, so it runs again.
            await reader.refresh()
            assert calls == [7, 7]
            # Now it completed; the same catalog does not call it again.
            await reader.refresh()
            assert calls == [7, 7]
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_concurrent_refreshes_share_one_request(self, gateway):
        reader = McpGatewayCatalog(gateway.url)
        try:
            gateway.catalog_gate = threading.Event()
            calls = [asyncio.ensure_future(reader.refresh()) for _ in range(5)]
            await _until(lambda: gateway.catalog_requests == 1)
            await asyncio.sleep(0.05)
            gateway.catalog_gate.set()
            results = await asyncio.gather(*calls)
            assert gateway.catalog_requests == 1
            assert {catalog.generation for catalog in results} == {7}
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_proxy_environment_is_not_used(self, gateway, monkeypatch):
        # A proxy that does not exist: a request through it would fail.
        for name in ("HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"):
            monkeypatch.setenv(name, "http://127.0.0.1:9")
        monkeypatch.delenv("NO_PROXY", raising=False)
        monkeypatch.delenv("no_proxy", raising=False)
        reader = McpGatewayCatalog(gateway.url)
        try:
            await reader.start()
            assert reader.applied is not None
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_close_stops_the_retry(self, gateway):
        gateway.stop()
        reader = McpGatewayCatalog(gateway.url, retry_initial=0.01, retry_max=0.01)
        await reader.start()
        assert reader.retrying is True
        await reader.close()
        assert reader.retrying is False
        assert await reader.refresh() is None
