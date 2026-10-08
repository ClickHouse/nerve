"""The MCP gateway catalog: contract checks and the catalog reader.

The contract checks run against the shared fixtures of the gateway's catalog
contract in ``tests/fixtures/mcp_gateway`` (see the README there). The reader
talks to a fake gateway, a real HTTP server on loopback, so these tests cover
the request, the error statuses, an unreachable gateway and the background
retry.
"""

from __future__ import annotations

import asyncio
import copy
import json
import threading
from pathlib import Path

import pytest
import pytest_asyncio

from nerve.mcp_gateway import (
    MAX_CATALOG_BYTES,
    CatalogError,
    GatewayHealth,
    GatewayInventory,
    GatewayServer,
    McpGatewayCatalog,
    desired_revision,
    follow_inventory,
    parse_catalog,
)
from tests.fake_mcp_gateway import FakeMcpGateway, catalog_payload

BASE = "http://192.0.2.1:8080"

# The shared fixtures of the catalog contract, byte for byte.
FIXTURES = Path(__file__).parent / "fixtures" / "mcp_gateway"
CATALOG_FILES = (
    "catalog.json",
    "catalog-all-tools.json",
    "catalog-empty-inventory.json",
    "catalog-no-servers.json",
    "catalog-no-tools-yet.json",
    "catalog-older-desired-revision.json",
    "catalog-review-changes.json",
    "catalog-stale-after-failure.json",
    "catalog-unknown-inventory.json",
)


def _fixture(name: str):
    return json.loads((FIXTURES / name).read_bytes())


VECTORS = _fixture("catalog-vectors.json")
EXAMPLE = _fixture("catalog.json")


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
        assert catalog.desired_revision == EXAMPLE["desiredRevision"]
        docs, github = EXAMPLE["servers"]
        assert catalog.servers == (
            GatewayServer(
                id="docs", url=f"{BASE}/s/docs/mcp",
                tools=("fetch_page", "search"), display_name="Docs",
                description="Search the product documentation.",
                tool_mode="selected",
                desired_revision=docs["desiredRevision"],
                inventory=GatewayInventory(
                    revision=docs["inventory"]["revision"], tool_count=2,
                    observed_at="2026-10-17T12:00:00Z",
                    desired_revision=docs["desiredRevision"],
                ),
                health=GatewayHealth(
                    inventory_state="current", checked_at="2026-10-17T12:00:00Z",
                ),
            ),
            GatewayServer(
                id="github", url=f"{BASE}/s/github/mcp",
                tools=("create_issue", "get_issue", "list_issues"),
                display_name="GitHub", description="",
                tool_mode="selected",
                desired_revision=github["desiredRevision"],
                inventory=GatewayInventory(
                    revision=github["inventory"]["revision"], tool_count=3,
                    observed_at="2026-10-17T12:00:00Z",
                    desired_revision=github["desiredRevision"],
                ),
                health=GatewayHealth(
                    inventory_state="current", checked_at="2026-10-17T12:00:00Z",
                ),
            ),
        )

    @pytest.mark.parametrize("name", CATALOG_FILES)
    def test_every_contract_fixture_is_accepted(self, name):
        payload = _fixture(name)
        catalog = parse_catalog(payload, BASE)
        # Every desired server stays, whatever the gateway observed of it.
        assert [server.id for server in catalog.servers] == [
            server["id"] for server in payload["servers"]
        ]
        assert catalog.desired_revision == payload["desiredRevision"]

    def test_unknown_inventory_is_not_an_empty_list(self):
        catalog = parse_catalog(_fixture("catalog-unknown-inventory.json"), BASE)
        docs, github = catalog.servers
        assert docs.inventory is None
        assert docs.health == GatewayHealth("unknown", None, None)
        assert github.inventory is None
        assert github.health == GatewayHealth(
            "unknown", "2026-10-17T12:00:00Z", "credential_rejected",
        )

        empty = parse_catalog(_fixture("catalog-empty-inventory.json"), BASE).servers[0]
        assert empty.id == "docs"
        assert empty.inventory is not None and empty.inventory.tool_count == 0
        assert empty.health.inventory_state == "current"

    def test_a_failed_read_keeps_the_earlier_inventory(self):
        known = parse_catalog(EXAMPLE, BASE).servers[1]
        stale = parse_catalog(_fixture("catalog-stale-after-failure.json"), BASE).servers[1]
        assert stale.inventory == known.inventory
        assert stale.health == GatewayHealth(
            "stale", "2026-10-17T12:05:00Z", "upstream_timeout",
        )

    def test_an_observation_of_an_older_policy(self):
        catalog = parse_catalog(_fixture("catalog-older-desired-revision.json"), BASE)
        docs = catalog.servers[0]
        assert docs.tools == ("fetch_page", "search", "summarize")
        assert docs.inventory.desired_revision != docs.desired_revision
        assert docs.health.inventory_state == "stale"
        assert catalog.desired_revision != EXAMPLE["desiredRevision"]

    def test_a_server_without_a_tool_yet(self):
        tracker = parse_catalog(_fixture("catalog-no-tools-yet.json"), BASE).servers[1]
        assert (tracker.id, tracker.tools, tracker.inventory) == ("tracker", (), None)

    def test_an_agent_without_a_server(self):
        catalog = parse_catalog(_fixture("catalog-no-servers.json"), BASE)
        assert catalog.servers == ()
        assert catalog.desired_revision == desired_revision([])

    def test_a_server_that_gives_every_tool_names_none(self):
        docs, wiki = parse_catalog(_fixture("catalog-all-tools.json"), BASE).servers
        assert docs.tool_mode == "selected"
        assert (wiki.id, wiki.tool_mode, wiki.tools) == ("wiki", "all", None)
        assert wiki.url == f"{BASE}/s/wiki/mcp"
        # The inventory counts the tools of the last list, not a limit.
        assert wiki.inventory.tool_count == 2
        assert wiki.health.inventory_state == "current"

    def test_the_tool_change_behavior(self):
        docs, github = parse_catalog(_fixture("catalog-review-changes.json"), BASE).servers
        assert (docs.tool_changes, docs.tools) == ("review", ("fetch_page", "search"))
        # An absent value means accept.
        assert github.tool_changes == "accept"
        example = parse_catalog(EXAMPLE, BASE).servers[0]
        assert example.tool_changes == "accept"
        # The approved definitions are inputs of the desired revision only.
        assert docs.desired_revision != example.desired_revision
        assert docs.inventory.revision == example.inventory.revision

    @pytest.mark.parametrize(
        "vector", VECTORS["accept"], ids=[v["name"] for v in VECTORS["accept"]],
    )
    def test_contract_vectors_that_a_consumer_accepts(self, vector):
        catalog = parse_catalog(vector["catalog"], BASE)
        assert catalog.servers

    @pytest.mark.parametrize(
        "vector", VECTORS["reject"], ids=[v["name"] for v in VECTORS["reject"]],
    )
    def test_contract_vectors_that_a_consumer_rejects(self, vector):
        with pytest.raises(CatalogError) as refused:
            parse_catalog(vector["catalog"], BASE)
        # ``field`` names the member as the contract's validation names it.
        if "field" in vector:
            assert f"catalog {vector['field']} " in str(refused.value)

    def test_an_unknown_tool_mode_keeps_the_server_without_tool_names(self):
        vector = next(v for v in VECTORS["accept"] if v["name"] == "unknown tool mode with tools")
        mode = vector["catalog"]["servers"][0]["toolPolicy"]["mode"]
        server = parse_catalog(vector["catalog"], BASE).servers[0]
        assert mode != "selected"
        assert (server.tool_mode, server.tools) == (mode, None)
        assert server.url == f"{BASE}/s/{server.id}/mcp"

    def test_an_unknown_tool_change_behavior_keeps_the_server(self):
        vector = next(
            v for v in VECTORS["accept"] if v["name"] == "unknown tool change behavior"
        )
        changes = vector["catalog"]["servers"][0]["toolPolicy"]["changes"]
        server = parse_catalog(vector["catalog"], BASE).servers[0]
        assert changes not in ("accept", "review")
        assert (server.tool_changes, server.tools) == (changes, ("fetch_page", "search"))

    @pytest.mark.parametrize("changes", [None, ""])
    def test_a_null_or_empty_tool_change_behavior_is_accept(self, changes):
        payload = copy.deepcopy(EXAMPLE)
        payload["servers"][0]["toolPolicy"]["changes"] = changes
        assert parse_catalog(payload, BASE).servers[0].tool_changes == "accept"

    def test_unknown_fields_and_missing_optional_text_are_accepted(self):
        payload = copy.deepcopy(EXAMPLE)
        payload["future"] = {"x": 1}
        payload["servers"][0]["future"] = True
        del payload["servers"][1]["displayName"]
        payload["servers"][1]["description"] = None
        catalog = parse_catalog(payload, BASE)
        assert catalog.servers[1].display_name == ""
        assert catalog.servers[1].description == ""

    def test_a_lone_surrogate_in_display_text_is_replaced(self):
        payload = copy.deepcopy(EXAMPLE)
        payload["servers"][0]["displayName"] = "Docs \ud800"
        assert parse_catalog(payload, BASE).servers[0].display_name == "Docs �"

    @pytest.mark.parametrize("change, problem", [
        (lambda c: c.update(schemaVersion=True), "schemaVersion"),
        (lambda c: c.update(schemaVersion=2.0), "schemaVersion"),
        (lambda c: c.pop("schemaVersion"), "schemaVersion"),
        (lambda c: c.pop("generation"), "generation"),
        (lambda c: c.update(generation=True), "generation"),
        (lambda c: c.update(generation="7"), "generation"),
        (lambda c: c.update(generation=2**63), "generation"),
        (lambda c: c.update(digest=None), "digest"),
        (lambda c: c.update(servers={}), "servers"),
        (lambda c: c.pop("desiredRevision"), "desiredRevision"),
        (lambda c: c["servers"].append("docs"), r"servers\[2\] is not"),
        (lambda c: c["servers"][0].update(displayName=3), "displayName"),
        (lambda c: c["servers"][0].update(toolPolicy="selected"), "toolPolicy"),
        (lambda c: c["servers"][0]["toolPolicy"].update(tools="search"), "tools"),
        (lambda c: c["servers"][0]["toolPolicy"].update(mode="all", tools=[]), "tools"),
        (lambda c: c["servers"][0]["toolPolicy"].update(changes=True), "changes"),
        (lambda c: c["servers"][0]["toolPolicy"].update(changes="review\n"), "changes"),
        (lambda c: c["servers"][0].update(inventory=[]), "inventory"),
        (lambda c: c["servers"][0]["inventory"].update(toolCount=True), "toolCount"),
        (lambda c: c["servers"][0]["inventory"].pop("toolCount"), "toolCount"),
        (lambda c: c["servers"][0].update(health="current"), "health"),
        (lambda c: c["servers"][0]["health"].update(failure=7), "failure"),
        (lambda c: c["servers"][0]["inventory"].update(observedAt="2026-02-29T12:00:00Z"),
         "observedAt"),
        (lambda c: c["servers"][0]["inventory"].update(observedAt="2026-10-17T24:00:00Z"),
         "observedAt"),
        (lambda c: c["servers"][0]["inventory"].update(observedAt="2026-10-17T12:00:00Z\n"),
         "observedAt"),
        (lambda c: c["servers"][0]["inventory"].update(observedAt="２026-10-17T12:00:00Z"),
         "observedAt"),
    ])
    def test_contract_violations_are_refused(self, change, problem):
        payload = copy.deepcopy(EXAMPLE)
        change(payload)
        with pytest.raises(CatalogError, match=problem):
            parse_catalog(payload, BASE)

    def test_a_leap_day_is_a_time(self):
        payload = copy.deepcopy(EXAMPLE)
        payload["servers"][0]["inventory"]["observedAt"] = "2024-02-29T12:00:00Z"
        payload["servers"][0]["health"]["checkedAt"] = "2024-02-29T12:00:00Z"
        assert parse_catalog(payload, BASE).servers[0].inventory.observed_at == (
            "2024-02-29T12:00:00Z"
        )

    def test_not_an_object(self):
        with pytest.raises(CatalogError, match="object"):
            parse_catalog([EXAMPLE], BASE)

    def test_the_fixture_files_are_the_contract_set(self):
        assert sorted(p.name for p in FIXTURES.glob("*.json")) == sorted(
            [*CATALOG_FILES, "catalog-vectors.json"],
        )


class TestRevisions:
    def test_the_aggregate_of_no_server(self):
        assert desired_revision([]) == (
            "sha256:6de76e5f4ad97ea3356847d91222ceccbf0a01cfe7b2bd1f2cf932d26780b78c"
        )

    @pytest.mark.parametrize("name", CATALOG_FILES)
    def test_the_aggregate_of_each_fixture(self, name):
        payload = _fixture(name)
        assert desired_revision(
            (server["id"], server["desiredRevision"]) for server in payload["servers"]
        ) == payload["desiredRevision"]

    def test_the_aggregate_depends_on_order_and_pairs(self):
        a, b = "sha256:" + "a" * 64, "sha256:" + "b" * 64
        assert desired_revision([("docs", a), ("jira", b)]) != desired_revision(
            [("jira", b), ("docs", a)],
        )
        assert desired_revision([("docs", a)]) != desired_revision([("docs", b)])

    @pytest.mark.parametrize("known, observed, after, relist", [
        # A revision after another one: list the tools again.
        ("sha256:a", "sha256:b", "sha256:b", True),
        ("sha256:a", "sha256:a", "sha256:a", False),
        # A first revision: know it.
        (None, "sha256:a", "sha256:a", False),
        # No inventory after a known revision: list again once, know none.
        ("sha256:a", None, None, True),
        # No inventory and none known: nothing.
        (None, None, None, False),
    ])
    def test_the_inventory_rule(self, known, observed, after, relist):
        inventory = None if observed is None else GatewayInventory(
            revision=observed, tool_count=1, observed_at="2026-10-17T12:00:00Z",
            desired_revision="sha256:d",
        )
        assert follow_inventory(known, inventory) == (after, relist)


class TestCatalogReader:
    @pytest.mark.asyncio
    async def test_start_reads_the_catalog(self, gateway):
        changes = []

        async def on_change(catalog):
            changes.append(catalog)

        reader = McpGatewayCatalog(gateway.url + "/", on_change=on_change)
        try:
            await reader.start()
            assert [server.id for server in reader.servers] == ["docs", "github"]
            assert reader.servers[0].url == f"{gateway.url}/s/docs/mcp"
            assert [catalog.generation for catalog in changes] == [7]
            status = reader.status()
            assert status["url"] == gateway.url
            assert status["generation"] == 7
            assert status["desired_revision"] == gateway.catalog["desiredRevision"]
            assert [server["id"] for server in status["servers"]] == ["docs", "github"]
            assert status["error"] is None
            assert status["retrying"] is False
            assert status["fetched_at"] and status["checked_at"]
            assert "applied_at" not in status
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_a_new_desired_revision_replaces_the_catalog(self, gateway):
        changes = []

        async def on_change(catalog):
            changes.append(catalog.generation)

        reader = McpGatewayCatalog(gateway.url, on_change=on_change)
        try:
            await reader.start()
            first = reader.latest
            await reader.refresh()
            assert reader.latest == first
            assert changes == [7]

            gateway.catalog = catalog_payload(8, {"docs": ["search"]})
            await reader.refresh()
            assert reader.latest.generation == 8
            assert [server.id for server in reader.servers] == ["docs"]
            assert reader.servers[0].tools == ("search",)
            assert changes == [7, 8]
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_unreachable_at_start_retries_in_the_background(self, gateway):
        gateway.stop()
        reader = McpGatewayCatalog(gateway.url, retry_initial=0.05, retry_max=0.05)
        try:
            await reader.start()
            assert reader.latest is None
            assert reader.servers == ()
            status = reader.status()
            assert status["generation"] is None
            assert "cannot reach the MCP gateway" in status["error"]
            assert status["retrying"] is True

            gateway.start()
            await _until(lambda: reader.latest is not None)
            assert reader.latest.generation == 7
            await _until(lambda: not reader.retrying)
            assert reader.status()["error"] is None
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_an_outage_keeps_the_catalog(self, gateway):
        reader = McpGatewayCatalog(gateway.url, retry_initial=30)
        try:
            await reader.start()
            gateway.stop()
            await reader.refresh()
            assert reader.latest.generation == 7
            assert [server.id for server in reader.servers] == ["docs", "github"]
            assert reader.retrying is True
            assert "cannot reach" in reader.status()["error"]

            # A new session reads the catalog again, also while the retry
            # waits, and a success ends the retry.
            gateway.start()
            gateway.catalog = catalog_payload(9, {"docs": ["search"]})
            await reader.refresh()
            assert reader.latest.generation == 9
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
            assert reader.latest is None
            assert reader.status()["error"] == (
                "MCP gateway answered HTTP 503 (catalog_unavailable, "
                "request 0192f000-0000-7000-8000-000000000001)"
            )
        finally:
            await reader.close()

    @pytest.mark.parametrize("setup, problem", [
        (lambda g: setattr(g, "catalog", {**g.catalog, "schemaVersion": 1}),
         "schemaVersion"),
        (lambda g: setattr(g, "catalog", {**g.catalog, "schemaVersion": 3}),
         "schemaVersion"),
        (lambda g: setattr(g, "catalog", {**g.catalog, "desiredRevision": "sha256:" + "0" * 64}),
         "desiredRevision"),
        (lambda g: setattr(g, "catalog_body", b"{not json"), "not valid JSON"),
        (lambda g: setattr(g, "catalog_body", json.dumps(g.catalog).encode()[:-1] + b', "x": NaN}'),
         "not valid JSON"),
        (lambda g: setattr(g, "content_type", "text/html"), "not application/json"),
        (lambda g: setattr(g, "catalog_body", b" " * (MAX_CATALOG_BYTES + 1)),
         "larger than"),
    ])
    @pytest.mark.asyncio
    async def test_a_bad_catalog_keeps_the_last_one(self, gateway, setup, problem):
        reader = McpGatewayCatalog(gateway.url, retry_initial=30)
        try:
            await reader.start()
            setup(gateway)
            await reader.refresh()
            assert reader.latest.generation == 7
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
            assert reader.latest is not None
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


class TestWhatMakesAChange:
    """Only the desired revision and the inventory rule make a change."""

    @pytest_asyncio.fixture
    async def reader(self, gateway):
        changes: list[dict] = []

        async def on_change(catalog):
            changes.append({
                "desired": catalog.desired_revision,
                "inventory": {
                    server.id: server.inventory.revision if server.inventory else None
                    for server in catalog.servers
                },
            })

        reader = McpGatewayCatalog(gateway.url, on_change=on_change)
        reader.changes = changes
        await reader.start()
        yield reader
        await reader.close()

    @pytest.mark.asyncio
    async def test_health_times_and_generation_change_nothing(self, gateway, reader):
        catalog = copy.deepcopy(gateway.catalog)
        github = catalog["servers"][1]
        github["health"] = {
            "inventoryState": "stale", "checkedAt": "2026-10-17T12:05:00Z",
            "failure": "upstream_timeout",
        }
        catalog["servers"][0]["inventory"]["toolCount"] = 9
        catalog["servers"][0]["displayName"] = "Product docs"
        # Another agent's policy edit changes the tenant generation only.
        catalog["generation"] = 8
        catalog["digest"] = "sha256:" + "8" * 64
        gateway.catalog = catalog
        await reader.refresh()

        assert len(reader.changes) == 1
        # Diagnostics show what the gateway observed, all the same.
        status = reader.status()
        assert status["generation"] == 8
        assert status["servers"][1]["health"] == {
            "inventory_state": "stale", "checked_at": "2026-10-17T12:05:00Z",
            "failure": "upstream_timeout",
        }
        assert reader.latest.servers[0].display_name == "Product docs"

    @pytest.mark.asyncio
    async def test_a_new_inventory_revision_is_a_change(self, gateway, reader):
        gateway.catalog = catalog_payload(
            7, {"docs": ["fetch_page", "search"], "github": ["get_issue"]},
            inventory={"github": ["get_issue", "list_issues"]},
        )
        await reader.refresh()
        assert len(reader.changes) == 2
        assert reader.changes[0]["desired"] == reader.changes[1]["desired"]

    @pytest.mark.asyncio
    async def test_a_lost_observation_is_a_change_once(self, gateway, reader):
        """A replica without the observation answers: list again once."""
        tools = {"docs": ["fetch_page", "search"], "github": ["get_issue"]}
        observed = copy.deepcopy(gateway.catalog)
        lost = catalog_payload(7, tools, inventory={"github": None})
        for catalog in (lost, lost, observed, observed, lost):
            gateway.catalog = catalog
            await reader.refresh()
        # The first null after a known revision, then nothing until a revision
        # appears again; that revision is taken without a change.
        assert [change["inventory"]["github"] for change in reader.changes] == [
            observed["servers"][1]["inventory"]["revision"], None, None,
        ]

    @pytest.mark.asyncio
    async def test_a_first_observation_is_no_change(self, gateway):
        changes = []

        async def on_change(catalog):
            changes.append(catalog)

        tools = {"docs": ["search"]}
        gateway.catalog = catalog_payload(7, tools, inventory={"docs": None})
        reader = McpGatewayCatalog(gateway.url, on_change=on_change)
        try:
            await reader.start()
            assert reader.servers[0].inventory is None
            gateway.catalog = catalog_payload(7, tools)
            await reader.refresh()
            assert reader.servers[0].inventory is not None
            assert len(changes) == 1
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_the_status_shows_desired_and_observed_state_apart(self, gateway, reader):
        gateway.catalog = catalog_payload(
            7, {"docs": ["search"], "jira": []}, inventory={"jira": None},
        )
        await reader.refresh()
        docs, jira = reader.status()["servers"]
        assert docs == {
            "id": "docs",
            "desired_revision": gateway.catalog["servers"][0]["desiredRevision"],
            "tool_mode": "selected",
            "tool_changes": "accept",
            "inventory": {
                "revision": gateway.catalog["servers"][0]["inventory"]["revision"],
                "tool_count": 1,
                "observed_at": "2026-10-17T12:00:00Z",
                "desired_revision": gateway.catalog["servers"][0]["desiredRevision"],
            },
            "health": {
                "inventory_state": "current", "checked_at": "2026-10-17T12:00:00Z",
                "failure": None,
            },
        }
        assert jira["inventory"] is None
        assert jira["health"]["inventory_state"] == "unknown"
