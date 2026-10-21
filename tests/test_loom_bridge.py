"""The Loom memory backend against a fake Loom (``httpx.MockTransport``).

Covers every memory tool path through the real handlers, the bridge's
failure contract (an outage raises ``MemoryBackendUnavailable`` after bounded
retries; an abstaining search is empty, never an error), identity resolution,
and output parity with the memU bridge on the same inputs.

The fake is strict: a request field an operation does not take is refused,
as Loom refuses one on a write, so a misspelt field fails a test.
"""

from __future__ import annotations

import base64
import hashlib
import json
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import pytest_asyncio

from nerve.agent.tools.handlers.memory import (
    MEMORY_SPECS,
    category_update_handler,
    conversation_history_handler,
    memorize_handler,
    memory_delete_handler,
    memory_expand_category_handler,
    memory_recall_handler,
    memory_records_by_date_handler,
    memory_update_handler,
    session_context_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.config import (
    LoomMemoryConfig,
    MemoryCategoryConfig,
    MemoryConfig,
    NerveConfig,
)
from nerve.memory.backend import MemoryBackendUnavailable, create_memory_bridge
from nerve.memory.loom_bridge import LoomBridge, LoomRequestError
from nerve.memory.memu_bridge import MemUBridge

ORG = "acme"
HOME = "agent-home"
SHARED = "team-shared"
TOKEN = "test-token-value"

# The fields the bridge sends with each operation (besides `consumer`), as
# Loom's API names them. The fake refuses any other, so a misspelt or new field
# fails a test until it is checked against the API and added here.
_FIELDS = {
    "context.pack": {
        "org", "namespace", "agent", "query", "mode", "only_sections", "top_k",
        "section_token_budgets", "include_decisions",
    },
    "memory.set": {
        "org", "namespace", "agent", "session_id", "memory_key", "memory_type", "value",
        "tags", "valid_at",
    },
    "memory.search": {
        "org", "namespace", "agent", "query", "recipe", "namespace_scope", "top_k",
        "include_tags", "bodies", "explain", "legs",
    },
    "memory.list": {
        "org", "namespace", "agent", "user_id", "session_id", "tags", "sort_by", "limit",
        "offset", "preview_chars", "contains",
    },
    "memory.timeline": {
        "org", "namespace", "agent", "tags", "since", "until", "top_k", "order",
    },
    "memory.get": {"org", "namespace", "agent", "user_id", "session_id", "memory_key"},
    "memory.get_by_id": {"org", "memory_id"},
    "memory.update_by_id": {"org", "memory_id", "value", "tags", "if_match_sha256"},
    "memory.delete": {"org", "namespace", "agent", "user_id", "session_id", "memory_key"},
    "memory.assign_category": {"org", "namespace", "memory_key", "category_name", "tombstone"},
    "memory.set_from_messages": {
        "org", "namespace", "agent", "session_id", "messages", "last_messages", "defer",
        "extra_tags", "observation_date",
    },
    "memory.extraction_status": {
        "org", "namespace", "agent", "session_id", "queue_id", "limit",
    },
    "namespace.search_categories": {"org", "namespace", "query", "top_k"},
    "namespace.list_categories": {"org", "namespace"},
    "namespace.register_category": {"org", "namespace", "name", "description", "summary"},
    "namespace.summary": {"org", "namespace", "top_entities_limit"},
}
_MESSAGE_FIELDS = {"role", "content", "timestamp"}

# A context pack's item label percent-encodes (UTF-8, upper-case hex) the
# characters that would end the label or start another attribute in it.
_LABEL_STOPS = frozenset('[]=%",')
_PACK_CUT = "...[truncated]"


def _label(text: str) -> str:
    return "".join(
        "".join(f"%{byte:02X}" for byte in ch.encode()) if ch.isspace() or ch in _LABEL_STOPS else ch
        for ch in text
    )


def _pack_item(cite: str, text: str) -> str:
    """One memories-section item the way a pack renders it: the label, then
    the text, its later lines indented by two spaces."""
    body = "\n  ".join(text.splitlines())
    return f"- [{_label(cite)} source=fact by=process:sa%3Atest] {body}"


def _pack_section(items: list[str], budget_chars: int) -> str:
    """Items joined into a section with < and > escaped; a section longer than
    its budget keeps what fits ahead of a final cut-marker line."""
    section = "\n".join(items).replace("<", "&lt;").replace(">", "&gt;")
    if len(section) <= budget_chars:
        return section
    room = max(0, budget_chars - len(_PACK_CUT) - 2)
    return section[:room].rstrip() + "\n" + _PACK_CUT


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _dt(text: str | None) -> datetime | None:
    if not text:
        return None
    dt = datetime.fromisoformat(text)
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _sha(value) -> str:
    return hashlib.sha256(json.dumps(value).encode()).hexdigest()


def _ok(body: dict) -> httpx.Response:
    return httpx.Response(200, json=body)


def _error(status: int, code: str, message: str = "", **headers) -> httpx.Response:
    return httpx.Response(status, json={"detail": {"code": code, "message": message}}, headers=headers)


def _jwt(claims: dict) -> str:
    def part(data: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip("=")
    return f"{part({'alg': 'HS256'})}.{part(claims)}.signature"


def _compact(value) -> int:
    """Bytes of ``value`` as httpx sends it."""
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())


class FakeLoom:
    """Just enough of Loom's HTTP API, with state, to run every tool."""

    def __init__(self, *, namespaces=(HOME, SHARED), whoami_status=200, authenticated=True):
        self.namespaces = list(namespaces)
        self.whoami_status = whoami_status  # 0: the connection fails
        self.authenticated = authenticated
        self.whoami_calls = 0
        self.calls: list[tuple[str, dict, httpx.Headers]] = []
        self.sizes: dict[str, list[int]] = {}
        self.rows: dict[tuple[str, str], dict] = {}
        # Keyed by the exact name: Loom matches a category by its registered name.
        self.categories: dict[tuple[str, str], dict] = {}
        self.links: set[tuple[str, str, str]] = set()
        self.queued: list[dict] = []
        self.queue: dict[str, dict] = {}
        self.failures: dict[str, list] = {}
        self.search_response: dict | None = None

    # -- plumbing ---------------------------------------------------------

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    def fail(self, op: str, *responses) -> None:
        """Answer the next calls of ``op`` with these responses or exceptions."""
        self.failures.setdefault(op, []).extend(responses)

    def bodies(self, op: str) -> list[dict]:
        return [body for name, body, _ in self.calls if name == op]

    def headers(self, op: str) -> list[httpx.Headers]:
        return [headers for name, _, headers in self.calls if name == op]

    def handle(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/whoami":
            self.whoami_calls += 1
            if self.whoami_status == 0:
                raise httpx.ConnectError("connection refused")
            if self.whoami_status != 200:
                return httpx.Response(self.whoami_status, json={"detail": "Not Found"})
            if not self.authenticated:
                return _ok({"authenticated": False, "reason": "token refused"})
            return _ok({
                "authenticated": True, "subject": "sa:test", "org": ORG,
                "namespaces": list(self.namespaces),
            })
        op = request.url.path.removeprefix("/v1/")
        body = json.loads(request.content or b"{}")
        self.calls.append((op, body, request.headers))
        self.sizes.setdefault(op, []).append(len(request.content))
        unknown = set(body) - _FIELDS[op] - {"consumer"}
        for message in body.get("messages", []) + body.get("last_messages", []):
            unknown |= {f"messages[].{k}" for k in set(message) - _MESSAGE_FIELDS}
        if unknown:
            return _error(422, "UNKNOWN_ARGUMENT", f"{op} takes no {sorted(unknown)}")
        pending = self.failures.get(op)
        if pending:
            failure = pending.pop(0)
            if isinstance(failure, Exception):
                raise failure
            return failure
        return getattr(self, "_" + op.replace(".", "_"))(body)

    def add_row(self, namespace, key, value, *, memory_type="semantic", tags=(),
                valid_at=None, created_at=None, updated_at=None, agent="nerve",
                session_id="-") -> dict:
        row = {
            "memory_id": str(uuid.uuid4()), "namespace": namespace, "memory_key": key,
            "value": value, "memory_type": memory_type, "tags": [t.lower() for t in tags],
            "valid_at": valid_at, "created_at": created_at or _iso(_now()),
            "updated_at": updated_at, "agent": agent, "user_id": "-",
            "session_id": session_id, "sha": _sha(value),
        }
        self.rows[(namespace, key)] = row
        return row

    def add_category(self, namespace, name, description="", summary="") -> str:
        cat_id = hashlib.sha256(f"{ORG}\x00{namespace}\x00{name}".encode()).hexdigest()[:32]
        self.categories[(namespace, name)] = {
            "category_id": cat_id, "name": name, "description": description, "summary": summary,
        }
        return cat_id

    def _row_by_id(self, memory_id) -> dict | None:
        return next((r for r in self.rows.values() if r["memory_id"] == memory_id), None)

    def _hits(self, query: str, top_k: int, include_tags: bool, namespace: str = "") -> list[dict]:
        """Rows matching a word of ``query``: in every namespace (all_mine),
        or in ``namespace`` only."""
        words = [w.lower() for w in query.split() if len(w) > 2]
        hits = []
        for (ns, key), row in self.rows.items():
            if namespace and ns != namespace:
                continue
            if any(w in str(row["value"]).lower() for w in words):
                hit = {
                    "memory_key": key, "memory_type": row["memory_type"],
                    "content_excerpt": str(row["value"]), "distance": 0.2,
                    "similarity": 0.8, "namespace": ns,
                }
                if include_tags:
                    hit["tags"] = list(row["tags"])
                hits.append(hit)
        return hits[:top_k]

    # -- operations -------------------------------------------------------

    def _context_pack(self, b):
        # Startup mode with a query renders the search's hits of the namespace
        # it names, even where memory.search would abstain, in a section of
        # four characters per budgeted token (80 tokens at least).
        items = [
            _pack_item(f"memory:{hit['namespace']}/{hit['memory_key']}", hit["content_excerpt"])
            for hit in self._hits(b.get("query", ""), b.get("top_k", 5), False, b["namespace"])
        ]
        tokens = (b.get("section_token_budgets") or {}).get("memories", 300)
        body = _pack_section(items, 4 * max(tokens, 80)) if items else ""
        return self.pack_response({"memories": body} if body else {})

    @staticmethod
    def pack_response(sections: dict[str, str]) -> httpx.Response:
        return _ok({
            "rendered": "\n".join(f"<{name}>\n{text}\n</{name}>" for name, text in sections.items()),
            "sections": sections, "blocks": [], "section_meta": {},
            "section_count": len(sections), "truncated": False, "token_budget": 2000,
            "mode": "startup",
        })

    def _memory_set(self, b):
        row = self.add_row(
            b["namespace"], b["memory_key"], b["value"], memory_type=b.get("memory_type", "semantic"),
            tags=b.get("tags", []), valid_at=b.get("valid_at"), agent=b["agent"],
            session_id=b.get("session_id", "-"),
        )
        return _ok({
            "version": 1, "memory_id": row["memory_id"], "content_sha256": row["sha"],
            "status": "active", "namespace": b["namespace"], "memory_key": b["memory_key"],
            "conflict_verdict": "clear",
        })

    def _memory_search(self, b):
        if self.search_response is not None:
            return _ok(self.search_response)
        hits = self._hits(b["query"], b.get("top_k", 5), bool(b.get("include_tags")))
        return _ok({"results": hits, "abstain": not hits})

    def _namespace_search_categories(self, b):
        cats = [c for (ns, _), c in self.categories.items() if ns == b["namespace"]]
        return _ok({"results": [
            # Loom's excerpt is substring(summary, 1, 280): BYTES, perhaps
            # ending inside a character.
            {"category_id": c["category_id"], "name": c["name"], "description": c["description"],
             "summary_excerpt": c["summary"].encode()[:280].decode("utf-8", "replace"),
             "distance": 0.3, "similarity": 0.7}
            for c in cats[: b.get("top_k", 5)]
        ]})

    def _namespace_list_categories(self, b):
        ns = b["namespace"]
        return _ok({"categories": [
            {"category_id": c["category_id"], "name": c["name"], "description": c["description"],
             "item_count": sum(1 for link in self.links if link[0] == ns and link[2] == c["category_id"])}
            for (cns, _), c in self.categories.items() if cns == ns
        ]})

    def _namespace_register_category(self, b):
        cat_id = self.add_category(b["namespace"], b["name"], b.get("description", ""), b.get("summary", ""))
        return _ok({"category_id": cat_id, "embedded": True})

    def _memory_list(self, b):
        rows = [r for (ns, _), r in self.rows.items() if ns == b["namespace"]]
        tags = {t.lower() for t in b.get("tags") or []}
        if tags:
            rows = [r for r in rows if tags & set(r["tags"])]
        if b.get("contains"):
            rows = [r for r in rows if b["contains"].lower() in str(r["value"]).lower()]
        rows.sort(key=lambda r: r["created_at"], reverse=True)
        offset, limit = b.get("offset", 0), b.get("limit", 100)
        preview = b.get("preview_chars", 2048)
        page = rows[offset: offset + limit]
        return _ok({
            "entries": [
                {"memory_key": r["memory_key"], "memory_type": r["memory_type"], "confidence": 1.0,
                 "version": 1, "created_at": r["created_at"], "updated_at": r["updated_at"],
                 "valid_at": r["valid_at"], "tags": r["tags"], "agent": r["agent"],
                 "user_id": r["user_id"], "session_id": r["session_id"],
                 "value": str(r["value"])[:preview]}
                for r in page
            ],
            "truncated": offset + limit < len(rows),
            "total": len(rows),
        })

    def _memory_timeline(self, b):
        tags = set(b.get("tags") or [])
        rows = [r for (ns, _), r in self.rows.items() if ns == b["namespace"] and tags & set(r["tags"])]
        since, until = _dt(b.get("since")), _dt(b.get("until"))

        def axis(row):
            return _dt(row["valid_at"] or row["created_at"])

        rows = [r for r in rows if (since is None or axis(r) >= since) and (until is None or axis(r) <= until)]
        rows.sort(key=axis, reverse=True)
        return _ok({"events": [
            {"memory_key": r["memory_key"], "memory_type": r["memory_type"],
             "content_excerpt": str(r["value"]), "valid_at": r["valid_at"],
             "created_at": r["created_at"], "namespace": r["namespace"]}
            for r in rows[: b.get("top_k", 50)]
        ], "total_matching": len(rows)})

    def _memory_get(self, b):
        row = self.rows.get((b["namespace"], b["memory_key"]))
        if row is None:
            return _ok({"found": False})
        return _ok({
            "found": True, "memory_id": row["memory_id"], "value": row["value"],
            "memory_type": row["memory_type"], "tags": row["tags"],
            "content_sha256": row["sha"], "agent": row["agent"],
        })

    def _memory_get_by_id(self, b):
        row = self._row_by_id(b["memory_id"])
        if row is None:
            return _ok({"found": False})
        return _ok({
            "found": True, "memory_id": row["memory_id"], "namespace": row["namespace"],
            "agent": row["agent"], "user_id": row["user_id"], "session_id": row["session_id"],
            "memory_key": row["memory_key"], "memory_type": row["memory_type"],
            "value": row["value"], "content_sha256": row["sha"],
        })

    def _memory_update_by_id(self, b):
        row = self._row_by_id(b["memory_id"])
        if row is None:
            return _ok({"updated": False})
        if b.get("if_match_sha256") and b["if_match_sha256"] != row["sha"]:
            return _error(409, "PRECONDITION_FAILED", "the row changed")
        if b.get("value") is not None:
            row["value"] = b["value"]
            row["sha"] = _sha(b["value"])
        if b.get("tags") is not None:
            row["tags"] = [t.lower() for t in b["tags"]]
        row["updated_at"] = _iso(_now())
        return _ok({"updated": True, "new_version": 2, "content_sha256": row["sha"]})

    def _memory_delete(self, b):
        row = self.rows.pop((b["namespace"], b["memory_key"]), None)
        return _ok({"tombstoned": row is not None, "found": row is not None, "version": 3})

    def _memory_assign_category(self, b):
        cat = self.categories.get((b["namespace"], b["category_name"]))
        if cat is None:
            return _error(404, "CATEGORY_NOT_FOUND", "not registered")
        link = (b["namespace"], b["memory_key"], cat["category_id"])
        if b.get("tombstone"):
            self.links.discard(link)
        else:
            self.links.add(link)
        return _ok({"assigned": not b.get("tombstone"), "category_id": cat["category_id"]})

    def _memory_set_from_messages(self, b):
        assert 1 <= len(b["messages"]) <= 400
        self.queued.append(b)
        queue_id = f"q{len(self.queued)}"
        self.queue[queue_id] = {
            "queue_id": queue_id, "agent": b["agent"], "user_id": "-",
            "session_id": b.get("session_id", "-"), "status": "pending", "attempts": 0,
            "message_count": len(b["messages"]), "content_chars": 0, "written": 0,
            "last_error": "", "enqueued_at": _iso(_now()), "updated_at": _iso(_now()),
        }
        return _ok({"written": [], "deferred": True, "queue_id": queue_id})

    def _memory_extraction_status(self, b):
        rows = list(reversed(self.queue.values()))  # newest first
        if b.get("session_id", "-") != "-":
            rows = [r for r in rows if r["session_id"] == b["session_id"]]
        if b.get("queue_id"):
            rows = [r for r in rows if r["queue_id"] == b["queue_id"]]
        if b.get("status"):
            rows = [r for r in rows if r["status"] == b["status"]]
        rows = rows[: b.get("limit", 50)]
        return _ok({
            "batches": rows,
            "pending": sum(1 for r in rows if r["status"] in ("pending", "running")),
            "failed": sum(1 for r in rows if r["status"] == "failed"),
        })

    def _namespace_summary(self, b):
        rows = [r for (ns, _), r in self.rows.items() if ns == b["namespace"]]
        counts = Counter(r["memory_type"] for r in rows)
        return _ok({
            "org": b["org"], "namespace": b["namespace"], "total_memories": len(rows),
            "type_counts": [{"memory_type": t, "n": n} for t, n in counts.items()],
            "top_entities": [], "recent_writes": 0, "recent_recalls": 0,
            "distinct_agents": 1, "last_activity_at": None,
        })


def _config(**loom) -> NerveConfig:
    config = NerveConfig()
    config.memory = MemoryConfig(
        backend="loom",
        loom=LoomMemoryConfig(**{"base_url": "http://loom.test", "token": TOKEN, **loom}),
    )
    return config


@pytest_asyncio.fixture
async def make_bridge():
    made: list[LoomBridge] = []

    async def _make(fake: FakeLoom, *, config: NerveConfig | None = None, audit_db=None, **loom):
        bridge = LoomBridge(config or _config(**loom), audit_db=audit_db, transport=fake.transport())
        bridge._backoff_base_s = 0
        assert await bridge.initialize()
        made.append(bridge)
        return bridge

    yield _make
    for bridge in made:
        await bridge.shutdown()


def _ctx(bridge, session_id: str = "s-1") -> ToolContext:
    return ToolContext(session_id=session_id, workspace=Path("/tmp/ws"), memory_bridge=bridge)


def _text(result) -> str:
    return result.content[0]["text"]


# --------------------------------------------------------------------------- #
# Factory, lifecycle, identity
# --------------------------------------------------------------------------- #


def test_factory_builds_the_configured_backend() -> None:
    assert isinstance(create_memory_bridge(_config()), LoomBridge)
    assert isinstance(create_memory_bridge(NerveConfig()), MemUBridge)


def test_tool_descriptions_name_no_backend() -> None:
    for spec in MEMORY_SPECS:
        text = spec.description + json.dumps(spec.input_schema)
        assert "memu" not in text.lower(), spec.name


@pytest.mark.asyncio
async def test_the_fake_refuses_fields_an_operation_does_not_take(make_bridge) -> None:
    bridge = await make_bridge(FakeLoom())
    with pytest.raises(LoomRequestError, match="UNKNOWN_ARGUMENT"):
        await bridge._op("memory.search", {"query": "x", "namepsace_scope": "all_mine"})


@pytest.mark.asyncio
async def test_identity_from_whoami_uses_the_first_namespace(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    await bridge.recall("anything")
    body = fake.bodies("memory.search")[0]
    assert body["org"] == ORG and body["namespace"] == HOME
    assert fake.whoami_calls == 1
    await bridge.recall("again")
    assert fake.whoami_calls == 1  # cached for the process


@pytest.mark.asyncio
async def test_explicit_settings_skip_whoami(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake, org="other-org", namespace="pinned")
    await bridge.recall("anything")
    body = fake.bodies("memory.search")[0]
    assert (body["org"], body["namespace"]) == ("other-org", "pinned")
    assert fake.whoami_calls == 0


@pytest.mark.asyncio
async def test_org_setting_combines_with_whoami_namespace(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake, org="my-org")
    await bridge.recall("anything")
    body = fake.bodies("memory.search")[0]
    assert (body["org"], body["namespace"]) == ("my-org", HOME)


@pytest.mark.asyncio
async def test_identity_falls_back_to_the_jwt_claims(make_bridge) -> None:
    fake = FakeLoom(whoami_status=404)  # e.g. a proxy that does not forward it
    bridge = await make_bridge(fake, token=_jwt({"org": "jwt-org", "namespaces": ["jwt-home", "x"]}))
    await bridge.recall("anything")
    body = fake.bodies("memory.search")[0]
    assert (body["org"], body["namespace"]) == ("jwt-org", "jwt-home")


@pytest.mark.asyncio
async def test_unresolvable_identity_is_an_outage_naming_each_step(make_bridge) -> None:
    fake = FakeLoom(whoami_status=404)
    bridge = await make_bridge(fake)  # the token is not a JWT
    assert bridge.available  # boot does not fail on it
    with pytest.raises(MemoryBackendUnavailable) as err:
        await bridge.recall("anything")
    message = str(err.value)
    assert "GET /v1/whoami answered HTTP 404" in message
    assert "not a JWT" in message
    assert "memory.loom.namespace" in message and "LOOM_NAMESPACE" in message
    assert TOKEN not in message
    calls = fake.whoami_calls
    with pytest.raises(MemoryBackendUnavailable):
        await bridge.recall("again")
    assert fake.whoami_calls == calls  # remembered, not looked up on every call
    assert fake.bodies("memory.search") == []


@pytest.mark.asyncio
async def test_unreachable_loom_is_reported_as_such(make_bridge) -> None:
    fake = FakeLoom(whoami_status=0)  # the lookup cannot connect
    bridge = await make_bridge(fake)
    assert bridge.available  # boot does not fail on it
    with pytest.raises(MemoryBackendUnavailable) as err:
        await bridge.recall("anything")
    assert "Loom at http://loom.test could not be asked" in str(err.value)
    assert "ConnectError" in str(err.value)


@pytest.mark.asyncio
async def test_an_unavailable_whoami_names_the_availability_problem(make_bridge) -> None:
    bridge = await make_bridge(FakeLoom(whoami_status=503))
    with pytest.raises(MemoryBackendUnavailable) as err:
        await bridge.recall("anything")
    assert "could not be asked for the org and namespace (HTTP 503)" in str(err.value)
    assert "Set memory.loom.org" not in str(err.value)


@pytest.mark.asyncio
async def test_a_refused_token_names_the_credential(make_bridge) -> None:
    for fake in (FakeLoom(whoami_status=401), FakeLoom(authenticated=False)):
        bridge = await make_bridge(fake)
        with pytest.raises(MemoryBackendUnavailable) as err:
            await bridge.recall("anything")
        assert "refused the configured token" in str(err.value)
        assert "memory.loom.token" in str(err.value)


@pytest.mark.asyncio
async def test_identity_is_resolved_again_after_a_401(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    await bridge.recall("warm up")
    fake.namespaces = ["renewed-home"]
    fake.fail("memory.search", _error(401, "AUTH_INVALID", "token expired"))
    await bridge.recall("after renewal")
    assert fake.bodies("memory.search")[-1]["namespace"] == "renewed-home"
    assert fake.whoami_calls == 2


@pytest.mark.asyncio
async def test_a_refused_home_credential_is_an_outage(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    fake.fail("memory.set", *[_error(403, "AUTH_INSUFFICIENT", "no writer grant")] * 2)
    with pytest.raises(MemoryBackendUnavailable, match="refused the credential.*403"):
        await bridge.memorize_fact("x")
    assert fake.whoami_calls == 2  # looked up again once


@pytest.mark.asyncio
async def test_a_403_for_another_namespace_is_a_refusal_not_an_outage(make_bridge) -> None:
    fake = FakeLoom()
    fake.add_row(SHARED, "team_rule", "Deploy on Tuesdays")
    bridge = await make_bridge(fake)
    # The id is first tried as a key at home (not found), then in the shared
    # namespace, where the token only reads.
    fake.fail("memory.delete", _ok({"tombstoned": False, "found": False}),
              _error(403, "AUTH_INSUFFICIENT", "viewer only"))
    text = _text(await memory_delete_handler(_ctx(bridge), {"memory_id": f"{SHARED}/team_rule"}))
    assert text == "Error: Loom refused memory.delete (HTTP 403 AUTH_INSUFFICIENT): viewer only"
    assert fake.bodies("memory.delete")[-1]["namespace"] == SHARED
    assert fake.whoami_calls == 1  # no identity lookup for it
    assert (SHARED, "team_rule") in fake.rows


@pytest.mark.asyncio
async def test_a_403_on_a_row_of_another_namespace_is_a_refusal(make_bridge) -> None:
    fake = FakeLoom()
    row = fake.add_row(SHARED, "team_rule", "Deploy on Tuesdays")
    bridge = await make_bridge(fake)
    fake.fail("memory.update_by_id", _error(403, "AUTH_INSUFFICIENT", "viewer only"))
    text = _text(await memory_update_handler(
        _ctx(bridge), {"memory_id": row["memory_id"], "content": "Deploy on Fridays"},
    ))
    assert text == "Error: Loom refused memory.update_by_id (HTTP 403 AUTH_INSUFFICIENT): viewer only"
    assert "namespace" not in fake.bodies("memory.update_by_id")[0]
    assert fake.whoami_calls == 1


@pytest.mark.asyncio
async def test_a_policy_refusal_is_a_refusal_not_an_outage(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    fake.fail("memory.set", _error(403, "POLICY_FORBIDDEN", "writes are paused"))
    text = _text(await memorize_handler(_ctx(bridge), {"content": "x"}))
    assert text == "Error: Loom refused memory.set (HTTP 403 POLICY_FORBIDDEN): writes are paused"
    assert fake.whoami_calls == 1


@pytest.mark.asyncio
async def test_a_failed_lookup_after_a_refusal_does_not_block_later_calls(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    await bridge.recall("warm up")
    fake.whoami_status = 503
    fake.fail("memory.search", _error(401, "AUTH_INVALID", "blip"))
    with pytest.raises(MemoryBackendUnavailable, match="refused the credential"):
        await bridge.recall("x")
    fake.whoami_status = 200
    await bridge.recall("right after")  # the old identity, nothing held back
    assert fake.bodies("memory.search")[-1]["namespace"] == HOME


@pytest.mark.asyncio
async def test_every_request_carries_consumer_org_and_bearer(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake, agent="agent-7")
    await bridge.memorize_fact("The deploy window is Tuesday", "knowledge", session_id="sess-9")
    await bridge.recall("deploy window")
    for op, body, headers in fake.calls:
        assert body["consumer"] == "nerve", op
        assert body["org"] == ORG, op
        assert headers["authorization"] == f"Bearer {TOKEN}"
        assert headers.get("x-request-id")
    write = fake.bodies("memory.set")[0]
    assert write["agent"] == "agent-7" and write["session_id"] == "sess-9"
    search = fake.bodies("memory.search")[0]
    assert search["agent"] == "*" and "session_id" not in search


@pytest.mark.asyncio
async def test_bad_url_fails_initialize_without_raising() -> None:
    bridge = LoomBridge(_config(base_url="not a url"), transport=FakeLoom().transport())
    assert await bridge.initialize() is False
    assert bridge.available is False
    assert await bridge.recall("x") == []
    await bridge.shutdown()


@pytest.mark.asyncio
async def test_shutdown_is_idempotent(make_bridge) -> None:
    bridge = await make_bridge(FakeLoom())
    await bridge.shutdown()
    await bridge.shutdown()
    assert bridge.available is False
    LoomBridge._release_memory()  # the sweep calls it; a no-op here


# --------------------------------------------------------------------------- #
# Transport: retries, the breaker, rate limits
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_outage_raises_after_three_attempts_then_fails_fast(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    await bridge.recall("warm up")  # identity resolved
    fake.fail("memory.search", *[httpx.ConnectError("refused")] * 3)
    with pytest.raises(MemoryBackendUnavailable, match="cannot reach Loom"):
        await bridge.recall("anything")
    assert len(fake.bodies("memory.search")) == 1 + 3
    # The breaker: the next call does not wait out the timeouts again.
    with pytest.raises(MemoryBackendUnavailable, match="not retried for another"):
        await bridge.recall("anything")
    assert len(fake.bodies("memory.search")) == 1 + 3


@pytest.mark.asyncio
async def test_a_dropped_connection_or_full_pool_does_not_trip_the_breaker(make_bridge) -> None:
    fake = FakeLoom()
    fake.add_row(HOME, "k1", "the sky is blue")
    bridge = await make_bridge(fake)
    fake.fail("memory.search", httpx.RemoteProtocolError("server disconnected"),
              httpx.PoolTimeout("no free connection"))
    assert (await bridge.recall("sky blue"))[0]["id"] == "k1"  # sent again, then answered
    fake.fail("memory.search", *[httpx.PoolTimeout("no free connection")] * 3)
    with pytest.raises(MemoryBackendUnavailable, match="PoolTimeout"):
        await bridge.recall("sky blue")
    assert bridge._down_until == 0.0
    assert (await bridge.recall("sky blue"))[0]["id"] == "k1"


@pytest.mark.asyncio
async def test_recall_handler_says_down_not_empty(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    fake.fail("memory.search", *[_error(503, "BACKEND_UNAVAILABLE", "storage down")] * 3)
    text = _text(await memory_recall_handler(_ctx(bridge), {"query": "q"}))
    assert "MEMORY BACKEND DOWN" in text and "NOT empty" in text
    # A fast HTTP answer does not open the breaker; the next call asks again.
    assert bridge._down_until == 0.0
    assert await bridge.recall("q") == []


@pytest.mark.asyncio
async def test_a_transient_5xx_is_retried(make_bridge) -> None:
    fake = FakeLoom()
    fake.add_row(HOME, "k1", "the sky is blue", tags=["nerve", "nerve:knowledge"])
    bridge = await make_bridge(fake)
    fake.fail("memory.search", _error(502, "BAD_GATEWAY"))
    out = await bridge.recall("sky blue")
    assert out and out[0]["summary"] == "the sky is blue"
    assert len(fake.bodies("memory.search")) == 2


@pytest.mark.asyncio
async def test_a_short_rate_limit_is_waited_out(make_bridge) -> None:
    fake = FakeLoom()
    fake.add_row(HOME, "k1", "the sky is blue")
    bridge = await make_bridge(fake)
    fake.fail("memory.search", _error(429, "RATE_LIMITED", "slow down", **{"Retry-After": "0"}))
    assert (await bridge.recall("sky blue"))[0]["id"] == "k1"


@pytest.mark.asyncio
async def test_a_long_rate_limit_fails_at_once_and_holds_the_operation(make_bridge) -> None:
    fake = FakeLoom()
    fake.add_row(HOME, "k1", "the sky is blue")
    bridge = await make_bridge(fake)
    fake.fail("memory.search", _error(429, "RATE_LIMITED", "slow down", **{"Retry-After": "300"}))
    with pytest.raises(MemoryBackendUnavailable, match="RATE_LIMITED"):
        await bridge.recall("sky blue")
    assert len(fake.bodies("memory.search")) == 1  # not sent again
    with pytest.raises(MemoryBackendUnavailable, match="rate limited; not retried for another (59|60)s"):
        await bridge.recall("sky blue")
    assert len(fake.bodies("memory.search")) == 1
    assert await bridge.memorize_fact("other operations still go")
    bridge._limited.clear()
    assert (await bridge.recall("sky blue"))[0]["id"] == "k1"


# --------------------------------------------------------------------------- #
# Recall
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_recall_request_and_result_shape(make_bridge) -> None:
    fake = FakeLoom()
    fake.search_response = {"results": [
        {"memory_key": "nerve_profile_" + "a" * 20, "memory_type": "semantic",
         "content_excerpt": "Alice lives in Metropolis", "tags": ["nerve", "nerve:profile"],
         "namespace": HOME},
        {"memory_key": "user_coffee", "memory_type": "preference",
         "content_excerpt": "Prefers black coffee", "tags": ["source:nerve"], "namespace": ""},
        {"memory_key": "chunk:memory.set:long_doc:2", "parent_memory_key": "long_doc",
         "memory_type": "semantic", "content_excerpt": "a passage of the long doc",
         "tags": [], "namespace": HOME, "source": "chunk"},
        {"memory_key": "long_doc", "memory_type": "semantic",
         "content_excerpt": "the whole long doc", "tags": [], "namespace": HOME},
        {"memory_key": "deploy_window", "memory_type": "semantic",
         "content_excerpt": "Deploys run Tuesdays", "tags": [], "namespace": SHARED},
        {"memory_key": "weekly_review", "memory_type": "procedural",
         "content_excerpt": "Reviews notes on Fridays", "tags": [], "namespace": HOME},
        {"memory_key": "met_bob", "memory_type": "episodic",
         "content_excerpt": "Met Bob at the conference", "tags": [], "namespace": HOME},
    ], "abstain": False}
    cat_id = fake.add_category(HOME, "preferences", "How things should be done", "# x\n" + "y" * 5000)
    bridge = await make_bridge(fake)

    out = await bridge.recall("what does alice like", limit=10, category_limit=3)

    search = fake.bodies("memory.search")[0]
    assert search["recipe"] == "fast"
    assert search["namespace_scope"] == "all_mine"
    assert search["top_k"] == 10
    assert search["include_tags"] is True
    assert search["legs"] == ["memory"]
    assert fake.bodies("namespace.search_categories")[0]["top_k"] == 3
    items = [m for m in out if m["type"] != "category"]
    cats = [m for m in out if m["type"] == "category"]
    # Loom's types read in the tools' words.
    assert items == [
        {"id": "nerve_profile_" + "a" * 20, "type": "profile", "summary": "Alice lives in Metropolis"},
        {"id": "user_coffee", "type": "profile", "summary": "Prefers black coffee"},
        {"id": "long_doc", "type": "knowledge", "summary": "a passage of the long doc"},
        {"id": f"{SHARED}/deploy_window", "type": "knowledge", "summary": "Deploys run Tuesdays"},
        {"id": "weekly_review", "type": "behavior", "summary": "Reviews notes on Fridays"},
        {"id": "met_bob", "type": "event", "summary": "Met Bob at the conference"},
    ]
    assert cats == [{"id": f"cat:{cat_id}", "type": "category", "name": "preferences",
                     "summary": "How things should be done"}]


@pytest.mark.asyncio
async def test_recall_clips_items_and_crumbs(make_bridge) -> None:
    fake = FakeLoom()
    fake.search_response = {"results": [
        {"memory_key": f"k{n}", "memory_type": "semantic", "content_excerpt": "x" * 1200, "tags": []}
        for n in range(8)
    ]}
    for n in range(6):
        fake.add_category(HOME, f"cat{n}", "", "# heading\n- " + "z" * 900)
    bridge = await make_bridge(fake)
    out = await bridge.recall("q", limit=3, category_limit=2)
    items = [m for m in out if m["type"] != "category"]
    assert [m["id"] for m in items] == ["k0", "k1", "k2"]
    assert all(len(m["summary"]) == 500 and m["summary"].endswith("…") for m in items)
    cats = [m for m in out if m["type"] == "category"]
    assert len(cats) == 2
    assert all(len(c["summary"]) <= 200 for c in cats)


@pytest.mark.asyncio
async def test_abstain_means_no_items_not_an_error(make_bridge) -> None:
    fake = FakeLoom()
    fake.search_response = {"results": [
        {"memory_key": "weak", "memory_type": "semantic", "content_excerpt": "weak match", "tags": []},
    ], "abstain": True, "abstain_code": "below_threshold"}
    bridge = await make_bridge(fake)
    assert await bridge.recall("unrelated question") == []
    result = await memory_recall_handler(_ctx(bridge), {"query": "unrelated question"})
    assert _text(result) == "No relevant memories found."


@pytest.mark.asyncio
async def test_abstain_keeps_the_topics(make_bridge) -> None:
    fake = FakeLoom()
    fake.search_response = {"results": [], "abstain": True}
    fake.add_category(HOME, "work", "Projects and deadlines")
    bridge = await make_bridge(fake)
    text = _text(await memory_recall_handler(_ctx(bridge), {"query": "q"}))
    assert text.startswith("Recalled 0 memories + 1 related topics:")


@pytest.mark.asyncio
async def test_a_refused_search_raises(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    fake.fail("memory.search", _error(422, "VALIDATION_FAILED", "query too long"))
    with pytest.raises(LoomRequestError, match="VALIDATION_FAILED"):
        await bridge.recall("x" * 10)
    assert bridge._down_until == 0.0
    fake.fail("memory.search", _error(422, "VALIDATION_FAILED", "query too long"))
    text = _text(await memory_recall_handler(_ctx(bridge), {"query": "x"}))
    assert text.startswith("Memory recall error: Loom refused memory.search (HTTP 422")


@pytest.mark.asyncio
async def test_a_failing_category_lookup_keeps_the_items(make_bridge) -> None:
    fake = FakeLoom()
    fake.add_row(HOME, "k1", "the sky is blue")
    bridge = await make_bridge(fake)
    fake.fail("namespace.search_categories", _error(400, "BAD", "no"))
    out = await bridge.recall("sky blue")
    assert [m["id"] for m in out] == ["k1"]


@pytest.mark.asyncio
async def test_recall_output_matches_memu_byte_for_byte(make_bridge, tmp_path) -> None:
    """The same memories render identically whichever backend recalled them."""
    items = [
        ("i1", "profile", "Alice lives in Metropolis"),
        ("i2", "behavior", "Prefers dark mode"),
        ("i3", "event", "Shipped the release on 2026-10-01"),
    ]
    cats = [("c1", "preferences", "How things should be done", "# preferences\n- a")]

    config = NerveConfig()
    config.memory = MemoryConfig(sqlite_dsn=f"sqlite:///{tmp_path / 'memu.sqlite'}")
    config.anthropic_api_key = "test-key"
    memu = MemUBridge(config)
    memu._available = True
    memu._service = MagicMock()
    memu._service.retrieve = AsyncMock(return_value={
        "items": [{"id": i, "memory_type": t, "summary": s} for i, t, s in items],
        "categories": [{"id": c, "name": n, "description": d, "summary": s} for c, n, d, s in cats],
    })

    fake = FakeLoom()
    fake.search_response = {"results": [
        {"memory_key": i, "memory_type": "semantic", "content_excerpt": s, "tags": [f"nerve:{t}"]}
        for i, t, s in items
    ]}
    fake.categories[(HOME, "preferences")] = {
        "category_id": "c1", "name": "preferences",
        "description": "How things should be done", "summary": "# preferences\n- a",
    }
    loom = await make_bridge(fake)

    args = {"query": "prefs", "limit": 10, "category_limit": 5}
    memu_text = _text(await memory_recall_handler(_ctx(memu), args))
    loom_text = _text(await memory_recall_handler(_ctx(loom), args))
    assert loom_text == memu_text
    assert "- [behavior] (id:i2) Prefers dark mode" in loom_text
    assert "- [preferences] (id:cat:c1) How things should be done" in loom_text


@pytest.mark.asyncio
async def test_recall_output_respects_the_size_budget(make_bridge) -> None:
    fake = FakeLoom()
    fake.search_response = {"results": [
        {"memory_key": f"k{n}", "memory_type": "semantic", "content_excerpt": "x" * 1200, "tags": []}
        for n in range(40)
    ]}
    bridge = await make_bridge(fake)
    text = _text(await memory_recall_handler(_ctx(bridge), {"query": "q", "limit": 40}))
    assert len(text.encode("utf-8")) < 10_000 + 200
    assert "truncated to fit recall size budget" in text


@pytest.mark.asyncio
async def test_session_context_uses_recall_within_the_budget(make_bridge) -> None:
    fake = FakeLoom()
    fake.add_row(HOME, "k1", "context for the deploy topic", tags=["nerve", "nerve:knowledge"])
    bridge = await make_bridge(fake)
    text = _text(await session_context_handler(_ctx(bridge), {"topic": "deploy topic"}))
    assert "- [knowledge] (id:k1) context for the deploy topic" in text
    assert fake.bodies("memory.search")[0]["top_k"] == 15
    fake.search_response = {"results": [
        {"memory_key": f"k{n}", "memory_type": "semantic", "content_excerpt": "y" * 1200, "tags": []}
        for n in range(60)
    ]}
    text = _text(await session_context_handler(_ctx(bridge), {"topic": "t", "memory_limit": 60}))
    assert "truncated to fit recall size budget" in text
    assert len(text.encode("utf-8")) < 12_000


# --------------------------------------------------------------------------- #
# Categories
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_expand_category_lists_the_tagged_rows_newest_first(make_bridge) -> None:
    fake = FakeLoom()
    cat_id = fake.add_category(HOME, "travel", "Trips")
    base = _now()
    fake.add_row(HOME, "old", "Flew to Lisbon", tags=["travel"], created_at=_iso(base - timedelta(days=2)))
    fake.add_row(HOME, "new", "Booked a train to Porto", tags=["Travel", "nerve:event"],
                 created_at=_iso(base))
    fake.add_row(HOME, "other", "Unrelated", tags=["work"])
    bridge = await make_bridge(fake)

    result = await bridge.expand_category(f"cat:{cat_id}")

    body = fake.bodies("memory.list")[0]
    assert body["tags"] == ["travel"] and body["sort_by"] == "recency"
    assert (body["agent"], body["user_id"], body["session_id"]) == ("*", "*", "*")
    assert body["preview_chars"] == 500
    assert result["name"] == "travel" and result["total"] == 2
    assert [i["id"] for i in result["items"]] == ["new", "old"]
    assert result["items"][0]["type"] == "event"

    text = _text(await memory_expand_category_handler(
        _ctx(bridge), {"category_id": f"cat:{cat_id}", "query": "porto"},
    ))
    assert fake.bodies("memory.list")[-1]["contains"] == "porto"
    assert text.startswith("Category 'travel' — showing 1 of 1 items (matching 'porto'):")


@pytest.mark.asyncio
async def test_expand_category_clips_a_long_keyword_and_says_so(make_bridge) -> None:
    fake = FakeLoom()
    cat_id = fake.add_category(HOME, "travel", "Trips")
    fake.add_row(HOME, "trip", "porto " * 60, tags=["travel"])
    bridge = await make_bridge(fake)
    keyword = "porto " * 50  # 300 characters
    text = _text(await memory_expand_category_handler(
        _ctx(bridge), {"category_id": f"cat:{cat_id}", "query": keyword},
    ))
    assert len(fake.bodies("memory.list")[-1]["contains"]) == 200
    assert "the keyword's first 200 characters" in text
    assert "showing 1 of 1 items" in text


@pytest.mark.asyncio
async def test_expand_unknown_category(make_bridge) -> None:
    bridge = await make_bridge(FakeLoom())
    assert (await bridge.expand_category("cat:nope"))["name"] is None
    text = _text(await memory_expand_category_handler(_ctx(bridge), {"category_id": "cat:nope"}))
    assert "No category found" in text


@pytest.mark.asyncio
async def test_create_category_registers_once(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    assert await bridge.create_category("health", "Doctors and habits")
    fake.categories[(HOME, "health")]["summary"] = "kept"
    assert await bridge.create_category("Health", "changed?")
    assert len(fake.bodies("namespace.register_category")) == 1
    assert fake.categories[(HOME, "health")]["summary"] == "kept"


@pytest.mark.asyncio
async def test_category_update_replaces_the_summary(make_bridge) -> None:
    fake = FakeLoom()
    audit = MagicMock()
    audit.log_audit = AsyncMock()
    cat_id = fake.add_category(HOME, "finance", "Money matters", "old summary")
    bridge = await make_bridge(fake, audit_db=audit)
    text = _text(await category_update_handler(
        _ctx(bridge), {"category_id": cat_id, "summary": "new summary"},
    ))
    assert text == f"Category {cat_id} updated and re-embedded."
    register = fake.bodies("namespace.register_category")[-1]
    assert (register["name"], register["description"], register["summary"]) == (
        "finance", "Money matters", "new summary",
    )
    assert audit.log_audit.call_args.args[4] == {"summary_changed": True, "description_changed": False}


@pytest.mark.asyncio
async def test_description_update_keeps_a_short_summary(make_bridge) -> None:
    fake = FakeLoom()
    audit = MagicMock()
    audit.log_audit = AsyncMock()
    cat_id = fake.add_category(HOME, "finance", "Money", "short summary")
    bridge = await make_bridge(fake, audit_db=audit)
    assert await bridge.update_category(f"cat:{cat_id}", description="Money and taxes")
    assert fake.categories[(HOME, "finance")] == {
        "category_id": cat_id, "name": "finance", "description": "Money and taxes",
        "summary": "short summary",
    }
    assert audit.log_audit.call_args.args[4] == {"summary_changed": False, "description_changed": True}


@pytest.mark.asyncio
async def test_description_update_refuses_to_cut_a_long_summary(make_bridge) -> None:
    fake = FakeLoom()
    cat_id = fake.add_category(HOME, "finance", "Money", "s" * 1000)
    bridge = await make_bridge(fake)
    with pytest.raises(ValueError, match="pass the summary as well"):
        await bridge.update_category(cat_id, description="Money and taxes")
    assert fake.bodies("namespace.register_category") == []
    text = _text(await category_update_handler(
        _ctx(bridge), {"category_id": cat_id, "description": "x"},
    ))
    assert text.startswith("Error:")


@pytest.mark.asyncio
async def test_description_update_counts_the_summary_in_bytes(make_bridge) -> None:
    """213 Cyrillic letters are 426 bytes, of which Loom returns 280."""
    fake = FakeLoom()
    cat_id = fake.add_category(HOME, "finance", "Money", "д" * 213)
    bridge = await make_bridge(fake)
    with pytest.raises(ValueError, match="280 bytes"):
        await bridge.update_category(cat_id, description="Money and taxes")
    assert fake.categories[(HOME, "finance")]["summary"] == "д" * 213


@pytest.mark.asyncio
async def test_description_update_refuses_a_category_search_does_not_show(make_bridge) -> None:
    fake = FakeLoom()
    cat_id = fake.add_category(HOME, "finance", "Money", "a summary to keep")
    bridge = await make_bridge(fake)
    fake.fail("namespace.search_categories", _ok({"results": []}))  # e.g. never embedded
    with pytest.raises(ValueError, match="does not show in Loom's category search"):
        await bridge.update_category(cat_id, description="Money and taxes")
    assert fake.categories[(HOME, "finance")]["summary"] == "a summary to keep"


@pytest.mark.asyncio
async def test_update_of_unknown_category_is_false(make_bridge) -> None:
    bridge = await make_bridge(FakeLoom())
    assert await bridge.update_category("missing", summary="s") is False


@pytest.mark.asyncio
async def test_apply_categories_registers_only_missing_ones(make_bridge) -> None:
    fake = FakeLoom()
    fake.add_category(HOME, "work", "already there", "keep me")
    bridge = await make_bridge(fake)
    ok = await bridge.apply_categories([
        MemoryCategoryConfig(name="work", description="new text"),
        MemoryCategoryConfig(name="family", description="People"),
    ])
    assert ok is True
    names = [b["name"] for b in fake.bodies("namespace.register_category")]
    assert names == ["family"]
    assert fake.categories[(HOME, "work")]["summary"] == "keep me"


@pytest.mark.asyncio
async def test_configured_categories_are_seeded_after_startup(make_bridge) -> None:
    fake = FakeLoom()
    config = _config()
    config.memory.categories = [MemoryCategoryConfig(name="projects", description="Work")]
    bridge = await make_bridge(fake, config=config)
    for task in list(bridge._background):
        await task
    assert (HOME, "projects") in fake.categories


@pytest.mark.asyncio
async def test_failed_seeding_is_tried_again_later(make_bridge) -> None:
    fake = FakeLoom()
    fake.fail("namespace.register_category", _error(503, "EMBED_TIMEOUT", "slow embedder"))
    config = _config()
    config.memory.categories = [MemoryCategoryConfig(name="projects", description="Work")]
    bridge = await make_bridge(fake, config=config)
    for task in list(bridge._background):
        await task
    assert (HOME, "projects") not in fake.categories
    await bridge.recall("x")
    assert not bridge._background  # not retried before the wait is over
    bridge._seed_retry_at = 0.0
    await bridge.recall("x")
    for task in list(bridge._background):
        await task
    assert (HOME, "projects") in fake.categories


# --------------------------------------------------------------------------- #
# Writes
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_memorize_tool_writes_one_fact(make_bridge) -> None:
    fake = FakeLoom()
    audit = MagicMock()
    audit.log_audit = AsyncMock()
    bridge = await make_bridge(fake, audit_db=audit)

    text = _text(await memorize_handler(
        _ctx(bridge, "web-123"), {"content": "Prefers tea over coffee", "memory_type": "profile"},
    ))

    assert text == "Memorized: Prefers tea over coffee"
    body = fake.bodies("memory.set")[0]
    assert body["value"] == "Prefers tea over coffee"
    assert body["memory_type"] == "semantic"
    assert body["tags"] == ["nerve", "nerve:profile"]
    assert body["session_id"] == "web-123"
    assert body["agent"] == "nerve"
    assert "valid_at" not in body
    assert body["memory_key"].startswith("nerve_profile_") and len(body["memory_key"]) == 34
    assert fake.headers("memory.set")[0].get("idempotency-key")
    audit.log_audit.assert_awaited()
    assert audit.log_audit.call_args.args[0] == "item_created"


@pytest.mark.asyncio
async def test_memorize_types_map_to_loom_types(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    await bridge.memorize_fact("Reviews PRs every morning", "behavior")
    await bridge.memorize_fact("Moved to Berlin", "event")
    behavior, event = fake.bodies("memory.set")
    assert behavior["memory_type"] == "procedural" and behavior["tags"] == ["nerve", "nerve:behavior"]
    assert event["memory_type"] == "semantic" and event["tags"] == ["nerve", "nerve:event"]
    assert event["valid_at"].endswith("Z")
    assert all(b["memory_type"] != "episodic" for b in fake.bodies("memory.set"))


@pytest.mark.asyncio
async def test_memorize_outage_says_not_saved(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    fake.fail("memory.set", *[httpx.ConnectError("refused")] * 3)
    text = _text(await memorize_handler(_ctx(bridge), {"content": "x"}))
    assert text.startswith("⚠️ NOT saved — MEMORY BACKEND UNAVAILABLE")


@pytest.mark.asyncio
async def test_memorize_refusal_is_an_error_not_an_outage(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    fake.fail("memory.set", _error(422, "BLACKLISTED", "matches a refuse pattern"))
    text = _text(await memorize_handler(_ctx(bridge), {"content": "secret"}))
    assert text.startswith("Error: Loom refused memory.set (HTTP 422 BLACKLISTED)")


@pytest.mark.asyncio
async def test_memorize_timeout_is_resent_with_the_same_idempotency_key(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    fake.fail("memory.set", httpx.ReadTimeout("slow"))
    assert await bridge.memorize_fact("fact", "knowledge")
    first, second = fake.headers("memory.set")
    assert first["idempotency-key"] == second["idempotency-key"]


def _messages(count: int, start: datetime, content: str = "message") -> list[dict]:
    return [
        {"role": "user" if n % 2 == 0 else "assistant", "content": f"{content} {n}",
         "created_at": (start + timedelta(minutes=n)).strftime("%Y-%m-%d %H:%M:%S")}
        for n in range(count)
    ]


@pytest.mark.asyncio
async def test_memorize_conversation_queues_batches(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    start = datetime(2026, 10, 18, 23, 0, tzinfo=UTC)
    messages = _messages(450, start) + [{"role": "user", "content": "", "created_at": None}]

    assert await bridge.memorize_conversation("sess-1", messages) is True

    first, second = fake.queued
    assert len(first["messages"]) == 400 and len(second["messages"]) == 50
    for batch in (first, second):
        assert batch["defer"] is True
        assert batch["extra_tags"] == ["source:nerve"]
        assert batch["session_id"] == "sess-1"
        assert batch["agent"] == "nerve"
    assert first["messages"][0] == {
        "role": "user", "content": "message 0", "timestamp": "2026-10-18T23:00:00Z",
    }
    assert "last_messages" not in first
    assert second["last_messages"] == first["messages"][-10:]
    assert first["observation_date"] == "2026-10-19"  # its last turn's day


@pytest.mark.asyncio
async def test_extraction_batches_stay_within_the_byte_budget(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake, extraction_batch_bytes=128 * 1024)
    messages = _messages(60, datetime(2026, 10, 18, tzinfo=UTC), content="w" * 8000)
    messages.append({"role": "user", "content": "é" * 300_000, "created_at": "2026-10-18 02:00:00"})
    await bridge.memorize_conversation("sess-1", messages)
    assert len(fake.queued) >= 6
    assert max(fake.sizes["memory.set_from_messages"]) <= 128 * 1024
    assert fake.queued[-1]["messages"][-1]["content"].endswith("[…truncated]")
    sent = [m["content"] for b in fake.queued for m in b["messages"]]
    assert sent[:60] == [f"{'w' * 8000} {n}" for n in range(60)]  # all of them, in order


@pytest.mark.asyncio
async def test_context_turns_are_clipped(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    messages = _messages(401, datetime(2026, 10, 18, tzinfo=UTC))
    messages[399]["content"] = "x" * 5000
    await bridge.memorize_conversation("sess-1", messages)
    first, second = fake.queued
    assert len(first["messages"][399]["content"]) == 5000  # extracted whole
    context = second["last_messages"][-1]
    assert context["content"].endswith("[…truncated]")
    assert _compact(context) <= 2000


@pytest.mark.asyncio
async def test_memorize_conversation_raises_and_does_not_requeue_accepted_batches(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    messages = _messages(450, datetime(2026, 10, 18, tzinfo=UTC))
    # The second batch fails: no response, so it was never accepted.
    fake.fail("memory.set_from_messages", _ok({"deferred": True, "queue_id": "q-first"}),
              *[httpx.ConnectError("refused")] * 3)
    with pytest.raises(MemoryBackendUnavailable):
        await bridge.memorize_conversation("sess-1", messages)
    bridge._down_until = 0.0  # the breaker would hold the retry back
    assert await bridge.memorize_conversation("sess-1", messages) is True
    sent = fake.bodies("memory.set_from_messages")
    assert [len(b["messages"]) for b in sent] == [400, 50, 50, 50, 50]
    assert len(fake.queued) == 1 and len(fake.queued[0]["messages"]) == 50


@pytest.mark.asyncio
async def test_extraction_timeout_is_not_resent(make_bridge) -> None:
    """set_from_messages is not idempotent: a timed-out batch may have been queued."""
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    fake.fail("memory.set_from_messages", httpx.ReadTimeout("slow"))
    with pytest.raises(MemoryBackendUnavailable, match="did not answer"):
        await bridge.memorize_conversation("s", _messages(3, _now()))
    assert len(fake.bodies("memory.set_from_messages")) == 1


@pytest.mark.asyncio
async def test_nothing_to_send_is_false(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    assert await bridge.memorize_conversation("s", []) is False
    assert await bridge.memorize_conversation("s", [{"role": "user", "content": "  "}]) is False
    assert fake.queued == []


@pytest.mark.asyncio
async def test_memorize_conversation_raises_when_the_bridge_is_down(make_bridge) -> None:
    """The sweep moves its watermark unless this raises."""
    bridge = await make_bridge(FakeLoom())
    await bridge.shutdown()
    with pytest.raises(MemoryBackendUnavailable, match="not running"):
        await bridge.memorize_conversation("s", _messages(2, _now()))
    assert await bridge.memorize_conversation("s", []) is False


def _engine_with(bridge):
    from nerve.agent.engine import AgentEngine
    from tests.actor_rows import mock_system_principal

    config = MagicMock()
    config.sessions.sticky_period_minutes = 5
    config.agent.max_concurrent = 3
    config.mcp_servers = []
    db = AsyncMock()
    mock_system_principal(db)
    engine = AgentEngine(config, db)
    engine._memory_bridge = bridge
    engine.db.get_session = AsyncMock(return_value={
        "connected_at": "2026-01-01T00:00:00+00:00", "last_memorized_at": None,
    })
    engine.db.get_messages = AsyncMock(return_value=[
        {"created_at": "2026-01-01 00:05:00", "role": "user", "content": "I moved to Berlin"},
    ])
    engine.db.get_sessions_needing_memorization = AsyncMock(return_value=[])
    return engine


@pytest.mark.asyncio
async def test_sweep_watermark_holds_while_loom_is_down(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    fake.fail("memory.set_from_messages", *[_error(503, "BACKEND_UNAVAILABLE")] * 3)
    engine = _engine_with(bridge)
    await engine._memorize_session("s1")
    engine.db.update_session_fields.assert_not_awaited()


@pytest.mark.asyncio
async def test_sweep_watermark_advances_once_loom_took_the_window(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    engine = _engine_with(bridge)
    await engine._memorize_session("s1")
    engine.db.update_session_fields.assert_awaited_once_with(
        "s1", {"last_memorized_at": "2026-01-01 00:05:00"},
    )
    assert fake.queued[0]["messages"][0]["content"] == "I moved to Berlin"


@pytest.mark.asyncio
async def test_memorize_file_parses_the_type_prefix(make_bridge, tmp_path) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    path = tmp_path / "memorize-1.txt"
    path.write_text("behavior: Writes tests first", encoding="utf-8")
    assert await bridge.memorize_file(str(path))
    body = fake.bodies("memory.set")[0]
    assert body["value"] == "Writes tests first" and body["tags"] == ["nerve", "nerve:behavior"]


# --------------------------------------------------------------------------- #
# Update and delete
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_update_content_is_no_clobber(make_bridge) -> None:
    fake = FakeLoom()
    row = fake.add_row(HOME, "user_city", "Lives in Paris", tags=["source:nerve"])
    bridge = await make_bridge(fake)
    text = _text(await memory_update_handler(
        _ctx(bridge), {"memory_id": "user_city", "content": "Lives in Lyon"},
    ))
    assert text == "Memory user_city updated."
    update = fake.bodies("memory.update_by_id")[0]
    assert update["memory_id"] == row["memory_id"]
    assert update["value"] == "Lives in Lyon"
    assert update["if_match_sha256"] == _sha("Lives in Paris")
    assert "tags" not in update  # unchanged
    assert fake.rows[(HOME, "user_city")]["value"] == "Lives in Lyon"


@pytest.mark.asyncio
async def test_update_rereads_after_a_concurrent_write(make_bridge) -> None:
    fake = FakeLoom()
    fake.add_row(HOME, "k", "v1")
    bridge = await make_bridge(fake)
    fake.fail("memory.update_by_id", _error(409, "PRECONDITION_FAILED", "changed"))
    assert await bridge.update_item("k", content="v2")
    assert len(fake.bodies("memory.get")) == 2
    assert fake.rows[(HOME, "k")]["value"] == "v2"


@pytest.mark.asyncio
async def test_update_type_and_categories(make_bridge) -> None:
    fake = FakeLoom()
    fake.add_category(HOME, "work", "Work")
    fake.add_category(HOME, "travel", "Trips")
    fake.add_row(HOME, "nerve_knowledge_" + "b" * 20, "Conference in Lisbon",
                 tags=["nerve", "nerve:knowledge", "work"])
    bridge = await make_bridge(fake)

    text = _text(await memory_update_handler(_ctx(bridge), {
        "memory_id": "nerve_knowledge_" + "b" * 20,
        "memory_type": "event",
        "categories": "Travel, unknown-category",
    }))

    assert text.endswith("updated.")
    row = fake.rows[(HOME, "nerve_knowledge_" + "b" * 20)]
    assert sorted(row["tags"]) == ["nerve", "nerve:event", "travel"]
    links = {(link[1], link[2]) for link in fake.links}
    travel_id = fake.categories[(HOME, "travel")]["category_id"]
    assert links == {("nerve_knowledge_" + "b" * 20, travel_id)}
    assigns = fake.bodies("memory.assign_category")
    assert {(a["category_name"], a["tombstone"]) for a in assigns} == {("travel", False), ("work", True)}


@pytest.mark.asyncio
async def test_dropping_a_capitalised_category_names_it_as_registered(make_bridge) -> None:
    fake = FakeLoom()
    fake.add_category(HOME, "Work Projects", "Work")
    fake.add_category(HOME, "Travel", "Trips")
    fake.add_row(HOME, "k", "Conference in Lisbon", tags=["Work Projects"])  # tags are lowercase
    bridge = await make_bridge(fake)
    assert await bridge.update_item("k", categories=["travel"])
    assigns = {(a["category_name"], a["tombstone"]) for a in fake.bodies("memory.assign_category")}
    assert assigns == {("Travel", False), ("Work Projects", True)}
    assert fake.rows[(HOME, "k")]["tags"] == ["travel"]


@pytest.mark.asyncio
async def test_update_by_loom_memory_id(make_bridge) -> None:
    fake = FakeLoom()
    row = fake.add_row(SHARED, "team_rule", "Deploy on Tuesdays", agent="other-agent", session_id="x")
    bridge = await make_bridge(fake)
    assert await bridge.update_item(row["memory_id"], content="Deploy on Thursdays")
    get = fake.bodies("memory.get")[0]
    assert (get["namespace"], get["agent"], get["session_id"]) == (SHARED, "other-agent", "x")
    assert fake.rows[(SHARED, "team_rule")]["value"] == "Deploy on Thursdays"


@pytest.mark.asyncio
async def test_update_of_a_missing_memory(make_bridge) -> None:
    bridge = await make_bridge(FakeLoom())
    text = _text(await memory_update_handler(_ctx(bridge), {"memory_id": "nope", "content": "x"}))
    assert text == "Failed to update memory nope."


@pytest.mark.asyncio
async def test_delete_by_key_and_by_qualified_id(make_bridge) -> None:
    fake = FakeLoom()
    fake.add_row(HOME, "k1", "one")
    fake.add_row(SHARED, "k2", "two")
    bridge = await make_bridge(fake)

    assert _text(await memory_delete_handler(_ctx(bridge), {"memory_id": "k1"})) == "Memory k1 deleted."
    assert (HOME, "k1") not in fake.rows
    first = fake.bodies("memory.delete")[0]
    assert (first["namespace"], first["memory_key"], first["agent"]) == (HOME, "k1", "nerve")

    assert await bridge.delete_item(f"{SHARED}/k2") is True
    assert (SHARED, "k2") not in fake.rows

    text = _text(await memory_delete_handler(_ctx(bridge), {"memory_id": "gone"}))
    assert text == "Failed to delete memory gone."


@pytest.mark.asyncio
async def test_delete_by_loom_memory_id_uses_the_row_identity(make_bridge) -> None:
    fake = FakeLoom()
    row = fake.add_row(SHARED, "k", "v", agent="writer-agent", session_id="sess-x")
    bridge = await make_bridge(fake)
    assert await bridge.delete_item(row["memory_id"])
    body = fake.bodies("memory.delete")[0]
    assert (body["namespace"], body["agent"], body["session_id"], body["memory_key"]) == (
        SHARED, "writer-agent", "sess-x", "k",
    )


# --------------------------------------------------------------------------- #
# Date tools
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_conversation_history_reads_the_timeline(make_bridge) -> None:
    fake = FakeLoom()
    fake.add_row(HOME, "trip", "Flew to Lisbon", tags=["source:nerve"],
                 valid_at="2026-10-18T09:00:00Z", created_at="2026-10-19T08:00:00Z")
    fake.add_row(HOME, "nerve_event_" + "c" * 20, "Signed the lease", tags=["nerve", "nerve:event"],
                 valid_at="2026-10-18T15:00:00Z", created_at="2026-10-18T15:00:00Z")
    fake.add_row(HOME, "undated", "Likes jazz", tags=["nerve", "nerve:profile"],
                 created_at="2026-10-18T10:00:00Z")
    fake.add_row(HOME, "elsewhere", "Not Nerve", tags=["other"], valid_at="2026-10-18T10:00:00Z")
    fake.add_row(HOME, "later", "Later event", tags=["source:nerve"], valid_at="2026-10-20T10:00:00Z")
    bridge = await make_bridge(fake)

    text = _text(await conversation_history_handler(_ctx(bridge), {"date": "2026-10-18"}))

    body = fake.bodies("memory.timeline")[0]
    assert body["tags"] == ["nerve", "source:nerve"]
    assert body["since"] == "2026-10-18T00:00:00Z" and body["until"] == "2026-10-18T23:59:59Z"
    assert body["order"] == "desc" and body["agent"] == "*" and body["top_k"] == 500
    assert text == (
        "Memories from 2026-10-18 (2 items):\n\n"
        f"- [event] (id:nerve_event_{'c' * 20}) Signed the lease\n"
        "- [knowledge] (id:trip) Flew to Lisbon"
    )


@pytest.mark.asyncio
async def test_conversation_history_pages_past_undated_rows(make_bridge) -> None:
    fake = FakeLoom()
    day = datetime(2026, 10, 18, 23, 0, tzinfo=UTC)
    for n in range(520):  # recent and undated: they fill the first page
        fake.add_row(HOME, f"u{n}", f"undated {n}", tags=["nerve"], created_at=_iso(day - timedelta(seconds=n)))
    for n in range(5):
        fake.add_row(HOME, f"d{n}", f"dated {n}", tags=["source:nerve"],
                     valid_at=_iso(day - timedelta(hours=10 + n)))
    bridge = await make_bridge(fake)
    rows = await bridge.conversation_history("2026-10-18", limit=10)
    assert [r["id"] for r in rows] == [f"d{n}" for n in range(5)]
    timeline = fake.bodies("memory.timeline")
    assert len(timeline) == 2
    assert timeline[1]["until"] == _iso(day - timedelta(seconds=499))  # the oldest row seen


@pytest.mark.asyncio
async def test_conversation_history_stops_at_the_limit(make_bridge) -> None:
    fake = FakeLoom()
    day = datetime(2026, 10, 18, 20, 0, tzinfo=UTC)
    for n in range(30):
        fake.add_row(HOME, f"d{n}", f"dated {n}", tags=["source:nerve"],
                     valid_at=_iso(day - timedelta(minutes=n)))
    bridge = await make_bridge(fake)
    rows = await bridge.conversation_history("2026-10-18", limit=7)
    assert [r["id"] for r in rows] == [f"d{n}" for n in range(7)]
    assert len(fake.bodies("memory.timeline")) == 1


@pytest.mark.asyncio
async def test_conversation_history_bad_date(make_bridge) -> None:
    bridge = await make_bridge(FakeLoom())
    text = _text(await conversation_history_handler(_ctx(bridge), {"date": "yesterday"}))
    assert text.startswith("Error: dates must be YYYY-MM-DD")


@pytest.mark.asyncio
async def test_records_by_date_reads_created_at(make_bridge) -> None:
    fake = FakeLoom()
    fake.add_row(HOME, "new", "Created today", tags=["source:nerve"], created_at="2026-10-20T10:00:00Z")
    fake.add_row(HOME, "dated", "About last year", tags=["source:nerve"],
                 created_at="2026-10-20T09:00:00Z", valid_at="2025-01-01T00:00:00Z")
    fake.add_row(HOME, "rewritten", "Updated today", tags=["nerve", "nerve:profile"],
                 created_at="2026-10-01T10:00:00Z", updated_at="2026-10-20T11:00:00Z")
    fake.add_row(HOME, "old", "Old", tags=["nerve"], created_at="2026-09-01T10:00:00Z")
    fake.add_row(HOME, "foreign", "Other tool", tags=["other"], created_at="2026-10-20T10:00:00Z")
    bridge = await make_bridge(fake)

    text = _text(await memory_records_by_date_handler(_ctx(bridge), {"date": "2026-10-20"}))
    assert text == (
        "Records from 2026-10-20 (2 items):\n\n"
        "- [knowledge] (id:new) Created today\n"
        "- [knowledge] (id:dated) About last year"
    )
    body = fake.bodies("memory.list")[0]
    assert body["tags"] == ["nerve", "source:nerve"] and body["sort_by"] == "recency"
    assert (body["agent"], body["user_id"], body["session_id"]) == ("*", "*", "*")
    assert body["preview_chars"] == 500

    text = _text(await memory_records_by_date_handler(
        _ctx(bridge), {"date": "2026-10-20", "updated": True},
    ))
    assert "- [profile] (id:rewritten) Updated today (updated)" in text
    assert "(id:old)" not in text


@pytest.mark.asyncio
async def test_records_by_date_pages_and_stops_at_the_window(make_bridge) -> None:
    fake = FakeLoom()
    base = datetime(2026, 10, 20, 23, 0, tzinfo=UTC)
    for n in range(700):
        fake.add_row(HOME, f"k{n:04d}", f"fact {n}", tags=["nerve"],
                     created_at=_iso(base - timedelta(minutes=2 * n)))
    bridge = await make_bridge(fake)
    rows = await bridge.records_by_date("2026-10-20", limit=1000)
    assert len(rows) == 691  # 23:00 back to 00:00, inclusive, in 2-minute steps
    assert rows[-1]["created_at"] == "2026-10-20T00:00:00Z"
    # The second page reaches the day before, which ends the scan.
    assert [b["offset"] for b in fake.bodies("memory.list")] == [0, 500]


# --------------------------------------------------------------------------- #
# Health, the dashboard link, routes
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_health_shape(make_bridge) -> None:
    fake = FakeLoom()
    fake.add_row(HOME, "a", "x", memory_type="semantic")
    fake.add_row(HOME, "b", "y", memory_type="procedural")
    fake.add_category(HOME, "work")
    bridge = await make_bridge(fake)
    await bridge.recall("x")

    health = await bridge.get_health()

    assert health["backend"] == "loom"
    assert health["service_available"] is True
    assert health["dashboard_url"] == f"http://loom.test/memories?namespace={HOME}"
    assert health["database"]["total_items"] == 2
    assert health["database"]["type_distribution"] == {"semantic": 1, "procedural": 1}
    assert health["database"]["total_categories"] == 1
    assert health["operations"]["recall"]["call_count"] == 1
    assert health["loom"]["namespace"] == HOME and health["loom"]["identity_source"] == "whoami"
    assert TOKEN not in json.dumps(health)


@pytest.mark.asyncio
async def test_health_when_loom_is_down(make_bridge) -> None:
    fake = FakeLoom()
    bridge = await make_bridge(fake)
    fake.fail("namespace.summary", httpx.ConnectError("refused"))
    health = await bridge.get_health()
    assert health["service_available"] is False
    assert "cannot reach Loom" in health["loom"]["error"]
    assert health["database"]["total_items"] == 0
    assert len(fake.bodies("namespace.summary")) == 1  # one attempt only


@pytest.mark.parametrize(("base_url", "dashboard_url", "expected"), [
    # A local proxy's address is no link for a browser.
    ("http://10.0.0.5:3000", "", ""),
    ("http://127.0.0.1:7777", "", ""),
    ("http://localhost:7777", "", ""),
    ("http://169.254.10.1:7777", "", ""),
    ("http://[::1]:7777", "", ""),
    ("http://10.0.0.5:3000", "https://loom.example.test/",
     f"https://loom.example.test/memories?namespace={HOME}"),
    ("https://loom.example.test", "", f"https://loom.example.test/memories?namespace={HOME}"),
])
@pytest.mark.asyncio
async def test_dashboard_link(make_bridge, base_url, dashboard_url, expected) -> None:
    bridge = await make_bridge(FakeLoom(), base_url=base_url, dashboard_url=dashboard_url)
    assert bridge.dashboard_url == expected


@pytest.mark.asyncio
async def test_snapshot_route_degrades_for_loom(make_bridge, monkeypatch) -> None:
    from nerve.gateway.routes import memory as routes

    bridge = await make_bridge(FakeLoom())
    deps = SimpleNamespace(engine=SimpleNamespace(_memory_bridge=bridge))
    monkeypatch.setattr(routes, "get_deps", lambda: deps)

    snapshot = await routes.get_memu_data()
    assert snapshot["available"] is False
    assert snapshot["backend"] == "loom"
    assert snapshot["reason"] == "not available for this backend"
    assert snapshot["dashboard_url"] == f"http://loom.test/memories?namespace={HOME}"

    health = await routes.memu_health()
    assert health["backend"] == "loom"


@pytest.mark.asyncio
async def test_mutation_routes_answer_503_when_loom_is_down(make_bridge, monkeypatch) -> None:
    from fastapi import HTTPException

    from nerve.gateway.routes import memory as routes

    fake = FakeLoom()
    bridge = await make_bridge(fake)
    deps = SimpleNamespace(engine=SimpleNamespace(_memory_bridge=bridge))
    monkeypatch.setattr(routes, "get_deps", lambda: deps)
    fake.fail("memory.delete", *[httpx.ConnectError("refused")] * 3)
    with pytest.raises(HTTPException) as err:
        await routes.delete_memu_item("k1")
    assert err.value.status_code == 503


@pytest.mark.asyncio
async def test_token_never_reaches_the_log(make_bridge, caplog) -> None:
    fake = FakeLoom(whoami_status=500)
    with caplog.at_level("DEBUG"):
        bridge = await make_bridge(fake)
        with pytest.raises(MemoryBackendUnavailable):
            await bridge.recall("x")
    assert TOKEN not in caplog.text


def test_request_error_carries_loom_code() -> None:
    err = LoomRequestError("memory.set", 409, "PINNED_CONFLICT", "pinned")
    assert err.code == "PINNED_CONFLICT" and err.status == 409
    assert "memory.set" in str(err) and "PINNED_CONFLICT" in str(err)
