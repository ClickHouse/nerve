"""The Loom ops Nerve calls, held to Loom's own op schemas.

``tests/fixtures/loom_lifecycle_ops.json`` is generated from a Loom checkout
by ``scripts/loom_contract_fixture.py``, never edited by hand. These tests
hold ``nerve/memory/loom.py`` to it: each op is served on MCP by the tool
Nerve calls, every argument Nerve sends on the wire exists in Loom's request
schema with the type Loom declares, and every result field Nerve reads is one
Loom answers with.
"""

from __future__ import annotations

import importlib.util
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

from nerve.memory.loom import LOOM_CALLS, LOOM_OP, LoomCall, LoomMemory
from tests.test_loom_memory import HOME, ORG, FakeGateway, _config, _messages

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = json.loads((ROOT / "tests" / "fixtures" / "loom_lifecycle_ops.json").read_text())
OPS = FIXTURE["ops"]
REGENERATE = "regenerate with scripts/loom_contract_fixture.py"


def _errors(value: Any, schema: dict, path: str = "") -> list[str]:
    """Where ``value`` breaks ``schema`` (the fixture's JSON-Schema subset)."""
    if "anyOf" in schema:
        if any(not _errors(value, option, path) for option in schema["anyOf"]):
            return []
        return [f"{path or 'value'}: {value!r} matches no type Loom allows"]
    if "enum" in schema:
        return [] if value in schema["enum"] else [f"{path}: {value!r} is not one of {schema['enum']}"]
    kind = schema.get("type")
    if kind is None:
        return []
    is_number = isinstance(value, int | float) and not isinstance(value, bool)
    matches = {
        "string": isinstance(value, str),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "number": is_number,
        "boolean": isinstance(value, bool),
        "null": value is None,
        "array": isinstance(value, list),
        "object": isinstance(value, dict),
    }[kind]
    if not matches:
        return [f"{path or 'value'}: {value!r} is not {kind}"]
    errors: list[str] = []
    if kind == "array":
        for index, item in enumerate(value):
            errors += _errors(item, schema.get("items", {}), f"{path}[{index}]")
    elif kind == "object":
        properties = schema.get("properties")
        errors += [f"{path}: {key} is missing" for key in schema.get("required", []) if key not in value]
        for key, item in value.items():
            if properties is not None and key in properties:
                errors += _errors(item, properties[key], f"{path}.{key}")
            elif "additionalProperties" in schema:
                errors += _errors(item, schema["additionalProperties"], f"{path}.{key}")
            elif properties is not None:
                errors.append(f"{path}: Loom takes no {key}")
    return errors


def test_the_fixture_describes_the_ops_nerve_calls() -> None:
    assert set(OPS) == set(LOOM_CALLS), REGENERATE


@pytest.mark.parametrize("op", sorted(LOOM_CALLS))
def test_each_op_is_called_through_the_tool_that_serves_it(op) -> None:
    """A listed op by its own tool (``loom_search`` for search.query);
    one the core toolset does not list through ``loom_op``."""
    assert LOOM_CALLS[op].tool == OPS[op]["tool"], REGENERATE
    assert OPS[op]["listed"] is (OPS[op]["tool"] != LOOM_OP)


@pytest.mark.parametrize("op", sorted(LOOM_CALLS))
def test_nerve_names_only_fields_loom_has(op) -> None:
    call = LOOM_CALLS[op]
    assert call.fields <= set(OPS[op]["request"]["properties"]), REGENERATE
    assert set(OPS[op]["request"]["required"]) <= call.fields
    assert call.reads <= set(OPS[op]["result"]["properties"]), REGENERATE


def test_only_reads_are_retried() -> None:
    """A call Nerve sends again after a lost answer must be one Loom marks
    idempotent: queueing a transcript twice extracts it twice."""
    from nerve.memory.loom import _RETRIED_OPS

    assert {op for op in _RETRIED_OPS if not OPS[op]["idempotent"]} == set()
    assert not OPS["memory.set_from_messages"]["idempotent"]


@pytest_asyncio.fixture
async def wired():
    """The backend with the settings that make Nerve name the org and the
    namespace, against a fake gateway."""
    gateway = FakeGateway()
    memory = LoomMemory(
        _config(org=ORG, namespace=HOME), transport=gateway.transport(), health_interval=None,
    )
    assert await memory.initialize()
    yield gateway, memory
    await memory.shutdown()


@pytest.mark.asyncio
async def test_the_calls_on_the_wire_match_loom_schemas(wired) -> None:
    """Every lifecycle call, the startup recall's fallback search and a
    window long enough to carry context turns included."""
    gateway, memory = wired
    gateway.add_row(HOME, "editor", "Prefers Helix as the editor")

    await memory.startup_recall("which editor do I use", limit=5)
    await memory.startup_recall("nothing matches this", limit=5)  # the search fallback
    await memory.memorize_conversation("sess-1", _messages(450, datetime(2026, 10, 18, tzinfo=UTC)))
    await memory.get_health()

    seen: set[str] = set()
    for tool, arguments, _ in gateway.calls:
        if tool == LOOM_OP:
            op, arguments = arguments["op"], arguments["arguments"]
        else:
            op = next(name for name, call in LOOM_CALLS.items() if call.tool == tool)
        seen.add(op)
        assert _errors(arguments, OPS[op]["request"], op) == []
    assert seen == set(LOOM_CALLS)
    queued = gateway.args("loom_memory_set_from_messages")
    assert "last_messages" in queued[-1] and queued[-1]["org"] == ORG


@pytest.mark.parametrize(("value", "broken"), [
    ({"query": "q", "top_k": "5"}, "top_k"),
    ({"query": "q", "mode": "warm"}, "mode"),
    ({"messages": [{"role": "user"}]}, "content is missing"),
    ({"query": "q", "agent": "someone"}, "Loom takes no agent"),
    ({"top_k": 5}, "query is missing"),
])
def test_the_check_catches_what_loom_would_refuse(value, broken) -> None:
    """The schema check itself is not vacuous."""
    schema = {
        "type": "object",
        "properties": {
            **OPS["search.query"]["request"]["properties"],
            "mode": OPS["context.pack"]["request"]["properties"]["mode"],
            "messages": OPS["memory.set_from_messages"]["request"]["properties"]["messages"],
        },
        "required": ["query"] if "messages" not in value else [],
    }
    assert any(broken in error for error in _errors(value, schema))


# --------------------------------------------------------------------------- #
# The generator, on sources of its own
# --------------------------------------------------------------------------- #

_OPS_SOURCE = '''
from dataclasses import dataclass
from typing import Literal, NotRequired, Optional, Required, TypedDict

@dataclass(frozen=True)
class OpSpec:
    name: str
    input: str
    body: str
    output: str
    idempotent: bool
    role: str
    maturity: str
    budget_p99_ms: int
    audit: bool
    read_only: bool

OPS: dict[str, OpSpec] = {
    "a.read": OpSpec("a.read", "AReadInput", "AReadBody", "AReadOutput",
                     True, "viewer", "stable", 10, False, True),
    "a.hidden": OpSpec("a.hidden", "AReadInput", "AReadBody", "AReadOutput",
                       True, "viewer", "stable", 10, False, True),
    "a.unlisted": OpSpec("a.unlisted", "AReadInput", "AReadBody", "AReadOutput",
                         True, "viewer", "stable", 10, False, True),
}

class Item(TypedDict, total=False):
    text: Required[str]
    when: NotRequired[Optional[str]]

class AReadBody(TypedDict, total=False):
    org: NotRequired[str]
    query: Required[str]
    mode: NotRequired[Literal["x", "y"]]
    items: NotRequired[list["Item"]]

class AReadOutput(TypedDict, total=False):
    found: Required[dict[str, int]]
'''

_TOOLSETS_SOURCE = '''
_MCP_HIDDEN_OPS: frozenset[str] = frozenset({"a.hidden"})
_MCP_CORE_OPS: frozenset[str] = frozenset({"a.read"})
SYNTHETIC_TOOL_OPS: frozenset[str] = frozenset({"search.query"})
'''


@pytest.fixture
def generator(monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "loom_contract_fixture", ROOT / "scripts" / "loom_contract_fixture.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def build(**calls: LoomCall):
        monkeypatch.setattr(module, "LOOM_CALLS", {k.replace("_", "."): v for k, v in calls.items()})
        return module.build(_OPS_SOURCE, _TOOLSETS_SOURCE)

    return build


def test_the_generator_describes_what_nerve_uses(generator) -> None:
    fixture, errors = generator(
        a_read=LoomCall("loom_a_read", frozenset({"query", "items"}), frozenset({"found"})),
        a_unlisted=LoomCall(LOOM_OP, frozenset({"query"}), frozenset()),
    )
    assert errors == []
    read = fixture["ops"]["a.read"]
    assert read["tool"] == "loom_a_read" and read["listed"] is True and read["role"] == "viewer"
    assert read["request"] == {
        "type": "object",
        "properties": {
            "items": {"type": "array", "items": {
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "when": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                },
                "required": ["text"],
            }},
            "query": {"type": "string"},
        },
        "required": ["query"],
    }
    assert read["result"]["properties"] == {
        "found": {"type": "object", "additionalProperties": {"type": "integer"}},
    }
    assert fixture["ops"]["a.unlisted"]["tool"] == LOOM_OP
    assert fixture["ops"]["a.unlisted"]["listed"] is False


def test_the_generator_refuses_what_loom_does_not_serve(generator) -> None:
    _, errors = generator(
        a_hidden=LoomCall(LOOM_OP, frozenset({"query"}), frozenset()),
        a_read=LoomCall("loom_a_read", frozenset({"query", "agent"}), frozenset({"gone"})),
        a_missing=LoomCall("loom_a_missing", frozenset(), frozenset()),
    )
    assert errors == [
        "Loom keeps a.hidden off MCP: no tool, loom_op included, runs it",
        "Loom has no op a.missing",
        "Loom's a.read takes no agent",
        "Loom's a.read answers no gone",
    ]
