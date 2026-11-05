#!/usr/bin/env python3
"""Write tests/fixtures/loom_lifecycle_ops.json from Loom's own op schemas.

The Loom memory backend (nerve/memory/loom.py) calls a few Loom ops through
the MCP gateway, listed in its ``LOOM_CALLS``. tests/test_loom_contract.py
holds those calls to this fixture, which is generated from a Loom checkout
rather than typed by hand. Run it again whenever LOOM_CALLS or Loom changes:

    python scripts/loom_contract_fixture.py --loom ../loom --ref origin/main

It reads two files of the checkout (with ``git show <ref>:<path>`` when
``--ref`` is given, so the checkout need not be on that ref):

* ``sdk/python/loom/ops.py``: each op's spec and the TypedDicts of its
  request body and its output;
* ``server/operations/toolsets.py``: which ops the default (``core``) MCP
  toolset lists, which ops MCP keeps off entirely, and which ops have a
  synthetic tool of their own.

Only what Nerve uses is written: the ops in LOOM_CALLS, the request fields
Nerve may send plus the ones Loom requires (a field with a fixed set of
values lists only the ones Nerve sends), the result fields Nerve reads
(an object or list of objects without its fields, unless Nerve reads them),
and the tools the agent-facing wording names
(``nerve.memory.loom_prompts.tool_names``). It fails, writing nothing, when
Nerve calls an op Loom does not have or does not serve on MCP, names a field
or a value Loom does not know, or tells agents about a tool Loom does not
list by default.
"""

from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "loom_lifecycle_ops.json"
OPS_FILE = "sdk/python/loom/ops.py"
TOOLSETS_FILE = "server/operations/toolsets.py"
# The tool name MCP gives an op with a synthetic tool (Loom's MCP reference).
SYNTHETIC_TOOLS = {"search.query": "loom_search"}
# The listed tool that runs an op the toolset does not list.
LOOM_OP = "loom_op"
# Tools every toolset lists that run no op of their own.
META_TOOLS = (LOOM_OP, "loom_activate")

sys.path.insert(0, str(ROOT))
from nerve.memory.loom import LOOM_CALLS
from nerve.memory.loom_prompts import tool_names


def read_source(loom: Path, path: str, ref: str | None) -> str:
    if ref:
        return subprocess.run(
            ["git", "-C", str(loom), "show", f"{ref}:{path}"],
            check=True, capture_output=True, text=True,
        ).stdout
    return (loom / path).read_text(encoding="utf-8")


class Schemas:
    """The op specs and TypedDicts of Loom's generated SDK module."""

    def __init__(self, source: str) -> None:
        tree = ast.parse(source)
        self.classes: dict[str, ast.ClassDef] = {
            node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
        }
        spec_fields = [
            item.target.id for item in self.classes["OpSpec"].body
            if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name)
        ]
        self.ops: dict[str, dict[str, Any]] = {}
        for node in tree.body:
            if (
                isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
                and node.target.id == "OPS" and isinstance(node.value, ast.Dict)
            ):
                for key, call in zip(node.value.keys, node.value.values, strict=True):
                    assert isinstance(key, ast.Constant) and isinstance(call, ast.Call)
                    values = [ast.literal_eval(arg) for arg in call.args]
                    self.ops[key.value] = dict(zip(spec_fields, values, strict=True))
        if not self.ops:
            raise SystemExit(f"no OPS table in {OPS_FILE}")

    def fields(self, class_name: str) -> dict[str, tuple[ast.expr, bool]]:
        """A TypedDict's fields: name -> (type annotation, required)."""
        node = self.classes[class_name]
        total = next(
            (ast.literal_eval(k.value) for k in node.keywords if k.arg == "total"), True,
        )
        out: dict[str, tuple[ast.expr, bool]] = {}
        for item in node.body:
            if not (isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name)):
                continue
            annotation, required = item.annotation, total
            if (
                isinstance(annotation, ast.Subscript) and isinstance(annotation.value, ast.Name)
                and annotation.value.id in ("Required", "NotRequired")
            ):
                required = annotation.value.id == "Required"
                annotation = annotation.slice
            out[item.target.id] = (annotation, required)
        return out

    def schema(self, node: ast.expr, depth: int = 0, *, expand: bool = True) -> dict[str, Any]:
        """A JSON-Schema-like description of a type annotation. Without
        ``expand`` a TypedDict is an object whose fields are not described."""
        if depth > 6:
            raise SystemExit("type nesting too deep")
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return self.schema(ast.parse(node.value, mode="eval").body, depth + 1, expand=expand)
        if isinstance(node, ast.Constant) and node.value is None:
            return {"type": "null"}
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            return {"anyOf": [
                self.schema(node.left, depth + 1, expand=expand),
                self.schema(node.right, depth + 1, expand=expand),
            ]}
        if isinstance(node, ast.Name):
            simple = {
                "str": {"type": "string"}, "int": {"type": "integer"},
                "float": {"type": "number"}, "bool": {"type": "boolean"}, "Any": {},
            }
            if node.id in simple:
                return simple[node.id]
            if node.id in self.classes:
                return self.object_schema(node.id, None, depth + 1) if expand else {"type": "object"}
            raise SystemExit(f"unknown type {node.id}")
        if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name):
            args = node.slice.elts if isinstance(node.slice, ast.Tuple) else [node.slice]
            kind = node.value.id
            if kind == "Optional":
                return {"anyOf": [self.schema(args[0], depth + 1, expand=expand), {"type": "null"}]}
            if kind == "Union":
                return {"anyOf": [self.schema(a, depth + 1, expand=expand) for a in args]}
            if kind == "list":
                return {"type": "array", "items": self.schema(args[0], depth + 1, expand=expand)}
            if kind == "dict":
                return {
                    "type": "object",
                    "additionalProperties": self.schema(args[1], depth + 1, expand=expand),
                }
            if kind == "Literal":
                return {"enum": [ast.literal_eval(a) for a in args]}
        raise SystemExit(f"unsupported annotation {ast.unparse(node)}")

    def item_class(self, node: ast.expr) -> str | None:
        """The TypedDict of each item of a ``list[...]`` annotation, or None."""
        if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name) and node.value.id == "list":
            item = node.slice
            if isinstance(item, ast.Constant) and isinstance(item.value, str):
                item = ast.parse(item.value, mode="eval").body
            if isinstance(item, ast.Name) and item.id in self.classes:
                return item.id
        return None

    def result_schema(
        self, output: dict[str, tuple[ast.expr, bool]], reads: frozenset[str], op: str,
        errors: list[str],
    ) -> dict[str, Any]:
        """The result fields Nerve reads: ``name``, or ``name[].field`` for a
        field of each item of a list; objects otherwise undescribed."""
        nested: dict[str, set[str]] = {}
        for read in reads:
            name, _, sub = read.partition("[].")
            nested.setdefault(name, set())
            if sub:
                nested[name].add(sub)
        properties: dict[str, Any] = {}
        for name, subs in sorted(nested.items()):
            if name not in output:
                errors.append(f"Loom's {op} answers no {name}")
                continue
            annotation = output[name][0]
            if not subs:
                properties[name] = self.schema(annotation, expand=False)
                continue
            item = self.item_class(annotation)
            fields = self.fields(item) if item else {}
            missing = sorted(sub for sub in subs if sub not in fields)
            errors += [f"Loom's {op} answers no {name}[].{sub}" for sub in missing]
            properties[name] = {"type": "array", "items": {"type": "object", "properties": {
                sub: self.schema(fields[sub][0], expand=False) for sub in sorted(subs) if sub in fields
            }}}
        return {"type": "object", "properties": properties}

    def object_schema(self, class_name: str, keep: set[str] | None, depth: int = 0) -> dict[str, Any]:
        """A TypedDict as an object schema: every field, or those in ``keep``
        plus the required ones."""
        fields = self.fields(class_name)
        names = sorted(
            name for name, (_, required) in fields.items()
            if keep is None or name in keep or required
        )
        return {
            "type": "object",
            "properties": {name: self.schema(fields[name][0], depth) for name in names},
            "required": [name for name in names if fields[name][1]],
        }


def frozensets(source: str, names: tuple[str, ...]) -> dict[str, frozenset[str]]:
    """The module-level ``NAME = frozenset({...})`` literals of ``source``."""
    out: dict[str, frozenset[str]] = {}
    for node in ast.parse(source).body:
        target = node.target if isinstance(node, ast.AnnAssign) else (
            node.targets[0] if isinstance(node, ast.Assign) and len(node.targets) == 1 else None
        )
        if not isinstance(target, ast.Name) or target.id not in names:
            continue
        value = node.value
        if isinstance(value, ast.Call) and value.args:
            out[target.id] = frozenset(ast.literal_eval(value.args[0]))
    missing = set(names) - set(out)
    if missing:
        raise SystemExit(f"{TOOLSETS_FILE} defines no {sorted(missing)}")
    return out


def narrow(schema: dict[str, Any], values: frozenset[str]) -> list[str]:
    """List only ``values`` in the fixed set ``schema`` (or its items)
    allows; the values Loom does not know, or every value when ``schema``
    names no fixed set."""
    if "enum" in schema:
        unknown = sorted(values - set(schema["enum"]))
        schema["enum"] = sorted(values)
        return unknown
    if schema.get("type") == "array" and isinstance(schema.get("items"), dict):
        return narrow(schema["items"], values)
    options = [option for option in schema.get("anyOf", []) if "enum" in option]
    if options:
        unknown = set(values)
        for option in options:
            unknown &= set(narrow(option, values))
        return sorted(unknown)
    return sorted(values)


def listed_tools(sets: dict[str, frozenset[str]]) -> set[str]:
    """The tools the default (``core``) toolset lists."""
    tools = {"loom_" + op.replace(".", "_") for op in sets["_MCP_CORE_OPS"]}
    tools |= {SYNTHETIC_TOOLS[op] for op in sets["SYNTHETIC_TOOL_OPS"] if op in SYNTHETIC_TOOLS}
    return tools | set(META_TOOLS)


def build(
    ops_source: str, toolsets_source: str, named: set[str] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    schemas = Schemas(ops_source)
    sets = frozensets(toolsets_source, ("_MCP_CORE_OPS", "_MCP_HIDDEN_OPS", "SYNTHETIC_TOOL_OPS"))
    errors: list[str] = []
    ops: dict[str, Any] = {}
    for name, call in sorted(LOOM_CALLS.items()):
        spec = schemas.ops.get(name)
        if spec is None:
            errors.append(f"Loom has no op {name}")
            continue
        if name in sets["_MCP_HIDDEN_OPS"]:
            errors.append(f"Loom keeps {name} off MCP: no tool, loom_op included, runs it")
            continue
        if name in sets["SYNTHETIC_TOOL_OPS"]:
            if name not in SYNTHETIC_TOOLS:
                errors.append(f"{name} has a synthetic tool this script does not know")
                continue
            tool, listed = SYNTHETIC_TOOLS[name], True
        elif name in sets["_MCP_CORE_OPS"]:
            tool, listed = "loom_" + name.replace(".", "_"), True
        else:
            tool, listed = LOOM_OP, False
        body = schemas.fields(spec["body"])
        output = schemas.fields(spec["output"])
        errors += [f"Loom's {name} takes no {f}" for f in sorted(call.fields - set(body))]
        request = schemas.object_schema(spec["body"], set(call.fields) & set(body))
        for field_name, values in sorted(call.values.items()):
            prop = request["properties"].get(field_name)
            if prop is None:
                continue  # reported above
            errors += [f"Loom's {name} {field_name} has no value {v}" for v in narrow(prop, values)]
        ops[name] = {
            "tool": tool,
            "listed": listed,
            "idempotent": spec["idempotent"],
            "request": request,
            # What Nerve reads; the rest of the answer is not described here.
            "result": schemas.result_schema(output, call.reads, name, errors),
        }
    listed = listed_tools(sets)
    named = tool_names() if named is None else named
    for name in sorted(named):
        if name.endswith("*"):
            if not any(tool.startswith(name[:-1]) for tool in listed):
                errors.append(f"Loom lists no tool {name}")
        elif name not in listed:
            errors.append(f"Loom does not list {name} by default")
    fixture = {
        "generated_by": "scripts/loom_contract_fixture.py",
        "from": [OPS_FILE, TOOLSETS_FILE],
        "ops": ops,
        # Each one listed by Loom's default toolset when this was written.
        "tools_named_in_prompts": sorted(named),
    }
    return fixture, errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--loom", type=Path, required=True, help="a Loom checkout")
    parser.add_argument("--ref", default=None, help="read the files at this git ref")
    parser.add_argument(
        "--check", action="store_true", help="fail if the fixture is not what would be written",
    )
    args = parser.parse_args()
    fixture, errors = build(
        read_source(args.loom, OPS_FILE, args.ref),
        read_source(args.loom, TOOLSETS_FILE, args.ref),
    )
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    text = json.dumps(fixture, indent=2, sort_keys=True) + "\n"
    if args.check:
        if not FIXTURE.exists() or FIXTURE.read_text(encoding="utf-8") != text:
            print(f"{FIXTURE.relative_to(ROOT)} is out of date", file=sys.stderr)
            return 1
        return 0
    FIXTURE.write_text(text, encoding="utf-8")
    print(f"wrote {FIXTURE.relative_to(ROOT)} ({len(fixture['ops'])} ops)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
