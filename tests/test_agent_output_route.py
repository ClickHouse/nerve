"""Shell and monitor output (``GET .../agents/{agent_id}/output``).

The CLI writes a background shell's output to
``<tmp>/claude-<uid>/<project>/<session>/tasks/<task_id>.output``; the route
serves its tail, and only from there.
"""

from __future__ import annotations

import pytest

from nerve.agent.backends import events as ev
from nerve.agent.engine import AgentEngine
from tests.test_agent_routes import SID, _agent, _detail, api  # noqa: F401 - fixture


async def _shell(engine: AgentEngine, task_id: str, output_file: str | None):
    await engine.agents.note_main_tool_use(SID, ev.ToolUse(
        tool_use_id=f"toolu_{task_id}", name="Bash",
        input={"command": "npm run e2e", "run_in_background": True},
    ))
    await engine.agents.on_system_event(SID, ev.SystemEvent("task_started", {
        "task_id": task_id, "tool_use_id": f"toolu_{task_id}", "task_type": "local_bash",
    }))
    rec = next(r for r in engine.agents._sessions[SID].records.values() if r.task_id == task_id)
    rec.output_file = output_file
    return rec


@pytest.fixture
def cli_tmp(tmp_path, monkeypatch):
    """The CLI's per-user temp root, redirected into the test's tmp dir."""
    import nerve.gateway.routes.agents as agents_routes

    root = tmp_path / "claude-tmp"
    (root / "project" / "sdk-session" / "tasks").mkdir(parents=True)
    monkeypatch.setattr(agents_routes, "_output_roots", lambda: [str(root.resolve())])
    return root


@pytest.mark.asyncio
async def test_shell_output_tail(api, cli_tmp):
    path = cli_tmp / "project" / "sdk-session" / "tasks" / "b1c2d3e4f5.output"
    path.write_text("line 1\nline 2\nall green\n", encoding="utf-8")
    rec = await _shell(api.engine, "b1c2d3e4f5", str(path))
    assert api.engine.agents.to_view(rec)["controls"]["output"] is True

    body = (await api.client.get(f"/api/sessions/{SID}/agents/{rec.id}/output")).json()
    assert body == {
        "agent_id": rec.id, "text": "line 1\nline 2\nall green\n",
        "size": 24, "truncated": False, "available": True,
    }
    tail = (await api.client.get(
        f"/api/sessions/{SID}/agents/{rec.id}/output?tail_bytes=10",
    )).json()
    assert (tail["text"], tail["size"], tail["truncated"]) == ("all green\n", 24, True)


@pytest.mark.asyncio
async def test_shell_output_paths_are_validated(api, cli_tmp, tmp_path):
    outside = tmp_path / "elsewhere" / "b1c2d3e4f5.output"
    outside.parent.mkdir()
    outside.write_text("secret", encoding="utf-8")
    inside_wrong_name = cli_tmp / "project" / "sdk-session" / "tasks" / "other.output"
    inside_wrong_name.write_text("x", encoding="utf-8")
    escape = cli_tmp / "project" / "sdk-session" / "tasks" / "b9c9d9e9f9.output"
    escape.symlink_to(outside)
    gone = cli_tmp / "project" / "sdk-session" / "tasks" / "b8c8d8e8f8.output"

    cases = [
        ("b1c2d3e4f5", str(outside)),             # outside the CLI's temp root
        ("b2c2d3e4f5", str(inside_wrong_name)),   # basename is not <task_id>.output
        ("b9c9d9e9f9", str(escape)),              # a symlink out of the root
        ("b8c8d8e8f8", str(gone)),                # the file is gone
        ("B-bad", str(cli_tmp / "B-bad.output")),  # not a CLI task id
        ("b7c7d7e7f7", None),                     # no output file at all
    ]
    for task_id, path in cases:
        rec = await _shell(api.engine, task_id, path)
        response = await api.client.get(f"/api/sessions/{SID}/agents/{rec.id}/output")
        assert response.status_code == 200, task_id
        assert response.json() == {
            "agent_id": rec.id, "text": "", "size": 0, "truncated": False, "available": False,
        }, task_id


@pytest.mark.asyncio
async def test_output_of_an_agent_is_refused(api, cli_tmp):
    rec = await _agent(api.engine, "toolu_A", "a1b2c3d4e5f6a7b8c")
    response = await api.client.get(f"/api/sessions/{SID}/agents/{rec.id}/output")
    assert response.status_code == 409
    assert _detail(response)["code"] == "not_a_shell"
    missing = await api.client.get(f"/api/sessions/{SID}/agents/ag_000000000000/output")
    assert missing.status_code == 404


def test_the_cli_temp_roots_cover_tmp_and_the_temp_dir():
    import os
    import tempfile

    from nerve.gateway.routes.agents import _output_roots

    roots = _output_roots()
    uid = os.getuid()
    assert os.path.realpath(f"/tmp/claude-{uid}") in roots
    assert os.path.realpath(os.path.join(tempfile.gettempdir(), f"claude-{uid}")) in roots
