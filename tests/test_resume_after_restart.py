"""Tests for `nerve restart --resume`: CLI enrollment + the startup drainer
(AgentEngine.resume_enrolled_sessions)."""

from __future__ import annotations

from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import click
import pytest

from nerve import cli, paths
from nerve.agent.engine import AgentEngine, _RESUME_AFTER_RESTART_PROMPT
from nerve.agent.sessions import SessionStatus


# --------------------------------------------------------------------------- #
#  Daemon side: AgentEngine.resume_enrolled_sessions                          #
# --------------------------------------------------------------------------- #

def _engine_with(sessions_by_id: dict) -> AgentEngine:
    """Bare AgentEngine stub exercising only resume_enrolled_sessions' deps."""
    engine = AgentEngine.__new__(AgentEngine)

    async def _get_session(sid):
        return sessions_by_id.get(sid)

    engine.db = SimpleNamespace(get_session=AsyncMock(side_effect=_get_session))
    engine.run = AsyncMock(return_value="ok")
    return engine


def _session(sid, *, status="idle", source="web", sdk="sdk-"):
    return {
        "id": sid,
        "status": status,
        "source": source,
        "sdk_session_id": (sdk + sid) if sdk is not None else None,
    }


@pytest.mark.asyncio
async def test_no_queue_file_is_noop(tmp_path):
    qf = tmp_path / "resume-after-restart"  # never created
    engine = _engine_with({})
    with patch("nerve.agent.engine.RESUME_QUEUE_FILE", qf):
        assert await engine.resume_enrolled_sessions() == 0
    engine.run.assert_not_awaited()


@pytest.mark.asyncio
async def test_resumes_valid_session_and_drains_file(tmp_path):
    qf = tmp_path / "resume-after-restart"
    qf.write_text("S1\n")
    engine = _engine_with({"S1": _session("S1")})
    with patch("nerve.agent.engine.RESUME_QUEUE_FILE", qf):
        n = await engine.resume_enrolled_sessions()
    assert n == 1
    assert not qf.exists()  # queue drained up front
    engine.run.assert_awaited_once()
    kwargs = engine.run.await_args.kwargs
    assert kwargs["session_id"] == "S1"
    assert kwargs["internal"] is True
    assert kwargs["source"] == "web"
    assert kwargs["user_message"] == _RESUME_AFTER_RESTART_PROMPT


@pytest.mark.asyncio
async def test_skips_missing_archived_external_and_no_sdk(tmp_path):
    qf = tmp_path / "resume-after-restart"
    qf.write_text("\n".join(["missing", "arch", "ext", "nosdk", "good"]) + "\n")
    sessions = {
        "arch": _session("arch", status=SessionStatus.ARCHIVED.value),
        "ext": _session("ext", source="external"),
        "nosdk": _session("nosdk", sdk=None),
        "good": _session("good"),
        # "missing" intentionally absent from the DB
    }
    engine = _engine_with(sessions)
    with patch("nerve.agent.engine.RESUME_QUEUE_FILE", qf):
        n = await engine.resume_enrolled_sessions()
    assert n == 1
    engine.run.assert_awaited_once()
    assert engine.run.await_args.kwargs["session_id"] == "good"


@pytest.mark.asyncio
async def test_dedupes_and_ignores_blank_lines(tmp_path):
    qf = tmp_path / "resume-after-restart"
    qf.write_text("S1\nS1\n  S1  \n\n")
    engine = _engine_with({"S1": _session("S1")})
    with patch("nerve.agent.engine.RESUME_QUEUE_FILE", qf):
        n = await engine.resume_enrolled_sessions()
    assert n == 1
    engine.run.assert_awaited_once()


@pytest.mark.asyncio
async def test_uses_session_source(tmp_path):
    qf = tmp_path / "resume-after-restart"
    qf.write_text("T1\n")
    engine = _engine_with({"T1": _session("T1", source="telegram")})
    with patch("nerve.agent.engine.RESUME_QUEUE_FILE", qf):
        await engine.resume_enrolled_sessions()
    assert engine.run.await_args.kwargs["source"] == "telegram"


@pytest.mark.asyncio
async def test_one_failure_does_not_block_others(tmp_path):
    qf = tmp_path / "resume-after-restart"
    qf.write_text("bad\ngood\n")
    engine = _engine_with({"bad": _session("bad"), "good": _session("good")})

    async def _run(**kwargs):
        if kwargs["session_id"] == "bad":
            raise RuntimeError("boom")
        return "ok"

    engine.run = AsyncMock(side_effect=_run)
    with patch("nerve.agent.engine.RESUME_QUEUE_FILE", qf):
        n = await engine.resume_enrolled_sessions()
    assert n == 1  # only "good" counted; "bad" swallowed
    assert engine.run.await_count == 2
    assert not qf.exists()


# --------------------------------------------------------------------------- #
#  CLI side: `nerve restart --resume` writes the queue for all modes          #
# --------------------------------------------------------------------------- #

def _invoke_restart(tmp_path, resume_ids, *, redirect_queue=True):
    """Drive ``nerve restart`` with the daemon and its side effects stubbed out.

    Returns the queue file the run should have written to. With
    ``redirect_queue=False`` the CLI's own module-level constant is left in
    place, so the write goes wherever the test-suite isolation put it.
    """
    with ExitStack() as stack:
        for cm in (
            patch("nerve.cli._is_docker_mode", return_value=False),
            patch("nerve.cli._is_systemd_managed", return_value=False),
            patch("nerve.cli._get_daemon_status", return_value=(False, None)),
            patch("nerve.cli.subprocess.Popen", MagicMock()),
            patch("nerve.paths.log_file", return_value=tmp_path / "nerve.log"),
        ):
            stack.enter_context(cm)
        if redirect_queue:
            qf = tmp_path / "resume-after-restart"
            stack.enter_context(patch("nerve.cli.RESUME_QUEUE_FILE", qf))
        else:
            qf = cli.RESUME_QUEUE_FILE
        ctx = click.Context(cli.restart)
        ctx.obj = {
            "config": SimpleNamespace(deployment="server"),
            "config_dir": str(tmp_path),
            "verbose": False,
        }
        ctx.invoke(cli.restart, resume_ids=resume_ids)
    return qf


def test_restart_resume_writes_queue(tmp_path):
    qf = _invoke_restart(tmp_path, ("S1", "S2"))
    assert qf.read_text() == "S1\nS2\n"


def test_restart_resume_appends(tmp_path):
    qf = tmp_path / "resume-after-restart"
    qf.write_text("EXISTING\n")
    _invoke_restart(tmp_path, ("S3",))
    assert qf.read_text() == "EXISTING\nS3\n"


def test_restart_without_resume_writes_nothing(tmp_path):
    qf = _invoke_restart(tmp_path, ())
    assert not qf.exists()


# --------------------------------------------------------------------------- #
#  The queue file must never be the developer's real one                      #
# --------------------------------------------------------------------------- #
#
# RESUME_QUEUE_FILE is a module-level Path, so it is resolved when nerve.config
# is imported — which, under pytest, is during collection, before any fixture
# has moved the state dir. nerve.cli and nerve.agent.engine each hold their own
# imported copy of that value. The two tests below run *without* patching it, so
# they fail unless the suite's isolation redirected every copy: the CLI appends
# to the queue and the drainer deletes it, which on a live box means corrupting
# and then destroying the real ~/.nerve/resume-after-restart.

def test_cli_writes_the_queue_inside_the_isolated_state_dir(tmp_path):
    qf = _invoke_restart(tmp_path, ("S1",), redirect_queue=False)
    assert qf == paths.nerve_path("resume-after-restart")
    assert paths.nerve_home() in qf.parents
    assert (Path.home() / ".nerve") not in qf.parents
    assert qf.read_text() == "S1\n"


@pytest.mark.asyncio
async def test_drainer_reads_and_deletes_only_the_isolated_queue():
    qf = paths.nerve_path("resume-after-restart")
    assert (Path.home() / ".nerve") not in qf.parents
    qf.parent.mkdir(parents=True, exist_ok=True)
    qf.write_text("S1\n")

    engine = _engine_with({"S1": _session("S1")})
    assert await engine.resume_enrolled_sessions() == 1
    assert not qf.exists()
