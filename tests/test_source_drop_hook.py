"""Guardrail drop hook — a command that learns what the guardrail dropped.

Dropped records are never persisted, so the hook is the only way to act on a
drop. These tests run real subprocesses (the current interpreter) and cover:
what the hook receives, when it runs, and that no hook failure can fail a sync.
"""

from __future__ import annotations

import json
import sys
import time

import pytest

from nerve.config import GitHubSyncConfig, NerveConfig
from nerve.sources import runner as runner_mod
from nerve.sources.base import Source
from nerve.sources.filters import FieldRule, InboxFilter
from nerve.sources.models import FetchResult, SourceRecord
from nerve.sources.registry import build_source_runners
from nerve.sources.runner import SourceRunner


def _rec(rid: str, repo: str, actors: list[str], reason: str = "mention") -> SourceRecord:
    return SourceRecord(
        id=rid,
        source="github",
        record_type="github_notification",
        summary=f"[{repo}] something ({reason})",
        content="untrusted body text",
        timestamp="2026-01-01T00:00:00Z",
        metadata={"repo_name": repo, "actors": actors, "reason": reason},
    )


class _FakeSource(Source):
    """A source that returns a fixed batch of records."""

    source_name = "github"

    def __init__(self, records: list[SourceRecord], next_cursor: str = "c1"):
        self._records = records
        self._next_cursor = next_cursor

    async def fetch(self, cursor, limit: int = 100) -> FetchResult:
        return FetchResult(records=list(self._records), next_cursor=self._next_cursor)


def _filter() -> InboxFilter:
    return InboxFilter(rules=[
        FieldRule(field="repo_name", deny=["evil/*"]),
        FieldRule(field="actors", allow=["alice"]),
    ])


def _capture_hook(out_path) -> list[str]:
    """A hook that copies its stdin to *out_path*."""
    code = (
        "import sys, pathlib; "
        f"pathlib.Path({str(out_path)!r}).write_text(sys.stdin.read())"
    )
    return [sys.executable, "-c", code]


def _warnings(caplog) -> str:
    return "\n".join(r.getMessage() for r in caplog.records if r.levelname == "WARNING")


def _batch() -> list[SourceRecord]:
    return [
        _rec("kept", "owner/repo", ["alice"]),
        _rec("by-actor", "owner/repo", ["stranger"]),
        _rec("by-repo", "evil/repo", ["alice"]),
    ]


# ---------------------------------------------------------------------------
# What the hook receives
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_hook_receives_only_dropped_records_as_json_lines(db, tmp_path):
    out = tmp_path / "hook.jsonl"
    runner = SourceRunner(
        source=_FakeSource(_batch()), db=db,
        inbox_filter=_filter(), drop_hook=_capture_hook(out),
    )

    result = await runner.run()

    assert result.error is None
    assert result.records_ingested == 1
    assert result.records_dropped == 2

    lines = [json.loads(line) for line in out.read_text().splitlines()]
    assert [line["id"] for line in lines] == ["by-actor", "by-repo"]
    # The field of the rule that fired, so a hook can tell drops apart.
    assert [line["dropped_on"] for line in lines] == ["actors", "repo_name"]
    first = lines[0]
    assert first["source"] == "github"
    assert first["record_type"] == "github_notification"
    assert first["timestamp"] == "2026-01-01T00:00:00Z"
    assert first["metadata"] == {
        "repo_name": "owner/repo", "actors": ["stranger"], "reason": "mention",
    }
    # Content stays out: the guardrail keeps it away from automation.
    assert "content" not in first
    assert "untrusted body text" not in out.read_text()

    # The kept record still reaches the inbox and the cursor moves.
    rows, _ = await db.list_source_messages(source="github", limit=100)
    assert [r["id"] for r in rows] == ["kept"]
    assert await db.get_sync_cursor("github") == "c1"


@pytest.mark.asyncio
async def test_hook_not_run_when_nothing_is_dropped(db, tmp_path):
    out = tmp_path / "hook.jsonl"
    runner = SourceRunner(
        source=_FakeSource([_rec("kept", "owner/repo", ["alice"])]), db=db,
        inbox_filter=_filter(), drop_hook=_capture_hook(out),
    )

    result = await runner.run()

    assert result.records_ingested == 1
    assert not out.exists()


@pytest.mark.asyncio
async def test_hook_not_run_when_persist_fails(db, tmp_path, monkeypatch):
    # The cursor does not advance on a failed persist, so the batch is fetched
    # again and the hook gets its drops on that run instead.
    out = tmp_path / "hook.jsonl"
    runner = SourceRunner(
        source=_FakeSource(_batch()), db=db,
        inbox_filter=_filter(), drop_hook=_capture_hook(out),
    )

    async def _boom(records):
        raise RuntimeError("disk full")

    monkeypatch.setattr(runner, "_persist_to_inbox", _boom)

    result = await runner.run()

    assert result.error == "disk full"
    assert not out.exists()
    assert await db.get_sync_cursor("github") is None


# ---------------------------------------------------------------------------
# A hook can never fail the sync
# ---------------------------------------------------------------------------

async def _assert_sync_unaffected(db, result) -> None:
    assert result.error is None
    assert result.records_ingested == 1
    assert result.records_dropped == 2
    rows, _ = await db.list_source_messages(source="github", limit=100)
    assert [r["id"] for r in rows] == ["kept"]
    assert await db.get_sync_cursor("github") == "c1"


@pytest.mark.asyncio
async def test_failing_hook_is_logged_not_raised(db, caplog):
    hook = [sys.executable, "-c", "import sys; sys.stderr.write('bad token'); sys.exit(3)"]
    runner = SourceRunner(
        source=_FakeSource(_batch()), db=db, inbox_filter=_filter(), drop_hook=hook,
    )

    result = await runner.run()

    await _assert_sync_unaffected(db, result)
    assert "exited 3 for 2 dropped record(s). stderr: 'bad token'" in caplog.text


@pytest.mark.asyncio
async def test_missing_hook_is_logged_not_raised(db, tmp_path, caplog):
    hook = [str(tmp_path / "no-such-hook")]
    runner = SourceRunner(
        source=_FakeSource(_batch()), db=db, inbox_filter=_filter(), drop_hook=hook,
    )

    result = await runner.run()

    await _assert_sync_unaffected(db, result)
    assert "did not start" in caplog.text


@pytest.mark.asyncio
async def test_hook_that_ignores_stdin_is_fine(db, caplog):
    # Exits at once without reading: writing the payload must not break.
    hook = [sys.executable, "-c", "pass"]
    runner = SourceRunner(
        source=_FakeSource(_batch() * 2000), db=db, inbox_filter=_filter(), drop_hook=hook,
    )

    result = await runner.run()

    assert result.error is None
    assert result.records_dropped == 4000
    assert "drop hook" not in _warnings(caplog)


@pytest.mark.asyncio
async def test_slow_hook_is_killed(db, monkeypatch, caplog):
    monkeypatch.setattr(runner_mod, "_DROP_HOOK_TIMEOUT_SECS", 0.5)
    hook = [sys.executable, "-c", "import time; time.sleep(30)"]
    runner = SourceRunner(
        source=_FakeSource(_batch()), db=db, inbox_filter=_filter(), drop_hook=hook,
    )

    result = await runner.run()

    await _assert_sync_unaffected(db, result)
    assert "was killed" in caplog.text


# A hook that starts a long-lived child, writes the child's pid to argv[1],
# then either waits ("wait") or exits at once ("exit"). The child inherits the
# hook's stdin and never reads it.
_SPAWNING_HOOK = """
import pathlib, subprocess, sys, time
child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
pathlib.Path(sys.argv[1]).write_text(str(child.pid))
if sys.argv[2] == "wait":
    time.sleep(30)
"""


def _gone(pid: int, within: float = 3.0) -> bool:
    """True once *pid* no longer runs (gone, or a zombie awaiting its reaper)."""
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        try:
            with open(f"/proc/{pid}/status") as f:
                if any(line.startswith("State:") and "Z" in line.split()[1] for line in f):
                    return True
        except (FileNotFoundError, ProcessLookupError):  # ESRCH: it died mid-read
            return True
        time.sleep(0.05)
    return False


async def _run_spawning_hook(db, tmp_path, monkeypatch, mode: str, records: list[SourceRecord]):
    monkeypatch.setattr(runner_mod, "_DROP_HOOK_TIMEOUT_SECS", 0.5)
    script = tmp_path / "hook.py"
    script.write_text(_SPAWNING_HOOK)
    pid_file = tmp_path / "child.pid"
    runner = SourceRunner(
        source=_FakeSource(records), db=db, inbox_filter=_filter(),
        drop_hook=[sys.executable, str(script), str(pid_file), mode],
    )
    started = time.monotonic()
    result = await runner.run()
    return result, time.monotonic() - started, int(pid_file.read_text())


@pytest.mark.asyncio
async def test_timeout_kills_the_hooks_children_too(db, tmp_path, monkeypatch, caplog):
    result, elapsed, child = await _run_spawning_hook(db, tmp_path, monkeypatch, "wait", _batch())

    assert elapsed < 5
    await _assert_sync_unaffected(db, result)
    assert "process group was killed" in caplog.text
    assert _gone(child), "the hook's child survived the timeout"


@pytest.mark.asyncio
async def test_timeout_kills_a_child_holding_stdin_after_the_hook_exited(
    db, tmp_path, monkeypatch, caplog,
):
    # More than a pipe buffer of payload: the write blocks on the child, which
    # inherited stdin, long after the hook itself exited 0.
    result, elapsed, child = await _run_spawning_hook(
        db, tmp_path, monkeypatch, "exit", _batch() * 2000,
    )

    assert elapsed < 5
    assert result.error is None
    assert result.records_dropped == 4000
    assert "process group was killed" in caplog.text
    assert _gone(child), "the child holding stdin survived the timeout"


@pytest.mark.asyncio
async def test_background_job_left_by_the_hook_does_not_stall_the_sync(db, caplog):
    hook = ["/bin/sh", "-c", f"{sys.executable} -c 'import time; time.sleep(5)' & exit 0"]
    runner = SourceRunner(
        source=_FakeSource(_batch()), db=db, inbox_filter=_filter(), drop_hook=hook,
    )

    started = time.monotonic()
    result = await runner.run()

    assert time.monotonic() - started < 3
    await _assert_sync_unaffected(db, result)
    assert "drop hook" not in _warnings(caplog)


@pytest.mark.asyncio
async def test_spawn_error_of_any_kind_is_logged_not_raised(db, monkeypatch, caplog):
    async def _bad_argv(*args, **kwargs):
        raise ValueError("embedded null byte")

    monkeypatch.setattr(runner_mod.asyncio, "create_subprocess_exec", _bad_argv)
    runner = SourceRunner(
        source=_FakeSource(_batch()), db=db, inbox_filter=_filter(),
        drop_hook=["/opt/hooks/ack"],
    )

    result = await runner.run()

    await _assert_sync_unaffected(db, result)
    assert "did not start: embedded null byte" in caplog.text


@pytest.mark.asyncio
async def test_unexpected_hook_error_never_holds_back_the_cursor(db, monkeypatch, caplog):
    runner = SourceRunner(
        source=_FakeSource(_batch()), db=db, inbox_filter=_filter(),
        drop_hook=["/opt/hooks/ack"],
    )

    def _boom(dropped, inbox_filter):
        raise TypeError("not serializable")

    monkeypatch.setattr(runner, "_drop_hook_payload", _boom)

    result = await runner.run()

    await _assert_sync_unaffected(db, result)
    assert "drop hook failed" in caplog.text


@pytest.mark.asyncio
async def test_hook_runs_before_the_cursor_advances(db, monkeypatch):
    runner = SourceRunner(
        source=_FakeSource(_batch()), db=db, inbox_filter=_filter(),
        drop_hook=["/opt/hooks/ack"],
    )
    seen = {}

    async def _spy(dropped):
        seen["cursor"] = await db.get_sync_cursor("github")
        seen["ids"] = [r.id for r in dropped]

    monkeypatch.setattr(runner, "_run_drop_hook", _spy)

    await runner.run()

    assert seen == {"cursor": None, "ids": ["by-actor", "by-repo"]}
    assert await db.get_sync_cursor("github") == "c1"


@pytest.mark.asyncio
async def test_hook_never_starts_without_an_active_guardrail(db, tmp_path):
    out = tmp_path / "hook.jsonl"
    runner = SourceRunner(
        source=_FakeSource(_batch()), db=db,
        inbox_filter=InboxFilter(rules=[FieldRule(field="actors")]),
        drop_hook=_capture_hook(out),
    )

    result = await runner.run()

    assert result.records_ingested == 3
    assert not out.exists()


@pytest.mark.parametrize("bad", [{"program": "/opt/hooks/ack"}, ["/opt/hooks/ack", 3], ["  ", "x"]])
def test_unusable_hook_is_disabled_with_a_warning(db, bad, caplog):
    runner = SourceRunner(source=_FakeSource([]), db=db, drop_hook=bad)
    assert runner.drop_hook == []
    assert "hook disabled" in caplog.text


# ---------------------------------------------------------------------------
# Config and wiring
# ---------------------------------------------------------------------------

def test_config_drop_hook_defaults_to_off():
    assert GitHubSyncConfig.from_dict({}).drop_hook == []


def test_config_drop_hook_is_an_argv_list_passed_as_is():
    # Nothing stripped or dropped: an empty argument keeps the positions after it.
    cfg = GitHubSyncConfig.from_dict({"drop_hook": ["/opt/hooks/ack", "", " drop ", 7]})
    assert cfg.drop_hook == ["/opt/hooks/ack", "", " drop ", "7"]


@pytest.mark.parametrize("off", [None, "", []])
def test_config_drop_hook_empty_values_mean_off(off):
    assert GitHubSyncConfig.from_dict({"drop_hook": off}).drop_hook == []


def test_config_drop_hook_string_is_one_program_not_split():
    # Never a shell string: no word splitting, no shell.
    cfg = GitHubSyncConfig.from_dict({"drop_hook": "/opt/my hooks/ack"})
    assert cfg.drop_hook == ["/opt/my hooks/ack"]


@pytest.mark.asyncio
async def test_build_source_runners_wires_drop_hook(db):
    cfg = NerveConfig.from_dict({
        "sync": {"github": {
            "enabled": True,
            "allow_actors": ["alice"],
            "drop_hook": ["/opt/hooks/ack", "drop"],
        }},
    })
    gh = next(r for r in build_source_runners(cfg, db) if r.source.source_name == "github")
    assert gh.drop_hook == ["/opt/hooks/ack", "drop"]


@pytest.mark.asyncio
async def test_build_source_runners_warns_about_hook_without_guardrail(db, caplog):
    cfg = NerveConfig.from_dict({
        "sync": {"github": {"enabled": True, "drop_hook": ["/opt/hooks/ack"]}},
    })
    build_source_runners(cfg, db)
    assert "drop_hook is set but no guardrail is" in caplog.text


@pytest.mark.asyncio
async def test_build_source_runners_warns_about_a_missing_program(db, tmp_path, caplog):
    cfg = NerveConfig.from_dict({
        "sync": {"github": {
            "enabled": True,
            "allow_actors": ["alice"],
            "drop_hook": [str(tmp_path / "no-such-hook")],
        }},
    })
    build_source_runners(cfg, db)
    assert "is not an executable" in caplog.text


@pytest.mark.asyncio
async def test_build_source_runners_survives_a_wrong_shaped_hook(db, caplog):
    cfg = NerveConfig.from_dict({
        "sync": {"github": {
            "enabled": True,
            "allow_actors": ["alice"],
            "drop_hook": {"program": "/opt/hooks/ack"},
        }},
    })
    gh = next(r for r in build_source_runners(cfg, db) if r.source.source_name == "github")
    assert gh.drop_hook == []
    assert "hook disabled" in caplog.text
