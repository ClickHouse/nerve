"""A failed write of a task's markdown must leave the file it was rewriting.

Every writer of a task file used ``Path.write_text``, which is
``open(path, "w")`` followed by ``write(str)``. The open truncates the live file
to 0 bytes and the str -> utf-8 encoding happens afterwards, inside the write,
so the old bytes are destroyed strictly before the new ones exist and the whole
encode-and-write sequence sits in the exposed window. One un-encodable
character in a caller-supplied note lands in it, and so does a write error
after the truncate (ENOSPC, EIO, EFBIG) or the process dying mid-write
(SIGKILL, power loss).

In ``move_task_file`` the truncating write targets the *source*, the live file
under active/, and the rename that would have moved it never runs: the task
keeps a row pointing at active/ and the markdown there is empty.

A lone surrogate is the trigger most of these tests use because it is the one
that needs nothing unusual to happen: ``json.loads('"\\ud800"')`` returns it,
so it arrives through the ordinary tool and HTTP wire. It fails in the encoder,
though, so every carrier also has a case that sends encodable content and fails
the write later, at its ``os.fsync``, where the rest land.

Each test asserts the file's *bytes* are unchanged rather than that it is
merely non-empty (a half-written file is a loss too), that the rewrite did not
re-permission it, and that no temp-file debris is left in the directory.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from nerve.agent.tools.handlers import tasks as task_handlers
from nerve.agent.tools.registry import ToolContext
from nerve.tasks import files as task_files

TASK_ID = "t1"
REL_PATH = f"memory/tasks/active/{TASK_ID}.md"
BODY = "# T1\n\n**Tags:** alpha\n\nbody worth keeping\n\n## Updates\n\n- seeded\n"

UNENCODABLE = "\ud800"

# Neither 0o600 (the atomic writer's credential default) nor 0o666 & ~0o022
# (what a fresh file gets under the umask CI runs with), so a wrapper that
# stops preserving it cannot pass by coincidence. Real task files carry it.
FILE_MODE = 0o664


async def _seed(tmp_path, db) -> tuple[Path, ToolContext]:
    """Workspace + row for one open task, its file at ``FILE_MODE``."""
    workspace = tmp_path / "ws"
    (workspace / "memory" / "tasks" / "active").mkdir(parents=True)
    (workspace / "memory" / "tasks" / "done").mkdir(parents=True)
    path = workspace / REL_PATH
    path.write_text(BODY, encoding="utf-8")
    path.chmod(FILE_MODE)
    await db.upsert_task(
        task_id=TASK_ID, file_path=REL_PATH, title="T1", status="in_progress",
        tags="alpha", content=BODY,
    )
    return workspace, ToolContext(session_id="test", db=db, workspace=workspace)


def _assert_file_intact(workspace: Path) -> None:
    """The live file, its permissions and its directory, all as seeded."""
    active = workspace / "memory" / "tasks" / "active"
    path = workspace / REL_PATH
    assert path.read_bytes() == BODY.encode("utf-8")
    assert stat.S_IMODE(path.stat().st_mode) == FILE_MODE
    assert sorted(p.name for p in active.iterdir()) == [f"{TASK_ID}.md"]


@pytest.mark.asyncio
async def test_task_done_with_an_unencodable_note_keeps_the_file(tmp_path, db):
    workspace, ctx = await _seed(tmp_path, db)

    with pytest.raises(UnicodeEncodeError):
        await task_handlers.task_done_handler(
            ctx, {"task_id": TASK_ID, "note": UNENCODABLE},
        )

    _assert_file_intact(workspace)
    # The failure has to leave one complete file under active/, not a wiped
    # source plus an empty done/.
    assert list((workspace / "memory" / "tasks" / "done").iterdir()) == []


@pytest.mark.asyncio
async def test_task_done_keeps_the_file_when_the_write_fails_late(
    tmp_path, db, monkeypatch,
):
    """The same guarantee when there is nothing wrong with the content.

    The unencodable cases all fail in the encoder, so by themselves they are
    also satisfied by a writer that encodes the content and only then truncates
    the live file. That writer still empties it on a write error after the
    truncate (ENOSPC, EIO, EFBIG) and when the process dies mid-write (SIGKILL,
    power loss), neither of which the content can be screened for. This case
    therefore sends an ordinary encodable note and fails the write at its
    ``os.fsync``, after the bytes have been handed to the filesystem, which is
    where those land.

    ``tests/test_utils_fs.py`` pins that late-failure behaviour for the writer
    in isolation; what this pins is that the carrier reaches it.
    """
    workspace, ctx = await _seed(tmp_path, db)
    reached = []

    def enospc(fd):
        reached.append(fd)
        raise OSError("No space left on device")

    monkeypatch.setattr(os, "fsync", enospc)

    with pytest.raises(OSError):
        await task_handlers.task_done_handler(
            ctx, {"task_id": TASK_ID, "note": "ordinary"},
        )

    # The write is the only thing on this path that syncs, so a run that never
    # got there proves nothing: every assertion below would also hold for a
    # failure raised before the first byte was written.
    assert reached, "the write never reached os.fsync"
    _assert_file_intact(workspace)
    assert list((workspace / "memory" / "tasks" / "done").iterdir()) == []


@pytest.mark.asyncio
async def test_task_update_with_an_unencodable_note_keeps_the_file(tmp_path, db):
    workspace, ctx = await _seed(tmp_path, db)

    with pytest.raises(UnicodeEncodeError):
        await task_handlers.task_update_handler(
            ctx, {"task_id": TASK_ID, "note": UNENCODABLE},
        )

    _assert_file_intact(workspace)


@pytest.mark.asyncio
async def test_task_write_with_unencodable_content_keeps_the_file(
    tmp_path, db, monkeypatch,
):
    workspace, ctx = await _seed(tmp_path, db)
    # task_write refuses a task that has not been read in this session.
    monkeypatch.setattr(task_handlers, "_tasks_read", {TASK_ID})

    with pytest.raises(UnicodeEncodeError):
        await task_handlers.task_write_handler(
            ctx, {"task_id": TASK_ID, "content": BODY + UNENCODABLE},
        )

    _assert_file_intact(workspace)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("handler", "args"),
    [
        ("task_update_handler", {"task_id": TASK_ID, "note": "ordinary"}),
        ("task_write_handler", {"task_id": TASK_ID, "content": BODY + "rewritten\n"}),
    ],
    ids=["task_update", "task_write"],
)
async def test_carriers_keep_the_file_when_the_write_fails_late(
    tmp_path, db, monkeypatch, handler, args,
):
    """The late-failure guarantee for the remaining tool carriers.

    Each carrier owns its own write call site rather than routing through
    another carrier's, so the encoder-only cases above leave each of them free
    to regress independently: a per-carrier writer that encodes first and
    truncates afterwards passes all of them and still empties the file.
    """
    workspace, ctx = await _seed(tmp_path, db)
    # task_write refuses a task that has not been read in this session. No
    # other carrier consults this set, so it is set for both.
    monkeypatch.setattr(task_handlers, "_tasks_read", {TASK_ID})
    reached = []

    def enospc(fd):
        reached.append(fd)
        raise OSError("No space left on device")

    monkeypatch.setattr(os, "fsync", enospc)

    with pytest.raises(OSError):
        await getattr(task_handlers, handler)(ctx, args)

    assert reached, "the write never reached os.fsync"
    _assert_file_intact(workspace)


@pytest.mark.parametrize(
    "existing_mode",
    [0o664, 0o640],
    ids=["group-writable", "group-readable"],
)
def test_write_task_file_keeps_an_existing_files_permissions(
    tmp_path, existing_mode,
):
    """``atomic_write_text`` defaults to ``0o600``; a task file is shared.

    The writer defaults that way because it was written for credential files,
    and it swaps in a fresh inode -- so an omitted mode does not "leave the
    permissions alone" the way ``open(path, "w")`` does, it clamps every task
    file to owner-only on its next write. The wrapper exists to make that
    decision in one place instead of at each call site.

    Both values differ from the two modes a wrapper that ignores the file's own
    would leave: the writer's ``0o600`` default, and ``0o666`` minus the umask.
    """
    path = tmp_path / f"{TASK_ID}.md"
    path.write_text(BODY, encoding="utf-8")
    path.chmod(existing_mode)
    rewritten = "# T1\n\nrewritten\n"

    task_files.write_task_file(path, rewritten)

    assert path.read_text(encoding="utf-8") == rewritten
    assert stat.S_IMODE(path.stat().st_mode) == existing_mode
