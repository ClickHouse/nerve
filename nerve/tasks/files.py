"""Filesystem writes and moves for task markdown files.

Shared by the task tool handlers, the task API routes and
:class:`~nerve.tasks.manager.TaskManager`, which all rewrite a task's markdown
and move it between ``memory/tasks/active/`` and ``memory/tasks/done/`` as a
task changes status.
"""

from __future__ import annotations

from pathlib import Path

from nerve.utils.fs import atomic_write_text


def write_task_file(path: Path, content: str) -> None:
    """Replace ``path``'s contents with ``content``, never truncating it first.

    ``path.write_text(content)`` opens the live file with mode ``w``, which
    truncates it to 0 bytes before the string is encoded. Anything that fails
    in that window -- one un-encodable character in the new content, ENOSPC,
    SIGKILL -- leaves the task's markdown empty and writes nothing in its
    place. The atomic writer never makes the live file the write target, so it
    holds either the complete old content or the complete new one.

    ``mode=None`` keeps the permissions the file already has. The writer
    defaults to ``0o600`` for credential files and swaps in a fresh inode, so
    an omitted mode would quietly clamp every task file to owner-only.

    Blocking. Call it through ``asyncio.to_thread``.
    """
    atomic_write_text(path, content, mode=None)


def move_task_file(src: Path, dst: Path, content: str) -> None:
    """Write ``content`` to ``src``, then move it to ``dst``.

    The write goes through :func:`write_task_file`, so a failed one leaves
    ``src`` byte-identical instead of empty and unmoved.

    The move is a rename, not a copy followed by an unlink. A copy deletes the
    file whenever ``src`` and ``dst`` are the same path, and that happens every
    time a task is completed twice: the first completion stores a ``file_path``
    under done/, and the second one reads that path back as its source. A
    rename onto one path is a no-op, so the same sequence now only appends the
    second note.

    Blocking. Call it through ``asyncio.to_thread``.
    """
    write_task_file(src, content)
    src.replace(dst)
