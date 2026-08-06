"""Filesystem moves for task markdown files.

Shared by the task tool handlers and :class:`~nerve.tasks.manager.TaskManager`,
which both move a file between ``memory/tasks/active/`` and
``memory/tasks/done/`` as a task changes status.
"""

from __future__ import annotations

from pathlib import Path


def move_task_file(src: Path, dst: Path, content: str) -> None:
    """Write ``content`` to ``src``, then move it to ``dst``.

    The move is a rename, not a copy followed by an unlink. A copy deletes the
    file whenever ``src`` and ``dst`` are the same path, and that happens every
    time a task is completed twice: the first completion stores a ``file_path``
    under done/, and the second one reads that path back as its source. A
    rename onto one path is a no-op, so the same sequence now only appends the
    second note.

    Blocking. Call it through ``asyncio.to_thread``.
    """
    src.write_text(content, encoding="utf-8")
    src.replace(dst)
