"""V63: the git checkouts each session works in, backfilled with the linked worktrees of its file snapshots."""

from __future__ import annotations

from pathlib import Path

import aiosqlite

SQL = """
CREATE TABLE IF NOT EXISTS session_workspaces (
    session_id   TEXT NOT NULL,
    toplevel     TEXT NOT NULL,
    branch       TEXT,
    last_seen_at TEXT NOT NULL,
    PRIMARY KEY (session_id, toplevel)
);
"""


def _linked_worktree(path: str) -> tuple[str, str | None] | None:
    # Only linked worktrees: a main checkout's branch today says nothing about the one an old session saw.
    if not path.startswith("/") or not Path(path).parent.is_dir():
        return None
    start = Path(path).resolve()
    for directory in (start, *start.parents):
        dot_git = directory / ".git"
        if dot_git.is_dir():
            return None
        if not dot_git.is_file():
            continue
        content = dot_git.read_text().strip()
        if not content.startswith("gitdir:"):
            return None
        git_dir = (directory / content[len("gitdir:"):].strip()).resolve()
        head = (git_dir / "HEAD").read_text().strip() if (git_dir / "HEAD").is_file() else ""
        branch = head[len("ref: refs/heads/"):] if head.startswith("ref: refs/heads/") else None
        return str(directory), branch
    return None


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
    async with db.execute(
        "SELECT session_id, file_path, created_at FROM session_file_snapshots"
    ) as cursor:
        snapshots = await cursor.fetchall()
    for session_id, file_path, created_at in snapshots:
        try:
            checkout = _linked_worktree(file_path)
        except (OSError, ValueError):
            continue
        if checkout is None:
            continue
        seen = created_at.replace(" ", "T") + "+00:00" if "T" not in created_at else created_at
        await db.execute(
            "INSERT INTO session_workspaces (session_id, toplevel, branch, last_seen_at)"
            " VALUES (?, ?, ?, ?)"
            " ON CONFLICT (session_id, toplevel) DO UPDATE SET"
            " last_seen_at = MAX(last_seen_at, excluded.last_seen_at)",
            (session_id, *checkout, seen),
        )
