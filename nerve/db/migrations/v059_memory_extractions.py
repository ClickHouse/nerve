"""V59: session windows queued for extraction by the memory backend.

The Loom memory backend hands a session window to Loom's extraction queue
(``memory.set_from_messages`` with ``defer``), and the indexing sweep then
moves the session's ``last_memorized_at`` past that window. Loom gives a queued
batch a few attempts and marks it failed for good after the last one, so the
window would be lost without a record of it. ``memory_extractions`` keeps one
row per queued batch: the session, Loom's queue id, and the times of the
window's first and last message. The sweep asks Loom about the queued rows,
deletes the ones Loom finished, sends a failed window again from the messages
still in ``messages`` (a bounded number of times), and keeps a window it gave
up on as ``failed`` for the diagnostics page.

memU never writes here. Idempotent (``IF NOT EXISTS``).
"""

from __future__ import annotations

import aiosqlite

SQL = """
CREATE TABLE IF NOT EXISTS memory_extractions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    queue_id TEXT NOT NULL,
    window_start TEXT NOT NULL DEFAULT '',
    window_end TEXT NOT NULL DEFAULT '',
    message_count INTEGER NOT NULL DEFAULT 0,
    sends INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'queued',
    last_error TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_memory_extractions_state
    ON memory_extractions(state, created_at);
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
