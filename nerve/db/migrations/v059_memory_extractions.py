"""V59: session windows queued for extraction by the memory backend.

The Loom memory backend hands a session window to Loom's extraction queue
(``memory.set_from_messages`` with ``defer``), and the indexing sweep then
moves the session's ``last_memorized_at`` past that window. Loom gives a queued
batch a few attempts and marks it failed for good after the last one, so the
window would be lost without a record of it. ``memory_extractions`` keeps one
row per queued batch: the session, the Loom org and namespace it was queued
in, Loom's queue id, and the ids of the batch's messages (a JSON array). The
sweep asks Loom about the rows of the namespace it now uses, deletes the ones
Loom finished, sends a failed batch again from exactly those messages (a
bounded number of times), and keeps a batch it gave up on as ``failed`` for
the diagnostics page. Deleting a session deletes its rows.

memU never writes here. Idempotent (``IF NOT EXISTS``).
"""

from __future__ import annotations

import aiosqlite

SQL = """
CREATE TABLE IF NOT EXISTS memory_extractions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    org TEXT NOT NULL DEFAULT '',
    namespace TEXT NOT NULL DEFAULT '',
    queue_id TEXT NOT NULL,
    message_ids TEXT NOT NULL DEFAULT '[]',
    message_count INTEGER NOT NULL DEFAULT 0,
    sends INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'queued',
    last_error TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_memory_extractions_state
    ON memory_extractions(state, org, namespace, created_at);

CREATE INDEX IF NOT EXISTS idx_memory_extractions_session
    ON memory_extractions(session_id);
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
