"""V46: durable spool for messages a channel saw but did not answer.

Sources are pull, cursor, and cron. Channels are push. This table is the
join between them: the channel appends on the dispatch path, and a
``ChannelSource`` drains it on the source runner's cadence, which buys
filtering, condensing, TTL, health, and cursor handling for free.

The alternative — writing straight to ``source_messages`` from the socket —
would skip the inbox guardrail, which is the one layer standing between
untrusted chat text and an autonomous agent. Spooling first keeps that
choke point where it already is.

``AUTOINCREMENT`` is load-bearing rather than decorative. A plain rowid is
reused after the highest row is deleted, and this table is pruned by design,
so a drained-and-pruned spool would hand out ids the cursor has already
passed and the next observations would be skipped. AUTOINCREMENT gives a
strictly increasing id that survives pruning, which is what makes
``WHERE id > cursor`` correct.

Payload is JSON rather than columns because the shape belongs to
:class:`~nerve.channels.base.ObservedMessage`, not to the database: only the
drain reads it, and a channel gaining a field should not need a migration.
The columns that exist are the ones the drain and the pruner filter on.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

SQL = """
CREATE TABLE IF NOT EXISTS channel_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel TEXT NOT NULL,
    channel_key TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

-- The drain: one channel's backlog past a cursor, in id order.
CREATE INDEX IF NOT EXISTS idx_channel_observations_drain
    ON channel_observations(channel, id);

-- The TTL sweep.
CREATE INDEX IF NOT EXISTS idx_channel_observations_expires
    ON channel_observations(expires_at);
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
    logger.info("v046: channel_observations spool created")
