"""V71: durable reply routes — which session a channel message belongs to.

The Telegram channel maps each message it sends or receives to a session, so
a user *reply* to that message can be routed back there. That map lived in
memory and was lost on every restart, after which every reply to an earlier
message was refused. This table makes it survive restarts.

Keyed per conversation (``target``): provider message ids are unique only
within a chat. Ids are text so other channels can share the table whatever
the shape of their ids. ``created_at`` is indexed for the retention sweep
(``cleanup_old_reply_routes``).
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

SQL = """
CREATE TABLE IF NOT EXISTS channel_reply_routes (
    channel TEXT NOT NULL,
    target TEXT NOT NULL,          -- conversation id (Telegram chat id) as text
    message_id TEXT NOT NULL,      -- provider message id as text
    session_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (channel, target, message_id)
);

-- The retention sweep.
CREATE INDEX IF NOT EXISTS idx_channel_reply_routes_created
    ON channel_reply_routes(created_at);
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
    logger.info("v071: channel_reply_routes created")
