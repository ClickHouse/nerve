"""V45: Transport-neutral notification delivery records."""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

SQL = """
CREATE TABLE IF NOT EXISTS notification_deliveries (
    notification_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    target TEXT NOT NULL DEFAULT '',
    message_id TEXT NOT NULL DEFAULT '',
    delivered_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (notification_id, channel, target),
    FOREIGN KEY (notification_id) REFERENCES notifications(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_notification_deliveries_scope
    ON notification_deliveries(channel, target, delivered_at DESC);
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
    await db.execute(
        """INSERT OR IGNORE INTO notification_deliveries
               (notification_id, channel, target, message_id, delivered_at)
           SELECT id, 'telegram', telegram_chat_id,
                  COALESCE(telegram_message_id, ''), created_at
           FROM notifications
           WHERE telegram_chat_id IS NOT NULL AND telegram_chat_id != ''""",
    )
    logger.info("V45 migration: added scoped notification deliveries")
