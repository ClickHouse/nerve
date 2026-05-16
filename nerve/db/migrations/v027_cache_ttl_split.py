"""V27: Split cache_creation tokens by TTL (5-minute vs 1-hour).

The Anthropic API returns ``cache_creation: {ephemeral_5m_input_tokens,
ephemeral_1h_input_tokens}`` alongside the legacy aggregate
``cache_creation_input_tokens``. The two TTLs are billed at different
rates (5m write = 1.25x base, 1h write = 2x base), so accurate cost
attribution requires tracking them separately.

This migration adds two columns to ``session_usage``. Existing rows
default to 0 — historical aggregates remain in
``cache_creation_input_tokens`` and can still be summed.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript("""
        ALTER TABLE session_usage
            ADD COLUMN cache_creation_5m_input_tokens INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE session_usage
            ADD COLUMN cache_creation_1h_input_tokens INTEGER NOT NULL DEFAULT 0;
    """)
    logger.info("v027: added 5m/1h ephemeral cache columns to session_usage")
