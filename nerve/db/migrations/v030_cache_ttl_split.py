"""V30: Split cache_creation tokens by TTL (5-minute vs 1-hour).

The Anthropic API returns ``cache_creation: {ephemeral_5m_input_tokens,
ephemeral_1h_input_tokens}`` alongside the legacy aggregate
``cache_creation_input_tokens``. The two TTLs are billed at different
rates (5m write = 1.25x base, 1h write = 2x base), so accurate cost
attribution requires tracking them separately.

This migration adds two columns to ``session_usage``. Existing rows
default to 0 — historical aggregates remain in
``cache_creation_input_tokens`` and can still be summed.

History note: this was originally numbered v027 but collided with
``v027_session_last_rotated``. The previous migration runner tracked
only ``MAX(version)``, so on databases where the *other* v027 was
applied first this one was silently skipped, breaking usage tracking
end-to-end. Renumbering to v030 lets the runner pick it up again on
already-migrated databases; the check below makes the migration
idempotent for DBs that were hand-patched in the interim.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    async with db.execute("PRAGMA table_info(session_usage)") as cur:
        existing = {row[1] for row in await cur.fetchall()}

    statements: list[str] = []
    if "cache_creation_5m_input_tokens" not in existing:
        statements.append(
            "ALTER TABLE session_usage "
            "ADD COLUMN cache_creation_5m_input_tokens INTEGER NOT NULL DEFAULT 0"
        )
    if "cache_creation_1h_input_tokens" not in existing:
        statements.append(
            "ALTER TABLE session_usage "
            "ADD COLUMN cache_creation_1h_input_tokens INTEGER NOT NULL DEFAULT 0"
        )

    if not statements:
        logger.info("v030: 5m/1h ephemeral cache columns already present, skipping")
        return

    for stmt in statements:
        await db.execute(stmt)
    logger.info(
        "v030: added %d ephemeral cache column(s) to session_usage", len(statements)
    )
