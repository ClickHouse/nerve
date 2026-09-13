"""V49: a per-account session epoch, so a claim can end the sessions before it.

A passwordless install hands a session to every caller who reaches it (0.5).
Claiming the account ends that state for *new* callers, but the tokens already
handed out are signed, unexpired and name the same account, so without this
they keep full owner authority for the rest of their thirty days — which is
exactly the window the claim exists to close.

The epoch is an integer on the account, carried in every session token it
mints and compared on every request. Claiming bumps it inside the same
transaction that sets the password, so every token issued before the claim is
one epoch behind and is refused at its next request.

Additive and idempotent: one column, `NOT NULL DEFAULT 0`, added only if the
table does not already have it. Existing rows land on epoch 0, and tokens
minted by an earlier build carry no epoch at all, which reads as 0 — so an
upgrade logs nobody out. Only a claim moves it.

Note what the epoch is *not*: a way to revoke one session. It is per account,
so bumping it ends every session that account has. That is the right shape for
the claim (the point is that none of the old ones survive) and the wrong shape
for "sign out this one device", which nothing here offers.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def _has_column(db: aiosqlite.Connection, table: str, column: str) -> bool:
    async with db.execute(f"PRAGMA table_info({table})") as cursor:
        rows = await cursor.fetchall()
    return any(row[1] == column for row in rows)


async def up(db: aiosqlite.Connection) -> None:
    if await _has_column(db, "accounts", "session_epoch"):
        logger.info("v049: accounts.session_epoch already present")
        return
    # A constant default, so SQLite adds the column without rebuilding the
    # table and every existing account starts where tokens minted before this
    # column existed already read as.
    await db.execute(
        "ALTER TABLE accounts ADD COLUMN session_epoch INTEGER NOT NULL DEFAULT 0"
    )
    logger.info("v049: accounts.session_epoch added (every account starts at 0)")
