"""V47: local accounts, actor identity, and instance secrets."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

import aiosqlite

logger = logging.getLogger(__name__)

SQL = """
CREATE TABLE IF NOT EXISTS actor_refs (
    id           TEXT PRIMARY KEY NOT NULL,
    -- 'human' or 'system'. There is no CHECK constraint, so a new kind does
    -- not need a rebuild of this table and of the tables that reference it.
    kind         TEXT NOT NULL,
    display_name TEXT,
    created_at   TEXT NOT NULL
);

-- One database is one agent installation and therefore has one system actor.
CREATE UNIQUE INDEX IF NOT EXISTS idx_actor_refs_one_system
    ON actor_refs(kind) WHERE kind = 'system';

CREATE TABLE IF NOT EXISTS accounts (
    id                TEXT PRIMARY KEY,
    actor_id          TEXT NOT NULL UNIQUE REFERENCES actor_refs(id),
    username          TEXT,
    credential_source TEXT NOT NULL
                      CHECK (credential_source IN ('config', 'local', 'none')),
    credential        TEXT,
    enabled           INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    created_at        TEXT NOT NULL,
    CHECK ((credential_source = 'local' AND credential IS NOT NULL)
           OR (credential_source IN ('config', 'none') AND credential IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_accounts_username
    ON accounts(username COLLATE NOCASE);

CREATE TABLE IF NOT EXISTS instance_secrets (
    name  TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS accounts_actor_must_be_human
BEFORE INSERT ON accounts
FOR EACH ROW
WHEN (SELECT kind FROM actor_refs WHERE id = NEW.actor_id) IS NOT 'human'
BEGIN
    SELECT RAISE(ABORT, 'accounts.actor_id must reference a human actor_ref');
END;

CREATE TRIGGER IF NOT EXISTS accounts_actor_must_stay_human
BEFORE UPDATE OF actor_id ON accounts
FOR EACH ROW
WHEN (SELECT kind FROM actor_refs WHERE id = NEW.actor_id) IS NOT 'human'
BEGIN
    SELECT RAISE(ABORT, 'accounts.actor_id must reference a human actor_ref');
END;

CREATE TRIGGER IF NOT EXISTS actor_refs_kind_frozen_while_referenced
BEFORE UPDATE OF kind ON actor_refs
FOR EACH ROW
WHEN NEW.kind IS NOT 'human'
     AND EXISTS (SELECT 1 FROM accounts WHERE actor_id = OLD.id)
BEGIN
    SELECT RAISE(ABORT, 'an actor_ref referenced by an account must stay human');
END;

CREATE TRIGGER IF NOT EXISTS system_actor_cannot_be_deleted
BEFORE DELETE ON actor_refs
FOR EACH ROW WHEN OLD.kind = 'system'
BEGIN
    SELECT RAISE(ABORT, 'the system actor cannot be deleted');
END;

CREATE TRIGGER IF NOT EXISTS system_actor_cannot_be_reclassified
BEFORE UPDATE OF kind ON actor_refs
FOR EACH ROW WHEN OLD.kind = 'system' AND NEW.kind != 'system'
BEGIN
    SELECT RAISE(ABORT, 'the system actor cannot be reclassified');
END;

-- REPLACE conflict handling can delete its victim without firing DELETE
-- triggers, so guard every INSERT/UPDATE shape that could target this row.
CREATE TRIGGER IF NOT EXISTS system_actor_cannot_be_replaced
BEFORE INSERT ON actor_refs
FOR EACH ROW
WHEN EXISTS (
    SELECT 1 FROM actor_refs
     WHERE kind = 'system' AND (NEW.kind = 'system' OR id IS NEW.id)
)
BEGIN
    SELECT RAISE(ABORT, 'the system actor cannot be replaced');
END;

CREATE TRIGGER IF NOT EXISTS system_actor_cannot_be_replaced_by_kind_update
BEFORE UPDATE OF kind ON actor_refs
FOR EACH ROW
WHEN OLD.kind != 'system' AND NEW.kind = 'system'
     AND EXISTS (SELECT 1 FROM actor_refs WHERE kind = 'system')
BEGIN
    SELECT RAISE(ABORT, 'the system actor cannot be replaced');
END;

CREATE TRIGGER IF NOT EXISTS system_actor_cannot_be_replaced_by_id_update
BEFORE UPDATE OF id ON actor_refs
FOR EACH ROW
WHEN OLD.kind != 'system'
     AND EXISTS (
         SELECT 1 FROM actor_refs WHERE kind = 'system' AND id IS NEW.id
     )
BEGIN
    SELECT RAISE(ABORT, 'the system actor cannot be replaced');
END;

CREATE TRIGGER IF NOT EXISTS system_actor_id_cannot_change
BEFORE UPDATE OF id ON actor_refs
FOR EACH ROW WHEN OLD.kind = 'system' AND NEW.id IS NOT OLD.id
BEGIN
    SELECT RAISE(ABORT, 'the system actor id cannot change');
END;
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
    now = datetime.now(timezone.utc).isoformat()
    # The SELECT keeps explicit migration replay idempotent without relying on
    # INSERT OR REPLACE/IGNORE conflict handling around the immutable row.
    await db.execute(
        "INSERT INTO actor_refs (id, kind, display_name, created_at) "
        "SELECT ?, 'system', NULL, ? "
        "WHERE NOT EXISTS (SELECT 1 FROM actor_refs WHERE kind = 'system')",
        (str(uuid.uuid4()), now),
    )
    logger.info("v047: created accounts, actor identity, and instance secrets")
