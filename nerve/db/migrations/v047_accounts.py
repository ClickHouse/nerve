"""V47: local accounts and actor identity — the expand step of local multi-user.

Two concerns that must stay in separate tables:

- ``actor_refs`` is *attribution identity*: the stable id that sessions and
  messages will reference, plus versioned presentation fields (display name,
  email). It exists in every identity mode. ``kind`` tells a human principal
  from an agent's system principal — the identity autonomous work (cron,
  channels, background agents) acts as.
- ``accounts`` is *local login state*: a username (NULL until one is needed),
  where the credential lives and whether the account is enabled. It only has
  rows in local mode, so an install that later moves to an external identity
  provider has no credential table in play at all.

``credential_source`` says where an account's password is:

- ``config`` — ``auth.password_hash`` in configuration. Nothing is copied, so a
  lockdown install whose hash is an ``${ENV}`` reference keeps working and the
  operator's file is never rewritten. Transitional: a later release moves every
  such row to ``local``.
- ``local``  — the bcrypt hash on the row itself (``credential``).
- ``none``   — passwordless.

The remaining tables give the local bootstrap the rows the architecture asks
for — one tenant, one agent with its system principal, the owner's explicit
membership and the bootstrap owner grant — in the same shape a control plane
would provision, so a later move is a data question rather than a type change.
The grant confers nothing locally (every account has full permissions); it is
recorded, never consulted.

``instance_secrets`` holds machine-local secrets that are *state* rather than
configuration. The first is the JWT signing secret an install without
``auth.jwt_secret`` used to run without (and therefore ran open); it is now
generated once on first start and kept here instead of being written into the
operator's config files.

This migration only creates empty tables. It deliberately reads no
configuration — a migration receives a bare connection and must replay
identically on every machine — so the rows are created by the
configuration-aware bootstrap in :mod:`nerve.migrate`, which runs after the
schema is current. Everything is ``IF NOT EXISTS``, and nothing existing is
altered, so older code keeps working against a database this has touched.

Ids are UUID4 strings. Timestamps are UTC ISO-8601 text written Python-side,
like the other recent tables.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

SQL = """
CREATE TABLE IF NOT EXISTS actor_refs (
    id              TEXT PRIMARY KEY,
    kind            TEXT NOT NULL CHECK (kind IN ('human', 'system')),
    display_name    TEXT,
    email           TEXT,
    profile_version INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS accounts (
    id                TEXT PRIMARY KEY,
    actor_id          TEXT NOT NULL UNIQUE REFERENCES actor_refs(id),
    username          TEXT,
    credential_source TEXT NOT NULL
                      CHECK (credential_source IN ('config', 'local', 'none')),
    credential        TEXT,
    enabled           INTEGER NOT NULL DEFAULT 1,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    disabled_at       TEXT,
    -- A credential lives on the row only for 'local'; 'config'/'none' keep
    -- none, so a stale hash can never linger on a config/passwordless account.
    CHECK ((credential_source = 'local' AND credential IS NOT NULL)
           OR (credential_source IN ('config', 'none') AND credential IS NULL)),
    -- disabled_at is set exactly when the account is disabled.
    CHECK ((enabled = 1 AND disabled_at IS NULL)
           OR (enabled = 0 AND disabled_at IS NOT NULL))
);

-- Usernames are looked up case-insensitively. NULLs are distinct in a SQLite
-- unique index, so the bootstrapped account (username NULL) is unaffected.
CREATE UNIQUE INDEX IF NOT EXISTS idx_accounts_username
    ON accounts(username COLLATE NOCASE);

CREATE TABLE IF NOT EXISTS tenants (
    id         TEXT PRIMARY KEY,
    slug       TEXT NOT NULL UNIQUE,
    name       TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agents (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL REFERENCES tenants(id),
    slug            TEXT NOT NULL,
    name            TEXT NOT NULL,
    system_actor_id TEXT NOT NULL UNIQUE REFERENCES actor_refs(id),
    created_at      TEXT NOT NULL,
    UNIQUE (tenant_id, slug)
);

CREATE TABLE IF NOT EXISTS tenant_memberships (
    id         TEXT PRIMARY KEY,
    tenant_id  TEXT NOT NULL REFERENCES tenants(id),
    actor_id   TEXT NOT NULL REFERENCES actor_refs(id),
    created_at TEXT NOT NULL,
    UNIQUE (tenant_id, actor_id)
);

CREATE TABLE IF NOT EXISTS agent_grants (
    id         TEXT PRIMARY KEY,
    agent_id   TEXT NOT NULL REFERENCES agents(id),
    actor_id   TEXT NOT NULL REFERENCES actor_refs(id),
    role       TEXT NOT NULL,
    source     TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (agent_id, actor_id, role)
);

CREATE TABLE IF NOT EXISTS instance_secrets (
    name       TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- An account is a *human* login. The agent's system principal (and any future
-- non-human actor) must never get one — attaching a login to it would let
-- autonomous work be authenticated as a person. A CHECK cannot reference
-- another table, so this is a trigger; a missing actor (NULL kind) trips it
-- too, alongside the foreign key.
CREATE TRIGGER IF NOT EXISTS accounts_actor_must_be_human
BEFORE INSERT ON accounts
FOR EACH ROW
WHEN (SELECT kind FROM actor_refs WHERE id = NEW.actor_id) IS NOT 'human'
BEGIN
    SELECT RAISE(ABORT, 'accounts.actor_id must reference a human actor_ref');
END;
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
    logger.info("v047: created accounts, actor_refs and local identity tables (empty)")
