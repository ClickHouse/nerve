"""Local accounts and actor identity data access.

Mechanism only. Which ``credential_source`` a bootstrapped account gets, and
whether a signing secret must be generated, are configuration decisions and
live in :mod:`nerve.migrate`; this module never reads configuration.

Two tables carry the model (see migration v047 for the full rationale):

- ``actor_refs`` — attribution identity. The ``id`` is what sessions and
  messages will reference; ``display_name``/``email`` are presentation
  snapshots versioned by ``profile_version`` and are never identity or
  authorization keys. ``kind`` is ``human`` for a person and ``system`` for an
  agent's system principal.
- ``accounts`` — local login state, one per human actor_ref.

Around them, the rows the local bootstrap creates once per install: the local
tenant, the local agent (whose ``system_actor_id`` is the system principal
autonomous work acts as), the owner's membership and the bootstrap owner
grant. ``instance_secrets`` holds the machine-local JWT signing secret for
installs that configure none.
"""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

CREDENTIAL_SOURCES = ("config", "local", "none")
ACTOR_KINDS = ("human", "system")

# Slugs the singleton local rows are found by across restarts. The ids are
# random UUIDs and persist; these are what a re-run looks them up with.
LOCAL_TENANT_SLUG = "local"
LOCAL_AGENT_SLUG = "local"

# ``instance_secrets.name`` of the JWT signing secret generated for installs
# without ``auth.jwt_secret``.
JWT_SECRET_NAME = "jwt_secret"

# Sentinel for "leave this field alone" in partial updates, distinct from None
# (which clears a nullable field).
_UNSET = object()


def new_id() -> str:
    """A fresh identity id — UUID4, the shape a control plane would issue."""
    return str(uuid.uuid4())


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _account(row) -> dict:
    d = dict(row)
    d["enabled"] = bool(d["enabled"])
    return d


@dataclass(frozen=True)
class LocalIdentity:
    """The ids of the singleton local rows, as found or created by bootstrap."""

    tenant_id: str
    agent_id: str
    system_actor_id: str
    # The owner account created by *this* bootstrap run, when it created one.
    # None when the accounts table was already populated — the existing
    # accounts are listed with :meth:`AccountStore.list_accounts`, not here.
    owner_account_id: str | None = None
    owner_actor_id: str | None = None
    # Which pieces this run created: any of "tenant", "agent", "owner".
    created: frozenset[str] = field(default_factory=frozenset)


class AccountStore:
    """Mixin: ``actor_refs``, ``accounts``, the local identity rows and
    ``instance_secrets``."""

    # -- actor_refs ----------------------------------------------------------

    async def create_actor_ref(
        self,
        *,
        kind: str,
        display_name: str | None = None,
        email: str | None = None,
        actor_id: str | None = None,
    ) -> dict:
        if kind not in ACTOR_KINDS:
            raise ValueError(f"actor kind must be one of {ACTOR_KINDS}, got {kind!r}")
        actor_id = actor_id or new_id()
        now = _now()
        await self._write(
            """INSERT INTO actor_refs
                   (id, kind, display_name, email, profile_version, created_at, updated_at)
               VALUES (?, ?, ?, ?, 1, ?, ?)""",
            (actor_id, kind, display_name, email, now, now),
        )
        return await self.get_actor_ref(actor_id)  # type: ignore[return-value]

    async def get_actor_ref(self, actor_id: str) -> dict | None:
        async with self.db.execute(
            "SELECT * FROM actor_refs WHERE id = ?", (actor_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def list_actor_refs(self, *, kind: str | None = None) -> list[dict]:
        if kind is None:
            sql, params = "SELECT * FROM actor_refs ORDER BY created_at, id", ()
        else:
            sql = "SELECT * FROM actor_refs WHERE kind = ? ORDER BY created_at, id"
            params = (kind,)
        async with self.db.execute(sql, params) as cursor:
            return [dict(row) async for row in cursor]

    async def update_actor_profile(
        self,
        actor_id: str,
        *,
        display_name: str | None | object = _UNSET,
        email: str | None | object = _UNSET,
    ) -> dict | None:
        """Change presentation fields, bumping ``profile_version``.

        Renaming rewrites nothing else: authorship references the id, so the
        history keeps pointing at the same actor under the new name.
        """
        sets: list[str] = []
        params: list = []
        if display_name is not _UNSET:
            sets.append("display_name = ?")
            params.append(display_name)
        if email is not _UNSET:
            sets.append("email = ?")
            params.append(email)
        if not sets:
            return await self.get_actor_ref(actor_id)
        sets.append("profile_version = profile_version + 1")
        sets.append("updated_at = ?")
        params.extend([_now(), actor_id])
        await self._write(
            f"UPDATE actor_refs SET {', '.join(sets)} WHERE id = ?", tuple(params),
        )
        return await self.get_actor_ref(actor_id)

    # -- accounts ------------------------------------------------------------

    async def create_account(
        self,
        *,
        actor_id: str,
        credential_source: str,
        credential: str | None = None,
        username: str | None = None,
        enabled: bool = True,
        account_id: str | None = None,
    ) -> dict:
        """Insert an account for an existing human actor_ref.

        Username rules (character set, reserved names) are the caller's; this
        only enforces what the schema does — case-insensitive uniqueness.
        """
        if credential_source not in CREDENTIAL_SOURCES:
            raise ValueError(
                f"credential_source must be one of {CREDENTIAL_SOURCES}, "
                f"got {credential_source!r}"
            )
        account_id = account_id or new_id()
        now = _now()
        await self._write(
            """INSERT INTO accounts
                   (id, actor_id, username, credential_source, credential, enabled,
                    created_at, updated_at, disabled_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)""",
            (account_id, actor_id, username, credential_source, credential,
             1 if enabled else 0, now, now),
        )
        return await self.get_account(account_id)  # type: ignore[return-value]

    async def get_account(self, account_id: str) -> dict | None:
        async with self.db.execute(
            "SELECT * FROM accounts WHERE id = ?", (account_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return _account(row) if row else None

    async def get_account_by_actor(self, actor_id: str) -> dict | None:
        async with self.db.execute(
            "SELECT * FROM accounts WHERE actor_id = ?", (actor_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return _account(row) if row else None

    async def get_account_by_username(self, username: str) -> dict | None:
        """Case-insensitive lookup, matching the unique index."""
        async with self.db.execute(
            "SELECT * FROM accounts WHERE username = ? COLLATE NOCASE", (username,)
        ) as cursor:
            row = await cursor.fetchone()
            return _account(row) if row else None

    async def list_accounts(self, *, include_disabled: bool = True) -> list[dict]:
        sql = "SELECT * FROM accounts"
        if not include_disabled:
            sql += " WHERE enabled = 1"
        sql += " ORDER BY created_at, id"
        async with self.db.execute(sql) as cursor:
            return [_account(row) async for row in cursor]

    async def count_accounts(self, *, enabled_only: bool = False) -> int:
        sql = "SELECT COUNT(*) FROM accounts"
        if enabled_only:
            sql += " WHERE enabled = 1"
        async with self.db.execute(sql) as cursor:
            return (await cursor.fetchone())[0]

    async def get_sole_account(self) -> dict | None:
        """The account when exactly one exists (enabled or not), else None.

        The single-account condition bounds passwordless access and the
        grandfathering of legacy session tokens: with two accounts neither can
        say which person a caller is, so both callers get None rather than
        whichever row sorts first.
        """
        accounts = await self.list_accounts()
        return accounts[0] if len(accounts) == 1 else None

    async def set_account_enabled(self, account_id: str, enabled: bool) -> dict | None:
        now = _now()
        await self._write(
            """UPDATE accounts
                  SET enabled = ?, updated_at = ?,
                      disabled_at = CASE WHEN ? THEN NULL ELSE COALESCE(disabled_at, ?) END
                WHERE id = ?""",
            (1 if enabled else 0, now, 1 if enabled else 0, now, account_id),
        )
        return await self.get_account(account_id)

    async def set_account_credential(
        self,
        account_id: str,
        *,
        credential_source: str,
        credential: str | None = None,
    ) -> dict | None:
        """Move an account's credential: ``local`` carries the hash on the row,
        ``config`` and ``none`` carry none (``credential`` is cleared)."""
        if credential_source not in CREDENTIAL_SOURCES:
            raise ValueError(
                f"credential_source must be one of {CREDENTIAL_SOURCES}, "
                f"got {credential_source!r}"
            )
        if credential_source != "local":
            credential = None
        await self._write(
            """UPDATE accounts
                  SET credential_source = ?, credential = ?, updated_at = ?
                WHERE id = ?""",
            (credential_source, credential, _now(), account_id),
        )
        return await self.get_account(account_id)

    async def set_account_username(self, account_id: str, username: str | None) -> dict | None:
        """Set the login identifier. Uniqueness is case-insensitive (unique
        index); the caller validates the character set and reserved names."""
        await self._write(
            "UPDATE accounts SET username = ?, updated_at = ? WHERE id = ?",
            (username, _now(), account_id),
        )
        return await self.get_account(account_id)

    # -- local identity ------------------------------------------------------

    async def get_local_identity(self) -> LocalIdentity | None:
        """The singleton tenant/agent/system-principal ids, or None before
        bootstrap."""
        async with self.db.execute(
            """SELECT t.id AS tenant_id, a.id AS agent_id, a.system_actor_id
                 FROM tenants t JOIN agents a ON a.tenant_id = t.id
                WHERE t.slug = ? AND a.slug = ?""",
            (LOCAL_TENANT_SLUG, LOCAL_AGENT_SLUG),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        return LocalIdentity(
            tenant_id=row["tenant_id"],
            agent_id=row["agent_id"],
            system_actor_id=row["system_actor_id"],
        )

    async def get_system_principal(self) -> dict | None:
        """The local agent's system principal (an ``actor_refs`` row of kind
        ``system``), or None before bootstrap. Autonomous work — cron, channel
        traffic, background agents — is attributed to this actor."""
        identity = await self.get_local_identity()
        if identity is None:
            return None
        return await self.get_actor_ref(identity.system_actor_id)

    async def bootstrap_local_identity(
        self,
        *,
        credential_source: str,
        display_name: str | None = None,
        agent_name: str = "nerve",
    ) -> LocalIdentity:
        """Find or create the local identity rows, in one transaction.

        Idempotent: the tenant and agent are looked up by slug and the account
        step runs only while ``accounts`` is empty, so repeated calls return
        the same ids. A disabled account is still a row, so it is never
        re-created — disablement stays durable across restarts.

        When ``accounts`` is empty this creates the owner: a human actor_ref,
        the account (username NULL, ``credential_source`` as given, no
        credential on the row), the owner's membership in the tenant and the
        bootstrap owner grant on the agent.
        """
        if credential_source not in CREDENTIAL_SOURCES:
            raise ValueError(
                f"credential_source must be one of {CREDENTIAL_SOURCES}, "
                f"got {credential_source!r}"
            )
        created: set[str] = set()
        owner_account_id: str | None = None
        owner_actor_id: str | None = None

        async with self._atomic():
            # Take the write lock up front. The emptiness check below decides
            # whether an account is inserted, and a deferred transaction would
            # let a second process (a `nerve migrate` beside the daemon) read
            # zero as well and insert a second owner.
            await self.db.execute("BEGIN IMMEDIATE")
            now = _now()

            async with self.db.execute(
                "SELECT id FROM tenants WHERE slug = ?", (LOCAL_TENANT_SLUG,)
            ) as cursor:
                row = await cursor.fetchone()
            if row is None:
                tenant_id = new_id()
                await self.db.execute(
                    "INSERT INTO tenants (id, slug, name, created_at) VALUES (?, ?, ?, ?)",
                    (tenant_id, LOCAL_TENANT_SLUG, LOCAL_TENANT_SLUG, now),
                )
                created.add("tenant")
            else:
                tenant_id = row["id"]

            async with self.db.execute(
                "SELECT id, system_actor_id FROM agents WHERE tenant_id = ? AND slug = ?",
                (tenant_id, LOCAL_AGENT_SLUG),
            ) as cursor:
                row = await cursor.fetchone()
            if row is None:
                agent_id, system_actor_id = new_id(), new_id()
                await self.db.execute(
                    """INSERT INTO actor_refs
                           (id, kind, display_name, email, profile_version,
                            created_at, updated_at)
                       VALUES (?, 'system', NULL, NULL, 1, ?, ?)""",
                    (system_actor_id, now, now),
                )
                await self.db.execute(
                    """INSERT INTO agents
                           (id, tenant_id, slug, name, system_actor_id, created_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (agent_id, tenant_id, LOCAL_AGENT_SLUG, agent_name, system_actor_id, now),
                )
                created.add("agent")
            else:
                agent_id, system_actor_id = row["id"], row["system_actor_id"]

            async with self.db.execute("SELECT COUNT(*) FROM accounts") as cursor:
                account_count = (await cursor.fetchone())[0]
            if account_count == 0:
                owner_actor_id, owner_account_id = new_id(), new_id()
                await self.db.execute(
                    """INSERT INTO actor_refs
                           (id, kind, display_name, email, profile_version,
                            created_at, updated_at)
                       VALUES (?, 'human', ?, NULL, 1, ?, ?)""",
                    (owner_actor_id, display_name, now, now),
                )
                await self.db.execute(
                    """INSERT INTO accounts
                           (id, actor_id, username, credential_source, credential,
                            enabled, created_at, updated_at, disabled_at)
                       VALUES (?, ?, NULL, ?, NULL, 1, ?, ?, NULL)""",
                    (owner_account_id, owner_actor_id, credential_source, now, now),
                )
                await self.db.execute(
                    """INSERT INTO tenant_memberships (id, tenant_id, actor_id, created_at)
                       VALUES (?, ?, ?, ?)""",
                    (new_id(), tenant_id, owner_actor_id, now),
                )
                await self.db.execute(
                    """INSERT INTO agent_grants
                           (id, agent_id, actor_id, role, source, created_at)
                       VALUES (?, ?, ?, 'owner', 'bootstrap', ?)""",
                    (new_id(), agent_id, owner_actor_id, now),
                )
                created.add("owner")

        return LocalIdentity(
            tenant_id=tenant_id,
            agent_id=agent_id,
            system_actor_id=system_actor_id,
            owner_account_id=owner_account_id,
            owner_actor_id=owner_actor_id,
            created=frozenset(created),
        )

    # -- instance secrets ----------------------------------------------------

    async def get_instance_secret(self, name: str) -> str | None:
        async with self.db.execute(
            "SELECT value FROM instance_secrets WHERE name = ?", (name,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

    async def ensure_instance_secret(self, name: str, value: str) -> str:
        """Store ``value`` under ``name`` unless one is already held.

        Returns the value in force afterwards — the existing one when there
        is one, so two racing generators agree on a single secret rather than
        the last writer's.
        """
        await self._write(
            "INSERT OR IGNORE INTO instance_secrets (name, value, created_at) VALUES (?, ?, ?)",
            (name, value, _now()),
        )
        stored = await self.get_instance_secret(name)
        return stored if stored is not None else value

    async def delete_instance_secret(self, name: str) -> bool:
        """Remove a stored secret for good; True if a row was there.

        ``secure_delete`` is switched on for the statement so SQLite
        overwrites the freed pages instead of merely unlinking them — a retired
        signing key must not linger in the file for a later dump to recover.
        """
        async with self._atomic():
            await self.db.execute("PRAGMA secure_delete=ON")
            try:
                cursor = await self.db.execute(
                    "DELETE FROM instance_secrets WHERE name = ?", (name,)
                )
                deleted = cursor.rowcount > 0
                await cursor.close()
            finally:
                await self.db.execute("PRAGMA secure_delete=OFF")
        return deleted


# -- Out-of-process readers -------------------------------------------------- #
#
# CLI commands (`nerve reload`, `nerve codex token`, ...) run in their own
# process with no live Database and only need to *read* what the daemon has
# stored. Plain sqlite3, read-only, tolerant of a database or table that is
# not there yet.


def _read_only(db_path: Path) -> sqlite3.Connection | None:
    db_path = Path(db_path)
    if not db_path.is_file():
        return None
    try:
        return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5.0)
    except sqlite3.Error:
        return None


def read_instance_secret(db_path: Path, name: str) -> str:
    """The secret stored under ``name`` in ``db_path``, or ``""``."""
    conn = _read_only(db_path)
    if conn is None:
        return ""
    try:
        row = conn.execute(
            "SELECT value FROM instance_secrets WHERE name = ?", (name,)
        ).fetchone()
        return str(row[0]) if row and row[0] else ""
    except sqlite3.Error:
        return ""
    finally:
        conn.close()


def count_accounts_readonly(db_path: Path) -> int | None:
    """Rows in ``accounts``, or None when the database or table does not exist
    yet (i.e. the schema migration has not run)."""
    conn = _read_only(db_path)
    if conn is None:
        return None
    try:
        row = conn.execute("SELECT COUNT(*) FROM accounts").fetchone()
        return int(row[0]) if row else 0
    except sqlite3.Error:
        return None
    finally:
        conn.close()


def list_credential_sources_readonly(db_path: Path) -> list[str] | None:
    """``credential_source`` of every account, oldest first, or None when the
    database or table does not exist yet."""
    conn = _read_only(db_path)
    if conn is None:
        return None
    try:
        rows = conn.execute(
            "SELECT credential_source FROM accounts ORDER BY created_at, id"
        ).fetchall()
        return [str(row[0]) for row in rows]
    except sqlite3.Error:
        return None
    finally:
        conn.close()
