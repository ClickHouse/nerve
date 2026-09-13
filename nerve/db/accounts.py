"""Internal storage for accounts, actor identity, and instance secrets."""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

BOOTSTRAP_CREDENTIAL_SOURCES = ("config", "none")
JWT_SECRET_NAME = "jwt_secret"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return str(uuid.uuid4())


def _account(row) -> dict:
    value = dict(row)
    value["enabled"] = bool(value["enabled"])
    return value


@dataclass(frozen=True)
class BootstrapAccount:
    created: bool
    account_id: str | None = None
    actor_id: str | None = None


class AccountStore:
    """Database mixin; callers use higher-level account and identity services."""

    async def _count_accounts(self) -> int:
        async with self.db.execute("SELECT COUNT(*) FROM accounts") as cursor:
            return (await cursor.fetchone())[0]

    async def _account_rows(self) -> list[dict]:
        async with self.db.execute(
            "SELECT * FROM accounts ORDER BY created_at, id"
        ) as cursor:
            return [_account(row) async for row in cursor]

    async def _account_identity(self, account_id: str) -> dict | None:
        """The request-resolution fields for one account, or ``None``."""
        async with self.db.execute(
            """SELECT a.id AS account_id, a.enabled,
                      r.id AS actor_id, r.kind AS actor_kind, r.display_name
                 FROM accounts a
                 LEFT JOIN actor_refs r ON r.id = a.actor_id
                WHERE a.id = ?""",
            (account_id,),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        value = dict(row)
        value["enabled"] = bool(value["enabled"])
        return value

    async def _sole_account_identity(self) -> dict | None:
        """The request-resolution fields when exactly one account exists."""
        async with self.db.execute(
            """SELECT a.id AS account_id, a.enabled,
                      r.id AS actor_id, r.kind AS actor_kind, r.display_name
                 FROM accounts a
                 LEFT JOIN actor_refs r ON r.id = a.actor_id
                ORDER BY a.created_at, a.id
                LIMIT 2"""
        ) as cursor:
            rows = [dict(row) async for row in cursor]
        if len(rows) != 1:
            return None
        rows[0]["enabled"] = bool(rows[0]["enabled"])
        return rows[0]

    async def _bootstrap_first_account(
        self, *, credential_source: str, display_name: str | None = None,
    ) -> BootstrapAccount:
        """Create the first human account atomically; never recreate one."""
        if credential_source not in BOOTSTRAP_CREDENTIAL_SOURCES:
            raise ValueError(
                "bootstrap credential_source must be one of "
                f"{BOOTSTRAP_CREDENTIAL_SOURCES}, "
                f"got {credential_source!r}"
            )
        async with self._atomic():
            # Take the write lock before the count, so two processes cannot
            # both see an empty table.
            await self.db.execute("BEGIN IMMEDIATE")
            if await self._count_accounts():
                return BootstrapAccount(created=False)
            actor_id, account_id, now = _new_id(), _new_id(), _now()
            await self.db.execute(
                """INSERT INTO actor_refs (id, kind, display_name, created_at)
                   VALUES (?, 'human', ?, ?)""",
                (actor_id, display_name, now),
            )
            await self.db.execute(
                """INSERT INTO accounts
                       (id, actor_id, username, credential_source, credential,
                        enabled, created_at)
                   VALUES (?, ?, NULL, ?, NULL, 1, ?)""",
                (account_id, actor_id, credential_source, now),
            )
        return BootstrapAccount(True, account_id, actor_id)

    async def _set_bootstrap_credential_source(
        self, account_id: str, credential_source: str,
    ) -> None:
        if credential_source not in ("config", "none"):
            raise ValueError("bootstrap credentials must remain in configuration")
        await self._write(
            "UPDATE accounts SET credential_source = ?, credential = NULL WHERE id = ?",
            (credential_source, account_id),
        )

    async def _get_instance_secret(self, name: str) -> str | None:
        async with self.db.execute(
            "SELECT value FROM instance_secrets WHERE name = ?", (name,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

    async def _ensure_instance_secret(self, name: str, value: str) -> str:
        """Store once so concurrent secret generators converge."""
        await self._write(
            "INSERT OR IGNORE INTO instance_secrets (name, value) VALUES (?, ?)",
            (name, value),
        )
        stored = await self._get_instance_secret(name)
        return stored if stored is not None else value

    async def _delete_instance_secret(self, name: str) -> bool:
        """Retire a secret without leaving its value in freed SQLite pages."""
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


def _read_only(db_path: Path) -> sqlite3.Connection | None:
    db_path = Path(db_path)
    if not db_path.is_file():
        return None
    try:
        return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5.0)
    except sqlite3.Error:
        return None


def read_instance_secret(db_path: Path, name: str) -> str:
    """Read a daemon-held secret from another process, or return ``""``."""
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


def inspect_bootstrap_state(db_path: Path) -> tuple[list[str], bool] | None:
    """Read-only dry-run state: credential sources and stored-secret presence."""
    conn = _read_only(db_path)
    if conn is None:
        return None
    try:
        sources = [
            str(row[0])
            for row in conn.execute(
                "SELECT credential_source FROM accounts ORDER BY created_at, id"
            ).fetchall()
        ]
        stored = conn.execute(
            "SELECT 1 FROM instance_secrets WHERE name = ?", (JWT_SECRET_NAME,)
        ).fetchone()
        return sources, stored is not None
    except sqlite3.Error:
        return None
    finally:
        conn.close()
