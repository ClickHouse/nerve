"""Internal storage for accounts, actor identity, and instance secrets."""

from __future__ import annotations

import re
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

BOOTSTRAP_CREDENTIAL_SOURCES = ("config", "none")
CREDENTIAL_SOURCES = ("config", "local", "none")
JWT_SECRET_NAME = "jwt_secret"

# Sentinel for "leave this field alone" in partial updates, distinct from None
# (which clears a nullable field).
_UNSET = object()


# Usernames are mutable lookup keys, never actor identities. ASCII-only storage
# makes SQLite's NOCASE uniqueness complete.
USERNAME_PATTERN = r"^[a-z0-9][a-z0-9._-]{1,31}$"
_USERNAME_RE = re.compile(USERNAME_PATTERN)
USERNAME_MIN_LENGTH = 2
USERNAME_MAX_LENGTH = 32

# Token subjects and authority-like/path names must not become logins.
RESERVED_USERNAMES = frozenset({
    "user",
    "admin",
    "system",
    "nerve",
    "agent-system",
    "backend-agent",
    "external-agent-mcp",
    "root",
    "me",
})


class AccountError(ValueError):
    """A rule about accounts was broken. Ingress turns these into 4xx."""


class InvalidUsernameError(AccountError):
    """The username is empty, too short or long, or outside the character set."""


class ReservedUsernameError(AccountError):
    """The username is one this instance keeps for itself."""


class UsernameTakenError(AccountError):
    """Another account already has this username (compared case-insensitively)."""


class PasswordlessInstanceError(AccountError):
    """A second account cannot exist while the first has no password."""


class UnnamedAccountError(AccountError):
    """An existing account needs a username before a second can exist."""


class StaleSessionError(AccountError):
    """The caller was admitted under a session epoch the account has left behind.

    Raised by a mutation that carried the epoch its credential stated into the
    transaction and found the row somewhere else — which is what claiming an
    unclaimed instance does to every session that existed before it (``v049``).
    A refusal, never a retry: the request was authorised by a credential that
    has since stopped being one, and re-running it would be doing the work the
    claim was meant to prevent.
    """


class NotClaimableError(AccountError):
    """The claim target is not exactly one unsecured account."""


class LastAccountError(AccountError):
    """The last enabled account cannot be disabled."""


def normalise_username(raw: str | None) -> str:
    """Strip, lower-case and validate a username, or raise."""
    if raw is None:
        raise InvalidUsernameError("A username is required")
    candidate = str(raw).strip().lower()
    if not candidate:
        raise InvalidUsernameError("A username is required")
    if not _USERNAME_RE.match(candidate):
        raise InvalidUsernameError(
            f"Usernames are {USERNAME_MIN_LENGTH}–{USERNAME_MAX_LENGTH} characters, "
            "start with a letter or digit, and may otherwise contain letters, "
            "digits, '.', '_' and '-'."
        )
    if candidate in RESERVED_USERNAMES:
        raise ReservedUsernameError(f"'{candidate}' is reserved; choose another username")
    return candidate


@dataclass(frozen=True, slots=True)
class LoginState:
    """Non-identifying facts shared by login and legacy-token resolution."""

    single_account: bool
    # A named account without a credential is still passwordless.
    passwordless: bool
    # Whether ``instance_setup`` records a completed setup. A credential also
    # completes setup; see ``nerve.gateway.routes.accounts.setup_required``.
    setup_complete: bool
    sole_account_id: str | None = None


def login_state_from(accounts: list[dict], *, setup_complete: bool) -> LoginState:
    """Build login state from rows a caller may already need for timing."""
    if len(accounts) != 1:
        return LoginState(
            single_account=False, passwordless=False, setup_complete=setup_complete,
        )
    sole = accounts[0]
    return LoginState(
        single_account=True,
        passwordless=sole["credential_source"] == "none",
        setup_complete=setup_complete,
        sole_account_id=sole["id"],
    )


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

    # -- actor_refs ----------------------------------------------------------

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
        acting_account_id: str | None = None,
        acting_session_epoch: int | None = None,
    ) -> dict | None:
        """Rename an actor if the acting session is still current."""
        if display_name is _UNSET:
            return await self.get_actor_ref(actor_id)
        async with self._atomic():
            await self.db.execute("BEGIN IMMEDIATE")
            await self._require_session_epoch(
                acting_account_id, acting_session_epoch,
            )
            await self.db.execute(
                "UPDATE actor_refs SET display_name = ? WHERE id = ?",
                (display_name, actor_id),
            )
        return await self.get_actor_ref(actor_id)

    # -- accounts ------------------------------------------------------------

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
            """SELECT a.id AS account_id, a.enabled, a.session_epoch,
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
            """SELECT a.id AS account_id, a.enabled, a.session_epoch,
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

    # -- account management --------------------------------------------------
    # Guards share their BEGIN IMMEDIATE transaction with the mutation. Accounts
    # are disabled, never deleted, so single-account relaxations cannot return.

    async def login_state(self) -> LoginState:
        """The account-shaped facts that decide how a caller may log in.

        One read, one predicate — see :class:`LoginState`. ``passwordless``
        keys off ``credential_source = 'none'``; a ``config`` row counts as
        having a credential because the startup mirror keeps that value in step
        with ``auth.password_hash``. A row remains on ``config`` only while a
        configured hash exists, and startup copies that hash to ``local``.
        """
        return login_state_from(
            await self.list_accounts(),
            setup_complete=await self.setup_completed(),
        )

    async def setup_completed(self) -> bool:
        """Whether ``instance_setup`` records a completed setup."""
        async with self.db.execute("SELECT 1 FROM instance_setup") as cursor:
            return await cursor.fetchone() is not None

    async def _record_setup_complete(self) -> None:
        """Mark setup complete. Call inside the caller's write transaction."""
        await self.db.execute(
            "INSERT OR IGNORE INTO instance_setup (id, completed_at) VALUES (1, ?)",
            (_now(),),
        )

    async def complete_passwordless_setup(
        self, *, invalidate_secret_name: str | None = None,
    ) -> bool:
        """Record the operator's choice of a passwordless installation.

        For the local installer only: it does not check a setup token. Returns
        ``False`` and changes nothing unless exactly one account exists and it
        has no credential. A configured ``auth.password_hash`` is the caller's
        to check.
        """
        async with self._atomic():
            await self.db.execute("BEGIN IMMEDIATE")
            async with self.db.execute(
                "SELECT credential_source FROM accounts"
            ) as cursor:
                rows = [row[0] async for row in cursor]
            if rows != ["none"]:
                return False
            await self._record_setup_complete()
            if invalidate_secret_name is not None:
                await self._secure_delete_secret_in_transaction(invalidate_secret_name)
        return True

    async def _secure_delete_secret_in_transaction(self, name: str) -> None:
        await self.db.execute("PRAGMA secure_delete=ON")
        try:
            await self.db.execute(
                "DELETE FROM instance_secrets WHERE name = ?", (name,),
            )
        finally:
            await self.db.execute("PRAGMA secure_delete=OFF")

    async def _require_session_epoch(
        self, account_id: str | None, expected: int | None,
    ) -> None:
        """Refuse unless ``account_id``'s row still carries ``expected``.

        Call inside the write transaction so a claim cannot commit between the
        comparison and mutation. ``None`` skips the check for credentials with
        no account or callers that did not request epoch validation.
        """
        if account_id is None or expected is None:
            return
        async with self.db.execute(
            "SELECT session_epoch FROM accounts WHERE id = ?", (account_id,)
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            raise StaleSessionError(
                "This session's account no longer exists; sign in again"
            )
        if int(row[0] or 0) != int(expected):
            raise StaleSessionError(
                "This instance was claimed after your session started, so that "
                "request was refused. Sign in again."
            )

    async def create_managed_account(
        self,
        *,
        username: str,
        credential: str,
        display_name: str | None = None,
        acting_account_id: str | None = None,
        acting_session_epoch: int | None = None,
    ) -> dict:
        """Atomically create a human actor and its password-bearing account."""
        username = normalise_username(username)
        if not credential:
            raise AccountError("A new account needs a password")

        actor_id, account_id = _new_id(), _new_id()
        async with self._atomic():
            # The write lock up front: the guards below are read-then-write, and
            # a deferred transaction would let two callers both read a state
            # that permits the insert and then both perform it.
            await self.db.execute("BEGIN IMMEDIATE")

            # Was the caller still allowed to be doing this when it landed? A
            # request admitted while the instance was passwordless, arriving
            # after somebody claimed it, must not leave an account behind.
            await self._require_session_epoch(
                acting_account_id, acting_session_epoch,
            )

            async with self.db.execute(
                "SELECT username, credential_source FROM accounts"
            ) as cursor:
                existing = [dict(row) async for row in cursor]

            if len(existing) == 1 and existing[0]["credential_source"] == "none":
                raise PasswordlessInstanceError(
                    "This instance is passwordless, so a second account could not "
                    "be told apart from the first. Set a password on the existing "
                    "account before adding anyone."
                )
            if existing and any(not row["username"] for row in existing):
                raise UnnamedAccountError(
                    "An existing account has no username, and an account without "
                    "one cannot be logged into once a second account exists. Give "
                    "the existing account a username first."
                )

            async with self.db.execute(
                "SELECT 1 FROM accounts WHERE username = ? COLLATE NOCASE", (username,)
            ) as cursor:
                if await cursor.fetchone() is not None:
                    raise UsernameTakenError(
                        f"The username '{username}' is already taken"
                    )

            now = _now()
            await self.db.execute(
                """INSERT INTO actor_refs (id, kind, display_name, created_at)
                   VALUES (?, 'human', ?, ?)""",
                (actor_id, display_name, now),
            )
            try:
                await self.db.execute(
                    """INSERT INTO accounts
                           (id, actor_id, username, credential_source, credential,
                            enabled, created_at)
                       VALUES (?, ?, ?, 'local', ?, 1, ?)""",
                    (account_id, actor_id, username, credential, now),
                )
            except sqlite3.IntegrityError as e:
                # The unique index is the real arbiter of the check above: two
                # processes racing on the same name are serialised by it, not by
                # the SELECT. The actor insert rolls back with this.
                raise UsernameTakenError(
                    f"The username '{username}' is already taken"
                ) from e

        return await self.get_account(account_id)  # type: ignore[return-value]

    async def update_account_login(
        self,
        account_id: str,
        *,
        username: str | object = _UNSET,
        credential: str | object = _UNSET,
        expected_session_epoch: int | None = None,
        revoke_sessions: bool = False,
    ) -> dict | None:
        """Change a username and/or move its password to the account row.

        ``expected_session_epoch`` makes the change conditional on the account
        still being at the epoch the caller's credential named — so a password
        change authorised before a claim cannot land after it. Raises
        :class:`StaleSessionError` when they have diverged.

        ``revoke_sessions`` advances that epoch in the same transaction as a
        credential change. The caller that changed its own password can mint a
        replacement at the returned epoch; every previously issued session is
        stale. It is invalid without ``credential`` because non-credential
        profile edits must not unexpectedly sign devices out.

        Returns the updated row, or ``None`` if there is no such account.
        """
        sets: list[str] = []
        params: list = []
        if username is not _UNSET:
            normalised = normalise_username(username)  # type: ignore[arg-type]
            sets.append("username = ?")
            params.append(normalised)
        if credential is not _UNSET:
            if not credential:
                raise AccountError("A password is required")
            sets.append("credential_source = 'local'")
            sets.append("credential = ?")
            params.append(credential)
        if revoke_sessions:
            if credential is _UNSET:
                raise ValueError("revoking sessions requires a credential change")
            sets.append("session_epoch = session_epoch + 1")
        if not sets:
            return await self.get_account(account_id)

        async with self._atomic():
            await self.db.execute("BEGIN IMMEDIATE")
            async with self.db.execute(
                "SELECT 1 FROM accounts WHERE id = ?", (account_id,)
            ) as cursor:
                if await cursor.fetchone() is None:
                    return None
            await self._require_session_epoch(account_id, expected_session_epoch)
            params.append(account_id)
            try:
                await self.db.execute(
                    f"UPDATE accounts SET {', '.join(sets)} WHERE id = ?", tuple(params),
                )
            except sqlite3.IntegrityError as e:
                raise UsernameTakenError("That username is already taken") from e
        return await self.get_account(account_id)

    async def replace_credential_if_unchanged(
        self, account_id: str, *, expected: str, credential: str,
    ) -> bool:
        """Replace a local hash only if its source and value are unchanged."""
        if not expected or not credential:
            raise AccountError("both the expected and the new credential are required")
        result = await self._write(
            """UPDATE accounts
                  SET credential = ?
                WHERE id = ? AND credential = ? AND credential_source = 'local'""",
            (credential, account_id, expected),
        )
        return result.rowcount > 0

    async def set_account_credential_if_source(
        self,
        account_id: str,
        *,
        expected_source: str,
        credential_source: str,
        credential: str | None = None,
    ) -> bool:
        """Move a credential only while its source is still the one observed."""
        for source in (expected_source, credential_source):
            if source not in CREDENTIAL_SOURCES:
                raise ValueError(
                    f"credential_source must be one of {CREDENTIAL_SOURCES}, "
                    f"got {source!r}"
                )
        if credential_source == "local":
            if not credential:
                raise ValueError(
                    "credential is required when moving to credential_source='local'"
                )
        else:
            credential = None
        result = await self._write(
            """UPDATE accounts
                  SET credential_source = ?, credential = ?
                WHERE id = ? AND credential_source = ?""",
            (credential_source, credential, account_id, expected_source),
        )
        return result.rowcount > 0

    async def claim_sole_account(
        self,
        *,
        username: str | None,
        credential: str | None,
        display_name: str | None = None,
        invalidate_secret_name: str | None = None,
    ) -> dict:
        """Atomically complete setup on exactly one unclaimed account.

        First-run "claim and secure": the account an install is created with has
        no password and no username, and this is what gives it both. One
        transaction, so a half-claimed account — named but still open, or
        secured but unreachable — never exists, not even between two requests.

        ``credential=None`` is the passwordless choice: the account keeps no
        password, ``username`` is optional, and setup is recorded as complete.
        With a credential, ``username`` is required.

        The *precondition* is checked inside the transaction as well, which is
        the difference between this and ``get_sole_account()`` followed by
        ``update_account_login()``: those are two transactions, so two callers
        racing to claim a fresh install can both read "one account, no
        password" and the second one silently overwrites the first's password
        with its own. Under ``BEGIN IMMEDIATE`` the loser reads the winner's
        committed row and raises :class:`NotClaimableError`.

        Raises :class:`NotClaimableError` unless exactly one account exists, it
        has no credential and setup is not complete, and the username errors of
        :func:`normalise_username`. ``display_name`` also renames the account's
        actor, in the same transaction. When ``invalidate_secret_name`` is
        supplied, that instance secret is securely deleted in the transaction
        too: the account cannot become claimed while its bearer claim token
        remains live after a cancellation or database error.

        **It also ends every session that existed before it**, by bumping
        ``session_epoch`` in the same statement that sets the password. A
        passwordless install hands a session to everyone who can reach it, and
        those tokens are signed, unexpired and name this same account — so
        without the bump the claim would secure the *next* caller and leave the
        previous ones with owner authority for the rest of their thirty days,
        which is the window claiming exists to close. See ``v049``.

        Note what this does **not** do: decide who may call it. A passwordless
        install admits everybody, so the caller is responsible for the guard
        that makes claiming meaningful (the persisted setup token).
        """
        if credential is not None and not credential:
            raise AccountError("A password is required")
        if credential is not None or (username is not None and username.strip()):
            username = normalise_username(username)
        else:
            username = None

        async with self._atomic():
            await self.db.execute("BEGIN IMMEDIATE")
            async with self.db.execute(
                "SELECT id, actor_id, credential_source FROM accounts"
            ) as cursor:
                rows = [dict(row) async for row in cursor]
            if len(rows) != 1:
                raise NotClaimableError(
                    "Claiming is for an install with exactly one account; this "
                    f"one has {len(rows)}."
                )
            account = rows[0]
            if account["credential_source"] != "none":
                raise NotClaimableError(
                    "This account already has a password, so it has been claimed "
                    "already. Sign in instead."
                )
            if await self.setup_completed():
                raise NotClaimableError(
                    "Setup of this instance is already complete. Sign in instead."
                )

            if credential is not None:
                sql = """UPDATE accounts
                            SET username = ?, credential_source = 'local',
                                credential = ?,
                                session_epoch = session_epoch + 1
                          WHERE id = ?"""
                params: tuple = (username, credential, account["id"])
            else:
                sql = """UPDATE accounts
                            SET username = COALESCE(?, username),
                                session_epoch = session_epoch + 1
                          WHERE id = ?"""
                params = (username, account["id"])
            try:
                await self.db.execute(sql, params)
            except sqlite3.IntegrityError as e:  # pragma: no cover - one account
                raise UsernameTakenError(
                    f"The username '{username}' is already taken"
                ) from e
            if display_name is not None:
                await self.db.execute(
                    "UPDATE actor_refs SET display_name = ? WHERE id = ?",
                    (display_name or None, account["actor_id"]),
                )
            await self._record_setup_complete()
            if invalidate_secret_name is not None:
                await self._secure_delete_secret_in_transaction(invalidate_secret_name)
        return await self.get_account(account["id"])  # type: ignore[return-value]

    async def disable_account(
        self,
        account_id: str,
        *,
        acting_account_id: str | None = None,
        acting_session_epoch: int | None = None,
    ) -> dict | None:
        """Idempotently disable an account unless it is the last enabled one."""
        async with self._atomic():
            await self.db.execute("BEGIN IMMEDIATE")
            await self._require_session_epoch(
                acting_account_id, acting_session_epoch,
            )
            account = await self.get_account(account_id)
            if account is None:
                return None
            if not account["enabled"]:
                return account
            cursor = await self.db.execute(
                """UPDATE accounts
                      SET enabled = 0
                    WHERE id = ? AND enabled = 1
                      AND (SELECT COUNT(*) FROM accounts WHERE enabled = 1) > 1""",
                (account_id,),
            )
            changed = cursor.rowcount
            await cursor.close()
            if not changed:
                raise LastAccountError(
                    "This is the last enabled account. Disabling it would lock "
                    "everybody out, so it is refused — add another account first, "
                    "or disable a different one."
                )
        return await self.get_account(account_id)

    async def enable_account(
        self,
        account_id: str,
        *,
        acting_account_id: str | None = None,
        acting_session_epoch: int | None = None,
    ) -> dict | None:
        """Re-enable an account. Idempotent; ``None`` if there is no such account."""
        async with self._atomic():
            await self.db.execute("BEGIN IMMEDIATE")
            await self._require_session_epoch(
                acting_account_id, acting_session_epoch,
            )
            account = await self.get_account(account_id)
            if account is None:
                return None
            if account["enabled"]:
                return account
            await self.db.execute(
                "UPDATE accounts SET enabled = 1 WHERE id = ?",
                (account_id,),
            )
        return await self.get_account(account_id)

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


def read_setup_required(db_path: Path, *, configured_password: bool) -> bool:
    """Read from another process whether setup is required; ``False`` if unknown.

    The same rule as ``nerve.gateway.routes.accounts.setup_required``.
    """
    if configured_password:
        return False
    conn = _read_only(db_path)
    if conn is None:
        return False
    try:
        sources = [
            row[0] for row in conn.execute("SELECT credential_source FROM accounts")
        ]
        complete = conn.execute("SELECT 1 FROM instance_setup").fetchone()
        return sources == ["none"] and complete is None
    except sqlite3.Error:
        return False
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
