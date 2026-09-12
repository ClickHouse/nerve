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

import re
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

# One definition of the kinds, shared with the value type the request path and
# (from PR 4) the attribution columns carry. ``nerve.identity`` imports nothing
# at runtime, so this direction — data layer depends on the identity type, never
# the reverse — stays free of cycles.
from nerve.identity import ACTOR_KINDS

CREDENTIAL_SOURCES = ("config", "local", "none")

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


# --------------------------------------------------------------------------- #
#  Usernames                                                                   #
# --------------------------------------------------------------------------- #
#
# A username is a *lookup key*, never an identity (0.7): ``actor_refs.id`` is
# the identity, and renaming an account changes what it logs in as and what is
# displayed without moving one byte of stored authorship.

# Two to thirty-two characters, starting with a letter or a digit, then letters,
# digits, dot, underscore or hyphen. ASCII only and stored lower-cased, which is
# what makes the case-insensitive uniqueness complete: SQLite's NOCASE collation
# folds ASCII and nothing else, so a charset with no non-ASCII letters in it
# leaves no room for a unicode look-alike to sit beside an existing name.
USERNAME_PATTERN = r"^[a-z0-9][a-z0-9._-]{1,31}$"
_USERNAME_RE = re.compile(USERNAME_PATTERN)
USERNAME_MIN_LENGTH = 2
USERNAME_MAX_LENGTH = 32

# Names that must never become a login.
#
# ``user`` is the load-bearing one: PR 2 grandfathers web sessions minted before
# per-account logins, whose subject is the literal string ``user``
# (``nerve.gateway.auth.LEGACY_SUBJECT``), so allowing it as a username would
# let a person's name collide with a token subject while that clause lives.
# ``agent-system``/``backend-agent``/``external-agent-mcp`` are the other token
# subjects, reserved for the same reason. ``me`` is a path segment
# (``/api/accounts/me``). The rest read as an authority this model does not have
# (0.4: every account has full permissions) and would mislead.
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
    """A second account cannot exist while the first one has no password (0.5).

    Passwordless admits every caller as the one account. With two accounts that
    is not a weaker login, it is an unanswerable question: nothing distinguishes
    the callers, so every one of them would be whoever the code picked.
    """


class UnnamedAccountError(AccountError):
    """An existing account has no username, so a second one cannot be told apart.

    The account an upgrade creates has ``username IS NULL`` on purpose — nothing
    needed one while password-only login was unambiguous. The moment a second
    account exists it does, and an account with no username cannot be logged
    into at all.
    """


class NotClaimableError(AccountError):
    """The sole-account claim found something other than one unsecured account.

    Either more than one account exists, or none does, or the one that does
    already has a password — in which case somebody has already claimed it and
    a second claim would be taking it from them.
    """


class LastAccountError(AccountError):
    """The last enabled account cannot be disabled — that is a locked-out install.

    The only guard in the model (0.4). Everything else any account may do to any
    other account, including this, right up to the point where nobody is left.
    """


def normalise_username(raw: str | None) -> str:
    """Canonicalise and validate a username, or raise.

    Surrounding whitespace is stripped and the result lower-cased, so
    ``" Alice "`` and ``"alice"`` are one name rather than two that happen to
    collide in the index. Raises :class:`InvalidUsernameError` for anything
    outside :data:`USERNAME_PATTERN` and :class:`ReservedUsernameError` for
    :data:`RESERVED_USERNAMES`.
    """
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
    """What the instance's accounts say about how a caller may log in.

    The "exactly one account" predicate, stated once so the three rules bounded
    by it cannot drift apart: grandfathered ``sub: "user"`` tokens (PR 2),
    password-only login, and passwordless access (0.5).

    Deliberately carries **no username and no credential** — it is what the
    unauthenticated ``/api/auth/status`` descriptor is built from, so there is
    nothing on it that must not be published.
    """

    accounts: int
    single_account: bool
    # Exactly one account and it has no credential at all: any password is
    # accepted and resolves to it. This is also the first-run state PR 6's
    # wizard claims — deliberately *not* a second field, because a second field
    # is a second thing to keep in step, and the one that existed keyed off the
    # username as well and therefore went false when a passwordless account was
    # merely named. A named account with no password is still an open one.
    passwordless: bool
    sole_account_id: str | None = None


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

        Enforces the identity invariants the schema also guards, with clearer
        errors: the actor must be human (never the system principal), a
        ``local`` account carries a credential and a ``config``/``none`` one
        does not, and ``disabled_at`` is set iff the account is disabled. A
        username, when given, goes through :func:`normalise_username` — every
        username that reaches the table does, whichever method put it there.

        The low-level primitive: it creates the account row and nothing else.
        Adding a *person* is :meth:`create_managed_account`, which also creates
        the actor in the same transaction and applies the account-management
        guards.
        """
        if credential_source not in CREDENTIAL_SOURCES:
            raise ValueError(
                f"credential_source must be one of {CREDENTIAL_SOURCES}, "
                f"got {credential_source!r}"
            )
        if credential_source == "local" and not credential:
            raise ValueError("credential is required when credential_source='local'")
        if credential_source in ("config", "none") and credential is not None:
            raise ValueError(
                f"credential must be None when credential_source={credential_source!r}"
            )
        # A login belongs to a human. An unknown actor_id is left to the foreign
        # key / trigger (IntegrityError); a known non-human is rejected here.
        actor = await self.get_actor_ref(actor_id)
        if actor is not None and actor["kind"] != "human":
            raise ValueError(
                "account actor_id must reference a human actor_ref, "
                f"not a {actor['kind']!r} principal"
            )
        if username is not None:
            username = normalise_username(username)
        account_id = account_id or new_id()
        now = _now()
        disabled_at = None if enabled else now
        await self._write(
            """INSERT INTO accounts
                   (id, actor_id, username, credential_source, credential, enabled,
                    created_at, updated_at, disabled_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (account_id, actor_id, username, credential_source, credential,
             1 if enabled else 0, now, now, disabled_at),
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
        if credential_source == "local":
            if not credential:
                raise ValueError(
                    "credential is required when moving to credential_source='local'"
                )
        else:
            credential = None
        await self._write(
            """UPDATE accounts
                  SET credential_source = ?, credential = ?, updated_at = ?
                WHERE id = ?""",
            (credential_source, credential, _now(), account_id),
        )
        return await self.get_account(account_id)

    async def set_account_username(self, account_id: str, username: str | None) -> dict | None:
        """Set the login identifier, validating it.

        ``None`` clears it. Anything else goes through
        :func:`normalise_username` (character set, length, reserved names) and
        is stored lower-cased; a clash with another account raises
        :class:`UsernameTakenError` — the unique index is what decides, so two
        callers racing on the same name cannot both win.
        """
        if username is not None:
            username = normalise_username(username)
        try:
            await self._write(
                "UPDATE accounts SET username = ?, updated_at = ? WHERE id = ?",
                (username, _now(), account_id),
            )
        except sqlite3.IntegrityError as e:
            raise UsernameTakenError(f"The username '{username}' is already taken") from e
        return await self.get_account(account_id)

    # -- account management (PR 3) -------------------------------------------
    #
    # Every guard below is evaluated *inside* the transaction that acts on it,
    # under BEGIN IMMEDIATE. A count read before a write is not a guard: two
    # callers can both read "two enabled accounts" and both disable one.
    #
    # Note what is deliberately absent: there is no way to delete an account.
    # Removal is disablement, and the row stays forever. That keeps the account
    # count monotone, which is what stops an install that briefly had two
    # accounts from falling back into the single-account relaxations — a
    # grandfathered ``sub: "user"`` token would otherwise start resolving again,
    # to whichever account happened to remain. The row is the tombstone.
    # ``actor_refs`` rows are never deleted either (0.9): attribution written by
    # PR 4 references them permanently.

    async def login_state(self) -> LoginState:
        """The account-shaped facts that decide how a caller may log in.

        One read, one predicate — see :class:`LoginState`. ``passwordless``
        keys off ``credential_source = 'none'``; a ``config`` row counts as
        having a credential because the startup mirror keeps that value in step
        with ``auth.password_hash`` (a row is only left on ``config`` while one
        is configured), and PR 3's startup migration moves every such row to
        ``local`` anyway.
        """
        accounts = await self.list_accounts()
        if len(accounts) != 1:
            return LoginState(
                accounts=len(accounts), single_account=False, passwordless=False,
            )
        sole = accounts[0]
        return LoginState(
            accounts=1,
            single_account=True,
            passwordless=sole["credential_source"] == "none",
            sole_account_id=sole["id"],
        )

    async def create_managed_account(
        self,
        *,
        username: str,
        credential: str,
        display_name: str | None = None,
    ) -> dict:
        """Add a person: one ``actor_refs`` row and one ``accounts`` row, atomically.

        The actor and the account are created in the same transaction. Two DAL
        calls would leave an orphaned actor behind whenever the second failed —
        and an orphaned *human* actor is not inert: it is a row a later bug
        could attach a login to.

        Refuses, inside that transaction:

        * :class:`PasswordlessInstanceError` — the instance is passwordless
          (0.5). Set a password on the existing account first.
        * :class:`UnnamedAccountError` — an existing account has no username,
          so it could not be logged into once this one exists.
        * :class:`UsernameTakenError` / :class:`InvalidUsernameError` /
          :class:`ReservedUsernameError` — see :func:`normalise_username`.

        The new account always carries its own credential (``local``): an
        account with no password is either an open door or unreachable,
        depending on how many accounts there are, and neither is worth creating.
        """
        username = normalise_username(username)
        if not credential:
            raise AccountError("A new account needs a password")

        actor_id, account_id = new_id(), new_id()
        async with self._atomic():
            # The write lock up front: the guards below are read-then-write, and
            # a deferred transaction would let two callers both read a state
            # that permits the insert and then both perform it.
            await self.db.execute("BEGIN IMMEDIATE")

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
                """INSERT INTO actor_refs
                       (id, kind, display_name, email, profile_version,
                        created_at, updated_at)
                   VALUES (?, 'human', ?, NULL, 1, ?, ?)""",
                (actor_id, display_name, now, now),
            )
            try:
                await self.db.execute(
                    """INSERT INTO accounts
                           (id, actor_id, username, credential_source, credential,
                            enabled, created_at, updated_at, disabled_at)
                       VALUES (?, ?, ?, 'local', ?, 1, ?, ?, NULL)""",
                    (account_id, actor_id, username, credential, now, now),
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
    ) -> dict | None:
        """Change what an account logs in *as* and *with*, in one transaction.

        Either or both. Setting a credential moves the row to
        ``credential_source = 'local'``, so a password set here supersedes
        ``auth.password_hash`` for this account and the configuration value
        stops applying to it.

        Both at once is PR 6's "claim and secure" step: the sole passwordless,
        unnamed account gets a username and a password together, so a
        half-claimed account — named but still open, or secured but unreachable
        — never exists, not even between two requests.

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
        if not sets:
            return await self.get_account(account_id)

        async with self._atomic():
            await self.db.execute("BEGIN IMMEDIATE")
            async with self.db.execute(
                "SELECT 1 FROM accounts WHERE id = ?", (account_id,)
            ) as cursor:
                if await cursor.fetchone() is None:
                    return None
            sets.append("updated_at = ?")
            params.extend([_now(), account_id])
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
        """Swap one stored hash for another, only if it is still the one seen.

        A compare-and-swap, and the comparison is the point. The one caller is
        the opportunistic re-hash on the login path, whichreads a credential,
        verifies a password against it, and then writes a replacement — three
        steps with room between them for the account's owner to change their
        password from another tab. An unconditional write would put the *old*
        password back, silently, and leave whoever knew it still able to log in.

        Conditioned on ``credential_source`` too, so a row that has moved off
        its own credential in the meantime (back to the configured one, say) is
        left alone rather than dragged back to ``local``.

        Returns whether a row changed. ``False`` is a benign no-op: something
        else got there first, and what it wrote is newer than what this had.
        """
        if not expected or not credential:
            raise AccountError("both the expected and the new credential are required")
        result = await self._write(
            """UPDATE accounts
                  SET credential = ?, updated_at = ?
                WHERE id = ? AND credential = ? AND credential_source = 'local'""",
            (credential, _now(), account_id, expected),
        )
        return result.rowcount > 0

    async def claim_sole_account(
        self,
        *,
        username: str,
        credential: str,
        display_name: str | None = None,
    ) -> dict:
        """Name and secure the one unclaimed account, in a single transaction.

        First-run "claim and secure": the account an install is created with has
        no password and no username, and this is what gives it both. One
        transaction, so a half-claimed account — named but still open, or
        secured but unreachable — never exists, not even between two requests.

        The *precondition* is checked inside the transaction as well, which is
        the difference between this and ``get_sole_account()`` followed by
        ``update_account_login()``: those are two transactions, so two callers
        racing to claim a fresh install can both read "one account, no
        password" and the second one silently overwrites the first's password
        with its own. Under ``BEGIN IMMEDIATE`` the loser reads the winner's
        committed row and raises :class:`NotClaimableError`.

        Raises :class:`NotClaimableError` unless exactly one account exists and
        it has no credential, and the username errors of
        :func:`normalise_username`. ``display_name`` also renames the account's
        actor, in the same transaction.

        Note what this does **not** do: decide who may call it. A passwordless
        install admits everybody, so the caller is responsible for the guard
        that makes claiming meaningful (a setup token, or proof that the request
        came from the machine itself).
        """
        username = normalise_username(username)
        if not credential:
            raise AccountError("A password is required")

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

            now = _now()
            try:
                await self.db.execute(
                    """UPDATE accounts
                          SET username = ?, credential_source = 'local',
                              credential = ?, updated_at = ?
                        WHERE id = ?""",
                    (username, credential, now, account["id"]),
                )
            except sqlite3.IntegrityError as e:  # pragma: no cover - one account
                raise UsernameTakenError(
                    f"The username '{username}' is already taken"
                ) from e
            if display_name is not None:
                await self.db.execute(
                    """UPDATE actor_refs
                          SET display_name = ?,
                              profile_version = profile_version + 1,
                              updated_at = ?
                        WHERE id = ?""",
                    (display_name or None, now, account["actor_id"]),
                )
        return await self.get_account(account["id"])  # type: ignore[return-value]

    async def disable_account(self, account_id: str) -> dict | None:
        """Disable an account unless it is the last enabled one.

        The lockout guard (0.4), enforced by the statement itself as well as by
        the surrounding transaction: the ``UPDATE`` carries the "more than one
        enabled account" condition in its ``WHERE``, so even a caller that
        somehow reached it with a stale count cannot make it fire. Raises
        :class:`LastAccountError` rather than reporting a count, so no caller
        can forget to look.

        Idempotent: disabling an already-disabled account returns it unchanged
        (and is never the lockout case — nothing changes).

        Returns ``None`` if there is no such account. The row is *never*
        removed; see the note at the top of this section.
        """
        async with self._atomic():
            await self.db.execute("BEGIN IMMEDIATE")
            account = await self.get_account(account_id)
            if account is None:
                return None
            if not account["enabled"]:
                return account
            now = _now()
            cursor = await self.db.execute(
                """UPDATE accounts
                      SET enabled = 0, updated_at = ?, disabled_at = COALESCE(disabled_at, ?)
                    WHERE id = ? AND enabled = 1
                      AND (SELECT COUNT(*) FROM accounts WHERE enabled = 1) > 1""",
                (now, now, account_id),
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

    async def enable_account(self, account_id: str) -> dict | None:
        """Re-enable an account. Idempotent; ``None`` if there is no such account."""
        async with self._atomic():
            await self.db.execute("BEGIN IMMEDIATE")
            account = await self.get_account(account_id)
            if account is None:
                return None
            if account["enabled"]:
                return account
            await self.db.execute(
                """UPDATE accounts
                      SET enabled = 1, updated_at = ?, disabled_at = NULL
                    WHERE id = ?""",
                (_now(), account_id),
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
