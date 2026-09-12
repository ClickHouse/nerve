"""Account management routes.

Every local account has full permissions (0.4): any account may list, create,
rename, disable and re-enable accounts, and the consequence is worth stating
rather than discovering — **adding a person gives them the power to remove
you.** That is the chosen property for a trusted-team self-hosted install, not
an oversight. The only guard is that the last enabled account cannot be
disabled, which is what stops an install locking everybody out.

Three rules are enforced in the data layer rather than here, because they are
read-then-write and this is where a check-then-act race would live (see
``nerve.db.accounts``): the passwordless guard, the "an existing account has no
username" guard and the last-account guard. This module turns their exceptions
into status codes and does nothing else about them.

**Nothing here returns a credential.** Account rows come back from the store as
plain dicts including ``credential``; :func:`_account_out` builds the response
field by field from an allowlist, so adding a column to the table cannot start
publishing it.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from nerve.db.accounts import (
    AccountError,
    LastAccountError,
    PasswordlessInstanceError,
    UnnamedAccountError,
    UsernameTakenError,
)
from nerve.gateway.auth import hash_password, require_auth, verify_password
from nerve.gateway.routes._deps import get_deps
from nerve.identity import Actor

logger = logging.getLogger(__name__)

router = APIRouter()

# 409 rather than 400 for the three state guards: the request is well-formed and
# would be accepted against a different state of the instance, which is what
# "conflict" means and what tells a UI to re-read and explain rather than to
# re-validate the form.
_CONFLICT = (PasswordlessInstanceError, UnnamedAccountError, UsernameTakenError,
             LastAccountError)


class AccountOut(BaseModel):
    """One account, as an authenticated caller may see it.

    Explicitly not here: ``credential`` (the bcrypt hash) and
    ``credential_source`` (where it lives). ``has_password`` is the only thing
    a UI needs from either, and it is a boolean.
    """

    id: str
    username: str | None
    display_name: str | None
    enabled: bool
    has_password: bool
    created_at: str
    updated_at: str
    disabled_at: str | None
    # Whether this row is the caller's own — the accounts screen uses it to
    # label the row and to hide "disable yourself" behind a confirmation.
    is_self: bool


class AccountListResponse(BaseModel):
    accounts: list[AccountOut]


class AccountCreateRequest(BaseModel):
    username: str
    password: str = Field(min_length=1)
    display_name: str | None = None


class AccountPatchRequest(BaseModel):
    username: str | None = None
    display_name: str | None = None


class PasswordChangeRequest(BaseModel):
    # Absent only while the account has no password yet: an account that has one
    # must prove it, so a stolen session token alone cannot change it.
    current_password: str | None = None
    new_password: str = Field(min_length=1)


def _account_out(account: dict, actor_display: str | None, *, actor: Actor) -> AccountOut:
    return AccountOut(
        id=account["id"],
        username=account["username"],
        display_name=actor_display,
        enabled=bool(account["enabled"]),
        has_password=account["credential_source"] != "none",
        created_at=account["created_at"],
        updated_at=account["updated_at"],
        disabled_at=account["disabled_at"],
        is_self=account["id"] == actor.account_id,
    )


async def _render(db, account: dict, *, actor: Actor) -> AccountOut:
    ref = await db.get_actor_ref(account["actor_id"])
    return _account_out(account, ref["display_name"] if ref else None, actor=actor)


async def require_account(actor: Actor = Depends(require_auth)) -> Actor:
    """Require a request made *by a person*, not by the instance itself.

    ``require_auth`` also admits the agent's system principal: the credential
    the CLI and the in-process agent tool mint for themselves. Those act
    autonomously (0.6) and have no account, so "every account may manage
    accounts" does not reach them — and keeping account administration off that
    credential means a prompt that talks the agent into calling its own API
    cannot mint itself a login.
    """
    if not actor.is_human or not actor.account_id:
        raise HTTPException(
            status_code=403,
            detail="Account management is for signed-in accounts; the agent's "
                   "system principal has none.",
        )
    return actor


@router.get("/api/accounts", response_model=AccountListResponse)
async def list_accounts(actor: Actor = Depends(require_account)):
    """Every account, oldest first — disabled ones included.

    A disabled account is still a row on purpose: it is the tombstone that keeps
    an install which once had two accounts from sliding back into the
    single-account relaxations (see ``nerve.db.accounts``).
    """
    db = get_deps().db
    accounts = await db.list_accounts()
    refs = {ref["id"]: ref for ref in await db.list_actor_refs(kind="human")}
    return AccountListResponse(accounts=[
        _account_out(
            account,
            (refs.get(account["actor_id"]) or {}).get("display_name"),
            actor=actor,
        )
        for account in accounts
    ])


@router.get("/api/accounts/me", response_model=AccountOut)
async def get_own_account(actor: Actor = Depends(require_account)):
    db = get_deps().db
    account = await db.get_account(actor.account_id)
    if account is None:  # pragma: no cover - require_auth resolved it a moment ago
        raise HTTPException(status_code=404, detail="Account not found")
    return await _render(db, account, actor=actor)


@router.post("/api/accounts", response_model=AccountOut, status_code=201)
async def create_account(req: AccountCreateRequest, actor: Actor = Depends(require_account)):
    """Add a person.

    Refused while the instance is passwordless (0.5) or while any existing
    account has no username — both 409 with a message saying what to do first,
    because both describe the instance rather than the request.
    """
    db = get_deps().db
    try:
        account = await db.create_managed_account(
            username=req.username,
            credential=hash_password(req.password),
            display_name=(req.display_name or None),
        )
    except _CONFLICT as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except AccountError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    logger.info(
        "Account %s created by account %s (%d accounts now)",
        account["id"], actor.account_id, await db.count_accounts(),
    )
    return await _render(db, account, actor=actor)


@router.patch("/api/accounts/{account_id}", response_model=AccountOut)
async def update_account(
    account_id: str,
    req: AccountPatchRequest,
    actor: Actor = Depends(require_account),
):
    """Set a username and/or a display name.

    A username is a lookup key, not an identity (0.7): renaming changes what the
    account logs in as and what is shown, and moves nothing that was stored —
    ``actor_refs.id`` is untouched, so every session and message already
    attributed to this person stays attributed to them.

    This is also how the account an upgrade created — which has no username —
    gets one, which it must before a second account can exist.
    """
    db = get_deps().db
    account = await db.get_account(account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")

    if req.username is not None:
        try:
            account = await db.update_account_login(account_id, username=req.username)
        except _CONFLICT as e:
            raise HTTPException(status_code=409, detail=str(e)) from e
        except AccountError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        if account is None:  # pragma: no cover - removed between two reads
            raise HTTPException(status_code=404, detail="Account not found")

    if req.display_name is not None:
        await db.update_actor_profile(
            account["actor_id"], display_name=(req.display_name.strip() or None),
        )
    return await _render(db, account, actor=actor)


@router.post("/api/accounts/{account_id}/disable", response_model=AccountOut)
async def disable_account(account_id: str, actor: Actor = Depends(require_account)):
    """Disable an account. Idempotent; 409 if it is the last enabled one.

    Takes effect at the account's *next* request, not retroactively: a token
    issued before this is still signed and unexpired, and the account row is
    what stops it — at every door. An open WebSocket keeps the identity it was
    accepted with until it reconnects.
    """
    db = get_deps().db
    try:
        account = await db.disable_account(account_id)
    except LastAccountError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    logger.info("Account %s disabled by account %s", account_id, actor.account_id)
    return await _render(db, account, actor=actor)


@router.post("/api/accounts/{account_id}/enable", response_model=AccountOut)
async def enable_account(account_id: str, actor: Actor = Depends(require_account)):
    """Re-enable an account. Idempotent."""
    db = get_deps().db
    account = await db.enable_account(account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    logger.info("Account %s enabled by account %s", account_id, actor.account_id)
    return await _render(db, account, actor=actor)


@router.put("/api/accounts/me/password", response_model=AccountOut)
async def change_own_password(
    req: PasswordChangeRequest, actor: Actor = Depends(require_account),
):
    """Set your own password. Nobody can set anyone else's.

    ``current_password`` is required whenever the account already has one —
    from its own row or from ``auth.password_hash`` — so a stolen session token
    is not on its own enough to take the account over. The account that has
    none (a passwordless install, before anyone has set one) is the one case
    that may set a first password without proving anything beyond being signed
    in, which is also what it has to do before a second account can exist.

    Setting a password moves the account to its own credential, after which
    ``auth.password_hash`` no longer applies to it.
    """
    from nerve.config import get_config

    db = get_deps().db
    account = await db.get_account(actor.account_id)
    if account is None:  # pragma: no cover - resolved a moment ago
        raise HTTPException(status_code=404, detail="Account not found")

    existing = account_credential(account, get_config())
    if existing:
        if not req.current_password or not verify_password(req.current_password, existing):
            raise HTTPException(status_code=403, detail="Current password is incorrect")

    try:
        updated = await db.update_account_login(
            account["id"], credential=hash_password(req.new_password),
        )
    except AccountError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if updated is None:  # pragma: no cover - removed between two reads
        raise HTTPException(status_code=404, detail="Account not found")
    logger.info("Account %s changed its own password", account["id"])
    return await _render(db, updated, actor=actor)


def account_credential(account: dict, config) -> str:
    """The bcrypt hash this account authenticates against, or ``""``.

    One function so the login route and the password change agree about where an
    account's credential lives:

    * ``local`` — the hash on the row (what every account has after PR 3's
      startup migration);
    * ``config`` — ``auth.password_hash``, still read for one release so a
      downgrade to code that only knows the configuration value still
      authenticates. The startup migration empties this case out;
    * ``none`` — passwordless: no hash, and admission is decided by the caller
      (only ever while exactly one account exists).
    """
    source = account["credential_source"]
    if source == "local":
        return account["credential"] or ""
    if source == "config":
        return config.auth.password_hash or ""
    return ""
