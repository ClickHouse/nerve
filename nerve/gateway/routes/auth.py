"""Authentication routes: logging in, and what an anonymous caller may know."""

from __future__ import annotations

import logging
import secrets as _secrets

from fastapi import APIRouter, Depends
from fastapi import HTTPException
from pydantic import BaseModel

from nerve.config import get_config
from nerve.gateway.auth import (
    NO_IDENTITY_DETAIL,
    create_session_token,
    effective_jwt_secret,
    hash_password,
    identity_store,
    require_auth,
    verify_password,
)
from nerve.gateway.routes.accounts import account_credential, instance_is_passwordless
from nerve.identity import Actor, ActorResolutionError, actor_for_account

logger = logging.getLogger(__name__)

router = APIRouter()

# One message for every way a login can fail on credentials. A caller must not
# be able to tell "no such username" from "wrong password": the first answer
# would turn this endpoint into a list of who works here.
_INVALID = "Invalid username or password"

# The identity mode this build implements. Reported so a client can tell a local
# install from a later externally-authenticated one without guessing from which
# fields happen to be present.
AUTH_MODE = "local"

# What the login form must collect.
LOGIN_NONE = "none"                        # passwordless: any password, one account
LOGIN_PASSWORD = "password"                # one account: password only, no username
LOGIN_USERNAME_PASSWORD = "username_password"   # two or more: a username is required

# Fail-closed descriptor. Used before startup has wired identity storage or
# pinned a signing secret — never a shape that tells a browser to log itself in.
_UNKNOWN_STATUS = {
    "auth_required": True,
    "mode": AUTH_MODE,
    "login": LOGIN_USERNAME_PASSWORD,
    "setup_pending": False,
    "multiple_accounts": False,
}

# A bcrypt hash of a random string nobody holds, compared against when the
# username names no account. Without it, a request for a username that does not
# exist returns before any hashing happens and one that does exist pays for a
# bcrypt comparison, which is a list of who works here measured with a
# stopwatch. Built once, lazily: hashing costs a quarter of a second and no
# import should.
_decoy: str | None = None


def _decoy_hash() -> str:
    global _decoy
    if _decoy is None:
        _decoy = hash_password(_secrets.token_urlsafe(32))
    return _decoy


class LoginRequest(BaseModel):
    password: str
    # Absent while exactly one account exists — the upgrade case, where the
    # account has no username to give. See the login docstring.
    username: str | None = None


class LoginResponse(BaseModel):
    token: str


@router.post("/api/auth/login", response_model=LoginResponse)
async def login(req: LoginRequest):
    """Exchange a username and password for a session token.

    **A username is required once a second account exists, and not before.**
    The account an upgrade creates has none — there is no identifier anywhere in
    the old configuration to make one from — so demanding one here would lock
    out every install that upgrades. Instead password-only login stays valid
    while exactly one account exists, which is the same bound that 0.5 puts on
    passwordless access and PR 2 puts on grandfathered session tokens. The
    account-management flow collects a username for the first account before it
    will create a second.

    The order is credential first, account state second: the disabled check
    lives on the way to the actor (``actor_for_account``), so a caller who has
    *not* proved the password cannot learn anything about the account from the
    answer. Every credential failure gives the same 401.
    """
    config = get_config()
    secret = effective_jwt_secret(config)
    if not secret:
        # No secret in configuration and none stored yet. The identity bootstrap
        # generates one before the gateway serves, so this only fires when it
        # has not run. It used to mint a token signed with the literal string
        # "dev-secret" — and skip the password check while at it — which made
        # a missing secret an open instance. Refuse instead.
        raise HTTPException(
            status_code=503,
            detail="No session signing secret is available yet; restart the "
            "gateway so one is generated, or set auth.jwt_secret.",
        )

    store = identity_store()
    if store is None:
        # Nothing to mint a token *for*. A session token names an account, and
        # without the database there is no account to name.
        raise HTTPException(status_code=503, detail=NO_IDENTITY_DETAIL)

    state = await store.login_state()
    passwordless = instance_is_passwordless(state, config)
    username = (req.username or "").strip()

    if username:
        account = await store.get_account_by_username(username)
    elif state.single_account:
        account = await store.get_sole_account()
    else:
        # Two or more accounts and no username: the request names nobody. Same
        # answer as a wrong password, so the count stays unpublished here too.
        account = None

    if account is None:
        # Spend the same time as a real comparison would, then refuse.
        verify_password(req.password or "x", _decoy_hash())
        raise HTTPException(status_code=401, detail=_INVALID)

    credential = account_credential(account, config)
    if credential:
        if not verify_password(req.password, credential):
            raise HTTPException(status_code=401, detail=_INVALID)
    elif not passwordless:
        # The account carries no credential and the instance is not in the
        # passwordless state, so nothing could authenticate this caller. Reached
        # by an account left without a password — a restored `--no-secrets`
        # bundle on a multi-account install, or a `config`-source row whose
        # configured hash has gone. Refuse rather than admit.
        verify_password(req.password or "x", _decoy_hash())
        raise HTTPException(status_code=401, detail=_INVALID)
    # Otherwise passwordless with exactly one account: any password is accepted
    # and resolves to it. That is the documented upgrade behaviour (0.7), and it
    # ends by itself the moment a second account exists.

    try:
        actor: Actor = await actor_for_account(store, account["id"])
    except ActorResolutionError as e:
        # A disabled (or vanished) account, learned only after the credential
        # checked out, so this message tells the right person something useful
        # and nobody else anything at all.
        raise HTTPException(status_code=401, detail=str(e)) from e

    return LoginResponse(token=create_session_token(secret, actor.account_id))


@router.get("/api/auth/status")
async def auth_status():
    """How to log in, and whether this install still needs setting up.

    Unauthenticated, so it publishes only what a login form has to know:

    | Field | Meaning |
    |---|---|
    | ``mode`` | the identity mode — ``local`` in this build |
    | ``login`` | ``none`` (passwordless), ``password`` (one account, no username needed) or ``username_password`` |
    | ``setup_pending`` | the sole account has neither a password nor a username: first-run state |
    | ``multiple_accounts`` | more than one account exists |
    | ``auth_required`` | kept for older clients; ``login != "none"`` |

    Deliberately **not** here: how many accounts there are, and any username.
    The spec sketched an account *count*; the boolean says everything a client
    needs (and is already implied by ``login``), while a count tells an
    anonymous caller how many people work here.

    ``auth_required`` used to be read from ``auth.password_hash``. It cannot be
    any more: after the startup migration the credential lives on the account
    row and the configuration key is gone, and a stale ``false`` computed from
    configuration would tell the browser to log itself in with an empty
    password. It is derived from the accounts now, like everything else here.

    Fails closed: before the gateway has finished starting, the answer is the
    one that makes a client ask for a username and a password.
    """
    config = get_config()
    store = identity_store()
    if store is None or not effective_jwt_secret(config):
        return dict(_UNKNOWN_STATUS)

    state = await store.login_state()
    if instance_is_passwordless(state, config):
        login_kind = LOGIN_NONE
    elif state.single_account:
        login_kind = LOGIN_PASSWORD
    else:
        login_kind = LOGIN_USERNAME_PASSWORD

    return {
        "auth_required": login_kind != LOGIN_NONE,
        "mode": AUTH_MODE,
        "login": login_kind,
        "setup_pending": state.setup_pending and login_kind == LOGIN_NONE,
        "multiple_accounts": state.accounts > 1,
    }


@router.get("/api/auth/check")
async def check_auth(actor: Actor = Depends(require_auth)):
    return {"authenticated": True}
