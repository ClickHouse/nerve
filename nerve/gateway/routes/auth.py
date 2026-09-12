"""Authentication routes: logging in, and what an anonymous caller may know."""

from __future__ import annotations

import asyncio
import logging
import time

from fastapi import APIRouter, Depends
from fastapi import HTTPException
from pydantic import BaseModel

from nerve.config import get_config
from nerve.gateway.auth import (
    BCRYPT_COST,
    NO_IDENTITY_DETAIL,
    bcrypt_cost,
    create_session_token,
    effective_jwt_secret,
    hash_password,
    identity_store,
    needs_rehash,
    password_length_problem,
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

# The hash compared against when the username names no account.
#
# Without it a request for a username that does not exist returns before any
# hashing happens, while one that does exist pays for a bcrypt comparison —
# which is a list of who works here, measured with a stopwatch.
#
# A *constant* rather than something generated, and that is the point:
# generating it lazily made the first unknown-username request per process pay
# for a hash **and** a comparison, so the leak this closes was still open once
# per process; generating it at import made every CLI command pay a quarter of a
# second for a web-login detail. Every request now performs exactly one
# comparison, whether or not the username exists.
#
# This is not a credential. It is the bcrypt hash of a random string that was
# generated once, used to produce this line and discarded; nothing knows the
# plaintext, and nothing anywhere accepts this hash as a password — it is only
# ever the right-hand side of a comparison that is about to fail. Cost 12 is
# this release's policy cost (nerve.gateway.auth.BCRYPT_COST), so it takes the
# same time as a comparison against a hash this release wrote.
_DECOY_HASH = "$2b$12$WSa90bUaYgZg94/cwtxqZuKQBrzC2BJA1MSEO/le348QlaMVKoaty"

# How long a failed login takes, at a minimum.
#
# The decoy equalises *whether* a comparison happens. It does not equalise how
# long one takes, and it cannot: bcrypt's work factor is a property of the
# stored hash, and the startup migration copies a configured hash byte for byte
# from whatever produced it — so one account can legitimately be cost 4 (1 ms)
# while the decoy is cost 12 (260 ms). Measuring the difference says the account
# exists just as loudly as measuring whether any hashing happened at all.
#
# So every failure is padded to a common budget, and the budget is calibrated
# against the slowest work factor this install *actually stores* — not against
# the decoy. Reacting to a slow comparison after making it is too late: the
# request that discovered a cost-14 account already took a second while an
# unknown username took a quarter of one, and that first probe is all an
# enumeration needs. So before the first login is served the accounts are read,
# the highest cost among them is taken, and the budget is derived for that cost.
#
# Derived rather than measured at that cost: bcrypt's work is exactly 2**cost
# iterations, so one cost-12 comparison and a doubling per step above it gives
# the number exactly, without hashing at a cost that could take seconds.
#
# The reactive high-water mark stays as a backstop for whatever the calibration
# could not know — a credential stored at a higher cost after startup, or a
# machine that is simply slower now than it was.
#
# The remedy for the underlying spread is upgrading the hashes, which
# _maybe_upgrade_hash does on each owner's next successful login; the budget is
# what holds the line until then, and it comes *down* as an install converges,
# because it is recalibrated at each process start.
_FAILURE_BUDGET_FLOOR_SECONDS = 0.05
# Ceiling for the *reactive* mark, so one scheduling hiccup cannot make every
# later refusal crawl. Not applied to the calibrated value: that one is derived
# from a work factor an account really carries, and clamping it below the
# comparison it exists to hide would simply put the enumeration back.
_REACTIVE_BUDGET_CEILING_SECONDS = 2.0
# ...and a ceiling for the calibrated value all the same, because a work factor
# nobody could log in with in under half a minute is a misconfiguration, not a
# case to keep padding for.
_CALIBRATED_BUDGET_CEILING_SECONDS = 30.0
# The budget sits this far above the comparison it was measured from. Without
# the headroom it lands exactly on one, ordinary variation in the next
# comparison steps over it, and the high-water mark ratchets the budget up over
# a process's life — which does not say which account was named, but does make a
# process's later failures slower than its earlier ones for no reason. With it,
# real comparisons stay underneath and the budget settles on one value.
_FAILURE_BUDGET_HEADROOM = 1.25
_failure_budget: float | None = None
# What calibration alone produced, kept apart from the reactive mark so the
# latter's ceiling can be expressed relative to it.
_calibrated_budget: float | None = None


def _slowest_stored_cost(accounts: list[dict], configured_password: str) -> int:
    """The highest bcrypt work factor this install can be asked to verify.

    Every account's own hash, plus the configured one that ``config`` and
    ``none`` rows read. Never below the policy cost, which is what the decoy —
    and therefore every unknown-username comparison — costs.
    """
    candidates = [
        bcrypt_cost(account["credential"] or "")
        for account in accounts
        if account["credential_source"] == "local"
    ]
    candidates.append(bcrypt_cost(configured_password or ""))
    return max([cost for cost in candidates if cost is not None] + [BCRYPT_COST])


async def prepare_login_timing(store, config) -> float:
    """Calibrate the failure budget, once per process. Returns it.

    Called at the top of :func:`login`, **before** that request takes its own
    start time: the measurement costs a comparison, and paying for it inside a
    request's timed window is what made the first failure of a process stand out
    from every later one. A future startup hook may call it earlier — it is
    idempotent and cheap after the first time — which would move that one cost
    off the first login.
    """
    global _failure_budget, _calibrated_budget
    if _failure_budget is not None:
        return _failure_budget

    started = time.monotonic()
    verify_password("measuring the login response budget", _DECOY_HASH)
    one_policy_comparison = time.monotonic() - started

    try:
        accounts = await store.list_accounts()
    except Exception as e:  # noqa: BLE001 - calibration must not fail a login
        logger.warning("Could not read accounts to calibrate login timing: %s", e)
        accounts = []
    slowest = _slowest_stored_cost(accounts, config.auth.password_hash)

    _calibrated_budget = min(
        _CALIBRATED_BUDGET_CEILING_SECONDS,
        max(
            _FAILURE_BUDGET_FLOOR_SECONDS,
            one_policy_comparison * (2 ** (slowest - BCRYPT_COST))
            * _FAILURE_BUDGET_HEADROOM,
        ),
    )
    _failure_budget = _calibrated_budget
    if slowest != BCRYPT_COST:
        logger.info(
            "Login failure budget calibrated to %.2fs: an account is stored at "
            "bcrypt cost %d rather than %d. It drops back once that account's "
            "owner next signs in, which re-hashes it at the current cost.",
            _failure_budget, slowest, BCRYPT_COST,
        )
    return _failure_budget


def _reactive_ceiling() -> float:
    base = _calibrated_budget or _REACTIVE_BUDGET_CEILING_SECONDS
    return max(_REACTIVE_BUDGET_CEILING_SECONDS, base * 2)


def _set_failure_budget(seconds: float | None) -> None:
    """Pin (or clear) the budget. For tests, which cannot afford a quarter of a
    second per failed login and need a known value to measure against."""
    global _failure_budget, _calibrated_budget
    _failure_budget = seconds
    _calibrated_budget = seconds


def _observe_comparison(seconds: float) -> None:
    """Raise the budget to cover a comparison that took longer than it.

    The backstop behind calibration: a credential stored at a higher cost after
    startup, or a machine that has become slower, would otherwise make its own
    failures stand out. Bounded, so one bad moment cannot make every later
    refusal crawl.
    """
    global _failure_budget
    if _failure_budget is not None and seconds > _failure_budget:
        _failure_budget = min(
            _reactive_ceiling(),
            max(_FAILURE_BUDGET_FLOOR_SECONDS, seconds * _FAILURE_BUDGET_HEADROOM),
        )


def _timed_verify(plain: str, hashed: str) -> bool:
    """``verify_password``, with the comparison's cost fed to the budget."""
    started = time.monotonic()
    try:
        return verify_password(plain, hashed)
    finally:
        _observe_comparison(time.monotonic() - started)


async def _refuse(started_at: float, detail: str) -> HTTPException:
    """Wait out the response budget, then hand back the refusal to raise.

    Returned rather than raised so that every failure reads ``raise await
    _refuse(...)`` at the point it happens — a helper that raises leaves the
    code after it looking reachable when it is not.
    """
    remaining = (_failure_budget or 0.0) - (time.monotonic() - started_at)
    if remaining > 0:
        await asyncio.sleep(remaining)
    return HTTPException(status_code=401, detail=detail)


async def _maybe_upgrade_hash(store, account: dict, password: str) -> None:
    """Re-hash a just-verified password at the policy cost, if it is not already.

    Opportunistic and silent: it happens on a login that has already succeeded,
    it writes through the ordinary credential path, and nothing about it reaches
    the client. This is what drains an install of the assorted work factors a
    migrated configuration hash can bring with it — which is what makes the
    response budget a transitional measure rather than a permanent one.

    Skipped for an account whose credential lives in configuration (moving it
    onto the row is the startup migration's job, not a side effect of somebody
    logging in) and for a password too long to hash, which
    :func:`verify_password` accepted by truncating: re-hashing it would refuse,
    and hashing the truncation would store a different password from the one its
    owner types.

    **Compare-and-swap.** The credential was read, compared against and is now
    being replaced — three steps, with room between them for the account's owner
    to change their password from another tab. Writing unconditionally would put
    the *old* password back and leave whoever knew it still able to log in, so
    the write is conditioned on the hash still being the one this request
    verified against. Losing that race is a no-op: what the other writer stored
    is newer than anything this could produce.
    """
    if account["credential_source"] != "local":
        return
    stored = account["credential"] or ""
    if not needs_rehash(stored):
        return
    if password_length_problem(password):
        return
    try:
        swapped = await store.replace_credential_if_unchanged(
            account["id"], expected=stored, credential=hash_password(password),
        )
    except Exception as e:  # noqa: BLE001 - a failed upgrade must not fail the login
        logger.warning(
            "Could not re-hash account %s at the current cost: %s", account["id"], e,
        )
        return
    if swapped:
        logger.info(
            "Re-hashed account %s at the current bcrypt cost after a successful "
            "login (its stored hash carried an older work factor)", account["id"],
        )
    else:
        logger.info(
            "Skipped re-hashing account %s: its credential changed while this "
            "login was in flight, so the newer one stands", account["id"],
        )


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

    # Before the clock starts, never inside it: calibration costs a comparison
    # and a read of the accounts, and paying for either within a request's own
    # timed window is what made the *first* failure of a process stand out.
    await prepare_login_timing(store, config)
    started_at = time.monotonic()

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
        # Do the work a real comparison would, then refuse on the same budget.
        _timed_verify(req.password, _DECOY_HASH)
        raise await _refuse(started_at, _INVALID)

    credential = account_credential(account, config)
    if credential:
        if not _timed_verify(req.password, credential):
            raise await _refuse(started_at, _INVALID)
    elif not passwordless:
        # The account carries no credential and the instance is not in the
        # passwordless state, so nothing could authenticate this caller. Reached
        # by an account left without a password — a restored `--no-secrets`
        # bundle on a multi-account install, or a `config`-source row whose
        # configured hash has gone. Refuse rather than admit.
        _timed_verify(req.password, _DECOY_HASH)
        raise await _refuse(started_at, _INVALID)
    # Otherwise passwordless with exactly one account: any password is accepted
    # and resolves to it. That is the documented upgrade behaviour (0.7), and it
    # ends by itself the moment a second account exists.

    try:
        actor: Actor = await actor_for_account(store, account["id"])
    except ActorResolutionError as e:
        # A disabled (or vanished) account, learned only after the credential
        # checked out, so this message tells the right person something useful
        # and nobody else anything at all. Padded like every other refusal: the
        # message already says more than the clock could.
        raise await _refuse(started_at, str(e)) from e

    if credential:
        await _maybe_upgrade_hash(store, account, req.password)

    return LoginResponse(token=create_session_token(secret, actor.account_id))


@router.get("/api/auth/status")
async def auth_status():
    """How to log in, and whether this install still needs setting up.

    Unauthenticated, so it publishes only what a login form has to know:

    | Field | Meaning |
    |---|---|
    | ``mode`` | the identity mode — ``local`` in this build |
    | ``login`` | ``none`` (passwordless), ``password`` (one account, no username needed) or ``username_password`` |
    | ``setup_pending`` | nothing has been secured yet: the one account has no password, so every caller is admitted as it |
    | ``multiple_accounts`` | more than one account exists |
    | ``auth_required`` | kept for older clients; ``login != "none"`` |

    ``setup_pending`` equals ``login == "none"`` today, and is a separate field
    on purpose: it is the *question* "is this instance still unsecured", which
    PR 6's wizard owns and may widen (a missing provider credential, say)
    without changing what the login form collects. It deliberately does **not**
    also require the account to be unnamed. The accounts screen can set a
    username on its own, and an install that did that first would otherwise
    stop reporting as pending — losing the warning and the route to the wizard
    — while still admitting every caller, which is the exact state the wizard
    exists to end.

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
        "setup_pending": login_kind == LOGIN_NONE,
        "multiple_accounts": state.accounts > 1,
    }


@router.get("/api/auth/check")
async def check_auth(actor: Actor = Depends(require_auth)):
    return {"authenticated": True}
