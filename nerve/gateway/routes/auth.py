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
from nerve.db.accounts import login_state_from
from nerve.gateway.routes.accounts import account_credential, instance_is_passwordless
from nerve.identity import Actor, ActorResolutionError, actor_for_account

logger = logging.getLogger(__name__)

router = APIRouter()

# One message for every way a login can fail on credentials. A caller must not
# be able to tell "no such username" from "wrong password": the first answer
# would turn this endpoint into a list of who works here.
_INVALID = "Invalid username or password"

# What the login form must collect.
LOGIN_NONE = "none"                        # passwordless: any password, one account
LOGIN_PASSWORD = "password"                # one account: password only, no username
LOGIN_USERNAME_PASSWORD = "username_password"   # two or more: a username is required

# Fail-closed descriptor. Used before startup has wired identity storage or
# pinned a signing secret — never a shape that tells a browser to log itself in.
_UNKNOWN_STATUS = {
    "auth_required": True,
    "login": LOGIN_USERNAME_PASSWORD,
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
# ...and a ceiling for the calibrated value all the same.
#
# **The cap question, decided.** Any work factor is *accepted* — refusing to
# verify a hash would lock out the install that carries it, which is the one
# thing this release promises not to do. So the ceiling is not a limit on what
# may be stored; it is the point past which the padding stops pretending. A
# comparison slower than this takes longer than the budget however long the
# budget is, and an account whose password takes half a minute to check is
# distinguishable by timing no matter what — while also being an account nobody
# can log into in a reasonable time. It is a misconfiguration to fix, not a case
# to keep padding for, and `docs/accounts.md` says so. Everything below the
# ceiling — which is every work factor bcrypt is used at in practice — is fully
# masked.
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
# One comparison at the policy cost, on this machine. The *only* thing cached
# across logins: it is a property of the hardware, while the slowest cost in use
# is a property of the accounts, and those change under a running gateway — a
# `config` or `none` row reads `auth.password_hash` the moment a reload swaps
# it, so a budget calibrated once at first login goes stale the instant somebody
# introduces a slower hash, and stays stale until a *known-user* probe raises
# the reactive mark. Which is one probe too late, again.
_policy_comparison_seconds: float | None = None
def _slowest_stored_cost(accounts: list[dict], configured_password: str) -> int:
    """Highest active bcrypt cost, never below the decoy's policy cost."""
    candidates = [
        bcrypt_cost(account["credential"] or "")
        for account in accounts
        if account["credential_source"] == "local"
    ]
    candidates.append(bcrypt_cost(configured_password or ""))
    return max([cost for cost in candidates if cost is not None] + [BCRYPT_COST])


def prepare_login_timing(config, accounts: list[dict]) -> float:
    """Set a common failure budget before the request clock starts.

    The machine's policy-cost measurement is cached, while active costs are
    reread every login so configuration reloads and rehashes take effect.
    """
    global _failure_budget, _calibrated_budget, _policy_comparison_seconds

    if _policy_comparison_seconds is None:
        started = time.monotonic()
        verify_password("measuring the login response budget", _DECOY_HASH)
        _policy_comparison_seconds = time.monotonic() - started

    slowest = _slowest_stored_cost(accounts, config.auth.password_hash)
    calibrated = min(
        _CALIBRATED_BUDGET_CEILING_SECONDS,
        max(
            _FAILURE_BUDGET_FLOOR_SECONDS,
            _policy_comparison_seconds * (2 ** (slowest - BCRYPT_COST))
            * _FAILURE_BUDGET_HEADROOM,
        ),
    )
    if slowest != BCRYPT_COST and calibrated != _calibrated_budget:
        logger.info(
            "Login failure budget is %.2fs: a credential in use is at bcrypt "
            "cost %d rather than %d. It drops back once that account's owner "
            "next signs in, which re-hashes it at the current cost.",
            calibrated, slowest, BCRYPT_COST,
        )
    _calibrated_budget = calibrated
    _failure_budget = calibrated
    return _failure_budget


def _reactive_ceiling() -> float:
    base = _calibrated_budget or _REACTIVE_BUDGET_CEILING_SECONDS
    return max(_REACTIVE_BUDGET_CEILING_SECONDS, base * 2)


def _observe_comparison(seconds: float) -> None:
    """Boundedly raise the budget when reality exceeds calibration."""
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
    """Silently converge a verified local hash with compare-and-swap.

    Configuration-owned and overlong legacy passwords are left for their
    dedicated migration/compatibility paths.
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

    # One read of the accounts, used for both: which login shape applies, and
    # the slowest work factor a comparison could take. Before the clock starts,
    # never inside it — calibration costs a comparison the first time, and
    # paying for that within a request's own timed window is what made the first
    # failure of a process stand out from every later one.
    accounts = await store.list_accounts()
    prepare_login_timing(config, accounts)
    started_at = time.monotonic()

    state = login_state_from(accounts)
    passwordless = instance_is_passwordless(state, config)
    username = (req.username or "").strip()

    if username:
        account = await store.get_account_by_username(username)
    elif state.single_account:
        account = accounts[0]
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
    """Describe the login form without identifying accounts.

    ``auth_required`` is the legacy spelling of ``login != 'none'``. Missing
    startup state fails closed to username and password.
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
        "login": login_kind,
    }


@router.get("/api/auth/check", dependencies=[Depends(require_auth)])
async def check_auth():
    return {"authenticated": True}
