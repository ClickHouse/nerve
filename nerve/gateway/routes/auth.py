"""Authentication routes."""

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
from nerve.gateway.routes._deps import get_deps
from nerve.gateway.routes.accounts import (
    AccountOut,
    _render,
    account_credential,
    instance_is_passwordless,
)
from nerve.gateway.routes.actors import actor_out
from nerve.identity import Actor, ActorResolutionError, actor_for_account

logger = logging.getLogger(__name__)

router = APIRouter()

# Do not reveal whether a username exists.
_INVALID = "Invalid username or password"

LOGIN_NONE = "none"
LOGIN_PASSWORD = "password"
LOGIN_USERNAME_PASSWORD = "username_password"

# Require both fields until authentication is ready.
_UNKNOWN_STATUS = {
    "auth_required": True,
    "login": LOGIN_USERNAME_PASSWORD,
}

# Compare unknown usernames against a fixed, unusable hash to keep login work
# independent of account existence. Its cost matches BCRYPT_COST.
_DECOY_HASH = "$2b$12$WSa90bUaYgZg94/cwtxqZuKQBrzC2BJA1MSEO/le348QlaMVKoaty"

# Pad failures to the slowest active bcrypt cost. This prevents hash cost from
# revealing whether a username exists.
_FAILURE_BUDGET_FLOOR_SECONDS = 0.05
# Limit adjustments caused by runtime jitter.
_REACTIVE_BUDGET_CEILING_SECONDS = 2.0
# Avoid making all failed logins unusable for a pathological stored cost.
_CALIBRATED_BUDGET_CEILING_SECONDS = 30.0
# Absorb normal comparison-time variation.
_FAILURE_BUDGET_HEADROOM = 1.25
_failure_budget: float | None = None
_calibrated_budget: float | None = None
# Cache the machine-dependent policy-cost measurement, not account-dependent
# costs, which may change after a configuration reload or password update.
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
    """Set the login-failure timing budget from current credential costs."""
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
    """Wait out the timing budget and return a login error."""
    remaining = (_failure_budget or 0.0) - (time.monotonic() - started_at)
    if remaining > 0:
        await asyncio.sleep(remaining)
    return HTTPException(status_code=401, detail=detail)


async def _maybe_upgrade_hash(store, account: dict, password: str) -> None:
    """Replace a verified local hash at an outdated cost."""
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
    username: str | None = None


class LoginResponse(BaseModel):
    token: str


@router.post("/api/auth/login", response_model=LoginResponse)
async def login(req: LoginRequest):
    """Authenticate a local account and return a session token."""
    config = get_config()
    secret = effective_jwt_secret(config)
    if not secret:
        # Startup normally generates or loads the signing secret.
        raise HTTPException(
            status_code=503,
            detail="No session signing secret is available yet; restart the "
            "gateway so one is generated, or set auth.jwt_secret.",
        )

    store = identity_store()
    if store is None:
        raise HTTPException(status_code=503, detail=NO_IDENTITY_DETAIL)

    # Calibrate before starting the request timer so the first login is not
    # distinguishable from later attempts.
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
        account = None

    if account is None:
        _timed_verify(req.password, _DECOY_HASH)
        raise await _refuse(started_at, _INVALID)

    credential = account_credential(account, config)
    if credential:
        if not _timed_verify(req.password, credential):
            raise await _refuse(started_at, _INVALID)
    elif not passwordless:
        # Multi-account restores without secrets have no usable credential.
        _timed_verify(req.password, _DECOY_HASH)
        raise await _refuse(started_at, _INVALID)
    # A sole passwordless account accepts any password.

    try:
        actor: Actor = await actor_for_account(store, account["id"])
    except ActorResolutionError as e:
        # Reveal account state only after the credential is verified.
        raise await _refuse(started_at, str(e)) from e

    if credential:
        await _maybe_upgrade_hash(store, account, req.password)

    # Use the account's current epoch so a post-claim login is immediately valid.
    return LoginResponse(token=create_session_token(
        secret, actor.account_id, session_epoch=account.get("session_epoch") or 0,
    ))


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


class ViewerActor(BaseModel):
    """One actor, in the shape ``GET /api/actors`` returns."""

    id: str
    kind: str
    display_name: str | None


class ViewerResponse(BaseModel):
    actor: ViewerActor
    account: AccountOut | None


@router.get("/api/auth/me", response_model=ViewerResponse)
async def who_am_i(actor: Actor = Depends(require_auth)):
    """The actor this request acts as, and its local account if it has one.

    The web UI compares message and session authors with ``actor``.
    ``account`` is ``None`` for a credential without an account, such as the
    system principal's.
    """
    db = get_deps().db
    ref = await db.get_actor_ref(actor.actor_id)
    if ref is None:  # pragma: no cover - require_auth resolved it a moment ago
        raise HTTPException(status_code=401, detail="This credential names no actor")
    account = await db.get_account(actor.account_id) if actor.account_id else None
    return ViewerResponse(
        actor=ViewerActor(**actor_out(ref)),
        account=await _render(db, account) if account else None,
    )
