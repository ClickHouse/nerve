"""The first-run setup wizard: a guarded claim, then a checklist.

Why this exists: ``nerve init`` prompts for a password, but the headless path
Docker uses reads ``NERVE_PASSWORD`` from the environment and defaults it to
empty. An install that omits it lands with one account and no password, and
for those installs the web wizard is the only setup surface anyone will ever
see.

**The account comes first, and that closes the window.** Step one names and
secures the one account this install was created with (0.5, 6.2); every later
step is an ordinary authenticated request. So exactly one endpoint here is
unauthenticated — ``POST /api/setup/claim`` — and the guard in
:mod:`nerve.setup_token` protects that one rather than the whole wizard.

**A checklist, not a linear gate.** Browser wizards get abandoned halfway, so
every step after the account is skippable and re-enterable, and an abandoned
wizard leaves a working instance on defaults with a "finish setup" affordance.
The order of the list is a suggestion; nothing enforces it.

**It is not the settings editor.** First-run decisions only, and only the ones
a running instance can decide for itself: the account, a provider credential,
timezone and display name, a channel token, and which optional crons and sync
sources are on. Deployment shape, the workspace path and the operator's own
keychain are not web decisions — a browser talking to a process inside the VM
cannot see the laptop it is running on.

**Under lockdown it is read-only.** Configuration is fleet-managed there and
machine-local values are environment references, so every write below refuses.
Claiming the account is the exception, and deliberately: it writes to
``nerve.db``, not to configuration, and an unclaimed fleet-managed install that
could never be claimed would stay open forever.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from nerve import boot, daemon, paths, setup_state
from nerve.config import get_config
from nerve.db.accounts import AccountError, NotClaimableError, UsernameTakenError
from nerve.gateway.auth import (
    NO_IDENTITY_DETAIL,
    create_session_token,
    effective_jwt_secret,
    hash_password,
    identity_store,
    password_length_problem,
    require_auth,
)
from nerve.gateway.routes._deps import get_deps
from nerve.gateway.routes.accounts import require_account
from nerve.identity import Actor, ActorResolutionError, actor_for_account
from nerve.setup_state import SetupState
from nerve.setup_token import (
    instance_is_unclaimed,
    invalidate_setup_token,
    peer_host,
    stored_setup_token,
    token_accepted,
    token_is_required,
)
from nerve.setup_writer import (
    CONFIG_YAML_HEADER,
    CronToggle,
    SetupChoices,
    build_config_layers,
    build_config_local,
    leaf_paths,
    list_optional_crons,
    merge_machine_paths,
    merge_private_paths,
    merge_settings_paths,
    set_optional_crons,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# One answer for every way the claim guard can refuse, so a caller cannot tell
# "there is no token" from "that is not the token" — and it says where to find
# the right one, because the person who most often sees it is the operator.
_GUARD_REFUSED = (
    "A setup token is required from this address. It is printed in the server "
    "log when the instance starts (`nerve logs`, or `docker logs` for a "
    "container) and by `nerve status` on the machine itself. A browser running "
    "on the machine needs no token."
)

# Every refused claim waits this long. It equalises the refusals that are about
# the token, and it bounds guessing at a few attempts a second — not a rate
# limiter, but the difference between "guessing is pointless" and "guessing is
# pointless and slow".
_REFUSAL_SECONDS = 0.25

# A step whose value is on disk but not in the running process yet. The
# checklist reports the restart separately; this keeps the step itself from
# reading as unanswered and inviting a second write of the same key.
_WRITTEN_NOT_LIVE = "Saved. It applies when the instance restarts."

# How long the restart helper waits before it signals anything, so the
# response to the request that asked for it is on the wire first.
_RESTART_DELAY_SECONDS = 0.75

# One restart per process. The helper takes this process's pid; a second one
# would race it over the same pid and the same pid file, and there is nothing
# a second restart could achieve that the first is not already doing.
_restart_lock = asyncio.Lock()
_restart_requested = False

_LOCKDOWN_REFUSED = (
    "This instance is in lockdown: its configuration is fleet-managed and is "
    "not written locally. The setup checklist is read-only here — change the "
    "configuration in the workspace repository instead."
)

STEP_ACCOUNT = "account"
STEP_PROVIDER = "provider"
STEP_PROFILE = "profile"
STEP_CHANNELS = "channels"
STEP_AUTOMATION = "automation"

# The checklist, in the order it is offered. ``required`` is true for exactly
# one step: the account. Everything else is a convenience, which is what makes
# abandoning the wizard safe.
_STEPS = (
    (STEP_ACCOUNT, "Claim this instance", True),
    (STEP_PROVIDER, "Provider credential", False),
    (STEP_PROFILE, "Timezone and name", False),
    (STEP_CHANNELS, "Telegram", False),
    (STEP_AUTOMATION, "Automation", False),
)

_SKIPPABLE = {step for step, _title, required in _STEPS if not required}


# --------------------------------------------------------------------------- #
#  Models                                                                      #
# --------------------------------------------------------------------------- #


class ClaimRequest(BaseModel):
    username: str
    password: str = Field(min_length=1)
    display_name: str | None = None
    # Required only when the request did not come from this machine; see
    # nerve.setup_token.
    setup_token: str | None = None


class ClaimResponse(BaseModel):
    """The claim signs you in, so the browser never has to ask for the password
    it just set."""

    token: str
    account_id: str
    actor_id: str
    username: str | None
    display_name: str | None


class SetupStepOut(BaseModel):
    id: str
    title: str
    status: str          # "done" | "skipped" | "pending"
    required: bool
    can_skip: bool
    detail: str = ""


class CronToggleOut(BaseModel):
    id: str
    name: str
    description: str
    enabled: bool


class SetupStateOut(BaseModel):
    """Everything the checklist screen needs, in one read."""

    # The instance still has an unclaimed account: step one is not done and
    # anybody who can reach the gateway is the owner.
    setup_pending: bool
    lockdown: bool
    writable: bool
    read_only_reason: str | None
    restart_pending: bool
    restart_pending_paths: list[str]
    # Every required step done and nothing waiting on a restart.
    finished: bool
    steps: list[SetupStepOut]
    crons: list[CronToggleOut]


class ProviderRequest(BaseModel):
    # Both optional, at least one required: an install may have arrived with
    # one of them already set from the environment.
    anthropic_api_key: str | None = None
    openai_api_key: str | None = None


class ProfileRequest(BaseModel):
    timezone: str | None = None
    display_name: str | None = None


class ChannelsRequest(BaseModel):
    telegram_bot_token: str = Field(min_length=1)
    telegram_allowed_users: list[int] | None = None


class AutomationRequest(BaseModel):
    crons: list[str] = Field(default_factory=list)
    github: bool = False
    gmail: bool = False
    gmail_accounts: list[str] = Field(default_factory=list)
    telegram: bool = False
    telegram_api_id: int | None = None
    telegram_api_hash: str | None = None


class RestartResponse(BaseModel):
    restarting: bool
    method: str
    message: str
    # The generation of the process that accepted the request. The client
    # polls /health until it reports a different one; anything else accepts
    # the process being replaced, which answers until the moment it stops.
    boot: str


# --------------------------------------------------------------------------- #
#  Step one: claim and secure                                                  #
# --------------------------------------------------------------------------- #


@router.post("/api/setup/claim", response_model=ClaimResponse)
async def claim(req: ClaimRequest, request: Request):
    """Name and secure the account this install was created with.

    The one unauthenticated write in the product, and it exists because the
    state it ends — one account, no password — already admits everybody: a
    passwordless install hands a session to any caller, so requiring one here
    would protect nothing. What protects it is the guard: a loopback socket
    peer, or the setup token. See :mod:`nerve.setup_token`.

    Not "create": PR 1 guarantees an account always exists, so this sets a
    username and a credential on the existing one and names its existing
    actor. Nothing that was ever attributed to this install moves.

    **It also ends every session that existed before it.** A passwordless
    install hands a session to anybody who asks, and those tokens name this
    same account — so the claim bumps the account's session epoch inside its
    transaction and every one of them is refused at its next request. The
    token returned here is minted at the new epoch, so the browser doing the
    claiming is the one session that survives.

    The guard is evaluated *before* the instance state, so a caller who fails
    it learns nothing here that ``/api/auth/status`` does not already say.
    """
    config = get_config()

    secret = effective_jwt_secret(config)
    store = identity_store()
    if store is None:
        raise HTTPException(status_code=503, detail=NO_IDENTITY_DETAIL)
    if not secret:
        # A claim that cannot hand back a session would leave the browser
        # holding a password it has no way to use.
        raise HTTPException(
            status_code=503,
            detail="No session signing secret is available yet; restart the "
                   "gateway so one is generated, or set auth.jwt_secret.",
        )

    host = peer_host(request.scope)
    if token_is_required(host, config):
        stored = await stored_setup_token(store)
        if not token_accepted(req.setup_token, stored):
            await asyncio.sleep(_REFUSAL_SECONDS)
            logger.warning(
                "Refused a setup claim from %s: no valid setup token", host or "?",
            )
            raise HTTPException(status_code=403, detail=_GUARD_REFUSED)

    if not await instance_is_unclaimed(store, config):
        raise HTTPException(
            status_code=409,
            detail="This instance has already been claimed. Sign in instead.",
        )

    problem = password_length_problem(req.password)
    if problem:
        # Checked before hashing, as the accounts routes do: bcrypt refuses a
        # password past 72 *bytes*, and the message has to say so in bytes —
        # "too long" on a password a person can see is twelve characters long
        # is not a usable error. `hash_password` raises as a backstop.
        raise HTTPException(status_code=400, detail=problem)
    credential = hash_password(req.password)

    try:
        account = await store.claim_sole_account(
            username=req.username,
            credential=credential,
            display_name=(req.display_name or "").strip() or None,
        )
    except NotClaimableError as e:
        # Lost a race with another claim, or the state changed under us. Same
        # answer as the check above, from inside the transaction that can
        # actually prove it.
        raise HTTPException(status_code=409, detail=str(e)) from e
    except UsernameTakenError as e:  # pragma: no cover - one account exists
        raise HTTPException(status_code=409, detail=str(e)) from e
    except AccountError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    # The token's job is done the moment the account has a password. A
    # credential left behind in a log file is one somebody eventually uses.
    await invalidate_setup_token(store)

    try:
        actor: Actor = await actor_for_account(store, account["id"])
    except ActorResolutionError as e:  # pragma: no cover - just claimed it
        raise HTTPException(status_code=500, detail=str(e)) from e

    logger.info(
        "Instance claimed: account %s is now named and has a password; every "
        "session issued while it was passwordless is now refused",
        account["id"],
    )
    return ClaimResponse(
        # At the epoch the claim just bumped to, so this token is the only one
        # that still works: every session handed out while the instance
        # admitted everybody is an epoch behind and is refused at its next
        # request (v049).
        token=create_session_token(
            secret, account["id"],
            session_epoch=account.get("session_epoch") or 0,
        ),
        account_id=account["id"],
        actor_id=actor.actor_id,
        username=account["username"],
        display_name=actor.display_name,
    )


# --------------------------------------------------------------------------- #
#  The checklist                                                               #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class _Context:
    """What every checklist read and write needs, resolved once."""

    config: Any
    state: SetupState
    unclaimed: bool
    # The caller's own name, which is what the profile step sets. Read from
    # the actor the request resolved to rather than from "the owner": on an
    # install that has grown a second account there is no single owner, and
    # the step edits the person asking.
    display_name: str | None


async def _context(actor: Actor) -> _Context:
    config = get_config()
    unclaimed = await instance_is_unclaimed(get_deps().db, config)
    return _Context(
        config=config,
        state=setup_state.load_state(),
        unclaimed=unclaimed,
        display_name=actor.display_name,
    )


def _writable_problem(config) -> str | None:
    """Why the checklist cannot write, or ``None``."""
    if config.lockdown:
        return _LOCKDOWN_REFUSED
    if not config.config_dir or not Path(config.config_dir).is_dir():
        return (
            "This instance has no machine-local configuration directory, so "
            "there is nowhere to write. Run `nerve init` on the machine."
        )
    return None


def _require_writable(config) -> Path:
    problem = _writable_problem(config)
    if problem:
        raise HTTPException(status_code=409, detail=problem)
    return Path(config.config_dir)


def _provider_detail(config) -> str:
    """What the instance is talking to a model with, if anything."""
    if config.provider.type == "bedrock":
        return f"AWS Bedrock in {config.provider.aws_region or 'an unset region'}"
    if config.proxy.enabled:
        return "A local proxy (CLIProxyAPI) is configured"
    if config.anthropic_api_key:
        return "An Anthropic API key is configured"
    if os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_CODE_OAUTH_TOKEN"):
        # A Docker install is handed its credential in the environment, and
        # `claude_oauth_token` in config.local.yaml is read by the entrypoint
        # rather than by NerveConfig — so the environment is where to look.
        return "A provider credential is set in the environment"
    return ""


def _step_status(
    step: str, context: _Context,
) -> tuple[str, str]:
    """``(status, detail)`` for one step.

    Derived wherever it can be: a step is done because the thing it does is
    true, not because a flag says so. Only "skipped" is remembered.
    """
    state, config = context.state, context.config

    if step == STEP_ACCOUNT:
        if context.unclaimed:
            return "pending", (
                "This instance has no password: everyone who can reach it is "
                "signed in as the owner."
            )
        return "done", "The account has a password."

    if step == STEP_PROVIDER:
        detail = _provider_detail(config)
        if detail:
            return "done", detail
        if step in state.done:
            # Written, but this process is still running without it — the
            # restart the checklist is already reporting as pending is what
            # picks it up. Saying "pending" here would invite a second write
            # of the same key.
            return "done", _WRITTEN_NOT_LIVE
        return ("skipped" if step in state.skipped else "pending"), ""

    if step == STEP_PROFILE:
        if step in state.done or context.display_name:
            return "done", (
                f"{context.display_name}, {config.timezone}"
                if context.display_name else config.timezone
            )
        return ("skipped" if step in state.skipped else "pending"), config.timezone

    if step == STEP_CHANNELS:
        if config.telegram.bot_token:
            return "done", "A Telegram bot token is configured."
        if step in state.done:
            return "done", _WRITTEN_NOT_LIVE
        return ("skipped" if step in state.skipped else "pending"), ""

    if step == STEP_AUTOMATION:
        if step in state.done:
            return "done", ""
        return ("skipped" if step in state.skipped else "pending"), ""

    raise HTTPException(status_code=404, detail=f"Unknown setup step: {step}")


def _render(context: _Context) -> SetupStateOut:
    config = context.config
    problem = _writable_problem(config)
    pending = setup_state.pending_paths(context.state, config)

    steps = []
    for step, title, required in _STEPS:
        status, detail = _step_status(step, context)
        steps.append(SetupStepOut(
            id=step, title=title, status=status, required=required,
            can_skip=step in _SKIPPABLE, detail=detail,
        ))

    crons: list[CronToggle] = []
    try:
        crons = list_optional_crons(Path(config.workspace))
    except OSError as e:  # pragma: no cover - unreadable workspace
        logger.warning("Could not read the cron file for setup: %s", e)

    finished = (
        not context.unclaimed
        and not pending
        and all(s.status in {"done", "skipped"} for s in steps)
    )

    return SetupStateOut(
        setup_pending=context.unclaimed,
        lockdown=bool(config.lockdown),
        writable=problem is None,
        read_only_reason=problem,
        restart_pending=bool(pending),
        restart_pending_paths=pending,
        finished=finished,
        steps=steps,
        crons=[
            CronToggleOut(
                id=c.id, name=c.name, description=c.description, enabled=c.enabled,
            )
            for c in crons
        ],
    )


@router.get("/api/setup", response_model=SetupStateOut)
async def get_setup(actor: Actor = Depends(require_account)):
    """The checklist: what is done, what is waiting, and what a restart owes.

    Authenticated. The only setup fact an anonymous caller needs is whether the
    instance is unclaimed, and ``/api/auth/status`` already carries it.
    """
    return _render(await _context(actor))


def _apply(
    context: _Context,
    *,
    step: str,
    choices: SetupChoices,
    machine_paths: tuple[str, ...] = (),
    portable_paths: tuple[str, ...] = (),
    secret_paths: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Write one step's keys, and only its keys.

    The values come from :func:`build_config_layers` and
    :func:`build_config_local` — the same functions ``nerve init`` writes
    from — and this selects the paths the step owns out of them. So which
    layer a value belongs in is decided in exactly one place: change it there
    and both the installer and the wizard follow.

    Returns what was applied, for the state file to remember.
    """
    config_dir = _require_writable(context.config)
    machine, portable, _shadowed = build_config_layers(choices)
    machine_flat = leaf_paths(machine)
    portable_flat = leaf_paths(portable)
    secret_flat = leaf_paths(build_config_local(choices))

    applied: dict[str, Any] = {}

    machine_updates = {p: machine_flat[p] for p in machine_paths if p in machine_flat}
    secret_updates = {p: secret_flat[p] for p in secret_paths if p in secret_flat}
    if machine_updates or secret_updates:
        # Two writers, because the two files are not the same kind of file.
        # config.local.yaml holds credentials and is forced owner-only, failing
        # closed if that cannot be guaranteed. config.yaml holds this box's
        # shape — no secret, and the file an operator edits by hand — so it is
        # republished at the mode and ownership it already had: in Docker the
        # container is root over a bind-mounted checkout, and a fresh
        # root-owned 0600 inode there locks the host's own CLI out of the file
        # it needs to recognise a Docker install.
        try:
            if secret_updates:
                merge_private_paths(
                    config_dir / "config.local.yaml", secret_updates,
                )
            if machine_updates:
                merge_machine_paths(
                    config_dir / "config.yaml", machine_updates,
                    header=CONFIG_YAML_HEADER,
                )
        except paths.InsecureFileError as e:
            # Nothing was written. Say so rather than reporting a step as done.
            raise HTTPException(status_code=500, detail=str(e)) from e
        except (OSError, ValueError) as e:
            raise HTTPException(
                status_code=500, detail=f"Could not write configuration: {e}",
            ) from e
        applied.update(machine_updates)
        applied.update(secret_updates)

    portable_updates = {p: portable_flat[p] for p in portable_paths if p in portable_flat}
    if portable_updates:
        outcome = merge_settings_paths(Path(context.config.workspace), portable_updates)
        if outcome.status in {"invalid_yaml", "not_a_mapping"}:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"{outcome.path} is not usable as settings "
                    f"({outcome.status.replace('_', ' ')}), so it was left alone. "
                    "Fix it in the workspace repository and try again."
                ),
            )
        applied.update(portable_updates)

    setup_state.record_applied(context.state, applied)
    context.state.done.add(step)
    context.state.skipped.discard(step)
    setup_state.save_state(context.state)
    return applied


@router.put("/api/setup/provider", response_model=SetupStateOut)
async def set_provider(req: ProviderRequest, actor: Actor = Depends(require_account)):
    """Store a provider API key in ``config.local.yaml``.

    Only what a browser can actually supply. The installer's credential
    waterfall reads the operator's macOS keychain and
    ``~/.claude/.credentials.json``; a process inside the VM cannot see either,
    so the wizard asks for a key instead of pretending to find one.
    """
    anthropic = (req.anthropic_api_key or "").strip()
    openai = (req.openai_api_key or "").strip()
    if not anthropic and not openai:
        raise HTTPException(
            status_code=400,
            detail="Give an Anthropic API key, an OpenAI key, or skip this step.",
        )

    context = await _context(actor)
    choices = SetupChoices(anthropic_api_key=anthropic, openai_api_key=openai)
    _apply(
        context, step=STEP_PROVIDER, choices=choices,
        secret_paths=("anthropic_api_key", "openai_api_key"),
    )
    logger.info("Setup: a provider credential was stored by account %s", actor.account_id)
    return _render(await _context(actor))


@router.put("/api/setup/profile", response_model=SetupStateOut)
async def set_profile(req: ProfileRequest, actor: Actor = Depends(require_account)):
    """Timezone into the tracked layer, display name onto the existing actor.

    The name is a presentation snapshot (0.7): it changes what every label in
    the app says and rewrites nothing that was stored, because it goes onto
    the actor the bootstrap created rather than creating a second one.
    """
    context = await _context(actor)
    timezone = (req.timezone or "").strip()
    if timezone:
        try:
            ZoneInfo(timezone)
        except (ZoneInfoNotFoundError, ValueError, OSError) as e:
            raise HTTPException(
                status_code=400,
                detail=f"{timezone!r} is not a time zone this machine knows.",
            ) from e
        # Both halves or neither. The name goes into the database and the zone
        # into a file, and a request that asked for both must not land the name
        # and then answer with the failure of the other — a committed write
        # reported as a failure is a form that primes a retry which can only
        # conflict with what already happened.
        _require_writable(context.config)

    db = get_deps().db
    if req.display_name is not None:
        # Written first because it is the half that works on a read-only
        # instance: under lockdown, naming yourself is the one thing the
        # checklist can still do.
        if not actor.account_id:  # pragma: no cover - require_account checked
            raise HTTPException(status_code=403, detail="No account to rename")
        account = await db.get_account(actor.account_id)
        if account is None:  # pragma: no cover - resolved a moment ago
            raise HTTPException(status_code=404, detail="Account not found")
        await db.update_actor_profile(
            account["actor_id"], display_name=(req.display_name.strip() or None),
        )

    if timezone:
        _apply(
            context, step=STEP_PROFILE,
            choices=SetupChoices(timezone=timezone),
            portable_paths=("timezone",),
        )
    elif req.display_name is not None:
        context.state.done.add(STEP_PROFILE)
        context.state.skipped.discard(STEP_PROFILE)
        setup_state.save_state(context.state)
    else:
        raise HTTPException(
            status_code=400, detail="Give a time zone, a display name, or both.",
        )
    return _render(await _context(actor))


@router.put("/api/setup/channels", response_model=SetupStateOut)
async def set_channels(req: ChannelsRequest, actor: Actor = Depends(require_account)):
    """A Telegram bot token, which is a secret, plus the switch that uses it.

    ``telegram.enabled`` is machine-local — it says whether *this box* was
    given a token — and the token itself never leaves ``config.local.yaml``.
    Both are restart-only, so the checklist reports the restart as pending
    until it happens.
    """
    context = await _context(actor)
    choices = SetupChoices(
        telegram_bot_token=req.telegram_bot_token.strip(),
        telegram_allowed_users=list(req.telegram_allowed_users or []),
    )
    _apply(
        context, step=STEP_CHANNELS, choices=choices,
        machine_paths=("telegram.enabled",),
        secret_paths=("telegram.bot_token", "telegram.allowed_users"),
    )
    logger.info("Setup: a Telegram bot token was stored by account %s", actor.account_id)
    return _render(await _context(actor))


@router.put("/api/setup/automation", response_model=SetupStateOut)
async def set_automation(req: AutomationRequest, actor: Actor = Depends(require_account)):
    """Which optional crons run, and which sources sync.

    The crons are the ones the installer already wrote: this flips ``enabled``
    on them and adds nothing, because whether an install is a personal or a
    worker one is a wizard answer no running instance records — and a
    checklist step should not be rewriting a file an operator may have edited.
    """
    context = await _context(actor)
    _require_writable(context.config)
    wanted = {c.strip() for c in req.crons if c.strip()}

    outcome = set_optional_crons(Path(context.config.workspace), wanted)
    if outcome.status == "unreadable":
        raise HTTPException(
            status_code=409,
            detail=f"{outcome.path} could not be read ({outcome.detail}); nothing "
                   "was changed.",
        )

    choices = SetupChoices(
        github_sync=req.github,
        gmail_sync=req.gmail,
        gmail_accounts=[a.strip() for a in req.gmail_accounts if a.strip()],
        telegram_sync=req.telegram,
        telegram_api_id=int(req.telegram_api_id or 0),
        telegram_api_hash=(req.telegram_api_hash or "").strip(),
    )
    _apply(
        context, step=STEP_AUTOMATION, choices=choices,
        machine_paths=("sync.gmail.accounts",),
        portable_paths=(
            "sync.github.enabled",
            "sync.github_events.enabled",
            "sync.gmail.enabled",
            "sync.telegram.enabled",
        ),
        secret_paths=("sync.telegram.api_id", "sync.telegram.api_hash"),
    )

    logger.info(
        "Setup: automation set by account %s (crons on: %s)",
        actor.account_id, ", ".join(sorted(wanted)) or "none",
    )
    return _render(await _context(actor))


@router.post("/api/setup/steps/{step_id}/skip", response_model=SetupStateOut)
async def skip_step(step_id: str, actor: Actor = Depends(require_account)):
    """Remember that this step was declined, so the checklist stops asking.

    "I do not want Telegram" and "I have not got to Telegram yet" look the
    same to a derived checklist, and one that keeps nagging about a decision
    already made is one people abandon. Reversible: writing the step again
    clears it, and so does :func:`unskip_step`.
    """
    if step_id not in _SKIPPABLE:
        raise HTTPException(
            status_code=400,
            detail=f"{step_id!r} cannot be skipped."
            if step_id in {s for s, _t, _r in _STEPS}
            else f"Unknown setup step: {step_id}",
        )
    context = await _context(actor)
    context.state.skipped.add(step_id)
    context.state.done.discard(step_id)
    setup_state.save_state(context.state)
    return _render(await _context(actor))


@router.post("/api/setup/steps/{step_id}/unskip", response_model=SetupStateOut)
async def unskip_step(step_id: str, actor: Actor = Depends(require_account)):
    """Put a skipped step back on the list."""
    if step_id not in _SKIPPABLE:
        raise HTTPException(status_code=400, detail=f"Unknown setup step: {step_id}")
    context = await _context(actor)
    context.state.skipped.discard(step_id)
    setup_state.save_state(context.state)
    return _render(await _context(actor))


# --------------------------------------------------------------------------- #
#  The last step: a restart                                                    #
# --------------------------------------------------------------------------- #


@router.post("/api/system/restart", response_model=RestartResponse)
async def restart_system(actor: Actor = Depends(require_account)):
    """Restart the daemon — what ``nerve restart`` does, asked for over HTTP.

    ``timezone``, the Telegram token and the gateway socket are restart-only
    (``docs/config.md``), so the wizard cannot apply them live and its last
    step is this. The response carries this process's **boot generation**; the
    browser polls ``/health`` until that value changes, which is the only
    honest way to know the *new* process is answering — the old one serves
    perfectly well while it shuts down, so a client that waits for any answer
    at all accepts the process it asked to replace.

    The browser lands **already signed in**: the signing secret is pinned and
    persisted, nothing here rotates it, and the session epoch is per account
    rather than per process, so the token in ``localStorage`` outlives the
    process that issued it.

    Allowed under lockdown: a restart writes no configuration, and an instance
    that could not be restarted from its own UI would be worse off for it.

    Serialised, and the helper is spawned **here** rather than in a background
    task: whether a restart was *begun* is knowable synchronously, and a
    caller told "restarting" for a helper that failed to start would wait for
    a process that is never coming. The helper holds a short delay before it
    signals anything, which is what gets this response onto the wire first.
    """
    config = get_config()
    config_dir = Path(config.config_dir) if config.config_dir else paths.nerve_home()
    # This process *is* the daemon being replaced. Reading the pid file would
    # be answering the same question less reliably.
    old_pid = os.getpid()

    async with _restart_lock:
        global _restart_requested
        if _restart_requested:
            # A second helper would race the first over the same pid and pid
            # file. One restart is all anybody can want, and it is already
            # under way.
            raise HTTPException(
                status_code=409,
                detail="A restart is already under way; this page reconnects "
                       "when the new instance answers.",
            )
        try:
            outcome = await asyncio.to_thread(
                daemon.restart_daemon,
                config_dir,
                old_pid=old_pid,
                delay_seconds=_RESTART_DELAY_SECONDS,
            )
        except Exception as e:  # noqa: BLE001 - reported, not swallowed
            logger.exception("Restart requested by account %s could not start", actor.account_id)
            raise HTTPException(
                status_code=500,
                detail=f"The restart could not be started: {e}. The instance is "
                       "still running; see the server log.",
            ) from e
        _restart_requested = True

    logger.info("Restart requested by account %s (%s)", actor.account_id, outcome.method)
    return RestartResponse(
        restarting=True,
        method=outcome.method,
        message="Restarting. This page reconnects on its own.",
        boot=boot.boot_id(),
    )


# Kept out of the checklist models on purpose: `require_auth`, not
# `require_account`, so the agent's own system principal can ask who it is
# without being told account management is not for it.
class MeResponse(BaseModel):
    actor_id: str
    account_id: str | None
    username: str | None
    display_name: str | None
    kind: str


@router.get("/api/auth/me", response_model=MeResponse)
async def auth_me(actor: Actor = Depends(require_auth)):
    """Who this request is. Nothing about how it authenticated.

    The wizard uses it for "signed in as"; the chat uses it to label your own
    message the moment you send it, instead of waiting for the server's copy.
    Built from the actor the request already resolved to, and it carries no
    credential, no credential source and no token — the account row is not
    read at all beyond the username.
    """
    username = None
    if actor.account_id:
        db = get_deps().db
        account = await db.get_account(actor.account_id)
        username = (account or {}).get("username")
    return MeResponse(
        actor_id=actor.actor_id,
        account_id=actor.account_id,
        username=username,
        display_name=actor.display_name,
        kind=actor.kind,
    )
