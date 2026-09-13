"""First-run setup: a token-guarded claim, then an authenticated checklist.

Why this exists: ``nerve init`` prompts for a password, but the headless path
Docker uses reads ``NERVE_PASSWORD`` from the environment and defaults it to
empty. An install that omits it lands with one account and no password, and
for those installs the web checklist is the only setup surface anyone will
ever see.

**The account comes first, and that closes the window.** Step one names and
secures the one account this install was created with; every later step is an
ordinary authenticated request. So exactly one endpoint here is
unauthenticated — ``POST /api/setup/claim`` — and it is not an exemption:
every claim, from every address, must carry the persisted setup token (see
:mod:`nerve.setup_token`). There is no loopback exception, and locality is
never read as authority.

**A checklist, not a linear gate.** Browser wizards get abandoned halfway, so
every step after the account is skippable and re-enterable, and an abandoned
checklist leaves a working instance on defaults with a "finish setup"
affordance.

**It is not the settings editor.** First-run decisions only, and only the ones
a running instance can decide for itself: the account, a provider credential,
timezone and display name, a channel token, and which optional crons and sync
sources are on. Deployment shape, the workspace path and the operator's own
keychain are not web decisions — a browser talking to a process inside the VM
cannot see the laptop it is running on.

**It does not restart anything.** The values it writes that are read at
startup are reported as pending, in words, with the command to run; applying
them is ``nerve restart`` on the server. A browser that can restart the
daemon is a browser holding process-control authority over the box, which is
not authority this checklist needs in order to be useful.

**Under lockdown it is read-only.** Configuration is fleet-managed there and
machine-local values are environment references, so every write below refuses.
Claiming the account is the exception, and deliberately: it writes to
``nerve.db``, not to configuration, and an unclaimed fleet-managed install
that could never be claimed would stay open forever.
"""

from __future__ import annotations

import asyncio
import logging
import os
import weakref
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from nerve import boot, paths, setup_state
from nerve.config import NerveConfig, get_config
from nerve.db.accounts import AccountError, NotClaimableError, UsernameTakenError
from nerve.gateway.auth import (
    NO_IDENTITY_DETAIL,
    create_session_token,
    effective_jwt_secret,
    hash_password,
    identity_store,
    password_length_problem,
)
from nerve.gateway.routes._deps import get_deps
from nerve.gateway.routes.accounts import require_account
from nerve.identity import Actor
from nerve.setup_state import SetupState
from nerve.setup_token import (
    SETUP_TOKEN_NAME,
    instance_is_unclaimed,
    stored_setup_token,
    token_accepted,
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
    plan_optional_crons,
    settings_problem,
)

logger = logging.getLogger(__name__)

router = APIRouter()

_GUARD_REFUSED = (
    "A valid setup token is required. Run 'nerve status' on the server to "
    "read the current token."
)

# A step whose value is on disk but not in the running process yet. The
# checklist reports the restart separately; this keeps the step itself from
# reading as unanswered and inviting a second write of the same key.
_WRITTEN_NOT_LIVE = "Saved. It applies when the instance restarts."

# What an operator runs to pick up everything below. Sent as data rather than
# only rendered in the page so that an API consumer is told the same thing.
RESTART_COMMAND = "nerve restart"

# One mutation at a time. Every step reads the checklist's notes, changes them
# and writes them back, so two requests in flight — two tabs, or one tab's form
# and another's skip — would each save over a snapshot taken before the other.
#
# One lock per event loop rather than one module-level lock, because an
# `asyncio.Lock` binds itself to the loop that first *waits* on it and refuses
# every other one afterwards. The daemon has exactly one loop, so this is one
# lock there; it is the test process — many loops in one interpreter — that
# needs the distinction, and a lock that only works until something contends
# it is worse than no lock, since it passes every test that does not.
#
# A lock the *processes* share would be needed for a multi-worker server. There
# is no such mode today.
_locks: "weakref.WeakKeyDictionary[Any, dict[str, asyncio.Lock]]" = (
    weakref.WeakKeyDictionary()
)


def _loop_lock(name: str) -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    locks = _locks.setdefault(loop, {})
    if name not in locks:
        locks[name] = asyncio.Lock()
    return locks[name]


# What a rewritten cron file the scheduler has not re-read amounts to.
_CRON_DEBT = "the scheduler is still running the cron settings it started with"

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
# abandoning the checklist safe.
_STEPS = (
    (STEP_ACCOUNT, "Claim this instance", True),
    (STEP_PROVIDER, "Provider credential", False),
    (STEP_PROFILE, "Timezone and name", False),
    (STEP_CHANNELS, "Telegram", False),
    (STEP_AUTOMATION, "Automation", False),
)

_SKIPPABLE = {step for step, _title, required in _STEPS if not required}

# Steps whose completion the *instance* can state on its own: a provider
# credential is configured or it is not, a bot token is there or it is not, a
# person has a display name or has not. Their recorded "done" is transitional —
# it answers "written, not live yet" for the process that wrote it, and a later
# process reads the instance instead (see setup_state.retire_transitional).
# `automation` is deliberately absent: which crons an operator wanted is a
# decision nothing else records.
_TRANSITIONAL_STEPS = {STEP_PROVIDER, STEP_PROFILE, STEP_CHANNELS}

# What `nerve init` writes when nobody chose one (nerve/config.py's own
# default). A timezone that differs from it is an answer somebody gave, which
# is what makes "the profile step was answered" derivable after a restart
# rather than only remembered.
DEFAULT_TIMEZONE = NerveConfig().timezone


# --------------------------------------------------------------------------- #
#  Models                                                                      #
# --------------------------------------------------------------------------- #


class ClaimRequest(BaseModel):
    username: str
    password: str = Field(min_length=1)
    setup_token: str = Field(min_length=1)
    display_name: str | None = None


class ClaimResponse(BaseModel):
    """The client session created by a successful claim."""

    token: str


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


class SetupValuesOut(BaseModel):
    """What the forms need in order to open on the instance's own answers.

    A form that opens on defaults is a form that submits defaults, which is
    how re-entering a step to change one thing changed the rest. Secrets are
    reported as **present or not** — the checklist writes credentials and
    never reads them back, and a setup screen is not a place to display one.
    """

    timezone: str
    display_name: str | None
    has_anthropic_key: bool
    has_openai_key: bool
    has_telegram_token: bool
    sync_github: bool
    sync_gmail: bool
    sync_telegram: bool
    # Deliberately not here: which mailboxes sync. Every account may read this
    # endpoint, the checklist has no field for them, and a list of somebody's
    # email addresses is not something to publish for a screen that does not
    # use it. An omitted `gmail_accounts` in a step leaves them untouched.


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
    # Things waiting on a restart that are not configuration keys — a cron file
    # the running scheduler has not picked up, say. Written for a person to
    # read, because there is no path to name.
    restart_pending_reasons: list[str]
    # What to run on the server to pick the above up. There is no endpoint
    # that does it: restarting the daemon is an operator action.
    restart_command: str = RESTART_COMMAND
    # Every required step done and nothing waiting on a restart.
    finished: bool
    steps: list[SetupStepOut]
    crons: list[CronToggleOut]
    values: SetupValuesOut
    # Set when a request's configuration landed but its bookkeeping did not,
    # which is neither success nor failure and has to be said as itself.
    warning: str | None = None


class ProviderRequest(BaseModel):
    # Both optional, at least one required: an install may have arrived with
    # one of them already set from the environment.
    anthropic_api_key: str | None = None
    openai_api_key: str | None = None


class ProfileRequest(BaseModel):
    timezone: str | None = None
    display_name: str | None = None


class ChannelsRequest(BaseModel):
    # Not `min_length=1`: that accepts a string of spaces, which strips to
    # nothing, writes nothing, and would still have marked the step done.
    telegram_bot_token: str
    # Omitted means "leave the allow-list alone" — pairing is how people are
    # added to it, and a setup step must not clear what pairing built.
    telegram_allowed_users: list[int] | None = None


class AutomationRequest(BaseModel):
    """Every field optional, and an omitted one means *untouched*.

    The step is re-enterable, so it is entered again to change one thing — and
    a body that defaulted the rest to ``false`` turned "enable this cron" into
    "and switch off the sync sources somebody configured elsewhere".
    """

    crons: list[str] | None = None
    github: bool | None = None
    gmail: bool | None = None
    gmail_accounts: list[str] | None = None
    telegram: bool | None = None
    telegram_api_id: int | None = None
    telegram_api_hash: str | None = None


# --------------------------------------------------------------------------- #
#  Step one: claim and secure                                                  #
# --------------------------------------------------------------------------- #


@router.post("/api/setup/claim", response_model=ClaimResponse)
async def claim(req: ClaimRequest):
    """Atomically name and secure the sole account, then sign the client in."""
    config = get_config()
    secret = effective_jwt_secret(config)
    store = identity_store()
    if store is None:
        raise HTTPException(status_code=503, detail=NO_IDENTITY_DETAIL)
    if not secret:
        raise HTTPException(
            status_code=503,
            detail="No session signing secret is available; restart the gateway "
                   "or set auth.jwt_secret.",
        )

    # Validate before reading claimability. A caller without the credential
    # learns nothing here, and an absent stored value still takes the same
    # constant-time comparison path against the module decoy.
    stored = await stored_setup_token(store)
    if not token_accepted(req.setup_token, stored):
        raise HTTPException(status_code=403, detail=_GUARD_REFUSED)

    problem = password_length_problem(req.password)
    if problem:
        raise HTTPException(status_code=400, detail=problem)
    credential = hash_password(req.password)

    if not await instance_is_unclaimed(store, config):
        raise HTTPException(
            status_code=409,
            detail="This instance has already been claimed. Sign in instead.",
        )

    try:
        account = await store.claim_sole_account(
            username=req.username,
            credential=credential,
            display_name=(req.display_name or "").strip() or None,
            invalidate_secret_name=SETUP_TOKEN_NAME,
        )
    except NotClaimableError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except UsernameTakenError as e:  # pragma: no cover - one account exists
        raise HTTPException(status_code=409, detail=str(e)) from e
    except AccountError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    # Closing is best-effort. Per-frame epoch checks independently prevent a
    # stale socket from acting if proactive closure misses or fails.
    try:
        from nerve.gateway.server import close_revoked_sockets

        await close_revoked_sockets()
    except Exception as e:  # noqa: BLE001 - the claim has already committed
        logger.warning("Claim: open sockets could not be closed: %s", e)

    logger.info(
        "Instance claimed: account %s now has a password; pre-claim sessions "
        "were revoked",
        account["id"],
    )
    return ClaimResponse(token=create_session_token(
        secret,
        account["id"],
        session_epoch=account.get("session_epoch") or 0,
    ))


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
    """The checklist's world, as one read.

    Retires an earlier process's notes *in memory* — anything left behind
    describes a write that has since been picked up, or undone by hand, and
    the instance is the better witness either way. It does not persist that:
    this runs inside the state lock for a mutation (which saves the retired
    state with its own change) and outside it for a plain read, and a save
    from outside the lock is exactly the interleaving the lock exists to
    prevent. :func:`get_setup` persists it under the lock instead.
    """
    config = get_config()
    unclaimed = await instance_is_unclaimed(get_deps().db, config)
    # require_account guarantees this. Passing it explicitly is the server-side
    # half of the account boundary: Alice's skip/done/answered decisions must
    # never become Bob's checklist merely because they share one instance.
    assert actor.account_id is not None  # noqa: S101
    state = setup_state.load_state(
        actor.account_id,
        # A v1/v2 setup-state file has no author. The sole account can inherit
        # it; once there are two, assigning it to whichever account reads first
        # would itself be the cross-account leak this boundary prevents.
        adopt_legacy_decisions=await get_deps().db.count_accounts() == 1,
    )
    setup_state.retire_transitional(state, _TRANSITIONAL_STEPS)
    return _Context(
        config=config,
        state=state,
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


# Rule one of the claim cutover: while nobody has claimed the instance, the
# only write anybody may perform is the claim itself.
#
# `require_account` is not a boundary here. A passwordless install mints a real
# session for any password, so every one of these endpoints is reachable by the
# visitor the setup token exists to keep out — and "account first" means the
# account comes first, not that it comes first among the steps a stranger may
# take.
_UNCLAIMED_REFUSED = (
    "This instance has not been claimed yet, so it accepts no changes but the "
    "claim itself: anyone who can reach it is signed in as the owner, and a "
    "setting written now would have been written by anybody. Finish step one "
    "(POST /api/setup/claim, with the setup token) first."
)


_STALE_REFUSED = (
    "This instance was claimed after your session started, so that request was "
    "refused. Sign in again."
)


def _require_claimed(context: _Context) -> None:
    """Refuse every mutation but the claim while the instance is unclaimed."""
    if context.unclaimed:
        raise HTTPException(status_code=409, detail=_UNCLAIMED_REFUSED)


async def _require_current_session(actor: Actor, db) -> None:
    """Rule two, for the writes that have no transaction to join.

    "Not unclaimed any more" and "you were allowed to ask" are different
    questions, and only the second one is about the caller: a session admitted
    while the instance was passwordless can resume *after* the claim, find the
    instance claimed, and write a provider key as the person it locked out.

    The account writes carry their epoch into their own transaction, which is
    stronger. A file write has no transaction, so this is read as late as it
    can be — inside the mutation lock, immediately before the write — and the
    claim's own bump is atomic, so what remains is a window measured in the
    time between this read and the next statement.
    """
    if actor.account_id is None or actor.session_epoch is None:
        return
    account = await db.get_account(actor.account_id)
    if account is None or int(account.get("session_epoch") or 0) != int(
        actor.session_epoch
    ):
        raise HTTPException(status_code=409, detail=_STALE_REFUSED)


def _provider_detail(config) -> str:
    """What the instance is talking to a model with, if anything."""
    if config.provider.type == "bedrock":
        return f"AWS Bedrock in {config.provider.aws_region or 'an unset region'}"
    if config.proxy.enabled:
        return "A local proxy (CLIProxyAPI) is configured"
    if config.anthropic_api_key and config.openai_api_key:
        return "An Anthropic API key and an OpenAI key are configured"
    if config.anthropic_api_key:
        return "An Anthropic API key is configured"
    if config.openai_api_key:
        # The step accepts an OpenAI key on its own, so the derivation has to
        # recognise one on its own: otherwise a valid answer goes back to
        # "to do" at the first restart, which is exactly when the checklist is
        # asking whether it is finished.
        return "An OpenAI key is configured"
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
        # Three ways this is answered, and two of them outlive a restart: a
        # display name is on the actor, and a timezone that is not the
        # installer's default was chosen by somebody. The third — the
        # transitional marker — covers the moment between writing a timezone
        # and the process that reads it.
        answered = (
            step in state.done
            or step in state.answered
            or bool(context.display_name)
            or config.timezone != DEFAULT_TIMEZONE
        )
        if answered:
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


def _render(context: _Context, *, warning: str | None = None) -> SetupStateOut:
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

    debts = sorted(context.state.debts)
    finished = (
        not context.unclaimed
        and not pending
        and not debts
        and all(s.status in {"done", "skipped"} for s in steps)
    )

    sync = config.sync
    values = SetupValuesOut(
        timezone=config.timezone,
        display_name=context.display_name,
        has_anthropic_key=bool(config.anthropic_api_key),
        has_openai_key=bool(config.openai_api_key),
        has_telegram_token=bool(config.telegram.bot_token),
        sync_github=bool(sync.github.enabled),
        sync_gmail=bool(sync.gmail.enabled),
        sync_telegram=bool(sync.telegram.enabled),
    )

    return SetupStateOut(
        setup_pending=context.unclaimed,
        lockdown=bool(config.lockdown),
        writable=problem is None,
        read_only_reason=problem,
        restart_pending=bool(pending) or bool(debts),
        restart_pending_paths=pending,
        restart_pending_reasons=debts,
        restart_command=RESTART_COMMAND,
        finished=finished,
        steps=steps,
        crons=[
            CronToggleOut(
                id=c.id, name=c.name, description=c.description, enabled=c.enabled,
            )
            for c in crons
        ],
        values=values,
        warning=warning,
    )


@router.get("/api/setup", response_model=SetupStateOut)
async def get_setup(actor: Actor = Depends(require_account)):
    """The checklist: what is done, what is waiting, and what a restart owes.

    Authenticated. The only setup fact an anonymous caller needs is whether the
    instance is unclaimed, and ``/api/auth/status`` already carries it.

    Takes the mutation lock, because reading can *write*: an earlier process's
    notes are retired on the first read after a restart, and persisting that
    from outside the lock would race a step that is saving its own change.
    """
    async with _loop_lock("state"):
        context = await _context(actor)
        if context.state.boot == boot.boot_id() and context.state.retired:
            # Best-effort: a state file that cannot be rewritten must not stop
            # the checklist from being read.
            setup_state.save_state(context.state)
        return _render(context)


def _write(
    context: _Context,
    *,
    choices: SetupChoices,
    machine_paths: tuple[str, ...] = (),
    portable_paths: tuple[str, ...] = (),
    secret_paths: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Write one step's keys, and only its keys. Returns what was applied.

    The values come from :func:`build_config_layers` and
    :func:`build_config_local` — the same functions ``nerve init`` writes
    from — and this selects the paths the step owns out of them. So which
    layer a value belongs in is decided in exactly one place: change it there
    and both the installer and the checklist follow.

    **Order matters, and it is the order of fallibility.** The tracked
    settings file is checked before anything is published, because "that file
    is not a mapping" is the failure most likely to arrive and a step that had
    already written a credential before discovering it would answer with an
    error for work it had half done. What remains after the check are two
    atomic publications; a failure between them leaves the credential written
    and its switch unset, which is the safe half to land — a bot that stays
    off rather than one started without a token.
    """
    config_dir = _require_writable(context.config)
    machine, portable, _shadowed = build_config_layers(choices)
    machine_flat = leaf_paths(machine)
    portable_flat = leaf_paths(portable)
    secret_flat = leaf_paths(build_config_local(choices))

    machine_updates = {p: machine_flat[p] for p in machine_paths if p in machine_flat}
    secret_updates = {p: secret_flat[p] for p in secret_paths if p in secret_flat}
    portable_updates = {
        p: portable_flat[p] for p in portable_paths if p in portable_flat
    }

    workspace = Path(context.config.workspace)
    if portable_updates:
        problem = settings_problem(workspace)
        if problem:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"{workspace / 'config' / 'settings.yaml'} is not usable as "
                    f"settings ({problem}), so nothing was written. Fix it in "
                    "the workspace repository and try again."
                ),
            )

    applied: dict[str, Any] = {}
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

    if portable_updates:
        outcome = merge_settings_paths(workspace, portable_updates)
        if outcome.status in {"invalid_yaml", "not_a_mapping"}:  # pragma: no cover
            # Checked above; reachable only if the file changed underneath us.
            raise HTTPException(
                status_code=409,
                detail=f"{outcome.path} is not usable as settings; it was left alone.",
            )
        applied.update(portable_updates)

    return applied


def _record(
    context: _Context,
    *,
    step: str,
    applied: dict[str, Any] | None = None,
    debts: tuple[str, ...] = (),
    answered: bool = False,
    done: bool = True,
) -> str | None:
    """Note that a step was answered, after every write it makes has landed.

    Separate from :func:`_write` so a failure in a later target cannot leave a
    step recorded as done — the checklist would then say "provider: done" for
    a request the caller was told had failed.

    Returns a warning when the note itself could not be saved. The
    configuration has landed by then, so this is neither success to report
    silently nor a failure to raise: the instance *is* configured and the
    checklist has lost its memory of it, which is partial success and has to be
    said as one.
    """
    if applied:
        setup_state.record_applied(context.state, applied)
    for debt in debts:
        context.state.debts.add(debt)
    if not done:
        # What landed, and nothing about the step being answered: a request
        # that failed part way has configuration on disk to account for and no
        # business reporting itself as done.
        setup_state.save_state(context.state)
        return None
    if answered:
        # A decision the instance cannot state for itself — somebody chose the
        # value that was already there — so it outlives the process that heard
        # it, unlike the transitional "done" beside it.
        context.state.answered.add(step)
    context.state.done.add(step)
    context.state.skipped.discard(step)
    if setup_state.save_state(context.state):
        return None
    return (
        f"The configuration was written, but the checklist could not record "
        f"the {step} step at {setup_state.state_file()} — it may ask for this "
        "again. The instance itself is configured."
    )


def _save_or_refuse(context: _Context) -> None:
    """Persist the checklist's notes, or refuse the request.

    For a request whose *only* effect is the note — skipping a step, putting
    one back — a save that did not happen is a request that did nothing, and
    answering 200 to it means the choice quietly reappears at the next read.
    Steps that write configuration first are the other case; see
    :func:`_record`.
    """
    if not setup_state.save_state(context.state):
        raise HTTPException(
            status_code=500,
            detail=(
                f"The checklist could not be saved to {setup_state.state_file()}, "
                "so that choice was not remembered. Nothing else changed."
            ),
        )


async def _publish_cron_plan(plan) -> tuple[str, ...]:
    """Publish a prepared cron change and hand it to the running scheduler.

    The plan was worked out — and its failures found — before anything else in
    the step was written; this half is one atomic rename and the reload after
    it. Returns the debts it left behind: a rewritten ``system.yaml`` the
    scheduler has not picked up is a change that has not happened, and a
    checklist reporting "nothing is waiting" over it would be wrong in the one
    way that matters — nobody would restart, and the crons they asked for
    would never run.
    """
    outcome = plan.publish()
    if outcome.status == "unchanged":
        return ()

    # The same reload `nerve reload` and POST /api/cron/reload perform. Read at
    # call time rather than imported at module load, because the lifespan
    # publishes it after this module is imported.
    from nerve.gateway.server import _cron_service

    if _cron_service is None:
        return (_CRON_DEBT,)
    try:
        await _cron_service.reload()
    except Exception as e:  # noqa: BLE001 - the write landed; only the apply failed
        logger.warning("Setup: the cron file was written but not reloaded: %s", e)
        return (_CRON_DEBT,)
    return ()


@router.put("/api/setup/provider", response_model=SetupStateOut)
async def set_provider(req: ProviderRequest, actor: Actor = Depends(require_account)):
    """Store a provider API key in ``config.local.yaml``.

    Only what a browser can actually supply. The installer's credential
    waterfall reads the operator's macOS keychain and
    ``~/.claude/.credentials.json``; a process inside the VM cannot see either,
    so the checklist asks for a key instead of pretending to find one.

    PATCH-shaped: a key that is absent or blank is left exactly as it is.
    """
    anthropic = (req.anthropic_api_key or "").strip()
    openai = (req.openai_api_key or "").strip()
    if not anthropic and not openai:
        raise HTTPException(
            status_code=400,
            detail="Give an Anthropic API key, an OpenAI key, or skip this step.",
        )

    async with _loop_lock("state"):
        context = await _context(actor)
        _require_claimed(context)
        await _require_current_session(actor, get_deps().db)
        applied = _write(
            context,
            choices=SetupChoices(anthropic_api_key=anthropic, openai_api_key=openai),
            secret_paths=("anthropic_api_key", "openai_api_key"),
        )
        warning = _record(context, step=STEP_PROVIDER, applied=applied)
        logger.info(
            "Setup: a provider credential was stored by account %s", actor.account_id,
        )
        return _render(await _context(actor), warning=warning)


@router.put("/api/setup/profile", response_model=SetupStateOut)
async def set_profile(req: ProfileRequest, actor: Actor = Depends(require_account)):
    """Timezone into the tracked layer, display name onto the existing actor.

    The name is a presentation snapshot: it changes what every label in the app
    says and rewrites nothing that was stored, because it goes onto the actor
    the bootstrap created rather than creating a second one.

    PATCH-shaped: each field is written only when it is given, so renaming
    yourself cannot move the instance's shared scheduling timezone.
    """
    timezone = (req.timezone or "").strip()
    display_name = req.display_name
    if not timezone and display_name is None:
        raise HTTPException(
            status_code=400, detail="Give a time zone, a display name, or both.",
        )
    if timezone:
        try:
            ZoneInfo(timezone)
        except (ZoneInfoNotFoundError, ValueError, OSError) as e:
            raise HTTPException(
                status_code=400,
                detail=f"{timezone!r} is not a time zone this machine knows.",
            ) from e

    async with _loop_lock("state"):
        context = await _context(actor)
        _require_claimed(context)
        await _require_current_session(actor, get_deps().db)
        db = get_deps().db

        # Everything that can be discovered before writing, first: whether the
        # instance may be written to at all, and whether there is an account to
        # rename. A step that answers 404 after changing the timezone is a step
        # whose error message is a lie.
        account = None
        if display_name is not None:
            if not actor.account_id:  # pragma: no cover - require_account checked
                raise HTTPException(status_code=403, detail="No account to rename")
            account = await db.get_account(actor.account_id)
            if account is None:  # pragma: no cover - resolved a moment ago
                raise HTTPException(status_code=404, detail="Account not found")

        applied: dict[str, Any] = {}
        if timezone:
            applied = _write(
                context,
                choices=SetupChoices(timezone=timezone),
                portable_paths=("timezone",),
            )

        # The rename first, now that the file it might have to answer for has
        # been written: a *name-only* request that fails must leave no trace at
        # all, and one that also moved the timezone has to leave the timezone's
        # restart debt behind whatever happens next.
        renamed = None
        if account is not None:
            try:
                await db.update_actor_profile(
                    account["actor_id"], display_name=(display_name.strip() or None),
                    acting_account_id=actor.account_id,
                    acting_session_epoch=actor.session_epoch,
                )
                renamed = display_name.strip() or None
            except Exception as e:  # noqa: BLE001 - said, not swallowed
                logger.exception("Setup: the display name could not be written")
                if timezone:
                    # Half of it landed, so the checklist has to carry the
                    # debt for that half before this request is failed.
                    _record(context, step=STEP_PROFILE, applied=applied,
                            answered=True)
                    landed = "The time zone was saved. "
                else:
                    landed = "Nothing was written. "
                raise HTTPException(
                    status_code=500,
                    detail=f"{landed}The display name could not be saved: {e}",
                ) from e

        warning = _record(
            context, step=STEP_PROFILE, applied=applied,
            # Choosing the timezone that was already there is an answer, and
            # nothing on disk can tell it from never having been asked.
            answered=bool(timezone),
        )

        # Rendered against the actor as it is *now*. The actor this request
        # was resolved with is immutable and still carries the old name, so a
        # successful rename would come back looking unsaved — and the form,
        # comparing what it sent against what it got, would stay "changed"
        # with Save still lit.
        fresh = actor if renamed is None else replace(actor, display_name=renamed)
        return _render(await _context(fresh), warning=warning)


@router.put("/api/setup/channels", response_model=SetupStateOut)
async def set_channels(req: ChannelsRequest, actor: Actor = Depends(require_account)):
    """A Telegram bot token, which is a secret, plus the switch that uses it.

    ``telegram.enabled`` is machine-local — it says whether *this box* was
    given a token — and the token itself never leaves ``config.local.yaml``.
    Both are restart-only, so the checklist reports the restart as pending
    until an operator runs it.

    PATCH-shaped: the allow-list is written only when it is given, so setting
    a token does not clear the people already paired with the bot.
    """
    token = (req.telegram_bot_token or "").strip()
    if not token:
        raise HTTPException(
            status_code=400,
            detail="A Telegram bot token is required here; skip the step instead.",
        )

    async with _loop_lock("state"):
        context = await _context(actor)
        _require_claimed(context)
        await _require_current_session(actor, get_deps().db)
        allowed = req.telegram_allowed_users
        choices = SetupChoices(
            telegram_bot_token=token,
            telegram_allowed_users=list(allowed or []),
        )
        secret_paths = ("telegram.bot_token",)
        if allowed is not None:
            secret_paths += ("telegram.allowed_users",)
        applied = _write(
            context, choices=choices,
            machine_paths=("telegram.enabled",),
            secret_paths=secret_paths,
        )
        warning = _record(context, step=STEP_CHANNELS, applied=applied)
        logger.info(
            "Setup: a Telegram bot token was stored by account %s", actor.account_id,
        )
        return _render(await _context(actor), warning=warning)


@router.put("/api/setup/automation", response_model=SetupStateOut)
async def set_automation(req: AutomationRequest, actor: Actor = Depends(require_account)):
    """Which optional crons run, and which sources sync.

    The crons are the ones the installer already wrote: this flips ``enabled``
    on them and adds nothing, because whether an install is a personal or a
    worker one is an installer answer no running instance records — and a
    checklist step should not be rewriting a file an operator may have edited.

    PATCH-shaped throughout. Every field is optional and an omitted one is
    left exactly as it is: re-entering this step to turn one cron on must not
    switch off the sync sources somebody configured on another screen.
    """
    async with _loop_lock("state"):
        context = await _context(actor)
        _require_claimed(context)
        await _require_current_session(actor, get_deps().db)
        _require_writable(context.config)
        live = context.config.sync

        # Omitted means untouched, so each toggle starts from what the instance
        # currently says rather than from a default.
        choices = SetupChoices(
            github_sync=live.github.enabled if req.github is None else req.github,
            gmail_sync=live.gmail.enabled if req.gmail is None else req.gmail,
            gmail_accounts=(
                list(live.gmail.accounts) if req.gmail_accounts is None
                else [a.strip() for a in req.gmail_accounts if a.strip()]
            ),
            telegram_sync=(
                live.telegram.enabled if req.telegram is None else req.telegram
            ),
            telegram_api_id=int(
                (live.telegram.api_id if req.telegram_api_id is None
                 else req.telegram_api_id) or 0
            ),
            telegram_api_hash=(
                (live.telegram.api_hash if req.telegram_api_hash is None
                 else req.telegram_api_hash) or ""
            ).strip(),
        )

        # Only the paths this request actually names are written. Taking the
        # live value for an omitted field and writing *that* would be a no-op
        # in meaning and a change in the file — a diff in a git-tracked
        # settings file for a question nobody was asked.
        # One path per field actually supplied: writing both because one was
        # given would erase the other, and an empty api_hash over a working
        # one is a source that silently stops reading.
        secret_paths: tuple[str, ...] = ()
        if req.telegram_api_id is not None:
            secret_paths += ("sync.telegram.api_id",)
        if (req.telegram_api_hash or "").strip():
            secret_paths += ("sync.telegram.api_hash",)
        machine_paths: tuple[str, ...] = ()
        if req.gmail_accounts is not None:
            machine_paths = ("sync.gmail.accounts",)
        portable_paths: tuple[str, ...] = ()
        if req.github is not None:
            portable_paths += ("sync.github.enabled", "sync.github_events.enabled")
        if req.gmail is not None:
            portable_paths += ("sync.gmail.enabled",)
        if req.telegram is not None:
            portable_paths += ("sync.telegram.enabled",)

        # Everything that can fail is decided *before* anything is written.
        # The cron file is parsed and its new contents built here; if it is
        # unreadable this answers without having touched the sync settings,
        # which is what "nothing was changed" has to mean when it is said.
        plan = None
        if req.crons is not None:
            wanted = {c.strip() for c in req.crons if c.strip()}
            plan = plan_optional_crons(Path(context.config.workspace), wanted)
            if not plan.ok:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"{plan.path} could not be read ({plan.problem}); "
                        "nothing was changed."
                    ),
                )

        applied = _write(
            context, choices=choices,
            machine_paths=machine_paths,
            portable_paths=portable_paths,
            secret_paths=secret_paths,
        )

        debts: tuple[str, ...] = ()
        if plan is not None:
            try:
                debts = await _publish_cron_plan(plan)
            except Exception as e:  # noqa: BLE001 - reported with what landed
                # The sync settings are on disk by now. Publication can still
                # fail on the rename itself — a full disk, a read-only mount —
                # and answering with a bare 500 would lose both the record of
                # what *did* land and the restart it needs.
                logger.exception("Setup: the cron file could not be published")
                # Only what landed. Marking the step done here left a checklist
                # that could report `finished` for a cron selection it never
                # applied — automation's completion is a durable note, so
                # nothing later would have taken it back — and the debt would
                # have been retired at the restart as though the file had been
                # written and merely not reloaded.
                _record(
                    context, step=STEP_AUTOMATION, applied=applied, done=False,
                )
                raise HTTPException(
                    status_code=500,
                    detail=(
                        "The sync settings were saved. The cron file could not "
                        f"be written ({e}), so the crons are unchanged — the "
                        "checklist records the rest and still asks for a restart."
                    ),
                ) from e
            logger.info(
                "Setup: automation set by account %s (crons on: %s)",
                actor.account_id, ", ".join(sorted(plan.enabled)) or "none",
            )

        warning = _record(context, step=STEP_AUTOMATION, applied=applied, debts=debts)
        return _render(await _context(actor), warning=warning)


def _require_skippable(step_id: str) -> None:
    """Refuse a step that cannot be skipped, saying which kind of refusal it is.

    "That step exists and is required" and "there is no such step" are
    different mistakes, and answering both with "unknown step" sends somebody
    looking for a typo in a name that was right.
    """
    if step_id in _SKIPPABLE:
        return
    raise HTTPException(
        status_code=400,
        detail=f"{step_id!r} cannot be skipped."
        if step_id in {s for s, _t, _r in _STEPS}
        else f"Unknown setup step: {step_id}",
    )


@router.post("/api/setup/steps/{step_id}/skip", response_model=SetupStateOut)
async def skip_step(step_id: str, actor: Actor = Depends(require_account)):
    """Remember that this step was declined, so the checklist stops asking.

    "I do not want Telegram" and "I have not got to Telegram yet" look the
    same to a derived checklist, and one that keeps nagging about a decision
    already made is one people abandon. Reversible: writing the step again
    clears it, and so does :func:`unskip_step`.
    """
    _require_skippable(step_id)
    async with _loop_lock("state"):
        context = await _context(actor)
        _require_claimed(context)
        await _require_current_session(actor, get_deps().db)
        context.state.skipped.add(step_id)
        context.state.done.discard(step_id)
        _save_or_refuse(context)
        return _render(await _context(actor))


@router.post("/api/setup/steps/{step_id}/unskip", response_model=SetupStateOut)
async def unskip_step(step_id: str, actor: Actor = Depends(require_account)):
    """Put a skipped step back on the list."""
    _require_skippable(step_id)
    async with _loop_lock("state"):
        context = await _context(actor)
        _require_claimed(context)
        await _require_current_session(actor, get_deps().db)
        context.state.skipped.discard(step_id)
        _save_or_refuse(context)
        return _render(await _context(actor))
