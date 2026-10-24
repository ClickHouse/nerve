"""First-account claim (the one unauthenticated write) and the setup checklist.

Two routers:

* ``claim_router`` — ``POST /api/setup/claim``, registered only in local
  mode. It gives the install-time account a username and password, or
  records the passwordless choice. Every request must carry the setup token.
* ``router`` — the checklist, registered in **both** auth modes. Every route
  requires a signed-in *person*; the step status is derived from
  configuration, workspace files and the database (:mod:`nerve.setup_checklist`),
  every mutation is a PATCH that returns the whole state, and writes go
  through the same :mod:`nerve.setup_writer` ``nerve init`` uses.

While setup is required (local mode, instance unclaimed) every checklist
route answers 409: the claim is the only write a stranger may perform. In
external mode nothing here reads local-account state at all.
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import ipaddress
import logging
import os
import re
import weakref
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from nerve import onboarding as onboarding_flow
from nerve import (
    boot,
    config_reload,
    daemon,
    pairing,
    paths,
    setup_checks,
    setup_provenance,
    setup_state,
    setup_telegram_login,
)
from nerve.config import DEFAULT_MODEL, NerveConfig, default_timezone, get_config
from nerve.config_reload import reload_all, reload_failures
from nerve.db.accounts import AccountError, NotClaimableError, UsernameTakenError
from nerve.gateway.auth import (
    NO_IDENTITY_DETAIL,
    create_external_mcp_token,
    create_session_token,
    effective_jwt_secret,
    hash_password,
    identity_store,
    is_external_mode,
    password_length_problem,
    require_auth,
)
from nerve.gateway.routes._deps import get_deps
from nerve.gateway.routes.accounts import instance_is_passwordless, setup_required
from nerve.identity import Actor
from nerve.onboarding_catalog import ITEMS as WORK_ITEMS
from nerve.onboarding_catalog import CUSTOM_MAX, RULES_MAX, WorkSelection, in_catalog_order
from nerve.setup_checklist import (
    BLOCKED_POLICY,
    GITHUB_SWITCH,
    LOCKDOWN_TEXT,
    MODE_LOCK_REASON,
    NO_LOCAL_SECRETS_TEXT,
    SKIPPABLE_STEPS,
    STATUS_BLOCKED,
    STEP_AGENTS,
    STEP_AUTOMATION,
    STEP_CHANNELS,
    STEP_IDS,
    STEP_MEMORY,
    STEP_MODE,
    STEP_PROFILE,
    STEP_PROVIDER,
    STEP_SOURCES,
    Facts,
    Step,
    derive,
    managed_switch_reason,
    sources_available,
    steps_for_mode,
    summarize,
)
from nerve.setup_state import SetupState
from nerve.setup_token import (
    SETUP_TOKEN_NAME,
    instance_is_unclaimed,
    stored_setup_token,
    token_accepted,
)
from nerve.setup_writer import (
    CONFIG_YAML_HEADER,
    DEFAULT_QUIET_WINDOW,
    ModeRefused,
    apply_mode,
    apply_profile_templates,
    bedrock_geo_prefix,
    build_config_layers,
    build_config_local,
    choices_from_config,
    leaf_paths,
    list_optional_crons,
    mcp_url,
    merge_machine_paths,
    merge_private_paths,
    merge_settings_defaults,
    merge_settings_paths,
    plan_optional_crons,
    render_work_description,
    replace_choices,
    settings_problem,
    task_description_from,
    work_selection_from,
    workspace_mode,
)

logger = logging.getLogger(__name__)

claim_router = APIRouter()
router = APIRouter()

_GUARD_REFUSED = (
    "A valid setup token is required. Run 'nerve status' on the server to "
    "read the current token."
)


class ClaimRequest(BaseModel):
    """Exactly one of ``password`` and ``passwordless: true``.

    ``username`` is required with a password and optional without one.
    """

    username: str | None = None
    password: str | None = Field(default=None, min_length=1)
    passwordless: bool = False
    setup_token: str = Field(min_length=1)
    display_name: str | None = None


class ClaimResponse(BaseModel):
    """The client session created by a successful claim."""

    token: str


@claim_router.post("/api/setup/claim", response_model=ClaimResponse)
async def claim(req: ClaimRequest):
    """Atomically complete setup of the sole account, then sign the client in."""
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

    # Check the token first, so a caller without it learns nothing.
    stored = await stored_setup_token(store)
    if not token_accepted(req.setup_token, stored):
        raise HTTPException(status_code=403, detail=_GUARD_REFUSED)

    if (req.password is None) != req.passwordless:
        raise HTTPException(
            status_code=400,
            detail="Supply a password, or set passwordless to true to keep "
                   "this installation passwordless. Not both.",
        )
    credential = None
    if req.password is not None:
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

    logger.info(
        "Instance claimed: account %s %s",
        account["id"],
        "now has a password" if credential else "stays passwordless by choice",
    )
    return ClaimResponse(token=create_session_token(secret, account["id"]))


# --------------------------------------------------------------------------- #
#  The checklist: plumbing                                                     #
# --------------------------------------------------------------------------- #


class SetupFieldError(HTTPException):
    """An error that names the JSON field it is about.

    400 for a validation problem; 409 (``status_code=409``) when the field is
    refused because of a read-only state — lockdown accepting only ``name``
    on the profile step, a value the operator provisioned — which is not
    something a different value could fix; 429 with ``retry_after`` (seconds)
    when a remote service asks for a pause. Rendered as
    ``{"detail": ..., "field": ...[, "retry_after": n]}`` by
    :func:`install_handlers` (``field`` only when there is one); without that
    handler FastAPI's default still answers with ``detail``.
    """

    def __init__(
        self,
        field: str | None,
        detail: str,
        *,
        status_code: int = 400,
        retry_after: int | None = None,
    ):
        super().__init__(status_code=status_code, detail=detail)
        self.field = field
        self.retry_after = retry_after


def install_handlers(app) -> None:
    """Register the field-error renderer on an app."""
    from fastapi.responses import JSONResponse

    @app.exception_handler(SetupFieldError)
    async def _setup_field_error(request, exc: SetupFieldError):  # noqa: ANN001
        content: dict[str, Any] = {"detail": exc.detail}
        if exc.field is not None:
            content["field"] = exc.field
        if exc.retry_after is not None:
            content["retry_after"] = exc.retry_after
        return JSONResponse(status_code=exc.status_code, content=content)


# Every single-line field the wizard writes into the agent's instruction files
# (USER.md, IDENTITY.md) or into a header: a control character would start a
# new line inside a file that is loaded into the system prompt, or an illegal
# HTTP header value in a probe. ``\x7f`` and C1 controls included.
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f-\x9f]")
_SINGLE_LINE_DETAIL = "Use a single line without control characters."


def _reject_control_chars(field: str, value: str | None, detail: str = _SINGLE_LINE_DETAIL) -> None:
    if value and _CONTROL_RE.search(value):
        raise SetupFieldError(field, detail)


async def require_person(actor: Actor = Depends(require_auth)) -> Actor:
    """Setup is driven by a person: a system or MCP token gets 403."""
    if not actor.is_human:
        raise HTTPException(
            status_code=403,
            detail="Setup is driven by a person, not by a system or MCP token.",
        )
    return actor


# One mutation at a time. Every step reads the checklist's notes, changes them
# and writes them back, so two requests in flight — two tabs, or one tab's form
# and another's skip — would each save over a snapshot taken before the other.
#
# One lock per event loop rather than one module-level lock, because an
# `asyncio.Lock` binds itself to the loop that first *waits* on it and refuses
# every other one afterwards.
_locks: "weakref.WeakKeyDictionary[Any, dict[str, asyncio.Lock]]" = (
    weakref.WeakKeyDictionary()
)


def _loop_lock(name: str) -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    locks = _locks.setdefault(loop, {})
    if name not in locks:
        locks[name] = asyncio.Lock()
    return locks[name]


_UNCLAIMED_REFUSED = "Setup of this instance is not complete; claim it first."
_NO_CONFIG_DIR = (
    "This instance has no machine-local configuration directory, so there is "
    "nowhere to write. Run nerve init on the machine."
)
_BUSY = "Another check is running; try again in a moment."
_RESTART_DELAY_SECONDS = 0.75

# How response bodies name the files the wizard writes: the display paths
# the Review table uses (``setup_checklist.files_for``), never an absolute
# server path — in external mode any signed-in person reads these bodies.
# The absolute paths go to the log, where the operator looks.
_DISPLAY_LOCAL = "config.local.yaml"
_DISPLAY_MACHINE = "config.yaml"
_DISPLAY_SETTINGS = "workspace/config/settings.yaml"
_DISPLAY_CRON = "workspace/config/cron/system.yaml"
_DISPLAY_STATE = "setup-state.json in the state directory"
_SAVED_BUT_STATE_UNWRITTEN = (
    "The configuration was written, but the checklist could not record it in "
    f"{_DISPLAY_STATE} — it may ask for this again. The instance itself is configured."
)

# The Claude Code login found on the gateway host belongs to whoever runs the
# server. In external mode every gateway-authenticated person may run setup,
# so importing that personal credential into the shared instance is not
# offered there unless the operator opts in explicitly.
HOST_LOGIN_OPT_IN_ENV = "NERVE_SETUP_HOST_LOGIN"
_HOST_LOGIN_NOT_OFFERED = (
    "The Claude Code login on this server is not offered on an instance with "
    "external authentication: it belongs to whoever runs the server, and any "
    "signed-in person could otherwise import it. Paste an API key or a "
    f"subscription token instead — or the operator sets {HOST_LOGIN_OPT_IN_ENV}=1 "
    "in the service environment to allow it."
)


def _host_login_refusal(config: NerveConfig) -> str | None:
    """Why the server's own Claude Code login is not offered here, or ``None``.

    Copying it writes a credential into config.local.yaml, so an instance
    that keeps none on disk refuses it first; then the external-mode rule.
    """
    if not config.setup.local_secrets:
        return NO_LOCAL_SECRETS_TEXT
    if is_external_mode() and os.environ.get(HOST_LOGIN_OPT_IN_ENV) != "1":
        return _HOST_LOGIN_NOT_OFFERED
    return None


def _hide_paths(text: str, config: NerveConfig | None = None) -> str:
    """``text`` with the instance's absolute roots replaced by display labels.

    The workspace, the config directory, the state directory and the home
    directory — longest first, so a workspace under the home directory is
    replaced as a whole.
    """
    roots: list[tuple[str, str]] = []
    if config is not None:
        if config.workspace:
            roots.append((str(Path(config.workspace)), "workspace"))
        if config.config_dir:
            roots.append((str(Path(config.config_dir)), "<config dir>"))
    roots.append((str(paths.nerve_home()), "<state dir>"))
    roots.append((str(Path.home()), "~"))
    for root, label in sorted(roots, key=lambda pair: -len(pair[0])):
        if root and root != "/":
            text = text.replace(root, label)
    return text


def _public_error(exc: BaseException, config: NerveConfig | None = None) -> str:
    """An exception's text for a response body — without a server path.

    ``str(OSError)`` embeds the filename, so an ``OSError`` is reduced to its
    ``strerror``; every other message has the instance's roots hidden
    (:func:`_hide_paths`). The full text belongs in the log, not here.
    """
    if isinstance(exc, OSError) and exc.strerror:
        text = exc.strerror
    else:
        try:
            text = str(exc) or type(exc).__name__
        except Exception:  # noqa: BLE001 — a broken __str__ must not leak anything
            text = type(exc).__name__
    return _hide_paths(text, config)

# Per-process memory: the last successful provider test, and the bot username
# from the last successful getMe, keyed by a hash of the token (never the
# token). Both are lost at the restart; the state file carries the username
# across it (telegram_bot_username), the test result is meant to be lost.
_last_provider_test: dict[str, Any] | None = None
_bot_usernames: dict[str, str] = {}
_restart_requested = False


def _token_key(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class _Context:
    facts: Facts
    actor: Actor

    @property
    def config(self) -> NerveConfig:
        return self.facts.config

    @property
    def state(self) -> SetupState:
        return self.facts.state


def _writable_problem(config: NerveConfig) -> str | None:
    if config.lockdown:
        return LOCKDOWN_TEXT
    if not config.config_dir or not Path(config.config_dir).is_dir():
        return _NO_CONFIG_DIR
    return None


def _telegram_live(config: NerveConfig) -> bool:
    """A Telegram channel is registered, started, and runs the live token."""
    try:
        engine = get_deps().engine
    except RuntimeError:
        return False
    channel = engine.channel("telegram") if engine is not None and hasattr(engine, "channel") else None
    if channel is None or getattr(channel, "_app", None) is None:
        return False
    started = config_reload.startup_config()
    if started is None:
        return bool(config.telegram.bot_token)
    return bool(config.telegram.bot_token) and (
        started.telegram.bot_token == config.telegram.bot_token
    )


def _bot_username(config: NerveConfig, state: SetupState, live: bool) -> str | None:
    if live:
        try:
            channel = get_deps().engine.channel("telegram")
            name = getattr(channel, "bot_username", None)
            if name:
                return str(name)
        except Exception:  # noqa: BLE001
            pass
    if not config.telegram.bot_token:
        # No token configured: there is no bot to name, whatever the state
        # file still remembers from an earlier token.
        return None
    cached = _bot_usernames.get(_token_key(config.telegram.bot_token))
    if cached:
        return cached
    return state.telegram_bot_username


async def _onboarding(config: NerveConfig) -> tuple[str, str | None, str]:
    """The onboarding status, its session id and its kind (:func:`nerve.onboarding.read_state`).

    Without a database (``nerve doctor``, a stand-in) a worker falls back to
    the ``TASK.md`` marker and a personal install reads ``not_applicable``.
    """
    try:
        db = get_deps().db
    except RuntimeError:
        db = None
    if db is None:
        from nerve.setup_checklist import onboarding_from_task

        status = onboarding_from_task(config.workspace, workspace_mode(config.workspace))
        return status, None, onboarding_flow.CONVERSATION
    state = await onboarding_flow.read_state(db, Path(config.workspace))
    return state.status, state.session_id, state.kind


def _onboarding_awaiting(status: str, session_id: str | None) -> str | None:
    """What the running onboarding session waits for from the person.

    ``plan`` while an ``ExitPlanMode`` decision is pending, ``question`` while
    an ``AskUserQuestion`` card or an ``ask_form`` form is (the ``form``
    interaction), else ``None`` (also for other pauses, and whenever
    onboarding is not running). A plan wins over a question.
    """
    if status != onboarding_flow.RUNNING or not session_id:
        return None
    from nerve.agent.interactive import get_handler

    handler = get_handler(session_id)
    if handler is None:
        return None
    waiting = handler.pending_interaction_types()
    if "plan_exit" in waiting:
        return "plan"
    if "question" in waiting or "form" in waiting:
        return "question"
    return None


async def _context(actor: Actor) -> _Context:
    """The checklist's world, as one read. No shell-outs, no network."""
    config = get_config()
    external = is_external_mode()
    deps = get_deps()
    db = deps.db
    unclaimed = False
    username = None
    passwordless = False
    if not external:
        login = await db.login_state()
        unclaimed = setup_required(login, config)
        passwordless = instance_is_passwordless(login, config)
        if actor.account_id:
            account = await db.get_account(actor.account_id)
            username = (account or {}).get("username")
    state = setup_state.load_state()
    setup_state.retire_on_boot(state)
    live = _telegram_live(config)
    onboarding, onboarding_id, onboarding_kind = await _onboarding(config)
    env_key = bool(os.environ.get("ANTHROPIC_API_KEY"))
    env_oauth = bool(os.environ.get("CLAUDE_CODE_OAUTH_TOKEN"))
    try:
        from nerve.external_agents.registry import AGENT_REGISTRY

        available = tuple(AGENT_REGISTRY.keys())
    except Exception:  # noqa: BLE001
        available = ()
    facts = Facts(
        credential_sources=setup_provenance.credential_sources(config),
        machine_switches=setup_provenance.machine_switches(config),
        hosted_gateway=_hosted_gateway_status(config),
        telegram_session_present=setup_telegram_login.session_present(),
        config=config,
        workspace=Path(config.workspace),
        state=state,
        external_auth=external,
        unclaimed=unclaimed,
        lockdown=bool(config.lockdown),
        writable_problem=_writable_problem(config),
        restart_pending=config_reload.restart_pending(),
        telegram_live=live,
        telegram_start_error=setup_state.boot_errors_for_current_boot(state).get("telegram"),
        env_credential=env_key or env_oauth,
        env_oauth_only=env_oauth and not env_key,
        default_timezone=default_timezone(),
        onboarding=onboarding,
        onboarding_session_id=onboarding_id,
        onboarding_kind=onboarding_kind,
        onboarding_awaiting=_onboarding_awaiting(onboarding, onboarding_id),
        display_name=actor.display_name,
        bot_username=_bot_username(config, state, live),
        account_username=username,
        account_passwordless=passwordless,
        provider_last_test=_last_provider_test,
        available_agents=available,
    )
    return _Context(facts=facts, actor=actor)


def _hosted_gateway_status(config: NerveConfig) -> dict[str, Any] | None:
    """What the hosted channel runtime reports about the gateway's streams
    (``slack.mode: hosted`` or ``sync.github.mode: hosted`` only); a quiet
    default when it has not started."""
    from nerve.channels.hosted.runtime import hosted_providers

    if not hosted_providers(config):
        return None
    try:
        from nerve.gateway import server

        runtime = getattr(server, "_hosted_channels", None)
        if runtime is not None:
            return runtime.streams.status()
    except Exception:  # noqa: BLE001 — a stand-in, or a runtime that failed to start
        pass
    return {"streams": 0, "connected": False, "last_event_at": None}


_MANAGED_DETAIL = {
    setup_provenance.SOURCE_ENVIRONMENT: (
        "This value is configured by your operator from the environment and cannot be changed here."
    ),
    setup_provenance.SOURCE_CONFIG_REPO: (
        "This value is managed from the configuration repository and cannot be changed here."
    ),
    setup_provenance.SOURCE_GATEWAY: (
        "This connection is held by the channel gateway; there is no token to change here."
    ),
}


def _refuse_managed(ctx: _Context, path: str, field: str) -> None:
    """409 ``{detail, field}`` when the value at ``path`` came from somewhere
    the page may not overwrite: the environment (a variable Nerve reads, or a
    ``${VAR}`` reference in a layer), the configuration repository, or the
    channel gateway — or, for a switch, a machine layer the operator holds it
    in (:func:`nerve.setup_checklist.managed_switch_reason`). A value from a
    file on this machine is replaceable."""
    reason = managed_switch_reason(ctx.facts, path)
    if reason is not None:
        raise SetupFieldError(field, reason, status_code=409)
    source = ctx.facts.credential_sources.get(path)
    if source in (None, setup_provenance.SOURCE_FILE):
        return
    detail = _MANAGED_DETAIL.get(source, _MANAGED_DETAIL[setup_provenance.SOURCE_ENVIRONMENT])
    raise SetupFieldError(field, detail, status_code=409)


def _refuse_without_local_secrets(ctx: _Context, field: str | None = None) -> None:
    """409 when this instance keeps no credentials on disk
    (``setup.local_secrets: false``) and the request would store one. With
    ``field`` the body names it — the shape of the managed-value refusal —
    for a request that is otherwise fine (the provider's model picker)."""
    if ctx.config.setup.local_secrets:
        return
    if field is not None:
        raise SetupFieldError(field, NO_LOCAL_SECRETS_TEXT, status_code=409)
    raise HTTPException(status_code=409, detail=NO_LOCAL_SECRETS_TEXT)


def _require_claimed(ctx: _Context) -> None:
    if ctx.facts.unclaimed:
        raise HTTPException(status_code=409, detail=_UNCLAIMED_REFUSED)


def _require_writable(ctx: _Context) -> Path:
    problem = ctx.facts.writable_problem
    if problem:
        raise HTTPException(status_code=409, detail=problem)
    return Path(ctx.config.config_dir)


def _restart_decision(facts: Facts) -> tuple[str, str, str | None, int]:
    """``(mode, message, command, timeout_hint_seconds)`` for the current state."""
    if not facts.restart_pending and not facts.state.debts:
        return (
            "reload", "Nothing needed a restart; the configuration was reloaded.",
            None, 0,
        )
    how = daemon.restart_mode()
    if how == "exit":
        if daemon.in_container():
            return (
                "restart",
                "The container exits and Docker starts it again (restart: unless-stopped). "
                "If you stopped it with docker compose stop, run docker compose up -d.",
                "docker compose up -d", 180,
            )
        # NERVE_SUPERVISOR=exit on a plain host: whatever supervises the
        # process restarts it; Docker Compose is not involved.
        return (
            "restart",
            "The process exits and its supervisor starts it again; this page reconnects on its own.",
            None, 180,
        )
    if how == "refuse":
        if daemon.is_systemd_managed():
            # Leaf unit only, `--user` under a user manager (daemon.py): a
            # wrong unit here would be a harmful instruction with a Copy button.
            command = daemon.systemd_restart_command()
            return (
                "unsupported",
                f"This instance is managed by systemd. On the server run: {command} "
                "— this page reconnects by itself.",
                command, 300,
            )
        # NERVE_SUPERVISOR=refuse without systemd: the operator's supervisor
        # must be asked; NERVE_RESTART_COMMAND names the command when known,
        # and the message says the variable exists when it is not set.
        command = (os.environ.get(daemon.RESTART_COMMAND_ENV) or "").strip() or None
        if command:
            message = (
                f"This instance is restarted by its operator. On the server run: {command} "
                "— this page reconnects by itself."
            )
        else:
            message = (
                "This instance is restarted by its operator. Ask whoever runs the supervisor "
                f"to restart Nerve; this page reconnects by itself. Set {daemon.RESTART_COMMAND_ENV} "
                "in the service environment to show that command here."
            )
        return ("unsupported", message, command, 300)
    return ("restart", "Restarting. This page reconnects on its own.", None, 120)


def _supervision() -> str:
    """``none`` | ``docker`` | ``systemd`` | ``supervisor`` — what restarts the process."""
    if daemon.in_container():
        return "docker"
    if daemon.is_systemd_managed():
        return "systemd"
    if (os.environ.get(daemon.SUPERVISOR_ENV) or "").strip().lower() in ("exit", "refuse"):
        return "supervisor"
    return "none"


def _render(ctx: _Context, *, warning: str | None = None) -> dict[str, Any]:
    body = summarize(ctx.facts)
    mode, message, command, _hint = _restart_decision(ctx.facts)
    body.update({
        "generation": boot.boot_id(),
        "restart_mode": mode,
        "restart_message": message,
        # The operator's command as data (`systemctl …`, `docker compose up
        # -d`, NERVE_RESTART_COMMAND), so the Review step never has to parse
        # it out of the sentence; null when there is none.
        "restart_command": command,
        "warning": warning,
    })
    return body


async def _reload(config_dir: Path) -> dict:
    from nerve.gateway import server

    try:
        engine = get_deps().engine
    except RuntimeError:
        engine = None
    return await reload_all(engine, getattr(server, "_cron_service", None), config_dir)


def _warning_from(summary: dict, saved: bool) -> str | None:
    parts = []
    failures = reload_failures(summary)
    if failures:
        parts.append("Reload reported: " + "; ".join(f"{k}: error: {v}" for k, v in sorted(failures.items())))
    if not saved:
        logger.warning("Setup: the state file %s could not be written", setup_state.state_file())
        parts.append(_SAVED_BUT_STATE_UNWRITTEN)
    return ("Saved. " + " ".join(parts)) if parts else None


def _save_or_refuse(state: SetupState) -> None:
    if not setup_state.save_state(state):
        logger.warning("Setup: the state file %s could not be written", setup_state.state_file())
        raise HTTPException(
            status_code=500,
            detail=(
                f"The checklist could not be saved to {_DISPLAY_STATE}, "
                "so that choice was not remembered. Nothing else changed."
            ),
        )


def _select(choices, machine_paths=(), portable_paths=(), secret_paths=()):
    """The three update dicts for the named paths, from the shared builders."""
    machine, portable, _shadowed = build_config_layers(choices)
    machine_flat = leaf_paths(machine)
    portable_flat = leaf_paths(portable)
    secret_flat = leaf_paths(build_config_local(choices))
    return (
        {p: machine_flat[p] for p in machine_paths if p in machine_flat},
        {p: portable_flat[p] for p in portable_paths if p in portable_flat},
        {p: secret_flat[p] for p in secret_paths if p in secret_flat},
    )


def _write(
    ctx: _Context,
    *,
    machine: dict[str, Any] | None = None,
    portable: dict[str, Any] | None = None,
    secret: dict[str, Any] | None = None,
    shadowed: list[str] | None = None,
) -> list[str]:
    """Publish one step's keys: L → M → S. Returns what landed (for errors).

    The tracked settings file is checked before anything is published, so a
    step does not write a credential and then discover the second file is
    unusable. ``None`` values remove the path.
    """
    config_dir = _require_writable(ctx)
    workspace = Path(ctx.config.workspace)
    machine = machine or {}
    portable = portable or {}
    secret = secret or {}
    shadowed = shadowed or []
    # Every file this step touches is checked for parseability *before* the
    # first publish, so a hand-corrupted file is a 409 with nothing written
    # rather than a failure after a credential already landed.
    config = ctx.config
    if portable or shadowed:
        problem = settings_problem(workspace)
        if problem:
            logger.warning("Setup: %s is not usable as settings (%s)", workspace / "config" / "settings.yaml", problem)
            raise HTTPException(
                status_code=409,
                detail=(
                    f"{_DISPLAY_SETTINGS} is not usable as settings "
                    f"({_hide_paths(problem, config)}), so nothing was written. Fix it in "
                    "the workspace repository and try again."
                ),
            )
    for name, wanted in ((_DISPLAY_LOCAL, secret), (_DISPLAY_MACHINE, machine)):
        if wanted:
            problem = _machine_file_problem(config_dir / name)
            if problem:
                logger.warning("Setup: %s %s", config_dir / name, problem)
                raise HTTPException(
                    status_code=409,
                    detail=f"{name} {problem}; nothing was written. Fix the file on the server and try again.",
                )
    landed: list[str] = []

    def _landed() -> str:
        return ", ".join(landed) or "nothing"

    try:
        if secret:
            merge_private_paths(config_dir / "config.local.yaml", secret)
            landed.append(_DISPLAY_LOCAL)
        if machine:
            merge_machine_paths(config_dir / "config.yaml", machine, header=CONFIG_YAML_HEADER)
            landed.append(_DISPLAY_MACHINE)
    except paths.InsecureFileError as e:
        logger.warning("Setup: %s", e)
        raise HTTPException(
            status_code=500, detail=f"{_public_error(e, config)} Landed so far: {_landed()}.",
        ) from e
    except (OSError, ValueError, yaml.YAMLError) as e:
        logger.warning("Setup: could not write configuration in %s: %s", config_dir, e)
        raise HTTPException(
            status_code=500,
            detail=f"Could not write configuration: {_public_error(e, config)}. Landed so far: {_landed()}.",
        ) from e
    if portable or shadowed:
        try:
            outcome = merge_settings_paths(workspace, portable, shadowed)
        except (OSError, ValueError, yaml.YAMLError) as e:
            logger.warning("Setup: could not write %s: %s", workspace / "config" / "settings.yaml", e)
            raise HTTPException(
                status_code=500,
                detail=f"Could not write {_DISPLAY_SETTINGS}: {_public_error(e, config)}. "
                       f"Landed so far: {_landed()}.",
            ) from e
        if outcome.status in {"invalid_yaml", "not_a_mapping"}:  # pragma: no cover
            logger.warning("Setup: %s is not usable as settings (%s)", outcome.path, outcome.status)
            raise HTTPException(
                status_code=409,
                detail=f"{_DISPLAY_SETTINGS} is not usable as settings; it was left alone. "
                       f"Landed so far: {_landed()}.",
            )
        landed.append(_DISPLAY_SETTINGS)
    return landed


def _machine_file_problem(path: Path) -> str | None:
    """Why a config-dir YAML file could not be merged into, or ``None``."""
    if not path.exists():
        return None
    try:
        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError:
        return "is not valid YAML"
    except OSError as e:
        return f"could not be read ({e.strerror or e})"
    if loaded is not None and not isinstance(loaded, dict):
        return "is not a mapping"
    return None


async def _finish_mutation(
    ctx: _Context,
    *,
    step: str,
    answered: bool = False,
    debts: dict[str, str] | None = None,
    clear_debts: tuple[str, ...] = (),
    reload: bool = True,
    after_reload: Any = None,
) -> dict[str, Any]:
    """Reload, book-keep, save, render — the tail of every PUT.

    ``after_reload`` (an async callable) runs once the new configuration is
    live and before the state is rendered: the place for a step to apply a
    value to a running service and note it as applied (§5.2).
    """
    summary: dict = {}
    # A debt this step settles is cleared *before* the reload is judged, so a
    # reload that fails right now records a fresh debt instead of having it
    # popped by the very step that caused it (the cron step clears "cron").
    for name in clear_debts:
        ctx.state.debts.pop(name, None)
    if reload:
        summary = await _reload(Path(ctx.config.config_dir))
        if reload_failures(summary).get("cron"):
            ctx.state.debts["cron"] = "the scheduler is still running the cron settings it started with"
        if after_reload is not None:
            await after_reload()
    for name, text in (debts or {}).items():
        ctx.state.debts[name] = text
    if answered:
        ctx.state.answered.add(step)
    ctx.state.skipped.discard(step)
    saved = setup_state.save_state(ctx.state)
    warning = _warning_from(summary, saved)
    return _render(await _context(ctx.actor), warning=warning)


# --------------------------------------------------------------------------- #
#  Models                                                                      #
# --------------------------------------------------------------------------- #


# Size bounds on every request body. These are coarse limits against an
# authenticated person posting megabytes into config.local.yaml or
# settings.yaml; the handlers keep their own, friendlier field-level 400s
# ("Keep this under 80 characters"), so the schema caps sit above them. The
# RequestValidationError handler strips ``input``, so a 422 echoes no value.
_SHORT = 320          # names, labels, languages (handler caps are 40–80)
_LONG = 2400          # free text such as "about" (handler cap 600)
_SECRET = 256         # API keys, OAuth tokens, hashes
_TOKEN = 128          # Telegram bot tokens
_LIST = 200           # allowlists, accounts, crons, agents
_USER_IDS = 1000      # Telegram user ids


class WorkRequest(BaseModel):
    """What a worker should do: catalog ids, custom work, rules (each optional)."""

    items: list[Annotated[str, Field(max_length=64)]] | None = Field(default=None, max_length=64)
    custom: str | None = Field(default=None, max_length=8000)
    rules: str | None = Field(default=None, max_length=4000)


class ModeRequest(BaseModel):
    mode: str = Field(max_length=32)
    work: WorkRequest | None = None
    # Older clients' free-text note, an alias for ``work.custom``:
    # ``task_description`` is what the shipped page sends, ``note`` the
    # contract's name for it.
    note: str | None = Field(default=None, max_length=8000)
    task_description: str | None = Field(default=None, max_length=8000)


class ProviderRequest(BaseModel):
    type: str | None = Field(default=None, max_length=32)
    anthropic_api_key: str | None = Field(default=None, max_length=_SECRET)
    claude_oauth_token: str | None = Field(default=None, max_length=_SECRET)
    aws_region: str | None = Field(default=None, max_length=64)
    aws_profile: str | None = Field(default=None, max_length=128)
    model: str | None = Field(default=None, max_length=128)
    effort: str | None = Field(default=None, max_length=16)


class ProfileRequest(BaseModel):
    name: str | None = Field(default=None, max_length=_SHORT)
    timezone: str | None = Field(default=None, max_length=64)
    # Declared before the times: a lockdown refusal names the first refused
    # key, and with the switch in the body that should be the switch.
    quiet_enabled: bool | None = None
    quiet_start: str | None = Field(default=None, max_length=16)
    quiet_end: str | None = Field(default=None, max_length=16)
    language: str | None = Field(default=None, max_length=_SHORT)
    agent_name: str | None = Field(default=None, max_length=_SHORT)
    agent_emoji: str | None = Field(default=None, max_length=32)
    about: str | None = Field(default=None, max_length=_LONG)


class MemoryRequest(BaseModel):
    openai_api_key: str | None = Field(default=None, max_length=_SECRET)
    embeddings: bool | None = None


class TelegramPatch(BaseModel):
    bot_token: str | None = Field(default=None, max_length=_TOKEN)
    enabled: bool | None = None
    allowed_users: list[Annotated[int, Field(gt=0)]] | None = Field(default=None, max_length=_USER_IDS)
    allowed_users_add: list[Annotated[int, Field(gt=0)]] | None = Field(default=None, max_length=_USER_IDS)


class SlackPatch(BaseModel):
    bot_token: str | None = Field(default=None, max_length=_SECRET)
    app_token: str | None = Field(default=None, max_length=_SECRET)
    enabled: bool | None = None
    allow_users: list[Annotated[str, Field(max_length=64)]] | None = Field(default=None, max_length=_LIST)
    allow_direct_messages: bool | None = None
    allow_channels: list[Annotated[str, Field(max_length=64)]] | None = Field(default=None, max_length=_LIST)


class ChannelsRequest(BaseModel):
    telegram: TelegramPatch | None = None
    slack: SlackPatch | None = None


class SourcesRequest(BaseModel):
    github: bool | None = None
    gmail: bool | None = None
    gmail_accounts: list[Annotated[str, Field(max_length=254)]] | None = Field(default=None, max_length=_LIST)
    telegram: bool | None = None
    telegram_api_id: int | None = Field(default=None, gt=0)
    telegram_api_hash: str | None = Field(default=None, max_length=_SECRET)


class AutomationRequest(BaseModel):
    crons: list[Annotated[str, Field(max_length=64)]] = Field(max_length=_LIST)


class AgentsRequest(BaseModel):
    agents: list[Annotated[str, Field(max_length=64)]] = Field(max_length=_LIST)
    conflict_policy: str = Field(default="backup", max_length=16)


class MemoryTestRequest(BaseModel):
    openai_api_key: str | None = Field(default=None, max_length=_SECRET)


class TelegramTestRequest(BaseModel):
    bot_token: str | None = Field(default=None, max_length=_TOKEN)


class SlackTestRequest(BaseModel):
    bot_token: str | None = Field(default=None, max_length=_SECRET)
    app_token: str | None = Field(default=None, max_length=_SECRET)


# --------------------------------------------------------------------------- #
#  GET /api/setup                                                              #
# --------------------------------------------------------------------------- #


@router.get("/api/setup")
async def get_setup(actor: Actor = Depends(require_person)):
    """The checklist. Pure over config, files, the database and the state file."""
    async with _loop_lock("state"):
        ctx = await _context(actor)
        _require_claimed(ctx)
        if ctx.state.retired:
            setup_state.save_state(ctx.state)
        return _render(ctx)


# --------------------------------------------------------------------------- #
#  PUT /api/setup/mode                                                         #
# --------------------------------------------------------------------------- #

TASK_LOCK_REASON = (
    "The task description was written to TASK.md when the mode was chosen, and "
    "the worker reads and maintains that file itself. To change the task, edit "
    "TASK.md in the workspace (or tell the worker in chat); the Mode step is "
    "chosen once."
)
# Unknown ids named in one refusal, at most.
_UNKNOWN_ITEMS_SHOWN = 3


def _task_description_on_disk(task_path: Path) -> str | None:
    """The description a not-yet-onboarded ``TASK.md`` carries — its "Task
    description (as given)" section — or ``None`` once the worker's onboarding
    has completed the file (or it is unreadable)."""
    try:
        text = task_path.read_text(encoding="utf-8")
    except OSError:
        return None
    return task_description_from(text)


def _work_from_request(req: ModeRequest, mode: str) -> WorkSelection:
    """The work a Mode ``PUT`` chooses, checked (``400`` naming the field).

    ``work.custom`` wins over the older clients' ``note`` and
    ``task_description``, and a refused length names the field the text came
    in. A body with a note and no ``work`` is a free-text selection, written
    as before: a plain paragraph. Ids are kept once each, in catalog order;
    an unknown one is refused, and so is any item for a personal install,
    whose custom work and rules are checked and, as before, written nowhere.
    """
    work = req.work
    if work is not None and work.custom is not None:
        custom, custom_field = work.custom, "work.custom"
    elif req.note is not None:
        custom, custom_field = req.note, "note"
    elif req.task_description is not None:
        custom, custom_field = req.task_description, "task_description"
    else:
        custom, custom_field = "", "work.custom"
    custom = custom.strip()
    rules = ((work.rules if work is not None else None) or "").strip()
    ids = [i.strip() for i in ((work.items if work is not None else None) or []) if i.strip()]
    unknown = sorted({i for i in ids if i not in WORK_ITEMS})
    if unknown:
        shown = ", ".join(unknown[:_UNKNOWN_ITEMS_SHOWN]) + (", …" if len(unknown) > _UNKNOWN_ITEMS_SHOWN else "")
        raise SetupFieldError("work.items", f"Unknown work item: {shown}.")
    if ids and mode != "worker":
        raise SetupFieldError(
            "work.items",
            "A personal assistant takes no work items: choose Worker for this work, or "
            "describe it to the agent in the onboarding conversation.",
        )
    if len(custom) > CUSTOM_MAX:
        what = "the task description" if custom_field == "task_description" else "the custom work"
        raise SetupFieldError(custom_field, f"Keep {what} under {CUSTOM_MAX} characters.")
    if len(rules) > RULES_MAX:
        raise SetupFieldError("work.rules", f"Keep what it must know or never do under {RULES_MAX} characters.")
    return WorkSelection(items=in_catalog_order(ids), custom=custom, rules=rules, free_text=work is None)


@router.put("/api/setup/mode")
async def set_mode(req: ModeRequest, actor: Actor = Depends(require_person)):
    mode = (req.mode or "").strip().lower()
    if mode not in ("personal", "worker"):
        raise SetupFieldError("mode", "Choose personal or worker.")
    selection = _work_from_request(req, mode)
    # A worker's choice becomes the description section of its TASK.md; a
    # personal install has no TASK.md.
    task = render_work_description(selection) if mode == "worker" else ""
    async with _loop_lock("state"):
        ctx = await _context(actor)
        _require_claimed(ctx)
        ws = Path(ctx.config.workspace)
        current = workspace_mode(ws)
        locked = bool(ctx.config.lockdown)
        if current is not None and current != mode:
            raise HTTPException(status_code=409, detail=MODE_LOCK_REASON)
        if current == "worker" and task and (ws / "TASK.md").exists():
            # Chosen once: the same choice again is a no-op (nothing is
            # rewritten), and a *different* one is refused rather than
            # silently replacing what a stale tab or a second person did not
            # see. The same items, custom work and rules are the same choice
            # in either form (a selection, or an older client's note). Only a
            # worker whose TASK.md is still absent (templates arrived from a
            # config repository) — or still the bare template, with no
            # description in it — gets it written here.
            on_disk = _task_description_on_disk(ws / "TASK.md")
            if on_disk is None or (on_disk and work_selection_from(on_disk) != selection):
                raise HTTPException(status_code=409, detail=TASK_LOCK_REASON)
            if on_disk:
                task = ""
        if current is None and locked:
            steps = {s["id"]: s for s in summarize(ctx.facts)["steps"]}
            raise HTTPException(status_code=409, detail=steps[STEP_MODE]["blocked_reason"])
        try:
            outcome = apply_mode(
                ws, mode, task, in_container=daemon.in_container(), locked=locked,
                memory_backend=ctx.config.memory.backend,
            )
        except ModeRefused as e:
            raise HTTPException(status_code=409, detail=str(e)) from e
        except OSError as e:
            logger.warning("Setup: could not write the workspace %s: %s", ws, e)
            raise HTTPException(
                status_code=500, detail=f"Could not write the workspace: {_public_error(e, ctx.config)}",
            ) from e
        logger.info(
            "Setup: mode %s chosen (templates created: %d, task written: %s)",
            mode, len(outcome.created), outcome.task_written,
        )

        async def _apply_memory_live() -> None:
            await _apply_memory_defaults_live(outcome.settings_defaults_added)

        return await _finish_mutation(ctx, step=STEP_MODE, after_reload=_apply_memory_live)


_APPLY_CATEGORIES_TIMEOUT = 15.0


async def _apply_memory_defaults_live(added: tuple[str, ...]) -> None:
    """Hand the Mode step's ``memory.*`` defaults to the running process.

    ``memory`` is restart-only in the reload table, and a fresh install's
    process started with an empty category list — so every browser setup
    would end in a restart for the one thing the Mode step always writes.
    Two leaves the step fills in can be made true of the running process now,
    after which the startup snapshot is patched at exactly those paths (§5.2)
    so the Review step tells the truth:

    * ``memory.categories`` — the running memU takes the set
      (``MemUBridge.apply_categories`` does what a boot does with it).
    * ``memory.embed_model`` — read only when an embedding profile exists,
      i.e. with an OpenAI key; without one nothing running reads it, and the
      Memory step's own ``openai_api_key`` debt covers the restart that
      builds the profile later.

    Without a running memU, or when the apply fails, the debt stays.
    """
    if "memory.categories" not in added:
        return
    try:
        engine = get_deps().engine
    except RuntimeError:
        return
    bridge = getattr(engine, "memory_bridge", None) if engine is not None else None
    apply = getattr(bridge, "apply_categories", None)
    if not callable(apply):
        return
    config = get_config()
    categories = list(config.memory.categories)
    if not categories:
        return
    try:
        applied = await asyncio.wait_for(apply(categories), _APPLY_CATEGORIES_TIMEOUT)
    except Exception as e:  # noqa: BLE001 — the debt simply stays
        logger.warning("Setup: memory categories could not be applied live (%s); a restart will", type(e).__name__)
        return
    if not applied:
        return
    config_reload.note_applied("memory.categories", categories)
    if "memory.embed_model" in added and not config.openai_api_key:
        config_reload.note_applied("memory.embed_model", config.memory.embed_model)
    logger.info("Setup: %d memory categories applied to the running memU", len(categories))


# --------------------------------------------------------------------------- #
#  PUT /api/setup/provider                                                     #
# --------------------------------------------------------------------------- #

_REGION_RE = re.compile(r"^[a-z]{2}(-[a-z]+)+-\d$")
# An AWS profile name is a plain identifier; it becomes an SDK argument.
_PROFILE_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
# Printable ASCII without spaces: what an API key or OAuth token is made of,
# and the only thing that may travel as an HTTP header value.
_SECRET_CHARS_RE = re.compile(r"^[\x21-\x7e]+$")
_EFFORTS = ("low", "medium", "high", "xhigh", "max")


@dataclass(frozen=True)
class _ProviderFields:
    """The validated, normalised fields of a provider body (PUT and test)."""

    kind: str | None
    key: str | None
    token: str | None
    region: str
    profile: str | None
    effort: str | None
    model: str | None


def _validate_provider_fields(req: ProviderRequest) -> _ProviderFields:
    """Shared by ``PUT /api/setup/provider`` and ``POST …/provider/test``.

    The region and the profile matter beyond the file they land in: the
    Anthropic SDK builds the Bedrock endpoint host from ``aws_region`` and
    passes ``aws_profile`` to boto, so a test with a crafted region would make
    the gateway host connect to an arbitrary host. Both are validated before
    any candidate configuration is built.
    """
    kind = (req.type or "").strip().lower() or None
    if kind is not None and kind not in ("anthropic", "oauth", "host_login", "bedrock"):
        raise SetupFieldError("type", "Choose anthropic, oauth, host_login or bedrock.")
    key = req.anthropic_api_key.strip() if isinstance(req.anthropic_api_key, str) else req.anthropic_api_key
    token = req.claude_oauth_token.strip() if isinstance(req.claude_oauth_token, str) else req.claude_oauth_token
    if key == "":
        key = None
    if token == "":
        token = None
    if isinstance(key, str) and not key.startswith("sk-ant-"):
        raise SetupFieldError(
            "anthropic_api_key",
            "That does not look like an Anthropic API key (they start with sk-ant-).",
        )
    # A pasted key with whitespace, a control or a non-ASCII character would
    # reach h11 as an illegal header value, and its error quotes the header
    # (bytes repr) — refused here so no client ever sees such a key.
    for field_name, value in (("anthropic_api_key", key), ("claude_oauth_token", token)):
        if isinstance(value, str) and not _SECRET_CHARS_RE.match(value):
            raise SetupFieldError(
                field_name,
                "The key contains a space, a line break or a character that is not "
                "plain ASCII; paste it again without them.",
            )
    region = (req.aws_region or "").strip()
    if region and not _REGION_RE.match(region):
        raise SetupFieldError("aws_region", "That does not look like an AWS region (for example eu-central-1).")
    profile = req.aws_profile.strip() if isinstance(req.aws_profile, str) else None
    if profile and not _PROFILE_RE.match(profile):
        raise SetupFieldError(
            "aws_profile",
            "That does not look like an AWS profile name (letters, digits, dots, dashes and underscores).",
        )
    effort = (req.effort or "").strip().lower() or None
    if effort is not None and effort not in _EFFORTS:
        raise SetupFieldError("effort", "Effort must be one of low, medium, high, xhigh, max.")
    model = (req.model or "").strip() or None
    return _ProviderFields(
        kind=kind, key=key, token=token, region=region, profile=profile, effort=effort, model=model,
    )
_BEDROCK_MODEL_PATHS = (
    "agent.model", "agent.cron_model", "agent.title_model",
    "memory.recall_model", "memory.memorize_model", "memory.fast_model",
)


def _model_allowed(model: str, config: NerveConfig, discovered: list[str]) -> bool:
    if model in config.selectable_claude_models(discovered):
        return True
    if model.startswith("claude-"):
        return True
    return bool(re.match(r"^(us|eu|apac|global)\.anthropic\.", model))


@router.put("/api/setup/provider")
async def set_provider(req: ProviderRequest, actor: Actor = Depends(require_person)):
    global _last_provider_test
    fields = _validate_provider_fields(req)
    kind, key, token = fields.kind, fields.key, fields.token
    region, effort, model = fields.region, fields.effort, fields.model
    # An explicit null removes the secret; a blank string is omitted, never
    # written and never taken as a removal.
    remove_key = "anthropic_api_key" in req.model_fields_set and req.anthropic_api_key is None
    remove_token = "claude_oauth_token" in req.model_fields_set and req.claude_oauth_token is None
    if kind == "anthropic" and not key and req.anthropic_api_key is None:
        pass  # a type switch without a new key keeps the stored one
    if kind == "bedrock" and not region and not get_config().provider.aws_region:
        raise SetupFieldError("aws_region", "Bedrock needs an AWS region.")

    # The refusal order (409 unclaimed → 409 read-only) is kept with a
    # lock-free read; then whatever talks to the network or shells out — the
    # model catalogue, the host-login detection — runs *outside* the state
    # lock, so no other person's read or save waits on it.
    pre = await _context(actor)
    _require_claimed(pre)
    _require_writable(pre)
    # An instance that keeps no credentials on disk stores neither a pasted
    # key nor a token; Bedrock (an IAM role, no secret), the model picker and
    # a removal store nothing and go through.
    if key:
        _refuse_without_local_secrets(pre, "anthropic_api_key")
    if token:
        _refuse_without_local_secrets(pre, "claude_oauth_token")
    # A credential the operator provisioned (environment, config repository)
    # is not replaced or removed from here; the step already reads done.
    if key or remove_key or kind == "host_login":
        _refuse_managed(pre, "anthropic_api_key", "anthropic_api_key")
    if token or remove_token or kind == "host_login":
        _refuse_managed(pre, "claude_oauth_token", "claude_oauth_token")
    discovered: list[str] = []
    if model is not None:
        try:
            from nerve import models_catalog

            discovered = list(await models_catalog.get_models(pre.config))
        except Exception:  # noqa: BLE001
            discovered = []
    found = None
    if kind == "host_login":
        refusal = _host_login_refusal(pre.config)
        if refusal:
            raise HTTPException(status_code=409, detail=refusal)
        try:
            found = await setup_checks.host_login()
        except setup_checks.Busy:
            raise HTTPException(status_code=429, detail=_BUSY)
        if not found.available or not found.value:
            raise HTTPException(status_code=409, detail="No Claude Code login was found on this server.")

    async with _loop_lock("state"):
        ctx = await _context(actor)
        _require_claimed(ctx)
        config = ctx.config
        _require_writable(ctx)
        # The policy is read again on the config as it is *now*: a reload may
        # have turned it off while the catalogue or the host login was being
        # looked up outside the lock, and the write below must not outrun it.
        if key:
            _refuse_without_local_secrets(ctx, "anthropic_api_key")
        if token:
            _refuse_without_local_secrets(ctx, "claude_oauth_token")
        if kind == "host_login" and found is not None:
            refusal = _host_login_refusal(config)
            if refusal:
                raise HTTPException(status_code=409, detail=refusal)
        if model is not None and not _model_allowed(model, config, discovered):
            raise SetupFieldError("model", "That model is not in the catalogue for these credentials.")

        choices = choices_from_config(config)
        secret: dict[str, Any] = {}
        machine: dict[str, Any] = {}
        portable: dict[str, Any] = {}
        shadowed: list[str] = []
        answered = False
        was_bedrock = config.provider.is_bedrock

        if kind == "host_login" and found is not None:  # resolved above, outside the lock
            if found.kind == "api_key":
                secret["anthropic_api_key"] = found.value
                secret["claude_oauth_token"] = None
            else:
                secret["claude_oauth_token"] = found.value
                secret["anthropic_api_key"] = None
            answered = True
            kind = "anthropic"
        elif kind == "anthropic":
            if key:
                secret["anthropic_api_key"] = key
                secret["claude_oauth_token"] = None
        elif kind == "oauth":
            if token:
                secret["claude_oauth_token"] = token
                secret["anthropic_api_key"] = None
        if remove_key and "anthropic_api_key" not in secret:
            secret["anthropic_api_key"] = None
        if remove_token and "claude_oauth_token" not in secret:
            secret["claude_oauth_token"] = None

        if kind in ("anthropic", "oauth"):
            choices = replace_choices(choices, provider_type="anthropic", aws_region="")
            portable_paths = ["provider.type"]
            if was_bedrock:
                # Leaving Bedrock: the geo-prefixed ids go back to the bare
                # catalogue (a model the person picks in the same request wins).
                portable_paths += [p for p in _BEDROCK_MODEL_PATHS if p != "agent.title_model"]
                shadowed += ["provider.aws_region", "agent.title_model"]
                if model is None:
                    choices = replace_choices(choices, agent_model="")
                if fields.profile is None:
                    machine["provider.aws_profile"] = None
        elif kind == "bedrock":
            choices = replace_choices(
                choices, provider_type="bedrock",
                aws_region=region or config.provider.aws_region,
                aws_profile=fields.profile or "",
            )
            portable_paths = ["provider.type", "provider.aws_region", *_BEDROCK_MODEL_PATHS]
            if fields.profile is not None:
                machine["provider.aws_profile"] = fields.profile or None
        else:
            portable_paths = []
        if model is not None:
            choices = replace_choices(choices, agent_model=model)
            # The pick is the cron model too (build_config_layers derives it).
            portable_paths += ["agent.model", "agent.cron_model"]
        if effort is not None:
            choices = replace_choices(choices, agent_effort=effort)
            portable_paths.append("agent.effort")
        if kind == "bedrock" and model is None:
            # Switching to Bedrock replaces a bare model id with the
            # geo-prefixed catalogue; a model the person picked stays.
            choices = replace_choices(choices, agent_model="")
        _m, portable_sel, _s = _select(choices, portable_paths=tuple(portable_paths))
        portable.update(portable_sel)
        if kind == "bedrock" and model is None and not config.agent.model.startswith(
            f"{bedrock_geo_prefix(choices.aws_region)}."
        ):
            pass  # the geo-prefixed default from the builders is what lands

        _write(ctx, machine=machine, portable=portable, secret=secret, shadowed=shadowed)
        if secret or kind == "bedrock" or (kind is not None and was_bedrock != (kind == "bedrock")):
            # The remembered test described a candidate credential; the saved
            # one just changed (a key written or removed, a type switch, a
            # Bedrock region/profile), so it would now describe the wrong
            # thing. A model/effort-only change keeps it.
            _last_provider_test = None
        # No cache reset here: the catalog cache is keyed by the credentials'
        # fingerprint (provider, base URL, key tail), so the saved key simply
        # misses an entry fetched without one — and finds the catalog the
        # provider test warmed for it (models_catalog.warm).
        logger.info("Setup: provider step written (type=%s)", kind or "unchanged")
        return await _finish_mutation(ctx, step=STEP_PROVIDER, answered=answered)


# --------------------------------------------------------------------------- #
#  PUT /api/setup/profile                                                      #
# --------------------------------------------------------------------------- #

_HHMM = re.compile(r"^\d{2}:\d{2}$")


def _valid_time(value: str) -> bool:
    if not _HHMM.match(value):
        return False
    hours, minutes = value.split(":")
    return 0 <= int(hours) <= 23 and 0 <= int(minutes) <= 59


_QUIET_PATHS = ("quiet_start", "quiet_end")


def _quiet_change(
    enabled: bool | None, given: dict[str, Any], config: NerveConfig,
) -> tuple[str, str, tuple[str, ...]] | None:
    """``(start, end, paths to write)`` for a profile body, or ``None`` (unchanged).

    ``("", "")`` is "no quiet hours". ``enabled`` false writes that; true
    writes the two times given (the caller has validated both), or — both
    omitted — the window configured now, else the default one. Omitted or
    null leaves quiet hours alone unless a time is given: a body with times
    only enables with them, as it always has, writing the times it names and
    filling the other only where it is empty now, so the pair never ends up
    half set.
    """
    current = (str(config.quiet_start or ""), str(config.quiet_end or ""))
    asked = {key: str(given.get(key) or "").strip() for key in _QUIET_PATHS}
    if enabled is False:
        return "", "", _QUIET_PATHS
    if enabled is True:
        if not any(asked.values()):
            start, end = current if all(current) else DEFAULT_QUIET_WINDOW
            return start, end, _QUIET_PATHS
        return asked["quiet_start"], asked["quiet_end"], _QUIET_PATHS
    if not any(asked.values()):
        return None
    start = asked["quiet_start"] or current[0] or DEFAULT_QUIET_WINDOW[0]
    end = asked["quiet_end"] or current[1] or DEFAULT_QUIET_WINDOW[1]
    paths = tuple(key for key, now in zip(_QUIET_PATHS, current) if asked[key] or not now)
    return start, end, paths


@router.put("/api/setup/profile")
async def set_profile(req: ProfileRequest, actor: Actor = Depends(require_person)):
    given = {k: v for k, v in req.model_dump().items() if k in req.model_fields_set}
    if req.quiet_enabled is False:
        # Quiet hours off: whatever times the form still holds are not used,
        # so they are neither validated nor written.
        for key in _QUIET_PATHS:
            given.pop(key, None)
    name = (req.name or "").strip() if req.name is not None else None
    tz = (req.timezone or "").strip() if req.timezone is not None else None
    if tz is not None:
        if not tz:
            raise SetupFieldError("timezone", "Not a time zone name. Use an IANA name such as Europe/Berlin.")
        try:
            ZoneInfo(tz)
        except (ZoneInfoNotFoundError, ValueError, OSError):
            raise SetupFieldError("timezone", "Not a time zone name. Use an IANA name such as Europe/Berlin.")
    for field_name in _QUIET_PATHS:
        value = given.get(field_name)
        if value is not None and not _valid_time(str(value).strip()):
            raise SetupFieldError(field_name, "Use a 24-hour time such as 22:00.")
    if req.quiet_enabled is True:
        # Switching quiet hours on with one time names the other one; with
        # neither, the configured (or default) window is used.
        stated = [key for key in _QUIET_PATHS if given.get(key) is not None]
        if len(stated) == 1:
            missing = "quiet_end" if stated[0] == "quiet_start" else "quiet_start"
            raise SetupFieldError(missing, "Quiet hours need both a start and an end time.")
    # Every one of these becomes one line of USER.md or IDENTITY.md — files
    # loaded into the agent's system prompt. A line break inside a "display
    # name" would plant arbitrary prompt lines there, so single-line fields
    # refuse control characters and the free-text one is folded to one line.
    about = req.about
    if isinstance(about, str):
        about = re.sub(r"\s+", " ", about).strip()
        _reject_control_chars("about", about)
    for field_name in ("name", "language", "agent_name", "agent_emoji", "timezone", "quiet_start", "quiet_end"):
        value = given.get(field_name)
        if isinstance(value, str):
            _reject_control_chars(field_name, value)
    limits = {"name": 80, "agent_name": 40, "language": 40, "about": 600}
    for field_name, limit in limits.items():
        value = about if field_name == "about" else given.get(field_name)
        if isinstance(value, str) and len(value.strip()) > limit:
            raise SetupFieldError(field_name, f"Keep this under {limit} characters.")
    emoji = (req.agent_emoji or "").strip() if req.agent_emoji is not None else None
    if emoji and len(emoji) > 8:
        raise SetupFieldError("agent_emoji", "One emoji, please.")

    async with _loop_lock("state"):
        ctx = await _context(actor)
        _require_claimed(ctx)
        config = ctx.config
        external = ctx.facts.external_auth
        mode = workspace_mode(config.workspace)
        if ctx.facts.lockdown:
            if external:
                raise HTTPException(status_code=409, detail=LOCKDOWN_TEXT)
            refused = [k for k in given if k != "name"]
            if refused:
                # A read-only state, not a value that could be fixed: 409,
                # with the field so the form can point at it (DESIGN §6.2).
                raise SetupFieldError(refused[0], LOCKDOWN_TEXT, status_code=409)
        elif mode is None:
            raise HTTPException(
                status_code=409,
                detail="Choose a mode first — the files this step fills in depend on it",
            )
        # An explicit null on a settings value is not a removal (the time
        # zone always has a value; quiet hours are switched off with
        # ``quiet_enabled: false``); it is treated as omitted, so it neither
        # writes nor counts as an answer.
        quiet = _quiet_change(req.quiet_enabled, given, config)
        overrides: dict[str, Any] = {}
        portable_paths: list[str] = []
        if tz:
            overrides["timezone"] = tz
            portable_paths.append("timezone")
        if quiet is not None:
            quiet_start, quiet_end, quiet_paths = quiet
            overrides.update(
                quiet_enabled=bool(quiet_start and quiet_end),
                quiet_start=quiet_start, quiet_end=quiet_end,
            )
            portable_paths.extend(quiet_paths)
        config_writes = bool(portable_paths)

        if config_writes:
            choices = replace_choices(choices_from_config(config), **overrides)
            _m, portable, _s = _select(choices, portable_paths=tuple(portable_paths))
            _write(ctx, portable=portable)

        if mode == "personal" and not ctx.facts.lockdown:
            try:
                apply_profile_templates(
                    Path(config.workspace),
                    name=name or None,
                    timezone=tz or None,
                    language=(req.language or "").strip() or None if req.language is not None else None,
                    # ("", "") writes "Quiet hours: none" while the line is pristine.
                    quiet=(quiet[0], quiet[1]) if quiet is not None else None,
                    agent_name=(req.agent_name or "").strip() or None if req.agent_name is not None else None,
                    agent_emoji=emoji or None,
                    about=about or None,
                )
            except OSError as e:
                logger.warning("Setup: workspace files under %s could not be written: %s", config.workspace, e)
                raise HTTPException(
                    status_code=500,
                    detail="Settings were saved; the workspace files could not be written: "
                           f"{_public_error(e, config)}",
                ) from e
            except ValueError as e:
                # The writer's own guard against a line break in a replacement
                # (defence in depth; the validation above should have refused it).
                raise SetupFieldError("name", str(e)) from e

        renamed = actor
        if name is not None and not external and actor.account_id:
            try:
                await get_deps().db.update_actor_profile(actor.actor_id, display_name=name or None)
                renamed = dataclasses.replace(actor, display_name=name or None)
            except Exception as e:  # noqa: BLE001
                logger.exception("Setup: the display name could not be written")
                raise HTTPException(
                    status_code=500,
                    detail=f"{'The settings were saved. ' if config_writes else 'Nothing was written. '}"
                           f"The display name could not be saved: {_public_error(e, config)}",
                ) from e

        answered = bool(config_writes) or (ctx.facts.lockdown and not external and name is not None)
        ctx_renamed = _Context(facts=ctx.facts, actor=renamed)
        return await _finish_mutation(ctx_renamed, step=STEP_PROFILE, answered=answered)


# --------------------------------------------------------------------------- #
#  PUT /api/setup/memory                                                       #
# --------------------------------------------------------------------------- #


# Under memory.backend: loom the Memory step has nothing to store.
_LOOM_MEMORY_REFUSAL = "Memory is kept in Loom on this instance: there is nothing to set up here."


def _refuse_loom_memory(ctx: Any) -> None:
    if ctx.config.memory.backend == "loom":
        raise HTTPException(status_code=409, detail=_LOOM_MEMORY_REFUSAL)


@router.put("/api/setup/memory")
async def set_memory(req: MemoryRequest, actor: Actor = Depends(require_person)):
    key_given = "openai_api_key" in req.model_fields_set
    key = (req.openai_api_key or "").strip() if req.openai_api_key is not None else None
    if key == "":
        key_given = False
        key = None
    if key is not None and not key.startswith("sk-"):
        raise SetupFieldError("openai_api_key", "That does not look like an OpenAI API key (they start with sk-).")
    async with _loop_lock("state"):
        ctx = await _context(actor)
        _require_claimed(ctx)
        _require_writable(ctx)
        _refuse_loom_memory(ctx)
        # The step exists to store an OpenAI key. Without local secrets it is
        # closed outright — "no embeddings" is what the instance does anyway.
        _refuse_without_local_secrets(ctx)
        before = ctx.config.openai_api_key
        debts: dict[str, str] = {}
        if key_given:
            _refuse_managed(ctx, "openai_api_key", "openai_api_key")
            _write(ctx, secret={"openai_api_key": key})
            if (key or "") != (before or ""):
                debts["openai_api_key"] = "embeddings start after a restart"
        answered = bool(key) or req.embeddings is False
        return await _finish_mutation(ctx, step=STEP_MEMORY, answered=answered, debts=debts)


# --------------------------------------------------------------------------- #
#  PUT /api/setup/channels                                                     #
# --------------------------------------------------------------------------- #

_TG_TOKEN_RE = re.compile(r"^\d{6,12}:[A-Za-z0-9_-]{30,}$")
_TG_UNREACHABLE = (
    "The server could not reach api.telegram.org to verify the token; nothing "
    "was saved. Try again."
)
_TG_REJECTED_HINT = (
    "In Telegram open @BotFather → /mybots → API Token and paste it again; a "
    "token that was revoked or re-issued answers Unauthorized."
)


def _remember_bot_username(token: str, username: str, state: SetupState) -> None:
    _bot_usernames[_token_key(token)] = username
    state.telegram_bot_username = username


@dataclass(frozen=True)
class _TelegramPlan:
    """What a Telegram patch does against one reading of the config."""

    removing: bool
    enabled_after: bool
    token_after: str
    #: ``getMe`` must pass before anything is written (C32): the channel
    #: would be enabled with a token it has not run with — a new token, or a
    #: stored one being switched on.
    verify: bool


def _telegram_plan(config: NerveConfig, tg: TelegramPatch, tg_token: str | None, tg_token_given: bool) -> _TelegramPlan:
    if tg_token_given and tg.bot_token is None:
        return _TelegramPlan(removing=True, enabled_after=False, token_after="", verify=False)
    if tg.enabled is not None:
        enabled_after = bool(tg.enabled)
    elif tg_token is not None:
        enabled_after = True
    else:
        enabled_after = bool(config.telegram.enabled)
    token_after = tg_token or (config.telegram.bot_token or "")
    if enabled_after and not token_after:
        raise HTTPException(status_code=409, detail="Add a bot token first.")
    turning_on = enabled_after and not bool(config.telegram.enabled)
    return _TelegramPlan(
        removing=False, enabled_after=enabled_after, token_after=token_after,
        verify=bool(enabled_after and (tg_token is not None or turning_on)),
    )


async def _verify_telegram(token: str) -> str:
    """``getMe`` for a token about to be enabled; the bot's username, or a 4xx."""
    try:
        ok, detail = await setup_checks.telegram_get_me(token)
    except setup_checks.Busy:
        raise HTTPException(status_code=429, detail=_BUSY)
    if not ok:
        low = detail.lower()
        if detail == "timeout" or "connect" in low or "resolve" in low or "network" in low:
            raise SetupFieldError("bot_token", _TG_UNREACHABLE)
        raise SetupFieldError("bot_token", f"Telegram rejected this token: {detail}. {_TG_REJECTED_HINT}")
    return detail


@dataclass(frozen=True)
class _SlackPlan:
    hosted: bool
    bot_after: str
    app_after: str
    enabled_after: bool
    verify: bool


def _slack_plan(config: NerveConfig, slack: SlackPatch, slack_bot: str | None, slack_app: str | None) -> _SlackPlan:
    if config.slack.mode == "hosted":
        if slack_bot is not None or slack_app is not None or (
            "bot_token" in slack.model_fields_set or "app_token" in slack.model_fields_set
        ):
            raise HTTPException(status_code=409, detail="Slack is managed by the channel gateway on this instance.")
        return _SlackPlan(
            hosted=True, bot_after="", app_after="",
            enabled_after=bool(slack.enabled) if slack.enabled is not None else bool(config.slack.enabled),
            verify=False,
        )
    bot_after = slack_bot if slack_bot is not None else (
        "" if ("bot_token" in slack.model_fields_set and slack.bot_token is None) else config.slack.bot_token
    )
    app_after = slack_app if slack_app is not None else (
        "" if ("app_token" in slack.model_fields_set and slack.app_token is None) else config.slack.app_token
    )
    if slack.enabled is not None:
        enabled_after = bool(slack.enabled)
    elif slack_bot is not None or slack_app is not None:
        enabled_after = bool(bot_after and app_after)
    else:
        enabled_after = bool(config.slack.enabled)
    if enabled_after and not (bot_after and app_after):
        raise HTTPException(status_code=409, detail="Slack needs both a bot token and an app token.")
    turning_on = enabled_after and not bool(config.slack.enabled)
    return _SlackPlan(
        hosted=False, bot_after=bot_after or "", app_after=app_after or "", enabled_after=enabled_after,
        verify=bool(enabled_after and (slack_bot is not None or slack_app is not None or turning_on)),
    )


async def _verify_slack(bot_token: str, app_token: str) -> None:
    try:
        result = await setup_checks.slack_test(bot_token, app_token)
    except setup_checks.Busy:
        raise HTTPException(status_code=429, detail=_BUSY)
    if not result.get("ok"):
        raise SetupFieldError(result.get("field") or "bot_token", result.get("detail") or "Slack rejected the tokens.")


def _hosted_switch_only(config: NerveConfig, tg: TelegramPatch | None, slack: SlackPatch | None) -> bool:
    """The one channels write an instance without local secrets accepts: the
    on/off switch of a Slack the channel gateway holds (no token involved)."""
    return (
        tg is None and slack is not None and config.slack.mode == "hosted"
        and slack.model_fields_set <= {"enabled"}
    )


@router.put("/api/setup/channels")
async def set_channels(req: ChannelsRequest, actor: Actor = Depends(require_person)):
    tg = req.telegram
    slack = req.slack
    tg_token_given = tg is not None and "bot_token" in tg.model_fields_set
    tg_token = (tg.bot_token or "").strip() if (tg is not None and tg.bot_token is not None) else None
    if tg_token == "":
        tg_token_given = False
        tg_token = None
    if tg_token is not None and not _TG_TOKEN_RE.match(tg_token):
        raise SetupFieldError("bot_token", "That does not look like a bot token from @BotFather.")
    if tg is not None and tg.allowed_users is not None and tg.allowed_users_add is not None:
        raise SetupFieldError("allowed_users", "Send allowed_users or allowed_users_add, not both.")
    slack_bot = (slack.bot_token or "").strip() if (slack is not None and slack.bot_token is not None) else None
    slack_app = (slack.app_token or "").strip() if (slack is not None and slack.app_token is not None) else None
    if slack_bot == "":
        slack_bot = None
    if slack_app == "":
        slack_app = None
    if slack_bot is not None and not slack_bot.startswith("xoxb-"):
        raise SetupFieldError("bot_token", "Slack bot tokens start with xoxb-.")
    if slack_app is not None and not slack_app.startswith("xapp-"):
        raise SetupFieldError("app_token", "Slack app tokens start with xapp-.")
    if tg is None and slack is None:
        raise HTTPException(status_code=400, detail="Give a telegram or slack object.")

    # The live verification (C32) runs *before* the state lock is taken: a
    # getMe or auth_test takes up to 10–20 s, and every other person's
    # GET /api/setup, save and the restart endpoint would otherwise wait on
    # it — one person posting unverifiable tokens could keep the whole page
    # busy. The refusal order (409 unclaimed → 409 read-only → 400) is kept
    # with a lock-free read first; inside the lock the decision is recomputed
    # against the config as it is *then*, and a token that was not the one
    # verified (the stored token changed meanwhile) is verified again.
    pre = await _context(actor)
    _require_claimed(pre)
    _require_writable(pre)
    # Without local secrets the step stores nothing — except the switch of a
    # Slack the channel gateway holds, which involves no token.
    if not _hosted_switch_only(pre.config, tg, slack):
        _refuse_without_local_secrets(pre)
    # Tokens the operator provisioned are not replaced or removed from here
    # (the gateway's Slack tokens never were — see _slack_plan).
    if tg_token_given:
        _refuse_managed(pre, "telegram.bot_token", "bot_token")
    if slack is not None and pre.config.slack.mode != "hosted":
        if "bot_token" in slack.model_fields_set and slack.bot_token != "":
            _refuse_managed(pre, "slack.bot_token", "bot_token")
        if "app_token" in slack.model_fields_set and slack.app_token != "":
            _refuse_managed(pre, "slack.app_token", "app_token")
    verified_tg: dict[str, str] = {}
    if tg is not None:
        plan = _telegram_plan(pre.config, tg, tg_token, tg_token_given)
        if plan.verify:
            verified_tg[plan.token_after] = await _verify_telegram(plan.token_after)
    verified_slack: set[tuple[str, str]] = set()
    if slack is not None:
        splan = _slack_plan(pre.config, slack, slack_bot, slack_app)
        if splan.verify:
            await _verify_slack(splan.bot_after, splan.app_after)
            verified_slack.add((splan.bot_after, splan.app_after))

    async with _loop_lock("state"):
        ctx = await _context(actor)
        _require_claimed(ctx)
        config = ctx.config
        _require_writable(ctx)
        # Read again on the config as it is now: a reload during the probe
        # above may have turned the policy on, and the token must not be
        # written on the strength of a check that predates it.
        if not _hosted_switch_only(config, tg, slack):
            _refuse_without_local_secrets(ctx)
        secret: dict[str, Any] = {}
        machine: dict[str, Any] = {}
        portable: dict[str, Any] = {}
        defaults: dict[str, Any] = {}

        if tg is not None:
            plan = _telegram_plan(config, tg, tg_token, tg_token_given)
            if plan.removing:
                secret["telegram.bot_token"] = None
                machine["telegram.enabled"] = False
                ctx.state.telegram_bot_username = None
            else:
                if plan.verify:
                    username = verified_tg.get(plan.token_after)
                    if username is None:
                        username = await _verify_telegram(plan.token_after)
                    _remember_bot_username(plan.token_after, username, ctx.state)
                elif tg_token is not None:
                    # Saved with the channel off: no getMe ran. A name a test
                    # of this exact token produced is kept; a stale one from
                    # an earlier token is not.
                    ctx.state.telegram_bot_username = _bot_usernames.get(_token_key(tg_token))
                if tg_token is not None:
                    secret["telegram.bot_token"] = tg_token
                if tg.enabled is not None or tg_token is not None:
                    machine["telegram.enabled"] = plan.enabled_after
                if tg.allowed_users is not None:
                    secret["telegram.allowed_users"] = sorted({int(u) for u in tg.allowed_users})
                elif tg.allowed_users_add:
                    merged = sorted({*(int(u) for u in config.telegram.allowed_users), *(int(u) for u in tg.allowed_users_add)})
                    secret["telegram.allowed_users"] = merged
            defaults["telegram.dm_policy"] = "pairing"
            defaults["telegram.stream_mode"] = "partial"

        if slack is not None:
            splan = _slack_plan(config, slack, slack_bot, slack_app)
            if splan.hosted:
                if slack.enabled is not None:
                    machine["slack.enabled"] = bool(slack.enabled)
            else:
                if splan.verify and (splan.bot_after, splan.app_after) not in verified_slack:
                    await _verify_slack(splan.bot_after, splan.app_after)
                if "bot_token" in slack.model_fields_set:
                    secret["slack.bot_token"] = slack_bot if slack_bot is not None else None
                if "app_token" in slack.model_fields_set:
                    secret["slack.app_token"] = slack_app if slack_app is not None else None
                if slack.enabled is not None or slack_bot is not None or slack_app is not None:
                    machine["slack.enabled"] = splan.enabled_after
                if slack.allow_users is not None:
                    portable["slack.allow_users"] = [u.strip() for u in slack.allow_users if u.strip()]
                if slack.allow_direct_messages is not None:
                    portable["slack.allow_direct_messages"] = bool(slack.allow_direct_messages)
                if slack.allow_channels is not None:
                    portable["slack.allow_channels"] = [c.strip() for c in slack.allow_channels if c.strip()]

        _write(ctx, machine=machine, portable=portable, secret=secret)
        if defaults and not ctx.facts.lockdown:
            merge_settings_defaults(Path(config.workspace), defaults)
        logger.info("Setup: channels step written (telegram=%s, slack=%s)", tg is not None, slack is not None)
        return await _finish_mutation(ctx, step=STEP_CHANNELS)


# --------------------------------------------------------------------------- #
#  PUT /api/setup/sources                                                      #
# --------------------------------------------------------------------------- #

_EMAIL_RE = re.compile(r"^[^\s@-][^\s@]*@[^\s@]+\.[^\s@]+$")

#: The source each field of a ``SourcesRequest`` belongs to.
_SOURCE_OF_FIELD = {
    "github": "github",
    "gmail": "gmail",
    "gmail_accounts": "gmail",
    "telegram": "telegram",
    "telegram_api_id": "telegram",
    "telegram_api_hash": "telegram",
}


def _refuse_closed_sources(ctx: _Context, req: SourcesRequest) -> list[str]:
    """The sources the page may switch here, after refusing the request for
    any other. Without local secrets every source keeps a credential on this
    machine — a GitHub login or token, the Gmail keyring, the Telegram API
    credentials — so nothing goes through (409, the policy's two sentences),
    except GitHub when the channel gateway polls it or its token comes from
    the environment: that switch stores nothing, and the Gmail and Telegram
    fields alone are refused, by name, with the step's reason. A GitHub switch
    the operator holds is never on offer (the handler refuses it first)."""
    available, unavailable_reason = sources_available(ctx.facts)
    if not available:
        _refuse_without_local_secrets(ctx)
    for name in SourcesRequest.model_fields:
        if name in req.model_fields_set and _SOURCE_OF_FIELD.get(name, name) not in available:
            raise SetupFieldError(name, unavailable_reason or NO_LOCAL_SECRETS_TEXT, status_code=409)
    return available


@router.put("/api/setup/sources")
async def set_sources(req: SourcesRequest, actor: Actor = Depends(require_person)):
    async with _loop_lock("state"):
        ctx = await _context(actor)
        _require_claimed(ctx)
        config = ctx.config
        if workspace_mode(config.workspace) == "worker":
            raise HTTPException(status_code=409, detail="Sources do not apply to a worker instance.")
        _require_writable(ctx)
        # The GitHub switch while the operator holds it in a machine layer
        # (hosted, ``sync.github.enabled`` in config.yaml or config.local.yaml):
        # this page writes settings.yaml, which that layer outranks, so the
        # save would change nothing the loader reads. A request that names the
        # field is refused whole, by name, as one that names a credential the
        # operator provides is: nothing it carries is applied, so no save lands
        # halfway, and a request without it still writes the other sources.
        # Ahead of the policy, which would answer without naming the field.
        if "github" in req.model_fields_set:
            _refuse_managed(ctx, GITHUB_SWITCH, "github")
        # The policy first, ranked above the wait for a mode as the checklist
        # ranks it: choosing a mode would not open a closed step, so that is
        # not the answer to give.
        available = _refuse_closed_sources(ctx, req)
        if workspace_mode(config.workspace) is None:
            raise HTTPException(status_code=409, detail="Choose a mode first")
        live = config.sync
        accounts = (
            [a.strip() for a in req.gmail_accounts if a.strip()]
            if req.gmail_accounts is not None else list(live.gmail.accounts)
        )
        for address in accounts:
            if not _EMAIL_RE.match(address):
                raise SetupFieldError("gmail_accounts", "That is not an email address.")
        gmail_on = live.gmail.enabled if req.gmail is None else bool(req.gmail)
        # A source the page may not switch here is the operator's to keep
        # consistent: its live configuration is not this request's to fail.
        if "gmail" in available and gmail_on and not accounts:
            raise SetupFieldError("gmail_accounts", "Add at least one Gmail address.")
        tg_on = live.telegram.enabled if req.telegram is None else bool(req.telegram)
        # The two sync credentials follow the secret rules: omitted = kept,
        # blank = omitted, null = removed — independently of each other.
        id_given = "telegram_api_id" in req.model_fields_set
        hash_given = "telegram_api_hash" in req.model_fields_set
        remove_id = id_given and req.telegram_api_id is None
        new_hash = req.telegram_api_hash.strip() if isinstance(req.telegram_api_hash, str) else None
        remove_hash = hash_given and req.telegram_api_hash is None
        if new_hash == "":
            new_hash, hash_given = None, False
        if new_hash is not None:
            _reject_control_chars("telegram_api_hash", new_hash)
        api_id = 0 if remove_id else (
            req.telegram_api_id if req.telegram_api_id is not None else int(live.telegram.api_id or 0)
        )
        api_hash = "" if remove_hash else (
            new_hash if new_hash is not None else (live.telegram.api_hash or "")
        )
        # Credentials and addresses the operator provisioned stay as they are.
        if id_given:
            _refuse_managed(ctx, "sync.telegram.api_id", "telegram_api_id")
        if hash_given or remove_hash:
            _refuse_managed(ctx, "sync.telegram.api_hash", "telegram_api_hash")
        if req.gmail_accounts is not None:
            _refuse_managed(ctx, "sync.gmail.accounts", "gmail_accounts")
        if req.telegram_api_id is not None and req.telegram_api_id <= 0:
            raise SetupFieldError("telegram_api_id", "The API id is a positive number from my.telegram.org.")
        if "telegram" in available and api_hash and not api_id:
            raise SetupFieldError(
                "telegram_api_id",
                "Add the API id that goes with this API hash (both come from my.telegram.org).",
            )
        if "telegram" in available and tg_on and not (api_id and api_hash):
            raise SetupFieldError("telegram_api_hash", "Telegram sync needs the API id and API hash from my.telegram.org.")

        choices = replace_choices(
            choices_from_config(config),
            github_sync=live.github.enabled if req.github is None else bool(req.github),
            gmail_sync=gmail_on, gmail_accounts=accounts, telegram_sync=tg_on,
            telegram_api_id=int(api_id or 0), telegram_api_hash=api_hash,
        )
        portable_paths: list[str] = []
        if req.github is not None:
            # The events source goes with the switch. It reads with the gh
            # CLI on this machine, which a GitHub that the channel gateway
            # polls does not set up, so with hosted GitHub the writer keeps it
            # off in both directions.
            portable_paths.extend(("sync.github.enabled", "sync.github_events.enabled"))
        if req.gmail is not None:
            portable_paths.append("sync.gmail.enabled")
        if req.telegram is not None:
            portable_paths.append("sync.telegram.enabled")
        machine_paths = ("sync.gmail.accounts",) if req.gmail_accounts is not None else ()
        secret_paths: list[str] = []
        if id_given:
            secret_paths.append("sync.telegram.api_id")
        if hash_given:
            secret_paths.append("sync.telegram.api_hash")
        machine, portable, secret = _select(
            choices, machine_paths=machine_paths, portable_paths=tuple(portable_paths),
            secret_paths=tuple(secret_paths),
        )
        # A removed credential is said so to the merge (``None`` deletes the
        # path) instead of being silently dropped — or, for the hash beside a
        # kept id, re-emitted by the builders as an empty string.
        if remove_id:
            secret["sync.telegram.api_id"] = None
        if remove_hash:
            secret["sync.telegram.api_hash"] = None
        for path in secret_paths:
            secret.setdefault(path, None)
        _write(ctx, machine=machine, portable=portable, secret=secret)
        # A decision about every source on offer — all three, or GitHub alone
        # where it is the one the page may switch — is an answer, "none of
        # these" included.
        answered = all(getattr(req, source) is not None for source in available)
        return await _finish_mutation(ctx, step=STEP_SOURCES, answered=answered)


# --------------------------------------------------------------------------- #
#  PUT /api/setup/automation                                                   #
# --------------------------------------------------------------------------- #


@router.put("/api/setup/automation")
async def set_automation(req: AutomationRequest, actor: Actor = Depends(require_person)):
    wanted = {c.strip() for c in req.crons if c.strip()}
    async with _loop_lock("state"):
        ctx = await _context(actor)
        _require_claimed(ctx)
        _require_writable(ctx)
        ws = Path(ctx.config.workspace)
        present = {t.id for t in list_optional_crons(ws)}
        unknown = sorted(wanted - present)
        if unknown:
            raise SetupFieldError("crons", f"Unknown cron job: {', '.join(unknown)}.")
        plan = plan_optional_crons(ws, wanted)
        if not plan.ok:
            if plan.problem != "missing":
                logger.warning("Setup: %s could not be read (%s)", plan.path, plan.problem)
            raise HTTPException(
                status_code=409,
                detail=f"{_DISPLAY_CRON} could not be read ({_hide_paths(plan.problem, ctx.config)}); "
                       "nothing was changed."
                if plan.problem != "missing" else
                "Choose a mode first — the job catalogue is created then",
            )
        try:
            plan.publish()
        except OSError as e:
            logger.warning("Setup: %s could not be written: %s", plan.path, e)
            raise HTTPException(
                status_code=500,
                detail=f"The cron file could not be written: {_public_error(e, ctx.config)}",
            ) from e
        logger.info("Setup: automation set (crons on: %s)", ", ".join(sorted(plan.enabled)) or "none")
        return await _finish_mutation(ctx, step=STEP_AUTOMATION, answered=True, clear_debts=("cron",))


# --------------------------------------------------------------------------- #
#  PUT /api/setup/agents                                                       #
# --------------------------------------------------------------------------- #


@router.put("/api/setup/agents")
async def set_agents(req: AgentsRequest, actor: Actor = Depends(require_person)):
    from nerve.external_agents.registry import AGENT_REGISTRY

    names = [n.strip() for n in req.agents if n.strip()]
    unknown = [n for n in names if n not in AGENT_REGISTRY]
    if unknown:
        raise SetupFieldError("agents", f"Unknown agent: {', '.join(unknown)}.")
    policy = (req.conflict_policy or "backup").strip()
    if policy not in ("backup", "skip", "merge"):
        raise SetupFieldError("conflict_policy", "Conflict policy must be backup, skip or merge.")
    async with _loop_lock("state"):
        ctx = await _context(actor)
        _require_claimed(ctx)
        config = ctx.config
        if workspace_mode(config.workspace) == "worker":
            raise HTTPException(status_code=409, detail="External agents do not apply to a worker instance.")
        config_dir = _require_writable(ctx)
        # Connecting an agent writes an MCP token into its config file.
        _refuse_without_local_secrets(ctx)
        failures: list[str] = []
        written: list[str] = []
        if names:
            from nerve.external_agents.writer import ConfigWriter

            secret = effective_jwt_secret(config)
            token = create_external_mcp_token(secret) if secret else "dev-mode-no-auth"
            url = mcp_url(config)
            writer = ConfigWriter(conflict_policy=policy)
            for name in names:
                agent = AGENT_REGISTRY[name]
                try:
                    await asyncio.wait_for(
                        agent.write_config(
                            nerve_url=url, mcp_token=token,
                            workspace=Path(config.workspace), writer=writer,
                        ),
                        10,
                    )
                    written.append(name)
                except Exception as e:  # noqa: BLE001 — reported per agent
                    logger.warning("Setup: agent %s could not be configured: %s: %s", name, type(e).__name__, e)
                    failures.append(f"{name}: {type(e).__name__}: {_public_error(e, config)[:120]}")
            if written:
                try:
                    from nerve.setup_writer import merge_external_agents

                    merge_external_agents(config_dir, targets=written, conflict_policy=policy)
                except (OSError, ValueError) as e:
                    logger.warning("Setup: %s could not be updated: %s", config_dir / "config.yaml", e)
                    raise HTTPException(
                        status_code=500,
                        detail="Agent files were written; config.yaml could not be updated: "
                               f"{_public_error(e, config)}",
                    ) from e
        elif config.external_agents.targets:
            merge_machine_paths(config_dir / "config.yaml", {"external_agents.targets": []}, header=CONFIG_YAML_HEADER)
        state = await _finish_mutation(ctx, step=STEP_AGENTS, answered=True)
        if failures:
            extra = "Some agents could not be configured: " + "; ".join(failures)
            state["warning"] = (state.get("warning") + " " + extra) if state.get("warning") else extra
        return state


# --------------------------------------------------------------------------- #
#  Skip / unskip                                                               #
# --------------------------------------------------------------------------- #


def _check_skippable(step_id: str) -> None:
    if step_id not in SKIPPABLE_STEPS:
        raise HTTPException(
            status_code=400,
            detail=f"{step_id!r} cannot be skipped." if step_id in STEP_IDS
            else f"Unknown setup step: {step_id}",
        )


def _step_here(ctx: _Context, step_id: str) -> Step:
    """The step as it stands on *this* instance; 400 when the step is not in
    this instance's list (a worker has no Sources or Agents step), so the
    state file only ever records decisions about steps that exist."""
    offered = {
        item[0] for item in steps_for_mode(
            workspace_mode(Path(ctx.config.workspace)), external_auth=ctx.facts.external_auth,
        )
    }
    if step_id not in offered:
        raise HTTPException(status_code=400, detail=f"The {step_id} step does not apply to this instance.")
    return next(s for s in derive(ctx.facts) if s.id == step_id)


@router.post("/api/setup/steps/{step_id}/skip")
async def skip_step(step_id: str, actor: Actor = Depends(require_person)):
    _check_skippable(step_id)
    async with _loop_lock("state"):
        ctx = await _context(actor)
        _require_claimed(ctx)
        here = _step_here(ctx, step_id)
        if here.status == "done":
            # A fact already made the step done; "skip" means nothing here and
            # is not recorded (`done` beats `skipped` anyway). Idempotent 200.
            return _render(ctx)
        if here.blocked_kind == BLOCKED_POLICY:
            # Closed by the instance, not waiting on anyone's decision: answered
            # like every other write the policy refuses, and nothing is recorded
            # about a step the page says is not available.
            raise HTTPException(status_code=409, detail=here.blocked_reason or NO_LOCAL_SECRETS_TEXT)
        ctx.state.skipped.add(step_id)
        _save_or_refuse(ctx.state)
        return _render(await _context(actor))


@router.post("/api/setup/steps/{step_id}/unskip")
async def unskip_step(step_id: str, actor: Actor = Depends(require_person)):
    _check_skippable(step_id)
    async with _loop_lock("state"):
        ctx = await _context(actor)
        _require_claimed(ctx)
        _step_here(ctx, step_id)
        ctx.state.skipped.discard(step_id)
        _save_or_refuse(ctx.state)
        return _render(await _context(actor))


# --------------------------------------------------------------------------- #
#  Tests and preflights                                                        #
# --------------------------------------------------------------------------- #


async def _claimed_context(actor: Actor) -> _Context:
    ctx = await _context(actor)
    _require_claimed(ctx)
    return ctx


@router.post("/api/setup/provider/test")
async def provider_test(req: ProviderRequest, actor: Actor = Depends(require_person)):
    global _last_provider_test
    # Validated exactly as the PUT validates (region and profile included)
    # before any candidate configuration exists — see _validate_provider_fields.
    fields = _validate_provider_fields(req)
    ctx = await _claimed_context(actor)
    config = ctx.config
    overrides: dict[str, Any] = {}
    kind = fields.kind or ""
    if kind == "bedrock" or (not kind and config.provider.is_bedrock and req.anthropic_api_key is None):
        overrides["type"] = "bedrock"
        if fields.region:
            overrides["aws_region"] = fields.region
        if fields.profile is not None:
            overrides["aws_profile"] = fields.profile
    elif kind in ("anthropic", "oauth", "host_login") or fields.key or fields.token:
        overrides["type"] = "anthropic"
    if fields.key:
        overrides["anthropic_api_key"] = fields.key
    if fields.token:
        overrides["claude_oauth_token"] = fields.token
        overrides.setdefault("anthropic_api_key", "")
    if kind == "oauth":
        overrides["anthropic_api_key"] = ""
    if kind == "host_login":
        refusal = _host_login_refusal(config)
        if refusal:
            raise HTTPException(status_code=409, detail=refusal)
        try:
            found = await setup_checks.host_login()
        except setup_checks.Busy:
            raise HTTPException(status_code=429, detail=_BUSY)
        if not found.available or not found.value:
            raise HTTPException(status_code=409, detail="No Claude Code login was found on this server.")
        if found.kind == "api_key":
            overrides["anthropic_api_key"] = found.value
        else:
            overrides["claude_oauth_token"] = found.value
            overrides["anthropic_api_key"] = ""
    candidate = setup_checks.candidate_config(config, **overrides)
    try:
        result = await setup_checks.provider_test(candidate)
    except setup_checks.Busy:
        raise HTTPException(status_code=429, detail=_BUSY)
    if result.get("ok") and result.get("checked") == "api" and not candidate.provider.is_bedrock \
            and candidate.resolve_anthropic_api_key():
        # The key works: fetch the catalog for it now (in the background,
        # keyed by this candidate's fingerprint) so the picker is complete the
        # moment the key is saved instead of waiting on a cold fetch then.
        try:
            from nerve import models_catalog

            models_catalog.schedule_warm(candidate)
        except Exception:  # noqa: BLE001
            pass
    # Remembered either way — a failure too, so the Review and Ready screens
    # can say "the last connection test failed" for a key the server could
    # not use. Data only (`last_test` is outside the etag; it never remounts a
    # form), scrubbed once more of every candidate and stored secret even
    # though the probe already scrubs its own text.
    _last_provider_test = {
        "ok": bool(result.get("ok")),
        "code": result.get("code"),
        "detail": setup_checks.scrub_secrets(
            str(result.get("detail") or ""),
            fields.key, fields.token, config.anthropic_api_key, config.claude_oauth_token,
            overrides.get("anthropic_api_key"), overrides.get("claude_oauth_token"),
        ) or None,
        "checked_at": _now(),
    }
    return result


@router.get("/api/setup/provider/host-login")
async def provider_host_login(refresh: int = 0, actor: Actor = Depends(require_person)):
    ctx = await _claimed_context(actor)
    refusal = _host_login_refusal(ctx.config)
    if refusal:
        # Not looked up at all: the answer would reveal that the operator's
        # personal login exists and where, to anyone the gateway signs in.
        return {
            "available": False, "kind": None, "source": None,
            "reason": refusal, "lookup": setup_checks.LOOKUP_NONE,
        }
    try:
        found = await setup_checks.host_login(refresh=bool(refresh))
    except setup_checks.Busy:
        raise HTTPException(status_code=429, detail=_BUSY)
    return found.public()


def _family(model_id: str) -> str:
    """``fable`` | ``opus`` | ``sonnet`` | ``haiku`` | ``other`` — fable first,
    so a Fable id is never filed under another family it happens to mention."""
    low = model_id.lower()
    for family in ("fable", "opus", "sonnet", "haiku"):
        if family in low:
            return family
    return "other"


def _label(model_id: str) -> str:
    """A picker label from a model id: ``Claude <Family> <version>``. A date
    suffix is dropped, a Bedrock geo prefix and ``-v1:0`` too, and consecutive
    numeric segments form one dotted version (``…-5-5`` → ``5.5``)."""
    base = model_id.split("anthropic.")[-1]
    parts = base.replace("-v1:0", "").split("-")
    words: list[str] = []
    version: list[str] = []
    for part in parts:
        if part.isdigit() and len(part) >= 8:
            continue  # a date stamp, not a version
        if part.isdigit():
            version.append(part)
            continue
        if version:
            words.append(".".join(version))
            version = []
        words.append(part.capitalize() if part.isalpha() else part)
    if version:
        words.append(".".join(version))
    return " ".join(words) or model_id


@router.get("/api/setup/models")
async def setup_models(actor: Actor = Depends(require_person)):
    ctx = await _claimed_context(actor)
    config = ctx.config
    discovered: list[str] = []
    try:
        from nerve import models_catalog

        discovered = list(await models_catalog.get_models(config))
    except Exception:  # noqa: BLE001
        discovered = []
    # The configured model first, then the catalog as the API orders it
    # (newest first), or the built-in list when nothing was discovered.
    models = config.selectable_claude_models(discovered)
    return {
        "models": [
            {"id": m, "label": _label(m), "family": _family(m), "recommended": m == config.agent.model}
            for m in models
        ],
        "current": {"model": config.agent.model, "effort": config.agent.effort},
        "efforts": ["max", "xhigh", "high", "medium", "low"],
        "discovered": bool(discovered) and not config.provider.is_bedrock,
        # Nerve's own default, so the page can say which entry it is even
        # when this instance was changed to another model.
        "default_model": DEFAULT_MODEL,
    }


@router.post("/api/setup/memory/test")
async def memory_test(req: MemoryTestRequest, actor: Actor = Depends(require_person)):
    ctx = await _claimed_context(actor)
    _refuse_loom_memory(ctx)
    # Refused with the step it belongs to, unlike the provider test: a Bedrock
    # or environment credential is still worth testing there, whereas the only
    # thing to do with a working OpenAI key here is to save it, which is refused.
    _refuse_without_local_secrets(ctx)
    key = (req.openai_api_key or "").strip() or ctx.config.openai_api_key
    if not key:
        return {"ok": False, "detail": "No OpenAI key to test.", "hint": "Enter a key first."}
    try:
        return await setup_checks.openai_test(key)
    except setup_checks.Busy:
        raise HTTPException(status_code=429, detail=_BUSY)


@router.post("/api/setup/channels/telegram/test")
async def telegram_test(req: TelegramTestRequest, actor: Actor = Depends(require_person)):
    ctx = await _claimed_context(actor)
    _refuse_without_local_secrets(ctx)
    token = (req.bot_token or "").strip() or ctx.config.telegram.bot_token
    if not token:
        return {"ok": False, "bot_username": None, "detail": "No bot token to test."}
    if not _TG_TOKEN_RE.match(token):
        return {"ok": False, "bot_username": None, "detail": "That does not look like a bot token from @BotFather."}
    try:
        ok, detail = await setup_checks.telegram_get_me(token)
    except setup_checks.Busy:
        raise HTTPException(status_code=429, detail=_BUSY)
    if ok:
        # Remembered in this process only, keyed by the token's hash: a test
        # names a *candidate* that may never be saved. The state file is
        # written by `PUT /api/setup/channels` alone (which picks this cached
        # name up when that same token is saved), so GET /api/setup never
        # names a bot the instance is not configured with.
        _bot_usernames[_token_key(token)] = detail
        return {"ok": True, "bot_username": detail, "detail": None, "hint": None}
    if detail == "timeout":
        return {
            "ok": False, "bot_username": None, "detail": _TG_UNREACHABLE,
            "hint": "This runs on the server, not in your browser. Check its internet access or proxy settings.",
        }
    return {
        "ok": False, "bot_username": None,
        "detail": f"Telegram rejected this token: {detail}", "hint": _TG_REJECTED_HINT,
    }


@router.post("/api/setup/channels/slack/test")
async def slack_test(req: SlackTestRequest, actor: Actor = Depends(require_person)):
    ctx = await _claimed_context(actor)
    _refuse_without_local_secrets(ctx)
    bot = (req.bot_token or "").strip() or ctx.config.slack.bot_token
    app = (req.app_token or "").strip() or ctx.config.slack.app_token
    if not bot or not app:
        return {"ok": False, "team": None, "bot_user": None, "detail": "Slack needs both a bot token and an app token."}
    try:
        result = await setup_checks.slack_test(bot, app)
    except setup_checks.Busy:
        raise HTTPException(status_code=429, detail=_BUSY)
    return {k: result.get(k) for k in ("ok", "team", "bot_user", "detail")}


# --------------------------------------------------------------------------- #
#  Telegram pairing                                                            #
# --------------------------------------------------------------------------- #


def _allowed_users_on_disk(config: NerveConfig) -> int:
    """Re-read the allow-list from config.local.yaml: /pair appends there."""
    try:
        path = Path(config.config_dir) / "config.local.yaml"
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        users = (data.get("telegram") or {}).get("allowed_users") or []
        return len(users) if isinstance(users, list) else len(config.telegram.allowed_users)
    except (OSError, yaml.YAMLError, AttributeError):
        return len(config.telegram.allowed_users)


def _pairing_state() -> tuple[bool, str | None]:
    state = pairing._read_state()  # noqa: SLF001 — the module's own file format
    if not state or pairing.read_pairing_code() is None:
        return False, None
    expires = state.get("expires_at")
    try:
        when = datetime.fromtimestamp(float(expires), tz=timezone.utc)
    except (TypeError, ValueError):
        return True, None
    return True, when.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _pairing_body(ctx: _Context) -> dict[str, Any]:
    facts = ctx.facts
    detail = next(
        s for s in summarize(facts)["steps"] if s["id"] == STEP_CHANNELS
    )["detail"]["telegram"]
    active, expires = _pairing_state()
    username = facts.bot_username
    return {
        "available": detail["pairing_available"],
        "reason": detail["pairing_unavailable_reason"],
        "live": facts.telegram_live,
        "bot_username": username,
        "link": f"https://t.me/{username}" if username else None,
        "allowed_users_count": _allowed_users_on_disk(ctx.config),
        "code_active": active,
        "expires_at": expires,
    }


@router.get("/api/setup/channels/telegram/pairing")
async def pairing_status(actor: Actor = Depends(require_person)):
    ctx = await _claimed_context(actor)
    return _pairing_body(ctx)


@router.post("/api/setup/channels/telegram/pairing")
async def pairing_issue(actor: Actor = Depends(require_person)):
    async with _loop_lock("state"):
        ctx = await _claimed_context(actor)
        if ctx.facts.lockdown:
            raise HTTPException(status_code=409, detail=LOCKDOWN_TEXT)
        _refuse_without_local_secrets(ctx)
        if not ctx.config.telegram.bot_token:
            raise HTTPException(status_code=409, detail="Add a bot token first.")
        if ctx.config.telegram.dm_policy != "pairing":
            raise HTTPException(status_code=409, detail="Telegram DM policy is not 'pairing'.")
        if not ctx.facts.telegram_live:
            raise HTTPException(status_code=409, detail="The bot is not running with the saved token yet. Restart, then pair.")
        code = pairing.get_or_create_pairing_code()
        body = _pairing_body(ctx)
        return {
            "code": code,
            "expires_at": body["expires_at"],
            "bot_username": body["bot_username"],
            "link": body["link"],
            "allowed_users_count": body["allowed_users_count"],
        }


# --------------------------------------------------------------------------- #
#  Sources preflight · agents detect · environment                             #
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
#  Telegram message sync: the signed-in session                                #
# --------------------------------------------------------------------------- #
#
# The Telethon source reads the person's own account and needs an authorised
# user session on disk; API credentials alone do nothing. These routes run the
# sign-in (phone → code → optional password) the terminal command
# `nerve setup-telegram` runs interactively. One pending login per process,
# in memory only (nerve/setup_telegram_login.py). The phone leaves only
# masked; the code, the password and Telegram's hash never leave at all.

_telegram_login = setup_telegram_login.TelegramLogin()


class TelegramPhoneRequest(BaseModel):
    phone: str = Field(min_length=1, max_length=40)


class TelegramCodeRequest(BaseModel):
    code: str = Field(min_length=1, max_length=16)


class TelegramPasswordRequest(BaseModel):
    password: str = Field(min_length=1, max_length=_SECRET)


def _telegram_session_scope(ctx: _Context, *, write: bool) -> None:
    """The sources step must exist on this instance and, to change the
    session, be actionable: not locked, not waiting for the Mode step."""
    step = next((s for s in derive(ctx.facts) if s.id == STEP_SOURCES), None)
    if step is None:
        raise HTTPException(status_code=409, detail="Sources do not apply to a worker instance.")
    if not write:
        return
    if ctx.facts.lockdown:
        raise HTTPException(status_code=409, detail=LOCKDOWN_TEXT)
    # The sign-in leaves a user session on this machine.
    _refuse_without_local_secrets(ctx)
    if step.status == STATUS_BLOCKED:
        raise HTTPException(status_code=409, detail=step.blocked_reason or "Choose a mode first")


def _login_failure(exc: setup_telegram_login.LoginError) -> HTTPException:
    if exc.field is not None or exc.retry_after is not None:
        return SetupFieldError(exc.field, exc.detail, status_code=exc.status_code, retry_after=exc.retry_after)
    return HTTPException(status_code=exc.status_code, detail=exc.detail)


async def _note_telegram_session(result: dict[str, Any]) -> None:
    """Remember (or forget) who the sync is signed in as, in the state file,
    so the pure checklist and ``nerve doctor`` can say "signed in as @name"
    without opening the session. ``authorized: null`` (busy, unreachable,
    not configured) leaves what was known alone."""
    authorized = result.get("authorized")
    if authorized is None:
        return
    user = result.get("user") if authorized else None
    async with _loop_lock("state"):
        state = setup_state.load_state()
        setup_state.retire_on_boot(state)
        if state.telegram_sync_user != user or state.retired:
            state.telegram_sync_user = user
            setup_state.save_state(state)


async def _telegram_session_call(actor: Actor, *, write: bool, call: Any) -> dict[str, Any]:
    ctx = await _claimed_context(actor)
    _telegram_session_scope(ctx, write=write)
    try:
        result = await call(ctx.config)
    except setup_telegram_login.LoginError as exc:
        raise _login_failure(exc) from None
    await _note_telegram_session(result)
    return result


@router.get("/api/setup/sources/telegram/session")
async def telegram_session_status(actor: Actor = Depends(require_person)):
    """Whether the sync is signed in: a short-lived client on the session file
    (five seconds at most), never while a sign-in is pending."""
    return await _telegram_session_call(actor, write=False, call=lambda config: _telegram_login.status(config))


@router.post("/api/setup/sources/telegram/session/start")
async def telegram_session_start(req: TelegramPhoneRequest, actor: Actor = Depends(require_person)):
    return await _telegram_session_call(
        actor, write=True, call=lambda config: _telegram_login.start(config, req.phone),
    )


@router.post("/api/setup/sources/telegram/session/code")
async def telegram_session_code(req: TelegramCodeRequest, actor: Actor = Depends(require_person)):
    return await _telegram_session_call(
        actor, write=True, call=lambda config: _telegram_login.code(config, req.code),
    )


@router.post("/api/setup/sources/telegram/session/password")
async def telegram_session_password(req: TelegramPasswordRequest, actor: Actor = Depends(require_person)):
    return await _telegram_session_call(
        actor, write=True, call=lambda config: _telegram_login.password(config, req.password),
    )


@router.post("/api/setup/sources/telegram/session/cancel")
async def telegram_session_cancel(actor: Actor = Depends(require_person)):
    return await _telegram_session_call(actor, write=True, call=lambda config: _telegram_login.cancel(config))


@router.post("/api/setup/sources/telegram/session/logout")
async def telegram_session_logout(actor: Actor = Depends(require_person)):
    return await _telegram_session_call(actor, write=True, call=lambda config: _telegram_login.logout(config))


@router.get("/api/setup/sources/preflight")
async def sources_preflight(actor: Actor = Depends(require_person)):
    ctx = await _claimed_context(actor)
    try:
        return await setup_checks.sources_preflight(ctx.config)
    except setup_checks.Busy:
        raise HTTPException(status_code=429, detail=_BUSY)


@router.get("/api/setup/agents/detect")
async def agents_detect(actor: Actor = Depends(require_person)):
    ctx = await _claimed_context(actor)
    try:
        return await setup_checks.agents_detect(ctx.config)
    except setup_checks.Busy:
        raise HTTPException(status_code=429, detail=_BUSY)


def is_loopback_peer(host: str | None) -> bool:
    """Whether an address is loopback. **Advisory only** — it decides the HTTPS
    hint on the Review step and is never an authorization input (the claim
    requires the setup token from every address)."""
    if not host:
        return False
    text = host.split("%", 1)[0]
    if text.startswith("::ffff:"):
        text = text[len("::ffff:"):]
    try:
        return ipaddress.ip_address(text).is_loopback
    except ValueError:
        return text == "localhost"


@router.get("/api/setup/environment")
async def environment(request: Request, actor: Actor = Depends(require_person)):
    ctx = await _claimed_context(actor)
    config = ctx.config
    try:
        checks = await setup_checks.environment(config)
    except setup_checks.Busy:
        raise HTTPException(status_code=429, detail=_BUSY)
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme or "http").split(",")[0].strip()
    bind_host = config.gateway.host
    host_is_loopback = is_loopback_peer(bind_host)
    client_host = request.client.host if request.client else None
    client_is_loopback = is_loopback_peer(client_host)
    supervision = _supervision()
    mode, message, command, _hint = _restart_decision(ctx.facts)
    return {
        "checks": checks,
        "served": {
            "scheme": scheme,
            "host_is_loopback": host_is_loopback,
            "client_is_loopback": client_is_loopback,
            "https_hint": scheme == "http" and not (host_is_loopback and client_is_loopback),
        },
        "bind_host": bind_host,
        "port": config.gateway.port,
        "in_container": daemon.in_container(),
        "supervision": supervision,
        "passwordless": None if ctx.facts.external_auth else bool(ctx.facts.account_passwordless),
        "restart_mode": mode,
        "restart_message": message,
        "command": command,
        "checked_at": _now(),
    }


# --------------------------------------------------------------------------- #
#  Finish and restart                                                          #
# --------------------------------------------------------------------------- #


@router.post("/api/setup/finish")
async def finish(actor: Actor = Depends(require_person)):
    async with _loop_lock("state"):
        ctx = await _claimed_context(actor)
        summary = await _reload(Path(ctx.config.config_dir)) if ctx.config.config_dir else {}
        ctx = await _context(actor)
        facts = ctx.facts
        onboarding = {"status": facts.onboarding, "session_id": facts.onboarding_session_id}
        restart = bool(facts.restart_pending or facts.state.debts)
        if facts.onboarding == onboarding_flow.PENDING:
            # The conversation opens now, for this person, when the agent can
            # answer; otherwise the next daemon start opens it. Finish always
            # opens the conversation, also for a worker that waits for its
            # autonomous onboarding.
            try:
                deps = get_deps()
                if restart or not facts.config.credential_available() or deps.engine is None:
                    await onboarding_flow.request(deps.db, facts.workspace, actor_id=actor.actor_id)
                else:
                    state = await onboarding_flow.start(
                        deps.engine, facts.workspace, actor=actor, cause=actor,
                    )
                    onboarding = {"status": state.status, "session_id": state.session_id}
                    ctx = await _context(actor)
            except Exception as e:  # noqa: BLE001
                logger.warning("Setup: the onboarding conversation could not be opened: %s", e)
        action = "restart_required" if restart else "reloaded"
        warning = _warning_from(summary, True)
        return {"state": _render(ctx, warning=warning), "action": action, "onboarding": onboarding}


@router.post("/api/setup/onboarding/skip")
async def skip_onboarding(actor: Actor = Depends(require_person)):
    """Skip the onboarding conversation. The session stays in the list."""
    async with _loop_lock("state"):
        ctx = await _claimed_context(actor)
        await onboarding_flow.skip(get_deps().db, Path(ctx.config.workspace), actor_id=actor.actor_id)
        return _render(await _context(actor))


class RestartResponse(BaseModel):
    mode: str
    generation: str
    message: str
    command: str | None = None
    timeout_hint_seconds: int = 0


@router.post("/api/system/restart", response_model=RestartResponse)
async def restart_system(actor: Actor = Depends(require_person)):
    """Restart the daemon — what ``nerve restart`` does, asked for over HTTP.

    Takes the state lock first, so an in-flight save (including its reload
    and its state write) has finished before anything is signalled. Nothing
    is spawned when nothing needs a restart: the configuration is reloaded
    and the answer says so. The once-per-process flag is set only after the
    helper or the exit timer has actually started.
    """
    global _restart_requested
    async with _loop_lock("state"), _loop_lock("restart"):
        ctx = await _claimed_context(actor)
        config = ctx.config
        generation = boot.boot_id()
        if _restart_requested:
            raise HTTPException(status_code=409, detail="A restart is already in progress.")
        if not ctx.facts.restart_pending and not ctx.state.debts:
            if config.config_dir:
                await _reload(Path(config.config_dir))
            return RestartResponse(
                mode="reload", generation=generation,
                message="Nothing needed a restart; the configuration was reloaded.",
                command=None, timeout_hint_seconds=0,
            )
        mode, message, command, hint = _restart_decision(ctx.facts)
        how = daemon.restart_mode()
        if how == "refuse":
            return RestartResponse(
                mode="unsupported", generation=generation, message=message,
                command=command, timeout_hint_seconds=hint,
            )
        if how != "exit" and not config.config_dir:
            # The helper starts `nerve -c <config_dir> start --foreground`;
            # without a known directory there is nothing truthful to pass it,
            # and guessing one would start the new process somewhere else.
            raise HTTPException(status_code=409, detail=_NO_CONFIG_DIR)
        try:
            if how == "exit":
                daemon.exit_for_supervisor(_RESTART_DELAY_SECONDS)
            else:
                await asyncio.to_thread(
                    daemon.restart_daemon, Path(config.config_dir),
                    old_pid=os.getpid(), delay_seconds=_RESTART_DELAY_SECONDS,
                )
        except OSError as e:
            logger.exception("Restart requested by %s could not start", actor.actor_id)
            why = e.strerror or (f"errno {e.errno}" if e.errno else type(e).__name__)
            raise HTTPException(
                status_code=500,
                detail=f"The restart could not be started: {why}. The instance is "
                       "still running; run nerve restart on the server.",
            ) from e
        _restart_requested = True
        logger.info("Restart requested by %s (%s)", actor.actor_id, how)
        return RestartResponse(
            mode="restart", generation=generation, message=message,
            command=command, timeout_hint_seconds=hint,
        )
