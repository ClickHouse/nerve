"""Unified config hot-reload.

A single entry point that re-reads config from disk and reloads every subsystem
that supports it without a restart: the process config object (so lockdown and
settings changes engage), the long-lived services that captured that object at
start-up, cron jobs, cron sources, MCP servers, and skills. Best-effort per
subsystem — one failure is reported but does not abort the rest, because
refusing a valid cron edit over an unrelated typo in ``settings.yaml`` is the
worse outcome. Which subsystems fell over is reported, never inferred: see
:func:`reload_failures`.

Nothing reloads on its own. A reload happens when an operator asks for one
(``nerve reload`` / ``POST /api/config/reload``) or when a workspace sync merges a
change. Editing a config file on the box does not apply itself.

Restart-only (NOT reloaded here): the gateway socket (host/port/SSL), the
Telegram bot's token and allow-list, the MCP endpoint (including the
``auth.jwt_secret`` it checks ``/mcp/v1`` against, which the web gateway reads
per request), Langfuse, the memory bridges, the Codex thread-sync service
(``sync.codex.*`` — a different service from the cron sources under ``sync.*``,
and the one place those two names diverge), anything a service derived from
config at construction, and a background loop that was never started because its
feature was off. The hot-reload table in ``docs/config.md`` is the
operator-facing list of exactly what a reload covers; keep the two in step.

:func:`restart_required` diffs the old and new config over
:data:`_RESTART_ONLY_PATHS` and records what changed in the summary. Without it
the summary reports only what was applied, which a caller cannot distinguish from
"nothing needed applying". ``gateway.host``/``port`` are the case that matters:
they live in the tracked settings, so the change can arrive by workspace sync.

It reports; it does not gate. A setting whose only protection is a line in that
report is a setting the daemon is not applying — which is tolerable for a bound
socket and not for a policy that was tightened. Where the holder can resolve
config per read instead, that is the fix, and the path leaves this list: see
:func:`_repoint`.
"""

from __future__ import annotations

import dataclasses
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# A subsystem that failed is reported in-band, as a marked string in the
# summary, so the caller sees the outcome of every subsystem rather than the
# first exception. Written and parsed through this one constant so the producer
# and the consumers cannot drift apart.
_ERROR_PREFIX = "error: "

# Config paths a reload cannot apply. Mirrors the "What still needs a restart"
# table in ``docs/config.md``; keep the two in step. That table says the check
# covers every unconditional entry in it, so a row without an entry here is a
# promise the daemon does not keep —
# ``tests/test_config_reload.py::TestRestartTableCoverage`` reads the table and
# fails on the difference.
#
# Only the entries this function can decide by comparing two values. The
# conditional ones in that table ("turning X on", "adding the first target", "a
# session already running") depend on runtime state not available here, and
# reporting them unconditionally would mean false alarms on every reload.
#
# A path may name a whole section (``proxy``) rather than its fields: where
# everything under a section is restart-only and there is nothing useful to say
# about which field moved, the section compares in one entry and keeps covering
# it when a field is added later. Where only part of a section is restart-only,
# or the individual values are what an operator needs to read, the entry names
# the field.
_RESTART_ONLY_PATHS = (
    "agent.max_concurrent",
    "auth.jwt_secret",
    "codex.home_dir",
    "external_agents.enabled",
    "gateway.host",
    "gateway.port",
    "gateway.ssl.cert",
    "gateway.ssl.key",
    "langfuse.host",
    "langfuse.public_key",
    "langfuse.redact_patterns",
    "langfuse.secret_key",
    "mcp_endpoint.enabled",
    "mcp_endpoint.include_hoa",
    "mcp_endpoint.path",
    "memory",
    "proxy",
    "sync.codex",
    "telegram.allowed_users",
    "telegram.bot_token",
    "telegram.enabled",
    "timezone",
    "workflows.enabled",
    "workflows.poll_interval_seconds",
    "workflows.review_loop.enabled",
    "workflows.review_loop.reconcile_interval_seconds",
    "workspace",
    "xmemory",
)

# Paths whose value the summary must not carry. It is logged and returned over
# HTTP, so a secret printed here is a secret in the log file as well.
_SECRET_PATHS = frozenset({
    "auth.jwt_secret",
    "langfuse.public_key",
    "langfuse.secret_key",
    "telegram.bot_token",
})

_UNSET = object()


def _dotted_attr(obj, dotted: str):
    """Follow a dotted attribute path, returning ``_UNSET`` if any hop is missing.

    ``_UNSET`` rather than ``None`` so that an absent attribute is distinguishable
    from one whose value is ``None``.
    """
    node = obj
    for part in dotted.split("."):
        if not hasattr(node, part):
            return _UNSET
        node = getattr(node, part)
    return node


def _describe(path: str, before, after) -> str:
    """One summary line for a changed path.

    The values are what make the warning actionable — "8900 → 9100" says which
    edit is waiting on the restart — so they are shown except where they cannot
    be: a secret, and a whole section, whose repr is both unreadable and liable
    to hold one (``proxy`` carries the local proxy's API key).
    """
    if path in _SECRET_PATHS or dataclasses.is_dataclass(before):
        return f"{path}: changed"
    return f"{path}: {before!r} → {after!r}"


def restart_required(old_config, new_config) -> list[str]:
    """Dotted paths that changed but cannot take effect until a restart.

    Returns an empty list if either argument is ``None`` — no live config yet, or
    the new one failed to load — since there is nothing to compare.
    """
    if old_config is None or new_config is None:
        return []
    changed = []
    for path in _RESTART_ONLY_PATHS:
        before = _dotted_attr(old_config, path)
        after = _dotted_attr(new_config, path)
        if before is _UNSET or after is _UNSET:
            continue
        if before != after:
            changed.append(_describe(path, before, after))
    return changed


def reload_failures(summary: dict) -> dict[str, str]:
    """The subsystems in a :func:`reload_all` summary that failed, mapped to why.

    ``reload_all`` never raises and keeps going after a failure, so this is the
    only way to tell a reload that applied everything from one that applied part
    of it. An empty dict means every subsystem took the new config.
    """
    return {
        name: outcome[len(_ERROR_PREFIX):]
        for name, outcome in summary.items()
        if isinstance(outcome, str) and outcome.startswith(_ERROR_PREFIX)
    }


def _external_agents_service():
    """The running external-agents sweeper, or ``None`` outside a live gateway."""
    try:
        from nerve.gateway.routes._deps import get_deps

        return get_deps().external_agents_sync
    except Exception:  # noqa: BLE001 — no gateway (CLI, tests): nothing to re-point
        return None


def _workflow_run_service():
    """The running workflow-run service, or ``None`` outside a live gateway."""
    try:
        from nerve.workflows import get_workflow_run_service

        return get_workflow_run_service()
    except Exception:  # noqa: BLE001 — no gateway (CLI, tests): nothing to re-point
        return None


def _repoint(new_config, engine, cron_service) -> list[str]:
    """Hand *new_config* to the long-lived objects still holding the old one.

    Replacing the process-wide config object covers everything that reads it per
    use, but the services built during start-up each kept their own reference. A
    service left pointing at the previous object is worse than a uniformly stale
    daemon: half the process runs on the new config and half on the old, with
    nothing to say which half is which.

    This list is deliberately short, and shrinking it is the better fix wherever
    it can be done: the agent backends used to need re-pointing and now resolve
    config through a callable onto the engine's attribute instead, which also
    removed a second-level cache (``codex``) that re-pointing alone would have
    missed. Prefer that shape — one authority, resolved per read — over adding a
    holder here, because every holder here is a place the two halves can drift.

    Only objects whose config reads all happen per use are re-pointed. A value
    some other object *derived* at construction — a semaphore's size, a
    scheduler's timezone, a bot's cached allow-list, a bound socket — is not
    rebuilt and still needs a restart.

    Returns a description of every holder that could not be re-pointed (normally
    empty). Never raises: having loaded the config, a reload should report which
    part of the hand-off failed rather than lose the whole step. It can only
    report what it attempts, which is the other reason to keep the list short.
    """
    problems: list[str] = []

    def hand_over(label: str, target) -> None:
        if target is None:
            return
        try:
            target.config = new_config
        except Exception as e:  # noqa: BLE001 — report, don't abandon the rest
            problems.append(f"{label}: {e}")
            logger.warning(
                "Could not re-point the %s at the reloaded config: %s", label, e,
            )

    hand_over("cron service", cron_service)
    if engine is not None and hasattr(engine, "config"):
        # Assignment, not a method call: the engine's setter takes the new object
        # and moves the collaborators it seeded (the session manager's backend
        # and model defaults) in the same step, and its backends read config
        # through it, so the whole agent subtree lands together.
        hand_over("agent engine", engine)
        # Every notification setting is read at send time, so re-pointing the
        # service is all it takes for channels, expiry and quiet hours to follow
        # a reload. The Telegram *channel* is not here because it needs nothing:
        # it resolves config through a callable. Its allow-list is still
        # restart-only, cached as a set when the bot was built.
        hand_over(
            "notification service", getattr(engine, "notification_service", None),
        )

    # The workflow-run service holds its own reference and reads through it at
    # use, so the budget ceiling, the concurrency limit, the warning fraction and
    # the journal location all follow a reload. Its poll cadence does not: the
    # monitor loop was handed an interval when it started, which is the
    # start-up-derived case above and still wants a restart.
    hand_over("workflow run service", _workflow_run_service())

    # The external-agents sweeper keeps its own reference, and the routes that
    # add, remove or toggle a target mutate the process config in place. Left on
    # the old object it would keep rendering the old target list while every
    # toggle reported success.
    sweeper = _external_agents_service()
    if sweeper is not None:
        try:
            sweeper.update_config(new_config)
        except Exception as e:  # noqa: BLE001
            problems.append(f"external-agents sync: {e}")
            logger.warning(
                "Could not re-point the external-agents sync at the reloaded "
                "config: %s", e,
            )

    return problems


async def reload_all(engine, cron_service, config_dir: Path) -> dict:
    """Re-read config and hot-reload all reloadable subsystems.

    Returns a per-subsystem summary dict; :func:`reload_failures` turns it into
    the list of subsystems that did not take the new config. Never raises — each
    step is guarded — so a caller that does not inspect the summary is claiming a
    success it has not checked.
    """
    from nerve.config import get_config, load_config, set_config

    summary: dict = {}

    # Captured before set_config replaces it, since the comparison below needs
    # the values the running process is actually using.
    try:
        old_config = get_config()
    except Exception:  # noqa: BLE001 — no config set yet (early boot, tests)
        old_config = None

    # 1. Config object — so lockdown and settings changes engage everywhere.
    try:
        new_config = load_config(config_dir)
        set_config(new_config)
    except Exception as e:  # noqa: BLE001 — e.g. an invalid edit; report, keep going
        summary["config"] = f"{_ERROR_PREFIX}{e}"
        logger.warning("config reload failed: %s", e, exc_info=True)
    else:
        summary["config"] = "reloaded"
        # Not an error — the reload applied everything it can. Reported so the
        # caller can tell "applied" from "cannot apply without a restart".
        pending = restart_required(old_config, new_config)
        if pending:
            summary["restart_required"] = "; ".join(pending)
            logger.warning(
                "config reload: these changed but need a restart to take "
                "effect: %s", summary["restart_required"],
            )
        stale = _repoint(new_config, engine, cron_service)
        if stale:
            # The config itself loaded, so this is its own line: the daemon is
            # running the new config in some places and the old one in others,
            # which is the state worth shouting about.
            summary["services"] = f"{_ERROR_PREFIX}{'; '.join(stale)}"

    # 2. Cron jobs + sources.
    if cron_service is not None:
        try:
            summary["cron"] = await cron_service.reload()
        except Exception as e:  # noqa: BLE001
            summary["cron"] = f"{_ERROR_PREFIX}{e}"
        try:
            summary["sources"] = await cron_service.reload_sources()
        except Exception as e:  # noqa: BLE001
            summary["sources"] = f"{_ERROR_PREFIX}{e}"

    # 3. MCP servers.
    if engine is not None:
        try:
            servers = await engine.reload_mcp_config()
            summary["mcp"] = f"{len(servers)} server(s)"
        except Exception as e:  # noqa: BLE001
            summary["mcp"] = f"{_ERROR_PREFIX}{e}"

    # 4. Skills (re-scan the workspace skills dir).
    mgr = getattr(engine, "_skill_manager", None) if engine is not None else None
    if mgr is not None:
        try:
            skills = await mgr.discover()
            summary["skills"] = f"{len(skills)} discovered"
        except Exception as e:  # noqa: BLE001
            summary["skills"] = f"{_ERROR_PREFIX}{e}"

    return summary
