"""Dispatch registry for ``approval``-kind notifications.

When a user answers an ``approval`` notification, the notification
service looks up the row's ``target_kind`` and routes the decision to
the matching dispatcher in this module. Dispatchers know how to act on
their target type (a mechanical-action proposal, a plan, etc.) and
return a structured ``DispatchResult`` so the service can audit-log the
outcome uniformly.

One dispatcher ships today: ``mechanical-action``. Snoozed approvals
are re-surfaced by the notification service's periodic maintenance
tick (``NotificationService.redeliver_due``); a future ``plan``
dispatcher can register here without service changes.

Design notes:

- The registry is keyed by ``target_kind`` (a string). Decisions
  (``approve`` / ``decline`` / ``snooze_24h`` / future values) are
  passed as a function arg, not part of the key, so adding a new
  decision doesn't need a new registry entry.
- Dispatchers receive the raw notification dict, the target id, the
  decision string, and the live ``NerveConfig`` (for the workspace
  path; see ``mechanical-action`` below). The service-side caller is
  responsible for committing the DB-level status flip, so dispatchers
  can stay pure-side-effect-only against the target system.
- Dispatchers should be deterministic in their audit_event keys so the
  audit log stays grep-able.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from nerve.config import NerveConfig

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
#  Result type
# ----------------------------------------------------------------------


@dataclass
class DispatchResult:
    """Outcome of a single dispatch call.

    Fields:
    - ``ok``: True when the dispatcher's downstream action completed
      successfully. False on a non-zero exit or an internal error. The
      service still flips the notification's status to ``answered`` on
      either outcome, so the user does not see a re-delivered approval
      that the system already acted on; the audit_event records the
      failure for replay.
    - ``audit_event``: a structured dict the caller appends to the
      mechanical-actions audit log. Already includes the dispatcher's
      own event name (``approval-acted``); the service adds the
      notification id and timestamp.
    - ``snooze_until``: ISO-8601 UTC timestamp for when a snoozed
      notification should be re-delivered. The service stamps it into
      the row's ``redeliver_at`` (and pushes ``expires_at`` past it) so
      the periodic maintenance tick re-surfaces the card. None for
      approve / decline.
    """

    ok: bool
    audit_event: dict[str, Any] = field(default_factory=dict)
    snooze_until: str | None = None


# ----------------------------------------------------------------------
#  Registry primitives
# ----------------------------------------------------------------------


Dispatcher = Callable[
    [dict[str, Any], str, str, "NerveConfig | None"], DispatchResult
]


_DISPATCHERS: dict[str, Dispatcher] = {}


def register(target_kind: str, fn: Dispatcher) -> None:
    """Register a dispatcher for a ``target_kind``.

    Idempotent: re-registering overwrites the previous entry so test
    code can swap in fakes per-test without leaking state across the
    module-level dict.
    """
    _DISPATCHERS[target_kind] = fn


def get(target_kind: str) -> Dispatcher | None:
    """Look up a dispatcher by ``target_kind``. None if unregistered."""
    return _DISPATCHERS.get(target_kind)


def known_kinds() -> list[str]:
    """Return the currently registered ``target_kind`` values."""
    return sorted(_DISPATCHERS.keys())


# ----------------------------------------------------------------------
#  mechanical-action dispatcher
# ----------------------------------------------------------------------


# Valid decisions for the mechanical-action dispatcher. Kept here so a
# typo in the caller surfaces as an explicit failure rather than a
# silent no-op shell call.
_MECHANICAL_DECISIONS = frozenset({"approve", "decline", "snooze_24h"})


def _resolve_workspace(
    config: "NerveConfig | None",
) -> Path | None:
    """Pick the workspace directory.

    Priority:
    1. ``$NERVE_WORKSPACE_PATH`` env var (test / override hook).
    2. ``config.workspace`` from the live NerveConfig.
    3. None if neither is available.
    """
    override = os.environ.get("NERVE_WORKSPACE_PATH")
    if override:
        return Path(override).expanduser()
    if config is not None and config.workspace:
        return Path(config.workspace).expanduser()
    return None


def _dispatch_mechanical_action(
    notification: dict[str, Any],
    target_id: str,
    decision: str,
    config: "NerveConfig | None",
) -> DispatchResult:
    """Approve / decline / snooze a queued mechanical-action proposal.

    Shells out to ``<workspace>/scripts/mechanical-action.sh`` which is
    the decide-side wrapper around the propose-mechanical-action
    primitive. The wrapper handles audit-log writes for the
    approve / decline paths; this dispatcher writes its own
    ``approval-acted`` audit event so the chain is observable from the
    notification side as well.

    Snooze: rather than touch the queue file directly, this dispatcher
    calls ``mechanical-action.sh snooze <id> --hours 24`` and lets the
    decide-side script record the audit event and update the queue
    entry's ``not_before`` field. The returned ``snooze_until`` makes
    the service stamp the notification row's ``redeliver_at``, and the
    periodic maintenance tick re-delivers the card at that time.
    """
    notif_id = notification.get("id", "")
    base_event: dict[str, Any] = {
        "event": "approval-acted",
        "notification_id": notif_id,
        "target_kind": "mechanical-action",
        "target_id": target_id,
        "decision": decision,
    }

    if decision not in _MECHANICAL_DECISIONS:
        logger.warning(
            "mechanical-action dispatch: unsupported decision %r on %s",
            decision, target_id,
        )
        return DispatchResult(
            ok=False,
            audit_event={
                **base_event,
                "ok": False,
                "error": f"unsupported decision: {decision}",
            },
        )

    workspace = _resolve_workspace(config)
    if workspace is None:
        logger.error(
            "mechanical-action dispatch: no workspace configured; "
            "set NERVE_WORKSPACE_PATH or config.workspace",
        )
        return DispatchResult(
            ok=False,
            audit_event={
                **base_event,
                "ok": False,
                "error": "workspace path unresolved",
            },
        )

    script_path = workspace / "scripts" / "mechanical-action.sh"
    if not script_path.is_file():
        logger.error(
            "mechanical-action dispatch: script missing at %s",
            script_path,
        )
        return DispatchResult(
            ok=False,
            audit_event={
                **base_event,
                "ok": False,
                "error": f"script missing: {script_path}",
            },
        )

    if shutil.which("bash") is None:
        logger.error("mechanical-action dispatch: bash not on PATH")
        return DispatchResult(
            ok=False,
            audit_event={
                **base_event,
                "ok": False,
                "error": "bash not on PATH",
            },
        )

    if decision == "approve":
        cmd = ["bash", str(script_path), "approve", target_id]
        snooze_until = None
    elif decision == "decline":
        reason = (
            f"user declined via notification {notif_id}"
            if notif_id else "user declined via notification"
        )
        cmd = [
            "bash", str(script_path), "decline", target_id,
            "--reason", reason,
        ]
        snooze_until = None
    else:  # snooze_24h
        cmd = [
            "bash", str(script_path), "snooze", target_id,
            "--hours", "24",
        ]
        snooze_until = (
            datetime.now(timezone.utc) + timedelta(hours=24)
        ).isoformat()

    try:
        completed = subprocess.run(
            cmd, capture_output=True, text=True, check=False, timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        logger.error(
            "mechanical-action dispatch: subprocess failed: %s", exc,
        )
        return DispatchResult(
            ok=False,
            audit_event={
                **base_event,
                "ok": False,
                "error": f"subprocess error: {exc}",
            },
            snooze_until=snooze_until,
        )

    ok = completed.returncode == 0
    audit_event = {
        **base_event,
        "ok": ok,
        "exit_code": completed.returncode,
    }
    # Capture a short tail of stderr on failure so the audit log shows
    # what went wrong without becoming a wall of text.
    if not ok and completed.stderr:
        audit_event["stderr_tail"] = completed.stderr[-512:]

    return DispatchResult(
        ok=ok,
        audit_event=audit_event,
        snooze_until=snooze_until,
    )


register("mechanical-action", _dispatch_mechanical_action)


# ----------------------------------------------------------------------
#  Convenience: default approval options
# ----------------------------------------------------------------------


def default_approval_options() -> list[dict[str, str]]:
    """Return the canonical Approve / Decline / Snooze 24h triplet.

    Used by the ``propose_action`` MCP tool when the caller does not
    supply its own options list. Keeping this here (next to the
    dispatcher) keeps the option set co-located with the decisions the
    dispatcher actually understands.
    """
    return [
        {"label": "Approve", "value": "approve"},
        {"label": "Decline", "value": "decline"},
        {"label": "Snooze 24h", "value": "snooze_24h"},
    ]
