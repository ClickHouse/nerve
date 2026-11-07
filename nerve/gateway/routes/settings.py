"""The panel's Settings page: one route group for each section.

A section is a list of fields, each a dotted configuration path with a value,
a state and the file a change goes to (the states are in ``docs/api.md``,
"Settings"). ``PUT`` checks each value and the merged configuration, writes
each value to the file it comes from (a new value to ``settings.yaml``) and
reloads. The guards and writers are the setup checklist's
(:mod:`nerve.gateway.routes.setup`), so the people who can change setup can
change settings.
"""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, Body, Depends, HTTPException

from nerve import setup_provenance
from nerve.config import (
    NOTIFY_CHANNELS,
    NOTIFY_LEVEL_ALL,
    NOTIFY_LEVELS,
    NOTIFY_RECORD_CHANNEL,
    NerveConfig,
    workspace_settings_file,
)
from nerve.config_reload import reload_failures
from nerve.config_validate import validate_config_bundle
from nerve.gateway.routes._deps import get_deps
from nerve.gateway.routes.setup import (
    _MANAGED_DETAIL,
    SetupFieldError,
    _Context,
    _context,
    _loop_lock,
    _reload,
    _require_claimed,
    _require_writable,
    _write,
    require_person,
)
from nerve.identity import Actor
from nerve.notifications.quiet_hours import in_quiet_hours, minutes_of_day
from nerve.setup_writer import merge_machine_paths, merge_private_paths, merge_settings_paths

logger = logging.getLogger(__name__)

router = APIRouter()

STATE_EDITABLE = "editable"
STATE_MACHINE = "machine"
STATE_MANAGED = "managed"
STATE_READ_ONLY = "read_only"

SETTINGS_FILE = "settings.yaml"
_MACHINE_FILE, _LOCAL_FILE = "config.yaml", "config.local.yaml"

CONTROL_PLANE_TEXT = "Set by an admin in the control plane."
_UNKNOWN_FIELD = "This is not a setting of this section."
_NOT_VALID = "With this value the configuration is not valid: {}"
_QUIET_TIME_TEXT = "Use a 24-hour time such as 22:00."

#: Bounds of the question fields.
EXPIRY_HOURS_RANGE = (1, 720)
REDELIVERIES_RANGE = (0, 10)
#: How long the page waits for Slack to name a channel.
SLACK_NAME_TIMEOUT_S = 3.0

TEST_TITLE = "Test notification"
TEST_BODY = "Sent from Settings to check where notifications arrive."
# High, so that a channel set to "Questions and important" gets it too.
TEST_PRIORITY = "high"


@dataclass(frozen=True)
class SettingField:
    """One field of a section: its configuration path, how the loaded
    configuration gives its value, and how a body value is checked.
    ``parse`` raises :class:`ValueError` with the text the person sees."""

    path: str
    read: Callable[[NerveConfig], Any]
    parse: Callable[[Any], Any]


def _parse_channels(value: Any) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise ValueError("Give a list of channel names.")
    unknown = sorted(set(value) - set(NOTIFY_CHANNELS))
    if unknown:
        raise ValueError(f"Unknown channel: {unknown[0]}.")
    if NOTIFY_RECORD_CHANNEL not in value:
        raise ValueError("The panel always gets notifications.")
    return list(dict.fromkeys(value))


def _parse_level(value: Any) -> str:
    if value not in NOTIFY_LEVELS:
        raise ValueError(f"Use one of: {', '.join(NOTIFY_LEVELS)}.")
    return value


def _parse_slack_channel(value: Any) -> str | None:
    from nerve.channels.slack import is_slack_id

    text = str(value or "").strip()
    if not text:
        return None
    if not (is_slack_id(text) and text[0] in "CGD"):
        raise ValueError("Use a Slack channel ID such as C0123ABCD.")
    return text


def _parse_chat_id(value: Any) -> int | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        chat_id = 0 if isinstance(value, bool) else int(str(value).strip())
    except ValueError:
        chat_id = 0
    if chat_id == 0:
        raise ValueError("Use a numeric chat ID, such as -1001234567890.")
    return chat_id


def _int_between(low: int, high: int) -> Callable[[Any], int]:
    def parse(value: Any) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
            raise ValueError(f"Use a whole number from {low} to {high}.")
        return value
    return parse


def _parse_quiet_time(value: Any) -> str:
    """``HH:MM``, or ``""`` (no quiet hours)."""
    if not isinstance(value, str):
        raise ValueError(_QUIET_TIME_TEXT)
    text = value.strip()
    if text:
        try:
            minutes_of_day(text)
        except ValueError:
            raise ValueError(_QUIET_TIME_TEXT) from None
    return text


def _level_field(channel: str) -> SettingField:
    return SettingField(
        f"notifications.levels.{channel}",
        lambda c: c.notifications.levels.get(channel, NOTIFY_LEVEL_ALL),
        _parse_level,
    )


SLACK_DESTINATION = "notifications.slack_channel_id"
QUIET_START, QUIET_END = "quiet_start", "quiet_end"

NOTIFICATION_FIELDS: tuple[SettingField, ...] = (
    SettingField("notifications.channels", lambda c: c.notifications.delivery_channels(), _parse_channels),
    _level_field("slack"),
    _level_field("telegram"),
    SettingField(SLACK_DESTINATION, lambda c: c.notifications.slack_channel_id or None, _parse_slack_channel),
    SettingField(
        "notifications.telegram_chat_id", lambda c: c.notifications.telegram_chat_id, _parse_chat_id,
    ),
    SettingField(
        "notifications.default_expiry_hours",
        lambda c: c.notifications.default_expiry_hours, _int_between(*EXPIRY_HOURS_RANGE),
    ),
    SettingField(
        "notifications.max_redeliveries",
        lambda c: c.notifications.max_redeliveries, _int_between(*REDELIVERIES_RANGE),
    ),
    SettingField(QUIET_START, lambda c: str(c.quiet_start or ""), _parse_quiet_time),
    SettingField(QUIET_END, lambda c: str(c.quiet_end or ""), _parse_quiet_time),
)


def _check_quiet_hours(values: dict[str, Any], config: NerveConfig) -> None:
    """Quiet hours have both times or neither, and two different times.
    400 with the field to fix."""
    if QUIET_START not in values and QUIET_END not in values:
        return
    start = values.get(QUIET_START, str(config.quiet_start or ""))
    end = values.get(QUIET_END, str(config.quiet_end or ""))
    if bool(start) != bool(end):
        raise SetupFieldError(QUIET_END if start else QUIET_START, "Quiet hours need both a start and an end time.")
    if start and start == end:
        raise SetupFieldError(QUIET_END if QUIET_END in values else QUIET_START, "Use two different times.")


def _managed_reason(path: str, layers: setup_provenance.RawLayers, config: NerveConfig) -> str | None:
    if path == SLACK_DESTINATION and config.slack.mode == "hosted":
        return CONTROL_PLANE_TEXT
    if setup_provenance.source_of(path, layers) == setup_provenance.SOURCE_ENVIRONMENT:
        return _MANAGED_DETAIL[setup_provenance.SOURCE_ENVIRONMENT]
    return None


def _field_state(
    field: SettingField, ctx: _Context, layers: setup_provenance.RawLayers,
) -> dict[str, Any]:
    """``{value, state, file, reason}`` of one field."""
    config = ctx.config
    body: dict[str, Any] = {"value": field.read(config), "state": STATE_EDITABLE, "file": SETTINGS_FILE, "reason": None}
    problem = ctx.facts.writable_problem
    if problem:
        return {**body, "state": STATE_READ_ONLY, "file": None, "reason": problem}
    reason = _managed_reason(field.path, layers, config)
    if reason:
        return {**body, "state": STATE_MANAGED, "file": None, "reason": reason}
    machine = setup_provenance.machine_layer_of(field.path, layers)
    if machine:
        return {**body, "state": STATE_MACHINE, "file": machine}
    return body


def _fields(ctx: _Context, fields: tuple[SettingField, ...]) -> dict[str, dict[str, Any]]:
    layers = setup_provenance.read_layers(ctx.config)
    return {field.path: _field_state(field, ctx, layers) for field in fields}


def _parse_body(body: dict[str, Any], fields: tuple[SettingField, ...]) -> dict[str, Any]:
    """The checked values of a partial body; 400 ``{detail, field}`` for the first bad one."""
    known = {field.path: field for field in fields}
    parsed: dict[str, Any] = {}
    for path, value in body.items():
        field = known.get(path)
        if field is None:
            raise SetupFieldError(path, _UNKNOWN_FIELD)
        try:
            parsed[path] = field.parse(value)
        except ValueError as e:
            raise SetupFieldError(path, str(e)) from None
    return parsed


def _settings_update(updates: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """``settings.yaml`` updates as the settings writer takes them: the values
    to set, and the paths to remove (a ``None`` value)."""
    return (
        {path: value for path, value in updates.items() if value is not None},
        [path for path, value in updates.items() if value is None],
    )


def _new_problems(config: NerveConfig, writes: dict[str, dict[str, Any]]) -> list[str]:
    """The validator's errors for the configuration with ``writes`` applied
    that it does not report for the configuration as it is.

    Both checks run on copies of the three files, so nothing live is
    written before the result is known.
    """
    config_dir, workspace = Path(config.config_dir), Path(config.workspace)
    with tempfile.TemporaryDirectory(prefix="nerve-settings-") as tmp:
        copy_dir, copy_ws = Path(tmp) / "config", Path(tmp) / "workspace"
        copy_dir.mkdir()
        for name in (_MACHINE_FILE, _LOCAL_FILE):
            if (config_dir / name).is_file():
                shutil.copy2(config_dir / name, copy_dir / name)
        settings = workspace_settings_file(workspace)
        if settings.is_file():
            workspace_settings_file(copy_ws).parent.mkdir(parents=True)
            shutil.copy2(settings, workspace_settings_file(copy_ws))
        before = set(validate_config_bundle(copy_dir, workspace_override=copy_ws).errors)
        if writes[_LOCAL_FILE]:
            merge_private_paths(copy_dir / _LOCAL_FILE, writes[_LOCAL_FILE])
        if writes[_MACHINE_FILE]:
            merge_machine_paths(copy_dir / _MACHINE_FILE, writes[_MACHINE_FILE])
        if writes[SETTINGS_FILE]:
            merge_settings_paths(copy_ws, *_settings_update(writes[SETTINGS_FILE]))
        after = validate_config_bundle(copy_dir, workspace_override=copy_ws).errors
        return [error.replace(tmp, "") for error in after if error not in before]


async def _save(ctx: _Context, values: dict[str, Any]) -> dict[str, str]:
    """Write ``values`` (checked) and reload. Returns the reload failures."""
    _require_writable(ctx)
    layers = setup_provenance.read_layers(ctx.config)
    writes: dict[str, dict[str, Any]] = {SETTINGS_FILE: {}, _MACHINE_FILE: {}, _LOCAL_FILE: {}}
    for path, value in values.items():
        reason = _managed_reason(path, layers, ctx.config)
        if reason:
            raise SetupFieldError(path, reason, status_code=409)
        writes[setup_provenance.machine_layer_of(path, layers) or SETTINGS_FILE][path] = value
    problems = await asyncio.to_thread(_new_problems, ctx.config, writes)
    if problems:
        field = next(iter(values)) if len(values) == 1 else None
        raise SetupFieldError(field, _NOT_VALID.format(problems[0]))
    portable, removed = _settings_update(writes[SETTINGS_FILE])
    _write(ctx, machine=writes[_MACHINE_FILE], secret=writes[_LOCAL_FILE], portable=portable, shadowed=removed)
    logger.info("Settings: wrote %s", ", ".join(sorted(values)))
    return reload_failures(await _reload(Path(ctx.config.config_dir)))


def _slack_connected(config: NerveConfig) -> bool:
    slack = config.slack
    if slack.mode == "hosted":
        return bool(slack.enabled)
    return bool(slack.enabled and slack.bot_token and slack.app_token)


async def _slack_channel_name(config: NerveConfig) -> str | None:
    """The name of the configured notification channel, when Slack runs."""
    channel_id = config.notifications.slack_channel_id.strip()
    if not channel_id:
        return None
    try:
        slack = get_deps().engine.channel("slack")
    except (RuntimeError, AttributeError):
        return None
    lookup = getattr(slack, "conversation_name", None)
    if lookup is None or not getattr(slack, "is_available", False):
        return None
    try:
        return await asyncio.wait_for(lookup(channel_id), SLACK_NAME_TIMEOUT_S)
    except Exception:  # noqa: BLE001 - a timeout or a Slack error: the page shows the ID only
        return None


async def _notifications_body(ctx: _Context) -> dict[str, Any]:
    config = ctx.config
    return {
        "read_only_reason": ctx.facts.writable_problem,
        "fields": _fields(ctx, NOTIFICATION_FIELDS),
        "slack": {
            "connected": _slack_connected(config),
            "channel_name": await _slack_channel_name(config),
        },
        "telegram": {"connected": bool(config.telegram.enabled and config.telegram.bot_token)},
        "quiet_hours": {"timezone": config.timezone, "active": in_quiet_hours(config)},
    }


@router.get("/api/settings/notifications")
async def get_notifications(actor: Actor = Depends(require_person)):
    ctx = await _context(actor)
    _require_claimed(ctx)
    return await _notifications_body(ctx)


@router.put("/api/settings/notifications")
async def put_notifications(
    body: dict[str, Any] = Body(...), actor: Actor = Depends(require_person),
):
    values = _parse_body(body, NOTIFICATION_FIELDS)
    async with _loop_lock("state"):
        ctx = await _context(actor)
        _require_claimed(ctx)
        _check_quiet_hours(values, ctx.config)
        failures = await _save(ctx, values) if values else {}
        fresh = await _context(actor)
    return {**await _notifications_body(fresh), "reload_errors": failures}


@router.post("/api/settings/notifications/test")
async def test_notifications(actor: Actor = Depends(require_person)):
    """Send one test notification through the normal fan-out, and say for
    each listed channel whether it took it."""
    ctx = await _context(actor)
    _require_claimed(ctx)
    deps = get_deps()
    service = deps.notification_service
    if service is None:
        raise HTTPException(status_code=503, detail="Notifications are not running.")
    notification_id = await service.send_notification(
        session_id="system", title=TEST_TITLE, body=TEST_BODY, priority=TEST_PRIORITY,
    )
    row = await deps.db.get_notification(notification_id) or {}
    delivered = set(json.loads(row.get("channels_delivered") or "[]"))
    return {
        "notification_id": notification_id,
        "silenced": row.get("status") == "silenced",
        "results": [
            {"channel": channel, "delivered": channel in delivered}
            for channel in ctx.config.notifications.delivery_channels()
        ],
    }
