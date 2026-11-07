"""Quiet hours: the daily time window in which notifications make no sound.

``quiet_start`` and ``quiet_end`` are ``HH:MM`` times in the configured
``timezone``. The window can cross midnight (``22:00`` to ``07:00``). Both
values empty means no quiet hours. During the window, each notification that
is not ``urgent`` is still delivered, but without sound.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

if TYPE_CHECKING:
    from nerve.config import NerveConfig

#: The one priority that makes a sound during quiet hours.
URGENT_PRIORITY = "urgent"

_MINUTES_PER_HOUR = 60


def _minutes(hhmm: str) -> int:
    hours, minutes = hhmm.split(":")
    return int(hours) * _MINUTES_PER_HOUR + int(minutes)


def in_quiet_hours(config: NerveConfig, now: datetime | None = None) -> bool:
    """True when ``now`` (default: the current time) is inside quiet hours.

    An empty ``quiet_start`` or ``quiet_end`` means no quiet hours. A value
    that is not ``HH:MM``, or a time zone that is not known, also gives
    False.
    """
    start = str(config.quiet_start or "").strip()
    end = str(config.quiet_end or "").strip()
    if not start or not end:
        return False
    try:
        tz = ZoneInfo(config.timezone)
        start_minutes, end_minutes = _minutes(start), _minutes(end)
    except (ValueError, OSError, ZoneInfoNotFoundError):
        return False
    local = (now or datetime.now(tz)).astimezone(tz)
    current = local.hour * _MINUTES_PER_HOUR + local.minute
    if start_minutes <= end_minutes:
        return start_minutes <= current < end_minutes
    return current >= start_minutes or current < end_minutes


def deliver_silently(config: NerveConfig, priority: str, now: datetime | None = None) -> bool:
    """True when a notification of ``priority`` goes out without sound:
    it is not ``urgent`` and ``now`` is inside quiet hours."""
    return priority != URGENT_PRIORITY and in_quiet_hours(config, now)
