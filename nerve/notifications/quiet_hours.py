"""Quiet hours: the daily time window in which notifications make no sound.

``quiet_start`` and ``quiet_end`` are 24-hour ``HH:MM`` times in the
configured ``timezone``. The window can cross midnight (``22:00`` to ``07:00``). Both
values empty means no quiet hours. During the window, each notification that
is not ``urgent`` is still delivered, but without sound.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

if TYPE_CHECKING:
    from nerve.config import NerveConfig

#: The one priority that makes a sound during quiet hours.
URGENT_PRIORITY = "urgent"

_MINUTES_PER_HOUR = 60
_HOURS_PER_DAY = 24
_HHMM = re.compile(r"([0-9]{2}):([0-9]{2})")


def minutes_of_day(hhmm: str) -> int:
    """The minutes after midnight of a 24-hour ``HH:MM`` time, ``00:00`` to
    ``23:59``. Raises :class:`ValueError` for any other text."""
    match = _HHMM.fullmatch(hhmm)
    if not match or int(match[1]) >= _HOURS_PER_DAY or int(match[2]) >= _MINUTES_PER_HOUR:
        raise ValueError(f"not a 24-hour HH:MM time: {hhmm!r}")
    return int(match[1]) * _MINUTES_PER_HOUR + int(match[2])


def in_quiet_hours(config: NerveConfig, now: datetime | None = None) -> bool:
    """True when ``now`` (default: the current time) is inside quiet hours.

    An empty ``quiet_start`` or ``quiet_end`` means no quiet hours. A value
    that :func:`minutes_of_day` refuses, or a time zone that is not known,
    also gives False.
    """
    start = str(config.quiet_start or "").strip()
    end = str(config.quiet_end or "").strip()
    if not start or not end:
        return False
    try:
        tz = ZoneInfo(config.timezone)
        start_minutes, end_minutes = minutes_of_day(start), minutes_of_day(end)
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
