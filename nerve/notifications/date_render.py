"""Render ISO date placeholders in notification text.

Models (especially in cron-driven sessions like inbox-processor) regularly
miscompute weekdays for absolute dates — e.g. writing "24 июня (вт)" when
24 June is a Wednesday. Asking the model to "be more careful" or
embedding a 14-day lookup table in the system prompt both fail: the first
is unreliable, the second doesn't scale beyond two weeks.

Instead, this module follows ``print(f"{x:.2f}")`` logic: the model
declares semantic intent ("this is an event date") via a placeholder,
and the code formats it deterministically.

Syntax accepted in any notification ``title`` or ``body``:

    <YYYY-MM-DD>           → "24 июня (ср)"
    <YYYY-MM-DD HH:MM>     → "24 июня (ср), 19:00"

If the placeholder date matches today / tomorrow / yesterday relative
to the rendering ``now``, a relative label is prepended:

    <2026-06-24>           (today)     → "сегодня, 24 июня (ср)"
    <2026-06-25 19:00>     (tomorrow)  → "завтра, 25 июня (чт), 19:00"
    <2026-06-23>           (yesterday) → "вчера, 23 июня (вт)"

The relative-label arithmetic is done in Python from a known ``now``, so
the failure mode that originally motivated this module — the model
miscounting day deltas — cannot recur here.

Malformed placeholders (impossible dates like ``<2026-02-31>``,
out-of-range hours, wrong digits) are left untouched so the model sees
its own broken output rather than a silent miscarriage.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Optional


# Order matters: month index = position (1-based)
_MONTHS_RU_GENITIVE = (
    "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
)

_WEEKDAYS_RU_SHORT = ("пн", "вт", "ср", "чт", "пт", "сб", "вс")

# Strict: 4-digit year, 2-digit month, 2-digit day, optional HH:MM time.
# Wrapped in angle brackets so it never collides with markdown / HTML.
_ISO_DATE_RE = re.compile(
    r"<(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2}))?>"
)


def render_iso_dates(text: str, now: Optional[datetime] = None) -> str:
    """Replace ``<YYYY-MM-DD[ HH:MM]>`` placeholders with localized strings.

    Args:
        text: Notification title or body. Any string is safe — non-matching
            text passes through untouched.
        now: Reference time for "сегодня/завтра/вчера" labels. Defaults to
            the current local time. Tests inject a fixed value.

    Returns:
        Text with every well-formed placeholder rendered. Malformed
        placeholders (impossible dates, bad hours) are left as-is.
    """
    if not text or "<" not in text:
        return text

    today = (now or datetime.now()).date()

    def _replace(match: re.Match) -> str:
        year_s, month_s, day_s, hour_s, minute_s = match.groups()
        try:
            event = date(int(year_s), int(month_s), int(day_s))
        except ValueError:
            # Impossible date like 31 February — leave the placeholder
            # visible so the model sees its own malformed output.
            return match.group(0)

        time_part = ""
        if hour_s is not None:
            try:
                h, m = int(hour_s), int(minute_s)
                if not (0 <= h <= 23 and 0 <= m <= 59):
                    return match.group(0)
                time_part = f", {h:02d}:{m:02d}"
            except ValueError:
                return match.group(0)

        month_name = _MONTHS_RU_GENITIVE[event.month - 1]
        weekday_short = _WEEKDAYS_RU_SHORT[event.weekday()]
        absolute = f"{event.day} {month_name} ({weekday_short})"

        delta_days = (event - today).days
        relative = ""
        if delta_days == 0:
            relative = "сегодня, "
        elif delta_days == 1:
            relative = "завтра, "
        elif delta_days == -1:
            relative = "вчера, "

        return f"{relative}{absolute}{time_part}"

    return _ISO_DATE_RE.sub(_replace, text)
