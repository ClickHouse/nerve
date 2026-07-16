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

Weekday-name placeholder
------------------------

Delivery emails frequently state the estimate as a bare weekday name with
no calendar date — Amazon's "Dispatched — Arriving Tuesday", DHL's
"Zustellung am Dienstag". Resolving "Tuesday" to an absolute date is the
same day-delta arithmetic the model gets wrong (it once kept a stale
"22 July" from the order-confirmation instead of computing that the
dispatch email's "Tuesday" meant 21 July). So the model may instead copy
the weekday name verbatim into a placeholder and let the code resolve it:

    <dow:Tuesday>          → nearest upcoming Tuesday, e.g. "21 июля (вт)"

Resolution is deterministic: the nearest date whose weekday matches,
counting forward from ``now`` (today itself counts if it matches). English,
Russian, and German weekday names (full or common short forms) are
accepted, since the source email may be in any of them. Combines with the
relative label exactly like an absolute date, so a "Tuesday" that lands on
tomorrow renders "завтра, 21 июля (вт)". Unrecognized names are left as-is.

Malformed placeholders (impossible dates like ``<2026-02-31>``,
out-of-range hours, wrong digits) are left untouched so the model sees
its own broken output rather than a silent miscarriage.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Optional


# Order matters: month index = position (1-based)
_MONTHS_RU_GENITIVE = (
    "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
)

_WEEKDAYS_RU_SHORT = ("пн", "вт", "ср", "чт", "пт", "сб", "вс")

# Weekday name → Python weekday index (Mon=0 .. Sun=6). English, Russian, and
# German, full and common short forms, since the source email may be in any of
# them. All keys are lowercase; lookup lowercases its input.
_WEEKDAY_NAMES = {
    # Monday
    "monday": 0, "mon": 0,
    "понедельник": 0, "пн": 0,
    "montag": 0, "mo": 0,
    # Tuesday
    "tuesday": 1, "tue": 1, "tues": 1,
    "вторник": 1, "вт": 1,
    "dienstag": 1, "di": 1,
    # Wednesday
    "wednesday": 2, "wed": 2,
    "среда": 2, "ср": 2,
    "mittwoch": 2, "mi": 2,
    # Thursday
    "thursday": 3, "thu": 3, "thur": 3, "thurs": 3,
    "четверг": 3, "чт": 3,
    "donnerstag": 3, "do": 3,
    # Friday
    "friday": 4, "fri": 4,
    "пятница": 4, "пт": 4,
    "freitag": 4, "fr": 4,
    # Saturday
    "saturday": 5, "sat": 5,
    "суббота": 5, "сб": 5,
    "samstag": 5, "sonnabend": 5, "sa": 5,
    # Sunday
    "sunday": 6, "sun": 6,
    "воскресенье": 6, "вс": 6,
    "sonntag": 6, "so": 6,
}

# Strict: 4-digit year, 2-digit month, 2-digit day, optional HH:MM time.
# Wrapped in angle brackets so it never collides with markdown / HTML.
_ISO_DATE_RE = re.compile(
    r"<(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2}))?>"
)

# Weekday-name placeholder: <dow:Tuesday>. The name is captured loosely and
# validated against _WEEKDAY_NAMES so unrecognized text is left untouched.
_DOW_RE = re.compile(r"<dow:\s*([^\s>]+)\s*>", re.IGNORECASE)


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

    def _format(event: date, time_part: str = "") -> str:
        """Localized 'сегодня, 21 июля (вт), 19:00' for an absolute date."""
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

    def _replace_iso(match: re.Match) -> str:
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

        return _format(event, time_part)

    def _replace_dow(match: re.Match) -> str:
        target = _WEEKDAY_NAMES.get(match.group(1).strip().lower())
        if target is None:
            # Unrecognized weekday name — leave it visible.
            return match.group(0)
        # Nearest date whose weekday matches, counting forward from today
        # (today itself counts if it already is that weekday).
        days_ahead = (target - today.weekday()) % 7
        return _format(today + timedelta(days=days_ahead))

    text = _ISO_DATE_RE.sub(_replace_iso, text)
    text = _DOW_RE.sub(_replace_dow, text)
    return text
