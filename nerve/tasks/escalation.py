"""Task escalation — deadline tracking and reminder escalation.

Escalation levels:
  0 = no reminder yet
  1 = soft (at deadline)
  2 = medium (+30 minutes)
  3 = urgent (+2 hours)

Respects quiet hours when they are configured.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from nerve.config import NerveConfig
from nerve.db import Database
from nerve.tasks.models import Task

logger = logging.getLogger(__name__)

# Escalation intervals (from deadline)
ESCALATION_INTERVALS = {
    1: timedelta(minutes=0),     # At deadline
    2: timedelta(minutes=30),    # +30 min
    3: timedelta(hours=2),       # +2 hours
}


def _is_quiet_hour(config: NerveConfig) -> bool:
    """Check if we're in quiet hours.

    Quiet hours are optional: an empty ``quiet_start`` or ``quiet_end`` (a
    worker's default is both) means there are none, so never quiet.
    """
    start = str(config.quiet_start or "").strip()
    end = str(config.quiet_end or "").strip()
    if not start or not end:
        return False
    try:
        tz = ZoneInfo(config.timezone)
        now = datetime.now(tz)
        current_minutes = now.hour * 60 + now.minute

        start_h, start_m = map(int, start.split(":"))
        end_h, end_m = map(int, end.split(":"))
        start_minutes = start_h * 60 + start_m
        end_minutes = end_h * 60 + end_m

        if start_minutes <= end_minutes:
            return start_minutes <= current_minutes < end_minutes
        else:
            return current_minutes >= start_minutes or current_minutes < end_minutes
    except Exception:
        return False


async def check_escalations(
    db: Database,
    config: NerveConfig,
) -> list[dict]:
    """Check all tasks for escalation needs. Returns list of {task, level} for tasks needing reminders."""
    if _is_quiet_hour(config):
        return []

    tasks = await db.list_tasks(status="pending")
    now = datetime.now(timezone.utc)
    escalations = []

    for task_row in tasks:
        deadline_str = task_row.get("deadline")
        if not deadline_str:
            continue

        try:
            deadline = datetime.fromisoformat(deadline_str)
            if deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

        current_level = task_row.get("escalation_level", 0)

        # Check each escalation level
        for level in [1, 2, 3]:
            if level <= current_level:
                continue

            trigger_time = deadline + ESCALATION_INTERVALS[level]
            if now >= trigger_time:
                escalations.append({
                    "task_id": task_row["id"],
                    "title": task_row["title"],
                    "deadline": deadline_str,
                    "level": level,
                })
                break  # Only escalate one level at a time

    return escalations


def format_escalation_message(task_id: str, title: str, deadline: str, level: int) -> str:
    """Format an escalation reminder message."""
    labels = {1: "Reminder", 2: "Follow-up", 3: "URGENT"}
    label = labels.get(level, "Reminder")

    return f"[{label}] Task: {title}\nDeadline: {deadline}\nTask ID: {task_id}"
