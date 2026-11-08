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

from nerve.config import NerveConfig
from nerve.db import Database
from nerve.notifications.quiet_hours import in_quiet_hours
from nerve.tasks.models import Task

logger = logging.getLogger(__name__)

# Escalation intervals (from deadline)
ESCALATION_INTERVALS = {
    1: timedelta(minutes=0),     # At deadline
    2: timedelta(minutes=30),    # +30 min
    3: timedelta(hours=2),       # +2 hours
}


async def check_escalations(
    db: Database,
    config: NerveConfig,
) -> list[dict]:
    """Check all tasks for escalation needs. Returns list of {task, level} for tasks needing reminders."""
    if in_quiet_hours(config):
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
