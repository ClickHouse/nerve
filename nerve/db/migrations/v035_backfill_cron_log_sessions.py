"""V35: Backfill cron_logs.session_id for runs recorded before v34.

V34 added the column but only new runs populate it. Historical rows can be
recovered because isolated cron sessions encode their run timestamp in the
id (``cron:<job>:<YYYYMMDD-HHMMSS>``, UTC) which lands within seconds of the
log row's ``started_at``. For each NULL row, the closest per-run session
within a tolerance window wins; if none matches but a persistent session
(``cron:<job>``) exists, that one is used. Source-runner logs have no agent
session and stay NULL.

Best-effort: rows that can't be matched are left untouched.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

import aiosqlite

# A per-run session may start slightly before or after the log row is
# inserted (catch-up runs, slow event loops). Two minutes is generous while
# still avoiding cross-run mismatches for jobs that fire at most per-minute.
_TOLERANCE_SECONDS = 120

_RUN_ID_RE = re.compile(r"^(?P<job>.+):(?P<ts>\d{8}-\d{6})$")


def _parse_log_ts(value: str) -> datetime | None:
    """Parse cron_logs.started_at (SQLite CURRENT_TIMESTAMP or ISO, UTC)."""
    if not value:
        return None
    ts = value.replace(" ", "T")
    try:
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


async def up(db: aiosqlite.Connection) -> None:
    # Index cron sessions: job -> persistent id / [(run_time, session_id)]
    persistent: dict[str, str] = {}
    runs: dict[str, list[tuple[datetime, str]]] = {}
    async with db.execute(
        "SELECT id FROM sessions WHERE id LIKE 'cron:%'"
    ) as cursor:
        async for row in cursor:
            session_id = row[0]
            rest = session_id[len("cron:"):]
            m = _RUN_ID_RE.match(rest)
            if m:
                try:
                    run_time = datetime.strptime(
                        m.group("ts"), "%Y%m%d-%H%M%S",
                    ).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
                runs.setdefault(m.group("job"), []).append((run_time, session_id))
            else:
                persistent[rest] = session_id

    if not persistent and not runs:
        return

    async with db.execute(
        "SELECT id, job_id, started_at FROM cron_logs WHERE session_id IS NULL"
    ) as cursor:
        logs = [(row[0], row[1], row[2]) async for row in cursor]

    updates: list[tuple[str, int]] = []
    for log_id, job_id, started_at in logs:
        log_time = _parse_log_ts(started_at)
        best: str | None = None
        if log_time is not None:
            candidates = runs.get(job_id, [])
            best_delta = float(_TOLERANCE_SECONDS)
            for run_time, session_id in candidates:
                delta = abs((run_time - log_time).total_seconds())
                if delta <= best_delta:
                    best_delta = delta
                    best = session_id
        if best is None:
            best = persistent.get(job_id)
        if best is not None:
            updates.append((best, log_id))

    for session_id, log_id in updates:
        await db.execute(
            "UPDATE cron_logs SET session_id = ? WHERE id = ?",
            (session_id, log_id),
        )
