"""Agent records and transcript items: ``session_agents`` and
``session_agent_events`` (see ``migrations/v058_session_agents.py``).

The engine's agent registry writes behind (``nerve/agent/agents/writer.py``):
one transaction per session per flush, records first (the items' foreign key
needs the row), then items. Reads serve the REST routes and the registry's
lazy load of a session.
"""

from __future__ import annotations

import json
from typing import Any

# Every session_agents column, in table order (mirrors
# nerve.agent.agents.model.ROW_COLUMNS, which builds the rows).
_AGENT_COLUMNS: tuple[str, ...] = (
    "id", "session_id", "backend", "kind", "task_id", "task_type",
    "spawn_tool_use_id", "route_ids", "parent_agent_id", "depth", "name",
    "description", "agent_type", "prompt", "command", "requested_model",
    "models", "model_changed_at", "background", "ambient", "status",
    "end_reason", "error", "activity", "summary", "result_text", "output_file",
    "tokens", "tool_uses", "usage", "cost_usd", "runs", "workflow",
    "created_at", "started_at", "run_started_at", "last_activity_at",
    "ended_at", "stop_requested_at", "stop_scope", "stopped_by_actor_id",
    "stopped_caused_by_actor_id", "orchestrator_notified_at", "rev",
    "updated_at",
)
_JSON_COLUMNS = frozenset({"route_ids", "models", "usage", "workflow"})
# JSON columns that are NOT NULL in the schema, with their empty value.
_JSON_DEFAULTS = {"route_ids": "[]", "models": "[]", "usage": "{}"}
# Kept on conflict: a record's identity and birth never change.
_IMMUTABLE = frozenset({"id", "session_id", "created_at"})
_LIVE_SQL = "('starting', 'running', 'stopping')"

_UPSERT_SQL = (
    f"INSERT INTO session_agents ({', '.join(_AGENT_COLUMNS)}) "
    f"VALUES ({', '.join('?' for _ in _AGENT_COLUMNS)}) "
    "ON CONFLICT(id) DO UPDATE SET "
    + ", ".join(
        f"{c} = excluded.{c}" for c in _AGENT_COLUMNS if c not in _IMMUTABLE
    )
)
_INSERT_EVENT_SQL = (
    "INSERT OR IGNORE INTO session_agent_events "
    "(agent_id, session_id, seq, kind, payload, created_at) "
    "VALUES (?, ?, ?, ?, ?, ?)"
)


def _encode(column: str, value: Any) -> Any:
    if column in _JSON_COLUMNS:
        if value is None:
            return _JSON_DEFAULTS.get(column)
        if isinstance(value, (str, bytes)):
            return value
        return json.dumps(value, ensure_ascii=False, default=str)
    if isinstance(value, bool):
        return int(value)
    return value


def _agent_params(row: dict) -> tuple:
    return tuple(_encode(c, row.get(c)) for c in _AGENT_COLUMNS)


def _decode_agent(row: Any) -> dict:
    out = dict(row)
    for column in _JSON_COLUMNS:
        value = out.get(column)
        if isinstance(value, (str, bytes)):
            try:
                out[column] = json.loads(value)
            except (TypeError, ValueError):
                out[column] = None if column == "workflow" else json.loads(
                    _JSON_DEFAULTS[column],
                )
    for column in ("background", "ambient"):
        if column in out and out[column] is not None:
            out[column] = bool(out[column])
    return out


def _decode_event(row: Any) -> dict:
    """A transcript item: the stored payload with ``seq``/``kind``/``at``
    taken from their columns."""
    try:
        item = json.loads(row["payload"])
    except (TypeError, ValueError):
        item = {}
    if not isinstance(item, dict):
        item = {"value": item}
    item["seq"] = int(row["seq"])
    item["kind"] = row["kind"]
    item.setdefault("at", row["created_at"])
    return item


class AgentStore:
    """Mixin: agent records and transcript items."""

    # -- writes -------------------------------------------------------------

    async def upsert_session_agents(self, rows: list[dict]) -> None:
        """Insert or update records (every column but id, session_id and
        created_at is replaced on conflict)."""
        if not rows:
            return
        async with self._atomic():
            await self.db.executemany(_UPSERT_SQL, [_agent_params(r) for r in rows])

    async def insert_agent_events(
        self, rows: list[tuple[str, str, int, str, str, str]],
    ) -> None:
        """Append transcript items: ``(agent_id, session_id, seq, kind,
        payload_json, created_at)``. Idempotent on retry (INSERT OR IGNORE on
        ``(agent_id, seq)``)."""
        if not rows:
            return
        async with self._atomic():
            await self.db.executemany(_INSERT_EVENT_SQL, rows)

    async def write_agent_batch(
        self, rows: list[dict], events: list[tuple[str, str, int, str, str, str]],
        *, delete_ids: list[str] | tuple[str, ...] = (),
    ) -> None:
        """One write-behind flush for one session, in one transaction:
        records folded into another one deleted first (with their items;
        their task id moves to the surviving row), then records (the items
        reference them), then items."""
        if not rows and not events and not delete_ids:
            return
        async with self._atomic():
            if delete_ids:
                marks = ", ".join("?" for _ in delete_ids)
                await self.db.execute(
                    f"DELETE FROM session_agent_events WHERE agent_id IN ({marks})",
                    tuple(delete_ids),
                )
                await self.db.execute(
                    f"DELETE FROM session_agents WHERE id IN ({marks})",
                    tuple(delete_ids),
                )
            if rows:
                await self.db.executemany(
                    _UPSERT_SQL, [_agent_params(r) for r in rows],
                )
            if events:
                await self.db.executemany(_INSERT_EVENT_SQL, events)

    async def mark_live_agents_interrupted(
        self, *, reason: str, now: str, rev: int, session_id: str | None = None,
    ) -> int:
        """Settle every live record (of one session, or all) as
        ``interrupted`` with ``reason``, appending an ``interrupted``
        lifecycle item to each transcript. Returns the number of records.

        Used at boot for rows a crash left live: their runtime is gone.
        """
        where = f"status IN {_LIVE_SQL}"
        params: list[Any] = []
        if session_id is not None:
            where += " AND session_id = ?"
            params.append(session_id)
        async with self._atomic():
            async with self.db.execute(
                f"SELECT id, session_id, runs FROM session_agents WHERE {where}",
                params,
            ) as cursor:
                live = [dict(r) for r in await cursor.fetchall()]
            if not live:
                return 0
            for rec in live:
                async with self.db.execute(
                    "SELECT COALESCE(MAX(seq), 0) FROM session_agent_events"
                    " WHERE agent_id = ?",
                    (rec["id"],),
                ) as cursor:
                    seq = int((await cursor.fetchone())[0]) + 1
                item = {
                    "seq": seq, "at": now, "kind": "lifecycle",
                    "event": "interrupted", "run": int(rec["runs"] or 1),
                    "reason": reason, "actor_id": None,
                }
                await self.db.execute(
                    _INSERT_EVENT_SQL,
                    (rec["id"], rec["session_id"], seq, "lifecycle",
                     json.dumps(item), now),
                )
            await self.db.execute(
                "UPDATE session_agents SET status = 'interrupted',"
                " end_reason = ?, ended_at = ?, rev = MAX(rev + 1, ?),"
                f" updated_at = ? WHERE {where}",
                [reason, now, rev, now, *params],
            )
        return len(live)

    async def mark_agents_notified(self, agent_ids: list[str], now: str) -> None:
        """Record that the orchestrator was told about these records."""
        if not agent_ids:
            return
        marks = ", ".join("?" for _ in agent_ids)
        await self._write(
            "UPDATE session_agents SET orchestrator_notified_at = ?,"
            f" updated_at = ? WHERE id IN ({marks})",
            (now, now, *agent_ids),
        )

    # -- reads --------------------------------------------------------------

    async def list_session_agents(
        self, session_id: str, *, limit: int = 500,
    ) -> list[dict]:
        """The session's records, oldest first: every live record plus the
        newest ``limit`` (JSON columns decoded)."""
        async with self.db.execute(
            "SELECT * FROM session_agents WHERE session_id = ? AND ("
            f" status IN {_LIVE_SQL} OR id IN ("
            "  SELECT id FROM session_agents WHERE session_id = ?"
            "  ORDER BY created_at DESC, rowid DESC LIMIT ?))"
            " ORDER BY created_at ASC, rowid ASC",
            (session_id, session_id, max(0, int(limit))),
        ) as cursor:
            return [_decode_agent(r) for r in await cursor.fetchall()]

    async def get_session_agent(self, agent_id: str) -> dict | None:
        async with self.db.execute(
            "SELECT * FROM session_agents WHERE id = ?", (agent_id,),
        ) as cursor:
            row = await cursor.fetchone()
        return _decode_agent(row) if row else None

    async def max_agent_event_seqs(self, agent_ids: list[str]) -> dict[str, int]:
        """The highest stored ``seq`` per agent (agents with none are absent)."""
        out: dict[str, int] = {}
        ids = [a for a in agent_ids if a]
        # Chunked: SQLite caps the number of bound parameters.
        for start in range(0, len(ids), 500):
            chunk = ids[start:start + 500]
            marks = ", ".join("?" for _ in chunk)
            async with self.db.execute(
                "SELECT agent_id, MAX(seq) FROM session_agent_events"
                f" WHERE agent_id IN ({marks}) GROUP BY agent_id",
                chunk,
            ) as cursor:
                for row in await cursor.fetchall():
                    out[row[0]] = int(row[1])
        return out

    async def list_agent_events(
        self, agent_id: str, *, before_seq: int | None = None,
        after_seq: int | None = None, limit: int = 200,
    ) -> list[dict]:
        """Transcript items of one agent, always in ascending ``seq``.

        No cursor: the last ``limit`` items. ``before_seq``: the ``limit``
        items just below it. ``after_seq``: the first ``limit`` items above
        it. Each item is the stored payload (decoded).
        """
        limit = max(0, int(limit))
        if after_seq is not None:
            sql = (
                "SELECT seq, kind, payload, created_at FROM session_agent_events"
                " WHERE agent_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?"
            )
            params: tuple = (agent_id, int(after_seq), limit)
            ascending = True
        elif before_seq is not None:
            sql = (
                "SELECT seq, kind, payload, created_at FROM session_agent_events"
                " WHERE agent_id = ? AND seq < ? ORDER BY seq DESC LIMIT ?"
            )
            params = (agent_id, int(before_seq), limit)
            ascending = False
        else:
            sql = (
                "SELECT seq, kind, payload, created_at FROM session_agent_events"
                " WHERE agent_id = ? ORDER BY seq DESC LIMIT ?"
            )
            params = (agent_id, limit)
            ascending = False
        async with self.db.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
        items = [_decode_event(r) for r in rows]
        return items if ascending else items[::-1]
