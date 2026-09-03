"""Channel observation buffer — the push-to-pull join for the sources layer.

A channel appends here on its dispatch path; a
:class:`~nerve.sources.channel.ChannelSource` drains it on the source
runner's cadence. See ``v046_channel_observations`` for why the id is
``AUTOINCREMENT`` and the payload is JSON.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

# A busy transport must not fill the disk between drains. Past this many rows
# for one transport, the oldest are dropped — losing the stale end of a
# backlog nobody drained beats losing the daemon. Trimming is amortized
# (see _TRIM_EVERY) so the dispatch path stays a single INSERT.
DEFAULT_MAX_ROWS = 10_000
_TRIM_EVERY = 100


class ObservationStore:
    """Mixin for the ``channel_observations`` buffer."""

    @property
    def _observation_writes(self) -> dict[str, int]:
        """Transport name -> inserts since that transport's last trim check.

        Built on first use: mixins here have no ``__init__``, and a class
        attribute would share one counter across every Database instance.
        """
        counts = self.__dict__.get("_observation_write_counts")
        if counts is None:
            counts = self.__dict__["_observation_write_counts"] = {}
        return counts

    # Longest a buffered message may be before it is truncated. Slack caps
    # posts near 40k and Telegram near 4k, so this only bites on a pathological
    # sender — but the cap is per row and the row count is capped separately,
    # so without it the two together still bound nothing in bytes.
    MAX_OBSERVED_TEXT = 16_000

    async def insert_channel_observation(
        self,
        channel: str,
        channel_key: str,
        payload: dict[str, Any],
        ttl_days: int = 7,
        max_rows: int = DEFAULT_MAX_ROWS,
    ) -> int:
        """Append one observation. Returns its id.

        This runs on the channel's dispatch path, so it is one INSERT and
        nothing else. The row cap is enforced once every ``_TRIM_EVERY``
        inserts rather than on each one; overshooting the cap by under a
        hundred rows is cheaper than a COUNT per message.
        """
        text = payload.get("text")
        if isinstance(text, str) and len(text) > self.MAX_OBSERVED_TEXT:
            payload = {
                **payload,
                "text": text[: self.MAX_OBSERVED_TEXT],
                "truncated": True,
            }

        now = datetime.now(timezone.utc)
        result = await self._write(
            "INSERT INTO channel_observations "
            "(channel, channel_key, payload, created_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                channel,
                channel_key,
                json.dumps(payload),
                now.isoformat(),
                (now + timedelta(days=ttl_days)).isoformat(),
            ),
        )

        # Start at the threshold rather than zero, so the first insert after
        # a restart trims. A purely process-local counter never fires on a
        # daemon that restarts more often than it writes _TRIM_EVERY rows,
        # and the cap silently stops existing.
        seen = self._observation_writes.get(channel, _TRIM_EVERY) + 1
        if seen >= _TRIM_EVERY:
            self._observation_writes[channel] = 0
            await self._trim_channel_observations(channel, max_rows)
        else:
            self._observation_writes[channel] = seen

        return result.lastrowid or 0

    async def _trim_channel_observations(self, channel: str, max_rows: int) -> None:
        """Drop the oldest rows for one transport past ``max_rows``."""
        result = await self._write(
            "DELETE FROM channel_observations WHERE channel = ? AND id <= ("
            "  SELECT id FROM channel_observations WHERE channel = ?"
            "  ORDER BY id DESC LIMIT 1 OFFSET ?"
            ")",
            (channel, channel, max_rows),
        )
        if result.rowcount:
            logger.warning(
                "Transport %s observation buffer exceeded its %d-row target — "
                "dropped %d of the oldest rows. The drain is behind or not "
                "scheduled.",
                channel, max_rows, result.rowcount,
            )

    async def read_channel_observations(
        self, channel: str, after_id: int = 0, limit: int = 50,
    ) -> list[tuple[int, dict[str, Any] | None]]:
        """Buffered rows for one transport past ``after_id``, oldest first.

        Every scanned row comes back as ``(id, payload)``, with ``payload``
        None where the JSON would not parse. Dropping those rows here
        instead would hide their ids from the caller, and a batch of
        entirely unreadable rows would then pin the cursor in place and be
        re-read on every run, with everything behind them unreachable.
        Reporting them lets the drain skip the row and still move past it.
        """
        rows: list[tuple[int, dict[str, Any] | None]] = []
        async with self.db.execute(
            "SELECT id, payload FROM channel_observations "
            "WHERE channel = ? AND id > ? ORDER BY id LIMIT ?",
            (channel, after_id, limit),
        ) as cursor:
            async for row in cursor:
                try:
                    rows.append((row[0], json.loads(row[1])))
                except (ValueError, TypeError) as e:
                    logger.warning(
                        "Skipping unreadable observation %s on %s: %s",
                        row[0], channel, e,
                    )
                    rows.append((row[0], None))
        return rows

    async def get_channel_observation_max_id(self, channel: str) -> int:
        """Highest id buffered for one transport, or 0 if none."""
        async with self.db.execute(
            "SELECT COALESCE(MAX(id), 0) FROM channel_observations WHERE channel = ?",
            (channel,),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def cleanup_expired_channel_observations(self) -> int:
        """Delete observations past their TTL. Returns count deleted."""
        now = datetime.now(timezone.utc).isoformat()
        result = await self._write(
            "DELETE FROM channel_observations WHERE expires_at < ?", (now,),
        )
        return result.rowcount or 0
