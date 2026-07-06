"""MCP server data access methods."""

from __future__ import annotations

from datetime import datetime, timezone


class McpStore:
    """Mixin providing MCP server registry and tool usage operations."""

    async def upsert_mcp_server(
        self,
        name: str,
        server_type: str,
        enabled: bool = True,
        tool_count: int = 0,
    ) -> None:
        """Register or update an MCP server entry."""
        now = datetime.now(timezone.utc).isoformat()
        await self._write(
            """INSERT INTO mcp_servers (name, type, enabled, tool_count, first_seen_at, last_seen_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(name) DO UPDATE SET
                   type = excluded.type,
                   enabled = excluded.enabled,
                   tool_count = CASE WHEN excluded.tool_count > 0
                                     THEN excluded.tool_count
                                     ELSE mcp_servers.tool_count END,
                   last_seen_at = excluded.last_seen_at""",
            (name, server_type, enabled, tool_count, now, now),
        )

    async def record_mcp_tool_usage(
        self,
        server_name: str,
        tool_name: str,
        session_id: str | None = None,
        duration_ms: int | None = None,
        success: bool = True,
        error: str | None = None,
    ) -> None:
        """Log an MCP tool invocation."""
        await self._write(
            """INSERT INTO mcp_tool_usage
               (server_name, tool_name, session_id, duration_ms, success, error)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (server_name, tool_name, session_id, duration_ms, success, error),
        )

    async def get_mcp_server_stats(self) -> list[dict]:
        """List all MCP servers with aggregated usage stats."""
        async with self.db.execute("""
            SELECT s.*,
                   COALESCE(u.total_invocations, 0) as total_invocations,
                   COALESCE(u.success_count, 0) as success_count,
                   u.avg_duration_ms,
                   u.last_used
            FROM mcp_servers s
            LEFT JOIN (
                SELECT server_name,
                       COUNT(*) as total_invocations,
                       SUM(CASE WHEN success THEN 1 ELSE 0 END) as success_count,
                       ROUND(AVG(duration_ms), 0) as avg_duration_ms,
                       MAX(created_at) as last_used
                FROM mcp_tool_usage
                GROUP BY server_name
            ) u ON s.name = u.server_name
            ORDER BY s.name
        """) as cursor:
            return [dict(row) async for row in cursor]

    async def get_mcp_tool_breakdown(self, server_name: str) -> list[dict]:
        """Get per-tool usage breakdown for a server."""
        async with self.db.execute("""
            SELECT tool_name,
                   COUNT(*) as invocations,
                   SUM(CASE WHEN success THEN 1 ELSE 0 END) as success_count,
                   ROUND(AVG(duration_ms), 0) as avg_duration_ms,
                   MAX(created_at) as last_used
            FROM mcp_tool_usage
            WHERE server_name = ?
            GROUP BY tool_name
            ORDER BY invocations DESC
        """, (server_name,)) as cursor:
            return [dict(row) async for row in cursor]

    async def get_mcp_server_usage(
        self, server_name: str, limit: int = 50,
    ) -> list[dict]:
        """Get recent usage history for a specific MCP server."""
        async with self.db.execute(
            """SELECT * FROM mcp_tool_usage
               WHERE server_name = ?
               ORDER BY created_at DESC LIMIT ?""",
            (server_name, limit),
        ) as cursor:
            return [dict(row) async for row in cursor]
