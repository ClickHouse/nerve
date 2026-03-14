# API Reference

## REST API

All endpoints require JWT authentication via `Authorization: Bearer <token>` header or `nerve_token` cookie.

### Auth

#### `POST /api/auth/login`
Login with password, receive JWT.

```json
Request:  { "password": "..." }
Response: { "token": "eyJ..." }
```

#### `GET /api/auth/check`
Verify current authentication.

```json
Response: { "authenticated": true }
```

### Sessions

#### `GET /api/sessions`
List all sessions, ordered by most recently updated.

```json
Response: { "sessions": [{ "id": "main", "title": "Main", "source": "system", "updated_at": "..." }] }
```

#### `POST /api/sessions`
Create a new session.

```json
Request:  { "title": "My Session" }
Response: { "id": "a1b2c3d4", "title": "My Session", "source": "web" }
```

#### `GET /api/sessions/{id}`
Get session details.

#### `GET /api/sessions/{id}/messages?limit=100`
Get messages for a session.

```json
Response: { "messages": [{ "id": 1, "role": "user", "content": "...", "channel": "web", "created_at": "..." }] }
```

#### `DELETE /api/sessions/{id}`
Delete a session (cannot delete "main"). Disconnects any active SDK client before deletion.

#### `GET /api/sessions/{id}/status`
Session status with lifecycle info.

```json
Response: {
  "session_id": "a1b2c3d4",
  "status": "active",
  "is_running": true,
  "sdk_session_id": "8fbba4a4-...",
  "connected_at": "2026-02-27T12:00:00+00:00",
  "parent_session_id": null,
  "message_count": 42,
  "total_cost_usd": 0.0
}
```

#### `POST /api/sessions/fork`
Fork a session, optionally from a specific message point.

```json
Request:  { "source_session_id": "main", "at_message_id": "msg-42", "title": "My Fork" }
Response: { "id": "fork-a1b2c3d4", "title": "My Fork", "source": "web", "status": "created", "parent_session_id": "main" }
```

#### `POST /api/sessions/{id}/resume`
Resume a stopped or idle session (must have a stored `sdk_session_id`).

```json
Response: { "id": "a1b2c3d4", "status": "created", "sdk_session_id": "..." }
```

#### `POST /api/sessions/{id}/archive`
Archive a session (soft delete, cannot archive "main"). Disconnects any active SDK client.

```json
Response: { "archived": true }
```

#### `GET /api/sessions/{id}/events?limit=50`
Get session lifecycle event log (newest first).

```json
Response: {
  "events": [
    { "id": 3, "session_id": "abc", "event_type": "idle", "details": { "resumable": true }, "created_at": "..." },
    { "id": 2, "session_id": "abc", "event_type": "started", "details": { "sdk_session_id": "..." }, "created_at": "..." },
    { "id": 1, "session_id": "abc", "event_type": "created", "details": { "source": "web" }, "created_at": "..." }
  ]
}
```

### Modified Files

#### `GET /api/sessions/{id}/modified-files`
List files modified during a session with diff stats. Reads from `session_file_snapshots` table and compares against current file content on disk.

```json
Response: {
  "files": [
    { "path": "/home/user/project/foo.py", "short_path": "project/foo.py", "status": "modified", "stats": { "additions": 15, "deletions": 3 }, "created_at": "2026-03-04T07:00:00Z" }
  ],
  "summary": { "total_files": 1, "total_additions": 15, "total_deletions": 3 }
}
```

#### `GET /api/sessions/{id}/file-diff?path=...&context=4`
Compute a unified diff for a single file against its session baseline snapshot. Returns structured hunks with line numbers for GitHub PR-style rendering.

```json
Response: {
  "path": "/home/user/project/foo.py",
  "short_path": "project/foo.py",
  "status": "modified",
  "binary": false,
  "stats": { "additions": 15, "deletions": 3 },
  "hunks": [
    {
      "old_start": 10, "old_count": 5, "new_start": 10, "new_count": 7, "header": "class Foo:",
      "lines": [
        { "type": "context", "content": "    def bar(self):", "old_line": 10, "new_line": 10 },
        { "type": "deletion", "content": "        return None", "old_line": 11 },
        { "type": "addition", "content": "        return 42", "new_line": 11 }
      ]
    }
  ],
  "truncated": false
}
```

### Chat

#### `POST /api/chat`
Send a message and get the complete response (non-streaming).

```json
Request:  { "message": "Hello", "session_id": "main" }
Response: { "response": "Hi there!", "session_id": "main" }
```

For streaming, use the WebSocket endpoint.

### Tasks

#### `GET /api/tasks?status=pending`
List tasks with optional status filter. Valid statuses: `pending`, `in_progress`, `done`, `deferred`, or empty (all non-done).

#### `GET /api/tasks/search?q=keyword&status=`
Full-text search on task titles and content (FTS5). Optional status filter.

```json
Response: { "tasks": [{ "id": "2026-03-01-fix-bug", "title": "Fix bug", "status": "pending", ... }] }
```

#### `POST /api/tasks`
Create a task.

```json
Request: { "title": "Fix bug", "content": "Details...", "deadline": "2026-03-01" }
```

#### `GET /api/tasks/{id}`
Get task details including full markdown file content.

```json
Response: { "id": "2026-03-01-fix-bug", "title": "Fix bug", "status": "pending", "content": "# Fix bug\n\n...", ... }
```

#### `PATCH /api/tasks/{id}`
Update a task. All fields are optional. `content` replaces the full markdown file; title and deadline are re-synced to SQLite.

```json
Request: { "status": "done", "note": "Fixed in PR #123" }
Request: { "content": "# Updated Title\n\n**Deadline:** 2026-03-15\n\nNew details..." }
```

### Skills

#### `GET /api/skills`
List all skills with aggregated usage statistics.

```json
Response: {
  "skills": [{
    "id": "my-skill", "name": "my-skill", "description": "Query database...",
    "version": "1.0.0", "enabled": true, "total_invocations": 5, "success_count": 5,
    "avg_duration_ms": 12, "last_used": "2026-03-06T21:00:00"
  }]
}
```

#### `GET /api/skills/{id}`
Get full skill content, metadata, references, and usage stats.

#### `POST /api/skills`
Create a new skill.

```json
Request:  { "name": "code-review", "description": "This skill should be used when...", "content": "## Steps\n..." }
Response: { "id": "code-review", "name": "code-review", "created": true }
```

#### `PUT /api/skills/{id}`
Update a skill's SKILL.md content (full raw file including frontmatter).

```json
Request:  { "content": "---\nname: code-review\ndescription: ...\n---\n\n# Instructions\n..." }
Response: { "id": "code-review", "name": "code-review", "updated": true }
```

#### `DELETE /api/skills/{id}`
Delete a skill (removes directory and DB record).

#### `PATCH /api/skills/{id}/toggle`
Enable or disable a skill.

```json
Request:  { "enabled": false }
Response: { "id": "code-review", "enabled": false }
```

#### `GET /api/skills/{id}/usage?limit=50`
Get usage history and aggregate stats for a skill.

#### `GET /api/skills/stats`
Aggregate usage stats across all skills.

#### `POST /api/skills/sync`
Re-scan the `workspace/skills/` directory and sync to DB. Discovers new skills, removes deleted ones, preserves enabled state.

### MCP Servers

#### `GET /api/mcp-servers`
List all MCP servers with aggregated usage statistics.

```json
Response: {
  "servers": [{
    "name": "nerve", "type": "sdk", "enabled": true, "tool_count": 34,
    "total_invocations": 127, "success_count": 125, "avg_duration_ms": null,
    "last_used": "2026-03-14T19:00:00", "first_seen_at": "...", "last_seen_at": "..."
  }]
}
```

#### `GET /api/mcp-servers/{name}`
Get server detail including per-tool breakdown and recent usage.

```json
Response: {
  "name": "nerve", "type": "sdk", ...,
  "tools": [{ "tool_name": "task_list", "invocations": 42, "success_count": 42, "avg_duration_ms": null, "last_used": "..." }],
  "recent_usage": [{ "id": 1, "server_name": "nerve", "tool_name": "task_list", "session_id": "abc", "success": true, "created_at": "..." }]
}
```

#### `GET /api/mcp-servers/{name}/usage?limit=50`
Paginated usage history for a server.

#### `POST /api/mcp-servers/reload`
Re-read MCP server config from YAML files and refresh the in-memory cache. New sessions will use updated config.

```json
Response: { "reloaded": 2, "servers": [...] }
```

### Memory Files

#### `GET /api/memory/files`
List markdown files in workspace.

#### `GET /api/memory/file/{path}`
Read a memory file.

#### `PUT /api/memory/file/{path}`
Write a memory file.

```json
Request: { "content": "# Updated content..." }
```

### memU Semantic Memory

#### `GET /api/memory/memu`
Get memU categories, items, and indexed resources.

```json
Response: {
  "available": true,
  "categories": [{ "id": "...", "name": "preferences", "description": "...", "summary": "..." }],
  "items": [{ "id": "...", "memory_type": "profile", "summary": "User works at Acme Corp", "resource_id": "...", "created_at": "...", "happened_at": "..." }],
  "resources": [{ "id": "...", "url": "/path/to/file.md", "modality": "document", "caption": "...", "created_at": "..." }],
  "category_items": { "category_id": ["item_id_1", "item_id_2"] }
}
```

#### `POST /api/memory/memu/categories`
Create a new category.

```json
Request:  { "name": "travel", "description": "Travel plans and logistics" }
Response: { "name": "travel", "created": true }
```

#### `PATCH /api/memory/memu/categories/{id}`
Update a category's summary or description. Re-embeds the category after update.

```json
Request:  { "summary": "Updated summary text", "description": "New description" }
Response: { "id": "...", "updated": true }
```

#### `PATCH /api/memory/memu/items/{id}`
Update a memory item's content, type, or category assignments.

```json
Request:  { "content": "New text", "memory_type": "knowledge", "categories": ["work"] }
Response: { "id": "...", "updated": true }
```

#### `DELETE /api/memory/memu/items/{id}`
Delete a memory item.

```json
Response: { "id": "...", "deleted": true }
```

#### `GET /api/memory/memu/health`
memU service health metrics and operation stats.

```json
Response: {
  "initialized_at": "...", "service_available": true,
  "operations": { "recall": { "call_count": 5, "avg_duration_s": 0.8, "error_count": 0 }, ... },
  "in_flight": [],
  "database": { "total_items": 2924, "total_categories": 24, "db_size_mb": 132.97, "type_distribution": { "profile": 671, ... } }
}
```

#### `GET /api/memory/memu/audit?action=&target_type=&limit=100&offset=0`
Paginated audit log of memU mutations.

```json
Response: {
  "logs": [{ "id": 1, "timestamp": "...", "action": "item_deleted", "target_type": "item", "target_id": "abc123", "source": "agent_tool", "details": {} }],
  "offset": 0, "limit": 100
}
```

### Diagnostics

#### `GET /api/diagnostics`
System health and status, including task/FTS index health.

```json
Response: {
  "system": { "hostname": "...", "memory_mb": 65.2, "disk_free_gb": 180.5 },
  "tasks": { "total": 92, "active": 16, "done": 76, "fts_indexed": 92, "fts_ok": true },
  "sync": { "github": { "cursor": "...", "last_run": "...", "records_fetched": 3, "records_processed": 3, "error": null } },
  "recent_cron_logs": [...]
}
```

#### `GET /api/cron/logs?job_id=&limit=50`
Get cron job execution logs.

### Health

#### `GET /health`
No auth required.

```json
Response: { "status": "ok", "version": "0.1.0" }
```

## WebSocket Protocol

Connect to `ws[s]://host:port/ws?token=<jwt>`.

### Client → Server

```typescript
// Send a chat message
{ type: "message", content: "Hello", session_id: "main" }

// Stop the running agent
{ type: "stop", session_id: "main" }

// Switch active session
{ type: "switch_session", session_id: "abc123" }

// Fork a session
{ type: "fork", session_id: "main", at_message_id: "msg-42", title: "My Fork" }

// Resume a stopped/idle session
{ type: "resume", session_id: "abc123" }

// Keep-alive
{ type: "ping" }
```

### Server → Client

```typescript
// Streaming token (parent_tool_use_id set when from a sub-agent)
{ type: "token", session_id: "main", content: "Hello", parent_tool_use_id?: "toolu_parent" }

// Extended thinking
{ type: "thinking", session_id: "main", content: "Let me check...", parent_tool_use_id?: "toolu_parent" }

// Tool call started
{ type: "tool_use", session_id: "main", tool: "Read", input: { file_path: "..." }, tool_use_id: "toolu_...", parent_tool_use_id?: "toolu_parent" }

// Tool call result
{ type: "tool_result", session_id: "main", tool_use_id: "toolu_...", result: "...", is_error: false, parent_tool_use_id?: "toolu_parent" }

// Sub-agent started (Task tool invoked)
{ type: "subagent_start", session_id: "main", tool_use_id: "toolu_...", subagent_type: "Explore", description: "find auth", model?: "haiku" }

// Sub-agent completed
{ type: "subagent_complete", session_id: "main", tool_use_id: "toolu_...", duration_ms: 12345, is_error: false }

// Agent turn complete (includes context usage and boundary)
{ type: "done", session_id: "main", usage: { input_tokens: 1234, output_tokens: 567, cache_read_input_tokens: 890, cache_creation_input_tokens: 0 }, max_context_tokens: 1048576, context_boundary: "2026-02-25T10:00:00+00:00" }

// Agent stopped by user
{ type: "stopped", session_id: "main" }

// Error occurred
{ type: "error", session_id: "main", error: "..." }

// Session switch confirmed (includes running state, lifecycle status, buffered events for reconnect)
{ type: "session_status", session_id: "abc123", is_running: true, status: "active", buffered_events: [...] }
{ type: "session_switched", session_id: "abc123" }

// Session title updated (AI-generated)
{ type: "session_updated", session_id: "abc123", title: "Italy Vacation Planning" }

// Session forked
{ type: "session_forked", source_id: "main", fork_id: "fork-a1b2c3d4", title: "My Fork" }

// Session resumed
{ type: "session_resumed", session_id: "abc123" }

// Session archived
{ type: "session_archived", session_id: "abc123" }

// Plan file updated (Write/Edit to .claude/plans/)
{ type: "plan_update", session_id: "main", content: "# Plan\n..." }

// File modified by agent (Edit/Write/NotebookEdit succeeded)
{ type: "file_changed", session_id: "main", path: "/home/user/project/foo.py", operation: "edit", tool_use_id: "toolu_..." }

// Interactive tool waiting for user input (AskUserQuestion, ExitPlanMode, EnterPlanMode)
{ type: "interaction", session_id: "main", interaction_id: "uuid", interaction_type: "question" | "plan_exit" | "plan_enter", tool_name: "AskUserQuestion", tool_input: { ... } }

// Keep-alive response
{ type: "pong" }
```
