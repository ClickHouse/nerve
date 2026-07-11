# Codex thread sync

Nerve can pull Codex thread transcripts into its own message store so
Codex sessions become first-class citizens — searchable via
`memory_recall`, processed by the memory sweep, rendered in the
satellite-session UI, and deduped against the external MCP server's
tool-call log. Without this sync, Nerve only sees the *tool calls*
Codex makes, not the user prompts and agent replies that frame them.

The pipeline is:

```
~/.codex/sessions/.../rollout-*.jsonl
        │
        ▼
LocalRolloutOrigin (tail with poll)
        │
        ▼
parser  → translator  → CodexIngester  ──▶  Nerve `messages` + `sessions`
                                              │
                                              ▼
                                       broadcaster (live UI)
```

Migration v039 maintains a `session_native_threads` mapping. A thread created
by Nerve is ingested back into that same Nerve session; only otherwise-unknown
threads fall back to a satellite session id'd `codex:<thread_uuid>`. Tool calls
and transcript messages therefore share one row without relying on optional
client metadata.

## Enabling

```yaml
# config.yaml
sync:
  codex:
    enabled: true
    workspace_filter:
      mode: nerve_workspace   # only threads whose cwd matches Nerve's workspace
      # alternative modes:
      # mode: explicit
      # explicit_paths:
      #   - /home/alice/nerve-workspace
      #   - /home/alice/other-project
      # mode: any              # sync every thread (not recommended)
    origins:
      - id: local
        type: local_rollout
        path: ~/.codex/sessions
        archive_path: ~/.codex/archived_sessions
        poll_interval_seconds: 2.0
    store_encrypted_reasoning: true  # keep the encrypted blob in metadata
```

`local_rollout` is the only origin implemented today. `app_server` and
`cloud` are scaffolded — enabling them yields a clear configuration
error rather than silently doing nothing.

## How the workspace filter works

Every Codex rollout starts with a `session_meta` line carrying the
thread's `cwd`. The filter reads ONLY that first line; out-of-scope
threads cost a single `readline()` and skip the rest of the file. The
decision is cached per file in the cursor so we never re-evaluate.

`mode: nerve_workspace` (default) matches when `cwd` equals
`config.workspace` after symlink resolution. Mid-session `cd` away from
the workspace doesn't unstick a thread — once in scope, always in
scope. Threads that *start* outside Nerve's workspace and later `cd`
in are not picked up (rare in practice; can be added later).

## What gets ingested

| Codex item                              | Nerve message |
|----------------------------------------|----------------|
| `event_msg/user_message`                | user role + text block |
| `event_msg/agent_message`               | assistant role + text block |
| `response_item/reasoning`               | thinking placeholder; encrypted blob kept in metadata |
| `response_item/function_call`           | assistant role + `tool_call` block |
| `response_item/function_call_output`    | tool role + `tool_result` block (`exec_command` header stripped) |
| `event_msg/mcp_tool_call_begin/end`     | structured `tool_call` + `tool_result` (preferred over raw `function_call`) |
| `response_item/message/developer`       | skipped (Codex sandbox/skill instructions) |
| `response_item/message/user` (AGENTS.md) | skipped (auto-injection, not real input) |
| `event_msg/token_count`                 | skipped (usage telemetry, not transcript) |

Each translated message carries a deterministic `external_id`:

* `msg:<event_id_or_seq>` for user/assistant messages
* `reasoning:<thread>:<seq>` for encrypted reasoning blocks
* `tool_call:<call_id>` and `tool_result:<call_id>` for tool flows

The partial unique index `(session_id, external_id)` (migration v028)
drops duplicates whether the same item arrives via the MCP server or
the rollout sync.

## Cursor persistence

Each origin stores its cursor in `sync_cursors` keyed
`codex:<origin_id>`. The cursor is a JSON blob recording every known
file's byte offset plus the in-scope / out-of-scope / archived
decisions. Restart Nerve and an origin resumes exactly where it left
off — partial trailing lines (Codex still writing) are retried until
the newline is flushed.

## Diagnostics

`GET /api/diagnostics` now includes `codex_thread_sync`:

```json
{
  "codex_thread_sync": {
    "started": true,
    "origins": [
      {
        "origin_id": "local",
        "running": true,
        "cancelled": false,
        "error": null,
        "stats": {
          "messages_inserted": 47,
          "messages_skipped_duplicate": 0,
          "messages_skipped_oos": 0,
          "threads_in_scope": 3,
          "threads_out_of_scope": 0,
          "threads_archived": 0
        }
      }
    ]
  }
}
```

`null` when the feature is disabled.

## Convergence with the external MCP server

Backend-managed Codex gets a session-bound MCP token, so its tool calls are
attributed directly to the real Nerve session. Once app-server returns the
thread ID, Nerve binds `(codex, thread_id)` to that session. Rollout ingestion
looks up this mapping before creating anything, and external threads bind their
fallback satellite on first sight. The `(session_id, external_id)` unique index
then deduplicates the two ingestion paths. Archiving an external rollout only
archives satellite sessions; it never changes the lifecycle of a Nerve-owned
chat.
