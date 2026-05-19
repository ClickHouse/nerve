# Codex rollout format reference

This document captures the on-disk rollout schema as inspected on
Codex 0.130.0 (Pi, May 2026) so future maintainers don't need to
re-derive it from a live install.

Rollouts live at:

```
~/.codex/sessions/YYYY/MM/DD/rollout-<ISO-ts>-<thread-uuid>.jsonl
~/.codex/archived_sessions/rollout-<ISO-ts>-<thread-uuid>.jsonl
```

Each line is a JSON object with `timestamp` (UTC ISO 8601 ending in
`Z`), `type` (outer), and `payload`.

## Outer type matrix

| Outer type      | Notes |
|-----------------|-------|
| `session_meta`  | First line. Carries `id` (thread UUID), `cwd`, `originator`, `source`, `cli_version`, `model_provider`, `base_instructions.text`. |
| `turn_context`  | Per turn. `turn_id`, `cwd` (may drift mid-session), `approval_policy`, `sandbox_policy`, `model`, etc. |
| `response_item` | The wire format Codex exchanges with OpenAI — see below. |
| `event_msg`     | Codex's deduplicated UX view. Preferred for transcripts. |

## `response_item` inner types

| `payload.type`           | `payload.role` | Handled as |
|--------------------------|----------------|------------|
| `message`                | `user`         | Skip — Codex auto-injection of AGENTS.md. |
| `message`                | `developer`    | Skip — sandbox / skill instructions. |
| `message`                | `assistant`    | Skip — `event_msg/agent_message` is the canonical form. |
| `reasoning`              | (n/a)          | Translated to a thinking placeholder; `encrypted_content` preserved in metadata. |
| `function_call`          | (n/a)          | Tool call. `call_id`, `name`, JSON-string `arguments`, optional `namespace: "mcp__nerve__"`. |
| `function_call_output`   | (n/a)          | Tool result. `call_id`, `output` (raw text, may start with `Chunk ID: ...\nWall time: ...\nOutput:\n`). |

## `event_msg` inner types

| `payload.type`           | Handled as |
|--------------------------|------------|
| `task_started`           | `turn_started` event (no message). |
| `task_complete`          | `turn_completed` event (no message). |
| `user_message`           | **Canonical user input.** Has `message`, `images`, `local_images`, `text_elements`. |
| `agent_message`          | **Canonical agent reply.** Has `message`, `phase` (`final_answer` etc.), `memory_citation`. |
| `mcp_tool_call_begin`    | Tool call (preferred over `function_call` for MCP-routed tools). |
| `mcp_tool_call_end`      | Tool result. Has `call_id`, `invocation: {server, tool, arguments}`, `result: {Ok|Err}`. |
| `token_count`            | Skip — usage telemetry, not transcript. |

## Encrypted reasoning

Codex never exposes plaintext reasoning. The `encrypted_content`
field is an OpenAI-encrypted blob the platform can replay back if
needed. Nerve stores it in the message's metadata so a future Codex
version with a decrypt API could materialise the plaintext after the
fact.

## Workspace filter

`session_meta.payload.cwd` is the first-line cwd. The sync source
reads only that line to decide whether the rest of the file is in
scope — out-of-scope rollouts cost one `readline()` plus a JSON parse.

`turn_context.payload.cwd` can change mid-session if the agent `cd`s
elsewhere. The sync source filters on `session_meta.cwd` only; mid-
session drift doesn't unstick an in-scope thread.

## Idempotency keys

Every translated message carries a deterministic `external_id`:

* `msg:<event_id_or_thread:seq>` — user / assistant messages
* `reasoning:<thread>:<seq>` — encrypted reasoning blobs
* `tool_call:<call_id>` — tool calls
* `tool_result:<call_id>` — tool results

The partial unique index `idx_messages_external_id` (migration v028)
on `(session_id, external_id)` ensures the rollout sync and the
external MCP server's tool-call audit cannot create duplicates for
the same logical item.
