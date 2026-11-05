# Sub-agents and background tasks

A session's orchestrator (the main agent) can start work that keeps running
on its own: sub-agents (`Agent`/`Task`), workflows (`Workflow`), background
shells (`Bash` with `run_in_background`), monitors (`Monitor`), teammates
and remote agents. Nerve keeps a durable **roster** of that work per session:
one record per agent, with an accurate status, its brief, live progress, a
transcript, tokens and an estimated cost.

Two facts are kept apart and never derived from each other:

| Fact | Meaning | Drives |
|---|---|---|
| **The turn is running** | The orchestrator is producing a turn: a message you sent, or an autonomous turn the runtime started after background work settled | `is_running`, the Stop button, Esc, `session_running` |
| **Agents are live** | At least one agent is `starting`, `running` or `stopping` | `has_background_tasks`, `background_count`, the agent dock, keeping the runtime alive between turns |

Background agents alone never make a session look "running": the composer
stays usable while they work, and messages you send while a turn streams
wait in a visible queue.

Sub-agent traffic never enters the orchestrator's turn. Everything a
sub-agent produces (text, thinking, tool calls and results, model usage)
goes to its own record and transcript; the orchestrator's messages contain
only its own work. The spawning `tool_call` block in the orchestrator's
message carries the record's `agent_id`.

## Records

Every record has a public id (`ag_` + 12 hex, for example
`ag_3f9a1c2b7d4e`) from the moment its spawning tool call appears; the
runtime's task id binds later. The runtime reports a background task before
the spawning call's result, often without the call's id; that result names
the task, so every frame of the task lands on the one record the call
created (a task no call claims within 10 seconds gets a record of its own).

A foreground `Bash` call is part of the orchestrator's (or an agent's) turn,
not background work: it gets no record, unless it runs past its timeout and
the runtime moves it to the background.

**Kinds:** `agent`, `workflow`, `shell`, `monitor`, `teammate`,
`remote_agent`, `mcp_task`, `other`. Housekeeping tasks of the CLI
(`dream`, `auto_mode_scan`) are *ambient*: kept out of every list and count.

**Statuses:**

| Status | Meaning |
|---|---|
| `starting` | The spawning call ran; the runtime has not reported the task yet |
| `running` | Working |
| `stopping` | Nerve asked the runtime to stop it; waiting for the runtime to confirm |
| `completed` | Finished its work |
| `failed` | Ended with an error (or its spawn failed) |
| `stopped` | Stopped: by you (`user`), by Stop all (`stop_all`), with the turn (`turn_stop`), by the orchestrator's own `TaskStop` (`orchestrator`), or by something unknown (`unknown_stop`). Stopped is never shown as done |
| `interrupted` | Its runtime went away: `gateway_restart`, `process_ended`, `force_stop`, `model_switch`, `archived` |
| `lost` | It left the runtime's live set and no final status ever came (`vanished`), or it never started within 5 minutes |

The first final status wins; only a `SendMessage` resume of the same task
makes a finished agent `running` again (`runs` counts the runs).

**Attention** (server side): `needs_input` (a foreground agent asked a
question), `failed`, `interrupted`, `lost`, `stop_unconfirmed` (a stop with
no confirmation after `stop_confirm_seconds`). The web UI adds `quiet` for a
running agent with no activity for `quiet_after_minutes`.

**Tokens and cost.** Tokens come from the runtime's own meter when it
reports one, otherwise from the agent's model calls (each call counted once,
by its last frame). Cost is a live estimate from those calls, shown as `~$`;
session totals still come from the runtime and are not changed by it.

## In the web UI

- **Inline lines.** Each `Agent` call in the chat renders as one line at its
  spawn point: status, name, activity, elapsed time, tokens, `[Open]` and a
  two-step `[■]` stop. Old history without a record keeps the old card.
- **Agent dock.** One line above the composer while the session has agents:
  counts (running, needing attention, done) and the latest activity. Click it
  to open the Agents panel.
- **Agents panel.** A tab in the side panel, grouped Needs attention →
  Running (nested children under their parent) → Done → Ended, with
  `[Stop all…]` in the header.
- **Agent tabs.** One per opened agent: the live transcript, or a shell's
  output, or a workflow's tree. They never close on their own.
- **Split Stop.** The red button stops the turn. Its caret offers "Stop turn
  and send" and "Stop all agents…".
- **Queued messages.** While a turn streams, Enter queues the message: it is
  shown above the composer until the turn ends, and you can edit or cancel it
  (either way its text comes back into the composer).

## Stopping

| Action | What ends | Where |
|---|---|---|
| **Stop** (red button, Esc, WebSocket `stop`, channel `/stop`) | The orchestrator's current turn only. Background agents, workflows, shells and monitors keep running. With no turn running, nothing is sent | `POST /api/sessions/{id}/stop` `{"scope": "turn"}` |
| **Stop one agent** | That agent, and the agents it started (deepest first). The turn and every other agent keep going | `POST /api/sessions/{id}/agents/{agent_id}/stop` |
| **Stop all agents…** | Every live agent (shells and monitors optionally), then the turn. Task notifications the runtime already queued are dropped, so they cannot wake the orchestrator into relaunching | `{"scope": "all"}` |
| **Force stop…** | Everything: the session's runtime is shut down (the next message starts a new one); every live agent ends as `interrupted` (`force_stop`) | `{"scope": "force"}` |

Messages queued in Nerve are never touched by a stop: a queued message runs
after the stopped turn. Cancel it from its own chip (only its sender can).

**When the turn does not stop.** If the turn has not ended 5 s after the
interrupt and agents are live, the turn is *not* cancelled (a cancel shuts down
the runtime and would end them): the stop answers `not_stopped` with the
agents that would end, and the UI offers Force stop. With no live agents the
turn is cancelled as before.

**When the runtime cannot spare agents.** Nerve tells the Claude CLI at its
first `initialize` that an interrupt must end only the turn (the CLI's
`perTaskStopAffordance` flag, which the Python SDK cannot send; Nerve adds it
to that one frame). If that flag could not be sent (it is turned off, or an
SDK upgrade moved the code it relies on), an interrupt would end every
background agent and workflow. Stop then answers `needs_confirmation` with
the agents it would end and sends nothing; the UI asks first. Channel
`/stop` (Telegram, Slack) never confirms: on such a runtime it ends nothing
and replies that it couldn't stop the session without stopping its
background agents (stop them from the web UI). A turn that did not end
within 5 s while agents run is left running too, and `/stop` says so.

A stopped agent cannot be resumed with `SendMessage` (the CLI refuses);
relaunch it instead. Stops are recorded as session events with who asked:
`stop_requested` (details `scope`, `outcome`, `live_agents`),
`agent_stop_requested`, `agents_stop_all_requested`, `force_stop_requested`.

## The turn queue

A message that arrives while the session's turn runs waits for it. While it
waits it is listed (`GET /api/sessions/{id}/queue`, the `queue_update`
WebSocket event, the agent snapshot) with a preview, where it came from
(`web`, `api`, a channel name) and who sent it. Internal triggers (scheduled
wakeups and runs) are listed with a fixed label, never their prompt.

Its sender can cancel it (`DELETE /api/sessions/{id}/queue/{queue_id}`): it
then runs nothing and stores nothing, and its text comes back. A message
sent with a `client_msg_id` that the session already saw in the last 10
minutes (a retry after a dropped connection) runs once.

## Restarts, reloads and teardown

- Records and transcripts are stored (`session_agents`,
  `session_agent_events`), written behind in batches about once a second per
  session; status changes are written at once. The roster survives a reload,
  a second tab and a gateway restart.
- Every way a session's runtime goes away (a crash, a model switch, a force
  stop, an idle reap, archive, delete, a gateway shutdown) marks its live
  agents `interrupted` with the reason. A gateway that crashed without a
  shutdown settles leftover live records as `interrupted (gateway_restart)`
  when it starts.
- The next turn after such an ending carries one reminder for the
  orchestrator listing the agents that ended outside its control; it is not
  stored in the history.
- A runtime with live agents is never reaped for idleness, and a model switch
  waits until they finish (`PATCH /api/sessions/{id}` answers `409
  live_background_agents`).
- Archiving a session with live agents is refused (`skipped` with the reason
  `background agents running`) unless forced (`?force=true`); the web UI asks
  first. Automatic cleanup skips such sessions. Deleting a session deletes its
  records.

## Keyboard

| Keys | Action |
|---|---|
| `⌘/Ctrl ⇧ E` | Toggle the Agents panel |
| `↓` in an empty message box | Open the Agents panel |
| `↑` `↓` `Home` `End` (in the panel) | Move between agents |
| `Enter` | Open the agent's tab |
| `x` `x` | Stop the focused agent (press twice) |
| `Esc` (in the panel) | Back to the message box (never stops the turn) |

## Configuration

`agent.subagents` in `config.yaml` (see [config.md](config.md#agent)):

| Key | Default | Meaning |
|---|---|---|
| `per_task_stop_affordance` | `true` | Stop spares background agents (the CLI `initialize` flag). Off: Stop asks before it would end them |
| `progress_summaries` | `true` | A one-line, model-written status per running agent (`agentProgressSummaries`; a few small-model calls per agent) |
| `forward_subagent_text` | `true` | Stream foreground and nested agents' text and thinking too (background agents always stream everything) |
| `quiet_after_minutes` | `10` | The UI flags a running agent with no activity for this long |
| `lost_grace_seconds` | `30` | A member of the runtime's live background set that disappears with no final status becomes `lost` after this long |
| `stop_confirm_seconds` | `10` | A stop with no confirmation after this long is flagged `stop_unconfirmed` |
| `transcript_item_max_bytes` | `16384` | Cap on each stored or streamed tool input or result in a transcript |

Every key takes effect without a restart. The first three are read when a
session's runtime starts, so they apply to runtimes started after the change.

## Limits

- **Codex sessions.** Collab agents appear as read-only records (no
  per-agent stop, no transcript); their result settles them, and one the
  turn ended without a result becomes `lost`.
- **Workflows** are one record each; the agents inside a workflow stay in its
  tree and cannot be stopped one by one (stop the workflow).
- Transcripts follow telemetry retention when it is enabled
  (`retention.*`, pruned after `retention_days`, starred sessions included);
  they are not part of the message compaction after `retention_full_days`.
  Records (with their prompt and final report) live as long as the session.
- Messaging an agent directly is not possible from the host: "Follow up…"
  asks the orchestrator to use `SendMessage`.
- A tab subscribes to at most 8 agent transcripts at once.

## For operators

- The `perTaskStopAffordance` patch relies on private SDK code
  (`SubprocessCLITransport`, `_configure_can_use_tool`, the shape of the
  `initialize` frame), pinned by `tests/test_claude_transport.py`. After an
  SDK upgrade, check that a background agent survives Stop. When the patch
  does not apply, Nerve logs `claude transport seam unavailable` or never
  logs `claude transport: initialize patched`, and Stop asks before ending
  agents.
- `nerve -v` logs one line per runtime task frame:
  `agents[<session>] task_started task=<id> tool_use=<call id> type=<task_type> bg=… status=… depth=…`.
- The tables are `session_agents` (one row per record) and
  `session_agent_events` (`agent_id`, `seq`, `kind`, `payload`). See
  [api.md](api.md#agents) for the routes and events.
