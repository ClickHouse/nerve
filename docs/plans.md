# Proactive Task Planning

## Overview

Nerve includes a proactive planner that autonomously picks open tasks, explores the codebase, and proposes implementation plans for human review. Plans are never executed automatically — they go through an approval workflow.

## How It Works

```
Cron (every 4h) → persistent "task-planner" job
  → agent browses tasks, checks memory, picks one worth planning
  → explores codebase with Read, Glob, Grep, Bash, etc.
  → calls plan_propose(task_id, content) to store the proposal
  → plan appears in /plans UI for review

User reviews (via /plans UI or chat tools)
  → approve → spawns implementation session (visible in Chat)
  → decline → marks plan declined
  → request revision → sends feedback to same persistent planner session
```

## Agent Tools

| Tool | Description |
|------|-------------|
| `plan_propose` | Propose an implementation plan for a task. Stored for async human review. |
| `plan_update` | Revise a pending plan in place — supersedes the old version and creates v+1 linked via `parent_plan_id`. Preferred over decline+propose for self-refinement. |
| `plan_list` | List existing plans. Used to check which tasks already have pending plans. |
| `plan_read` | Read full plan content. Used to review details before approving/declining. |
| `plan_approve` | Approve a pending plan and spawn an implementation session. |
| `plan_decline` | Decline a pending plan with optional feedback. Moves the related task to `done` (declining is treated as abandoning the effort, not pausing it). |
| `plan_revise` | Request revision of a pending plan — sends feedback to the planner session, which calls `plan_update` to produce v+1. |

### `plan_propose(task_id, content, plan_type?)`

- Validates the task exists
- Checks no pending/implementing plan already exists → returns error if duplicate
- Auto-detects `plan_type` from task source (`skill-extractor` → `skill-create`, etc.)
- Auto-increments version if previous plans exist for the same task
- Supersedes any prior pending plan for the same task
- Returns `{ plan_id, task_id, version }`

### `plan_list(status?)`

- Default: returns pending + implementing plans
- Supports filtering: `pending`, `approved`, `declined`, `implementing`, `superseded`

### `plan_read(plan_id)`

- Fetches full plan record from database (joins with task for title)
- Returns formatted header (ID, version, status, task, type, dates, feedback, impl session) + full plan content
- Used by agents to inspect a plan before taking action (approve/decline/revise)

### `plan_approve(plan_id)`

- Guards: only pending plans can be approved
- Marks plan as `implementing` (prevents double-approve)
- Creates implementation session (`impl-{uuid}`) with full tool access
- Updates task status to `in_progress`
- Builds skill-aware prompt (skill plans use `skill_create`/`skill_update` tools)
- Supports **runtime selection**: when `runtime=houseofagents` is passed, the prompt is augmented with instructions to use the `hoa_execute` MCP tool for multi-agent execution
- Spawns `engine.run()` in background (unchanged — the agent decides how to implement)
- Returns `{ plan_id, impl_session_id }`

### `plan_update(plan_id, content, feedback?)`

- Guards: only pending plans can be updated
- Marks the old plan as `superseded` (and stores optional `feedback` on it explaining the change)
- Creates a new plan record: `version = old.version + 1`, `parent_plan_id = old.id`, same `task_id`/`plan_type`, fresh `session_id` from the updating agent
- Writes a "Plan updated: …" note to task history
- Task status is untouched — the plan is just being refined
- Returns `{ new_plan_id, version }`

**When to use which:**
- `plan_update` — you (or the planner) want to refine your own pending plan. The task stays open, history is linked. **Default choice for revisions.**
- `plan_decline` — the user truly rejects the plan and abandons the effort. The task moves to `done`.
- `plan_revise` — the user wants the original planner agent to rethink the plan. Sends feedback to the planner session, which then calls `plan_update`.

### `plan_decline(plan_id, feedback?)`

- Guards: only pending plans can be declined
- Sets status to `declined` with timestamp
- Stores optional feedback on the plan
- Moves the related task to `done` with a note explaining the closure (uses the feedback as the reason, or a generic "closed without a specified reason" if none was given)
- Returns confirmation

### `plan_revise(plan_id, feedback)`

- Guards: only pending plans can be revised; feedback is required
- Stores feedback on the plan record
- Writes revision note to task history
- Sends feedback as a message to the persistent `cron:task-planner` session
- Planner agent sees prior context + feedback, calls `plan_update` to produce v+1 linked to the existing plan
- The current pending plan is automatically superseded by the update

## Plan Statuses

| Status | Meaning |
|--------|---------|
| `pending` | Awaiting human review |
| `approved` | Approved (briefly, before implementation starts) |
| `implementing` | Implementation session is running |
| `declined` | Rejected by user |
| `superseded` | Replaced by a newer version |

## Cron Job

Defined in `~/.nerve/cron/jobs.yaml` as `task-planner`:

- **Schedule:** Every 4 hours (`0 */4 * * *`)
- **Session mode:** Persistent (keeps context for revisions)
- **Context rotation:** Weekly (168 hours)
- **Model:** claude-opus-4-7

The planner is a standard persistent cron job — no special service or engine code needed.

## Revision Flow

When the user requests a revision:

1. User writes feedback in the plan detail page
2. API sends feedback as a new message to the persistent `cron:task-planner` session
3. The agent sees its prior planning context + the feedback
4. Agent calls `plan_update(plan_id, content, feedback)` — old version becomes `superseded`, new version (v+1) is pending review
5. The two plans stay linked via `parent_plan_id` so reviewers can see what changed

This works because the planner uses a **persistent session** — the agent retains conversation history across triggers and revision requests. The same flow works for any agent refining its own plan in chat — call `plan_update` directly instead of `plan_decline + plan_propose`.

## Approval → Auto-Implementation

When a plan is approved:

1. Plan status → `implementing`
2. A new chat session is created (`impl-{uuid}`) with full tool access
3. The session receives the task content + approved plan as instructions
4. Session runs in the background, visible in the Chat page
5. Task status → `in_progress`

The user can monitor, stop, or interact with the implementation session from the Chat UI.

### Skill Proposals

Plans from the `skill-extractor` and `skill-reviser` cron jobs follow a different approval path. When approved:

1. Plan content is parsed as a full SKILL.md file (YAML frontmatter + body)
2. If the skill already exists → updated; otherwise → created
3. Plan status → `completed`; task status → `done`
4. No implementation session is spawned — the plan *is* the deliverable

This is handled automatically by the plan approval handler based on the task's `source` field (`skill-extractor` or `skill-reviser`).

## Database

Plans are stored in the `plans` SQLite table (schema v12):

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT PK | Plan ID (`plan-{uuid}`) |
| `task_id` | TEXT | Linked task |
| `session_id` | TEXT | Planner session that created it |
| `impl_session_id` | TEXT | Implementation session (after approval) |
| `status` | TEXT | pending/approved/declined/superseded/implementing |
| `content` | TEXT | Plan markdown |
| `feedback` | TEXT | User revision feedback |
| `version` | INTEGER | Version number (increments on revision) |
| `parent_plan_id` | TEXT | Previous version's ID |
| `model` | TEXT | Model used to generate |

## Web UI

### Plan List (`/plans`)

- Status filter tabs: All, Pending, Approved, Implementing, Declined
- Cards show: task title, plan version, status badge, creation date
- Click → navigate to plan detail

### Plan Detail (`/plans/:planId`)

- Rendered markdown plan content
- Task link + implementation session link (when applicable)
- Action bar for pending plans:
  - **Approve & Implement** — spawns implementation session, redirects to Chat
  - **Decline** — marks plan as declined
  - **Request Revision** — quote-style feedback input, sends to planner session
- Previous feedback shown as blockquote
- **Multi-Agent toggle** — when houseofagents is enabled and available, shows a "Multi-Agent" toggle with mode/agents selection. Sends `runtime=houseofagents` to the approve endpoint.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/plans` | List plans (query: `status`, `task_id`) |
| GET | `/api/plans/:id` | Get plan detail |
| PATCH | `/api/plans/:id` | Update status/feedback |
| POST | `/api/plans/:id/approve` | Approve + spawn implementation (body: `{runtime?, hoa_mode?, hoa_agents?, hoa_pipeline_id?}`) |
| POST | `/api/plans/:id/revise` | Send revision feedback to planner |
| GET | `/api/tasks/:id/plans` | Plans for a specific task |
