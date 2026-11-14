# Proactive Task Planning

## Overview

Nerve includes a proactive planner that autonomously picks open tasks, explores the codebase, and proposes implementation plans for human review. Plans are never executed automatically — they go through an approval workflow.

## How It Works

```
Cron (every 4h) → persistent "task-planner" job
  → agent browses tasks, checks memory, picks one worth planning
  → explores codebase with Read, Glob, Grep, Bash, etc.
  → calls plan_propose(task_id, content, summary) to store the proposal and its short summary
  → the task moves to plan_review, and one notification links to the plan
  → plan appears in /plans UI for review

User reviews (via /plans UI or chat tools)
  → approve → spawns implementation session (visible in Chat)
  → decline → marks plan declined
  → request revision → sends feedback to the session that proposed the plan
```

## Agent Tools

| Tool | Description |
|------|-------------|
| `plan_propose` | Propose an implementation plan for a task. Stored for async human review. |
| `plan_update` | Revise a pending plan in place — supersedes the old version and creates v+1 linked via `parent_plan_id`. Preferred over decline+propose for self-refinement. |
| `plan_list` | List existing plans. Used to check which tasks already have pending plans. |
| `plan_read` | Read full plan content. Used to review details before approving/declining. |
| `plan_approve` | Approve a pending plan and spawn an implementation session. With `implement_here=true`, record the approval for the calling session and start nothing. |
| `plan_decline` | Decline a pending plan with optional feedback. Moves the related task to `done` (declining is treated as abandoning the effort, not pausing it). |
| `plan_revise` | Request revision of a pending plan — sends feedback to the session that proposed it (the planner session for cron-made plans; `cron:task-planner` when the plan records no session or its session was deleted; an archived session is unarchived first), which calls `plan_update` to produce v+1. |

### `plan_propose(task_id, content, plan_type?, summary?)`

- Validates the task exists
- Stores the optional `summary`: up to 3 short lines for the reviewer, at most 400 characters. The tool clips a longer summary and says so in its result. The plan page shows the summary under the title, or the first lines of the plan when there is none.
- Checks no pending/implementing plan already exists → returns error if duplicate
- Auto-detects `plan_type` from task source (`skill-extractor` → `skill-create`, etc.)
- Auto-increments version if previous plans exist for the same task
- Supersedes any prior pending plan for the same task
- Moves the task to `plan_review` when its status is `pending`, `backlog` or `in_progress`. A task in any other status keeps it. The move names the task revision that was read, so a status that another writer set in the meantime stays.
- Writes a task note with a markdown link to the plan: `Plan proposed: [plan-…](/plans/plan-…) (v1)`
- Sends one notification, "Plan ready for review: <task title>", with priority `high`. Its metadata holds `plan_id` and `task_id`. The web UI shows "Open plan" and "Open task" for it, and Telegram shows an "Open plan" button that opens the plan in the chat. During quiet hours it is sent without sound. It dismisses the task's earlier plan notifications, and approve and decline dismiss the last one. Agents do not send their own `notify` about the plan.
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
- Spawns `engine.run()` in background (unchanged — the agent decides how to implement)
- Returns `{ plan_id, impl_session_id }`

### `plan_approve(plan_id, implement_here=true)`

For a plan that the user approved in the chat of the session that does the work. Without it, such a plan stays `pending`: it waits on the Plans page, and its session stays in the sidebar's "Needs you" group.

- Guards: only pending plans; needs a calling session
- Marks the plan `implementing` with `impl_session_id` = the calling session, or `done` when the task is already done
- Starts no session and runs no turn
- Moves a task in `plan_review` to `in_progress`; any other task status does not change. Writes a task note with links to the plan and the session, and dismisses the task's pending plan notifications
- Returns the plan ID and the new plan status

### `plan_update(plan_id, content, feedback?, summary?)`

- Guards: only pending plans can be updated
- Stores the optional `summary` on the new version, as `plan_propose` does. The summary of the old version is not copied, because it can describe the old content.
- Marks the old plan as `superseded` (and stores optional `feedback` on it explaining the change)
- Creates a new plan record: `version = old.version + 1`, `parent_plan_id = old.id`, same `task_id`/`plan_type`, fresh `session_id` from the updating agent
- Writes a "Plan updated: …" note to task history, with links to both versions
- Moves the task to `plan_review` and sends a notification for the new version, by the same rules as `plan_propose`
- Returns `{ new_plan_id, version }`

**When to use which:**
- `plan_update` — you (or the planner) want to refine your own pending plan. The task stays open, history is linked. **Default choice for revisions.**
- `plan_decline` — the user truly rejects the plan and abandons the effort. The task moves to `done`.
- `plan_revise` — the user wants the agent that proposed the plan to rethink it. Sends feedback to the proposing session (the planner session for cron-made plans), which then calls `plan_update`.

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
- Sends feedback as a message to the session that proposed the plan (`cron:task-planner` for the planner's own plans)
- Planner agent sees prior context + feedback, calls `plan_update` to produce v+1 linked to the existing plan
- The current pending plan is automatically superseded by the update

## Plan Statuses

| Status | Meaning |
|--------|---------|
| `pending` | Awaiting human review |
| `approved` | Approved (briefly, before implementation starts) |
| `implementing` | Implementation session is running |
| `done` | The task of the plan was marked done. A `failed` plan becomes `done` too when its own implementation session marks the task done |
| `failed` | The first implementation turn failed: it raised, or it ended with a failed outcome. A task event (`updated`) follows the write |
| `declined` | Rejected by user |
| `superseded` | Replaced by a newer version |

## Cron Job

Defined in `<workspace>/config/cron/jobs.yaml` as `task-planner`:

- **Schedule:** Every 4 hours (`0 */4 * * *`)
- **Session mode:** Persistent (keeps context for revisions)
- **Context rotation:** Weekly (168 hours)
- **Model:** claude-opus-5

The planner is a standard persistent cron job — no special service or engine code needed.

## Revision Flow

When the user requests a revision:

1. User writes feedback in the plan detail page
2. API sends feedback as a new message to the session that proposed the plan (the persistent `cron:task-planner` session for its plans)
3. The agent sees its prior planning context + the feedback
4. Agent calls `plan_update(plan_id, content, summary, feedback)`: the old version becomes `superseded`, and the new version (v+1) waits for review
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

A plan is a skill plan when its `plan_type` is `skill-create` or `skill-update`: the `plan_propose` call states the type, or the task's `source` is `skill-extractor` or `skill-reviser`. The `skill-extractor` cron job states it. A skill plan goes through the same approval path as any other plan — an `impl-<id>` session is spawned — with a prompt for the plan type. A session that implements the plan itself (`plan_approve` with `implement_here=true`) gets no such prompt.

`skill-extractor` does not run while three or more of its proposals wait for review (a [`plans_in_review`](cron.md#plans_in_review) gate). Approve or decline some, and it goes on at its next scheduled time, if the agent has worked since its last run. Closing the task of a proposal does not decide its plan.

A `skill-create` plan is a proposal for a person to judge. It says what the skill is for, the evidence that it is needed (where the workflow occurred and what was repeated) and the direction the skill should take. The prompt has the session write the skill:

1. The session looks at the evidence the plan names and at the existing skills
2. It decides the skill's name, description and content, within the plan's direction, and calls `skill_create`
3. It then calls `task_done` with a note describing what was done

When the evidence does not support a skill, or an existing skill covers it, the session creates none, tells the user why with `notify`, and says so in the note. A plan can also hold a complete SKILL.md: one that `skill-extractor` proposed in an earlier release, one a person wrote, or one from a job with a prompt of its own. The session takes it as a draft, checks it against the evidence, and passes its body, without the frontmatter, to `skill_create`.

A `skill-update` plan is the full revised SKILL.md file (YAML frontmatter + body). The session calls `skill_update` with it, and then `task_done`.

The plan is marked `implementing` on approval and `failed` if the first implementation turn fails.

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
| `summary` | TEXT | Short summary from `plan_propose` or `plan_update`; NULL when the agent gave none |
| `feedback` | TEXT | User revision feedback |
| `version` | INTEGER | Version number (increments on revision) |
| `parent_plan_id` | TEXT | Previous version's ID |
| `model` | TEXT | Model used to generate |
| `reviewed_at` | TIMESTAMP | Time of the approve or decline decision |
| `reviewed_by_actor_id` | TEXT | Who made the last review decision (approve, decline, revision request, or a status or feedback change through `PATCH`) |
| `reviewed_caused_by_actor_id` | TEXT | On whose behalf that decision was made |
| `created_by_actor_id` | TEXT | Who wrote this version |
| `caused_by_actor_id` | TEXT | On whose behalf this version was written |

A review through the web UI or the REST API records the signed-in person in
both reviewer columns. A review through a plan tool records the system actor
and the cause of the session that called the tool, so a plan that the agent
approves in a session that Alice started shows "Nerve for Alice".
`plan_propose` and `plan_update` record the system actor and the session's
cause as the writer of the version they create. The implementation session
of an approval gets the approver (REST) or the approving session's cause
(tool) as its cause, and a planner session that a revision request creates
gets the person who asked. `plan_update` records no reviewer on the version it
replaces, because the planner replacing its own plan is not a review. All
four columns are actor IDs; see [Accounts and identity](accounts.md#attribution).

A decision on a plan (approve, decline, `PATCH`, and `plan_update`
superseding the old version) applies only while the plan still has the
status that the decision read. Of two concurrent decisions that change the
status, one wins; the other changes nothing and gets HTTP 409 (REST) or a
"no longer pending" answer (tools). Only the winner starts an
implementation or changes the task. A decision that keeps the status (a
`PATCH` with only feedback, a revision request) does not exclude another
decision: two such requests can both apply, and a revision and an approval
of the same plan can both proceed.

## Web UI

### Plan List (`/plans`)

- Status filter tabs: All, Pending, Approved, Implementing, Declined
- Cards show: task title, plan version, status badge, creation date
- Click → navigate to plan detail

### Plan Detail (`/plans/:planId`)

- Under the title, the plan's `summary` in up to 3 lines. A plan without one
  shows its first heading and the two lines after it
- Rendered markdown plan content
- Task link
- For an approved plan, a progress card above the plan: the implementation
  session's title, its state and the time since the approval, and **Open
  session**. The state is Running, Waiting on background work (a scheduled
  wake-up or background work, no turn), Waiting for you (a question, or the
  turn ended while the task is open), Done (the plan is `done`) or Failed
  (the plan is `failed` and its session does no work). The page loads the
  plan again on each `session_running`, `session_awaiting_input`,
  `session_agents` or `session_updated` event of that session, on each
  `task_updated` event (`done` or `updated`) of the task, and after a
  reconnect. It stops when the plan is `done` and the session's last turn
  ended, so the time is final. A load that answers `implementing` after the
  page has `done` or `failed` is older and is ignored
- Who proposed this version and who reviewed the plan, from the writer and
  reviewer columns
- A decision bar at the bottom of the page for pending plans. It stays in view
  at every scroll position, above the bottom nav on a phone. It uses the
  decision controls of the chat's plan card (`PlanDecisionControls`):
  - **Approve & implement** starts the implementation session and opens it
    in Chat
  - **Request revision…** opens a required "What should change?" field in the
    bar; **Send** sends it to the session that proposed the plan
  - **Decline…** opens an optional reason field in the bar; **Decline plan**
    declines the plan and closes its task
  - Cmd/Ctrl+Enter sends a field, Escape closes it. The buttons are disabled
    while a decision is sent, and an error shows above them
- Previous feedback shown as blockquote

## Telegram

`/plans` shows the pending and implementing plans as an inline keyboard, one
button per plan, with paging and a refresh button. Opening a plan shows its
content and, while it is pending, three actions:

- **Approve**, then **Yes, approve & implement**: the same approval as the web
  UI, which spawns the implementation session.
- **Decline**, then **Yes, decline & close**: marks the plan declined and
  closes its task.
- **Revise**: asks for feedback in a reply. Only a reply to that prompt, from
  the user who tapped it, is taken as the feedback; it goes to the planner
  session as with `plan_revise`.

Every button and the revision reply are checked against the bot's allowed
users. Telegram users are not mapped to local identities, so a decision made
from Telegram records no reviewer (both reviewer columns stay empty), and its
task updates and sessions have no actor or cause.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/plans` | List plans (query: `status`, `task_id`) |
| GET | `/api/plans/:id` | Get plan detail, with `impl_session` (see below) |
| PATCH | `/api/plans/:id` | Update status/feedback |
| POST | `/api/plans/:id/approve` | Approve + spawn implementation |
| POST | `/api/plans/:id/revise` | Send revision feedback to planner |
| GET | `/api/tasks/:id/plans` | Plans for a specific task |

`GET /api/plans/:id` adds `impl_session`: `null` without an implementation
session (or when its row is gone), else `{id, title, updated_at,
is_running, awaiting_input, parked}`. `parked` is true when no turn runs but
a wake-up is scheduled or background work still runs.

Plan rows include `reviewed_by_actor_id`, `reviewed_caused_by_actor_id`,
`created_by_actor_id` and `caused_by_actor_id`. Approve, `PATCH` and revise
record the signed-in person as the reviewer, and return 409 when another
decision changed the plan first. The
task status change of an approval or a decline records the person too, with
origin `web`.
