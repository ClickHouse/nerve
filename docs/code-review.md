# Pull request reviews

Code Review keeps one task per `(repository ID, PR number, profile)`. The task is
open until GitHub confirms closure or merge. A removed team request, a missing
search result or a failed HTTP request never closes it. Known closed PRs are also
polled, so reopening restores the same task and history. Drafts receive no new
task or review; tracked drafts retain history and only their state is observed.

Configure the service in the shareable `config/settings.yaml` layer:

```yaml
code_review:
  board_name: Tasks Code Review
  profiles:
    - id: example
      repository: alice/example
      query: team-review-requested:alice/reviewers
      enabled: false
      schedule: '0 */2 * * *'
      timezone: UTC
      version_budget_usd: 5
      daily_budget_usd: 30
      max_attempts: 3
```

All fields in `code_review` require a restart. The service reads GitHub through
`gh api --method GET` using the operator's existing authentication. No credentials
belong in this section. Search always adds repository, open PR and `draft:false`
constraints, checks completeness and caps, and then fetches PRs directly.
Repository data belongs in configuration; no organization is built into the app.

The authenticated `/tasks-code-review` page shows the actual registered schedule,
last successful poll, next poll, observation errors, queue states and budget.
Its active and archive columns each paginate over the full set, ordered by last
content change and stable PR identity. Poll heartbeats do not reorder cards.
The pause button persistently stops the profile and only its owned workflow runs;
tasks and immutable artifacts remain. Resume performs one catch-up and keeps the
existing budget ledger. A configured disabled profile cannot be resumed from the
UI and never starts.
One startup catch-up replaces missed launches. Normal GitHub polling uses no LLM.

## Versions and audit

Full head/base/merge-base SHAs, PR text, comments/reviews, changed file paths and CI
are captured. Head, base, requirements and new failure context create input
versions. CI-only transitions update evidence without repeating the audit. Full
changed files at the recorded SHAs are supplied to the worker; it must inspect
needed callers, contracts and tests using read-only GitHub requests pinned to SHA.
Truncated file/search pagination fails visibly. Source acquisition has a 100 MB
bound and does not silently substitute a truncated API patch.

Workers use three session-bound MCP tools, without shell execution:
`code_review_input` supplies the snapshot and prior report;
`code_review_read_source` reads numbered, paginated source at the stored head,
base or merge-base SHA (including unchanged files); `code_review_checkpoint`
saves the four report stages in order and validates the final verdict. These
tools reject non-review sessions, paused/disabled profiles and settled or stopped
attempts. They accept no repository, run ID, arbitrary SHA or local output path.
Source reads are GET-only; report writes use directory descriptors and refuse
symlink traversal. The restricted Codex sandbox remains enabled. This also works
in containers where nested Linux namespaces prevent shell sandbox startup.
No command execution capability is exposed by these tools.

Each workflow writes description, before/after sequence diagrams and code review
in order. The instructions cover bugs, adjacent behavior, compatibility, enablement
and invariants. New behavior uses Mermaid `rect rgb(220, 252, 231)` and the text
«новое»; diagrams also retain their copyable source. Mermaid renders locally with
strict security, sanitization, source configuration/link restrictions and a size
cap. Invalid diagrams fall back to source. No diagram rendering service is used.
See [Mermaid sequence highlighting](https://mermaid.js.org/syntax/sequenceDiagram.html#background-highlighting)
and [security configuration](https://mermaid.js.org/config/schema-docs/config.html).

Reviews currently do **not** run repository code or tests: there is no provisioned
secret-free isolated test runner. Workers must disclose this and propose focused
tests. Test execution requires a separately implemented isolated runner, never a
shell command in the authenticated Nerve environment. No GitHub comments, approvals,
branch changes or merges are part of this service.

Artifacts live under `workspace/memory/code-reviews/<PR>/<version>/`. Each attempt
has a working output directory and a write-once `result.json`; snapshots and final
artifacts are read back before index completion. Failed/interrupted attempts keep
partial output. SQLite stores identities, queue, ledger, events and artifact paths.
The ordinary workspace backup includes `memory/`; retain the SQLite backup together
with it. A workflow journal is not the only copy. History is served through normal
authenticated API/page routes, including inside the task detail.

A new SHA during review cannot make the old report current. Superseded queued
versions remain in history. Draft is rechecked before dispatch and before admission.
A workflow waiting for a global slot has a dispatch guard, so it checks again when
it actually starts. Per-profile concurrency is one. The queue keeps a PR-level waiting timestamp
across superseded versions and moves a claimed PR to the back, so frequent base
changes cannot repeatedly prioritize the same PRs. Failed attempts back off and
are capped. Observation errors do not starve other PRs and are retried at the next
poll.

## Budget semantics and recovery

Reservations are committed before workflow creation. A unique run ID links the
outbox to Nerve's workflow row. A controller lease excludes competing daemons;
SQLite `BEGIN IMMEDIATE` makes reservations safe across connections. Startup first
reconciles workflow recovery, then polls and dispatches. No retry resets a version's
spend, and every profile shares the daily cap. All profiles must use the same daily
cap and time zone. Active reservations carry across midnight. A run spanning dates
is conservatively charged in full to each date it touches (the UI calls this
committed budget); per-attempt actual spend is shown separately. This can reduce
throughput but cannot create capacity by crossing midnight.

A finished report whose final GitHub observation fails waits in a separate
read-only admission queue with backoff. Its paid run is settled and other PRs can
proceed. Recovery does not pay to recreate an already valid report.

A terminal workflow row is not sufficient to release a reservation: the executor,
kill and teardown tasks must have ended and recorded cost is reconciled. Metering
is delayed, so actual provider billing may overshoot. The UI displays recorded
spend and excess; new runs stop when committed budget reaches the cap. If a run
actually started but was interrupted by a daemon restart, its unreported remainder
stays reserved as uncertainty, separately from measured spend. Pending runs that
never started are not charged. A per-version limit preserves the partial review
and never completes the PR task. Daily exhaustion keeps versions queued while
polling continues.

Notifications combine discoveries and lifecycle changes; ordinary results
accumulate in a ten-minute digest. Unchanged serious findings are deduplicated.
Sustained observation errors and budget exhaustion produce notifications;
unchanged polls remain silent.

## Deployment and verification

1. Install the application change through the normal reviewed application path.
   Back up the database/workspace, run migrations and the frontend build. Migration
   v062 is idempotent and adds tables without rewriting existing task data.
2. Review the disabled workspace profile through the configuration PR path. Keep
   other cron jobs/model settings unchanged. Synchronize explicitly when automatic
   workspace sync is disabled. Validate with the newly installed application version.
3. Enable the reviewed profile and restart. Check `/api/code-review/status` and the
   board for the actual `code-review-<profile>` job, time zone, last success and next
   run. Check that search was complete and unique task count matches eligible PRs.
4. Inspect one complete workflow (or an explicit budget queue), its SHA, all three
   stages and previous-version selector. Repeated unchanged polling must preserve
   board order; a new SHA must produce a version. Check draft, close and reopen.
5. Pause the profile to stop its work without deleting the archive. Disable it in
   reviewed configuration before the next installation if shutdown is permanent.

Local regression checks: `pytest tests/test_code_review.py`, migration numbering
check, full backend tests, and frontend test/lint/build. Tests use synthetic PRs
and temporary databases/workspaces; they never launch paid review workflows.
