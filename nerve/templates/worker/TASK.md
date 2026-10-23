# Task
<!-- nerve:onboarding pending: the onboarding conversation fills this file in; this line goes when onboarding is done. -->

## Task description (as given)
{{TASK_DESCRIPTION}}

## Identity
_(onboarding fills this in: which account acts, exact git author, fork/remote names)_
- Example: `ci-bot`; git author `CI Bot <ci-bot@example.com>`; fork `ci-bot/repo-a` (remote `fork`), upstream `org/repo-a` (remote `origin`).

## Mission
_(onboarding fills this in: one paragraph ending in the quality bar)_
- Example: Watch CI on `org/repo-a`, fix regressions and flaky tests at the source, and tell maintainers what they need to know. Fewer, better PRs beat many quick ones.

## Capabilities
_(onboarding fills this in: what this worker is expected to be able to do, and what it may never dismiss; at most 10 lines)_
- Example: a warm debug build (`skill_get build-and-test`) and read access to the failure database.
- Example: never dismiss a failure as too deep, "infra noise" or someone else's code. A missing tool is named, then installed or flagged, never an excuse.

## Scope
_(onboarding fills this in: repos, CI, data sources, and the one exhaustive reference doc or skill; pointers, no credentials)_
- Example: repository `org/repo-a`; CI on GitHub Actions; failure DB endpoint and auth mode in `TOOLS.md`.
- Example: CI internals in `docs/ci-internals.md` or the `ci-db-query` skill; read them instead of reconstructing the CI system from memory.

## Hard rules
_(onboarding fills this in: the non-negotiables, each its own heading with a marker and provenance; as few as you can defend)_
- Example heading: `### ⛔ HARD RULE: builds and tests run in the foreground as one call [operator directive YYYY-MM-DD]`
- Example body: one sentence of rule before any why; name the only outcomes that permit the action, and everything else refuses.

## Triggers
_(onboarding fills this in: what creates work, one line each: scan crons, inbox and mentions, human directives)_
- Example: `ci-scan` cron (every 30 min): new main-branch failures → tasks tagged `source:ci-main`
- Example: human directives (chat, task notes tagged `human-directive`) → outrank everything

## Actions
_(onboarding fills this in: the status state machine as a transition table, then an anti-patterns list)_

| From | Event | To | Who holds the ball |
|---|---|---|---|
| _example:_ `pending` | a run claims it | `in_progress` | this worker |
| _example:_ `in_progress` | PR pushed, CI running | `waiting_ci` | CI |

- Example anti-pattern: pushing a cleanly waiting task back to `pending` burns a run.

## Approval Required
_(onboarding fills this in: what needs a human, what is autonomous, where decisions are recorded)_
- Example: opening a public issue, anything irreversible or external → human approval
- Example: classifying a test as flaky, notifying → autonomous, recorded in the task note
- Example: self-raised work → only with a passing `## Impact` section

## References
_(onboarding fills this in: endpoints with auth mode (never passwords), schema columns, canonical filters, label vocabulary; the long form lives in `TOOLS.md`)_
- Example: failure DB columns `job, test, status, log_url`; failures are `status IN ('failed','errored')`
- Example: the toolchain pin lives in `ci/toolchain.lock`; read it, never quote it

---
Read the full guide: https://github.com/ClickHouse/nerve/blob/main/docs/worker-best-practices.md
