# Review Loops

A **review loop** is a deterministic implement→verify cycle built from
[workflow runs](workflow-runs.md):

- an **implementer leg** (a budget-capped workflow run) works toward a
  **Goal** prompt;
- an independent **verifier leg** (a separate workflow run, its own
  engine/model) judges the workspace **end state** against the pinned
  **Verifier** criteria, executes checks, and returns a structured verdict;
- a server-side controller routes on the verdict and iterates until the
  criteria pass or a cap fires (iterations, dollar budget, no-progress).

The loop lives in code, never in an LLM: the implementer never decides it
passed, and the verifier never decides to run "one more iteration". Legs
are fresh runs/sessions every time — feedback travels in composed prompts
and files, not shared conversation state.

## Creating a loop

**Web UI**: the 🔁 toggle in a new chat's composer opens the panel — Goal,
Verifier criteria, budget, plus advanced options. "Start review loop"
creates the session (the loop's *observer*: milestones stream into it) and
kicks the first leg.

**MCP tool** (chat/cron): `review_loop_start(goal=..., verifier=...,
budget_usd=...)` — the calling session becomes the observer. Inspect with
`review_loop_status` (by loop id or session), stop with `review_loop_kill`.

**Session API**: `POST /api/sessions` with a `review_loop` field.

Write the Verifier criteria as bullet lines — each becomes a pinned
criterion with a stable id (`C1..Cn`). Prefer executable, observable
statements ("`pytest tests/` passes", "GET /health returns 200", "file X
contains Y") over vibes ("code is clean"): the verifier must show evidence
per criterion, and checkable criteria make gaming and endless nitpicking
structurally hard.

## The verdict

The verifier ends its final message with one fenced JSON block:

```json
{
  "verdict": "pass" | "needs_improvement" | "fail",
  "summary": "one paragraph",
  "criteria": [{"id": "C1", "status": "met|unmet|unverifiable",
                "evidence": "command + output excerpt", "fix_hint": "..."}],
  "findings": [{"title": "...", "body": "...", "priority": 0,
                "location": "file:line", "criterion_id": "C1"}],
  "proposed_criteria": [{"statement": "...", "rationale": "...",
                         "severity": "blocking|advisory"}],
  "gamed": false
}
```

The pass gate is mechanical: **every known criterion** must be reported
`met` with non-empty evidence, no P0/P1 finding may block (P2/P3 are
advisory — reviewer noise never forces an iteration), and a `gamed: true`
verdict (weakened tests, hardcoded outputs) parks the loop for a human
instead of retrying — re-running a gaming implementer applies optimization
pressure in exactly the wrong direction. Malformed verdicts get one
schema-repair retry, then escalate.

## Criteria adoption (`criteria_adoption`)

What happens when the verifier *discovers* missing criteria:

- **`no`** (default; build/fix goals): your criteria are the contract.
  Proposals surface in milestones and on decision cards, but never block.
- **`ask`**: adopting a proposal requires your decision.
- **`auto`** (research/audit goals where completeness is discovered):
  blocking proposals auto-adopt — append-only, capped per iteration and in
  total — and then must be met to pass. A discovery-grace guard escalates
  "scope keeps growing" instead of letting the checklist expand forever.

## Termination & decisions

A loop ends `passed`, or parks in `awaiting_user` on: iteration cap,
budget exhaustion, no-progress (same unmet criteria twice with an
unchanged workspace), repeated leg failures, verifier errors, gaming, or a
restart that interrupted an implementer leg. Parked loops raise an
actionable card — **Accept as-is / Grant +2 iterations / Abandon / Remind
me in 4h** — and the same decisions are available via
`POST /api/review-loops/{id}/decision`. Expired cards are re-proposed a
bounded number of times, then the loop closes as failed with a
notification: nothing parks silently, and paid-for work is never
discarded without telling you.

Budgets are hard: each leg is a metered workflow run; a reserve fraction
guarantees the final implementation attempt is always verified. Killing
the loop (UI, REST, or `review_loop_kill`) kills its current leg only.

## When to use it

Review loops shine when completion is **checkable** — code with tests,
deployments, file artifacts, data pipelines, research with enumerable
coverage. For open-ended "make it better" goals a single stronger-model
run (or best-of-N) is usually better spend: a loop roughly triples calls,
and unverifiable criteria are what make verify-loops oscillate.

## Configuration

See the `workflows.review_loop` section in `config.example.yaml`. Notable
defaults: implementer = `claude-workflow`, verifier = `codex-ultracode`
(cross-vendor verification — a different model family has decorrelated
blind spots and no self-preference toward the implementer's work; the
loop falls back to same-vendor when Codex isn't configured), 3 iterations
(hard ceiling 8), $10 budget. Codex verifier legs run with a
`workspace-write` sandbox override and require a `codex.pricing` entry
(fail-closed metering).

## Internals (for the curious)

State machine row in `review_loops`; every leg dispatch writes an
append-only `review_loop_attempts` outbox row *in the same transaction*
as the state transition, with a pre-generated run id — a crash can never
orphan a paid leg. Restart recovery routes on the leg run's actual status
(including "finished but unprocessed"); verifier legs re-issue
automatically, implementer legs park for a decision by default
(`auto_reissue_implementer`) because re-running them can double-apply
side effects. Implementer legs maintain a handoff state file
(`.nerve-review/<loop-id>/STATE.md`) that later legs receive as
explicitly *unverified* claims — only verifier evidence is carried
forward as fact.
