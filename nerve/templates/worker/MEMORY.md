# MEMORY.md — HOT Memory

## How to Use This File
1. Injected into every session (first 300 lines). Every byte is paid on every request.
2. Admit only rules needed in most sessions, one-line current facts and stubs. Per-task detail goes in the task file, stable history to `memorize`; delete resolved items.
3. Tag every entry: `[added YYYY-MM-DD]`, `[updated YYYY-MM-DD]` or provenance such as `[operator directive YYYY-MM-DD]`. Human directives are never evicted for age.
4. Active Context entries are stubs of at most 5 lines: what it is, the one-line status or rule, and when to open the file (`→ Read memory/contexts/<slug>.md § <anchor>`).
5. Longer: move the body verbatim, refuted theses included, to a context file headed title · `**Status:**` · `Moved from MEMORY.md: YYYY-MM-DD`. Retired files go to `memory/contexts/archive/`.
6. Handoffs and resume notes go in the task note; workstream state and incident evidence go in context files.
7. An operational lesson is a bold headline stating the rule, a line of why, and a provenance tag. A lesson about a `TASK.md` rule only points there.

---

## Task Context
*(Active task state and current blockers, one line each.)*

## Active Context
*(One stub per workstream, incident family or lesson group.)*

## Known Patterns
*(Recurring failures and flaky tests, one line each with a pointer to the context file.)*

## Operational Lessons
*(Human directives and hard-won lessons, in the format above.)*
