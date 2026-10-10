# MEMORY.md — HOT Memory

## How to Use This File

**This is HOT memory — your L1 cache.** It is injected into the system prompt of every session (first 300 lines), so every byte here costs tokens on every request. Keep ONLY:
- **Rules needed in most sessions** — directives and hard-won lessons: one bold headline, a line of why, a tag
- **One-line facts** — currently relevant: active investigations, ongoing blockers
- **≤5-line stubs** pointing at `memory/contexts/<slug>.md` (see below)

**What does NOT belong here:**
- Stable historical facts → `memorize` to memU
- Resolved items → remove (already in memU from when they were active)
- Rarely referenced details → memU; per-task detail → the task file
- Workstream state, incident evidence, refuted theses → `memory/contexts/<slug>.md`

**Active Context entries are stubs, not state (hard rule):** ≤5 lines — what it is, the one-line status or rule, and a greppable pointer `→ Read memory/contexts/<slug>.md § <anchor>` saying when to read it. When an entry exceeds 5 lines, move the body VERBATIM into `memory/contexts/<slug>.md` (header: title · `**Status:** …` · `Moved from MEMORY.md: YYYY-MM-DD`) and leave the stub. Retired workstreams go to `memory/contexts/archive/`. Nerve does not load context files — Read them on demand.

**Volatile facts are pointers, not values.** Toolchain pins, schedules, caps and settings "live in `<file>` — read it"; when this file disagrees with the source, trust the source and fix this entry.

**Every entry carries a tag** — `[added YYYY-MM-DD]`, `[updated …]`, or provenance such as `[operator directive YYYY-MM-DD]`. Human directives are never evicted for age. Anything else ~2 weeks old and unaccessed → `memorize`, then remove.

**Recall before acting:** phrase `memory_recall` queries as full questions ("What do we know about the flaky test X?"), several per task.

---

## Task Context
<!-- Active task state, current blockers, in-progress work — one-liners -->

## Active Context
<!-- One ≤5-line stub per workstream / incident family / lesson group → memory/contexts/<slug>.md -->

## Known Patterns
<!-- Recurring issues, flaky tests, common failure modes you see regularly -->

## Operational Lessons
<!-- Hard-won knowledge that affects how you approach work; human directives tagged with provenance -->
