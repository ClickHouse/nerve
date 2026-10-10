# MEMORY.md — HOT Memory

## How to Use This File

**This is HOT memory — your L1 cache.** It is injected into the system prompt of every session (first 300 lines), so every byte here costs tokens on every turn. Only store things that are:
- **Frequently accessed** — context you need in most conversations
- **Currently relevant** — active projects, upcoming events, recent decisions
- **Hard to recall** — things `memory_recall` would struggle to find quickly

**What does NOT belong here:**
- Stable historical facts (employment start dates, home purchase details) → `memorize` to memU
- Resolved items (completed migrations, past events) → remove, already in memU
- Rarely referenced details (specific PR numbers, one-off fixes) → memU
- Full workstream state and session handoffs → `memory/contexts/<slug>.md` (the warm tier: one file per workstream, never injected, read on demand with the Read tool; only a stub stays here)

**Active Context entries are stubs, not state (hard rule):** ≤5 lines / ~500 bytes each — what it is, a one-line status, an "OPEN for <human>:" list if any, and the pointer `→ Read memory/contexts/<slug>.md BEFORE any <X> work.` When an entry outgrows that, move the body verbatim into the context file (header: title · `**Status:** …` · `**Moved from MEMORY.md:** YYYY-MM-DD`) and leave the stub. Retire a workstream: memorize the essentials → move its file to `memory/contexts/archive/` → drop the stub.

**Lifecycle of entries:**
1. New important fact → add here with `[added YYYY-MM-DD]`
2. Fact stays relevant → keep it; `[updated YYYY-MM-DD]` when it changes, `[dormant YYYY-MM-DD]` for idle workstreams
3. Fact becomes stale or rarely needed → `memorize` to memU, then remove from here
4. During hot-file housekeeping (your own, or a job you add): review tags, evict anything ~2 weeks old that hasn't been accessed, enforce the stub rule

**Every entry must carry a tag** — `[added …]`, `[updated …]`, or `[stable]` for permanent facts like contact info. This is how housekeeping — a manual sweep, or a maintenance job you add — tells fresh from stale; the built-in `memory-maintenance` cron only tends memU.

---

## Your Human
- **Name:** *(fill in)* `[stable]`
- **Email:** *(fill in)* `[stable]`
- **Timezone:** *(fill in)* `[stable]`
- **Primary chat:** *(platform and ID — e.g., Telegram @username)* `[stable]`
- **Quiet hours:** *(e.g., 02:00-12:00 — don't ping unless urgent)* `[stable]`

## Active Context

*(One stub per active workstream — status, open items for your human, pointer to its `memory/contexts/<slug>.md`. Shape: `**project-a — phase 1 live; phase 2 awaits go.** OPEN: pick a date. → Read memory/contexts/project-a.md BEFORE any project-a work. [added YYYY-MM-DD]`)*

## Operational Lessons

*(Hard-won lessons that affect how you work. Format: one bold headline stating the rule in the imperative and its origin — your human's explicit rule, or an incident — then a line of why and a tag. Evidence stays in the workstream's context file.)*
