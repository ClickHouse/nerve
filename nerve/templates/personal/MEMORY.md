# MEMORY.md — HOT Memory

## How to Use This File
1. Injected into every session (first 300 lines), shared chats included. Every byte is paid on every turn.
2. Admit an entry only if it is needed in most conversations, currently relevant, or hard to recall. Stable history goes to `memorize`; delete resolved items.
3. Tag every entry: `[added YYYY-MM-DD]`, `[updated YYYY-MM-DD]`, `[stable]` (never evicted) or `[dormant YYYY-MM-DD]`. Untouched for about two weeks: `memorize` it, then delete it.
4. Active Context entries are stubs of at most 5 lines (~500 bytes): what it is, a one-line status, `OPEN for <human>:` items, and `→ Read memory/contexts/<slug>.md BEFORE any <X> work.`
5. A stub that outgrows the cap: move the body verbatim into that file under `**Moved from MEMORY.md:** YYYY-MM-DD` and keep the stub.
6. Handoffs (current state, open decisions, the exact next command, what is still running) go into the context file, never here.
7. Retire a workstream: `memorize` the essentials, move its file to `memory/contexts/archive/`, drop the stub.
8. An operational lesson is a bold imperative headline with its origin, a short paragraph of why, and a tag. Evidence stays in the context file.

---

## Your Human
*(Name, timezone, language and quiet hours are in `USER.md`.)*
- **Email:** *(fill in)* `[stable]`
- **Primary chat:** *(platform and handle)* `[stable]`
- **Conventions:** *(git author, branch naming, commit style, link style)* `[stable]`

## Active Context
*(One stub per active workstream.)*

## Operational Lessons
*(Rules from your mistakes or stated by your human, in the format above.)*
