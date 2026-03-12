# Memory System

## Overview

Nerve uses a dual-layer memory system:
1. **File-based** — Markdown files as the curated source of truth (identity, preferences, notes)
2. **memU** — Semantic index over conversations and files (SQLite-persisted)

The .md files are what you browse to see the agent's organized memory. memU is what the agent searches when it needs to find something from the past.

## File Memory

### Workspace Structure

```
workspace/
├── SOUL.md          # Agent personality and values
├── IDENTITY.md      # Agent identity and capabilities
├── USER.md          # User profile and preferences
├── AGENTS.md        # Agent behavior guidelines
├── TOOLS.md         # Tool usage notes
└── MEMORY.md        # HOT memory — L1 cache (loaded into system prompt every session)
```

### MEMORY.md

HOT memory — the agent's L1 cache. Loaded into the system prompt every main session, so size directly impacts token cost. Contains only frequently accessed, currently relevant context: active projects, upcoming deadlines, operational lessons. Each entry is tagged with `[added YYYY-MM-DD]` or `[stable]` for lifecycle management. Stale entries (>2 weeks, not accessed) get moved to memU via `memorize` and removed.

## memU Integration

memU provides semantic search over conversations and workspace files. It runs embedded as a Python library with SQLite persistence at `~/.nerve/memu.sqlite`.

### How it works

1. **Conversation indexing** — When a session closes (daily rotation, shutdown, crash recovery), all context messages are indexed into memU
2. **Explicit memorize** — The agent can use the `memorize` tool to save specific facts on demand
3. **Recall** — The agent uses `memory_recall` for semantic search, `conversation_history` for event-date queries, and `memory_records_by_date` for creation/update-date queries
4. **Pre-recall** — When a new SDK client is created, relevant memories are recalled and injected into the system prompt

### Session Lifecycle

- **Main session** rotates daily at midnight UTC: messages are indexed into memU, the session is archived (renamed to `main-YYYYMMDD-HHMMSS`), and a fresh `main` session is created
- **Other sessions** are indexed when their SDK client is discarded (error or shutdown)
- **Crash recovery** — On startup, sessions with a `connected_at` marker in metadata (meaning they had an active client that wasn't cleanly closed) are detected, indexed into memU, and archived

### Categories

Memory items are organized into categories defined in `config.yaml`. Categories are seeded on startup — only missing ones are created, so adding new entries is safe.

```yaml
memory:
  categories:
    - name: work
      description: "Work projects, PRs, on-call, reviews"
    - name: personal
      description: "Personal tasks, errands, finances"
```

### Knowledge Quality Filtering

Knowledge extraction is filtered at three levels to prevent generic programming/CS facts from polluting memory:

1. **Custom extraction prompt** — The knowledge memory type uses custom rules (same mechanism as event date resolution) that instruct the LLM to only extract project-specific, environment-specific, or non-obvious knowledge. General CS/DevOps facts that any experienced engineer would know are explicitly forbidden.

2. **Post-extraction relevance filter** (opt-in, `memory.knowledge_filter: true`) — After memorize completes, newly created knowledge items are batch-evaluated by a fast model (Haiku). Items identified as generic knowledge are auto-deleted. Disabled by default because it adds an extra API call per memorize and can be overly aggressive.

3. **Semantic deduplication** — See below.

### Deduplication

memU deduplicates at two levels:

1. **Content hash** — When a new memory's normalized text matches an existing one exactly, it increments `reinforcement_count` on the original instead of creating a duplicate.

2. **Semantic similarity** — When no exact hash match exists, cosine similarity is checked against all items of the same memory type. If the top match exceeds the configured threshold (default 0.85), the existing item is reinforced instead of creating a near-duplicate. This prevents items saying the same thing with different wording from proliferating.

Reinforced items rank higher in search results via salience-aware ranking: `similarity × log(reinforcement + 1) × recency_decay`.

### Configuration

memU uses three LLM profiles:
- **Chat** — Anthropic API for recall routing (claude-sonnet-4-6)
- **Fast** — Anthropic API for fact extraction and categorization (claude-haiku-4-5)
- **Embedding** — OpenAI text-embedding-3-small for vector search

Config in `config.yaml`:
```yaml
memory:
  chat_model: claude-sonnet-4-6       # recall routing
  fast_model: claude-haiku-4-5-20251001  # extraction & categorization
  embed_model: text-embedding-3-small
  categories: [...]  # see Categories section above
```

### Event Date Resolution

After a conversation is indexed, Nerve resolves temporal context for extracted event items. Event items get their `happened_at` field set via an LLM call (using `fast_model` / Haiku) that parses dates from the content. Non-event items (profiles, knowledge, behavior) stay timeless.

Date resolution runs regardless of whether the `memorize_file` call succeeded — on timeout, memU may have partially persisted items before the pipeline was cancelled, so those orphan items still need `happened_at` populated.

This uses the `anthropic` Python SDK directly (not the OpenAI-compatible endpoint) since it runs synchronously in a thread. The `anthropic` package must be installed — see `pyproject.toml`.

### Performance Optimizations

- **Vector cache** — All item and category embeddings are preloaded into memory at startup, eliminating repeated SQLite JSON parsing (~2s per search saved)
- **Fast model** — Extraction and category summary updates use Haiku instead of Sonnet
- **Disabled pipeline steps** — Route intention, sufficiency checks, and resource retrieval are disabled in the retrieve pipeline (saves 3+ LLM calls per recall)
- **Category embedding reuse** — Category ranking uses stored embeddings instead of re-embedding summaries on every recall
- **Client warmup** — Anthropic LLM clients are pinged during startup to force HTTP/2 connection establishment (avoids a cold-start hang on the first memorize call)
- **Memorize timeout** — Each memorize call is capped at 300s; if it hangs, it is cancelled, LLM clients are evicted (cache cleared + HTTP transport closed), a fresh client is created, and one retry is attempted
- **Per-call LLM timeout** — Base LLM client `.chat()` methods are wrapped with a 120s `asyncio.wait_for()` at init time (instance attribute shadowing). A single dead HTTP/2 connection fails fast instead of consuming the entire 300s pipeline budget. Hung calls log `memU LLM HUNG [profile]: no response after 120s (prompt=N chars)`
- **LLM call instrumentation** — Uses memU's `LLMInterceptorRegistry` (before/after/on_error hooks on `LLMClientWrapper`) to log every LLM call with profile, step ID, call type, prompt size, latency, and response size. Log format: `memU LLM call [profile/step_id]: kind, prompt=N chars` / `memU LLM done [profile/step_id]: Nms, response=N chars`
- **Concurrency guard** — Only one memorize operation runs at a time; concurrent sweeps are skipped to prevent SQLite deadlocks

### Agent Tools

Eight tools for interacting with memory:

```
memory_recall(query="user's work preferences", limit=10)
```
Semantic search across all indexed content (files + conversations). Best for topic-based queries.

```
conversation_history(date="2026-02-25", end_date="2026-02-26", limit=30)
```
Date-based lookup of event items by `happened_at`. Best for temporal queries like "what did I do yesterday?" Only returns items where `happened_at` is set (events), not profiles/knowledge/behavior.

```
memory_records_by_date(date="2026-03-02", end_date="2026-03-02", limit=100, updated=true)
```
Lists ALL records created or updated on a date, regardless of memory type. Queries by `created_at`/`updated_at`, not `happened_at`. Best for memory maintenance and auditing — "what was saved yesterday?"

```
memorize(content="User prefers dark mode in all apps", memory_type="profile")
```
Explicitly save a fact to memU. Use when told "remember this" or when learning something important mid-conversation.

```
memory_update(memory_id="abc123", content="Updated fact", memory_type="knowledge", categories="work,infrastructure")
```
Update an existing memory item's content, type, or category assignments. Re-embeds and regenerates category summaries automatically.

```
memory_delete(memory_id="abc123")
```
Delete a memory item. Use for wrong, duplicate, or stale memories.

```
category_update(category_id="abc123", summary="Updated summary", description="New description")
```
Update a category's summary and/or description. Re-embeds the category after update to keep vector search in sync.

All recall and history results include memory IDs (`id:abc123...`), enabling the agent to target specific items for update or deletion.

### Audit Log

All memU mutations are logged to a `memu_audit_log` table in nerve.db:
- **Actions**: `item_created`, `item_updated`, `item_deleted`, `category_created`, `category_updated`, `conversation_indexed`, `file_indexed`
- **Sources**: `bridge` (automatic), `web_ui` (web interface), `agent_tool` (agent actions)

Accessible via `GET /api/memory/memu/audit` and the Log tab in the web UI.

### Migration from OpenClaw

```bash
nerve migrate-openclaw              # index all OpenClaw conversations into memU
nerve migrate-openclaw --dry-run    # preview without indexing
nerve migrate-openclaw --timeout 180  # longer per-session timeout
```

Resumes automatically — skips sessions already indexed in the database.

## Web UI

- **Files tab** (`/files`) — File browser with markdown editing for workspace files
- **Memory tab** (`/memory`) — memU semantic memory browser with four sub-tabs:
  - **Facts** — Category-organized facts with editable category summaries (click to edit inline, saves + re-embeds)
  - **Timeline** — Chronological events with 6-month heatmap calendar (click a day to filter)
  - **Sources** — Resources grouped by day with expandable cards showing extracted items
  - **Log** — Full audit trail of all memU mutations with action/type filters

## API

- `GET /api/memory/files` — List workspace markdown files
- `GET /api/memory/file/{path}` — Read a file
- `PUT /api/memory/file/{path}` — Write a file
- `GET /api/memory/memu` — Get memU categories, items, and resources for the UI
- `POST /api/memory/memu/categories` — Create a new category
- `PATCH /api/memory/memu/categories/{id}` — Update a category (summary, description; re-embeds)
- `PATCH /api/memory/memu/items/{id}` — Update a memory item (content, type, categories)
- `DELETE /api/memory/memu/items/{id}` — Delete a memory item
- `GET /api/memory/memu/health` — memU health metrics and operation stats
- `GET /api/memory/memu/audit` — Audit log (filterable by action, target_type; paginated)
