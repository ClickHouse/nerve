# Sources

## Overview

Sources are cursor-based data streams that pull records from external services and persist them to a local inbox (`source_messages` table). The architecture follows a **producer/consumer** pattern inspired by Kafka:

- **Producers** (source runners) — Fetch records from external APIs, preprocess and condense them, persist to the inbox, and advance the source cursor. No agent processing happens here.
- **Consumers** (agent tools) — Read from the inbox using independent persistent cursors. Multiple consumers can read the same messages without interfering with each other.

```
PRODUCERS (sources):
  Source.fetch(cursor, limit)
      ↓
  Source.preprocess(records)          ← source-specific cleanup (e.g., strip email boilerplate)
      ↓
  Persist to source_messages table    ← inbox storage (with TTL)
      ↓
  SourceRunner._condense_long_content ← LLM condensation via Haiku (if condense: true)
      ↓
  Advance source cursor in SQLite     ← always, no processing dependency

CONSUMERS (agent tools):
  list_sources(consumer="inbox")      → discover sources + unread counts
  poll_source(source, consumer, ...)  → read NEW messages (advances consumer cursor)
  read_source(source, ...)            → browse historical messages (no cursor advancement)
```

## How It Works

### Ingestion (Producer Side)

1. **Fetch** — The source adapter calls an external API (gh CLI, gog CLI, Telethon) and returns normalized `SourceRecord` objects with an opaque cursor
2. **Preprocess** — Two-stage content cleanup:
   - **Source-specific** (`source.preprocess()`) — Each source can override this for programmatic cleanup. Gmail strips boilerplate paragraphs (legal disclaimers, unsubscribe blocks, tracking URLs). Default: no-op
   - **LLM condensation** (`condense: true`) — Records still over 800 chars are sent to a fast model (Haiku) that extracts only essential information. Configurable per source, runs concurrently with a 30s timeout per record, falls back to original content on failure
3. **Persist** — Records are saved to the `source_messages` table with a configurable TTL
4. **Advance** — Source cursor is saved to SQLite after successful persistence

### Consumption (Consumer Side)

Consumers read from the inbox using the `poll_source` agent tool with a named consumer cursor:

1. **Discover** — `list_sources(consumer="inbox")` shows available sources with unread counts for the consumer
2. **Poll** — `poll_source(source="github", consumer="inbox")` returns new messages since the consumer's last read position
3. **Act** — The agent creates tasks, memorizes facts, or ignores noise based on message content
4. **Advance** — Consumer cursor automatically advances to the last message read

**Key properties:**
- Consumer cursors are independent — multiple consumers can read the same source at different positions
- New consumer cursors initialize to the latest message (no backlog flooding)
- Consumer cursors expire after N days of inactivity (configurable, default: 2 days)
- Cross-source deduplication happens naturally because one agent session sees messages from all sources

### Default Consumer: `inbox-processor`

A persistent cron job (`inbox-processor`) runs every 15 minutes:
1. Calls `list_sources(consumer="inbox")` to check for unread messages
2. Polls each source with unread messages
3. Reviews all messages in one session — naturally deduplicates cross-source events (e.g., GitHub notification + email about the same issue)
4. Creates tasks, memorizes facts, or ignores noise

## Built-in Sources

### Gmail
- **Adapter:** `nerve/sources/gmail.py` — uses `gog gmail messages search` + `gog gmail get` CLI
- **Cursor:** Epoch timestamp from Gmail's `internalDate` (the receive timestamp Gmail uses for `after:` filtering)
- **First run:** Fetches emails from the last 24 hours (`newer_than:1d`)
- **Subsequent runs:** Uses `after:<epoch+1>` with client-side dedup (Gmail's `after:` has ~2s tolerance window)
- **Two-step fetch:** Search returns metadata only; body + `internalDate` are fetched per-message via `gog gmail get` (up to 5 concurrent)
- **Default schedule:** `*/15 * * * *` (every 15 min)

### GitHub
- **Adapter:** `nerve/sources/github.py` — uses `gh api notifications` CLI
- **Cursor:** ISO 8601 timestamp of the newest notification's `updated_at`
- **First run:** Fetches from the last 24 hours
- **Subsequent runs:** Uses `since=<cursor + 1s>` with `Z` suffix (not `+00:00` — the `+` in a URL query string is decoded as a space, breaking the filter)
- **Filter:** `participating=true` (assigned, review requested, mentioned)
- **Enrichment:** Each notification is enriched with actual content from the subject (PR/issue body, state, assignees, labels) and the latest comment, fetched in parallel (up to 5 concurrent `gh api` calls)
- **Default schedule:** `*/15 * * * *` (every 15 min)

### GitHub Events
- **Adapter:** `nerve/sources/github_events.py` — uses `gh api /users/{username}/events` CLI
- **Cursor:** Event ID (string, monotonically increasing)
- **First run:** Fetches the latest batch to establish a cursor baseline — no backfill of historical events
- **Subsequent runs:** Fetches newest events, stops at the last-seen event ID
- **Purpose:** Captures the user's **own** GitHub activity (pushes, PR creates/merges, reviews, comments, branch operations). Complements the notification source which only shows what *others* did involving you
- **Username:** Auto-detected from `gh auth` on first call, or set manually via `username` config
- **Repo filter:** Optional `repos` list — empty means all repos
- **Event types:** PushEvent, PullRequestEvent, IssueCommentEvent, IssuesEvent, PullRequestReviewEvent, CreateEvent, DeleteEvent, ForkEvent (with type-specific formatting); other event types get generic formatting
- **Note:** GitHub's Events API returns truncated payloads (e.g., PR titles and URLs may be missing). The source constructs URLs from repo name + number and handles missing fields gracefully
- **Default schedule:** `*/15 * * * *` (every 15 min)

### Telegram
- **Adapter:** `nerve/sources/telegram.py` — uses Telethon (user account API)
- **Mechanism:** Telegram's native `updates.getDifference` API — asks "what's new since this state?" using PTS/QTS/date
- **Cursor:** JSON-encoded Telegram state `{pts, qts, date, seq}` — the server's own update tracking
- **First run:** Calls `updates.getState()` to snapshot current position, returns 0 records (prevents flooding the agent with history)
- **Subsequent runs:** Calls `updates.getDifference(pts, date, qts)` to get only new messages since the saved state. Handles `DifferenceSlice` (large gaps, paginated) and `DifferenceTooLong` (gap too large, reset) gracefully
- **Default schedule:** `*/5 * * * *` (every 5 min)
- **Requires setup:** Run `nerve sync telegram` interactively once to authenticate with Telethon (phone number + code). The session is stored at `~/.nerve/telegram_sync.session`

## Configuration

Sources are configured under the `sync:` key in `config.yaml` / `config.local.yaml`:

```yaml
sync:
  message_ttl_days: 7               # How long to keep inbox messages
  consumer_cursor_ttl_days: 2       # Consumer cursors expire after N days of inactivity

  telegram:
    enabled: true
    api_id: 12345678              # From my.telegram.org
    api_hash: "abc123..."         # From my.telegram.org
    schedule: "*/5 * * * *"       # Every 5 minutes
    batch_size: 50                # Max records per fetch
    condense: false
    exclude_chats: []             # Chat IDs to skip
    monitored_folders: []         # Telegram folder names to filter

  gmail:
    enabled: true
    accounts:                     # One source per account, each with own cursor
      - personal@gmail.com
      - work@company.com
    keyring_password: "..."       # gog keyring password (in config.local.yaml)
    schedule: "*/15 * * * *"
    batch_size: 20
    condense: true                # Strip boilerplate + Haiku extraction for long emails

  github:
    enabled: true
    schedule: "*/15 * * * *"
    batch_size: 30
    condense: true                # Haiku extraction for long notifications

  github_events:
    enabled: true
    schedule: "*/15 * * * *"
    repos: []                     # Empty = all repos. Example: ["owner/repo"]
    username: ""                  # Auto-detect from gh auth
    batch_size: 50
    condense: false               # Events are naturally short
```

### Config Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Enable/disable this source |
| `schedule` | string | varies | Crontab expression or interval (`2h`, `30m`) |
| `batch_size` | int | `50` | Max records per fetch cycle |
| `condense` | bool | `false` | LLM-condense long records via `memory.fast_model` before storing |
| `message_ttl_days` | int | `7` | How long to keep inbox messages |
| `consumer_cursor_ttl_days` | int | `2` | Consumer cursors expire after N days of inactivity |

## Agent Tools

### `list_sources`
List available sources with message counts and optional consumer cursor status.

```
list_sources(consumer="inbox")
→ - github: 45 messages | consumer "inbox": 3 unread
  - gmail:user@example.com: 120 messages | consumer "inbox": 0 unread
  - telegram: 200 messages | consumer "inbox": 5 unread
```

### `poll_source`
Poll new messages from a specific source using a persistent consumer cursor. Advances the cursor.

```
poll_source(source="github", consumer="inbox", limit=50)
→ ## 3 message(s) from github
  ⚠️ UNTRUSTED DATA — ...
  ### [1/3] github: [myorg/myrepo] Issue #42 (assign)
  ...
```

**Security:** All message content is prefixed with an untrusted data warning. The agent is instructed not to follow instructions embedded in message content.

### `read_source`
Browse historical messages from a source without advancing any cursor. For debugging or review.

```
read_source(source="github", limit=5)
read_source(source="github", before_seq=1234, limit=10)   # paginate backwards
read_source(source="github", after_seq=1000, limit=10)    # paginate forwards
```

### `sync_status` (legacy)
Check the status of sync source fetch cursors. Kept for backward compatibility.

## CLI Usage

```bash
# Run all sources manually
nerve sync

# Run a specific source
nerve sync gmail
nerve sync github
nerve sync telegram
```

Output shows per-source results:
```
Running sync: all
  Running: gmail ... [OK] 5 ingested
  Running: github ... [OK] 3 ingested
  Running: telegram ... [OK] 0 ingested
```

## Monitoring

### Web UI
The Sources page (`/sources`) has three tabs:
- **Inbox** — Messages sorted by timestamp, with source-specific renderers (HTML for email, GitHub cards, markdown)
- **Runs** — Source fetch history, filterable (empty runs hidden by default)
- **Consumers** — Active consumer cursors with unread counts, linked sessions, and expiry times

### Database tables
- `sync_cursors` — Current source fetch cursor per source (producer-side)
- `consumer_cursors` — Per (consumer, source) read position with TTL and session linking
- `source_messages` — Inbox messages with `raw_content` (original HTML), `processed_content` (LLM-condensed), TTL-based expiry
- `source_run_log` — Per-run diagnostics (records ingested, errors, timestamps)
- `cron_logs` — Job execution history (source jobs use `source:<name>` as job ID)

### API Endpoints
- `GET /api/sources/overview` — Per-source stats (cursor, message count, storage, run history)
- `GET /api/sources/messages` — Paginated inbox messages
- `GET /api/sources/runs` — Source fetch run history
- `GET /api/sources/consumers` — Active consumer cursors with unread counts
- `POST /api/sources/{source}/sync` — Trigger source sync manually
- `POST /api/sources/sync-all` — Trigger all sources

## Adding a New Source

1. **Create the adapter** at `nerve/sources/mysource.py`:

```python
from nerve.sources.base import Source
from nerve.sources.models import FetchResult, SourceRecord

class MySource(Source):
    source_name = "mysource"

    def __init__(self, config: dict):
        self._config = config

    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        # Fetch records from your service
        # cursor is opaque — you define the semantics (timestamp, ID, page token, etc.)
        records = []
        next_cursor = cursor

        # ... fetch logic here ...
        # Build SourceRecord objects:
        # SourceRecord(id, source, record_type, summary, content, timestamp, metadata)

        return FetchResult(
            records=records,
            next_cursor=next_cursor,
            has_more=False,
        )

    async def preprocess(self, records: list[SourceRecord]) -> list[SourceRecord]:
        # Optional: source-specific content cleanup before storage.
        # Default (inherited from Source): returns records unchanged.
        return records

    async def close(self) -> None:
        # Optional cleanup (disconnect clients, etc.)
        pass
```

2. **Add config dataclass** in `nerve/config.py`:

```python
@dataclass
class MySourceSyncConfig:
    enabled: bool = True
    schedule: str = "*/15 * * * *"
    batch_size: int = 50
    condense: bool = False
    # ... source-specific fields ...

    @classmethod
    def from_dict(cls, d: dict) -> MySourceSyncConfig:
        return cls(
            enabled=d.get("enabled", True),
            schedule=d.get("schedule", "*/15 * * * *"),
            batch_size=d.get("batch_size", 50),
            condense=d.get("condense", False),
        )
```

Add it to `SyncConfig`:
```python
@dataclass
class SyncConfig:
    telegram: TelegramSyncConfig = ...
    gmail: GmailSyncConfig = ...
    github: GitHubSyncConfig = ...
    mysource: MySourceSyncConfig = field(default_factory=MySourceSyncConfig)
```

3. **Register in the registry** at `nerve/sources/registry.py`:

```python
# In build_source_runners():
ms = config.sync.mysource
if ms.enabled:
    from nerve.sources.mysource import MySource
    source = MySource(config={...})
    runners.append(SourceRunner(
        source=source, db=db,
        batch_size=ms.batch_size,
        condense=ms.condense,
        condense_model=condense_model,
        condense_client_factory=condense_factory,
        ttl_days=ttl_days,
    ))
```

4. **Add config** to `config.yaml`:
```yaml
sync:
  mysource:
    enabled: true
    schedule: "*/15 * * * *"
```

That's it. The source will auto-register as an APScheduler job on next restart. Messages will appear in the inbox and be picked up by the `inbox-processor` consumer.

## Adding a New UI Renderer

The inbox detail panel uses source-specific renderers to display message content. Each source type gets an appropriate renderer (e.g., emails render HTML, GitHub shows a repo card + markdown).

### Renderer Architecture

```
web/src/components/Sources/
├── MessageContent.tsx              ← Dispatcher (picks renderer by source type)
└── renderers/
    ├── index.ts                    ← Registry: source name → renderer type mapping
    ├── MarkdownRenderer.tsx        ← Default: ReactMarkdown with prose styles
    ├── EmailRenderer.tsx           ← Gmail: sandboxed iframe for HTML + text toggle
    └── GitHubRenderer.tsx          ← GitHub: repo/PR header card + markdown body
```

### Steps

1. **Create the renderer** at `web/src/components/Sources/renderers/MyRenderer.tsx`:

```tsx
import { MarkdownRenderer } from './MarkdownRenderer';

interface Props {
  content: string;
  rawContent?: string | null;
  metadata?: Record<string, any>;
  summary: string;
}

export function MyRenderer({ content, metadata, summary }: Props) {
  return (
    <div>
      {/* Custom header/card/etc */}
      <MarkdownRenderer content={content} />
    </div>
  );
}
```

2. **Register the source type** in `renderers/index.ts`
3. **Handle in dispatcher** — add the case to `MessageContent.tsx`

## Architecture

### Key Files

| File | Purpose |
|------|---------|
| `nerve/sources/models.py` | `SourceRecord`, `FetchResult`, `IngestResult` dataclasses |
| `nerve/sources/base.py` | `Source` abstract base class |
| `nerve/sources/runner.py` | `SourceRunner` — pure ingestion pipeline (fetch → persist → condense → advance) |
| `nerve/sources/processor.py` | Legacy agent prompt building (unused by runner, may be used by tools) |
| `nerve/sources/registry.py` | Config → `list[SourceRunner]` factory |
| `nerve/sources/telegram.py` | Telegram adapter (Telethon) |
| `nerve/sources/gmail.py` | Gmail adapter (gog CLI) |
| `nerve/sources/github.py` | GitHub notifications adapter (gh CLI) |
| `nerve/sources/github_events.py` | GitHub user events adapter (gh CLI) |
| `nerve/agent/tools.py` | Consumer tools: `list_sources`, `poll_source`, `read_source` |
| `web/src/components/Sources/` | Source message renderers (email, GitHub, default markdown) |

### Cursor Design

**Source cursors** (producer-side): Each source owns its cursor semantics. The framework treats cursors as opaque strings stored in the `sync_cursors` SQLite table. Sources return the next cursor in `FetchResult.next_cursor` — the runner stores it as-is. Cursors advance after successful ingestion (persist to inbox).

**Consumer cursors**: Stored in `consumer_cursors` table with composite key `(consumer, source)`. Each consumer has an independent cursor per source, using the implicit `rowid` of `source_messages` as the offset. New cursors initialize to `MAX(rowid)` (no backlog). Cursors expire after `consumer_cursor_ttl_days` of inactivity.

**Inclusive APIs:** Both GitHub and Gmail APIs use inclusive cursor semantics (returning records `>= cursor`). Sources handle this by advancing the cursor at query time (+1s) and applying client-side dedup as a safety net.

**Concurrency safety:** Each `SourceRunner` holds an `asyncio.Lock` to prevent concurrent execution of the same source. If a cron schedule fires while a manual sync is in progress, the second call returns immediately with 0 records.

### Security

All message content returned by `poll_source` and `read_source` is prefixed with an untrusted data warning. Source messages come from external services and may contain prompt injection attempts. The agent is instructed to act only on factual information (sender, subject, PR numbers) and never follow instructions embedded in message content.
