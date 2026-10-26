# Sources

## Overview

Sources are cursor-based data streams that persist records to a local inbox
(`source_messages` table). Most fetch an external service. Channel sources
drain records that Slack or Telegram already delivered to a local buffer. The
architecture follows a **producer/consumer** pattern inspired by Kafka:

- **Producers** (source runners) — Fetch or drain records, preprocess them, persist them to the inbox, condense long ones, and advance the source cursor. No agent processing happens here.
- **Consumers** (agent tools) — Read from the inbox using independent persistent cursors. Multiple consumers can read the same messages without interfering with each other.

```
PRODUCERS (sources):
  Source.fetch(cursor, limit)
      ↓
  Source.preprocess(records)          ← source-specific cleanup (e.g., strip email boilerplate)
      ↓
  InboxFilter.partition(records)      ← guardrail: drop records that fail allow/deny rules
      ↓
  Persist to source_messages table    ← inbox storage (with TTL)
      ↓
  SourceRunner._condense_long_content ← LLM condensation via Haiku (if condense: true)
      ↓
  SourceRunner._run_drop_hook         ← dropped records to the drop hook (if drop_hook is set)
      ↓
  Advance source cursor in SQLite     ← always, no processing dependency

CONSUMERS (agent tools):
  list_sources(consumer="inbox")      → discover sources + unread counts
  poll_source(source, consumer, ...)  → read NEW messages (advances consumer cursor)
  read_source(source, ...)            → browse historical messages (no cursor advancement)
```

## How It Works

### Ingestion (Producer Side)

1. **Fetch** — The source adapter reads an external service (gh CLI, gog CLI, Telethon) or a local channel buffer and returns normalized `SourceRecord` objects with an opaque cursor
2. **Preprocess** (`source.preprocess()`) — Each source can override this for programmatic cleanup. Gmail strips boilerplate paragraphs (legal disclaimers, unsubscribe blocks, tracking URLs). Default: no-op
3. **Guardrail** — An optional `InboxFilter` drops records that fail the source's allow/deny rules (see [Guardrails](#guardrails-inbox-filtering)). Dropped records are never persisted
4. **Persist** — Records are saved to the `source_messages` table with a configurable TTL. A record identical to the stored one is skipped: it is not condensed and not counted as ingested.
5. **LLM condensation** (`condense: true`) — Stored records over 800 chars are sent to a fast model (Haiku) that extracts only essential information, saved as `processed_content`. Configurable per source, runs concurrently with a 30s timeout per record, falls back to original content on failure
6. **Advance** — Source cursor is saved to SQLite after successful persistence

### Consumption (Consumer Side)

Consumers read from the inbox using the `poll_source` agent tool with a named consumer cursor:

1. **Discover** — `list_sources(consumer="inbox")` shows available sources with unread counts for the consumer
2. **Poll** — `poll_source(source="github", consumer="inbox")` returns new messages since the consumer's last read position
3. **Act** — The agent creates tasks, memorizes facts, or ignores noise based on message content
4. **Advance** — Consumer cursor automatically advances to the last message read

**Key properties:**
- Consumer cursors are independent — multiple consumers can read the same source at different positions
- New consumer cursors initialize to the latest message (no backlog flooding)
- Consumer cursors expire after N days of inactivity (configurable, default: 2 days); a `messages` gate that checks the consumer renews the cursors it reads (see [cron.md](cron.md#messages))
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

### IMAP
- **Adapter:** `nerve/sources/imap.py` — plain `imaplib`, no CLI or provider API
- **Scope:** Any IMAP mailbox the Gmail source cannot reach (that one goes through the `gog` CLI / Gmail API), e.g. GMX, Fastmail, a company server
- **Instances:** One source per configured account (`imap:<label>`), each with an independent cursor
- **Cursor:** `<UIDVALIDITY>:<max_uid>` — UIDs are only stable within a UIDVALIDITY, so a server-side reset is detected and re-baselined instead of trusted
- **First run:** `SINCE` window of `initial_lookback_days` (default 1)
- **Subsequent runs:** `UID SEARCH <last+1>:*`, filtered client-side (the `N:*` range always returns at least the highest UID); oldest UIDs are processed first when a batch is full
- **Failure policy:** A search or message fetch/parse failure fails the batch and leaves the cursor unchanged. `SourceRunner` reports the error and applies its health/backoff policy. Fix or remove a permanently malformed message, then the next run retries it; manually advancing the cursor is an operator recovery action when skipping that UID is confirmed safe
- **Blocking I/O:** the whole IMAP conversation runs in a worker thread via `asyncio.to_thread`
- **Credentials:** passwords live in `sync.imap.passwords` keyed by username (put them in `config.local.yaml`); an account with no password is skipped with a warning instead of failing the sync
- **Optional image pass:** see below
- **Default schedule:** `*/30 * * * *` (every 30 min)
- **Disabled by default**

#### Reading mail that is only legible as an image

Some senders put the part you actually care about in an inline image — a
scan, a photo, a rendered document — so the body text is useless to an
agent. `sync.imap.match` singles those messages out and `sync.imap.vision`
runs a multimodal model over the image at ingest time, so the inbox
consumer gets plain text.

Both are inert by default: with no match rules, this is a plain mailbox
source and no model is ever called.

`vision.prompt` and `vision.answer_key` are a **matched pair** — the prompt
tells the model which label to emit, and the parser reads the line after
exactly that label. Change one without the other and every message silently
comes back as `unknown_answer`, which looks like the model degrading rather
than a config mistake. Leave `answer_key` empty to just take the first
non-empty line.

```yaml
sync:
  imap:
    enabled: true
    accounts:
      - host: imap.example.net
        username: me@example.net
        label: scans              # source name becomes imap:scans
    passwords:                    # config.local.yaml
      me@example.net: "..."
    match:
      sender_contains: ["scans@example.net"]   # substrings of From:
      attachment_contains: ["scan"]            # substrings of Content-ID / filename
      only_matched: true                       # drop everything else at the source
    vision:
      enabled: true
      model: ""                   # empty = memory.fast_model
      prompt: |
        Read the scan. Answer with EXACTLY one line:
        Sender: <company or person, or "unreadable">
      answer_key: "Sender:"
      unknown_answer: "unreadable"
      summary: "[{label}] scan from {answer}"
      content: "{vision}\n\nSubject: {subject}\nDate: {date}"
      summary_unknown: "[{label}] unreadable scan"
      content_unknown: "The image could not be read.\n\nSubject: {subject}"
```

Wording templates accept `{label} {answer} {vision} {subject} {sender}
{date} {body}`, where `{answer}` is the parsed line and `{vision}` the full
model reply. An unknown placeholder logs a warning and leaves the template
visible rather than failing the fetch.

### GitHub
- **Adapter:** `nerve/sources/github.py` — uses `gh api notifications` CLI
- **Cursor:** ISO 8601 `updated_at` of the first notification in the last listing (GitHub lists the most recently notified thread first)
- **Paging:** every page since the cursor is read in one run (`gh api --paginate`), so a backlog after downtime is ingested in full; `batch_size` does not cap this source
- **First run:** Fetches from the last 24 hours
- **Subsequent runs:** Uses `since=<cursor>` (inclusive: the cursor's own second is listed again, and an unchanged re-listed thread is skipped by storage) with `Z` suffix (not `+00:00` — the `+` in a URL query string is decoded as a space, breaking the filter)
- **Filter:** `sync.github.inbox` decides which threads are listed. `participating` (the default) sends `participating=true`: only the threads the account takes part in (assigned, review requested, mentioned, its own, commented on). `all` leaves that parameter out, so GitHub lists the account's whole inbox as its GitHub watch settings produce it: the participating threads plus `subscribed` threads of watched repositories (new issues and pull requests, per the repository's watch or Custom watch setting), `ci_activity` for the account's own workflow runs, and whatever else GitHub puts in the inbox; the guardrails pick within it (see [GitHub reason guardrail](#github-reason-guardrail)). Both modes send `all=true` (read threads too). GitHub marks a thread read as soon as a mail client renders the notification email with its images, which can happen seconds after the notification, so a listing of unread threads alone can miss a thread before anyone has seen it; a re-listed thread whose stored record is unchanged is skipped by storage, and the record carries no read state. A thread that GitHub lists as read and that was last read more than a minute before its update is left out: GitHub moves the thread of the account's own merge or review and leaves it read. Note that in participating mode it never returns `ci_activity` — workflow-run notifications are not "participating", so CI never reaches the inbox through this source in participating mode (verified against a live account: 25 `ci_activity` listed with `all=true` alone, 0 once `participating=true` is added)
- **Enrichment:** Each notification is enriched with actual content from the subject (PR/issue body, state, assignees, labels) and the latest comment, fetched in parallel (up to 5 concurrent `gh api` calls)
- **Metadata:** `reason`, `repo_name`, `actors` (every login involved) and `ci_branch` (branch of a CheckSuite run, `""` otherwise) — all four are guardrail-filterable, see [Guardrails](#guardrails-inbox-filtering)
- **Default schedule:** `*/15 * * * *` (every 15 min)
- **Hosted mode:** with `sync.github.mode: hosted` the channel gateway polls GitHub instead; see below

#### Hosted GitHub source

With `sync.github.mode: hosted`, this process does not poll GitHub and needs no
`gh` login. The shared channel gateway (the one that serves
[hosted Slack](config.md#hosted-mode), set up with the same `channels.hosted`
settings) polls GitHub for the agent with the GitHub credential connected to
that agent, builds each record the way the polling source does, enrichment
included, and keeps the records until Nerve reads them, also while Nerve is not
running. Nerve reads them over the hosted channel streams.

- **What arrives:** `github_notification` records, one for each notification
  thread whose `updated_at` moved, with the summary, content and metadata of the
  polling source. With `pr_watch: true` (the default) also `github_pr_activity`
  records: one whenever one of the agent's own open pull requests changes —
  its state, head commit, review decision, CI checks, or a comment or review by
  someone else. A pull request seen for the first time gives no record. Such a
  record has the notification metadata plus `number`, `state`, `head`,
  `review_decision` and `changes`, a list of `state`, `head`, `ci`, `review`
  and `comment`. The agent is always its author, so its `reason` is always
  `author`, and its `actors` are the reviewers and commenters. Only
  `allow_repos` and `deny_repos` apply to it, at the gateway and here; the
  actor, reason and CI branch lists are not checked for it (see
  [Pull request activity records](#github-pull-request-activity-records)).
- **The credential is the switch.** The gateway polls GitHub for an agent only
  while a GitHub credential is connected to that agent and Nerve subscribes,
  which it does while `enabled` is true. Your operator can therefore set
  `sync.github.enabled: true` in `config.yaml` or `config.local.yaml`, which
  outrank the `settings.yaml` that the setup page writes. GitHub then arrives
  once a GitHub credential is connected to the agent and stops when it is
  removed, with nothing to switch in Nerve. The Sources step of the setup page
  shows GitHub as managed by the channel gateway, with no switch, and refuses a
  save that includes it (`409`, naming `github`); Gmail and Telegram work as
  before. Until a gateway stream opens, the step says that it is waiting for
  the channel gateway and that a GitHub credential may still need to be
  connected: Nerve cannot tell those two cases apart. Once a stream is open,
  it says that the stream is connected and that records arrive once a GitHub
  credential is connected to the agent, and never that GitHub is connected,
  also when the page holds the switch: the streams carry every hosted
  provider, so a hosted Slack opens them too, and Nerve cannot see whether a
  credential is connected. A `false` in either file keeps GitHub off the same
  way. Without the key in either file, the Sources step switches GitHub as
  before.
- **The filters are published from this config.** Nerve sends the gateway the
  guardrail patterns of `sync.github` (`allow_repos`, `deny_repos`,
  `allow_actors`, `deny_actors`, `allow_reasons`, `deny_reasons`,
  `deny_ci_branches`), `pr_watch` and `inbox` whenever a stream opens and again
  on every reload. The gateway keeps the newest set and applies it before it
  stores a record, so a record that the filters drop never reaches this agent and is
  never a reason to start it. The guardrail still runs here too. A list that the
  gateway cannot take — more than 256 patterns, or a pattern longer than 256
  bytes or with a control character — goes to the gateway wider, never
  narrower: an allow list is left out and a deny list keeps the patterns that
  fit, and the local guardrail drops the rest. Blank patterns are not sent; here
  a blank allow pattern matches only an empty value, which GitHub never gives a
  repo, an actor or a reason. If the whole subscription is still larger than
  the gateway's frame limit, it goes with no patterns at all, and the guardrail
  here does all the filtering. With `enabled: false` Nerve tells the gateway
  that it subscribes to nothing.
- **The gateway must know the subscriptions.** A gateway that predates hosted
  sources closes a stream that publishes them, hosted Slack traffic included,
  so update the gateway before an agent switches to `mode: hosted`.
- **`inbox: all` needs a gateway that supports it.** The gateway lists the
  threads that `inbox` names, as the polling source does. Nerve leaves the
  `inbox` member out of the subscription unless it is `all`, and the gateway
  reads a missing one as `participating`, so with the default nothing changes
  and an older gateway is unaffected. A gateway that predates `inbox` refuses a
  subscription that carries it and closes the stream, hosted Slack traffic
  included, so update the gateway before you set `inbox: all` in hosted mode.
- **Same source, same name.** The records go to a local buffer, and the
  `source:github` runner drains it. The drop hook, `condense`, health checks,
  consumer cursors, `messages` gates and the Sources page work as they do for
  the polling source. The drain also runs on `sync.github.schedule`, as a
  fallback; that run is not the instance's next scheduled task while nothing
  waits in the buffer (normally the drain on delivery has taken everything), so
  a platform that parks the machine does not wake it for the fallback.
- **Runs on delivery.** As soon as records arrive, the runner drains them, and
  the jobs gated on `messages` for `github`, or for any source, run once
  `cron.delivery_debounce_seconds` (default 60) after the first record of a
  burst; see [Runs on delivery](cron.md#runs-on-delivery). Gate a pull request
  monitor on `messages` with `sources: [github]` rather than on
  `github_pr_activity`, which always passes in hosted mode.
- **The events source stays off.** `github_events` reads your own activity
  with `gh` on this machine, which hosted GitHub does not set up. While the
  mode is `hosted`, the GitHub switch of the setup page's Sources step and
  `nerve init` write `sync.github_events.enabled: false`, where in poll mode
  they switch it on and off together with GitHub. On a machine with a `gh`
  login it can still be switched on in the settings, until the GitHub switch
  is saved again.
- **A switched-off hosted Slack holds GitHub back.** Nerve reads the events of
  every hosted provider from one ordered inbox at the gateway, and it stops
  reading while a hosted channel cannot take events. With `slack.mode: hosted`
  and `slack.enabled: false`, Nerve therefore reads nothing from the gateway:
  the GitHub records wait there too, no delivery starts a job, and past the
  gateway's limit of stored records the oldest are dropped. They arrive once
  Slack is switched on again. While GitHub is hosted, keep a hosted Slack
  switched on, or switch both off; `nerve doctor` warns about this
  combination. This is a known limitation: reading each provider's events on
  its own, so that GitHub records arrive while hosted Slack is off, is a
  follow-up.
- **Restart-only.** A change of `mode` takes effect at the next restart. To go
  back from `hosted` to `poll`, switch the source off first: set
  `enabled: false` and reload while the mode is still `hosted`. Nerve then
  tells the gateway that it subscribes to nothing (a subscriptions frame of
  `{"version":"1"}`), and the gateway stops polling GitHub for the agent at
  once. Then set `mode: poll`, switch `enabled` back on, and restart. In poll
  mode Nerve publishes no subscriptions at all. The gateway also counts an agent
  as subscribed to nothing when a stream opens and publishes no subscriptions
  within a short grace period, or when Nerve no longer serves the stream
  endpoint. Until it notices, though, it keeps polling and delivering records
  that Nerve no longer takes, which is why the reload comes first.

```yaml
sync:
  github:
    mode: hosted
    pr_watch: true
    deny_reasons: ["subscribed"]   # applied by the gateway and again here
```

`nerve doctor` checks the hosted settings and names the jobs that still rely
on the `github_pr_activity` gate.

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

### Telegram account sync
- **Adapter:** `nerve/sources/telegram.py` — uses Telethon (user account API).
  This is the pull sync for your own Telegram account, and is separate from the
  `telegram:observed` bot-channel source below
- **Mechanism:** Telegram's native `updates.getDifference` API — asks "what's new since this state?" using PTS/QTS/date
- **Cursor:** JSON-encoded Telegram state `{pts, qts, date, seq}` — the server's own update tracking
- **First run:** Calls `updates.getState()` to snapshot current position, returns 0 records (prevents flooding the agent with history)
- **Subsequent runs:** Calls `updates.getDifference(pts, date, qts)` to get only new messages since the saved state. Handles `DifferenceSlice` (large gaps, paginated) and `DifferenceTooLong` (gap too large, reset) gracefully
- **Default schedule:** `*/5 * * * *` (every 5 min)
- **Requires setup:** Run `nerve sync telegram` interactively once to authenticate with Telethon (phone number + code). The session is stored at `~/.nerve/telegram_sync.session`

### Chat channels (Slack, Telegram)

A chat channel can feed the inbox with the group traffic it already receives.

- **Adapter:** `nerve/sources/channel.py` — one `ChannelSource` per enabled
  channel source
- **Mechanism:** for each message delivered by the channel transport, matching
  messages are written to the `channel_observations` buffer. `ChannelSource`
  drains that buffer into the inbox; it does not poll the chat service.
- **Source name:** `<channel>:observed` — `slack:observed`,
  `telegram:observed` — so a cron gate reads `sources: [slack:observed]`.
  Compound like `gmail:<account>`, and deliberately not the bare channel name:
  `telegram` is already the Telethon pull source for your *user* account, and
  sharing a name would mean sharing a cron job id and a cursor key.
- **Config:** `slack.source.*` / `telegram.source.*`, not `sync.*` — see
  [config.md](config.md). The runner carries its own `schedule` because there
  is no `config.sync.<name>` section to look one up in.
- **Default schedule:** `*/5 * * * *`
- **Not the next scheduled task while nothing waits.** Only the running
  instance writes the buffer, so nothing is added to it while the machine is
  parked; what arrives meanwhile is delivered after the next wake. A scheduled
  drain with nothing waiting is therefore not counted in `next` of
  [What runs next](cron.md#what-runs-next) nor in the platform activity
  report's `nextScheduledAt` (it is still listed, with forecast `skip`), so a
  platform that parks an idle machine is not kept up, or woken, for each
  scheduled drain. A drain with messages waiting since the last one counts
  from the next forecast on (a buffered message is not announced, so at most
  one activity report interval later): the machine stays up, or is woken, to
  take them, and the jobs gated on `<channel>:observed` run after it as before.
  While the instance runs, the schedule drains the buffer as it always did.
- **Cursor:** the buffer's autoincrement row id. `AUTOINCREMENT` is
  load-bearing: the buffer is pruned, and a plain SQLite rowid is reused after
  the highest row is deleted, which would reissue ids the cursor has already
  passed and skip everything after them.
- **Idempotent:** the record id is `<conversation>:<message_id>`, so the same
  message collected twice collapses on the inbox's `(source, id)` key.
- **Buffer target:** `max_stored_messages` (default 10 000) is one budget per
  *transport*, covering all of its watched conversations together, not one per
  conversation. Every 100 writes, Nerve drops the oldest rows back to that
  target and logs a warning. The buffer can therefore exceed it by up to 99
  rows. Values below 100 are refused and 100 used instead: `0` reads like "no
  limit" but tells the trim to keep nothing. Daily cleanup removes rows after
  `sync.message_ttl_days`.
- **Backlog:** one run keeps fetching while the buffer reports more, up to 20
  batches, so a chat busier than one `batch_size` per tick still drains. Each
  batch is persisted before the next starts, so a run that stops at the bound
  resumes there and logs a warning. Raise `batch_size` if that repeats.
- **Requires setup (Telegram only):** with privacy mode enabled, a non-admin bot
  does not receive ordinary group messages. Make it an admin or disable privacy
  mode with BotFather. See Telegram's
  [privacy-mode documentation](https://core.telegram.org/bots/faq#what-messages-will-my-bot-get).

#### Routing: which messages get collected

For eligible shared-channel and group-chat messages, the live route and source
route use separate rules. Access rules (`slack.allow_users`,
`telegram.allowed_users`, …) decide who may drive the agent.
`<channel>.source.*` decides whose traffic may reach the inbox. A message can
take either route, both routes, or neither.

| Live route | Source grant | `include_handled_messages` | Result |
|---|---|---|---|
| accepts | no match | — | channel only |
| accepts | matches | `false` (default) | channel only |
| accepts | matches | `true` | channel **and** source |
| refuses | matches | — | source only |
| refuses | no match | — | dropped |

By default, a message accepted by the live route is not also copied to the
source. Set `include_handled_messages: true` only when source consumers should
receive the same message later. "Handled" means accepted for live routing, not
that the agent produced a reply.

Whether the live route accepts differs by transport, and only affects which row
of that table you land in:

- **Slack** accepts a shared-channel message for live routing when it mentions
  the bot or continues a thread with an active session, and the sender passes
  the access policy. A matching source can still collect a message that fails
  the live access policy.
- **Telegram** has no "addressed to me" test: authorization alone decides.
- **Hosted Slack** (`slack.mode: hosted`): the gateway sends a separate
  observation for each message it admits for observation, and a message that
  also starts a turn arrives twice. The live route accepts the turn copy, and
  the table above decides the observation copy. The gateway gives no trusted
  names, so only a channel ID or member ID can grant; a name matches only the
  display name in the event, which can deny but never grant. Nerve
  acknowledges an observation to the gateway only after the buffer row is
  written.
  With `slack.source.gateway_grant: true`, the gateway's own grant is enough:
  it sends observations only for channels where the agent's channel policy
  allows them, and Nerve collects all of them without a local allow list. A
  local allow list still narrows that, and the deny lists still apply.

**Never collected, whatever the config says:** direct and group DMs, the
agent's own messages, other bots and apps, join/leave and similar service
events, malformed events, and Telegram `/pair` commands. Duplicate buffered
records collapse to one inbox record on the `(source, id)` key.

**The source is off by default and fail-closed.** An empty allow list collects
nothing rather than everything — the inverse of the access rules, because this
is a standing grant to record other people's messages. `enabled: true` with
nothing listed logs a warning and registers no drain. Hosted Slack with
`gateway_grant: true` is the one exception: there the gateway's grant is the
written one.

#### Threat model

**Everything collected is untrusted input.** Most of it comes from people not
authorized to instruct the agent — that is the usual reason to watch a room —
so treat a buffered message as attacker-controlled text an agent will later
read. What actually protects you:

- `<channel>.source.*` decides **whose messages are collected** before anything
  is written.
- At drain time, Nerve rechecks conversation and sender deny rules against raw
  IDs. Name-based denies apply only before buffering.
- Inbox filters inspect metadata, not content. They cannot distinguish a report
  from an instruction. Polling advances a cursor; it does not delete inbox
  records. Daily cleanup removes them after their TTL expires. Agents see them
  only through explicit source reads or cron gates, never in a live turn.

So scope the grant to conversations whose participants you would already trust
to file a ticket, and treat any workflow that acts on collected content without
review as accepting prompt injection.

**Slack lookup cost.** Names and globs use cached `conversations.info` or
`users.info` lookups. An ID skips the lookup, but only in uppercase. A
lowercase pattern could equally be a name, so it is resolved rather than
assumed. A channel the grant does not name is refused before any sender is
resolved.

**Reply context is not expanded.** Slack stores `thread_ts`; Telegram stores
`reply_to_message_id`. The parent message is not fetched.

**Attachments are named, not fetched.** An upload contributes `[File: <name>]`,
`[Photo]`, or `[Sticker: <emoji>]` ahead of any caption. A message with neither
text nor an attachment is not collected.

## Configuration

Pull sources are configured under the `sync:` key in `config.yaml` /
`config.local.yaml`. Channel sources use `slack.source.*` and
`telegram.source.*` instead.

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

  imap:
    enabled: false                # Off by default
    accounts:                     # One source per mailbox, each with own cursor
      - host: imap.example.net
        username: me@example.net
        label: mailbox            # Source name becomes imap:mailbox
        port: 993
        mailbox: INBOX
    passwords:                    # Keyed by username (in config.local.yaml)
      me@example.net: "..."
    schedule: "*/30 * * * *"
    batch_size: 20
    initial_lookback_days: 1
    condense: false
    match: {}                     # See "Reading mail that is only legible as an image"
    vision: {}

  github:
    enabled: true
    mode: poll                    # poll = gh CLI here; hosted = the channel gateway polls (restart-only)
    pr_watch: true                # Hosted mode: also a record when one of your open PRs changes
    inbox: participating          # participating = threads you take part in; all = the whole inbox (see "GitHub reason guardrail")
    schedule: "*/15 * * * *"
    batch_size: 30
    condense: true                # Haiku extraction for long notifications
    allow_repos: []               # Guardrail allowlist — empty = all repos. Example: ["ClickHouse/*"]
    deny_repos: []                # Guardrail denylist — always dropped (takes precedence)
    allow_actors: []              # Guardrail allowlist of GitHub logins — empty = all. Example: ["alice", "bob"]
    deny_actors: []               # Guardrail denylist of GitHub logins — always dropped (takes precedence)
    allow_reasons: []             # Guardrail allowlist of GitHub reasons — empty = all. Example: ["mention"]
    deny_reasons: []              # Guardrail denylist of reasons. Example: ["comment", "subscribed"]
    deny_ci_branches: []          # Drop CI runs on these branches. Example: ["main", "master"]
    drop_hook: []                 # Command (argv) that gets the dropped records. See "Drop hook"

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
| `condense` | bool | `false` | LLM-condense long stored records via `memory.fast_model` |
| `message_ttl_days` | int | `7` | How long to keep inbox messages |
| `consumer_cursor_ttl_days` | int | `2` | Consumer cursors expire after N days of inactivity |

## Guardrails (Inbox Filtering)

Guardrails programmatically limit **which records ever reach the inbox**. Non-matching
records are dropped at ingestion — they are never persisted to `source_messages`, so the
agent can never see them and they never cost tokens. This shrinks the prompt-injection
attack surface, which matters most in **worker mode** where the agent acts on inbox
content without a human in the loop.

Filtering happens at the choke point in `SourceRunner` (after `source.preprocess()`,
before persist). The source cursor still advances normally. A source with an inclusive
cursor (GitHub) can list a dropped record again on the next run, and the guardrail drops
it again. The number of dropped records is reported in the run summary (visible in the
**Runs** tab and `nerve sync` output).

### How it works

Guardrails are declarative allow/deny rules evaluated against `SourceRecord` fields — a
`metadata` key, or the special `source` / `record_type` attributes. The engine lives in
`nerve/sources/filters.py` (`FieldRule`, `InboxFilter`) and is source-agnostic.

Per-rule semantics:
- **deny wins** — a value matching any `deny` pattern is always dropped.
- **allow is a gate** — if `allow` is non-empty, the value must match one of its patterns,
  otherwise the record is dropped. An absent field fails closed (cannot satisfy `allow`).
- an empty `allow` list means "allow anything not denied".

A record is kept only if it passes **every** rule. Matching is case-insensitive and
supports shell-style globs (`ClickHouse/*`). List-valued metadata (e.g. Gmail `labels`)
matches if any element matches; non-string scalars (e.g. Telegram `chat_id`) are coerced
to strings.

### GitHub repo guardrail

The GitHub notification source matches on `repo_name` (the repo `full_name`, e.g.
`ClickHouse/nerve`):

```yaml
sync:
  github:
    allow_repos: ["ClickHouse/*", "myorg/myrepo"]   # Only these repos reach the inbox
    deny_repos:  ["myorg/noisy-repo"]                # Always dropped, even if allowed above
```

With `allow_repos` empty (the default), all repos pass — behavior is unchanged.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `github.allow_repos` | list | `[]` | Allowlist of repo globs. Empty = all repos pass |
| `github.deny_repos` | list | `[]` | Denylist of repo globs. Takes precedence over `allow_repos` |

### GitHub actor guardrail

The GitHub notification source also matches on `actors` — the list of every GitHub login
involved in a notification (issue/PR author, assignees, comment/review authors, and on a
`reason=assign` notification the account that performed the assignment). This restricts
**who** can put a notification in front of the worker, so a drive-by `@mention` from an
untrusted account is dropped before the agent ever sees it:

```yaml
sync:
  github:
    allow_actors: ["alice", "bob"]   # Only notifications involving these logins reach the inbox
    deny_actors:  ["noisy-bot"]       # Always dropped, even if otherwise allowed
```

`actors` is list-valued, so a notification is kept when **any** involved login matches the
allowlist. A non-empty `allow_actors` is **fail-closed**: a notification with no
identifiable actor (e.g. enrichment failed) is dropped. With `allow_actors` empty (the
default), all actors pass — behavior is unchanged. The repo and actor rules AND together.

Assignments are worth calling out. An `assign` notification names the assignee but not the
assigner, and carries no comment text, so the assigner is fetched from the thread's event
history and rendered as `Assigned by: <login>`. Without it, an assignment on a subject the
inbox account itself authored has no third-party login anywhere — every candidate is that
one account — and a non-empty `allow_actors` would drop it fail-closed. If you rely on
"a maintainer assigns an issue to the bot" as a work signal, that login needs to be in
`allow_actors`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `github.allow_actors` | list | `[]` | Allowlist of GitHub login globs. Empty = all actors pass |
| `github.deny_actors` | list | `[]` | Denylist of GitHub login globs. Takes precedence over `allow_actors` |

### GitHub reason guardrail

`reason` is GitHub's own answer to "why am I being told this": `author` (you opened the
thread), `mention`, `team_mention`, `assign`, `review_requested`, `comment` (you commented
on it once), `subscribed` (you only watch it), `manual` (you subscribed by hand),
`ci_activity`, `state_change`, `security_alert`.

```yaml
sync:
  github:
    deny_reasons: ["comment", "subscribed", "manual", "state_change"]
```

That denylist is the compact way to say *"only threads that are mine or that call me by
name"* — it keeps `author`, `mention`, `team_mention`, `assign`, `review_requested` and
`ci_activity`. Prefer `deny_reasons` over `allow_reasons`: the allowlist is fail-closed, so
a reason GitHub adds later would be silently dropped.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `github.allow_reasons` | list | `[]` | Allowlist of reason globs. Empty = all reasons pass |
| `github.deny_reasons` | list | `[]` | Denylist of reason globs. Takes precedence over `allow_reasons` |

#### Watched repositories: `inbox: all`

With the default `inbox: participating` the source asks GitHub for participating threads
only, so a new pull request that someone else opens in a repository the account watches
never arrives: GitHub files it under `subscribed`, which `participating=true` leaves out.
`inbox: all` lists the account's whole inbox instead, and the guardrails pick what
reaches the agent:

```yaml
sync:
  github:
    inbox: all
    deny_reasons: [ci_activity, comment, state_change]
```

That keeps the threads that are the account's or that call it by name (`author`,
`mention`, `team_mention`, `assign`, `review_requested`) and adds the `subscribed` ones:
the issues and pull requests of watched repositories, as the account's watch settings
produce them, when they are opened and whenever GitHub notifies the account about them
again. It drops the account's own workflow runs and the churn on threads it only
commented on or whose state it changed. Two settings of the GitHub account matter
here:

- **Automatic watching.** GitHub's "Automatically watch repositories" is on by default:
  every repository the account gets push access to is watched for all activity, and each
  of its threads then arrives as `subscribed` (in poll mode, each costs a few GitHub API
  calls for its content before the guardrail can drop it). Switch it off in the account's
  notification settings, which leaves the repositories already watched as they are
  (https://github.com/watching lists them), or watch the repositories that matter with a
  Custom watch (for example Pull requests only).
- **E-mail.** Independently of Nerve, GitHub also e-mails the notifications of watched
  repositories unless the account's "Watching" notifications are set to web only ("On
  GitHub"). Set that on the account, or its mailbox gets a mail for every watched thread.

In hosted mode the channel gateway must support `inbox` (see
[Hosted GitHub source](#hosted-github-source)).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `github.inbox` | string | `participating` | `participating`: only the threads the account takes part in. `all`: its whole inbox as its watch settings produce it, for the guardrails to pick from |

### GitHub CI-branch guardrail

Keeping `ci_activity` lets in every workflow-run notification, including default-branch
runs that have nothing to do with your work — upstream syncs, scheduled cleanup jobs,
deploys. GitHub has no server-side setting for this: one workflow file serves both the
`push`-to-`main` runs and the `pull_request` runs, and the Actions notification preference
is account-wide.

CheckSuite notifications carry no `subject.url` and can't be enriched, but the branch is
right there in the title (`"CI workflow run failed for main branch"`). The source parses it
into the `ci_branch` metadata key, which is `""` for every other notification:

```yaml
sync:
  github:
    deny_ci_branches: ["main", "master"]
```

You keep CI failures on your own PR branches and drop the default-branch noise. This rule
is **deny-only** by design — `ci_branch` is empty for non-CI records, and a non-empty allow
list is fail-closed, so it would drop the entire feed.

**Inert in participating mode:** with `inbox: participating` (the default) the fetch uses
`participating=true`, which never returns `ci_activity` (see the GitHub adapter notes
above), so no CheckSuite record reaches the guardrail. With `inbox: all` the account's own
workflow runs arrive as `ci_activity`, and this rule is what keeps the default-branch runs
among them out; deny the `ci_activity` reason instead to drop every run.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `github.deny_ci_branches` | list | `[]` | Denylist of branch globs for CI runs. Empty = all CI notifications pass |

### GitHub pull request activity records

With `sync.github.mode: hosted` and `pr_watch: true`, the channel gateway also
delivers `github_pr_activity` records, one for each change on the agent's own open
pull requests (see [Hosted GitHub source](#hosted-github-source)). The agent is always
the author of those pull requests: the `reason` of such a record is always `author`, and
its `actors` are the reviewers and commenters. So only the repo guardrail applies to it.
`allow_actors`, `deny_actors`, `allow_reasons`, `deny_reasons` and `deny_ci_branches` are
not checked for these records, neither by the gateway nor here. An `allow_actors` list
that names none of the reviewers, or an `allow_reasons` list without `author`, does not
drop them; `deny_repos` and an `allow_repos` list that the repo does not match do.

### Debugging drops

Dropped records are never persisted, so the run summary count is backed by per-record log
lines naming the rule that fired:

```
Source github: guardrail dropped 3/5 records (e.g. '[owner/repo] CI workflow run failed …')
Source github: dropped 19542… on ci_branch='main' — [owner/repo] CI workflow run failed …
```

### Drop hook

A guardrail drop is silent by design: the agent never sees the record, and nothing
answers the person who sent it. A drop hook lets you act on a drop without letting
the content in front of the agent. Typical uses: acknowledge the dropped message
(for example, react to a GitHub comment from an account that is not on
`allow_actors`), or keep an audit trail of drops.

```yaml
sync:
  github:
    allow_actors: ["alice", "bob"]
    drop_hook: ["/usr/local/bin/my-drop-hook", "--source", "github"]
```

`drop_hook` is an argv list. Nerve runs it directly, never through a shell. A bare
string is one program path, not a command line. The hook runs once per batch that
dropped at least one record, and reads one JSON object per dropped record on stdin:

```json
{"source": "github", "id": "1234", "record_type": "github_notification",
 "summary": "[owner/repo] Some issue (mention)", "timestamp": "2026-01-01T00:00:00Z",
 "metadata": {"reason": "mention", "repo_name": "owner/repo", "actors": ["mallory"], "...": "..."},
 "dropped_on": "actors"}
```

`dropped_on` is the field of the rule that fired. The record's `content` is not
sent: the guardrail exists to keep untrusted content away from automation, and the
identifiers and metadata are enough to act on a drop. Do not use a hook to pass
dropped records to an agent. That reopens the hole the guardrail closes.

**Every field is untrusted input**, not only the content that is left out. A GitHub
`summary` carries the issue or PR title, which whoever opened it chose. Never put a
field into a shell command (`sh -c`, `eval`), a templated argument, or an LLM prompt.
Read the JSON and pass values as data.

For GitHub, `id` is the notification **thread** id. It stays the same when new activity
arrives on the thread, so key idempotency on `id` + `timestamp`, not on `id` alone. The
record does not name the comment that triggered it. A hook that needs it can look it up
with `gh api notifications/threads/<id>` (`subject.latest_comment_url`) or by listing
the thread's comments.

Rules a hook can rely on:

- **Best effort.** If the command is missing, cannot start, exits non-zero, or runs
  longer than 60 seconds, Nerve logs a warning (with the tail of the hook's stderr) and
  the sync continues. A hook never fails a sync and never holds back the cursor.
- **At least one attempt, no retry.** The hook runs after the batch is persisted and
  before the cursor advances. A batch whose persist fails is fetched again, and the hook
  gets its drops on that run. A hook that *fails* is not retried. If every drop must be
  handled, spool the records inside the hook and retry from there.
- **Once per `id` + `timestamp`.** A source can list a dropped record again. GitHub
  queries `since=<cursor>` inclusively, so the newest thread comes back on every run
  until a newer one notifies. Nerve remembers the last 4096 `id` + `timestamp` pairs it
  passed to the hook and does not pass them again. New activity on a thread changes its
  `timestamp`, so that reaches the hook. This memory is in the process only. After a
  restart, a source reload, a CLI `nerve sync`, or a crash between the hook and the
  cursor advance, the hook can get a record again. Make the hook idempotent.
- **Own process group.** The hook starts in a new session. On timeout, Nerve kills the
  whole process group, so a child process (a `gh` or `curl` call from a shell script)
  does not keep running. A process that leaves the group (`setsid`, a daemon) is out of
  reach; Nerve then waits at most 5 more seconds. stdout is discarded and stderr goes to
  a temporary file, so a background job the hook leaves behind does not hold the sync.
  That job keeps running, and it is the hook's own business. Read all of stdin before
  you exit, or start background jobs with stdin from `/dev/null`: a job that inherits
  an unread stdin holds the batch until the timeout.
- **Daemon context.** The hook inherits the daemon's environment (including any API
  keys in it), working directory, and privileges. A `gh` call uses the daemon's GitHub
  login, and its API quota: anyone who can trigger drops (for example, by @-mentioning
  the account) can make the hook run, so rate-limit expensive work inside the hook.
- **Validated at startup.** Without a guardrail nothing is dropped, so the hook never
  runs, and Nerve logs a warning. A program that is not an executable (on `PATH` or by
  path) also logs a warning. A value that is not a list of strings, or has an empty
  program name, turns the hook off with a warning.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `github.drop_hook` | list | `[]` | argv of a command that gets each dropped record as a JSON line on stdin. Passed as is: no shell, nothing stripped. Empty = off |

### Extending to other sources

Adding a guardrail to another source is a config field plus one registry line. In
`build_source_runners()`, build an `InboxFilter` from the relevant metadata key and pass
it to the `SourceRunner`:

```python
from nerve.sources.filters import InboxFilter

# e.g. filter Gmail by label, or Telegram by chat_id
flt = InboxFilter.from_field("labels", allow=cfg.allow_labels, deny=cfg.deny_labels)
runners.append(SourceRunner(..., inbox_filter=flt))
```

A drop hook is one more argument: `SourceRunner(..., inbox_filter=flt, drop_hook=cfg.drop_hook)`.

(Telegram already filters by folder/chat inside `fetch()` via `monitored_folders` /
`exclude_chats`; those remain source-specific.)

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
- `channel_observations` — Push buffer for chat messages a channel collected for the inbox, drained by `ChannelSource`, and for the records of the hosted GitHub source, drained by the `github` runner. Trimmed toward a per-transport row target and TTL-swept by daily cleanup
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

4. **Add config** to `<workspace>/config/settings.yaml`:
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
| `nerve/sources/runner.py` | `SourceRunner` — pure ingestion pipeline (fetch → preprocess → guardrail → persist → condense → drop hook → advance) |
| `nerve/sources/filters.py` | `InboxFilter` / `FieldRule` — declarative allow/deny guardrails applied before persist |
| `nerve/sources/processor.py` | Legacy agent prompt building (unused by runner, may be used by tools) |
| `nerve/sources/registry.py` | Config → `list[SourceRunner]` factory |
| `nerve/sources/telegram.py` | Telegram adapter (Telethon) |
| `nerve/sources/gmail.py` | Gmail adapter (gog CLI) |
| `nerve/sources/github.py` | GitHub notifications adapter (gh CLI), record builder, GitHub guardrail |
| `nerve/sources/github_hosted.py` | GitHub source in hosted mode: drains the records the channel gateway delivered |
| `nerve/sources/github_events.py` | GitHub user events adapter (gh CLI) |
| `nerve/agent/tools.py` | Consumer tools: `list_sources`, `poll_source`, `read_source` |
| `web/src/components/Sources/` | Source message renderers (email, GitHub, default markdown) |

### Cursor Design

**Source cursors** (producer-side): Each source owns its cursor semantics. The framework treats cursors as opaque strings stored in the `sync_cursors` SQLite table. Sources return the next cursor in `FetchResult.next_cursor` — the runner stores it as-is. Cursors advance after successful ingestion (persist to inbox).

**Consumer cursors**: Stored in `consumer_cursors` table with composite key `(consumer, source)`. Each consumer has an independent cursor per source, using the implicit `rowid` of `source_messages` as the offset. New cursors initialize to `MAX(rowid)` (no backlog). Cursors expire after `consumer_cursor_ttl_days` of inactivity.

**Inclusive APIs:** Both GitHub and Gmail APIs use inclusive cursor semantics (returning records `>= cursor`). Gmail advances the cursor at query time (+1s) and applies client-side dedup as a safety net. GitHub queries the cursor itself, because a notification can land in the cursor's own second; storage skips the unchanged thread listed again.

**Concurrency safety:** Each `SourceRunner` holds an `asyncio.Lock` to prevent concurrent execution of the same source. If a cron schedule fires while a manual sync is in progress, the second call returns immediately with 0 records.

### Security

All message content returned by `poll_source` and `read_source` is prefixed with an untrusted data warning. Source messages come from external services and may contain prompt injection attempts. The agent is instructed to act only on factual information (sender, subject, PR numbers) and never follow instructions embedded in message content.
